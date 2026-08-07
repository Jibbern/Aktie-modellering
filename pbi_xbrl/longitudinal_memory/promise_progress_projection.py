"""Pure source-native Promise Progress projection and shadow lineage.

The projection deliberately has no workbook dependency.  It consumes one accepted
longitudinal-memory package plus a declarative product plan, creates four distinct
block projections, and serializes one immutable ``PromiseProgressProduct@1``.
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Sequence

from .serialization import serialize_package


PRODUCT_CONTRACT_ID = "contract:promise-progress-product@1"
PRODUCT_TYPE = "PromiseProgressProduct@1"
SHADOW_SCHEMA_ID = "schema:promise-progress-shadow-matrix@1"
SHEET_NAME = "Promise_Progress_UI"

SCORECARD_BLOCK_ID = "block:promise-progress:management-credibility-scorecard@1"
ANNUAL_BLOCK_ID = "block:promise-progress:annual-guidance-progression@1"
OPEN_BLOCK_ID = "block:promise-progress:open-guidance@1"
TIMELINE_BLOCK_ID = "block:promise-progress:quarterly-revision-timeline@1"
BLOCK_ORDER = (SCORECARD_BLOCK_ID, ANNUAL_BLOCK_ID, OPEN_BLOCK_ID, TIMELINE_BLOCK_ID)

MILESTONE_STATES = frozenset(
    {"completed", "in_progress", "not_started", "failed", "withdrawn", "unknown"}
)

PARITY_DIFFERENCE_CLASSES = frozenset(
    {
        "exact-match",
        "accepted-semantic-match",
        "registered-authorized-exception",
        "unauthorized-exception-binding",
        "unregistered-difference",
        "legacy-only-field",
        "source-native-only-field",
        "mapping-alignment-defect",
        "structurally-incomparable",
    }
)

PARITY_OBSERVED_DIFFERENCE_CLASSES = frozenset(
    {
        "unregistered-difference",
        "reviewed-destination-remap",
    }
)

PARITY_DIFFERENCE_REASON_CODES = frozenset(
    {
        "accepted_annual_actual_unavailable",
        "equivalent_label_display_normalized",
        "equivalent_value_display_normalized",
        "legacy_generic_source_note_replaced_by_lineage",
        "legacy_static_status_replaced_by_replay",
        "legacy_temporal_leakage_removed",
        "legacy_unreviewed_scorecard_value",
        "legacy_parallel_tracker_ownership_removed",
        "legacy_lossy_support_matrix_replaced",
        "legacy_fuzzy_trace_key_replaced",
        "reviewed_row_destination_remap",
        "reviewed_semantic_row_pair",
        "legacy_unsupported_by_accepted_product",
        "legacy_terminal_summary_replaced_by_typed_history",
        "source_native_canonical_row_legacy_omitted",
        "source_native_typed_history_legacy_omitted",
    }
)

PARITY_ROW_DISPOSITION_KINDS = frozenset({"paired", "legacy_only", "source_native_only"})
PARITY_ROW_MAPPING_KINDS = frozenset(
    {"same-destination-equivalent", "reviewed-destination-remap", "none"}
)
PARITY_STRUCTURAL_CONDITION_TYPES = frozenset(
    {
        "empty-tracker-parallel-ownership",
        "fuzzy-hidden-trace-identity",
        "lossy-support-matrix-ownership",
    }
)
PARITY_STRUCTURAL_OBSERVATION_STATES = {
    "empty-tracker-parallel-ownership": {
        "observed_legacy_state": (
            "Promise_Tracker is empty while Promise_Progress_UI and legacy support surfaces "
            "separately materialize product state."
        ),
        "observed_source_native_state": (
            "One immutable PromiseProgressProduct owns selection, status, visible rows, "
            "evidence lineage, trace keys, and shadow fields."
        ),
    },
    "fuzzy-hidden-trace-identity": {
        "observed_legacy_state": (
            "Hidden column O uses guidance-centric display slugs that do not provide a "
            "canonical versioned row identity."
        ),
        "observed_source_native_state": (
            "Every visible product row owns one stable versioned row_id and resolves to "
            "complete field lineage in the shadow matrix."
        ),
    },
    "lossy-support-matrix-ownership": {
        "observed_legacy_state": (
            "Legacy support matrices and the visible UI contain different subsets, bases, "
            "and repair-pass results."
        ),
        "observed_source_native_state": (
            "One complete field-level shadow matrix is the sole semantic projection for UI, "
            "validation, and parity reporting."
        ),
    },
}
PARITY_STRUCTURAL_BLOCK_SCOPE = {
    "empty-tracker-parallel-ownership": BLOCK_ORDER,
    "fuzzy-hidden-trace-identity": (TIMELINE_BLOCK_ID,),
    "lossy-support-matrix-ownership": BLOCK_ORDER,
}
ROW_DISPOSITION_VERSION = "row-disposition-version:1@1"
STRUCTURAL_OBSERVATION_VERSION = "structural-observation-version:1@1"
STRUCTURAL_BINDING_VERSION = "structural-binding-version:1@1"
LEGACY_CAPTURE_MANIFEST_VERSION = "legacy-capture-manifest-version:1@1"
SOURCE_SCOPE_MANIFEST_VERSION = "source-scope-manifest-version:1@1"
LEGACY_COMPARISON_SCOPE = "Promise_Progress_UI!A5:L87:reviewed-populated-product-rows@1"

SCORECARD_EXCEPTION_ID = "parity:scorecard-unreviewed-legacy@1"
ACTUAL_COVERAGE_EXCEPTION_ID = "parity:anf-accepted-actual-coverage-gap@1"
STATIC_STATUS_EXCEPTION_ID = "parity:static-status-without-rule@1"
GENERIC_SOURCE_EXCEPTION_ID = "parity:generic-source-note-without-locator@1"
TEMPORAL_EXCEPTION_ID = "parity:temporal-leakage-pre-release-actuals@1"
EMPTY_TRACKER_EXCEPTION_ID = "parity:empty-tracker-parallel-ownership@1"
LOSSY_MATRIX_EXCEPTION_ID = "parity:lossy-support-matrix@1"
FUZZY_TRACE_EXCEPTION_ID = "parity:fuzzy-hidden-trace-keys@1"
DISPLAY_NORMALIZATION_EXCEPTION_ID = "parity:equivalent-display-normalization@1"
ROW_REMAP_EXCEPTION_ID = "parity:reviewed-row-destination-remap@1"

CLOSED_PARITY_EXCEPTION_IDS = frozenset(
    {
        TEMPORAL_EXCEPTION_ID,
        SCORECARD_EXCEPTION_ID,
        EMPTY_TRACKER_EXCEPTION_ID,
        GENERIC_SOURCE_EXCEPTION_ID,
        STATIC_STATUS_EXCEPTION_ID,
        LOSSY_MATRIX_EXCEPTION_ID,
        FUZZY_TRACE_EXCEPTION_ID,
        ACTUAL_COVERAGE_EXCEPTION_ID,
        DISPLAY_NORMALIZATION_EXCEPTION_ID,
        ROW_REMAP_EXCEPTION_ID,
    }
)

_PARITY_FIELD_SCOPES: Mapping[str, frozenset[tuple[str, str]]] = MappingProxyType(
    {
        SCORECARD_EXCEPTION_ID: frozenset(
            (SCORECARD_BLOCK_ID, field_role) for field_role in ("score", "evidence", "read")
        ),
        ACTUAL_COVERAGE_EXCEPTION_ID: frozenset(
            (ANNUAL_BLOCK_ID, field_role) for field_role in ("actual", "status", "notes_source")
        ),
        STATIC_STATUS_EXCEPTION_ID: frozenset(
            (block_id, "status") for block_id in (ANNUAL_BLOCK_ID, OPEN_BLOCK_ID, TIMELINE_BLOCK_ID)
        ),
        GENERIC_SOURCE_EXCEPTION_ID: frozenset(
            {
                (ANNUAL_BLOCK_ID, "notes_source"),
                (OPEN_BLOCK_ID, "notes_source"),
                (TIMELINE_BLOCK_ID, "source_note"),
            }
        ),
        TEMPORAL_EXCEPTION_ID: frozenset(
            (TIMELINE_BLOCK_ID, field_role) for field_role in ("actual", "status", "source_note")
        ),
        EMPTY_TRACKER_EXCEPTION_ID: frozenset(),
        LOSSY_MATRIX_EXCEPTION_ID: frozenset(),
        FUZZY_TRACE_EXCEPTION_ID: frozenset(),
        DISPLAY_NORMALIZATION_EXCEPTION_ID: frozenset(),
        ROW_REMAP_EXCEPTION_ID: frozenset(),
    }
)

_STRUCTURAL_PARITY_EXCEPTION_IDS = frozenset(
    {EMPTY_TRACKER_EXCEPTION_ID, LOSSY_MATRIX_EXCEPTION_ID, FUZZY_TRACE_EXCEPTION_ID}
)

PARITY_EXCEPTION_REGISTRY: Mapping[str, Mapping[str, str]] = MappingProxyType(
    {
        TEMPORAL_EXCEPTION_ID: MappingProxyType(
            {
                "legacy_behavior": "Pre-release rows may show later final actual, progress, or status values.",
                "source_native_behavior": "Each historical row uses only records eligible at its explicit UI as-of date.",
                "semantic_reason": "Prevents look-ahead and preserves historical epistemic state.",
                "acceptance_owner": "longitudinal-memory architecture owner",
                "duration": "permanent",
            }
        ),
        SCORECARD_EXCEPTION_ID: MappingProxyType(
            {
                "legacy_behavior": "The legacy scorecard contains hard-coded scores and investor reads without a reviewed assessment artifact.",
                "source_native_behavior": "The scorecard geometry remains present while accepted Score and Read content remains Needs Review.",
                "semantic_reason": "Separates analytical assessment from source facts.",
                "acceptance_owner": "Promise Progress product owner",
                "duration": "temporary until a reviewed assessment artifact is accepted",
            }
        ),
        EMPTY_TRACKER_EXCEPTION_ID: MappingProxyType(
            {
                "legacy_behavior": "Promise_Tracker is empty while other legacy sheets separately materialize product data.",
                "source_native_behavior": "One immutable product owns selection, status, visible rows, traces, and shadow fields.",
                "semantic_reason": "Removes parallel truth engines.",
                "acceptance_owner": "workbook architecture owner",
                "duration": "permanent",
            }
        ),
        GENERIC_SOURCE_EXCEPTION_ID: MappingProxyType(
            {
                "legacy_behavior": "Visible notes may use generic publisher/date prose without immutable locators.",
                "source_native_behavior": "Compact prose remains visible while each shadow field retains exact documents and occurrences.",
                "semantic_reason": "Adds replayable lineage without expanding the first-parity UI.",
                "acceptance_owner": "evidence-lineage owner",
                "duration": "permanent",
            }
        ),
        STATIC_STATUS_EXCEPTION_ID: MappingProxyType(
            {
                "legacy_behavior": "Legacy Status cells contain non-replayable static labels and fills.",
                "source_native_behavior": "Every nonblank status replays one closed rule; unsafe labels become Needs Review or Basis-dependent.",
                "semantic_reason": "Makes status deterministic and fail-closed.",
                "acceptance_owner": "Promise Progress product owner",
                "duration": "permanent",
            }
        ),
        LOSSY_MATRIX_EXCEPTION_ID: MappingProxyType(
            {
                "legacy_behavior": "Legacy support matrices and the visible UI contain different lossy subsets.",
                "source_native_behavior": "One complete field-level shadow matrix generates validation, parity, and later UI views.",
                "semantic_reason": "Retains target history, basis, evidence, review, and temporal state.",
                "acceptance_owner": "workbook architecture owner",
                "duration": "permanent",
            }
        ),
        FUZZY_TRACE_EXCEPTION_ID: MappingProxyType(
            {
                "legacy_behavior": "Hidden column O uses fuzzy display-derived trace slugs while M and N are unused.",
                "source_native_behavior": "M and N remain reserved; O receives only a stable versioned product row_id in a later writer phase.",
                "semantic_reason": "Replaces lossy fuzzy identity with deterministic lineage.",
                "acceptance_owner": "evidence-lineage owner",
                "duration": "permanent",
            }
        ),
        ACTUAL_COVERAGE_EXCEPTION_ID: MappingProxyType(
            {
                "legacy_behavior": "Legacy annual cells contain hard-coded outcomes not all present as compatible accepted source-native facts.",
                "source_native_behavior": "Unsupported Actual cells remain blank or Needs Review and comparable sales is not relabelled net-sales growth.",
                "semantic_reason": "Prevents workbook values and incompatible metrics from becoming canonical facts.",
                "acceptance_owner": "source-native product owner",
                "duration": "temporary source-coverage gap",
            }
        ),
        DISPLAY_NORMALIZATION_EXCEPTION_ID: MappingProxyType(
            {
                "legacy_behavior": "The legacy UI uses presentation-specific labels and value formatting.",
                "source_native_behavior": "Equivalent values use deterministic product-owned display text while retaining the same economic identity.",
                "semantic_reason": "Normalizes equivalent display representation without changing economics.",
                "acceptance_owner": "Promise Progress product owner",
                "duration": "permanent",
            }
        ),
        ROW_REMAP_EXCEPTION_ID: MappingProxyType(
            {
                "legacy_behavior": "A reviewed legacy business row occupies a different visible destination from its source-native counterpart.",
                "source_native_behavior": "The semantic counterpart is selected by typed identity and rendered in deterministic product order.",
                "semantic_reason": "Preserves a reviewed business concept while removing workbook-row-order ownership.",
                "acceptance_owner": "Promise Progress product owner",
                "duration": "permanent",
            }
        ),
    }
)

ACTUAL_FY_ID = "actual:promise-progress:fiscal-year-outcome@1"
ACTUAL_QUARTER_ID = "actual:promise-progress:quarter-outcome@1"
ACTUAL_YTD_ID = "actual:promise-progress:year-to-date-outcome@1"
ACTUAL_CUMULATIVE_ID = "actual:promise-progress:cumulative-outcome@1"
ACTUAL_MILESTONE_ID = "actual:promise-progress:milestone-outcome@1"
ACTUAL_COMPOSITE_ID = "actual:promise-progress:labelled-composite@1"
CLOSED_ACTUAL_ROLE_IDS = frozenset(
    {ACTUAL_FY_ID, ACTUAL_QUARTER_ID, ACTUAL_YTD_ID, ACTUAL_CUMULATIVE_ID, ACTUAL_MILESTONE_ID, ACTUAL_COMPOSITE_ID}
)

PROGRESS_FY_ID = "progress:promise-progress:fiscal-year-actual@1"
PROGRESS_YTD_ID = "progress:promise-progress:year-to-date-actual@1"
PROGRESS_CUMULATIVE_ID = "progress:promise-progress:cumulative-actual@1"
PROGRESS_RUN_RATE_ID = "progress:promise-progress:annualized-run-rate@1"
PROGRESS_REALIZED_ID = "progress:promise-progress:realized-period-amount@1"
PROGRESS_IDENTIFIED_ID = "progress:promise-progress:identified-or-initiated-amount@1"
PROGRESS_REMAINING_ID = "progress:promise-progress:remaining-amount@1"
PROGRESS_DELTA_ID = "progress:promise-progress:delta-to-target@1"
PROGRESS_MILESTONE_ID = "progress:promise-progress:milestone-state@1"
PROGRESS_DIRECTIONAL_ID = "progress:promise-progress:directional-qualitative@1"
CLOSED_PROGRESS_ROLE_IDS = frozenset(
    {
        PROGRESS_FY_ID,
        PROGRESS_YTD_ID,
        PROGRESS_CUMULATIVE_ID,
        PROGRESS_RUN_RATE_ID,
        PROGRESS_REALIZED_ID,
        PROGRESS_IDENTIFIED_ID,
        PROGRESS_REMAINING_ID,
        PROGRESS_DELTA_ID,
        PROGRESS_MILESTONE_ID,
        PROGRESS_DIRECTIONAL_ID,
    }
)

ACTUAL_ROLE_SEMANTIC_CLASSES: Mapping[str, str] = MappingProxyType(
    {
        ACTUAL_FY_ID: "fiscal-year-outcome",
        ACTUAL_QUARTER_ID: "quarter-outcome",
        ACTUAL_YTD_ID: "year-to-date-outcome",
        ACTUAL_CUMULATIVE_ID: "cumulative-outcome",
        ACTUAL_MILESTONE_ID: "milestone-outcome",
        ACTUAL_COMPOSITE_ID: "labelled-composite",
    }
)

PROGRESS_ROLE_SEMANTIC_CLASSES: Mapping[str, str] = MappingProxyType(
    {
        PROGRESS_FY_ID: "fiscal-year-actual",
        PROGRESS_YTD_ID: "year-to-date-actual",
        PROGRESS_CUMULATIVE_ID: "cumulative-actual",
        PROGRESS_RUN_RATE_ID: "annualized-run-rate",
        PROGRESS_REALIZED_ID: "realized-period-amount",
        PROGRESS_IDENTIFIED_ID: "identified-or-initiated-amount",
        PROGRESS_REMAINING_ID: "remaining-amount",
        PROGRESS_DELTA_ID: "delta-to-target",
        PROGRESS_MILESTONE_ID: "milestone-state",
        PROGRESS_DIRECTIONAL_ID: "directional-qualitative",
    }
)

_CLOSED_PERIOD_TYPES = frozenset({"annual", "quarter", "month", "year_to_date", "instant", "program"})

STATUS_POINT_ID = "assessment:promise-progress:numeric-point-target@1"
STATUS_RANGE_ID = "assessment:promise-progress:numeric-range-target@1"
STATUS_MIN_ID = "assessment:promise-progress:minimum-bound-target@1"
STATUS_MAX_ID = "assessment:promise-progress:maximum-bound-target@1"
STATUS_APPROX_ID = "assessment:promise-progress:approximate-target@1"
STATUS_CUMULATIVE_ID = "assessment:promise-progress:cumulative-target@1"
STATUS_RUN_RATE_ID = "assessment:promise-progress:annualized-or-run-rate-target@1"
STATUS_MILESTONE_ID = "assessment:promise-progress:date-or-milestone@1"
STATUS_QUALITATIVE_ID = "assessment:promise-progress:qualitative-commitment@1"
STATUS_OPEN_ID = "assessment:promise-progress:active-open-guidance@1"
STATUS_BASIS_ID = "assessment:promise-progress:basis-composite@1"
STATUS_REVIEW_ID = "assessment:promise-progress:conflicting-or-insufficient-evidence@1"
CLOSED_STATUS_RULE_IDS = frozenset(
    {
        STATUS_POINT_ID,
        STATUS_RANGE_ID,
        STATUS_MIN_ID,
        STATUS_MAX_ID,
        STATUS_APPROX_ID,
        STATUS_CUMULATIVE_ID,
        STATUS_RUN_RATE_ID,
        STATUS_MILESTONE_ID,
        STATUS_QUALITATIVE_ID,
        STATUS_OPEN_ID,
        STATUS_BASIS_ID,
        STATUS_REVIEW_ID,
    }
)

STATUS_LABELS = MappingProxyType(
    {
        "completed": "Completed",
        "hit": "Hit",
        "beat": "Beat",
        "on_track": "On track",
        "open": "Open",
        "mixed": "Mixed",
        "missed": "Missed",
        "basis_dependent": "Basis-dependent",
        "needs_review": "Needs Review",
        "withdrawn": "Withdrawn",
    }
)

SCORECARD_CATEGORIES = (
    "Sales guidance accuracy",
    "Margin guidance accuracy",
    "EPS guidance accuracy",
    "Buyback/capital allocation delivery",
    "Inventory discipline",
)

ANNUAL_DATA_ROWS = (13, 14, 15, 16, 17, 18, 19, 20, 24, 25, 26, 30, 31, 35)
OPEN_DATA_ROWS = tuple(range(39, 47)) + tuple(range(48, 57))
TIMELINE_DATA_ROWS = (
    61,
    62,
    63,
    64,
    65,
    66,
    67,
    71,
    72,
    73,
    74,
    78,
    79,
    80,
    81,
    82,
    83,
    86,
    87,
    88,
    92,
    93,
    94,
    95,
    99,
    100,
    101,
    102,
)

BLOCK_FIELD_LAYOUT: Mapping[str, tuple[tuple[str, str, str], ...]] = MappingProxyType(
    {
        SCORECARD_BLOCK_ID: (
            ("category", "A", "A"),
            ("score", "B", "B"),
            ("evidence", "C", "C:F"),
            ("read", "G", "G:L"),
        ),
        ANNUAL_BLOCK_ID: (
            ("metric", "A", "A"),
            ("initial_guide", "B", "B"),
            ("q1_guide", "C", "C"),
            ("q2_guide", "D", "D"),
            ("q3_guide", "E", "E"),
            ("q4_guide", "F", "F"),
            ("actual", "G", "G"),
            ("status", "H", "H"),
            ("notes_source", "I", "I:L"),
        ),
        OPEN_BLOCK_ID: (
            ("metric", "A", "A"),
            ("current_guide", "B", "B"),
            ("horizon", "C", "C"),
            ("status", "D", "D"),
            ("notes_source", "E", "E:L"),
        ),
        TIMELINE_BLOCK_ID: (
            ("metric", "A", "A"),
            ("previous_guide", "B", "B"),
            ("current_guide", "C", "C"),
            ("change_type", "D", "D"),
            ("actual", "E", "E"),
            ("progress", "F", "F"),
            ("status", "G", "G"),
            ("horizon", "H", "H"),
            ("stated_in", "I", "I"),
            ("source_date", "J", "J"),
            ("source_note", "K", "K:L"),
        ),
    }
)

_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@/-]*$")
_SHA_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class PromiseProgressProjectionError(ValueError):
    """Raised when a product cannot be projected without guessing."""


def _parse_date(value: str, *, label: str) -> date:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise PromiseProgressProjectionError(f"{label} must be an ISO date, received {value!r}.") from exc


def _canonical(value: Any) -> Any:
    if dataclasses.is_dataclass(value):
        value = {item.name: getattr(value, item.name) for item in dataclasses.fields(value)}
    if isinstance(value, Mapping):
        return {str(key): _canonical(child) for key, child in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, tuple):
        return [_canonical(child) for child in value]
    if isinstance(value, list):
        return [_canonical(child) for child in value]
    if isinstance(value, float):
        raise PromiseProgressProjectionError("Floating-point values are forbidden in Promise Progress products.")
    return value


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze(child) for key, child in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(child) for child in value)
    return value


def _canonical_bytes(value: Any) -> bytes:
    return (json.dumps(_canonical(value), ensure_ascii=False, allow_nan=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(_canonical(value), ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).hexdigest()


def _short_digest(value: Any, length: int = 20) -> str:
    return _digest(value)[:length]


def _require_id(value: str, *, label: str) -> str:
    if not value or not _ID_PATTERN.fullmatch(value):
        raise PromiseProgressProjectionError(f"{label} is not a stable product identity: {value!r}.")
    return value


def _require_sha(value: str, *, label: str) -> str:
    if not _SHA_PATTERN.fullmatch(value):
        raise PromiseProgressProjectionError(f"{label} must be a lowercase SHA-256 digest.")
    return value


def _decimal(value: Any) -> Decimal:
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise PromiseProgressProjectionError(f"Invalid canonical decimal {value!r}.") from exc
    if not result.is_finite():
        raise PromiseProgressProjectionError("Product decimal values must be finite.")
    return result


def _plain_decimal(value: Any) -> str:
    parsed = _decimal(value)
    text = format(parsed, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _sorted_unique(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted({str(value) for value in values if str(value)}))


@dataclass(frozen=True)
class DisplayValue:
    value_form: str
    display_text: str
    machine_value: Any = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "machine_value", _freeze(self.machine_value))
        if self.value_form not in {"exact", "approximate", "range", "bound", "percentage", "qualitative", "date", "missing"}:
            raise PromiseProgressProjectionError(f"Unsupported display value form {self.value_form!r}.")
        if self.value_form == "missing" and (self.display_text or self.machine_value is not None):
            raise PromiseProgressProjectionError("Missing values must display as a blank and have a null machine value.")
        if self.value_form in {"exact", "percentage"}:
            if not isinstance(self.machine_value, str):
                raise PromiseProgressProjectionError("Exact numeric display values require a canonical decimal string.")
            _decimal(self.machine_value)
        elif self.value_form == "approximate":
            if not isinstance(self.machine_value, Mapping) or set(self.machine_value) != {"value", "qualifier", "tolerance"}:
                raise PromiseProgressProjectionError("Approximate display values require value, qualifier and tolerance.")
            _decimal(self.machine_value["value"])
            if self.machine_value["qualifier"] not in {"around", "about", "approximately", "tilde"}:
                raise PromiseProgressProjectionError("Approximate display value has an unsupported qualifier.")
            if self.machine_value["tolerance"] is not None:
                _decimal(self.machine_value["tolerance"])
        elif self.value_form == "range":
            if not isinstance(self.machine_value, Mapping) or set(self.machine_value) != {
                "low", "high", "low_inclusive", "high_inclusive"
            }:
                raise PromiseProgressProjectionError("Range display values require the closed bound representation.")
            if not all(isinstance(self.machine_value[key], bool) for key in ("low_inclusive", "high_inclusive")):
                raise PromiseProgressProjectionError("Range inclusivity flags must be Boolean.")
            if _decimal(self.machine_value["low"]) > _decimal(self.machine_value["high"]):
                raise PromiseProgressProjectionError("Range display value has reversed bounds.")
        elif self.value_form == "bound":
            if not isinstance(self.machine_value, Mapping) or set(self.machine_value) != {"operator", "value"}:
                raise PromiseProgressProjectionError("Bound display values require operator and value.")
            if self.machine_value["operator"] not in {"gt", "gte", "lt", "lte"}:
                raise PromiseProgressProjectionError("Bound display value has an unsupported operator.")
            _decimal(self.machine_value["value"])
        elif self.value_form == "date":
            if not isinstance(self.machine_value, str):
                raise PromiseProgressProjectionError("Date display values require an ISO date machine value.")
            _parse_date(self.machine_value, label="display date")
        elif self.value_form == "qualitative":
            machine = self.machine_value
            if isinstance(machine, Mapping):
                if set(machine) != {"text", "normalized_band"} or not str(machine["text"]).strip():
                    raise PromiseProgressProjectionError("Qualitative source text has an unsupported machine representation.")
                if machine["normalized_band"] is not None and not isinstance(machine["normalized_band"], str):
                    raise PromiseProgressProjectionError("Qualitative normalized band must be text or null.")
            elif isinstance(machine, tuple):
                for component in machine:
                    if not isinstance(component, Mapping) or set(component) != {"label", "record_id", "value"}:
                        raise PromiseProgressProjectionError("Qualitative composite has an unsupported component.")
                if not isinstance(component["record_id"], str) or not component["record_id"]:
                    raise PromiseProgressProjectionError(
                        "qualitative component record_id must be a non-empty canonical record identity."
                    )
                    _decimal(component["value"])
            elif machine is not None and not isinstance(machine, str):
                raise PromiseProgressProjectionError("Qualitative display value has an unsupported machine representation.")

    def to_dict(self) -> dict[str, Any]:
        return {"value_form": self.value_form, "display_text": self.display_text, "machine_value": _canonical(self.machine_value)}


MISSING_DISPLAY = DisplayValue("missing", "", None)


@dataclass(frozen=True)
class SemanticIdentity:
    metric_id: str | None
    definition_id: str | None
    basis_id: str | None
    unit_id: str | None
    dimensions: tuple[tuple[str, str], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "metric_id": self.metric_id,
            "definition_id": self.definition_id,
            "basis_id": self.basis_id,
            "unit_id": self.unit_id,
            "dimensions": [{"axis_id": axis, "member_id": member} for axis, member in self.dimensions],
        }


EMPTY_SEMANTIC_IDENTITY = SemanticIdentity(None, None, None, None, ())


@dataclass(frozen=True)
class MilestoneState:
    state: str
    source_text: str
    assessment_method_id: str
    knowledge_date: str
    deadline_or_horizon_id: str | None
    review_state: str
    source_occurrence_ids: tuple[str, ...]
    source_document_ids: tuple[str, ...]
    lineage_digest: str

    def __post_init__(self) -> None:
        if self.state not in MILESTONE_STATES:
            raise PromiseProgressProjectionError(f"Unknown milestone state {self.state!r}.")
        if not self.source_text.strip():
            raise PromiseProgressProjectionError("A milestone state requires exact source text.")
        _require_id(self.assessment_method_id, label="milestone assessment_method_id")
        _parse_date(self.knowledge_date, label="milestone knowledge_date")
        if self.deadline_or_horizon_id is not None:
            _require_id(self.deadline_or_horizon_id, label="milestone deadline_or_horizon_id")
        if self.review_state not in {"accepted", "needs_review"}:
            raise PromiseProgressProjectionError("A milestone state has an unsupported review state.")
        _require_sha(self.lineage_digest, label="milestone lineage_digest")

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclass(frozen=True)
class ActualSelection:
    actual_id: str
    actual_role_id: str
    semantic_class: str
    selection_state: str
    canonical_observation_ids: tuple[str, ...]
    semantic_identity: SemanticIdentity
    effective_or_fiscal_period_id: str | None
    publication_date: str | None
    knowledge_date: str | None
    value_form: str
    source_occurrence_ids: tuple[str, ...]
    source_document_ids: tuple[str, ...]
    display_value: DisplayValue
    milestone_state: MilestoneState | None
    selection_method_id: str
    lineage_state: str
    lineage_digest: str

    def __post_init__(self) -> None:
        if self.actual_role_id not in CLOSED_ACTUAL_ROLE_IDS:
            raise PromiseProgressProjectionError(f"Unknown Actual role {self.actual_role_id!r}.")
        if self.semantic_class != ACTUAL_ROLE_SEMANTIC_CLASSES[self.actual_role_id]:
            raise PromiseProgressProjectionError("Actual semantic class differs from the closed role registry.")
        if self.selection_state not in {"selected", "missing_by_absence", "missing_by_cutoff", "incompatible", "conflicting", "blocked_by_review"}:
            raise PromiseProgressProjectionError(f"Unknown Actual selection state {self.selection_state!r}.")
        if self.selection_state == "selected" and not self.canonical_observation_ids:
            raise PromiseProgressProjectionError("A selected Actual requires canonical observation input.")
        if self.selection_state != "selected" and self.display_value.value_form != "missing":
            raise PromiseProgressProjectionError("An unselected Actual must remain visibly blank.")
        if self.milestone_state is not None:
            if self.actual_role_id != ACTUAL_MILESTONE_ID or self.selection_state != "selected":
                raise PromiseProgressProjectionError("Milestone state may attach only to a selected milestone Actual.")
            if self.milestone_state.source_text != self.display_value.display_text:
                raise PromiseProgressProjectionError("Milestone state source text differs from the selected source-backed Actual.")
            if not set(self.milestone_state.source_occurrence_ids) <= set(self.source_occurrence_ids):
                raise PromiseProgressProjectionError("Milestone state occurrences are not owned by the selected Actual.")
            if not set(self.milestone_state.source_document_ids) <= set(self.source_document_ids):
                raise PromiseProgressProjectionError("Milestone state documents are not owned by the selected Actual.")

    def to_dict(self) -> dict[str, Any]:
        return {
            "actual_id": self.actual_id,
            "actual_role_id": self.actual_role_id,
            "semantic_class": self.semantic_class,
            "selection_state": self.selection_state,
            "canonical_observation_ids": list(self.canonical_observation_ids),
            "semantic_identity": self.semantic_identity.to_dict(),
            "effective_or_fiscal_period_id": self.effective_or_fiscal_period_id,
            "publication_date": self.publication_date,
            "knowledge_date": self.knowledge_date,
            "value_form": self.value_form,
            "source_occurrence_ids": list(self.source_occurrence_ids),
            "source_document_ids": list(self.source_document_ids),
            "display_value": self.display_value.to_dict(),
            "milestone_state": self.milestone_state.to_dict() if self.milestone_state else None,
            "selection_method_id": self.selection_method_id,
            "lineage_state": self.lineage_state,
            "lineage_digest": self.lineage_digest,
        }


@dataclass(frozen=True)
class ProgressSelection:
    progress_id: str
    progress_role_id: str
    semantic_class: str
    canonical_input_ids: tuple[str, ...]
    governing_target_version_id: str | None
    semantic_identity: SemanticIdentity
    period_or_horizon_id: str | None
    method_id: str
    ui_as_of_date: str
    publication_dates: tuple[str, ...]
    knowledge_dates: tuple[str, ...]
    display_value: DisplayValue
    review_state: str
    source_occurrence_ids: tuple[str, ...]
    source_document_ids: tuple[str, ...]
    lineage_digest: str

    def __post_init__(self) -> None:
        if self.progress_role_id not in CLOSED_PROGRESS_ROLE_IDS:
            raise PromiseProgressProjectionError(f"Unknown Progress role {self.progress_role_id!r}.")
        if self.semantic_class != PROGRESS_ROLE_SEMANTIC_CLASSES[self.progress_role_id]:
            raise PromiseProgressProjectionError("Progress semantic class differs from the closed role registry.")
        if not self.canonical_input_ids:
            raise PromiseProgressProjectionError("A displayed Progress value requires canonical inputs.")
        if self.display_value.value_form == "missing":
            raise PromiseProgressProjectionError("Missing Progress is represented by no Progress object, not a synthetic value.")
        calculated = self.progress_role_id in {PROGRESS_REMAINING_ID, PROGRESS_DELTA_ID}
        if calculated != (self.governing_target_version_id is not None):
            raise PromiseProgressProjectionError(
                "Only calculated remaining/delta Progress may own a governing target version."
            )
        if self.governing_target_version_id and self.governing_target_version_id not in self.canonical_input_ids:
            raise PromiseProgressProjectionError(
                "Calculated Progress lineage must retain its governing target version."
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "progress_id": self.progress_id,
            "progress_role_id": self.progress_role_id,
            "semantic_class": self.semantic_class,
            "canonical_input_ids": list(self.canonical_input_ids),
            "governing_target_version_id": self.governing_target_version_id,
            "semantic_identity": self.semantic_identity.to_dict(),
            "period_or_horizon_id": self.period_or_horizon_id,
            "method_id": self.method_id,
            "ui_as_of_date": self.ui_as_of_date,
            "publication_dates": list(self.publication_dates),
            "knowledge_dates": list(self.knowledge_dates),
            "display_value": self.display_value.to_dict(),
            "review_state": self.review_state,
            "source_occurrence_ids": list(self.source_occurrence_ids),
            "source_document_ids": list(self.source_document_ids),
            "lineage_digest": self.lineage_digest,
        }


@dataclass(frozen=True)
class StatusAssessment:
    status_assessment_id: str
    status_code: str
    visible_label: str
    assessment_rule_id: str
    canonical_input_ids: tuple[str, ...]
    target_version_id: str | None
    actual_or_progress_role_id: str | None
    ui_as_of_date: str
    assessment_result: str
    review_state: str
    explanation: str
    review_issue_ids: tuple[str, ...]
    lineage_digest: str

    def __post_init__(self) -> None:
        if self.assessment_rule_id not in CLOSED_STATUS_RULE_IDS:
            raise PromiseProgressProjectionError(f"Unknown Status rule {self.assessment_rule_id!r}.")
        if self.status_code not in STATUS_LABELS:
            raise PromiseProgressProjectionError(f"Unknown status code {self.status_code!r}.")
        if STATUS_LABELS[self.status_code] != self.visible_label:
            raise PromiseProgressProjectionError("A status label cannot override the closed status-code registry.")

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclass(frozen=True)
class ProductField:
    product_id: str
    block_id: str
    row_id: str
    field_id: str
    field_role: str
    anchor_cell: str
    display_range: str
    display_value: DisplayValue
    canonical_record_ids: tuple[str, ...]
    target_version_id: str | None
    actual_selection_id: str | None
    actual_observation_id: str | None
    progress_selection_id: str | None
    progress_observation_ids: tuple[str, ...]
    status_assessment_id: str | None
    semantic_identity: SemanticIdentity
    period_or_horizon_id: str | None
    publication_date: str | None
    event_date: str | None
    ui_as_of_date: str
    knowledge_dates: tuple[str, ...]
    source_document_ids: tuple[str, ...]
    source_occurrence_ids: tuple[str, ...]
    selection_or_calculation_method_id: str
    review_issue_ids: tuple[str, ...]
    parity_exception_ids: tuple[str, ...]
    lineage_state: str
    lineage_digest: str

    def __post_init__(self) -> None:
        _require_id(self.field_id, label="field_id")
        if self.lineage_state not in {"accepted", "diagnostic", "needs-review", "missing", "blocked"}:
            raise PromiseProgressProjectionError(f"Unknown field lineage state {self.lineage_state!r}.")
        unknown = set(self.parity_exception_ids) - CLOSED_PARITY_EXCEPTION_IDS
        if unknown:
            raise PromiseProgressProjectionError(f"Unregistered parity exceptions: {sorted(unknown)!r}.")
        cutoff = _parse_date(self.ui_as_of_date, label="field ui_as_of_date")
        if any(_parse_date(value, label="knowledge_date") > cutoff for value in self.knowledge_dates):
            raise PromiseProgressProjectionError("A field contains knowledge learned after its UI as-of date.")

    def to_shadow_dict(self) -> dict[str, Any]:
        return {
            "product_id": self.product_id,
            "block_id": self.block_id,
            "row_id": self.row_id,
            "field_id": self.field_id,
            "field_role": self.field_role,
            "destination": {"sheet": SHEET_NAME, "anchor_cell": self.anchor_cell, "display_range": self.display_range},
            "display_value": self.display_value.to_dict(),
            "canonical_record_ids": list(self.canonical_record_ids),
            "target_version_id": self.target_version_id,
            "actual_selection_id": self.actual_selection_id,
            "actual_observation_id": self.actual_observation_id,
            "progress_selection_id": self.progress_selection_id,
            "progress_observation_ids": list(self.progress_observation_ids),
            "status_assessment_id": self.status_assessment_id,
            "semantic_identity": self.semantic_identity.to_dict(),
            "period_or_horizon_id": self.period_or_horizon_id,
            "publication_date": self.publication_date,
            "event_date": self.event_date,
            "ui_as_of_date": self.ui_as_of_date,
            "knowledge_dates": list(self.knowledge_dates),
            "source_document_ids": list(self.source_document_ids),
            "source_occurrence_ids": list(self.source_occurrence_ids),
            "selection_or_calculation_method_id": self.selection_or_calculation_method_id,
            "review_issue_ids": list(self.review_issue_ids),
            "parity_exception_ids": list(self.parity_exception_ids),
            "lineage_state": self.lineage_state,
            "lineage_digest": self.lineage_digest,
        }


@dataclass(frozen=True)
class ProductRow:
    product_id: str
    block_id: str
    row_id: str
    row_variant: str
    business_order: int
    visible_sheet_row: int
    ui_as_of_date: str
    canonical_series_or_program_id: str | None
    canonical_version_id: str | None
    fields: tuple[ProductField, ...]
    review_issue_ids: tuple[str, ...]
    parity_exception_ids: tuple[str, ...]
    lineage_digest: str

    def __post_init__(self) -> None:
        _require_id(self.row_id, label="row_id")
        expected_roles = tuple(role for role, _, _ in BLOCK_FIELD_LAYOUT[self.block_id])
        if tuple(field.field_role for field in self.fields) != expected_roles:
            raise PromiseProgressProjectionError(f"Row {self.row_id!r} does not contain the closed field layout for its block.")
        if any(field.row_id != self.row_id or field.block_id != self.block_id for field in self.fields):
            raise PromiseProgressProjectionError("Product field ownership differs from its containing row.")
        unknown = set(self.parity_exception_ids) - CLOSED_PARITY_EXCEPTION_IDS
        if unknown:
            raise PromiseProgressProjectionError(f"Unregistered row parity exceptions: {sorted(unknown)!r}.")

    def to_shadow_dict(self) -> dict[str, Any]:
        return {
            "product_id": self.product_id,
            "block_id": self.block_id,
            "row_id": self.row_id,
            "row_variant": self.row_variant,
            "business_order": self.business_order,
            "visible_sheet_row": self.visible_sheet_row,
            "ui_as_of_date": self.ui_as_of_date,
            "canonical_series_or_program_id": self.canonical_series_or_program_id,
            "canonical_version_id": self.canonical_version_id,
            "field_ids": [field.field_id for field in self.fields],
            "review_issue_ids": list(self.review_issue_ids),
            "parity_exception_ids": list(self.parity_exception_ids),
            "lineage_digest": self.lineage_digest,
        }


@dataclass(frozen=True)
class ManagementCredibilityScorecardBlock:
    rows: tuple[ProductRow, ...]
    block_id: str = field(default=SCORECARD_BLOCK_ID, init=False)


@dataclass(frozen=True)
class AnnualGuidanceProgressionBlock:
    rows: tuple[ProductRow, ...]
    block_id: str = field(default=ANNUAL_BLOCK_ID, init=False)


@dataclass(frozen=True)
class OpenGuidanceBlock:
    rows: tuple[ProductRow, ...]
    block_id: str = field(default=OPEN_BLOCK_ID, init=False)


@dataclass(frozen=True)
class QuarterlyRevisionTimelineBlock:
    rows: tuple[ProductRow, ...]
    block_id: str = field(default=TIMELINE_BLOCK_ID, init=False)


ProductBlock = ManagementCredibilityScorecardBlock | AnnualGuidanceProgressionBlock | OpenGuidanceBlock | QuarterlyRevisionTimelineBlock


@dataclass(frozen=True)
class PromiseProgressProduct:
    product_id: str
    company_id: str
    ui_as_of_date: str
    knowledge_cutoff: str
    source_package_id: str
    source_package_sha256: str
    template_oracle_sha256: str
    blocks: tuple[ProductBlock, ...]
    actuals: tuple[ActualSelection, ...]
    progress_values: tuple[ProgressSelection, ...]
    status_assessments: tuple[StatusAssessment, ...]
    structural_parity_exception_ids: tuple[str, ...]
    applied_parity_exception_ids: tuple[str, ...]
    legacy_parity_oracle: Mapping[str, Any] | None
    source_reference_catalog: Mapping[str, tuple[str, ...]]
    validation_results: tuple[Mapping[str, Any], ...]
    product_contract_id: str = field(default=PRODUCT_CONTRACT_ID, init=False)
    product_type: str = field(default=PRODUCT_TYPE, init=False)

    def __post_init__(self) -> None:
        _require_id(self.product_id, label="product_id")
        _require_sha(self.source_package_sha256, label="source_package_sha256")
        _require_sha(self.template_oracle_sha256, label="template_oracle_sha256")
        _parse_date(self.ui_as_of_date, label="ui_as_of_date")
        _parse_date(self.knowledge_cutoff, label="knowledge_cutoff")
        if tuple(block.block_id for block in self.blocks) != BLOCK_ORDER:
            raise PromiseProgressProjectionError("PromiseProgressProduct must own the four blocks in locked product order.")
        rows = self.ordered_rows
        if len({row.row_id for row in rows}) != len(rows):
            raise PromiseProgressProjectionError("Product row identities must be unique.")
        fields = self.fields
        if len({field.field_id for field in fields}) != len(fields):
            raise PromiseProgressProjectionError("Product field identities must be unique.")
        if any(field.product_id != self.product_id for field in fields):
            raise PromiseProgressProjectionError("Every field must belong to the immutable product.")
        unknown = set(self.applied_parity_exception_ids) - CLOSED_PARITY_EXCEPTION_IDS
        if unknown:
            raise PromiseProgressProjectionError(f"Product applies unregistered parity exceptions: {sorted(unknown)!r}.")
        if set(self.structural_parity_exception_ids) - _STRUCTURAL_PARITY_EXCEPTION_IDS:
            raise PromiseProgressProjectionError("Product structural parity exceptions differ from the closed structural register.")
        if self.legacy_parity_oracle is not None:
            object.__setattr__(self, "legacy_parity_oracle", _freeze(self.legacy_parity_oracle))
        object.__setattr__(self, "source_reference_catalog", _freeze(self.source_reference_catalog))

    @property
    def ordered_rows(self) -> tuple[ProductRow, ...]:
        return tuple(row for block in self.blocks for row in block.rows)

    @property
    def fields(self) -> tuple[ProductField, ...]:
        return tuple(field for row in self.ordered_rows for field in row.fields)

    def shadow_matrix(self) -> dict[str, Any]:
        rows = [row.to_shadow_dict() for row in self.ordered_rows]
        fields = [field.to_shadow_dict() for field in self.fields]
        references = {
            **{
                key: list(values)
                for key, values in sorted(self.source_reference_catalog.items())
            },
            "actual_selection_ids": [value.actual_id for value in self.actuals],
            "progress_selection_ids": [value.progress_id for value in self.progress_values],
            "status_assessment_ids": [value.status_assessment_id for value in self.status_assessments],
            "parity_exception_ids": list(self.applied_parity_exception_ids),
        }
        payload = {
            "schema_id": SHADOW_SCHEMA_ID,
            "product_id": self.product_id,
            "company_id": self.company_id,
            "template_oracle_sha256": self.template_oracle_sha256,
            "sheet_name": SHEET_NAME,
            "ui_as_of_date": self.ui_as_of_date,
            "knowledge_cutoff": self.knowledge_cutoff,
            "reference_catalog": references,
            "rows": rows,
            "fields": fields,
        }
        payload["lineage_digest"] = _digest(payload)
        return payload

    def parity_report(self) -> dict[str, Any]:
        comparison = _compare_legacy_and_source_native_fields(self)
        usage: list[dict[str, Any]] = []
        for exception_id in self.applied_parity_exception_ids:
            compared_field_ids = {
                row["source_native_field_id"]
                for row in comparison["field_comparisons"]
                if row["exception_id"] == exception_id and row["source_native_field_id"] is not None
            }
            fields = tuple(
                field
                for field in self.fields
                if exception_id in field.parity_exception_ids or field.field_id in compared_field_ids
            )
            if exception_id == FUZZY_TRACE_EXCEPTION_ID:
                affected_rows = tuple(row.row_id for row in self.ordered_rows)
                affected_blocks = BLOCK_ORDER
            elif exception_id in {EMPTY_TRACKER_EXCEPTION_ID, LOSSY_MATRIX_EXCEPTION_ID}:
                affected_rows = ()
                affected_blocks = BLOCK_ORDER
            else:
                affected_rows = _sorted_unique(field.row_id for field in fields)
                affected_blocks = _sorted_unique(field.block_id for field in fields)
            usage.append(
                {
                    "exception_id": exception_id,
                    "affected_product_ids": [self.product_id],
                    "affected_block_ids": list(affected_blocks),
                    "affected_row_ids": list(affected_rows),
                    "affected_field_ids": [field.field_id for field in fields],
                    **dict(PARITY_EXCEPTION_REGISTRY[exception_id]),
                }
            )
        report = {
            "register_id": "register:promise-progress:parity-exceptions@1",
            "comparison_scope": comparison["comparison_scope"],
            "comparison_counts": comparison["comparison_counts"],
            "field_comparisons": comparison["field_comparisons"],
            "unregistered_difference_count": comparison["unregistered_difference_count"],
            "unused_accepted_difference_bindings": comparison["unused_accepted_difference_bindings"],
            "unused_registered_exception_ids": comparison["unused_registered_exception_ids"],
            "applied": usage,
            "lineage_digest": _digest({"comparison": comparison, "applied": usage}),
        }
        for key in (
            "row_disposition_counts",
            "row_dispositions",
            "structural_counts",
            "structural_bindings",
            "completeness",
        ):
            if key in comparison:
                report[key] = comparison[key]
        return report

    def to_dict(self) -> dict[str, Any]:
        return {
            "product_contract_id": self.product_contract_id,
            "product_type": self.product_type,
            "product_id": self.product_id,
            "company_id": self.company_id,
            "ui_as_of_date": self.ui_as_of_date,
            "knowledge_cutoff": self.knowledge_cutoff,
            "source_longitudinal_memory": {
                "package_id": self.source_package_id,
                "package_sha256": self.source_package_sha256,
            },
            "template": {
                "sheet_name": SHEET_NAME,
                "oracle_sha256": self.template_oracle_sha256,
                "visible_columns": "A:L",
                "hidden_columns": {"M": "reserved-blank", "N": "reserved-blank", "O": "row_id"},
            },
            "blocks": [
                {"block_id": block.block_id, "row_ids": [row.row_id for row in block.rows]}
                for block in self.blocks
            ],
            "actuals": [value.to_dict() for value in self.actuals],
            "progress_values": [value.to_dict() for value in self.progress_values],
            "status_assessments": [value.to_dict() for value in self.status_assessments],
            "shadow_matrix": self.shadow_matrix(),
            "validation_report": {"issue_count": len(self.validation_results), "issues": [_canonical(row) for row in self.validation_results]},
            "parity_report": self.parity_report(),
            "deterministic_serialization": {"canonical_json": "utf-8-lf-sort-keys@1", "generated_timestamp": None},
        }


def serialize_promise_progress_product(product: PromiseProgressProduct) -> bytes:
    """Serialize one immutable product without a generated timestamp."""

    return _canonical_bytes(product.to_dict())


def serialize_shadow_matrix(product: PromiseProgressProduct) -> bytes:
    """Serialize the field-level shadow matrix from the same product owner."""

    return _canonical_bytes(product.shadow_matrix())


def _normalized_parity_text(value: Any) -> str:
    text = "" if value is None else str(value)
    replacements = {
        "\u00a0": " ",
        "\u2013": "-",
        "\u2014": "-",
        "â€“": "-",
        "â€”": "-",
        "â‰¥": ">=",
        "â‰¤": "<=",
        "\u2265": ">=",
        "\u2264": "<=",
        "Â·": "·",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return " ".join(text.split())


def _parity_difference_digest(
    *,
    product_id: str,
    block_id: str,
    reviewed_legacy_business_key: str,
    source_native_row_id: str,
    row_type: str,
    business_key_class: str,
    field_role: str,
    legacy_destination_cell: str,
    source_native_destination_cell: str,
    underlying_difference_class: str,
    legacy_display_value: Any,
    source_native_display_value: Any,
) -> str:
    return _digest(
        {
            "product_id": product_id,
            "block_id": block_id,
            "reviewed_legacy_business_key": reviewed_legacy_business_key,
            "source_native_row_id": source_native_row_id,
            "row_type": row_type,
            "business_key_class": business_key_class,
            "field_role": field_role,
            "legacy_destination_cell": legacy_destination_cell,
            "source_native_destination_cell": source_native_destination_cell,
            "underlying_difference_class": underlying_difference_class,
            "legacy_display_value": legacy_display_value,
            "source_native_display_value": source_native_display_value,
        }
    )


def _parity_row_semantic_identity(row: ProductRow) -> dict[str, Any]:
    identities: dict[str, dict[str, Any]] = {}
    for field_value in row.fields:
        identity = field_value.semantic_identity.to_dict()
        identities[json.dumps(_canonical(identity), sort_keys=True, separators=(",", ":"))] = identity
    return {
        "semantic_identities": [identities[key] for key in sorted(identities)],
        "period_or_horizon_ids": list(
            _sorted_unique(
                field_value.period_or_horizon_id
                for field_value in row.fields
                if field_value.period_or_horizon_id is not None
            )
        ),
    }


def _business_key_class_for_source_row(row: ProductRow) -> str:
    by_variant = {
        "scorecard_assessment": "business-key-class:promise-progress:scorecard-assessment@1",
        "annual_guidance_series": "business-key-class:promise-progress:annual-guidance-series@1",
        "diagnostic_coverage_gap": "business-key-class:promise-progress:annual-coverage-gap@1",
        "open_guidance": "business-key-class:promise-progress:open-guidance-series@1",
        "guidance_version": "business-key-class:promise-progress:timeline-guidance-version@1",
        "promise_version": "business-key-class:promise-progress:timeline-promise-version@1",
    }
    try:
        return by_variant[row.row_variant]
    except KeyError as exc:
        raise PromiseProgressProjectionError(
            f"Product row variant {row.row_variant!r} has no closed parity business-key class."
        ) from exc


def _derive_parity_difference_reason(
    *,
    mapping_kind: str,
    source_row: ProductRow,
    source_field: ProductField,
) -> str | None:
    """Classify one observed difference without consulting its reviewed binding."""

    if mapping_kind == "reviewed-destination-remap":
        return "reviewed_row_destination_remap"
    if source_row.block_id == SCORECARD_BLOCK_ID and source_field.field_role in {
        "score",
        "evidence",
        "read",
    }:
        return "legacy_unreviewed_scorecard_value"
    if TEMPORAL_EXCEPTION_ID in source_field.parity_exception_ids:
        return "legacy_temporal_leakage_removed"
    if ACTUAL_COVERAGE_EXCEPTION_ID in source_field.parity_exception_ids:
        return "accepted_annual_actual_unavailable"
    if (
        source_field.field_role == "status"
        and STATIC_STATUS_EXCEPTION_ID in source_field.parity_exception_ids
    ):
        return "legacy_static_status_replaced_by_replay"
    if (
        source_field.field_role in {"notes_source", "source_note"}
        and GENERIC_SOURCE_EXCEPTION_ID in source_field.parity_exception_ids
    ):
        return "legacy_generic_source_note_replaced_by_lineage"
    if source_field.field_role in {
        "initial_guide",
        "q1_guide",
        "q2_guide",
        "q3_guide",
        "q4_guide",
        "current_guide",
        "previous_guide",
        "actual",
        "progress",
    }:
        return "equivalent_value_display_normalized"
    if source_field.field_role in {
        "metric",
        "category",
        "change_type",
        "horizon",
        "stated_in",
        "source_date",
    }:
        return "equivalent_label_display_normalized"
    return None


def _exception_authorization_digest(
    *,
    binding: Mapping[str, Any],
    policy: Mapping[str, Any],
    independently_derived_reason: str,
) -> str:
    reviewed_scope = {
        key: value
        for key, value in binding.items()
        if key != "exception_authorization_digest"
    }
    return _digest(
        {
            "exception_identity": {
                "exception_id": binding["exception_id"],
                "exception_policy_version": binding["exception_policy_version"],
            },
            "resolved_exception_policy": _canonical(policy),
            "independently_derived_difference_reason": independently_derived_reason,
            "reviewed_binding_scope": reviewed_scope,
        }
    )


def _validate_exception_policy(policy: Mapping[str, Any]) -> dict[str, Any]:
    required = {
        "policy_id",
        "authorization_kind",
        "exception_id",
        "exception_policy_version",
        "state",
        "allowed_difference_reason_codes",
        "allowed_product_ids",
        "allowed_block_ids",
        "allowed_row_types",
        "allowed_business_key_classes",
        "allowed_reviewed_business_keys",
        "allowed_legacy_row_ids",
        "allowed_source_native_row_ids",
        "allowed_disposition_kinds",
        "allowed_field_roles",
        "allowed_difference_classes",
        "allowed_destination_pairs",
        "allowed_structural_binding_ids",
        "allowed_structural_condition_types",
        "allowed_sheet_names",
        "duration",
        "acceptance_owner",
        "source_native_rationale",
        "legacy_behavior_category",
    }
    if not isinstance(policy, Mapping) or set(policy) != required:
        raise PromiseProgressProjectionError("Parity exception policy differs from the closed authorization contract.")
    canonical = {key: _canonical(value) for key, value in policy.items()}
    _require_id(str(canonical["policy_id"]), label="parity exception policy_id")
    _require_id(str(canonical["exception_id"]), label="parity exception_id")
    _require_id(str(canonical["exception_policy_version"]), label="parity exception policy version")
    if canonical["exception_id"] not in CLOSED_PARITY_EXCEPTION_IDS:
        raise PromiseProgressProjectionError("Parity exception policy uses an unknown exception identity.")
    if canonical["authorization_kind"] not in {
        "field-difference",
        "row-disposition",
        "structural-product-difference",
        "deny-all",
    }:
        raise PromiseProgressProjectionError("Parity exception policy has an unsupported authorization kind.")
    if canonical["state"] not in {"active", "inactive"}:
        raise PromiseProgressProjectionError("Parity exception policy has an unsupported state.")
    list_keys = {
        "allowed_difference_reason_codes",
        "allowed_product_ids",
        "allowed_block_ids",
        "allowed_row_types",
        "allowed_business_key_classes",
        "allowed_reviewed_business_keys",
        "allowed_legacy_row_ids",
        "allowed_source_native_row_ids",
        "allowed_disposition_kinds",
        "allowed_field_roles",
        "allowed_difference_classes",
        "allowed_destination_pairs",
        "allowed_structural_binding_ids",
        "allowed_structural_condition_types",
        "allowed_sheet_names",
    }
    for key in list_keys:
        values = canonical[key]
        if not isinstance(values, list) or len(values) != len({json.dumps(value, sort_keys=True) for value in values}):
            raise PromiseProgressProjectionError(f"Parity exception policy {key!r} must be a unique closed list.")
        if any(value == "*" or (isinstance(value, str) and "*" in value) for value in values):
            raise PromiseProgressProjectionError("Wildcard parity authorization is forbidden.")
    if set(canonical["allowed_difference_reason_codes"]) - PARITY_DIFFERENCE_REASON_CODES:
        raise PromiseProgressProjectionError("Parity exception policy uses an unknown semantic reason code.")
    if set(canonical["allowed_disposition_kinds"]) - PARITY_ROW_DISPOSITION_KINDS:
        raise PromiseProgressProjectionError("Parity exception policy uses an unknown row-disposition kind.")
    if set(canonical["allowed_structural_condition_types"]) - PARITY_STRUCTURAL_CONDITION_TYPES:
        raise PromiseProgressProjectionError("Parity exception policy uses an unknown structural condition type.")
    if set(canonical["allowed_sheet_names"]) - {SHEET_NAME}:
        raise PromiseProgressProjectionError("Parity exception policy uses an unsupported sheet scope.")
    if set(canonical["allowed_product_ids"]) == set() or set(canonical["allowed_block_ids"]) - set(BLOCK_ORDER):
        raise PromiseProgressProjectionError("Parity exception policy product/block authorization is not closed.")
    if set(canonical["allowed_difference_classes"]) - (
        PARITY_OBSERVED_DIFFERENCE_CLASSES | {"structural-product-difference"}
    ):
        raise PromiseProgressProjectionError("Parity exception policy uses an unsupported difference class.")
    destination_pairs: list[dict[str, str]] = []
    for pair in canonical["allowed_destination_pairs"]:
        if not isinstance(pair, Mapping) or set(pair) != {
            "legacy_destination_cell",
            "source_native_destination_cell",
        }:
            raise PromiseProgressProjectionError("Parity exception destination pair is not closed.")
        legacy_destination = str(pair["legacy_destination_cell"])
        source_destination = str(pair["source_native_destination_cell"])
        for destination in (legacy_destination, source_destination):
            if not re.fullmatch(r"[A-L](?:[1-9]|[1-9][0-9]|10[0-2])", destination):
                raise PromiseProgressProjectionError("Parity exception policy has an invalid destination.")
        destination_pairs.append(
            {
                "legacy_destination_cell": legacy_destination,
                "source_native_destination_cell": source_destination,
            }
        )
    canonical["allowed_destination_pairs"] = destination_pairs
    if canonical["authorization_kind"] == "field-difference":
        required_nonempty = {
            "allowed_difference_reason_codes",
            "allowed_product_ids",
            "allowed_block_ids",
            "allowed_row_types",
            "allowed_business_key_classes",
            "allowed_reviewed_business_keys",
            "allowed_source_native_row_ids",
            "allowed_field_roles",
            "allowed_difference_classes",
            "allowed_destination_pairs",
        }
        if any(not canonical[key] for key in required_nonempty):
            raise PromiseProgressProjectionError("Field-difference parity policy contains an unrestricted empty scope.")
        if any(
            canonical[key]
            for key in (
                "allowed_legacy_row_ids",
                "allowed_disposition_kinds",
                "allowed_structural_binding_ids",
                "allowed_structural_condition_types",
                "allowed_sheet_names",
            )
        ):
            raise PromiseProgressProjectionError("Field-difference parity policy may not authorize row or structural scope.")
    elif canonical["authorization_kind"] == "row-disposition":
        required_nonempty = {
            "allowed_difference_reason_codes",
            "allowed_product_ids",
            "allowed_block_ids",
            "allowed_row_types",
            "allowed_business_key_classes",
            "allowed_disposition_kinds",
            "allowed_sheet_names",
        }
        if any(not canonical[key] for key in required_nonempty) or not (
            canonical["allowed_legacy_row_ids"] or canonical["allowed_source_native_row_ids"]
        ):
            raise PromiseProgressProjectionError("Row-disposition policy contains an unrestricted empty scope.")
        if any(
            canonical[key]
            for key in (
                "allowed_field_roles",
                "allowed_difference_classes",
                "allowed_destination_pairs",
                "allowed_structural_binding_ids",
                "allowed_structural_condition_types",
            )
        ):
            raise PromiseProgressProjectionError("Row-disposition policy may not authorize field or structural scope.")
    elif canonical["authorization_kind"] == "structural-product-difference":
        if canonical["allowed_difference_classes"] != ["structural-product-difference"]:
            raise PromiseProgressProjectionError("Structural parity policy must authorize only its structural class.")
        if any(
            canonical[key]
            for key in (
                "allowed_row_types",
                "allowed_business_key_classes",
                "allowed_reviewed_business_keys",
                "allowed_legacy_row_ids",
                "allowed_source_native_row_ids",
                "allowed_disposition_kinds",
                "allowed_field_roles",
                "allowed_destination_pairs",
            )
        ):
            raise PromiseProgressProjectionError("Structural parity policy may not authorize field scope.")
        if any(
            not canonical[key]
            for key in (
                "allowed_difference_reason_codes",
                "allowed_product_ids",
                "allowed_block_ids",
                "allowed_structural_binding_ids",
                "allowed_structural_condition_types",
                "allowed_sheet_names",
            )
        ):
            raise PromiseProgressProjectionError("Structural parity policy contains an unrestricted empty scope.")
    else:
        if canonical["state"] != "inactive" or canonical["allowed_destination_pairs"]:
            raise PromiseProgressProjectionError("Deny-all parity policy must be inactive and destination-free.")
    return canonical


def _policy_authorizes_observed_difference(
    policy: Mapping[str, Any],
    *,
    binding: Mapping[str, Any],
    independently_derived_reason: str,
) -> bool:
    pair = {
        "legacy_destination_cell": binding["legacy_destination_cell"],
        "source_native_destination_cell": binding["source_native_destination_cell"],
    }
    return bool(
        policy["authorization_kind"] == "field-difference"
        and policy["state"] == "active"
        and binding["exception_id"] == policy["exception_id"]
        and binding["exception_policy_version"] == policy["exception_policy_version"]
        and independently_derived_reason in policy["allowed_difference_reason_codes"]
        and binding["product_id"] in policy["allowed_product_ids"]
        and binding["block_id"] in policy["allowed_block_ids"]
        and binding["row_type"] in policy["allowed_row_types"]
        and binding["business_key_class"] in policy["allowed_business_key_classes"]
        and binding["reviewed_legacy_business_key"] in policy["allowed_reviewed_business_keys"]
        and binding["source_native_row_id"] in policy["allowed_source_native_row_ids"]
        and binding["field_role"] in policy["allowed_field_roles"]
        and binding["difference_class"] in policy["allowed_difference_classes"]
        and pair in policy["allowed_destination_pairs"]
    )


def _source_row_business_key(row: ProductRow) -> str:
    return str(row.canonical_version_id or row.canonical_series_or_program_id or row.row_id)


def _source_row_field_inventory(row: ProductRow) -> dict[str, Any]:
    fields = [
        {
            "field_id": field_value.field_id,
            "field_role": field_value.field_role,
            "destination_cell": field_value.anchor_cell,
        }
        for field_value in row.fields
    ]
    payload = {
        "source_native_row_id": row.row_id,
        "block_id": row.block_id,
        "row_type": row.row_variant,
        "fields": fields,
    }
    return {**payload, "field_inventory_digest": _digest(payload)}


def _legacy_row_field_inventory(row: Mapping[str, Any]) -> dict[str, Any]:
    fields = [
        {
            "field_role": field_role,
            "destination_cell": row["fields_by_role"][field_role]["destination_cell"],
            "semantic_classification": row["fields_by_role"][field_role]["semantic_classification"],
            "structural_classification": row["fields_by_role"][field_role]["structural_classification"],
        }
        for field_role, _, _ in BLOCK_FIELD_LAYOUT[row["block_id"]]
    ]
    payload = {
        "legacy_row_id": row["legacy_row_id"],
        "block_id": row["block_id"],
        "visible_sheet_row": row["visible_sheet_row"],
        "fields": fields,
    }
    return {**payload, "field_inventory_digest": _digest(payload)}


def _source_row_counterpart_signature(row: ProductRow) -> dict[str, Any]:
    return {
        "block_id": row.block_id,
        "source_row_type": row.row_variant,
        "business_key_class": _business_key_class_for_source_row(row),
        "source_native_business_key": _source_row_business_key(row),
        "canonical_series_or_program_id": row.canonical_series_or_program_id,
        "canonical_version_id": row.canonical_version_id,
        "reviewed_semantic_identity": _parity_row_semantic_identity(row),
        "field_roles": [field_value.field_role for field_value in row.fields],
    }


def _derive_row_disposition_reason(
    *,
    disposition_kind: str,
    legacy_row: Mapping[str, Any] | None,
    source_row: ProductRow | None,
    counterpart_row_ids: Sequence[str],
) -> str | None:
    """Classify row ownership without consulting the reviewed disposition reason."""

    if disposition_kind == "paired":
        if legacy_row is not None and source_row is not None and tuple(counterpart_row_ids) == (
            source_row.row_id,
        ):
            return "reviewed_semantic_row_pair"
        return None
    if disposition_kind == "legacy_only" and legacy_row is not None and source_row is None:
        if counterpart_row_ids:
            return None
        if legacy_row["row_type"] == "legacy-row-type:timeline-terminal-summary@1":
            return "legacy_terminal_summary_replaced_by_typed_history"
        if legacy_row["row_type"] in {
            "legacy-row-type:annual-capital-structure@1",
            "legacy-row-type:timeline-unsupported@1",
        }:
            return "legacy_unsupported_by_accepted_product"
        return None
    if disposition_kind == "source_native_only" and source_row is not None and legacy_row is None:
        if counterpart_row_ids:
            return None
        if source_row.block_id == ANNUAL_BLOCK_ID and source_row.row_variant in {
            "annual_guidance_series",
            "diagnostic_coverage_gap",
        }:
            return "source_native_canonical_row_legacy_omitted"
        if source_row.block_id == TIMELINE_BLOCK_ID and source_row.row_variant in {
            "guidance_version",
            "promise_version",
        }:
            return "source_native_typed_history_legacy_omitted"
    return None


def _row_disposition_authorization_digest(
    *,
    disposition: Mapping[str, Any],
    policy: Mapping[str, Any],
    independently_derived_reason: str,
    counterpart_row_ids: Sequence[str],
    legacy_capture_manifest_sha256: str,
    source_scope_manifest_sha256: str,
) -> str:
    reviewed_scope = {
        key: value for key, value in disposition.items() if key != "authorization_digest"
    }
    return _digest(
        {
            "disposition_identity": {
                "disposition_id": disposition["disposition_id"],
                "disposition_version": disposition["disposition_version"],
            },
            "resolved_disposition_policy": _canonical(policy),
            "independently_derived_reason": independently_derived_reason,
            "independently_replayed_counterpart_row_ids": list(counterpart_row_ids),
            "reviewed_parity_scope": {
                "legacy_capture_manifest_sha256": legacy_capture_manifest_sha256,
                "source_scope_manifest_sha256": source_scope_manifest_sha256,
            },
            "reviewed_disposition_scope": reviewed_scope,
        }
    )


def _policy_authorizes_row_disposition(
    policy: Mapping[str, Any],
    *,
    disposition: Mapping[str, Any],
    independently_derived_reason: str,
) -> bool:
    legacy_row_id = disposition["legacy_row_id"]
    source_row_id = disposition["source_native_row_id"]
    legacy_business_key = disposition["legacy_business_key"]
    return bool(
        policy["authorization_kind"] == "row-disposition"
        and policy["state"] == "active"
        and disposition["policy_id"] == policy["policy_id"]
        and disposition["policy_version"] == policy["exception_policy_version"]
        and independently_derived_reason in policy["allowed_difference_reason_codes"]
        and disposition["product_id"] in policy["allowed_product_ids"]
        and disposition["block_id"] in policy["allowed_block_ids"]
        and disposition["row_type"] in policy["allowed_row_types"]
        and disposition["business_key_class"] in policy["allowed_business_key_classes"]
        and disposition["disposition_kind"] in policy["allowed_disposition_kinds"]
        and SHEET_NAME in policy["allowed_sheet_names"]
        and (
            legacy_row_id is None
            or legacy_row_id in policy["allowed_legacy_row_ids"]
        )
        and (
            source_row_id is None
            or source_row_id in policy["allowed_source_native_row_ids"]
        )
        and (
            legacy_business_key is None
            or legacy_business_key in policy["allowed_reviewed_business_keys"]
        )
    )


def _structural_observation_reason(condition_type: str) -> str:
    reasons = {
        "empty-tracker-parallel-ownership": "legacy_parallel_tracker_ownership_removed",
        "fuzzy-hidden-trace-identity": "legacy_fuzzy_trace_key_replaced",
        "lossy-support-matrix-ownership": "legacy_lossy_support_matrix_replaced",
    }
    try:
        return reasons[condition_type]
    except KeyError as exc:
        raise PromiseProgressProjectionError("Structural observation uses an unknown condition type.") from exc


def _structural_authorization_digest(
    *,
    binding: Mapping[str, Any],
    observation: Mapping[str, Any],
    policy: Mapping[str, Any],
    independently_derived_reason: str,
) -> str:
    return _digest(
        {
            "binding_scope": {
                key: value for key, value in binding.items() if key != "structural_authorization_digest"
            },
            "resolved_observation": _canonical(observation),
            "resolved_structural_policy": _canonical(policy),
            "independently_derived_reason": independently_derived_reason,
        }
    )


def _compare_legacy_and_source_native_fields(product: PromiseProgressProduct) -> dict[str, Any]:
    oracle = product.legacy_parity_oracle
    if oracle is None:
        scope = {
            "state": "not-declared",
            "reason": "No frozen full-field legacy oracle is declared for this product.",
            "product_id": product.product_id,
        }
        return {
            "comparison_scope": scope,
            "comparison_counts": {},
            "field_comparisons": [],
            "unregistered_difference_count": None,
            "unused_accepted_difference_bindings": [],
            "unused_registered_exception_ids": list(product.applied_parity_exception_ids),
        }

    required_oracle_keys = {
        "comparison_scope_id",
        "comparison_scope_state",
        "product_id",
        "workbook_oracle_id",
        "workbook_oracle_sha256",
        "sheet_name",
        "block_contracts",
        "rows",
        "capture_manifest",
        "source_scope_manifest",
        "row_dispositions",
        "structural_observations",
        "structural_bindings",
        "exception_policies",
        "accepted_differences",
        "independent_expected_digests",
    }
    if set(oracle) != required_oracle_keys or oracle.get("comparison_scope_state") != "declared":
        raise PromiseProgressProjectionError("Legacy parity oracle differs from the closed declared-scope contract.")
    if oracle.get("product_id") != product.product_id:
        raise PromiseProgressProjectionError("Legacy parity oracle is scoped to a different product.")
    _require_id(str(oracle["comparison_scope_id"]), label="parity comparison_scope_id")
    _require_id(str(oracle["workbook_oracle_id"]), label="parity workbook_oracle_id")
    _require_sha(str(oracle["workbook_oracle_sha256"]), label="parity workbook oracle SHA")
    if oracle["sheet_name"] != SHEET_NAME:
        raise PromiseProgressProjectionError("Legacy parity oracle targets an unsupported sheet.")
    independent_expected_digests = oracle["independent_expected_digests"]
    expected_digest_keys = {
        "capture_manifest_sha256",
        "source_scope_manifest_sha256",
        "row_disposition_graph_sha256",
        "structural_observation_set_sha256",
        "structural_binding_set_sha256",
    }
    if not isinstance(independent_expected_digests, Mapping) or set(independent_expected_digests) != expected_digest_keys:
        raise PromiseProgressProjectionError("Legacy parity independent expected digests differ from the closed contract.")
    for key in expected_digest_keys:
        _require_sha(str(independent_expected_digests[key]), label=f"legacy parity {key}")

    contracts: dict[str, tuple[tuple[str, str], ...]] = {}
    for contract in oracle["block_contracts"]:
        if not isinstance(contract, Mapping) or set(contract) != {
            "block_id",
            "field_roles",
            "anchor_columns",
        }:
            raise PromiseProgressProjectionError("Legacy parity block contract is not closed.")
        block_id = str(contract["block_id"])
        expected = BLOCK_FIELD_LAYOUT.get(block_id)
        roles = tuple(str(value) for value in contract["field_roles"])
        columns = tuple(str(value) for value in contract["anchor_columns"])
        if expected is None or roles != tuple(item[0] for item in expected) or columns != tuple(item[1] for item in expected):
            raise PromiseProgressProjectionError("Legacy parity block contract differs from the product field registry.")
        if block_id in contracts:
            raise PromiseProgressProjectionError("Legacy parity block contracts contain a duplicate block.")
        contracts[block_id] = tuple(zip(roles, columns, strict=True))
    if set(contracts) != set(BLOCK_ORDER):
        raise PromiseProgressProjectionError("Legacy parity oracle must declare all four product blocks.")

    rows_by_id = {row.row_id: row for row in product.ordered_rows}
    source_rows_by_destination = {
        field_value.anchor_cell: rows_by_id[field_value.row_id]
        for field_value in product.fields
    }
    if len(source_rows_by_destination) != len(product.fields):
        raise PromiseProgressProjectionError("Source-native projection repeats a parity destination.")

    legacy_rows: list[dict[str, Any]] = []
    reviewed_business_keys: set[str] = set()
    for row in sorted(
        oracle["rows"],
        key=lambda item: (BLOCK_ORDER.index(str(item["block_id"])), int(item["visible_sheet_row"])),
    ):
        required_row_keys = {
            "legacy_row_id",
            "block_id",
            "visible_sheet_row",
            "reviewed_legacy_business_key",
            "row_type",
            "business_key_class",
            "reviewed_semantic_identity",
            "counterpart_signature",
            "display_values",
            "semantic_classifications",
            "structural_classification",
        }
        if not isinstance(row, Mapping) or set(row) != required_row_keys:
            raise PromiseProgressProjectionError("Legacy parity row differs from the frozen matrix contract.")
        block_id = str(row["block_id"])
        row_number = int(row["visible_sheet_row"])
        legacy_row_id = _require_id(str(row["legacy_row_id"]), label="legacy parity row_id")
        business_key = _require_id(str(row["reviewed_legacy_business_key"]), label="reviewed legacy business key")
        if business_key in reviewed_business_keys:
            raise PromiseProgressProjectionError("Legacy parity matrix repeats a reviewed business key.")
        reviewed_business_keys.add(business_key)
        row_type = _require_id(str(row["row_type"]), label="legacy parity row_type")
        business_key_class = _require_id(str(row["business_key_class"]), label="legacy parity business-key class")
        role_columns = contracts.get(block_id)
        values = tuple(row["display_values"])
        classifications = tuple(str(value) for value in row["semantic_classifications"])
        if role_columns is None or len(values) != len(role_columns) or len(classifications) != len(role_columns):
            raise PromiseProgressProjectionError("Legacy parity row does not match its block field contract.")
        fields_by_role: dict[str, dict[str, Any]] = {}
        for (field_role, column), display_value, semantic_classification in zip(
            role_columns, values, classifications, strict=True
        ):
            destination = f"{column}{row_number}"
            fields_by_role[field_role] = {
                "field_role": field_role,
                "destination_cell": destination,
                "display_value": "" if display_value is None else str(display_value),
                "semantic_classification": semantic_classification,
                "structural_classification": str(row["structural_classification"]),
            }
        reviewed_identity = row["reviewed_semantic_identity"]
        if not isinstance(reviewed_identity, Mapping) or set(reviewed_identity) != {
            "semantic_identities",
            "period_or_horizon_ids",
        }:
            raise PromiseProgressProjectionError("Legacy parity semantic identity differs from the closed contract.")
        counterpart_signature = row["counterpart_signature"]
        if not isinstance(counterpart_signature, Mapping) or set(counterpart_signature) != {
            "block_id",
            "source_row_type",
            "business_key_class",
            "source_native_business_key",
            "canonical_series_or_program_id",
            "canonical_version_id",
            "reviewed_semantic_identity",
            "field_roles",
        }:
            raise PromiseProgressProjectionError("Legacy parity counterpart signature differs from the closed contract.")
        if counterpart_signature["block_id"] != block_id:
            raise PromiseProgressProjectionError("Legacy parity counterpart signature changed its owning block.")
        legacy_rows.append(
            {
                "legacy_row_id": legacy_row_id,
                "block_id": block_id,
                "visible_sheet_row": row_number,
                "reviewed_legacy_business_key": business_key,
                "row_type": row_type,
                "business_key_class": business_key_class,
                "reviewed_semantic_identity": _canonical(reviewed_identity),
                "counterpart_signature": _canonical(counterpart_signature),
                "mapping_kind": None,
                "source_row": None,
                "fields_by_role": fields_by_role,
            }
        )

    if len({row["legacy_row_id"] for row in legacy_rows}) != len(legacy_rows):
        raise PromiseProgressProjectionError("Legacy parity matrix repeats a frozen legacy row identity.")
    legacy_rows_by_id = {row["legacy_row_id"]: row for row in legacy_rows}

    capture_manifest = _canonical(oracle["capture_manifest"])
    capture_manifest_keys = {
        "manifest_id",
        "manifest_version",
        "workbook_oracle_id",
        "workbook_oracle_sha256",
        "sheet_name",
        "comparison_scope_id",
        "used_comparison_scope",
        "ordered_legacy_row_ids",
        "destination_cells",
        "row_field_inventories",
        "row_count",
        "field_count",
        "legacy_matrix_digest",
    }
    if not isinstance(capture_manifest, Mapping) or set(capture_manifest) != capture_manifest_keys:
        raise PromiseProgressProjectionError("Frozen legacy capture manifest differs from the closed contract.")
    for key in ("manifest_id", "manifest_version"):
        _require_id(str(capture_manifest[key]), label=f"legacy capture {key}")
    if (
        capture_manifest["manifest_version"] != LEGACY_CAPTURE_MANIFEST_VERSION
        or capture_manifest["used_comparison_scope"] != LEGACY_COMPARISON_SCOPE
    ):
        raise PromiseProgressProjectionError("Frozen legacy capture uses an unsupported version or comparison scope.")
    legacy_inventories = [_legacy_row_field_inventory(row) for row in legacy_rows]
    ordered_legacy_ids = [row["legacy_row_id"] for row in legacy_rows]
    legacy_destinations = [
        inventory_field["destination_cell"]
        for inventory in legacy_inventories
        for inventory_field in inventory["fields"]
    ]
    expected_capture_manifest = {
        "manifest_id": capture_manifest["manifest_id"],
        "manifest_version": capture_manifest["manifest_version"],
        "workbook_oracle_id": str(oracle["workbook_oracle_id"]),
        "workbook_oracle_sha256": str(oracle["workbook_oracle_sha256"]),
        "sheet_name": SHEET_NAME,
        "comparison_scope_id": str(oracle["comparison_scope_id"]),
        "used_comparison_scope": capture_manifest["used_comparison_scope"],
        "ordered_legacy_row_ids": ordered_legacy_ids,
        "destination_cells": sorted(legacy_destinations),
        "row_field_inventories": legacy_inventories,
        "row_count": len(legacy_rows),
        "field_count": len(legacy_destinations),
        "legacy_matrix_digest": _digest(
            {
                "block_contracts": _canonical(oracle["block_contracts"]),
                "rows": _canonical(
                    sorted(
                        oracle["rows"],
                        key=lambda item: (
                            BLOCK_ORDER.index(str(item["block_id"])),
                            int(item["visible_sheet_row"]),
                        ),
                    )
                ),
            }
        ),
    }
    if capture_manifest != expected_capture_manifest:
        raise PromiseProgressProjectionError("Frozen legacy matrix does not replay its reviewed capture manifest.")
    if _digest(capture_manifest) != independent_expected_digests["capture_manifest_sha256"]:
        raise PromiseProgressProjectionError("Frozen legacy capture manifest differs from its independent expected identity.")

    source_scope_manifest = _canonical(oracle["source_scope_manifest"])
    source_scope_manifest_keys = {
        "manifest_id",
        "manifest_version",
        "product_type",
        "product_id",
        "block_ids",
        "row_scope",
        "row_count",
        "field_count",
        "scope_digest",
    }
    if not isinstance(source_scope_manifest, Mapping) or set(source_scope_manifest) != source_scope_manifest_keys:
        raise PromiseProgressProjectionError("Source-native parity scope manifest differs from the closed contract.")
    for key in ("manifest_id", "manifest_version"):
        _require_id(str(source_scope_manifest[key]), label=f"source scope {key}")
    if source_scope_manifest["manifest_version"] != SOURCE_SCOPE_MANIFEST_VERSION:
        raise PromiseProgressProjectionError("Source-native parity scope uses an unsupported manifest version.")
    source_row_scope = []
    for row in product.ordered_rows:
        inventory = _source_row_field_inventory(row)
        source_row_scope.append(
            {
                "source_native_row_id": row.row_id,
                "source_native_business_key": _source_row_business_key(row),
                "block_id": row.block_id,
                "row_type": row.row_variant,
                "business_key_class": _business_key_class_for_source_row(row),
                "visible_sheet_row": row.visible_sheet_row,
                "canonical_series_or_program_id": row.canonical_series_or_program_id,
                "canonical_version_id": row.canonical_version_id,
                "field_roles": [field_value.field_role for field_value in row.fields],
                "destination_cells": [field_value.anchor_cell for field_value in row.fields],
                "field_inventory_digest": inventory["field_inventory_digest"],
                "counterpart_signature": _source_row_counterpart_signature(row),
            }
        )
    expected_source_scope_manifest = {
        "manifest_id": source_scope_manifest["manifest_id"],
        "manifest_version": source_scope_manifest["manifest_version"],
        "product_type": PRODUCT_TYPE,
        "product_id": product.product_id,
        "block_ids": list(BLOCK_ORDER),
        "row_scope": source_row_scope,
        "row_count": len(product.ordered_rows),
        "field_count": len(product.fields),
        "scope_digest": _digest(source_row_scope),
    }
    if source_scope_manifest != expected_source_scope_manifest:
        raise PromiseProgressProjectionError("Source-native parity scope does not replay its reviewed manifest.")
    if _digest(source_scope_manifest) != independent_expected_digests["source_scope_manifest_sha256"]:
        raise PromiseProgressProjectionError("Source-native parity scope differs from its independent expected identity.")

    policies = tuple(
        sorted(
            (_validate_exception_policy(policy) for policy in oracle["exception_policies"]),
            key=lambda value: value["policy_id"],
        )
    )
    if len({policy["policy_id"] for policy in policies}) != len(policies):
        raise PromiseProgressProjectionError("Parity exception policy identities must be unique.")
    closed_source_row_types = {
        "scorecard_assessment",
        "annual_guidance_series",
        "diagnostic_coverage_gap",
        "open_guidance",
        "guidance_version",
        "promise_version",
    }
    closed_legacy_row_types = {
        "legacy-row-type:annual-capital-structure@1",
        "legacy-row-type:timeline-unsupported@1",
        "legacy-row-type:timeline-terminal-summary@1",
    }
    for policy in policies:
        if set(policy["allowed_row_types"]) - (closed_source_row_types | closed_legacy_row_types):
            raise PromiseProgressProjectionError("Parity exception policy contains an unknown row type.")
        permitted_roles = {
            role
            for block_id in policy["allowed_block_ids"]
            for role, _, _ in BLOCK_FIELD_LAYOUT[block_id]
        }
        if set(policy["allowed_field_roles"]) - permitted_roles:
            raise PromiseProgressProjectionError("Parity exception policy contains a field role outside its block contract.")
        unknown_source_rows = set(policy["allowed_source_native_row_ids"]) - set(rows_by_id)
        if unknown_source_rows:
            raise PromiseProgressProjectionError("Parity exception policy references an unknown source-native row.")
        unknown_business_keys = set(policy["allowed_reviewed_business_keys"]) - reviewed_business_keys
        if unknown_business_keys:
            raise PromiseProgressProjectionError("Parity exception policy references an unknown reviewed business key.")
        unknown_legacy_rows = set(policy["allowed_legacy_row_ids"]) - set(legacy_rows_by_id)
        if unknown_legacy_rows:
            raise PromiseProgressProjectionError("Parity exception policy references an unknown frozen legacy row.")
    used_policy_ids: set[str] = set()

    disposition_keys = {
        "disposition_id", "disposition_version", "disposition_kind", "product_id",
        "block_id", "legacy_row_id", "source_native_row_id", "legacy_business_key",
        "source_native_business_key", "row_type", "business_key_class",
        "semantic_counterpart_class", "mapping_kind", "reviewed_mapping_reason",
        "reason_code", "expected_legacy_field_inventory_digest",
        "expected_source_native_field_inventory_digest", "counterpart_search_result",
        "policy_id", "policy_version", "review_owner", "duration",
        "authorization_digest",
    }
    dispositions: list[dict[str, Any]] = []
    legacy_disposition_counts = {row_id: 0 for row_id in legacy_rows_by_id}
    source_disposition_counts = {row_id: 0 for row_id in rows_by_id}
    row_disposition_report: list[dict[str, Any]] = []
    for raw_disposition in sorted(
        oracle["row_dispositions"], key=lambda item: str(item["disposition_id"])
    ):
        if not isinstance(raw_disposition, Mapping) or set(raw_disposition) != disposition_keys:
            raise PromiseProgressProjectionError("Reviewed row disposition differs from the closed contract.")
        disposition = {key: _canonical(value) for key, value in raw_disposition.items()}
        for key in ("disposition_id", "disposition_version", "product_id", "block_id", "policy_id", "policy_version"):
            _require_id(str(disposition[key]), label=f"row disposition {key}")
        if disposition["disposition_version"] != ROW_DISPOSITION_VERSION:
            raise PromiseProgressProjectionError("Reviewed row disposition uses an unsupported version.")
        if disposition["disposition_kind"] not in PARITY_ROW_DISPOSITION_KINDS:
            raise PromiseProgressProjectionError("Reviewed row disposition uses an unsupported kind.")
        if disposition["product_id"] != product.product_id or disposition["block_id"] not in BLOCK_ORDER:
            raise PromiseProgressProjectionError("Reviewed row disposition is scoped to another product or block.")
        _require_sha(str(disposition["authorization_digest"]), label="row disposition authorization digest")
        for key in ("expected_legacy_field_inventory_digest", "expected_source_native_field_inventory_digest"):
            if disposition[key] is not None:
                _require_sha(str(disposition[key]), label=f"row disposition {key}")

        legacy_row_id = disposition["legacy_row_id"]
        source_row_id = disposition["source_native_row_id"]
        legacy_row = legacy_rows_by_id.get(str(legacy_row_id)) if legacy_row_id is not None else None
        source_row = rows_by_id.get(str(source_row_id)) if source_row_id is not None else None
        if legacy_row_id is not None:
            _require_id(str(legacy_row_id), label="row disposition legacy_row_id")
            if legacy_row is None:
                raise PromiseProgressProjectionError("Reviewed row disposition references an unknown frozen legacy row.")
            legacy_disposition_counts[str(legacy_row_id)] += 1
        if source_row_id is not None:
            _require_id(str(source_row_id), label="row disposition source_native_row_id")
            if source_row is None:
                raise PromiseProgressProjectionError("Reviewed row disposition references an unknown source-native row.")
            source_disposition_counts[str(source_row_id)] += 1
        kind = str(disposition["disposition_kind"])
        if kind == "paired":
            if legacy_row is None or source_row is None or disposition["mapping_kind"] == "none":
                raise PromiseProgressProjectionError("Paired row disposition requires both rows and an explicit mapping kind.")
        elif kind == "legacy_only":
            if legacy_row is None or source_row is not None or disposition["mapping_kind"] != "none":
                raise PromiseProgressProjectionError("Legacy-only disposition must resolve exactly one frozen legacy row.")
        elif source_row is None or legacy_row is not None or disposition["mapping_kind"] != "none":
            raise PromiseProgressProjectionError("Source-native-only disposition must resolve exactly one product row.")
        if disposition["mapping_kind"] not in PARITY_ROW_MAPPING_KINDS:
            raise PromiseProgressProjectionError("Reviewed row disposition uses an unsupported mapping kind.")

        if legacy_row is not None:
            counterpart_row_ids = tuple(sorted(
                row_id for row_id, candidate in rows_by_id.items()
                if legacy_row["counterpart_signature"] == _source_row_counterpart_signature(candidate)
            ))
        else:
            assert source_row is not None
            signature = _source_row_counterpart_signature(source_row)
            counterpart_row_ids = tuple(sorted(
                row_id for row_id, candidate in legacy_rows_by_id.items()
                if candidate["counterpart_signature"] == signature
            ))
        if list(counterpart_row_ids) != disposition["counterpart_search_result"]:
            raise PromiseProgressProjectionError("Reviewed row disposition counterpart search result does not replay.")
        reason = _derive_row_disposition_reason(
            disposition_kind=kind,
            legacy_row=legacy_row,
            source_row=source_row,
            counterpart_row_ids=counterpart_row_ids,
        )
        if reason is None or disposition["reason_code"] != reason:
            raise PromiseProgressProjectionError("Reviewed row disposition reason differs from independent classification.")

        if legacy_row is not None:
            legacy_inventory = _legacy_row_field_inventory(legacy_row)
            if disposition["legacy_business_key"] != legacy_row["reviewed_legacy_business_key"]:
                raise PromiseProgressProjectionError("Reviewed row disposition changed its legacy business key.")
            if disposition["expected_legacy_field_inventory_digest"] != legacy_inventory["field_inventory_digest"]:
                raise PromiseProgressProjectionError("Reviewed row disposition legacy field inventory does not replay.")
        elif disposition["legacy_business_key"] is not None or disposition["expected_legacy_field_inventory_digest"] is not None:
            raise PromiseProgressProjectionError("Source-native-only disposition may not claim legacy ownership.")
        if source_row is not None:
            source_inventory = _source_row_field_inventory(source_row)
            if disposition["source_native_business_key"] != _source_row_business_key(source_row):
                raise PromiseProgressProjectionError("Reviewed row disposition changed its source-native business key.")
            if disposition["expected_source_native_field_inventory_digest"] != source_inventory["field_inventory_digest"]:
                raise PromiseProgressProjectionError("Reviewed row disposition source-native field inventory does not replay.")
        elif disposition["source_native_business_key"] is not None or disposition["expected_source_native_field_inventory_digest"] is not None:
            raise PromiseProgressProjectionError("Legacy-only disposition may not claim source-native ownership.")

        owning_row_type = source_row.row_variant if source_row is not None else legacy_row["row_type"]
        owning_business_class = (
            _business_key_class_for_source_row(source_row) if source_row is not None
            else legacy_row["business_key_class"]
        )
        owner_block = source_row.block_id if source_row is not None else legacy_row["block_id"]
        if disposition["row_type"] != owning_row_type or disposition["business_key_class"] != owning_business_class:
            raise PromiseProgressProjectionError("Reviewed row disposition changed its typed row identity.")
        if disposition["block_id"] != owner_block:
            raise PromiseProgressProjectionError("Reviewed row disposition changed its owning block.")

        if kind == "paired":
            assert legacy_row is not None and source_row is not None
            if counterpart_row_ids != (source_row.row_id,):
                raise PromiseProgressProjectionError("Paired disposition does not resolve exactly one semantic counterpart.")
            if legacy_row["row_type"] != source_row.row_variant or legacy_row["business_key_class"] != _business_key_class_for_source_row(source_row):
                raise PromiseProgressProjectionError("Paired disposition differs in row type or business-key class.")
            if legacy_row["reviewed_semantic_identity"] != _parity_row_semantic_identity(source_row):
                raise PromiseProgressProjectionError("Paired disposition has incompatible semantic identities.")
            if disposition["semantic_counterpart_class"] != legacy_row["business_key_class"]:
                raise PromiseProgressProjectionError("Paired disposition changed its semantic counterpart class.")
            expected_mapping_reason = (
                "same_destination_semantic_counterpart"
                if disposition["mapping_kind"] == "same-destination-equivalent"
                else "reviewed_destination_remap"
            )
            if disposition["reviewed_mapping_reason"] != expected_mapping_reason:
                raise PromiseProgressProjectionError("Paired disposition changed its reviewed mapping reason.")
            if disposition["mapping_kind"] == "same-destination-equivalent" and legacy_row["visible_sheet_row"] != source_row.visible_sheet_row:
                raise PromiseProgressProjectionError("Same-destination row disposition changed the visible row.")
            if disposition["mapping_kind"] == "reviewed-destination-remap" and legacy_row["visible_sheet_row"] == source_row.visible_sheet_row:
                raise PromiseProgressProjectionError("Reviewed row remap does not move the row.")
            legacy_row["source_row"] = source_row
            legacy_row["mapping_kind"] = disposition["mapping_kind"]
        elif disposition["semantic_counterpart_class"] is not None or disposition["reviewed_mapping_reason"] is not None:
            raise PromiseProgressProjectionError("One-sided row disposition may not claim paired-row semantics.")

        matching_policies = [policy for policy in policies if _policy_authorizes_row_disposition(
            policy, disposition=disposition, independently_derived_reason=reason
        )]
        if len(matching_policies) != 1:
            raise PromiseProgressProjectionError("Reviewed row disposition requires exactly one authorized policy scope.")
        policy = matching_policies[0]
        expected_authorization = _row_disposition_authorization_digest(
            disposition=disposition, policy=policy,
            independently_derived_reason=reason, counterpart_row_ids=counterpart_row_ids,
            legacy_capture_manifest_sha256=independent_expected_digests["capture_manifest_sha256"],
            source_scope_manifest_sha256=independent_expected_digests["source_scope_manifest_sha256"],
        )
        if disposition["authorization_digest"] != expected_authorization:
            raise PromiseProgressProjectionError("Reviewed row disposition authorization digest does not replay.")
        if (
            disposition["review_owner"] != policy["acceptance_owner"]
            or disposition["duration"] != policy["duration"]
        ):
            raise PromiseProgressProjectionError("Reviewed row disposition review ownership differs from its policy.")
        used_policy_ids.add(policy["policy_id"])
        dispositions.append(disposition)
        row_disposition_report.append({
            "disposition_id": disposition["disposition_id"],
            "disposition_kind": kind,
            "legacy_row_id": legacy_row_id,
            "source_native_row_id": source_row_id,
            "block_id": disposition["block_id"],
            "row_type": disposition["row_type"],
            "business_key_class": disposition["business_key_class"],
            "reason_code": reason,
            "policy_id": policy["policy_id"],
            "counterpart_search_result": list(counterpart_row_ids),
            "legacy_field_count": len(legacy_row["fields_by_role"]) if legacy_row is not None else 0,
            "source_native_field_count": len(source_row.fields) if source_row is not None else 0,
            "duration": disposition["duration"],
            "authorization_digest": expected_authorization,
        })

    if any(count != 1 for count in legacy_disposition_counts.values()):
        raise PromiseProgressProjectionError("Reviewed row dispositions do not partition every frozen legacy row exactly once.")
    if any(count != 1 for count in source_disposition_counts.values()):
        raise PromiseProgressProjectionError("Reviewed row dispositions do not partition every source-native row exactly once.")
    if len({item["disposition_id"] for item in dispositions}) != len(dispositions):
        raise PromiseProgressProjectionError("Reviewed row disposition identities are duplicated.")
    if _digest(sorted(dispositions, key=lambda item: item["disposition_id"])) != independent_expected_digests["row_disposition_graph_sha256"]:
        raise PromiseProgressProjectionError("Reviewed row-disposition graph differs from its independent expected identity.")

    for policy in policies:
        if policy["authorization_kind"] != "row-disposition" or policy["state"] != "active":
            continue
        associated = [item for item in dispositions if item["policy_id"] == policy["policy_id"] and item["policy_version"] == policy["exception_policy_version"]]
        if not associated:
            raise PromiseProgressProjectionError("Active row-disposition policy has no reviewed disposition use.")
        expected_scopes = {
            "allowed_difference_reason_codes": {item["reason_code"] for item in associated},
            "allowed_product_ids": {item["product_id"] for item in associated},
            "allowed_block_ids": {item["block_id"] for item in associated},
            "allowed_row_types": {item["row_type"] for item in associated},
            "allowed_business_key_classes": {item["business_key_class"] for item in associated},
            "allowed_reviewed_business_keys": {item["legacy_business_key"] for item in associated if item["legacy_business_key"] is not None},
            "allowed_legacy_row_ids": {item["legacy_row_id"] for item in associated if item["legacy_row_id"] is not None},
            "allowed_source_native_row_ids": {item["source_native_row_id"] for item in associated if item["source_native_row_id"] is not None},
            "allowed_disposition_kinds": {item["disposition_kind"] for item in associated},
            "allowed_sheet_names": {SHEET_NAME},
        }
        for key, expected in expected_scopes.items():
            if set(policy[key]) != expected:
                raise PromiseProgressProjectionError(f"Row-disposition policy {policy['policy_id']!r} contains unused or overbroad {key}.")

    paired_source_row_ids = [str(item["source_native_row_id"]) for item in dispositions if item["disposition_kind"] == "paired"]
    reviewed_source_only = tuple(str(item["source_native_row_id"]) for item in dispositions if item["disposition_kind"] == "source_native_only")
    dispositions_by_legacy_row_id = {
        str(item["legacy_row_id"]): item for item in dispositions if item["legacy_row_id"] is not None
    }
    dispositions_by_source_row_id = {
        str(item["source_native_row_id"]): item for item in dispositions if item["source_native_row_id"] is not None
    }

    observation_keys = {
        "structural_observation_id", "observation_version", "product_id", "sheet_name",
        "block_ids", "condition_type", "observed_legacy_state",
        "observed_source_native_state", "difference_reason_code", "comparison_digest",
    }
    structural_observations: list[dict[str, Any]] = []
    observations_by_id: dict[str, dict[str, Any]] = {}
    for raw_observation in sorted(
        oracle["structural_observations"], key=lambda item: str(item["structural_observation_id"])
    ):
        if not isinstance(raw_observation, Mapping) or set(raw_observation) != observation_keys:
            raise PromiseProgressProjectionError("Structural observation differs from the closed contract.")
        observation = {key: _canonical(value) for key, value in raw_observation.items()}
        _require_id(str(observation["structural_observation_id"]), label="structural observation_id")
        _require_id(str(observation["observation_version"]), label="structural observation version")
        _require_sha(str(observation["comparison_digest"]), label="structural observation comparison digest")
        if observation["observation_version"] != STRUCTURAL_OBSERVATION_VERSION:
            raise PromiseProgressProjectionError("Structural observation uses an unsupported version.")
        condition_type = str(observation["condition_type"])
        if condition_type not in PARITY_STRUCTURAL_CONDITION_TYPES:
            raise PromiseProgressProjectionError("Structural observation uses an unknown condition type.")
        expected_states = PARITY_STRUCTURAL_OBSERVATION_STATES[condition_type]
        if (
            observation["product_id"] != product.product_id
            or observation["sheet_name"] != SHEET_NAME
            or tuple(observation["block_ids"]) != PARITY_STRUCTURAL_BLOCK_SCOPE[condition_type]
            or observation["observed_legacy_state"] != expected_states["observed_legacy_state"]
            or observation["observed_source_native_state"] != expected_states["observed_source_native_state"]
        ):
            raise PromiseProgressProjectionError("Structural observation does not replay the independently reviewed condition.")
        reason = _structural_observation_reason(condition_type)
        if observation["difference_reason_code"] != reason:
            raise PromiseProgressProjectionError("Structural observation reason differs from independent classification.")
        expected_comparison_digest = _digest(
            {key: value for key, value in observation.items() if key != "comparison_digest"}
        )
        if observation["comparison_digest"] != expected_comparison_digest:
            raise PromiseProgressProjectionError("Structural observation comparison digest does not replay.")
        observation_id = str(observation["structural_observation_id"])
        if observation_id in observations_by_id:
            raise PromiseProgressProjectionError("Structural observation identities must be unique.")
        observations_by_id[observation_id] = observation
        structural_observations.append(observation)
    if set(observation["condition_type"] for observation in structural_observations) != PARITY_STRUCTURAL_CONDITION_TYPES:
        raise PromiseProgressProjectionError("Structural observations do not exactly cover the active reviewed conditions.")
    if _digest(structural_observations) != independent_expected_digests["structural_observation_set_sha256"]:
        raise PromiseProgressProjectionError("Structural observation set differs from its independent expected identity.")

    structural_binding_keys = {
        "structural_binding_id", "binding_version", "structural_observation_id",
        "policy_id", "exception_id", "policy_version", "product_id", "sheet_name",
        "block_ids", "condition_type", "difference_reason_code", "difference_class",
        "comparison_digest", "structural_authorization_digest",
    }
    structural_bindings: list[dict[str, Any]] = []
    bound_observation_counts = {observation_id: 0 for observation_id in observations_by_id}
    structural_binding_report: list[dict[str, Any]] = []
    for raw_binding in sorted(
        oracle["structural_bindings"], key=lambda item: str(item["structural_binding_id"])
    ):
        if not isinstance(raw_binding, Mapping) or set(raw_binding) != structural_binding_keys:
            raise PromiseProgressProjectionError("Structural binding differs from the closed contract.")
        binding = {key: _canonical(value) for key, value in raw_binding.items()}
        for key in (
            "structural_binding_id", "binding_version", "structural_observation_id",
            "policy_id", "exception_id", "policy_version", "product_id",
        ):
            _require_id(str(binding[key]), label=f"structural binding {key}")
        _require_sha(str(binding["comparison_digest"]), label="structural binding comparison digest")
        _require_sha(
            str(binding["structural_authorization_digest"]),
            label="structural binding authorization digest",
        )
        if binding["binding_version"] != STRUCTURAL_BINDING_VERSION:
            raise PromiseProgressProjectionError("Structural binding uses an unsupported version.")
        observation = observations_by_id.get(str(binding["structural_observation_id"]))
        if observation is None:
            raise PromiseProgressProjectionError("Structural binding references an unknown observation.")
        bound_observation_counts[str(binding["structural_observation_id"])] += 1
        reason = _structural_observation_reason(str(observation["condition_type"]))
        if (
            binding["product_id"] != observation["product_id"]
            or binding["sheet_name"] != observation["sheet_name"]
            or binding["block_ids"] != observation["block_ids"]
            or binding["condition_type"] != observation["condition_type"]
            or binding["difference_reason_code"] != reason
            or binding["difference_class"] != "structural-product-difference"
            or binding["comparison_digest"] != observation["comparison_digest"]
        ):
            raise PromiseProgressProjectionError("Structural binding differs from its independently observed condition.")
        matching_policies = [
            policy
            for policy in policies
            if policy["authorization_kind"] == "structural-product-difference"
            and policy["state"] == "active"
            and binding["policy_id"] == policy["policy_id"]
            and binding["exception_id"] == policy["exception_id"]
            and binding["policy_version"] == policy["exception_policy_version"]
            and reason in policy["allowed_difference_reason_codes"]
            and binding["product_id"] in policy["allowed_product_ids"]
            and set(binding["block_ids"]).issubset(policy["allowed_block_ids"])
            and binding["structural_binding_id"] in policy["allowed_structural_binding_ids"]
            and binding["condition_type"] in policy["allowed_structural_condition_types"]
            and binding["sheet_name"] in policy["allowed_sheet_names"]
            and binding["difference_class"] in policy["allowed_difference_classes"]
        ]
        if len(matching_policies) != 1:
            raise PromiseProgressProjectionError("Structural binding requires exactly one authorized policy scope.")
        policy = matching_policies[0]
        expected_authorization = _structural_authorization_digest(
            binding=binding,
            observation=observation,
            policy=policy,
            independently_derived_reason=reason,
        )
        if binding["structural_authorization_digest"] != expected_authorization:
            raise PromiseProgressProjectionError("Structural binding authorization digest does not replay.")
        used_policy_ids.add(policy["policy_id"])
        structural_bindings.append(binding)
        structural_binding_report.append(
            {
                "structural_binding_id": binding["structural_binding_id"],
                "structural_observation_id": binding["structural_observation_id"],
                "policy_id": policy["policy_id"],
                "exception_id": policy["exception_id"],
                "condition_type": binding["condition_type"],
                "difference_reason_code": reason,
                "product_id": binding["product_id"],
                "sheet_name": binding["sheet_name"],
                "block_ids": list(binding["block_ids"]),
                "comparison_digest": binding["comparison_digest"],
                "structural_authorization_digest": expected_authorization,
            }
        )
    if any(count != 1 for count in bound_observation_counts.values()):
        raise PromiseProgressProjectionError("Every structural observation must have exactly one reviewed binding.")
    if len({item["structural_binding_id"] for item in structural_bindings}) != len(structural_bindings):
        raise PromiseProgressProjectionError("Structural binding identities must be unique.")
    if _digest(structural_bindings) != independent_expected_digests["structural_binding_set_sha256"]:
        raise PromiseProgressProjectionError("Structural binding set differs from its independent expected identity.")
    for policy in policies:
        if policy["authorization_kind"] != "structural-product-difference" or policy["state"] != "active":
            continue
        associated = [
            item for item in structural_bindings
            if item["policy_id"] == policy["policy_id"]
            and item["policy_version"] == policy["exception_policy_version"]
        ]
        if not associated:
            raise PromiseProgressProjectionError("Active structural policy has no observed reviewed binding.")
        expected_scopes = {
            "allowed_difference_reason_codes": {item["difference_reason_code"] for item in associated},
            "allowed_product_ids": {item["product_id"] for item in associated},
            "allowed_block_ids": {block_id for item in associated for block_id in item["block_ids"]},
            "allowed_structural_binding_ids": {item["structural_binding_id"] for item in associated},
            "allowed_structural_condition_types": {item["condition_type"] for item in associated},
            "allowed_sheet_names": {item["sheet_name"] for item in associated},
            "allowed_difference_classes": {item["difference_class"] for item in associated},
        }
        for key, expected in expected_scopes.items():
            if set(policy[key]) != expected:
                raise PromiseProgressProjectionError(
                    f"Structural policy {policy['policy_id']!r} contains unused or overbroad {key}."
                )

    binding_keys = {
        "binding_id",
        "exception_id",
        "exception_policy_version",
        "semantic_reason_code",
        "product_id",
        "block_id",
        "reviewed_legacy_business_key",
        "source_native_row_id",
        "row_type",
        "business_key_class",
        "field_role",
        "legacy_destination_cell",
        "source_native_destination_cell",
        "difference_class",
        "legacy_value",
        "source_native_value",
        "comparison_digest",
        "exception_authorization_digest",
    }
    bindings: list[dict[str, Any]] = []
    for raw_binding in oracle["accepted_differences"]:
        if not isinstance(raw_binding, Mapping) or set(raw_binding) != binding_keys:
            raise PromiseProgressProjectionError("Accepted parity binding differs from the closed reviewed contract.")
        binding = {key: _canonical(value) for key, value in raw_binding.items()}
        for key in (
            "binding_id",
            "exception_id",
            "exception_policy_version",
            "product_id",
            "block_id",
            "reviewed_legacy_business_key",
            "source_native_row_id",
            "row_type",
            "business_key_class",
        ):
            _require_id(str(binding[key]), label=f"accepted parity {key}")
        if binding["exception_id"] not in CLOSED_PARITY_EXCEPTION_IDS:
            raise PromiseProgressProjectionError("Accepted parity binding uses an unknown exception identity.")
        if binding["semantic_reason_code"] not in PARITY_DIFFERENCE_REASON_CODES:
            raise PromiseProgressProjectionError("Accepted parity binding uses an unknown semantic reason code.")
        if binding["block_id"] not in BLOCK_ORDER:
            raise PromiseProgressProjectionError("Accepted parity binding uses an unknown block.")
        if binding["difference_class"] not in PARITY_OBSERVED_DIFFERENCE_CLASSES:
            raise PromiseProgressProjectionError("Accepted parity binding uses an unsupported observed difference class.")
        for destination_key in ("legacy_destination_cell", "source_native_destination_cell"):
            if not re.fullmatch(r"[A-L](?:[1-9]|[1-9][0-9]|10[0-2])", str(binding[destination_key])):
                raise PromiseProgressProjectionError("Accepted parity binding has an invalid destination.")
        _require_sha(str(binding["comparison_digest"]), label="accepted parity comparison_digest")
        _require_sha(
            str(binding["exception_authorization_digest"]),
            label="accepted parity exception_authorization_digest",
        )
        bindings.append(binding)
    bindings.sort(key=lambda value: value["binding_id"])
    if len({binding["binding_id"] for binding in bindings}) != len(bindings):
        raise PromiseProgressProjectionError("Accepted parity binding identities must be unique.")

    comparisons: list[dict[str, Any]] = []
    used_binding_indexes: set[int] = set()
    used_exception_ids: set[str] = set()

    def append_paired_comparison(legacy_row: Mapping[str, Any], source_row: ProductRow) -> None:
        row_disposition = dispositions_by_legacy_row_id[legacy_row["legacy_row_id"]]
        source_by_role = {field_value.field_role: field_value for field_value in source_row.fields}
        if set(source_by_role) != set(legacy_row["fields_by_role"]):
            raise PromiseProgressProjectionError("Legacy/source-native semantic row pair has different field roles.")
        for field_role, _, _ in BLOCK_FIELD_LAYOUT[source_row.block_id]:
            legacy_field = legacy_row["fields_by_role"][field_role]
            source_field = source_by_role[field_role]
            legacy_destination = legacy_field["destination_cell"]
            source_destination = source_field.anchor_cell
            if legacy_row["mapping_kind"] == "same-destination-equivalent" and legacy_destination != source_destination:
                raise PromiseProgressProjectionError("Same-destination semantic row pair changed a field destination.")
            if legacy_row["mapping_kind"] == "reviewed-destination-remap":
                underlying = "reviewed-destination-remap"
            elif legacy_field["display_value"] == source_field.display_value.display_text:
                underlying = "exact-match"
            elif _normalized_parity_text(legacy_field["display_value"]) == _normalized_parity_text(
                source_field.display_value.display_text
            ):
                underlying = "accepted-semantic-match"
            else:
                underlying = "unregistered-difference"
            comparison_digest = _parity_difference_digest(
                product_id=product.product_id,
                block_id=source_row.block_id,
                reviewed_legacy_business_key=legacy_row["reviewed_legacy_business_key"],
                source_native_row_id=source_row.row_id,
                row_type=source_row.row_variant,
                business_key_class=legacy_row["business_key_class"],
                field_role=field_role,
                legacy_destination_cell=legacy_destination,
                source_native_destination_cell=source_destination,
                underlying_difference_class=underlying,
                legacy_display_value=legacy_field["display_value"],
                source_native_display_value=source_field.display_value.display_text,
            )
            reason = (
                None
                if underlying in {"exact-match", "accepted-semantic-match"}
                else _derive_parity_difference_reason(
                    mapping_kind=legacy_row["mapping_kind"],
                    source_row=source_row,
                    source_field=source_field,
                )
            )
            classification = underlying
            exception_id: str | None = None
            policy_id: str | None = None
            authorization_digest: str | None = None
            authorization_failure_reason: str | None = None
            destination_candidates = [
                (position, binding)
                for position, binding in enumerate(bindings)
                if binding["legacy_destination_cell"] == legacy_destination
                and binding["source_native_destination_cell"] == source_destination
            ]
            if underlying in {"exact-match", "accepted-semantic-match"}:
                if destination_candidates:
                    authorization_failure_reason = "accepted binding targets a field without an observed difference"
                    classification = "unauthorized-exception-binding"
                    used_binding_indexes.update(position for position, _ in destination_candidates)
            elif reason is None:
                classification = "unregistered-difference"
            elif len(destination_candidates) == 0:
                classification = "unregistered-difference"
            elif len(destination_candidates) > 1:
                classification = "unauthorized-exception-binding"
                authorization_failure_reason = "observed difference resolves more than one accepted binding"
                used_binding_indexes.update(position for position, _ in destination_candidates)
            else:
                position, binding = destination_candidates[0]
                used_binding_indexes.add(position)
                observed_scope = {
                    "product_id": product.product_id,
                    "block_id": source_row.block_id,
                    "reviewed_legacy_business_key": legacy_row["reviewed_legacy_business_key"],
                    "source_native_row_id": source_row.row_id,
                    "row_type": source_row.row_variant,
                    "business_key_class": legacy_row["business_key_class"],
                    "field_role": field_role,
                    "legacy_destination_cell": legacy_destination,
                    "source_native_destination_cell": source_destination,
                    "difference_class": underlying,
                    "legacy_value": legacy_field["display_value"],
                    "source_native_value": source_field.display_value.display_text,
                    "comparison_digest": comparison_digest,
                }
                mismatched = [key for key, value in observed_scope.items() if binding[key] != value]
                if mismatched:
                    classification = "unauthorized-exception-binding"
                    authorization_failure_reason = (
                        "accepted binding scope differs from the independently observed difference: "
                        + ", ".join(sorted(mismatched))
                    )
                elif binding["semantic_reason_code"] != reason:
                    classification = "unauthorized-exception-binding"
                    authorization_failure_reason = "accepted binding reason differs from independent classification"
                else:
                    matching_policies = [
                        policy
                        for policy in policies
                        if _policy_authorizes_observed_difference(
                            policy,
                            binding=binding,
                            independently_derived_reason=reason,
                        )
                    ]
                    if len(matching_policies) == 0:
                        classification = "unauthorized-exception-binding"
                        authorization_failure_reason = "zero exception-policy scopes authorize the observed difference"
                    elif len(matching_policies) > 1:
                        classification = "unauthorized-exception-binding"
                        authorization_failure_reason = "multiple exception-policy scopes authorize the observed difference"
                    else:
                        policy = matching_policies[0]
                        expected_authorization_digest = _exception_authorization_digest(
                            binding=binding,
                            policy=policy,
                            independently_derived_reason=reason,
                        )
                        if binding["exception_authorization_digest"] != expected_authorization_digest:
                            classification = "unauthorized-exception-binding"
                            authorization_failure_reason = "exception authorization digest does not replay"
                        elif binding["exception_id"] not in product.applied_parity_exception_ids:
                            classification = "unauthorized-exception-binding"
                            authorization_failure_reason = "exception is not activated by the reviewed product plan"
                        else:
                            classification = "registered-authorized-exception"
                            exception_id = binding["exception_id"]
                            policy_id = policy["policy_id"]
                            authorization_digest = expected_authorization_digest
                            used_exception_ids.add(exception_id)
                            used_policy_ids.add(policy_id)
            comparisons.append(
                {
                    "classification": classification,
                    "underlying_difference_class": underlying,
                    "difference_reason_code": reason,
                    "block_id": source_row.block_id,
                    "legacy_visible_sheet_row": legacy_row["visible_sheet_row"],
                    "source_native_visible_sheet_row": source_row.visible_sheet_row,
                    "reviewed_legacy_business_key": legacy_row["reviewed_legacy_business_key"],
                    "source_native_row_id": source_row.row_id,
                    "row_type": source_row.row_variant,
                    "business_key_class": legacy_row["business_key_class"],
                    "field_role": field_role,
                    "legacy_destination_cell": legacy_destination,
                    "source_native_destination_cell": source_destination,
                    "legacy_display_value": legacy_field["display_value"],
                    "source_native_display_value": source_field.display_value.display_text,
                    "source_native_field_id": source_field.field_id,
                    "exception_id": exception_id,
                    "exception_policy_id": policy_id,
                    "comparison_digest": comparison_digest,
                    "exception_authorization_digest": authorization_digest,
                    "authorization_failure_reason": authorization_failure_reason,
                    "row_disposition_id": row_disposition["disposition_id"],
                    "row_disposition_reason_code": row_disposition["reason_code"],
                    "row_disposition_policy_id": row_disposition["policy_id"],
                    "row_disposition_authorization_digest": row_disposition["authorization_digest"],
                }
            )

    for legacy_row in legacy_rows:
        source_row = legacy_row["source_row"]
        if source_row is not None:
            append_paired_comparison(legacy_row, source_row)
            continue
        row_disposition = dispositions_by_legacy_row_id[legacy_row["legacy_row_id"]]
        for field_role, _, _ in BLOCK_FIELD_LAYOUT[legacy_row["block_id"]]:
            legacy_field = legacy_row["fields_by_role"][field_role]
            comparisons.append(
                {
                    "classification": "legacy-only-field",
                    "underlying_difference_class": "legacy-only-field",
                    "difference_reason_code": row_disposition["reason_code"],
                    "block_id": legacy_row["block_id"],
                    "legacy_visible_sheet_row": legacy_row["visible_sheet_row"],
                    "source_native_visible_sheet_row": None,
                    "reviewed_legacy_business_key": legacy_row["reviewed_legacy_business_key"],
                    "source_native_row_id": None,
                    "row_type": legacy_row["row_type"],
                    "business_key_class": legacy_row["business_key_class"],
                    "field_role": field_role,
                    "legacy_destination_cell": legacy_field["destination_cell"],
                    "source_native_destination_cell": None,
                    "legacy_display_value": legacy_field["display_value"],
                    "source_native_display_value": None,
                    "source_native_field_id": None,
                    "exception_id": None,
                    "exception_policy_id": None,
                    "comparison_digest": None,
                    "exception_authorization_digest": None,
                    "authorization_failure_reason": None,
                    "row_disposition_id": row_disposition["disposition_id"],
                    "row_disposition_reason_code": row_disposition["reason_code"],
                    "row_disposition_policy_id": row_disposition["policy_id"],
                    "row_disposition_authorization_digest": row_disposition["authorization_digest"],
                }
            )

    for source_row_id in sorted(
        reviewed_source_only,
        key=lambda value: (
            BLOCK_ORDER.index(rows_by_id[value].block_id),
            rows_by_id[value].visible_sheet_row,
            value,
        ),
    ):
        source_row = rows_by_id[source_row_id]
        row_disposition = dispositions_by_source_row_id[source_row_id]
        for source_field in source_row.fields:
            comparisons.append(
                {
                    "classification": "source-native-only-field",
                    "underlying_difference_class": "source-native-only-field",
                    "difference_reason_code": row_disposition["reason_code"],
                    "block_id": source_row.block_id,
                    "legacy_visible_sheet_row": None,
                    "source_native_visible_sheet_row": source_row.visible_sheet_row,
                    "reviewed_legacy_business_key": None,
                    "source_native_row_id": source_row.row_id,
                    "row_type": source_row.row_variant,
                    "business_key_class": _business_key_class_for_source_row(source_row),
                    "field_role": source_field.field_role,
                    "legacy_destination_cell": None,
                    "source_native_destination_cell": source_field.anchor_cell,
                    "legacy_display_value": None,
                    "source_native_display_value": source_field.display_value.display_text,
                    "source_native_field_id": source_field.field_id,
                    "exception_id": None,
                    "exception_policy_id": None,
                    "comparison_digest": None,
                    "exception_authorization_digest": None,
                    "authorization_failure_reason": None,
                    "row_disposition_id": row_disposition["disposition_id"],
                    "row_disposition_reason_code": row_disposition["reason_code"],
                    "row_disposition_policy_id": row_disposition["policy_id"],
                    "row_disposition_authorization_digest": row_disposition["authorization_digest"],
                }
            )

    comparisons.sort(
        key=lambda value: (
            BLOCK_ORDER.index(value["block_id"]),
            value["legacy_visible_sheet_row"]
            if value["legacy_visible_sheet_row"] is not None
            else value["source_native_visible_sheet_row"],
            value["source_native_visible_sheet_row"] or 0,
            value["field_role"],
        )
    )
    for policy in policies:
        if policy["authorization_kind"] != "field-difference" or policy["state"] != "active":
            continue
        associated = [
            binding
            for binding in bindings
            if binding["exception_id"] == policy["exception_id"]
            and binding["exception_policy_version"] == policy["exception_policy_version"]
        ]
        if not associated:
            continue
        expected_scopes = {
            "allowed_difference_reason_codes": {item["semantic_reason_code"] for item in associated},
            "allowed_product_ids": {item["product_id"] for item in associated},
            "allowed_block_ids": {item["block_id"] for item in associated},
            "allowed_row_types": {item["row_type"] for item in associated},
            "allowed_business_key_classes": {item["business_key_class"] for item in associated},
            "allowed_reviewed_business_keys": {
                item["reviewed_legacy_business_key"] for item in associated
            },
            "allowed_source_native_row_ids": {item["source_native_row_id"] for item in associated},
            "allowed_field_roles": {item["field_role"] for item in associated},
            "allowed_difference_classes": {item["difference_class"] for item in associated},
        }
        for key, expected in expected_scopes.items():
            if set(policy[key]) != expected:
                raise PromiseProgressProjectionError(
                    f"Parity exception policy {policy['policy_id']!r} contains unused or overbroad {key}."
                )
        expected_pairs = {
            (item["legacy_destination_cell"], item["source_native_destination_cell"])
            for item in associated
        }
        policy_pairs = {
            (item["legacy_destination_cell"], item["source_native_destination_cell"])
            for item in policy["allowed_destination_pairs"]
        }
        if policy_pairs != expected_pairs:
            raise PromiseProgressProjectionError(
                f"Parity exception policy {policy['policy_id']!r} contains unused or overbroad destination scope."
            )
    counts = {
        classification: sum(1 for row in comparisons if row["classification"] == classification)
        for classification in sorted(PARITY_DIFFERENCE_CLASSES)
    }
    unused_bindings = [
        _canonical(binding)
        for position, binding in enumerate(bindings)
        if position not in used_binding_indexes
    ]
    unused_policies = sorted(
        policy["policy_id"]
        for policy in policies
        if policy["state"] == "active" and policy["policy_id"] not in used_policy_ids
    )
    row_counts = {
        "paired_rows": sum(1 for item in dispositions if item["disposition_kind"] == "paired"),
        "authorized_legacy_only_rows": sum(
            1 for item in dispositions if item["disposition_kind"] == "legacy_only"
        ),
        "authorized_source_native_only_rows": sum(
            1 for item in dispositions if item["disposition_kind"] == "source_native_only"
        ),
        "unauthorized_one_sided_rows": 0,
        "missing_row_dispositions": 0,
        "duplicate_row_dispositions": 0,
        "counterpart_conflicts": 0,
        "mapping_alignment_defects": counts["mapping-alignment-defect"],
    }
    structural_counts = {
        "observed_structural_differences": len(structural_observations),
        "authorized_structural_bindings": len(structural_bindings),
        "unauthorized_structural_bindings": 0,
        "unused_active_structural_policies": sum(
            1
            for policy in policies
            if policy["state"] == "active"
            and policy["authorization_kind"] == "structural-product-difference"
            and policy["policy_id"] in unused_policies
        ),
        "overbroad_structural_policy_scopes": 0,
    }
    completeness_report = {
        "legacy_capture_manifest_sha256": independent_expected_digests["capture_manifest_sha256"],
        "legacy_capture_digest_state": "exact",
        "expected_legacy_row_ids": list(capture_manifest["ordered_legacy_row_ids"]),
        "observed_legacy_row_ids": [row["legacy_row_id"] for row in legacy_rows],
        "expected_legacy_row_count": capture_manifest["row_count"],
        "observed_legacy_row_count": len(legacy_rows),
        "expected_legacy_field_count": capture_manifest["field_count"],
        "observed_legacy_field_count": sum(len(row["fields_by_role"]) for row in legacy_rows),
        "source_scope_manifest_sha256": independent_expected_digests["source_scope_manifest_sha256"],
        "source_scope_digest_state": "exact",
        "expected_source_native_row_ids": [item["source_native_row_id"] for item in source_scope_manifest["row_scope"]],
        "observed_source_native_row_ids": [row.row_id for row in product.ordered_rows],
        "expected_source_native_row_count": source_scope_manifest["row_count"],
        "observed_source_native_row_count": len(product.ordered_rows),
        "expected_source_native_field_count": source_scope_manifest["field_count"],
        "observed_source_native_field_count": len(product.fields),
        "field_inventory_state": "exact",
        "row_disposition_graph_sha256": independent_expected_digests["row_disposition_graph_sha256"],
    }
    blocking_authorization_count = (
        counts["unregistered-difference"]
        + counts["unauthorized-exception-binding"]
        + counts["mapping-alignment-defect"]
        + counts["structurally-incomparable"]
        + row_counts["unauthorized_one_sided_rows"]
        + row_counts["missing_row_dispositions"]
        + row_counts["duplicate_row_dispositions"]
        + row_counts["counterpart_conflicts"]
        + structural_counts["unauthorized_structural_bindings"]
        + structural_counts["unused_active_structural_policies"]
        + structural_counts["overbroad_structural_policy_scopes"]
    )
    return {
        "comparison_scope": {
            "state": "declared",
            "comparison_scope_id": str(oracle["comparison_scope_id"]),
            "product_id": product.product_id,
            "workbook_oracle_id": str(oracle["workbook_oracle_id"]),
            "workbook_oracle_sha256": str(oracle["workbook_oracle_sha256"]),
            "sheet_name": str(oracle["sheet_name"]),
            "legacy_field_count": sum(len(row["fields_by_role"]) for row in legacy_rows),
            "source_native_field_count": len(product.fields),
            "semantic_pair_count": len(paired_source_row_ids),
            "legacy_only_row_count": sum(1 for row in legacy_rows if row["source_row"] is None),
            "source_native_only_row_count": len(reviewed_source_only),
            "accepted_binding_count": len(bindings),
            "exception_policy_definition_count": len(policies),
        },
        "comparison_counts": counts,
        "field_comparisons": comparisons,
        "row_disposition_counts": row_counts,
        "row_dispositions": row_disposition_report,
        "structural_counts": structural_counts,
        "structural_bindings": structural_binding_report,
        "completeness": completeness_report,
        "unregistered_difference_count": blocking_authorization_count,
        "unused_accepted_difference_bindings": unused_bindings,
        "unused_registered_exception_ids": unused_policies,
    }


def _validate_shadow_shape(shadow: Mapping[str, Any]) -> list[dict[str, str]]:
    """Validate scalar and collection shape before semantic cross-reference replay."""

    issues: list[dict[str, str]] = []
    root_keys = {
        "schema_id", "product_id", "company_id", "template_oracle_sha256",
        "sheet_name", "ui_as_of_date", "knowledge_cutoff", "reference_catalog", "rows", "fields",
        "lineage_digest",
    }
    row_keys = {
        "product_id", "block_id", "row_id", "row_variant", "business_order",
        "visible_sheet_row", "ui_as_of_date", "canonical_series_or_program_id",
        "canonical_version_id", "field_ids", "review_issue_ids",
        "parity_exception_ids", "lineage_digest",
    }
    field_keys = {
        "product_id", "block_id", "row_id", "field_id", "field_role",
        "destination", "display_value", "canonical_record_ids", "target_version_id",
        "actual_selection_id", "actual_observation_id", "progress_selection_id",
        "progress_observation_ids", "status_assessment_id", "semantic_identity",
        "period_or_horizon_id", "publication_date", "event_date", "ui_as_of_date",
        "knowledge_dates", "source_document_ids", "source_occurrence_ids",
        "selection_or_calculation_method_id", "review_issue_ids",
        "parity_exception_ids", "lineage_state", "lineage_digest",
    }
    if set(shadow) != root_keys:
        issues.append({"rule_id": "shadow_root_shape", "message": "Shadow root keys differ from the closed schema."})
        return issues
    if shadow.get("schema_id") != SHADOW_SCHEMA_ID or shadow.get("sheet_name") != SHEET_NAME:
        issues.append({"rule_id": "shadow_identity", "message": "Shadow schema or sheet identity is not supported."})
    try:
        _require_id(str(shadow.get("product_id", "")), label="shadow product_id")
        _require_sha(str(shadow.get("template_oracle_sha256", "")), label="shadow template oracle")
        _parse_date(str(shadow.get("ui_as_of_date", "")), label="shadow ui_as_of_date")
        _parse_date(str(shadow.get("knowledge_cutoff", "")), label="shadow knowledge_cutoff")
    except PromiseProgressProjectionError as exc:
        issues.append({"rule_id": "shadow_scalar_contract", "message": str(exc)})
    rows = shadow.get("rows")
    fields = shadow.get("fields")
    if not isinstance(rows, list) or not isinstance(fields, list):
        issues.append({"rule_id": "shadow_collection_type", "message": "Shadow rows and fields must be arrays."})
        return issues
    row_ids: set[str] = set()
    field_ids: set[str] = set()
    for row in rows:
        if not isinstance(row, Mapping) or set(row) != row_keys:
            issues.append({"rule_id": "shadow_row_shape", "message": "A shadow row differs from the closed schema."})
            continue
        row_id = str(row["row_id"])
        if row_id in row_ids:
            issues.append({"rule_id": "shadow_duplicate_row", "message": f"Duplicate row {row_id}."})
        row_ids.add(row_id)
        if row["block_id"] not in BLOCK_ORDER or not 1 <= int(row["visible_sheet_row"]) <= 102:
            issues.append({"rule_id": "shadow_row_layout", "message": f"Row {row_id} has an unsupported block or destination row."})
        if len(row["field_ids"]) != len(set(row["field_ids"])):
            issues.append({"rule_id": "shadow_duplicate_row_field", "message": f"Row {row_id} repeats a field identity."})
    for field_value in fields:
        if not isinstance(field_value, Mapping) or set(field_value) != field_keys:
            issues.append({"rule_id": "shadow_field_shape", "message": "A shadow field differs from the closed schema."})
            continue
        field_id = str(field_value["field_id"])
        if field_id in field_ids:
            issues.append({"rule_id": "shadow_duplicate_field", "message": f"Duplicate field {field_id}."})
        field_ids.add(field_id)
        destination = field_value.get("destination")
        if not isinstance(destination, Mapping) or set(destination) != {"sheet", "anchor_cell", "display_range"}:
            issues.append({"rule_id": "shadow_destination_shape", "message": f"Field {field_id} has an invalid destination."})
        elif destination["sheet"] != SHEET_NAME or not re.fullmatch(r"[A-O](?:[1-9]|[1-9][0-9]|10[0-2])", str(destination["anchor_cell"])):
            issues.append({"rule_id": "shadow_destination", "message": f"Field {field_id} has an unsupported destination."})
        if field_value["row_id"] not in row_ids or field_value["block_id"] not in BLOCK_ORDER:
            issues.append({"rule_id": "shadow_field_owner", "message": f"Field {field_id} has no valid row or block owner."})
        if field_value["lineage_state"] not in {"accepted", "diagnostic", "needs-review", "missing", "blocked"}:
            issues.append({"rule_id": "shadow_lineage_state", "message": f"Field {field_id} has an unknown lineage state."})
        if set(field_value["parity_exception_ids"]) - CLOSED_PARITY_EXCEPTION_IDS:
            issues.append({"rule_id": "shadow_exception", "message": f"Field {field_id} uses an unregistered exception."})
        try:
            cutoff = _parse_date(str(field_value["ui_as_of_date"]), label="shadow field as-of")
            for knowledge_date in field_value["knowledge_dates"]:
                if _parse_date(str(knowledge_date), label="shadow knowledge date") > cutoff:
                    issues.append({"rule_id": "shadow_temporal_leakage", "message": f"Field {field_id} leaks later knowledge."})
        except PromiseProgressProjectionError as exc:
            issues.append({"rule_id": "shadow_date", "message": str(exc)})
    referenced_fields = {str(identity) for row in rows if isinstance(row, Mapping) for identity in row.get("field_ids", ())}
    if referenced_fields != field_ids:
        issues.append({"rule_id": "shadow_field_coverage", "message": "Row field references do not exactly cover shadow fields."})
    digest_payload = {key: value for key, value in shadow.items() if key != "lineage_digest"}
    if shadow.get("lineage_digest") != _digest(digest_payload):
        issues.append({"rule_id": "shadow_lineage_digest", "message": "Shadow lineage digest is stale."})
    return issues


def validate_shadow_matrix(shadow: Mapping[str, Any]) -> list[dict[str, str]]:
    """Independently replay the closed shadow schema and semantic ownership graph."""

    issues = list(_validate_shadow_shape(shadow))
    rows = shadow.get("rows")
    fields = shadow.get("fields")
    references = shadow.get("reference_catalog")
    if not isinstance(rows, list) or not isinstance(fields, list) or not isinstance(references, Mapping):
        return issues
    reference_keys = {
        "canonical_record_ids",
        "series_or_program_ids",
        "source_document_ids",
        "source_occurrence_ids",
        "review_issue_ids",
        "period_or_horizon_ids",
        "metric_ids",
        "definition_ids",
        "basis_ids",
        "unit_ids",
        "axis_ids",
        "member_ids",
        "actual_selection_ids",
        "progress_selection_ids",
        "status_assessment_ids",
        "parity_exception_ids",
    }
    if set(references) != reference_keys:
        issues.append(
            {
                "rule_id": "shadow_reference_catalog_shape",
                "message": "Shadow reference catalog differs from the closed contract.",
            }
        )
        return issues
    reference_sets: dict[str, set[str]] = {}
    for key in sorted(reference_keys):
        values = references[key]
        if not isinstance(values, list) or any(not isinstance(value, str) or not value for value in values):
            issues.append(
                {
                    "rule_id": "shadow_reference_catalog_type",
                    "message": f"Reference catalog {key!r} is not an identity array.",
                }
            )
            reference_sets[key] = set()
            continue
        if values != sorted(set(values)):
            issues.append(
                {
                    "rule_id": "shadow_reference_catalog_order",
                    "message": f"Reference catalog {key!r} is not sorted and unique.",
                }
            )
        reference_sets[key] = set(values)
    if reference_sets["parity_exception_ids"] - CLOSED_PARITY_EXCEPTION_IDS:
        issues.append(
            {
                "rule_id": "shadow_reference_catalog_exception",
                "message": "Reference catalog contains an unknown parity exception.",
            }
        )

    product_id = str(shadow.get("product_id", ""))
    row_map: dict[str, Mapping[str, Any]] = {
        str(row.get("row_id")): row
        for row in rows
        if isinstance(row, Mapping) and row.get("row_id")
    }
    fields_by_row: dict[str, list[Mapping[str, Any]]] = {identity: [] for identity in row_map}
    allowed_rows = {
        SCORECARD_BLOCK_ID: set(range(5, 10)),
        ANNUAL_BLOCK_ID: set(ANNUAL_DATA_ROWS),
        OPEN_BLOCK_ID: set(OPEN_DATA_ROWS),
        TIMELINE_BLOCK_ID: set(TIMELINE_DATA_ROWS),
    }
    allowed_variants = {
        SCORECARD_BLOCK_ID: {"scorecard_assessment"},
        ANNUAL_BLOCK_ID: {"annual_guidance_series", "diagnostic_coverage_gap"},
        OPEN_BLOCK_ID: {"open_guidance"},
        TIMELINE_BLOCK_ID: {"guidance_version", "promise_version"},
    }
    for row in rows:
        if not isinstance(row, Mapping) or not row.get("row_id"):
            continue
        row_id = str(row["row_id"])
        block_id = str(row.get("block_id", ""))
        if row.get("product_id") != product_id:
            issues.append({"rule_id": "shadow_row_product", "message": f"Row {row_id} belongs to another product."})
        if block_id not in BLOCK_FIELD_LAYOUT or int(row.get("visible_sheet_row", 0)) not in allowed_rows.get(block_id, set()):
            issues.append({"rule_id": "shadow_row_destination", "message": f"Row {row_id} is outside its locked block geometry."})
        if row.get("row_variant") not in allowed_variants.get(block_id, set()):
            issues.append({"rule_id": "shadow_row_variant", "message": f"Row {row_id} has a variant invalid for its block."})
        if set(row.get("review_issue_ids", ())) - reference_sets["review_issue_ids"]:
            issues.append({"rule_id": "shadow_row_review_reference", "message": f"Row {row_id} references an unknown review issue."})
        if set(row.get("parity_exception_ids", ())) - reference_sets["parity_exception_ids"]:
            issues.append({"rule_id": "shadow_row_parity_reference", "message": f"Row {row_id} references an unowned parity exception."})
        series_or_program_id = row.get("canonical_series_or_program_id")
        if series_or_program_id is not None and series_or_program_id not in reference_sets["series_or_program_ids"]:
            issues.append({"rule_id": "shadow_row_series_reference", "message": f"Row {row_id} references an unknown series or program identity."})
        version_id = row.get("canonical_version_id")
        if version_id is not None and version_id not in reference_sets["canonical_record_ids"]:
            issues.append({"rule_id": "shadow_row_canonical_reference", "message": f"Row {row_id} references an unknown canonical version identity."})

    for field_value in fields:
        if not isinstance(field_value, Mapping) or not field_value.get("field_id"):
            continue
        field_id = str(field_value["field_id"])
        row_id = str(field_value.get("row_id", ""))
        row = row_map.get(row_id)
        if row is None:
            continue
        fields_by_row[row_id].append(field_value)
        block_id = str(field_value.get("block_id", ""))
        field_role = str(field_value.get("field_role", ""))
        if field_value.get("product_id") != product_id:
            issues.append({"rule_id": "shadow_field_product", "message": f"Field {field_id} belongs to another product."})
        if block_id != row.get("block_id"):
            issues.append({"rule_id": "shadow_field_block_owner", "message": f"Field {field_id} block differs from its owning row."})
        layout = {role: (anchor, display_range) for role, anchor, display_range in BLOCK_FIELD_LAYOUT.get(block_id, ())}
        if field_role not in layout:
            issues.append({"rule_id": "shadow_field_role", "message": f"Field {field_id} has a role invalid for its block."})
        else:
            row_number = int(row["visible_sheet_row"])
            anchor_column, display_columns = layout[field_role]
            expected_anchor = f"{anchor_column}{row_number}"
            expected_range = (
                f"{display_columns}{row_number}"
                if ":" not in display_columns
                else f"{display_columns.split(':')[0]}{row_number}:{display_columns.split(':')[1]}{row_number}"
            )
            destination = field_value.get("destination", {})
            if not isinstance(destination, Mapping) or destination.get("anchor_cell") != expected_anchor or destination.get("display_range") != expected_range or destination.get("sheet") != SHEET_NAME:
                issues.append({"rule_id": "shadow_field_destination_contract", "message": f"Field {field_id} destination is invalid for its block and role."})
            expected_field_id = f"{row_id}:field:{field_role.replace('_', '-')}@1"
            if field_id != expected_field_id:
                issues.append({"rule_id": "shadow_field_identity", "message": f"Field {field_id} does not replay from row and role identity."})
        try:
            display = field_value.get("display_value", {})
            DisplayValue(
                str(display.get("value_form", "")),
                str(display.get("display_text", "")),
                display.get("machine_value"),
            )
        except (AttributeError, PromiseProgressProjectionError) as exc:
            issues.append({"rule_id": "shadow_machine_value", "message": f"Field {field_id}: {exc}"})

        canonical_ids = set(field_value.get("canonical_record_ids", ()))
        if canonical_ids - reference_sets["canonical_record_ids"]:
            issues.append({"rule_id": "shadow_canonical_reference", "message": f"Field {field_id} references an unknown canonical input."})
        for key in ("target_version_id", "actual_observation_id"):
            identity = field_value.get(key)
            if identity is not None and identity not in reference_sets["canonical_record_ids"]:
                issues.append({"rule_id": "shadow_canonical_reference", "message": f"Field {field_id} has an unresolved {key}."})
        if set(field_value.get("progress_observation_ids", ())) - reference_sets["canonical_record_ids"]:
            issues.append({"rule_id": "shadow_progress_observation_reference", "message": f"Field {field_id} has an unresolved Progress observation."})
        selection_references = (
            ("actual_selection_id", "actual_selection_ids"),
            ("progress_selection_id", "progress_selection_ids"),
            ("status_assessment_id", "status_assessment_ids"),
        )
        for field_key, catalog_key in selection_references:
            identity = field_value.get(field_key)
            if identity is not None and identity not in reference_sets[catalog_key]:
                issues.append({"rule_id": "shadow_selection_reference", "message": f"Field {field_id} has an unresolved {field_key}."})
        if set(field_value.get("source_document_ids", ())) - reference_sets["source_document_ids"]:
            issues.append({"rule_id": "shadow_source_reference", "message": f"Field {field_id} references an unknown source document."})
        if set(field_value.get("source_occurrence_ids", ())) - reference_sets["source_occurrence_ids"]:
            issues.append({"rule_id": "shadow_evidence_reference", "message": f"Field {field_id} references an unknown EvidenceOccurrence."})
        if set(field_value.get("review_issue_ids", ())) - reference_sets["review_issue_ids"]:
            issues.append({"rule_id": "shadow_review_reference", "message": f"Field {field_id} references an unknown review issue."})
        exceptions = set(field_value.get("parity_exception_ids", ()))
        if exceptions - reference_sets["parity_exception_ids"]:
            issues.append({"rule_id": "shadow_parity_reference", "message": f"Field {field_id} references an unowned parity exception."})
        for exception_id in exceptions & CLOSED_PARITY_EXCEPTION_IDS:
            if (block_id, field_role) not in _PARITY_FIELD_SCOPES[exception_id]:
                issues.append({"rule_id": "shadow_parity_scope", "message": f"Field {field_id} applies a parity exception outside its closed scope."})
        period_id = field_value.get("period_or_horizon_id")
        if period_id is not None and period_id not in reference_sets["period_or_horizon_ids"]:
            issues.append({"rule_id": "shadow_period_reference", "message": f"Field {field_id} references an unknown period or horizon."})
        semantic = field_value.get("semantic_identity", {})
        semantic_catalogs = (
            ("metric_id", "metric_ids"),
            ("definition_id", "definition_ids"),
            ("basis_id", "basis_ids"),
            ("unit_id", "unit_ids"),
        )
        if isinstance(semantic, Mapping):
            for semantic_key, catalog_key in semantic_catalogs:
                identity = semantic.get(semantic_key)
                if identity is not None and identity not in reference_sets[catalog_key]:
                    issues.append({"rule_id": "shadow_semantic_reference", "message": f"Field {field_id} has an unresolved {semantic_key}."})
            dimensions = semantic.get("dimensions", ())
            dimension_pairs: list[tuple[str, str]] = []
            for dimension in dimensions if isinstance(dimensions, list) else ():
                if isinstance(dimension, Mapping):
                    pair = (str(dimension.get("axis_id", "")), str(dimension.get("member_id", "")))
                    dimension_pairs.append(pair)
                    if pair[0] not in reference_sets["axis_ids"] or pair[1] not in reference_sets["member_ids"]:
                        issues.append({"rule_id": "shadow_dimension_reference", "message": f"Field {field_id} has an unresolved dimension member."})
            if dimension_pairs != sorted(set(dimension_pairs)):
                issues.append({"rule_id": "shadow_dimension_order", "message": f"Field {field_id} dimensions are not sorted and unique."})
        try:
            for key in ("publication_date", "event_date"):
                if field_value.get(key) is not None:
                    _parse_date(str(field_value[key]), label=f"shadow {key}")
        except PromiseProgressProjectionError as exc:
            issues.append({"rule_id": "shadow_date_contract", "message": f"Field {field_id}: {exc}"})

        destination = field_value.get("destination", {})
        recalculated_field = _selection_lineage(
            "field",
            {
                "product_id": field_value.get("product_id"),
                "block_id": block_id,
                "row_id": row_id,
                "field_id": field_id,
                "field_role": field_role,
                "anchor_cell": destination.get("anchor_cell") if isinstance(destination, Mapping) else None,
                "display_range": destination.get("display_range") if isinstance(destination, Mapping) else None,
                "display": field_value.get("display_value"),
                "canonical_record_ids": tuple(field_value.get("canonical_record_ids", ())),
                "target_version_id": field_value.get("target_version_id"),
                "actual_id": field_value.get("actual_selection_id"),
                "progress_id": field_value.get("progress_selection_id"),
                "status_id": field_value.get("status_assessment_id"),
                "semantic_identity": field_value.get("semantic_identity"),
                "period_or_horizon_id": field_value.get("period_or_horizon_id"),
                "ui_as_of_date": field_value.get("ui_as_of_date"),
                "knowledge_dates": tuple(field_value.get("knowledge_dates", ())),
                "source_occurrence_ids": tuple(field_value.get("source_occurrence_ids", ())),
                "source_document_ids": tuple(field_value.get("source_document_ids", ())),
                "method_id": field_value.get("selection_or_calculation_method_id"),
                "review_issue_ids": tuple(field_value.get("review_issue_ids", ())),
                "parity_exception_ids": tuple(field_value.get("parity_exception_ids", ())),
            },
        )
        if recalculated_field != field_value.get("lineage_digest"):
            issues.append({"rule_id": "shadow_field_lineage_digest", "message": f"Field {field_id} has a stale semantic lineage digest."})

    for row_id, row in row_map.items():
        row_fields = fields_by_row.get(row_id, [])
        expected_roles = tuple(role for role, _, _ in BLOCK_FIELD_LAYOUT.get(str(row.get("block_id")), ()))
        if tuple(field.get("field_role") for field in row_fields) != expected_roles:
            issues.append({"rule_id": "shadow_row_field_roles", "message": f"Row {row_id} does not contain exactly its block-specific fields."})
        if list(row.get("field_ids", ())) != [field.get("field_id") for field in row_fields]:
            issues.append({"rule_id": "shadow_row_field_ownership", "message": f"Row {row_id} field identities do not exactly own its shadow fields."})
        recalculated_row = _selection_lineage(
            "row",
            {
                "product_id": row.get("product_id"),
                "block_id": row.get("block_id"),
                "row_id": row_id,
                "variant": row.get("row_variant"),
                "business_order": row.get("business_order"),
                "visible_sheet_row": row.get("visible_sheet_row"),
                "ui_as_of_date": row.get("ui_as_of_date"),
                "series_or_program": row.get("canonical_series_or_program_id"),
                "version": row.get("canonical_version_id"),
                "field_ids": list(row.get("field_ids", ())),
                "issues": tuple(row.get("review_issue_ids", ())),
                "exceptions": tuple(row.get("parity_exception_ids", ())),
            },
        )
        if recalculated_row != row.get("lineage_digest"):
            issues.append({"rule_id": "shadow_row_lineage_digest", "message": f"Row {row_id} has a stale semantic lineage digest."})
    return issues


def promise_progress_product_sha256(product: PromiseProgressProduct) -> str:
    return hashlib.sha256(serialize_promise_progress_product(product)).hexdigest()


@dataclass(frozen=True)
class _Indexes:
    package: Mapping[str, Any]
    company_id: str
    observations: Mapping[str, Mapping[str, Any]]
    entities: Mapping[str, Mapping[str, Any]]
    occurrences: Mapping[str, Mapping[str, Any]]
    documents: Mapping[str, Mapping[str, Any]]
    periods: Mapping[str, Mapping[str, Any]]
    resolutions: tuple[Mapping[str, Any], ...]
    relations: tuple[Mapping[str, Any], ...]
    review_issues: tuple[Mapping[str, Any], ...]
    dimension_sets: Mapping[str, tuple[tuple[str, str], ...]]
    catalog_ids: Mapping[str, frozenset[str]]
    unit_catalog: Mapping[str, Mapping[str, Any]]
    metric_names: Mapping[str, str]
    selected_record_ids: frozenset[str]


def _unique_index(rows: Iterable[Mapping[str, Any]], key: str, *, label: str) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        identity = str(row.get(key, ""))
        if not identity:
            raise PromiseProgressProjectionError(f"{label} contains a record without {key}.")
        if identity in result:
            raise PromiseProgressProjectionError(f"Duplicate {label} identity {identity!r}.")
        result[identity] = row
    return result


def _build_indexes(package: Mapping[str, Any]) -> _Indexes:
    if package.get("artifact_state") != "accepted":
        raise PromiseProgressProjectionError("Promise Progress projection requires an accepted longitudinal-memory package.")
    company_id = str(package.get("company_id", ""))
    if not company_id:
        raise PromiseProgressProjectionError("Longitudinal package has no company identity.")
    observations = _unique_index(
        ({"record_id": row.get("header", {}).get("record_id"), **row} for row in package.get("observations", ())),
        "record_id",
        label="observations",
    )
    entities = _unique_index(
        ({"entity_id": row.get("header", {}).get("entity_id"), **row} for row in package.get("entities", ())),
        "entity_id",
        label="entities",
    )
    occurrences = _unique_index(package.get("evidence_occurrences", ()), "evidence_occurrence_id", label="evidence occurrences")
    documents = _unique_index(package.get("source_documents", ()), "source_document_id", label="source documents")
    periods = _unique_index(package.get("periods", ()), "period_id", label="periods")
    catalog = package.get("catalog", {})
    catalog_specs = {
        "metrics": "metric_id",
        "definitions": "definition_id",
        "bases": "basis_id",
        "units": "unit_id",
        "dimensions": "dimension_id",
        "dimension_members": "member_id",
    }
    catalog_ids = {
        name: frozenset(str(row[id_key]) for row in catalog.get(name, ()))
        for name, id_key in catalog_specs.items()
    }
    dimension_sets = {
        str(row["dimension_set_id"]): tuple(
            sorted((str(member["dimension_id"]), str(member["member_id"])) for member in row.get("members", ()))
        )
        for row in catalog.get("dimension_sets", ())
    }
    if len(dimension_sets) != len(tuple(catalog.get("dimension_sets", ()))):
        raise PromiseProgressProjectionError("Duplicate dimension-set identity in source package.")
    unit_catalog = {str(row["unit_id"]): row for row in catalog.get("units", ())}
    metric_names = {str(row["metric_id"]): str(row.get("display_name", row["metric_id"])) for row in catalog.get("metrics", ())}
    selected = frozenset(
        str(row["selected_record_id"])
        for row in package.get("resolutions", ())
        if row.get("status") == "selected" and row.get("selected_record_id")
    )
    return _Indexes(
        package=package,
        company_id=company_id,
        observations=MappingProxyType(observations),
        entities=MappingProxyType(entities),
        occurrences=MappingProxyType(occurrences),
        documents=MappingProxyType(documents),
        periods=MappingProxyType(periods),
        resolutions=tuple(package.get("resolutions", ())),
        relations=tuple(package.get("relations", ())),
        review_issues=tuple(package.get("review_issues", ())),
        dimension_sets=MappingProxyType(dimension_sets),
        catalog_ids=MappingProxyType(catalog_ids),
        unit_catalog=MappingProxyType(unit_catalog),
        metric_names=MappingProxyType(metric_names),
        selected_record_ids=selected,
    )


def _semantic_from_payload(index: _Indexes, payload: Mapping[str, Any], dimension_set_id: str | None) -> SemanticIdentity:
    return SemanticIdentity(
        str(payload["metric_id"]) if payload.get("metric_id") else None,
        str(payload["definition_id"]) if payload.get("definition_id") else None,
        str(payload["basis_id"]) if payload.get("basis_id") else None,
        str(payload["unit_id"]) if payload.get("unit_id") else None,
        index.dimension_sets.get(str(dimension_set_id), ()),
    )


def _semantic_from_config(index: _Indexes, value: Mapping[str, Any]) -> SemanticIdentity:
    keys = ("metric_id", "definition_id", "basis_id", "unit_id")
    collections = ("metrics", "definitions", "bases", "units")
    resolved: list[str | None] = []
    for key, collection in zip(keys, collections, strict=True):
        item = value.get(key)
        if item is None:
            resolved.append(None)
            continue
        item = str(item)
        if item not in index.catalog_ids[collection]:
            raise PromiseProgressProjectionError(f"Projection plan references unknown {key} {item!r}.")
        resolved.append(item)
    dimension_set_id = value.get("dimension_set_id")
    dimensions: tuple[tuple[str, str], ...] = ()
    if dimension_set_id is not None:
        dimension_set_id = str(dimension_set_id)
        if dimension_set_id not in index.dimension_sets:
            raise PromiseProgressProjectionError(f"Projection plan references unknown dimension_set_id {dimension_set_id!r}.")
        dimensions = index.dimension_sets[dimension_set_id]
    return SemanticIdentity(resolved[0], resolved[1], resolved[2], resolved[3], dimensions)


def _role_semantic_assertion(
    index: _Indexes,
    assertion: Any,
    *,
    role_id: str,
    role_key: str,
    semantic_classes: Mapping[str, str],
    label: str,
) -> tuple[frozenset[str], frozenset[str], frozenset[str]]:
    """Validate one reviewed role assertion against a closed product-role registry."""

    if not isinstance(assertion, Mapping):
        raise PromiseProgressProjectionError(f"A {label} binding requires one reviewed role-semantic assertion.")
    if str(assertion.get(role_key, "")) != role_id:
        raise PromiseProgressProjectionError(f"{label} role differs from its reviewed semantic assertion.")
    expected_class = semantic_classes[role_id]
    if str(assertion.get("semantic_class", "")) != expected_class:
        raise PromiseProgressProjectionError(
            f"{label} semantic class must be the closed class {expected_class!r} for role {role_id!r}."
        )
    allowed_definitions = frozenset(str(value) for value in assertion.get("allowed_definition_ids", ()))
    allowed_bases = frozenset(str(value) for value in assertion.get("allowed_basis_ids", ()))
    allowed_period_types = frozenset(str(value) for value in assertion.get("allowed_period_types", ()))
    if not (allowed_definitions or allowed_bases or allowed_period_types):
        raise PromiseProgressProjectionError(f"A {label} role-semantic assertion cannot be unconstrained.")
    if allowed_definitions - index.catalog_ids["definitions"]:
        raise PromiseProgressProjectionError(f"{label} role assertion references an unknown definition identity.")
    if allowed_bases - index.catalog_ids["bases"]:
        raise PromiseProgressProjectionError(f"{label} role assertion references an unknown basis identity.")
    if allowed_period_types - _CLOSED_PERIOD_TYPES:
        raise PromiseProgressProjectionError(f"{label} role assertion references an unknown period type.")
    required_period_type = {
        ACTUAL_FY_ID: "annual",
        ACTUAL_QUARTER_ID: "quarter",
        ACTUAL_YTD_ID: "year_to_date",
        PROGRESS_FY_ID: "annual",
        PROGRESS_YTD_ID: "year_to_date",
    }.get(role_id)
    if required_period_type and allowed_period_types != {required_period_type}:
        raise PromiseProgressProjectionError(
            f"{label} role {role_id!r} requires exactly period type {required_period_type!r}."
        )
    if role_id in {ACTUAL_CUMULATIVE_ID, PROGRESS_CUMULATIVE_ID} and not (allowed_definitions and allowed_bases):
        raise PromiseProgressProjectionError(f"{label} cumulative semantics require closed definition and basis identities.")
    return allowed_definitions, allowed_bases, allowed_period_types


def _assert_record_role_semantics(
    index: _Indexes,
    record: Mapping[str, Any],
    *,
    allowed_definitions: frozenset[str],
    allowed_bases: frozenset[str],
    allowed_period_types: frozenset[str],
    label: str,
) -> None:
    payload = record["payload"]
    period = index.periods.get(str(record["header"].get("effective_period_id", "")))
    if allowed_definitions and str(payload.get("definition_id", "")) not in allowed_definitions:
        raise PromiseProgressProjectionError(f"{label} input definition is incompatible with its reviewed role.")
    if allowed_bases and str(payload.get("basis_id", "")) not in allowed_bases:
        raise PromiseProgressProjectionError(f"{label} input basis is incompatible with its reviewed role.")
    if allowed_period_types and (period is None or str(period.get("period_type", "")) not in allowed_period_types):
        raise PromiseProgressProjectionError(f"{label} input period type is incompatible with its reviewed role.")


def _require_record_evidence(index: _Indexes, record: Mapping[str, Any], *, label: str) -> None:
    occurrence_ids = tuple(record.get("header", {}).get("evidence_occurrence_ids", ()))
    if not occurrence_ids:
        record_id = str(record.get("header", {}).get("record_id", ""))
        raise PromiseProgressProjectionError(f"{label} record {record_id!r} has no source EvidenceOccurrence.")
    _source_ids(index, occurrence_ids)


def _source_ids(index: _Indexes, occurrence_ids: Iterable[str]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    occurrences = _sorted_unique(occurrence_ids)
    documents: list[str] = []
    for occurrence_id in occurrences:
        occurrence = index.occurrences.get(occurrence_id)
        if occurrence is None:
            raise PromiseProgressProjectionError(f"Missing evidence occurrence {occurrence_id!r}.")
        document_id = str(occurrence.get("source_document_id", ""))
        if document_id not in index.documents:
            raise PromiseProgressProjectionError(f"Occurrence {occurrence_id!r} references a missing source document.")
        documents.append(document_id)
    return occurrences, _sorted_unique(documents)


def _record_source(index: _Indexes, record: Mapping[str, Any]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    _require_record_evidence(index, record, label="Displayed")
    return _source_ids(index, record.get("header", {}).get("evidence_occurrence_ids", ()))


def _format_scalar(value: str, unit: Mapping[str, Any] | None, currency: str | None) -> str:
    number = _plain_decimal(value)
    if unit is None:
        return number
    kind = str(unit.get("unit_kind", ""))
    scale = str(unit.get("scale", "1"))
    if kind == "percent":
        return f"{number}%"
    if kind == "percentage-point":
        return f"{number} pp"
    if kind == "currency":
        symbol = "$" if currency == "USD" else (f"{currency} " if currency else "")
        suffix = "m" if scale == "1000000" else ("bn" if scale == "1000000000" else "")
        return f"{symbol}{number}{suffix}"
    if kind == "count":
        suffix = "bn" if scale == "1000000000" else ""
        return f"{number}{suffix}"
    return f"{number} {unit.get('display_name', '')}".strip()


def display_value_from_spec(
    value: Mapping[str, Any] | None,
    *,
    unit: Mapping[str, Any] | None = None,
    currency: str | None = None,
) -> DisplayValue:
    """Render a canonical ValueSpec without losing qualifiers, bounds or ranges."""

    if value is None:
        return MISSING_DISPLAY
    kind = str(value.get("kind", ""))
    unit_kind = str(unit.get("unit_kind", "")) if unit else ""
    exact_form = "percentage" if unit_kind in {"percent", "percentage-point"} else "exact"
    if kind == "exact":
        raw = _plain_decimal(value.get("value"))
        return DisplayValue(exact_form, _format_scalar(raw, unit, currency), raw)
    if kind == "approximate":
        raw = _plain_decimal(value.get("value"))
        qualifier = str(value.get("qualifier", ""))
        if qualifier not in {"around", "about", "approximately", "tilde"}:
            raise PromiseProgressProjectionError(f"Unsupported approximation qualifier {qualifier!r}.")
        tolerance = value.get("tolerance")
        machine = {"value": raw, "qualifier": qualifier, "tolerance": None if tolerance is None else _plain_decimal(tolerance)}
        prefix = "~" if qualifier == "tilde" else f"{qualifier} "
        return DisplayValue("approximate", prefix + _format_scalar(raw, unit, currency), machine)
    if kind == "range":
        low = _plain_decimal(value.get("low"))
        high = _plain_decimal(value.get("high"))
        low_text = _format_scalar(low, unit, currency)
        high_text = _format_scalar(high, unit, currency)
        if unit_kind == "currency" and currency == "USD" and low_text.startswith("$") and high_text.startswith("$"):
            high_text = high_text[1:]
        if unit_kind in {"percent", "percentage-point"}:
            suffix = "%" if unit_kind == "percent" else " pp"
            low_text = low_text.removesuffix(suffix)
        return DisplayValue(
            "range",
            f"{low_text}–{high_text}",
            {
                "low": low,
                "high": high,
                "low_inclusive": bool(value.get("low_inclusive", True)),
                "high_inclusive": bool(value.get("high_inclusive", True)),
            },
        )
    if kind == "bound":
        operator = str(value.get("operator", ""))
        if operator not in {"gt", "gte", "lt", "lte"}:
            raise PromiseProgressProjectionError(f"Unsupported bound operator {operator!r}.")
        raw = _plain_decimal(value.get("value"))
        symbols = {"gt": ">", "gte": "≥", "lt": "<", "lte": "≤"}
        return DisplayValue("bound", symbols[operator] + _format_scalar(raw, unit, currency), {"operator": operator, "value": raw})
    if kind == "qualitative":
        text = str(value.get("text", "")).strip()
        if not text:
            raise PromiseProgressProjectionError("Qualitative values require source-backed text.")
        return DisplayValue("qualitative", text, {"text": text, "normalized_band": value.get("normalized_band")})
    raise PromiseProgressProjectionError(f"Unsupported canonical value kind {kind!r}.")


def _matches_payload(record: Mapping[str, Any], selector: Mapping[str, Any]) -> bool:
    payload = record.get("payload", {})
    header = record.get("header", {})
    for key, expected in selector.items():
        if key == "kind":
            actual = payload.get("kind")
        elif key in header:
            actual = header.get(key)
        else:
            actual = payload.get(key)
        if actual != expected:
            return False
    return True


def _resolve_entity(index: _Indexes, selector: Mapping[str, Any]) -> tuple[str, Mapping[str, Any]]:
    matches = [
        (identity, record)
        for identity, record in index.entities.items()
        if _matches_payload(record, selector)
    ]
    if len(matches) != 1:
        raise PromiseProgressProjectionError(
            f"Projection entity selector must resolve exactly once; found {len(matches)} for {_canonical(selector)!r}."
        )
    return matches[0]


def _eligible(record: Mapping[str, Any], cutoff: str) -> bool:
    knowledge = record.get("header", {}).get("knowledge_date")
    return bool(knowledge) and _parse_date(str(knowledge), label="record knowledge_date") <= _parse_date(cutoff, label="row cutoff")


def _terminal_versions(index: _Indexes, records: Sequence[Mapping[str, Any]], cutoff: str) -> tuple[Mapping[str, Any], ...]:
    eligible = [record for record in records if _eligible(record, cutoff)]
    ids = {str(record.get("header", {}).get("record_id")) for record in eligible}
    superseded: set[str] = set()
    for relation in index.relations:
        if relation.get("relation_type") not in {"supersedes", "corrects", "reaffirms", "corroborates"}:
            continue
        newer = str(relation.get("from_record_id", ""))
        older = str(relation.get("to_record_id", ""))
        if newer in ids and older in ids:
            superseded.add(older)
    return tuple(sorted((record for record in eligible if record.get("header", {}).get("record_id") not in superseded), key=_version_sort_key))


def _version_sort_key(record: Mapping[str, Any]) -> tuple[str, str, str, str]:
    header = record.get("header", {})
    return (
        str(header.get("knowledge_date", "")),
        str(header.get("publication_date", "")),
        str(header.get("effective_period_id", "")),
        str(header.get("record_id", "")),
    )


def _series_versions(index: _Indexes, series_id: str) -> tuple[Mapping[str, Any], ...]:
    return tuple(
        sorted(
            (
                row
                for row in index.observations.values()
                if row.get("payload", {}).get("kind") == "GuidanceVersion"
                and row.get("payload", {}).get("guidance_series_id") == series_id
            ),
            key=_version_sort_key,
        )
    )


def _promise_versions(index: _Indexes, promise_id: str) -> tuple[Mapping[str, Any], ...]:
    versions = tuple(
        sorted(
            (
                row
                for row in index.observations.values()
                if row.get("payload", {}).get("kind") == "PromiseVersion"
                and row.get("payload", {}).get("promise_id") == promise_id
            ),
            key=_version_sort_key,
        )
    )
    origins = [row for row in versions if row.get("payload", {}).get("change_kind") == "origin"]
    if len(origins) != 1:
        raise PromiseProgressProjectionError("A Promise history must contain exactly one material origin.")
    for version in versions:
        predecessor = _relation_predecessor(index, version, versions)
        is_origin = version.get("payload", {}).get("change_kind") == "origin"
        if is_origin and predecessor is not None:
            raise PromiseProgressProjectionError("A Promise origin cannot have a predecessor.")
        if not is_origin and predecessor is None:
            raise PromiseProgressProjectionError("A non-origin PromiseVersion requires one explicit predecessor.")
    return versions


def _issues_for(index: _Indexes, identities: Iterable[str], explicit_rule_ids: Iterable[str] = ()) -> tuple[str, ...]:
    identity_set = set(identities)
    explicit = set(explicit_rule_ids)
    result: list[str] = []
    for issue in index.review_issues:
        if (
            str(issue.get("rule_id", "")) in explicit
            or str(issue.get("business_key", "")) in identity_set
            or identity_set.intersection(str(value) for value in issue.get("entity_ids", ()))
            or identity_set.intersection(str(value) for value in issue.get("candidate_record_ids", ()))
        ):
            result.append(str(issue["issue_id"]))
    missing = explicit - {str(issue.get("rule_id", "")) for issue in index.review_issues}
    if missing:
        raise PromiseProgressProjectionError(f"Projection plan expects absent review rules: {sorted(missing)!r}.")
    return _sorted_unique(result)


def _notes(index: _Indexes, occurrence_ids: Iterable[str], *, suffixes: Iterable[str] = ()) -> str:
    _, document_ids = _source_ids(index, occurrence_ids)
    parts: list[str] = []
    for document_id in document_ids:
        document = index.documents[document_id]
        publisher = str(document.get("publisher_id", ""))
        published = str(document.get("publication_date", ""))
        title = str(document.get("title", "")).strip()
        compact = " · ".join(value for value in (publisher, published, title) if value)
        if compact:
            parts.append(compact)
    parts.extend(str(value).strip() for value in suffixes if str(value).strip())
    return "; ".join(dict.fromkeys(parts))


def _horizon_display(index: _Indexes, period_id: str | None, override: str | None = None) -> str:
    if override:
        return override
    if not period_id:
        return "Program; no exact deadline disclosed"
    period = index.periods.get(period_id)
    if period is None:
        return period_id
    fiscal_year = period.get("fiscal_year")
    quarter = period.get("fiscal_quarter")
    if period.get("period_type") == "annual":
        return f"FY{fiscal_year}"
    if quarter:
        return f"FY{fiscal_year} Q{quarter}"
    return period_id


def _stated_in(index: _Indexes, record: Mapping[str, Any]) -> str:
    period_id = str(record.get("header", {}).get("effective_period_id", ""))
    period = index.periods.get(period_id)
    if not period:
        return period_id
    year = period.get("fiscal_year")
    quarter = period.get("fiscal_quarter")
    return f"Q{quarter} {year}" if quarter else f"FY{year}"


def _find_observations(index: _Indexes, selector: Mapping[str, Any], cutoff: str) -> tuple[Mapping[str, Any], ...]:
    return tuple(
        sorted(
            (
                record
                for record in index.observations.values()
                if _matches_payload(record, selector) and _eligible(record, cutoff)
            ),
            key=_version_sort_key,
        )
    )


def _canonical_candidates(index: _Indexes, selector: Mapping[str, Any], cutoff: str) -> tuple[Mapping[str, Any], ...]:
    return tuple(
        record
        for record in _find_observations(index, selector, cutoff)
        if str(record.get("header", {}).get("record_id")) in index.selected_record_ids
    )


def _future_canonical_candidates(index: _Indexes, selector: Mapping[str, Any], cutoff: str) -> tuple[Mapping[str, Any], ...]:
    boundary = _parse_date(cutoff, label="row cutoff")
    return tuple(
        record
        for record in index.observations.values()
        if _matches_payload(record, selector)
        and str(record.get("header", {}).get("record_id")) in index.selected_record_ids
        and _parse_date(str(record.get("header", {}).get("knowledge_date")), label="record knowledge_date") > boundary
    )


def _period_rank(index: _Indexes, record: Mapping[str, Any]) -> tuple[int, str]:
    period_id = str(record.get("header", {}).get("effective_period_id", ""))
    period = index.periods.get(period_id, {})
    return int(period.get("fiscal_ordinal", -1)), period_id


def _select_latest_period_candidate(
    index: _Indexes,
    selector: Mapping[str, Any],
    cutoff: str,
) -> tuple[str, tuple[Mapping[str, Any], ...]]:
    candidates = _canonical_candidates(index, selector, cutoff)
    all_candidates = _find_observations(index, selector, cutoff)
    if not candidates:
        return ("missing_by_cutoff" if _future_canonical_candidates(index, selector, cutoff) else "incompatible" if all_candidates else "missing_by_absence"), ()
    max_rank = max(_period_rank(index, record) for record in candidates)
    terminal = tuple(record for record in candidates if _period_rank(index, record) == max_rank)
    if len(terminal) != 1:
        return "conflicting", terminal
    return "selected", terminal


def _selection_lineage(kind: str, payload: Mapping[str, Any]) -> str:
    return _digest({"kind": kind, **_canonical(payload)})


def _missing_actual(
    *,
    product_id: str,
    business_key: str,
    role_id: str,
    semantic_identity: SemanticIdentity,
    period_id: str | None,
    selection_state: str,
) -> ActualSelection:
    payload = {
        "product_id": product_id,
        "business_key": business_key,
        "role_id": role_id,
        "period_id": period_id,
        "selection_state": selection_state,
        "semantic_identity": semantic_identity.to_dict(),
    }
    lineage = _selection_lineage("actual", payload)
    return ActualSelection(
        actual_id=f"actual-selection:{lineage[:24]}@1",
        actual_role_id=role_id,
        semantic_class=ACTUAL_ROLE_SEMANTIC_CLASSES[role_id],
        selection_state=selection_state,
        canonical_observation_ids=(),
        semantic_identity=semantic_identity,
        effective_or_fiscal_period_id=period_id,
        publication_date=None,
        knowledge_date=None,
        value_form="missing",
        source_occurrence_ids=(),
        source_document_ids=(),
        display_value=MISSING_DISPLAY,
        milestone_state=None,
        selection_method_id="selection:promise-progress:canonical-actual@1",
        lineage_state="missing" if selection_state.startswith("missing") else "blocked",
        lineage_digest=lineage,
    )


def _actual_from_binding(
    index: _Indexes,
    *,
    product_id: str,
    business_key: str,
    binding: Mapping[str, Any] | None,
    cutoff: str,
    default_semantic: SemanticIdentity,
    default_period_id: str | None,
) -> ActualSelection:
    if not binding:
        return _missing_actual(
            product_id=product_id,
            business_key=business_key,
            role_id=ACTUAL_FY_ID,
            semantic_identity=default_semantic,
            period_id=default_period_id,
            selection_state="missing_by_absence",
        )
    role_id = str(binding.get("actual_role_id", ACTUAL_FY_ID))
    if role_id not in CLOSED_ACTUAL_ROLE_IDS:
        raise PromiseProgressProjectionError(f"Unknown Actual role in plan: {role_id!r}.")
    allowed_definitions, allowed_bases, allowed_period_types = _role_semantic_assertion(
        index,
        binding.get("role_semantic_assertion"),
        role_id=role_id,
        role_key="actual_role_id",
        semantic_classes=ACTUAL_ROLE_SEMANTIC_CLASSES,
        label="Actual",
    )
    selectors = tuple(binding.get("component_selectors", ()))
    if not selectors and binding.get("selector"):
        selectors = (binding["selector"],)
    semantic = _semantic_from_config(index, binding.get("semantic_identity", {})) if binding.get("semantic_identity") else default_semantic
    if not selectors:
        return _missing_actual(
            product_id=product_id,
            business_key=business_key,
            role_id=role_id,
            semantic_identity=semantic,
            period_id=default_period_id,
            selection_state="missing_by_absence",
        )
    chosen: list[Mapping[str, Any]] = []
    state = "selected"
    for selector in selectors:
        candidates = _canonical_candidates(index, selector, cutoff)
        all_candidates = _find_observations(index, selector, cutoff)
        if len(candidates) != 1:
            state = (
                "conflicting"
                if len(candidates) > 1
                else "missing_by_cutoff"
                if _future_canonical_candidates(index, selector, cutoff)
                else "incompatible"
                if all_candidates
                else "missing_by_absence"
            )
            break
        chosen.append(candidates[0])
    if state != "selected":
        return _missing_actual(
            product_id=product_id,
            business_key=business_key,
            role_id=role_id,
            semantic_identity=semantic,
            period_id=default_period_id,
            selection_state=state,
        )
    for record in chosen:
        _assert_record_role_semantics(
            index,
            record,
            allowed_definitions=allowed_definitions,
            allowed_bases=allowed_bases,
            allowed_period_types=allowed_period_types,
            label="Actual",
        )
        _require_record_evidence(index, record, label="Selected Actual")
    record_ids = _sorted_unique(str(record["header"]["record_id"]) for record in chosen)
    periods = _sorted_unique(str(record["header"].get("effective_period_id", "")) for record in chosen)
    if len(periods) != 1:
        raise PromiseProgressProjectionError("A labelled Actual composite must use one compatible period.")
    if default_period_id is not None and periods[0] != default_period_id:
        raise PromiseProgressProjectionError("Selected Actual does not match the explicitly requested fiscal period.")
    occurrences = _sorted_unique(
        occurrence
        for record in chosen
        for occurrence in record["header"].get("evidence_occurrence_ids", ())
    )
    occurrences, documents = _source_ids(index, occurrences)
    knowledge_dates = _sorted_unique(str(record["header"]["knowledge_date"]) for record in chosen)
    publication_dates = _sorted_unique(str(record["header"].get("publication_date", "")) for record in chosen)
    if role_id == ACTUAL_COMPOSITE_ID:
        labels = tuple(str(value) for value in binding.get("component_labels", ()))
        if len(labels) != len(chosen):
            raise PromiseProgressProjectionError("A labelled Actual composite requires one label per component.")
        rendered: list[str] = []
        machine: list[dict[str, Any]] = []
        for label, record in zip(labels, chosen, strict=True):
            payload = record["payload"]
            raw = _plain_decimal(payload["value"]["value"])
            display_raw = str(abs(_decimal(raw))) if bool(binding.get("absolute_component_values", False)) else raw
            rendered.append(f"{display_raw} {label}".strip())
            machine.append({"label": label, "record_id": record["header"]["record_id"], "value": raw})
        display_text = str(binding.get("display_template", " / ")).format(*rendered)
        display = DisplayValue("qualitative", display_text, machine)
        value_form = "qualitative-milestone"
    else:
        if len(chosen) != 1:
            raise PromiseProgressProjectionError("Non-composite Actual selection must resolve to one observation.")
        payload = chosen[0]["payload"]
        selected_semantic = _semantic_from_payload(
            index,
            payload,
            str(chosen[0]["header"].get("dimension_set_id", "")),
        )
        if selected_semantic != semantic:
            raise PromiseProgressProjectionError(
                "Selected Actual differs from the reviewed metric, definition, basis, unit or dimensions."
            )
        display = display_value_from_spec(
            payload.get("value"), unit=index.unit_catalog.get(str(payload.get("unit_id"))), currency=payload.get("currency")
        )
        value_form = display.value_form
        semantic = selected_semantic
    milestone_state: MilestoneState | None = None
    milestone_binding = binding.get("milestone_state")
    if role_id == ACTUAL_MILESTONE_ID:
        if milestone_binding is not None:
            if not isinstance(milestone_binding, Mapping):
                raise PromiseProgressProjectionError("A reviewed milestone state must be a closed object.")
            required_milestone_keys = {
                "state",
                "exact_source_text",
                "assessment_method_id",
                "knowledge_date",
                "deadline_or_horizon_id",
                "review_state",
            }
            if set(milestone_binding) != required_milestone_keys:
                raise PromiseProgressProjectionError(
                    "A reviewed milestone state differs from the closed product binding contract."
                )
            exact_source_text = str(milestone_binding["exact_source_text"])
            if exact_source_text != display.display_text:
                raise PromiseProgressProjectionError(
                    "Reviewed milestone state text does not replay from the selected source-backed Actual."
                )
            milestone_knowledge = str(milestone_binding["knowledge_date"])
            if milestone_knowledge != max(knowledge_dates):
                raise PromiseProgressProjectionError(
                    "Reviewed milestone state knowledge date differs from the selected source record."
                )
            milestone_horizon = milestone_binding["deadline_or_horizon_id"]
            if milestone_horizon is not None and str(milestone_horizon) != periods[0]:
                raise PromiseProgressProjectionError(
                    "Reviewed milestone state horizon differs from the selected source period."
                )
            milestone_payload = {
                "state": str(milestone_binding["state"]),
                "source_text": exact_source_text,
                "assessment_method_id": str(milestone_binding["assessment_method_id"]),
                "knowledge_date": milestone_knowledge,
                "deadline_or_horizon_id": None if milestone_horizon is None else str(milestone_horizon),
                "review_state": str(milestone_binding["review_state"]),
                "source_occurrence_ids": occurrences,
                "source_document_ids": documents,
            }
            milestone_state = MilestoneState(
                **milestone_payload,
                lineage_digest=_selection_lineage("milestone-state", milestone_payload),
            )
    elif milestone_binding is not None:
        raise PromiseProgressProjectionError(
            "A reviewed milestone state may bind only a milestone Actual role."
        )
    payload = {
        "product_id": product_id,
        "business_key": business_key,
        "role_id": role_id,
        "record_ids": record_ids,
        "period_id": periods[0],
        "display": display.to_dict(),
    }
    if milestone_state is not None:
        payload["milestone_state"] = milestone_state.to_dict()
    lineage = _selection_lineage("actual", payload)
    return ActualSelection(
        actual_id=f"actual-selection:{lineage[:24]}@1",
        actual_role_id=role_id,
        semantic_class=ACTUAL_ROLE_SEMANTIC_CLASSES[role_id],
        selection_state="selected",
        canonical_observation_ids=record_ids,
        semantic_identity=semantic,
        effective_or_fiscal_period_id=periods[0],
        publication_date=max(publication_dates) if publication_dates else None,
        knowledge_date=max(knowledge_dates) if knowledge_dates else None,
        value_form=value_form,
        source_occurrence_ids=occurrences,
        source_document_ids=documents,
        display_value=display,
        milestone_state=milestone_state,
        selection_method_id="selection:promise-progress:canonical-actual@1",
        lineage_state="accepted",
        lineage_digest=lineage,
    )


def _progress_from_binding(
    index: _Indexes,
    *,
    product_id: str,
    business_key: str,
    binding: Mapping[str, Any] | None,
    cutoff: str,
    governing_target_version_id: str | None = None,
    target_value: Mapping[str, Any] | None = None,
    target_semantic: SemanticIdentity | None = None,
    target_period_or_horizon_id: str | None = None,
) -> ProgressSelection | None:
    if not binding:
        return None
    role_id = str(binding.get("progress_role_id", ""))
    if role_id not in CLOSED_PROGRESS_ROLE_IDS:
        raise PromiseProgressProjectionError(f"Unknown Progress role in plan: {role_id!r}.")
    allowed_definitions, allowed_bases, allowed_period_types = _role_semantic_assertion(
        index,
        binding.get("role_semantic_assertion"),
        role_id=role_id,
        role_key="progress_role_id",
        semantic_classes=PROGRESS_ROLE_SEMANTIC_CLASSES,
        label="Progress",
    )
    selectors = tuple(binding.get("component_selectors", ()))
    if not selectors and binding.get("selector"):
        selectors = (binding["selector"],)
    chosen: list[Mapping[str, Any]] = []
    for selector in selectors:
        if bool(binding.get("latest_period", False)):
            state, candidates = _select_latest_period_candidate(index, selector, cutoff)
            if state != "selected":
                return None
            chosen.extend(candidates)
        else:
            candidates = _canonical_candidates(index, selector, cutoff)
            if len(candidates) != 1:
                return None
            chosen.append(candidates[0])
    if not chosen:
        return None
    for record in chosen:
        _assert_record_role_semantics(
            index,
            record,
            allowed_definitions=allowed_definitions,
            allowed_bases=allowed_bases,
            allowed_period_types=allowed_period_types,
            label="Progress",
        )
        _require_record_evidence(index, record, label="Selected Progress")
    record_ids = _sorted_unique(str(record["header"]["record_id"]) for record in chosen)
    occurrences = _sorted_unique(
        occurrence
        for record in chosen
        for occurrence in record["header"].get("evidence_occurrence_ids", ())
    )
    occurrences, documents = _source_ids(index, occurrences)
    periods = _sorted_unique(str(record["header"].get("effective_period_id", "")) for record in chosen)
    publication_dates = _sorted_unique(str(record["header"].get("publication_date", "")) for record in chosen)
    knowledge_dates = _sorted_unique(str(record["header"].get("knowledge_date", "")) for record in chosen)
    semantic = _semantic_from_config(index, binding.get("semantic_identity", {})) if binding.get("semantic_identity") else _semantic_from_payload(
        index, chosen[-1]["payload"], str(chosen[-1]["header"].get("dimension_set_id", ""))
    )
    if binding.get("semantic_identity"):
        for record in chosen:
            selected_semantic = _semantic_from_payload(
                index,
                record["payload"],
                str(record["header"].get("dimension_set_id", "")),
            )
            if selected_semantic != semantic:
                raise PromiseProgressProjectionError(
                    "Selected Progress differs from the reviewed metric, definition, basis, unit or dimensions."
                )
    labels = tuple(str(value) for value in binding.get("component_labels", ()))
    if labels:
        if len(labels) != len(chosen):
            raise PromiseProgressProjectionError("Progress composite requires one label per selected input.")
        rendered: list[str] = []
        machine: list[dict[str, Any]] = []
        for label, record in zip(labels, chosen, strict=True):
            raw = _plain_decimal(record["payload"]["value"]["value"])
            display_raw = str(abs(_decimal(raw))) if bool(binding.get("absolute_component_values", False)) else raw
            rendered.append(f"{display_raw} {label}".strip())
            machine.append({"label": label, "record_id": record["header"]["record_id"], "value": raw})
        display = DisplayValue("qualitative", str(binding.get("display_template", " / ")).format(*rendered), machine)
    else:
        if len(chosen) != 1:
            raise PromiseProgressProjectionError("Non-composite Progress selection must resolve exactly one input.")
        payload = chosen[0]["payload"]
        base = display_value_from_spec(
            payload.get("value"), unit=index.unit_catalog.get(str(payload.get("unit_id"))), currency=payload.get("currency")
        )
        label = str(binding.get("display_suffix", "")).strip()
        display = DisplayValue(base.value_form, f"{base.display_text} {label}".strip(), base.machine_value)
    target_occurrences: tuple[str, ...] = ()
    target_documents: tuple[str, ...] = ()
    if role_id in {PROGRESS_REMAINING_ID, PROGRESS_DELTA_ID}:
        if labels or len(chosen) != 1:
            raise PromiseProgressProjectionError(
                "Calculated remaining/delta Progress requires exactly one numeric observed input."
            )
        if governing_target_version_id is None or target_value is None or target_semantic is None:
            return None
        target_record = index.observations.get(governing_target_version_id)
        if target_record is None:
            raise PromiseProgressProjectionError("Calculated Progress references a missing governing target version.")
        target_occurrences, target_documents = _record_source(index, target_record)
        if semantic != target_semantic:
            raise PromiseProgressProjectionError(
                "Calculated Progress target and observed input differ in metric, definition, basis, unit or dimensions."
            )
        if not periods or len(periods) != 1 or periods[0] != target_period_or_horizon_id:
            raise PromiseProgressProjectionError(
                "Calculated Progress target and observed input have incompatible periods or horizons."
            )
        if not isinstance(display.machine_value, str):
            raise PromiseProgressProjectionError("Calculated Progress requires an exact numeric observed input.")
        observed = _decimal(display.machine_value)
        direction = str(binding.get("target_direction", ""))
        if role_id == PROGRESS_REMAINING_ID:
            if direction != "upward-monotonic":
                raise PromiseProgressProjectionError(
                    "Remaining amount requires the reviewed upward-monotonic target direction."
                )
            target_kind = str(target_value.get("kind", ""))
            if target_kind == "exact":
                target_floor = _decimal(target_value.get("value"))
            elif target_kind == "bound" and target_value.get("operator") == "gte":
                target_floor = _decimal(target_value.get("value"))
            else:
                return None
            calculated = max(target_floor - observed, Decimal("0"))
            display = display_value_from_spec(
                {"kind": "exact", "value": _plain_decimal(calculated)},
                unit=index.unit_catalog.get(str(semantic.unit_id)),
                currency=chosen[0]["payload"].get("currency"),
            )
        else:
            if direction not in {"higher", "lower"}:
                raise PromiseProgressProjectionError(
                    "Delta to target requires a reviewed favorable target direction."
                )
            if target_value.get("kind") != "exact":
                return None
            calculated = observed - _decimal(target_value.get("value"))
            display = display_value_from_spec(
                {"kind": "exact", "value": _plain_decimal(calculated)},
                unit=index.unit_catalog.get(str(semantic.unit_id)),
                currency=chosen[0]["payload"].get("currency"),
            )
        label = str(binding.get("display_suffix", "")).strip()
        if label:
            display = DisplayValue(
                display.value_form,
                f"{display.display_text} {label}",
                display.machine_value,
            )
        occurrences = _sorted_unique((*occurrences, *target_occurrences))
        documents = _sorted_unique((*documents, *target_documents))
        publication_dates = _sorted_unique(
            (*publication_dates, str(target_record["header"].get("publication_date", "")))
        )
        knowledge_dates = _sorted_unique(
            (*knowledge_dates, str(target_record["header"].get("knowledge_date", "")))
        )
        record_ids = _sorted_unique((*record_ids, governing_target_version_id))
    payload = {
        "product_id": product_id,
        "business_key": business_key,
        "role_id": role_id,
        "record_ids": record_ids,
        "periods": periods,
        "display": display.to_dict(),
        "cutoff": cutoff,
    }
    if role_id in {PROGRESS_REMAINING_ID, PROGRESS_DELTA_ID}:
        payload["governing_target_version_id"] = governing_target_version_id
        payload["target_value"] = target_value
    lineage = _selection_lineage("progress", payload)
    method_by_role = {
        PROGRESS_FY_ID: "selection:promise-progress:canonical-progress-fy-actual@1",
        PROGRESS_YTD_ID: "selection:promise-progress:canonical-progress-ytd@1",
        PROGRESS_CUMULATIVE_ID: "selection:promise-progress:canonical-progress-cumulative@1",
        PROGRESS_RUN_RATE_ID: "selection:promise-progress:canonical-progress-run-rate@1",
        PROGRESS_REALIZED_ID: "selection:promise-progress:canonical-progress-realized-period@1",
        PROGRESS_IDENTIFIED_ID: "selection:promise-progress:canonical-progress-identified-initiated@1",
        PROGRESS_REMAINING_ID: "calculation:promise-progress:remaining-amount@1",
        PROGRESS_DELTA_ID: "calculation:promise-progress:delta-to-target@1",
        PROGRESS_MILESTONE_ID: "selection:promise-progress:milestone-state@1",
        PROGRESS_DIRECTIONAL_ID: "assessment:promise-progress:directional-progress@1",
    }
    return ProgressSelection(
        progress_id=f"progress-selection:{lineage[:24]}@1",
        progress_role_id=role_id,
        semantic_class=PROGRESS_ROLE_SEMANTIC_CLASSES[role_id],
        canonical_input_ids=record_ids,
        governing_target_version_id=(
            governing_target_version_id
            if role_id in {PROGRESS_REMAINING_ID, PROGRESS_DELTA_ID}
            else None
        ),
        semantic_identity=semantic,
        period_or_horizon_id=(
            target_period_or_horizon_id
            if role_id in {PROGRESS_REMAINING_ID, PROGRESS_DELTA_ID}
            else periods[-1] if len(periods) == 1 else None
        ),
        method_id=method_by_role[role_id],
        ui_as_of_date=cutoff,
        publication_dates=publication_dates,
        knowledge_dates=knowledge_dates,
        display_value=display,
        review_state="accepted",
        source_occurrence_ids=occurrences,
        source_document_ids=documents,
        lineage_digest=lineage,
    )


def assess_status(
    *,
    product_id: str,
    row_key: str,
    rule_id: str,
    target_version_id: str | None,
    target_value: Mapping[str, Any] | None,
    actual: ActualSelection | None,
    progress: ProgressSelection | None,
    ui_as_of_date: str,
    horizon_closed: bool,
    target_period_or_horizon_id: str | None = None,
    review_issue_ids: Iterable[str] = (),
    withdrawn: bool = False,
    favorable_direction: str | None = None,
) -> StatusAssessment:
    """Replay one closed Status rule over typed target, Actual and Progress inputs."""

    if rule_id not in CLOSED_STATUS_RULE_IDS:
        raise PromiseProgressProjectionError(f"Unknown assessment rule {rule_id!r}.")
    if favorable_direction not in {None, "higher", "lower"}:
        raise PromiseProgressProjectionError(f"Unknown favorable direction {favorable_direction!r}.")
    issue_ids = _sorted_unique(review_issue_ids)
    status = "needs_review"
    result = "insufficient-or-conflicting-evidence"
    explanation = "A safe status cannot be assessed from compatible canonical inputs."
    if withdrawn and rule_id == STATUS_OPEN_ID:
        status, result, explanation = "withdrawn", "explicit-withdrawal", "The governing guidance version is explicitly withdrawn."
    elif rule_id == STATUS_OPEN_ID:
        if target_version_id is not None and not issue_ids:
            status, result, explanation = "open", "governing-guidance-active", "One canonical active guidance version governs at the UI as-of date."
    elif rule_id == STATUS_APPROX_ID:
        tolerance = target_value.get("tolerance") if target_value else None
        if tolerance is None:
            explanation = "The approximate target has no source-backed tolerance; equality cannot imply completion."
        elif actual and actual.selection_state == "selected" and not issue_ids and horizon_closed:
            target = _decimal(target_value["value"])
            tol = _decimal(tolerance)
            machine = actual.display_value.machine_value
            actual_value = _decimal(machine if isinstance(machine, str) else machine.get("value"))
            status = "hit" if target - tol <= actual_value <= target + tol else "missed"
            result = "within-tolerance" if status == "hit" else "outside-tolerance"
            explanation = "The final compatible Actual was compared with the explicit approximation tolerance."
    elif rule_id in {STATUS_POINT_ID, STATUS_RANGE_ID, STATUS_MIN_ID, STATUS_MAX_ID, STATUS_CUMULATIVE_ID}:
        if actual and actual.selection_state == "selected" and not issue_ids and horizon_closed and target_value:
            machine = actual.display_value.machine_value
            if isinstance(machine, str):
                actual_value = _decimal(machine)
                kind = str(target_value.get("kind"))
                if rule_id in {STATUS_POINT_ID, STATUS_CUMULATIVE_ID} and kind == "exact":
                    target = _decimal(target_value["value"])
                    if actual_value == target:
                        status = "hit"
                    elif favorable_direction == "higher":
                        status = "beat" if actual_value > target else "missed"
                    elif favorable_direction == "lower":
                        status = "beat" if actual_value < target else "missed"
                elif rule_id == STATUS_RANGE_ID and kind == "range":
                    low, high = _decimal(target_value["low"]), _decimal(target_value["high"])
                    if low <= actual_value <= high:
                        status = "hit"
                    elif favorable_direction == "higher":
                        status = "beat" if actual_value > high else "missed"
                    elif favorable_direction == "lower":
                        status = "beat" if actual_value < low else "missed"
                elif rule_id == STATUS_MIN_ID and kind == "bound" and target_value.get("operator") in {"gt", "gte"}:
                    bound = _decimal(target_value["value"])
                    if target_value["operator"] == "gte" and actual_value == bound:
                        status = "hit"
                    elif actual_value > bound:
                        status = "beat"
                    else:
                        status = "missed"
                elif rule_id == STATUS_MAX_ID and kind == "bound" and target_value.get("operator") in {"lt", "lte"}:
                    bound = _decimal(target_value["value"])
                    if target_value["operator"] == "lte" and actual_value == bound:
                        status = "hit"
                    elif actual_value < bound:
                        status = "beat"
                    else:
                        status = "missed"
                if status != "needs_review":
                    result = "terminal-compatible-comparison"
                    explanation = "The closed target rule replayed one compatible final Actual."
                else:
                    explanation = "The target comparison requires a reviewed favorable direction."
        elif not horizon_closed and target_version_id and not issue_ids:
            status, result, explanation = "open", "horizon-open", "The target horizon remains open without a reviewed trajectory assessment."
    elif rule_id == STATUS_RUN_RATE_ID:
        if issue_ids:
            explanation = "Run-rate, realized and/or gross/net bases are unresolved; the evidence cannot prove target attainment."
        elif progress and target_value and horizon_closed and isinstance(progress.display_value.machine_value, str):
            progress_value = _decimal(progress.display_value.machine_value)
            target_kind = str(target_value.get("kind"))
            met = False
            if target_kind == "exact":
                met = progress_value >= _decimal(target_value["value"])
            elif target_kind == "range":
                met = _decimal(target_value["low"]) <= progress_value <= _decimal(target_value["high"])
            elif target_kind == "bound" and target_value.get("operator") in {"gt", "gte"}:
                bound = _decimal(target_value["value"])
                met = progress_value >= bound if target_value["operator"] == "gte" else progress_value > bound
            status = "hit" if met else "missed"
            result = "compatible-run-rate-terminal-comparison"
            explanation = "A compatible source-backed run rate was compared with an explicit run-rate objective at its horizon."
        elif progress and target_value:
            status, result, explanation = "open", "compatible-run-rate-open-horizon", "Compatible run-rate evidence exists, but no reviewed trajectory rule establishes On track."
    elif rule_id == STATUS_MILESTONE_ID:
        milestone = actual.milestone_state if actual and actual.selection_state == "selected" else None
        if milestone is None:
            explanation = "No reviewed source-backed milestone-state assessment is available."
        elif issue_ids or milestone.review_state != "accepted":
            explanation = "Milestone evidence is conflicting or not accepted for deterministic status assessment."
        elif milestone.deadline_or_horizon_id != target_period_or_horizon_id:
            explanation = "Reviewed milestone evidence and the governing target use different deadline or horizon identities."
        elif milestone.state == "completed":
            status = "completed"
            result = "reviewed-source-backed-completion"
            explanation = "Accepted source evidence explicitly reports completion of the reviewed milestone."
        elif milestone.state == "withdrawn":
            status = "withdrawn"
            result = "reviewed-source-backed-withdrawal"
            explanation = "Accepted source evidence explicitly reports withdrawal of the milestone commitment."
        elif milestone.state == "failed":
            status = "missed"
            result = "reviewed-source-backed-failure"
            explanation = "Accepted source evidence explicitly reports failure of the milestone."
        elif milestone.state in {"in_progress", "not_started"} and not horizon_closed and target_version_id:
            status = "open"
            result = f"reviewed-milestone-{milestone.state.replace('_', '-')}"
            explanation = "The reviewed milestone state remains open before its target horizon."
        elif milestone.state in {"in_progress", "not_started"} and horizon_closed:
            status = "missed"
            result = "reviewed-unmet-milestone-at-closed-horizon"
            explanation = "Compatible reviewed evidence shows the milestone unmet after its target horizon closed."
        else:
            explanation = "The reviewed milestone state is unknown and cannot support a terminal status."
    elif rule_id == STATUS_QUALITATIVE_ID:
        if not horizon_closed and target_version_id and not issue_ids:
            status, result, explanation = "open", "qualitative-commitment-open", "The qualitative commitment remains active without terminal reviewed evidence."
    elif rule_id == STATUS_BASIS_ID:
        status, result, explanation = "basis_dependent", "unresolved-basis-bridge", "The visible conclusion depends on an unresolved definition or basis bridge."
    inputs = _sorted_unique(
        tuple(actual.canonical_observation_ids if actual else ())
        + tuple(progress.canonical_input_ids if progress else ())
        + ((target_version_id,) if target_version_id else ())
    )
    role = progress.progress_role_id if progress else (actual.actual_role_id if actual else None)
    identity_payload = {
        "product_id": product_id,
        "row_key": row_key,
        "rule_id": rule_id,
        "target_version_id": target_version_id,
        "inputs": inputs,
        "ui_as_of_date": ui_as_of_date,
        "status": status,
        "issues": issue_ids,
        "favorable_direction": favorable_direction,
    }
    if rule_id == STATUS_MILESTONE_ID:
        identity_payload["target_period_or_horizon_id"] = target_period_or_horizon_id
    lineage = _selection_lineage("status", identity_payload)
    return StatusAssessment(
        status_assessment_id=f"status-assessment:{lineage[:24]}@1",
        status_code=status,
        visible_label=STATUS_LABELS[status],
        assessment_rule_id=rule_id,
        canonical_input_ids=inputs,
        target_version_id=target_version_id,
        actual_or_progress_role_id=role,
        ui_as_of_date=ui_as_of_date,
        assessment_result=result,
        review_state="needs_review" if status in {"needs_review", "basis_dependent"} else "accepted",
        explanation=explanation,
        review_issue_ids=issue_ids,
        lineage_digest=lineage,
    )


@dataclass
class _ProductAccumulator:
    index: _Indexes
    config: Mapping[str, Any]
    product_id: str
    actuals: dict[str, ActualSelection] = field(default_factory=dict)
    progress_values: dict[str, ProgressSelection] = field(default_factory=dict)
    statuses: dict[str, StatusAssessment] = field(default_factory=dict)

    def retain_actual(self, value: ActualSelection) -> ActualSelection:
        self.actuals[value.actual_id] = value
        return value

    def retain_progress(self, value: ProgressSelection | None) -> ProgressSelection | None:
        if value is not None:
            self.progress_values[value.progress_id] = value
        return value

    def retain_status(self, value: StatusAssessment) -> StatusAssessment:
        self.statuses[value.status_assessment_id] = value
        return value


def _row_id(product_id: str, block_id: str, business_key: str) -> str:
    product_digest = _short_digest(product_id, 12)
    business_digest = _short_digest({"block": block_id, "business_key": business_key}, 20)
    block_slug = {
        SCORECARD_BLOCK_ID: "scorecard",
        ANNUAL_BLOCK_ID: "annual",
        OPEN_BLOCK_ID: "open",
        TIMELINE_BLOCK_ID: "timeline",
    }[block_id]
    return f"promise-progress-row:{product_digest}:{block_slug}:{business_digest}@1"


def _field(
    *,
    product_id: str,
    block_id: str,
    row_id: str,
    row_number: int,
    field_role: str,
    display_value: DisplayValue,
    canonical_record_ids: Iterable[str] = (),
    target_version_id: str | None = None,
    actual: ActualSelection | None = None,
    progress: ProgressSelection | None = None,
    status: StatusAssessment | None = None,
    semantic_identity: SemanticIdentity = EMPTY_SEMANTIC_IDENTITY,
    period_or_horizon_id: str | None = None,
    publication_date: str | None = None,
    event_date: str | None = None,
    ui_as_of_date: str,
    knowledge_dates: Iterable[str] = (),
    source_occurrence_ids: Iterable[str] = (),
    source_document_ids: Iterable[str] = (),
    method_id: str,
    review_issue_ids: Iterable[str] = (),
    parity_exception_ids: Iterable[str] = (),
    lineage_state: str = "accepted",
) -> ProductField:
    layout = {role: (anchor, display_range) for role, anchor, display_range in BLOCK_FIELD_LAYOUT[block_id]}
    if field_role not in layout:
        raise PromiseProgressProjectionError(f"Field role {field_role!r} is not valid for block {block_id!r}.")
    anchor, display_range = layout[field_role]
    field_id = f"{row_id}:field:{field_role.replace('_', '-')}@1"
    canonical_ids = _sorted_unique(canonical_record_ids)
    occurrences = _sorted_unique(source_occurrence_ids)
    documents = _sorted_unique(source_document_ids)
    issues = _sorted_unique(review_issue_ids)
    exceptions = _sorted_unique(parity_exception_ids)
    knowledge = _sorted_unique(knowledge_dates)
    lineage_payload = {
        "product_id": product_id,
        "block_id": block_id,
        "row_id": row_id,
        "field_id": field_id,
        "field_role": field_role,
        "anchor_cell": f"{anchor}{row_number}",
        "display_range": f"{display_range.split(':')[0]}{row_number}" if ":" not in display_range else f"{display_range.split(':')[0]}{row_number}:{display_range.split(':')[1]}{row_number}",
        "display": display_value.to_dict(),
        "canonical_record_ids": canonical_ids,
        "target_version_id": target_version_id,
        "actual_id": actual.actual_id if actual else None,
        "progress_id": progress.progress_id if progress else None,
        "status_id": status.status_assessment_id if status else None,
        "semantic_identity": semantic_identity.to_dict(),
        "period_or_horizon_id": period_or_horizon_id,
        "ui_as_of_date": ui_as_of_date,
        "knowledge_dates": knowledge,
        "source_occurrence_ids": occurrences,
        "source_document_ids": documents,
        "method_id": method_id,
        "review_issue_ids": issues,
        "parity_exception_ids": exceptions,
    }
    return ProductField(
        product_id=product_id,
        block_id=block_id,
        row_id=row_id,
        field_id=field_id,
        field_role=field_role,
        anchor_cell=f"{anchor}{row_number}",
        display_range=f"{display_range.split(':')[0]}{row_number}" if ":" not in display_range else f"{display_range.split(':')[0]}{row_number}:{display_range.split(':')[1]}{row_number}",
        display_value=display_value,
        canonical_record_ids=canonical_ids,
        target_version_id=target_version_id,
        actual_selection_id=actual.actual_id if actual else None,
        actual_observation_id=(actual.canonical_observation_ids[0] if actual and len(actual.canonical_observation_ids) == 1 else None),
        progress_selection_id=progress.progress_id if progress else None,
        progress_observation_ids=(
            tuple(
                identity
                for identity in progress.canonical_input_ids
                if identity != progress.governing_target_version_id
            )
            if progress
            else ()
        ),
        status_assessment_id=status.status_assessment_id if status else None,
        semantic_identity=semantic_identity,
        period_or_horizon_id=period_or_horizon_id,
        publication_date=publication_date,
        event_date=event_date,
        ui_as_of_date=ui_as_of_date,
        knowledge_dates=knowledge,
        source_document_ids=documents,
        source_occurrence_ids=occurrences,
        selection_or_calculation_method_id=method_id,
        review_issue_ids=issues,
        parity_exception_ids=exceptions,
        lineage_state=lineage_state,
        lineage_digest=_selection_lineage("field", lineage_payload),
    )


def _row(
    *,
    product_id: str,
    block_id: str,
    business_key: str,
    row_variant: str,
    business_order: int,
    visible_sheet_row: int,
    ui_as_of_date: str,
    canonical_series_or_program_id: str | None,
    canonical_version_id: str | None,
    fields: Sequence[ProductField],
    review_issue_ids: Iterable[str],
    parity_exception_ids: Iterable[str],
) -> ProductRow:
    row_id = _row_id(product_id, block_id, business_key)
    if any(field.row_id != row_id for field in fields):
        raise PromiseProgressProjectionError("Field row identity was not derived from the same business key.")
    issues = _sorted_unique(review_issue_ids)
    exceptions = _sorted_unique(parity_exception_ids)
    lineage = _selection_lineage(
        "row",
        {
            "product_id": product_id,
            "block_id": block_id,
            "row_id": row_id,
            "variant": row_variant,
            "business_order": business_order,
            "visible_sheet_row": visible_sheet_row,
            "ui_as_of_date": ui_as_of_date,
            "series_or_program": canonical_series_or_program_id,
            "version": canonical_version_id,
            "field_ids": [field.field_id for field in fields],
            "issues": issues,
            "exceptions": exceptions,
        },
    )
    return ProductRow(
        product_id=product_id,
        block_id=block_id,
        row_id=row_id,
        row_variant=row_variant,
        business_order=business_order,
        visible_sheet_row=visible_sheet_row,
        ui_as_of_date=ui_as_of_date,
        canonical_series_or_program_id=canonical_series_or_program_id,
        canonical_version_id=canonical_version_id,
        fields=tuple(fields),
        review_issue_ids=issues,
        parity_exception_ids=exceptions,
        lineage_digest=lineage,
    )


def _config_bindings(index: _Indexes, config: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    for collection in ("guidance_bindings", "promise_bindings"):
        for binding in config.get(collection, ()):
            entity_id, _ = _resolve_entity(index, binding.get("entity_selector", {}))
            if entity_id in result:
                raise PromiseProgressProjectionError(f"More than one projection binding targets entity {entity_id!r}.")
            result[entity_id] = binding
    return result


def _configured_order(index: _Indexes, config: Mapping[str, Any]) -> dict[str, int]:
    result: dict[str, int] = {}
    for ordinal, item in enumerate(config.get("business_order", ()), start=1):
        if isinstance(item, Mapping) and "selector" in item:
            selector = item["selector"]
            configured_ordinal = int(item.get("order", ordinal))
        else:
            selector = item
            configured_ordinal = ordinal
        entity_id, _ = _resolve_entity(index, selector)
        if entity_id in result:
            raise PromiseProgressProjectionError("Business ordering cannot contain the same economic identity twice.")
        if configured_ordinal < 1:
            raise PromiseProgressProjectionError("Business ordering values must be positive integers.")
        if configured_ordinal in result.values():
            raise PromiseProgressProjectionError("Business ordering values must be unique.")
        result[entity_id] = configured_ordinal
    return result


def _metric_label(index: _Indexes, entity: Mapping[str, Any], binding: Mapping[str, Any] | None = None) -> str:
    if binding and binding.get("metric_label"):
        return str(binding["metric_label"])
    payload = entity.get("payload", {})
    metric_id = str(payload.get("metric_id", ""))
    if metric_id:
        return index.metric_names.get(metric_id, metric_id)
    subject = str(payload.get("promise_subject_id", ""))
    return subject


def _version_value(index: _Indexes, entity: Mapping[str, Any], record: Mapping[str, Any]) -> DisplayValue:
    payload = record.get("payload", {})
    entity_payload = entity.get("payload", {})
    spec = payload.get("value") if payload.get("kind") == "GuidanceVersion" else payload.get("target")
    unit_id = entity_payload.get("unit_id")
    unit = index.unit_catalog.get(str(unit_id))
    currency = entity_payload.get("currency")
    return display_value_from_spec(spec, unit=unit, currency=currency)


def _relation_predecessor(index: _Indexes, record: Mapping[str, Any], candidates: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    record_id = str(record.get("header", {}).get("record_id", ""))
    payload = record.get("payload", {})
    previous_id = payload.get("previous_version_id")
    candidate_map = {str(row.get("header", {}).get("record_id", "")): row for row in candidates}
    if previous_id:
        previous = candidate_map.get(str(previous_id))
        if previous is None:
            raise PromiseProgressProjectionError("PromiseVersion predecessor is absent from its material history.")
        return previous
    predecessors = {
        str(relation.get("to_record_id"))
        for relation in index.relations
        if str(relation.get("from_record_id")) == record_id
        and relation.get("relation_type") in {"supersedes", "corrects", "reaffirms", "corroborates"}
        and str(relation.get("to_record_id")) in candidate_map
    }
    if not predecessors:
        return None
    if len(predecessors) == 1:
        return candidate_map[next(iter(predecessors))]
    # A target update may reference both a governing target and an intervening
    # reaffirmation. Resolve only when the explicit predecessor subgraph has one
    # terminal node; publication recency and list order are never tie-breakers.
    superseded_predecessors = {
        str(relation.get("to_record_id"))
        for relation in index.relations
        if str(relation.get("from_record_id")) in predecessors
        and str(relation.get("to_record_id")) in predecessors
        and relation.get("relation_type") in {"supersedes", "corrects", "reaffirms", "corroborates"}
    }
    terminals = predecessors - superseded_predecessors
    if len(terminals) != 1:
        raise PromiseProgressProjectionError("Version history has multiple unresolved explicit predecessors.")
    return candidate_map[next(iter(terminals))]


def classify_timeline_change(
    current: Mapping[str, Any] | None,
    previous: Mapping[str, Any] | None,
    *,
    current_semantic: SemanticIdentity,
    previous_semantic: SemanticIdentity,
) -> str:
    """Classify one comparable timeline transition from typed economics."""

    if current_semantic != previous_semantic:
        same_economic_axis = (
            current_semantic.metric_id == previous_semantic.metric_id
            and current_semantic.unit_id == previous_semantic.unit_id
            and current_semantic.dimensions == previous_semantic.dimensions
        )
        if same_economic_axis and (
            current_semantic.definition_id != previous_semantic.definition_id
            or current_semantic.basis_id != previous_semantic.basis_id
        ):
            return "basis-change"
        return "unresolved-comparison"
    if current is None or previous is None or current.get("kind") != previous.get("kind"):
        return "unresolved-comparison"
    kind = current.get("kind")
    if kind == "range":
        current_pair = (_decimal(current["low"]), _decimal(current["high"]))
        prior_pair = (_decimal(previous["low"]), _decimal(previous["high"]))
        if current_pair == prior_pair:
            return "reaffirmation"
        if current_pair[0] >= prior_pair[0] and current_pair[1] >= prior_pair[1] and current_pair != prior_pair:
            return "target-increase"
        if current_pair[0] <= prior_pair[0] and current_pair[1] <= prior_pair[1] and current_pair != prior_pair:
            return "target-decrease"
    if kind in {"exact", "approximate"}:
        current_value, previous_value = _decimal(current["value"]), _decimal(previous["value"])
        if current_value == previous_value:
            return "reaffirmation"
        return "target-increase" if current_value > previous_value else "target-decrease"
    return "update"


def _change_type(
    record: Mapping[str, Any],
    predecessor: Mapping[str, Any] | None,
    *,
    current_semantic: SemanticIdentity,
    previous_semantic: SemanticIdentity | None = None,
) -> str:
    payload = record.get("payload", {})
    kind = payload.get("version_kind") if payload.get("kind") == "GuidanceVersion" else payload.get("change_kind")
    if kind == "origin":
        return "origin"
    if kind == "reaffirmation":
        return "reaffirmation"
    if kind in {"withdrawal", "withdrawn"}:
        return "withdrawal"
    current_value = payload.get("value") if payload.get("kind") == "GuidanceVersion" else payload.get("target")
    previous_payload = predecessor.get("payload", {}) if predecessor else {}
    previous_value = previous_payload.get("value") if previous_payload.get("kind") == "GuidanceVersion" else previous_payload.get("target")
    return (
        classify_timeline_change(
            current_value,
            previous_value,
            current_semantic=current_semantic,
            previous_semantic=previous_semantic or current_semantic,
        )
        if predecessor
        else "update"
    )


def _period_contains_annual(index: _Indexes, entity: Mapping[str, Any], years: set[int]) -> bool:
    period_id = str(entity.get("payload", {}).get("horizon_period_id", ""))
    period = index.periods.get(period_id)
    return bool(period and period.get("period_type") == "annual" and int(period.get("fiscal_year")) in years)


def _latest_ended_quarter(index: _Indexes, publication_date: str, fiscal_year: int) -> int | None:
    publication = _parse_date(publication_date, label="version publication_date")
    candidates = [
        period
        for period in index.periods.values()
        if period.get("period_type") == "quarter"
        and int(period.get("fiscal_year")) == fiscal_year
        and _parse_date(str(period.get("end_date")), label="period end_date") <= publication
    ]
    if not candidates:
        return None
    max_end = max(str(period.get("end_date")) for period in candidates)
    matches = [period for period in candidates if str(period.get("end_date")) == max_end]
    if len(matches) != 1:
        raise PromiseProgressProjectionError("Guidance display bucket does not resolve to one reviewed ended quarter.")
    return int(matches[0]["fiscal_quarter"])


_ANNUAL_BUCKET_ORDER = {
    "initial_guide": 0,
    "q1_guide": 1,
    "q2_guide": 2,
    "q3_guide": 3,
    "q4_guide": 4,
}


def _reviewed_bucket_override(
    index: _Indexes,
    *,
    entity_id: str,
    version: Mapping[str, Any],
    override: Any,
    fiscal_year: int,
) -> str:
    if not isinstance(override, Mapping):
        raise PromiseProgressProjectionError(
            "A reviewed guidance bucket override must carry typed event and version identity."
        )
    required = {"bucket_id", "version_id_sha256", "reporting_event"}
    if set(override) != required:
        raise PromiseProgressProjectionError(
            "A reviewed guidance bucket override differs from the closed chronology contract."
        )
    bucket = str(override["bucket_id"])
    if bucket not in {"q1_guide", "q2_guide", "q3_guide", "q4_guide"}:
        raise PromiseProgressProjectionError(f"Unknown reviewed guidance bucket override {bucket!r}.")
    record_id = str(version["header"]["record_id"])
    _require_sha(str(override["version_id_sha256"]), label="bucket version_id_sha256")
    if hashlib.sha256(record_id.encode("utf-8")).hexdigest() != override["version_id_sha256"]:
        raise PromiseProgressProjectionError(
            "Reviewed guidance bucket override does not identify the exact GuidanceVersion."
        )
    payload = version.get("payload", {})
    owner_id = payload.get("guidance_series_id") or payload.get("promise_id")
    if str(owner_id) != entity_id:
        raise PromiseProgressProjectionError(
            "Reviewed guidance bucket override crosses its typed economic series or program."
        )
    event = override["reporting_event"]
    if not isinstance(event, Mapping) or set(event) != {
        "event_type",
        "fiscal_year",
        "fiscal_quarter",
        "source_document_id_sha256",
    }:
        raise PromiseProgressProjectionError(
            "Reviewed guidance bucket override lacks a closed fiscal reporting-event identity."
        )
    quarter = int(event["fiscal_quarter"])
    if event["event_type"] != "fiscal-quarter-reporting-event" or quarter not in {1, 2, 3, 4}:
        raise PromiseProgressProjectionError("Guidance override reporting-event identity is unsupported.")
    if int(event["fiscal_year"]) != fiscal_year or bucket != f"q{quarter}_guide":
        raise PromiseProgressProjectionError(
            "Guidance bucket differs from its reviewed fiscal reporting-event identity."
        )
    document_sha = str(event["source_document_id_sha256"])
    _require_sha(document_sha, label="reporting event source_document_id_sha256")
    _, source_documents = _record_source(index, version)
    if document_sha not in {
        hashlib.sha256(document_id.encode("utf-8")).hexdigest()
        for document_id in source_documents
    }:
        raise PromiseProgressProjectionError(
            "Guidance reporting-event source is not evidence for the selected GuidanceVersion."
        )
    return bucket


def _version_is_descendant(
    index: _Indexes,
    current: Mapping[str, Any],
    ancestor: Mapping[str, Any],
    versions: Sequence[Mapping[str, Any]],
) -> bool:
    ancestor_id = str(ancestor["header"]["record_id"])
    cursor: Mapping[str, Any] | None = current
    visited: set[str] = set()
    while cursor is not None:
        cursor_id = str(cursor["header"]["record_id"])
        if cursor_id == ancestor_id:
            return True
        if cursor_id in visited:
            raise PromiseProgressProjectionError("Guidance version relationships contain a cycle.")
        visited.add(cursor_id)
        cursor = _relation_predecessor(index, cursor, versions)
    return False


def _validate_bucket_sequence(
    index: _Indexes,
    versions: Sequence[Mapping[str, Any]],
    selected: Mapping[str, Mapping[str, Any]],
    cutoff: str,
) -> None:
    ordered = [
        (bucket, selected[bucket])
        for bucket in _ANNUAL_BUCKET_ORDER
        if bucket in selected
    ]
    version_positions = {
        str(version["header"]["record_id"]): position
        for position, version in enumerate(sorted(versions, key=_version_sort_key))
    }
    previous_bucket: str | None = None
    previous: Mapping[str, Any] | None = None
    for bucket, version in ordered:
        if not _eligible(version, cutoff):
            raise PromiseProgressProjectionError("Guidance bucket contains future knowledge.")
        if previous is not None:
            previous_header = previous["header"]
            header = version["header"]
            if _parse_date(str(header["knowledge_date"]), label="bucket knowledge_date") < _parse_date(
                str(previous_header["knowledge_date"]), label="previous bucket knowledge_date"
            ):
                raise PromiseProgressProjectionError(
                    "Annual guidance buckets have decreasing knowledge dates."
                )
            if _parse_date(str(header["publication_date"]), label="bucket publication_date") < _parse_date(
                str(previous_header["publication_date"]), label="previous bucket publication_date"
            ):
                current_id = str(header["record_id"])
                previous_id = str(previous_header["record_id"])
                correction_path = any(
                    relation.get("relation_type") == "corrects"
                    and str(relation.get("from_record_id")) == current_id
                    and str(relation.get("to_record_id")) == previous_id
                    for relation in index.relations
                )
                if not correction_path:
                    raise PromiseProgressProjectionError(
                        "Annual guidance buckets have decreasing publication dates without an explicit correction."
                    )
            current_position = version_positions[str(header["record_id"])]
            previous_position = version_positions[str(previous_header["record_id"])]
            if current_position <= previous_position:
                raise PromiseProgressProjectionError(
                    "A later GuidanceVersion is assigned to an earlier annual progression bucket."
                )
            if not _version_is_descendant(index, version, previous, versions):
                raise PromiseProgressProjectionError(
                    f"Guidance buckets {previous_bucket!r} and {bucket!r} do not follow the explicit version chain."
                )
        previous_bucket, previous = bucket, version


def _bucket_versions(
    index: _Indexes,
    entity: Mapping[str, Any],
    versions: Sequence[Mapping[str, Any]],
    cutoff: str,
    binding: Mapping[str, Any] | None,
) -> dict[str, Mapping[str, Any]]:
    period = index.periods[str(entity["payload"]["horizon_period_id"])]
    fiscal_year = int(period["fiscal_year"])
    buckets: dict[str, list[Mapping[str, Any]]] = {"initial_guide": [], "q1_guide": [], "q2_guide": [], "q3_guide": [], "q4_guide": []}
    for version in versions:
        if not _eligible(version, cutoff):
            continue
        kind = str(version["payload"].get("version_kind", ""))
        if kind == "origin":
            bucket = "initial_guide"
        else:
            publication_date = str(version["header"]["publication_date"])
            override = binding.get("bucket_overrides", {}).get(publication_date) if binding else None
            if override is not None:
                bucket = _reviewed_bucket_override(
                    index,
                    entity_id=str(entity["header"]["entity_id"]),
                    version=version,
                    override=override,
                    fiscal_year=fiscal_year,
                )
            else:
                quarter = _latest_ended_quarter(index, publication_date, fiscal_year)
                if quarter is None:
                    raise PromiseProgressProjectionError("A non-origin annual guidance version has no unique reviewed display quarter.")
                bucket = f"q{quarter}_guide"
        buckets[bucket].append(version)
    result: dict[str, Mapping[str, Any]] = {}
    for bucket, candidates in buckets.items():
        if not candidates:
            continue
        terminal = _terminal_versions(index, candidates, cutoff)
        if len(terminal) != 1:
            raise PromiseProgressProjectionError(f"Guidance display bucket {bucket!r} has {len(terminal)} unresolved terminal versions.")
        result[bucket] = terminal[0]
    _validate_bucket_sequence(index, versions, result, cutoff)
    return result


def _scorecard_block(acc: _ProductAccumulator) -> ManagementCredibilityScorecardBlock:
    rows: list[ProductRow] = []
    cutoff = str(acc.config["ui_as_of_date"])
    categories = tuple(acc.config.get("scorecard_categories", SCORECARD_CATEGORIES))
    if len(categories) != 5:
        raise PromiseProgressProjectionError("The locked scorecard preserves exactly five legacy category rows.")
    for order, (category, row_number) in enumerate(zip(categories, range(5, 10), strict=True), start=1):
        business_key = f"scorecard:{category}"
        row_id = _row_id(acc.product_id, SCORECARD_BLOCK_ID, business_key)
        exception_ids = (SCORECARD_EXCEPTION_ID,)
        common = {
            "product_id": acc.product_id,
            "block_id": SCORECARD_BLOCK_ID,
            "row_id": row_id,
            "row_number": row_number,
            "ui_as_of_date": cutoff,
            "method_id": "projection:promise-progress:unreviewed-scorecard@1",
            "lineage_state": "needs-review",
        }
        fields = (
            _field(field_role="category", display_value=DisplayValue("qualitative", str(category), str(category)), **common),
            _field(field_role="score", display_value=DisplayValue("qualitative", "Needs Review", "needs_review"), parity_exception_ids=exception_ids, **common),
            _field(
                field_role="evidence",
                display_value=DisplayValue("qualitative", "No reviewed credibility assessment is accepted at this as-of date.", "missing-reviewed-assessment"),
                parity_exception_ids=exception_ids,
                **common,
            ),
            _field(field_role="read", display_value=DisplayValue("qualitative", "Reviewed assessment required.", "review-required"), parity_exception_ids=exception_ids, **common),
        )
        rows.append(
            _row(
                product_id=acc.product_id,
                block_id=SCORECARD_BLOCK_ID,
                business_key=business_key,
                row_variant="scorecard_assessment",
                business_order=order,
                visible_sheet_row=row_number,
                ui_as_of_date=cutoff,
                canonical_series_or_program_id=None,
                canonical_version_id=None,
                fields=fields,
                review_issue_ids=(),
                parity_exception_ids=exception_ids,
            )
        )
    return ManagementCredibilityScorecardBlock(tuple(rows))


def _binding_for_entity(index: _Indexes, config: Mapping[str, Any], entity_id: str) -> Mapping[str, Any] | None:
    return _config_bindings(index, config).get(entity_id)


def _binding_actual_for_cutoff(binding: Mapping[str, Any] | None, cutoff: str) -> Mapping[str, Any] | None:
    if not binding:
        return None
    result = dict(binding.get("actual", {}))
    if not result:
        return None
    result["cutoff"] = cutoff
    return result


def _binding_progress_for_cutoff(binding: Mapping[str, Any] | None, cutoff: str) -> Mapping[str, Any] | None:
    if not binding:
        return None
    result = dict(binding.get("progress", {}))
    if not result:
        return None
    result["cutoff"] = cutoff
    return result


def _status_rule_for_target(target: Mapping[str, Any] | None) -> str:
    if target is None:
        return STATUS_REVIEW_ID
    return {
        "exact": STATUS_POINT_ID,
        "range": STATUS_RANGE_ID,
        "bound": STATUS_MIN_ID if target.get("operator") in {"gt", "gte"} else STATUS_MAX_ID,
        "approximate": STATUS_APPROX_ID,
        "qualitative": STATUS_QUALITATIVE_ID,
    }.get(str(target.get("kind")), STATUS_REVIEW_ID)


def _annual_guidance_row(
    acc: _ProductAccumulator,
    *,
    entity_id: str,
    entity: Mapping[str, Any],
    binding: Mapping[str, Any] | None,
    business_order: int,
    row_number: int,
) -> ProductRow:
    index, cutoff = acc.index, str(acc.config["ui_as_of_date"])
    versions = _series_versions(index, entity_id)
    buckets = _bucket_versions(index, entity, versions, cutoff, binding)
    terminal = _terminal_versions(index, versions, cutoff)
    governing = terminal[0] if len(terminal) == 1 else None
    semantic = _semantic_from_payload(index, entity["payload"], str(entity["payload"].get("dimension_set_id", "")))
    horizon_id = str(entity["payload"]["horizon_period_id"])
    target = governing["payload"].get("value") if governing else None
    governing_id = str(governing["header"]["record_id"]) if governing else None
    actual = acc.retain_actual(
        _actual_from_binding(
            index,
            product_id=acc.product_id,
            business_key=entity_id,
            binding=_binding_actual_for_cutoff(binding, cutoff),
            cutoff=cutoff,
            default_semantic=semantic,
            default_period_id=horizon_id,
        )
    )
    progress = acc.retain_progress(
        _progress_from_binding(
            index,
            product_id=acc.product_id,
            business_key=entity_id,
            binding=_binding_progress_for_cutoff(binding, cutoff),
            cutoff=cutoff,
            governing_target_version_id=governing_id,
            target_value=target,
            target_semantic=semantic,
            target_period_or_horizon_id=horizon_id,
        )
    )
    record_ids = tuple(str(version["header"]["record_id"]) for version in versions if _eligible(version, cutoff))
    explicit_issue_rules = tuple(binding.get("review_issue_rule_ids", ())) if binding else ()
    issues = _issues_for(index, (entity_id, *record_ids, *actual.canonical_observation_ids), explicit_issue_rules)
    actual_exceptions = (
        (ACTUAL_COVERAGE_EXCEPTION_ID,)
        if actual.selection_state != "selected"
        and ACTUAL_COVERAGE_EXCEPTION_ID in set(acc.config.get("parity_exception_ids", ()))
        else ()
    )
    exceptions = _sorted_unique((GENERIC_SOURCE_EXCEPTION_ID, STATIC_STATUS_EXCEPTION_ID, *actual_exceptions))
    horizon = index.periods[horizon_id]
    horizon_closed = _parse_date(str(horizon["end_date"]), label="horizon end") < _parse_date(cutoff, label="product cutoff")
    rule_id = str(binding.get("status_rule_id")) if binding and binding.get("status_rule_id") else (
        STATUS_OPEN_ID
        if governing is not None and actual.selection_state != "selected" and not horizon_closed
        else STATUS_REVIEW_ID
        if actual.selection_state != "selected"
        else _status_rule_for_target(target)
    )
    status = acc.retain_status(
        assess_status(
            product_id=acc.product_id,
            row_key=entity_id,
            rule_id=rule_id,
            target_version_id=governing_id,
            target_value=target,
            actual=actual,
            progress=progress,
            ui_as_of_date=cutoff,
            horizon_closed=horizon_closed,
            target_period_or_horizon_id=horizon_id,
            review_issue_ids=issues,
            favorable_direction=str(binding["favorable_direction"]) if binding and binding.get("favorable_direction") else None,
        )
    )
    row_id = _row_id(acc.product_id, ANNUAL_BLOCK_ID, entity_id)
    occurrences = _sorted_unique(
        occurrence for version in versions if _eligible(version, cutoff) for occurrence in version["header"].get("evidence_occurrence_ids", ())
    )
    occurrences, documents = _source_ids(index, occurrences)
    knowledge = _sorted_unique(str(version["header"]["knowledge_date"]) for version in versions if _eligible(version, cutoff))
    notes = _notes(index, occurrences, suffixes=(status.explanation,))
    row_common = {
        "product_id": acc.product_id,
        "block_id": ANNUAL_BLOCK_ID,
        "row_id": row_id,
        "row_number": row_number,
        "ui_as_of_date": cutoff,
        "review_issue_ids": issues,
    }
    fields: list[ProductField] = [
        _field(
            field_role="metric",
            display_value=DisplayValue("qualitative", _metric_label(index, entity, binding), _metric_label(index, entity, binding)),
            canonical_record_ids=(entity_id,),
            semantic_identity=semantic,
            period_or_horizon_id=horizon_id,
            method_id="selection:promise-progress:typed-series-label@1",
            **row_common,
        )
    ]
    for role in ("initial_guide", "q1_guide", "q2_guide", "q3_guide", "q4_guide"):
        version = buckets.get(role)
        if version:
            value = _version_value(index, entity, version)
            method = "display-bucket:guidance-event-quarter@1"
            version_id = str(version["header"]["record_id"])
            version_occurrences, version_documents = _record_source(index, version)
            version_knowledge = (str(version["header"]["knowledge_date"]),)
            version_publication = str(version["header"].get("publication_date"))
        else:
            value, method = MISSING_DISPLAY, "selection:promise-progress:missing-guidance-bucket@1"
            version_id, version_occurrences, version_documents, version_knowledge, version_publication = None, (), (), (), None
        fields.append(
            _field(
                field_role=role,
                display_value=value,
                canonical_record_ids=(entity_id, *((version_id,) if version_id else ())),
                target_version_id=version_id,
                semantic_identity=semantic,
                period_or_horizon_id=horizon_id,
                publication_date=version_publication,
                knowledge_dates=version_knowledge,
                source_occurrence_ids=version_occurrences,
                source_document_ids=version_documents,
                method_id=method,
                lineage_state="accepted" if version else "missing",
                **row_common,
            )
        )
    governing_id = str(governing["header"]["record_id"]) if governing else None
    status_occurrences = _sorted_unique((*occurrences, *actual.source_occurrence_ids, *(progress.source_occurrence_ids if progress else ())))
    status_occurrences, status_documents = _source_ids(index, status_occurrences)
    status_knowledge = _sorted_unique((*knowledge, *((actual.knowledge_date,) if actual.knowledge_date else ()), *(progress.knowledge_dates if progress else ())))
    fields.extend(
        [
            _field(
                field_role="actual",
                display_value=actual.display_value,
                canonical_record_ids=actual.canonical_observation_ids,
                actual=actual,
                semantic_identity=actual.semantic_identity,
                period_or_horizon_id=actual.effective_or_fiscal_period_id,
                publication_date=actual.publication_date,
                knowledge_dates=((actual.knowledge_date,) if actual.knowledge_date else ()),
                source_occurrence_ids=actual.source_occurrence_ids,
                source_document_ids=actual.source_document_ids,
                method_id=actual.selection_method_id,
                lineage_state=actual.lineage_state,
                parity_exception_ids=actual_exceptions,
                **row_common,
            ),
            _field(
                field_role="status",
                display_value=DisplayValue("qualitative", status.visible_label, status.status_code),
                canonical_record_ids=status.canonical_input_ids,
                target_version_id=governing_id,
                actual=actual,
                progress=progress,
                status=status,
                semantic_identity=semantic,
                period_or_horizon_id=horizon_id,
                publication_date=str(governing["header"].get("publication_date")) if governing else None,
                knowledge_dates=status_knowledge,
                source_occurrence_ids=status_occurrences,
                source_document_ids=status_documents,
                method_id=status.assessment_rule_id,
                lineage_state="needs-review" if status.review_state == "needs_review" else "accepted",
                parity_exception_ids=_sorted_unique((STATIC_STATUS_EXCEPTION_ID, *actual_exceptions)),
                **row_common,
            ),
            _field(
                field_role="notes_source",
                display_value=DisplayValue("qualitative", notes, notes),
                canonical_record_ids=(entity_id, *record_ids, *actual.canonical_observation_ids, *(progress.canonical_input_ids if progress else ())),
                target_version_id=governing_id,
                semantic_identity=semantic,
                period_or_horizon_id=horizon_id,
                knowledge_dates=status_knowledge,
                source_occurrence_ids=status_occurrences,
                source_document_ids=status_documents,
                method_id="projection:promise-progress:compact-source-note@1",
                lineage_state="needs-review" if status.review_state == "needs_review" else "accepted",
                parity_exception_ids=_sorted_unique((GENERIC_SOURCE_EXCEPTION_ID, *actual_exceptions)),
                **row_common,
            ),
        ]
    )
    return _row(
        product_id=acc.product_id,
        block_id=ANNUAL_BLOCK_ID,
        business_key=entity_id,
        row_variant="annual_guidance_series",
        business_order=business_order,
        visible_sheet_row=row_number,
        ui_as_of_date=cutoff,
        canonical_series_or_program_id=entity_id,
        canonical_version_id=str(governing["header"]["record_id"]) if governing else None,
        fields=fields,
        review_issue_ids=issues,
        parity_exception_ids=exceptions,
    )


def _annual_promise_row(
    acc: _ProductAccumulator,
    *,
    entity_id: str,
    entity: Mapping[str, Any],
    binding: Mapping[str, Any],
    business_order: int,
    row_number: int,
) -> ProductRow:
    index, cutoff = acc.index, str(acc.config["ui_as_of_date"])
    versions = _promise_versions(index, entity_id)
    terminal = _terminal_versions(index, versions, cutoff)
    governing = terminal[0] if len(terminal) == 1 else None
    semantic = _semantic_from_config(index, binding["target_semantic_identity"])
    deadline = governing["payload"].get("deadline") if governing else entity["payload"].get("original_deadline")
    horizon_id = str(deadline.get("value")) if deadline and deadline.get("kind") == "period" else None
    target = governing["payload"].get("target") if governing else None
    governing_id = str(governing["header"]["record_id"]) if governing else None
    actual = acc.retain_actual(
        _actual_from_binding(
            index,
            product_id=acc.product_id,
            business_key=entity_id,
            binding=_binding_actual_for_cutoff(binding, cutoff),
            cutoff=cutoff,
            default_semantic=semantic,
            default_period_id=horizon_id,
        )
    )
    progress = acc.retain_progress(
        _progress_from_binding(
            index,
            product_id=acc.product_id,
            business_key=entity_id,
            binding=_binding_progress_for_cutoff(binding, cutoff),
            cutoff=cutoff,
            governing_target_version_id=governing_id,
            target_value=target,
            target_semantic=semantic,
            target_period_or_horizon_id=horizon_id,
        )
    )
    record_ids = tuple(str(version["header"]["record_id"]) for version in versions if _eligible(version, cutoff))
    issues = _issues_for(index, (entity_id, *record_ids, *actual.canonical_observation_ids), binding.get("review_issue_rule_ids", ()))
    rule_id = str(binding.get("status_rule_id", _status_rule_for_target(target)))
    horizon_closed = bool(
        horizon_id
        and horizon_id in index.periods
        and _parse_date(str(index.periods[horizon_id]["end_date"]), label="promise horizon end") < _parse_date(cutoff, label="product cutoff")
    )
    status = acc.retain_status(
        assess_status(
            product_id=acc.product_id,
            row_key=entity_id,
            rule_id=rule_id,
            target_version_id=governing_id,
            target_value=target,
            actual=actual,
            progress=progress,
            ui_as_of_date=cutoff,
            horizon_closed=horizon_closed,
            target_period_or_horizon_id=horizon_id,
            review_issue_ids=issues,
            favorable_direction=str(binding["favorable_direction"]) if binding.get("favorable_direction") else None,
        )
    )
    exceptions = [GENERIC_SOURCE_EXCEPTION_ID, STATIC_STATUS_EXCEPTION_ID]
    row_id = _row_id(acc.product_id, ANNUAL_BLOCK_ID, entity_id)
    occurrences = _sorted_unique(
        occurrence for version in versions if _eligible(version, cutoff) for occurrence in version["header"].get("evidence_occurrence_ids", ())
    )
    occurrences = _sorted_unique((*occurrences, *actual.source_occurrence_ids, *(progress.source_occurrence_ids if progress else ())))
    occurrences, documents = _source_ids(index, occurrences)
    knowledge = _sorted_unique(
        [str(version["header"]["knowledge_date"]) for version in versions if _eligible(version, cutoff)]
        + ([actual.knowledge_date] if actual.knowledge_date else [])
    )
    bucket_candidates: dict[str, list[Mapping[str, Any]]] = {
        bucket: [] for bucket in _ANNUAL_BUCKET_ORDER
    }
    for version in versions:
        if not _eligible(version, cutoff):
            continue
        role = "initial_guide" if version["payload"].get("change_kind") == "origin" else None
        if role is None:
            horizon_period = index.periods.get(horizon_id or "", {})
            fiscal_year = int(horizon_period.get("fiscal_year", 0))
            publication_date = str(version["header"]["publication_date"])
            override = binding.get("bucket_overrides", {}).get(publication_date)
            if override is not None:
                role = _reviewed_bucket_override(
                    index,
                    entity_id=entity_id,
                    version=version,
                    override=override,
                    fiscal_year=fiscal_year,
                )
            else:
                quarter = _latest_ended_quarter(index, publication_date, fiscal_year)
                role = f"q{quarter}_guide" if quarter else "q4_guide"
        bucket_candidates[role].append(version)
    buckets: dict[str, Mapping[str, Any]] = {}
    for role, candidates in bucket_candidates.items():
        if not candidates:
            continue
        terminal = _terminal_versions(index, candidates, cutoff)
        if len(terminal) != 1:
            raise PromiseProgressProjectionError(
                f"Promise display bucket {role!r} has {len(terminal)} unresolved terminal versions."
            )
        buckets[role] = terminal[0]
    _validate_bucket_sequence(index, versions, buckets, cutoff)
    notes = _notes(index, occurrences, suffixes=(status.explanation,))
    row_common = {
        "product_id": acc.product_id,
        "block_id": ANNUAL_BLOCK_ID,
        "row_id": row_id,
        "row_number": row_number,
        "ui_as_of_date": cutoff,
        "review_issue_ids": issues,
    }
    fields: list[ProductField] = [
        _field(
            field_role="metric",
            display_value=DisplayValue("qualitative", _metric_label(index, entity, binding), _metric_label(index, entity, binding)),
            canonical_record_ids=(entity_id,),
            semantic_identity=semantic,
            period_or_horizon_id=horizon_id,
            method_id="selection:promise-progress:typed-program-label@1",
            **row_common,
        )
    ]
    for role in ("initial_guide", "q1_guide", "q2_guide", "q3_guide", "q4_guide"):
        version = buckets.get(role)
        value = _version_value(index, {"payload": {"unit_id": semantic.unit_id, "currency": binding.get("currency")}}, version) if version else MISSING_DISPLAY
        version_id = str(version["header"]["record_id"]) if version else None
        version_occurrences, version_documents = _record_source(index, version) if version else ((), ())
        fields.append(
            _field(
                field_role=role,
                display_value=value,
                canonical_record_ids=(entity_id, *((version_id,) if version_id else ())),
                target_version_id=version_id,
                semantic_identity=semantic,
                period_or_horizon_id=horizon_id,
                publication_date=str(version["header"].get("publication_date")) if version else None,
                knowledge_dates=((str(version["header"]["knowledge_date"]),) if version else ()),
                source_occurrence_ids=version_occurrences,
                source_document_ids=version_documents,
                method_id="display-bucket:guidance-event-quarter@1" if version else "selection:promise-progress:missing-guidance-bucket@1",
                lineage_state="accepted" if version else "missing",
                **row_common,
            )
        )
    governing_id = str(governing["header"]["record_id"]) if governing else None
    status_occurrences = _sorted_unique((*occurrences, *actual.source_occurrence_ids, *(progress.source_occurrence_ids if progress else ())))
    status_occurrences, status_documents = _source_ids(index, status_occurrences)
    status_knowledge = _sorted_unique((*knowledge, *((actual.knowledge_date,) if actual.knowledge_date else ()), *(progress.knowledge_dates if progress else ())))
    fields.extend(
        [
            _field(
                field_role="actual", display_value=actual.display_value,
                canonical_record_ids=actual.canonical_observation_ids, actual=actual,
                semantic_identity=actual.semantic_identity,
                period_or_horizon_id=actual.effective_or_fiscal_period_id,
                publication_date=actual.publication_date,
                knowledge_dates=((actual.knowledge_date,) if actual.knowledge_date else ()),
                source_occurrence_ids=actual.source_occurrence_ids,
                source_document_ids=actual.source_document_ids,
                method_id=actual.selection_method_id, lineage_state=actual.lineage_state,
                **row_common,
            ),
            _field(
                field_role="status", display_value=DisplayValue("qualitative", status.visible_label, status.status_code),
                canonical_record_ids=status.canonical_input_ids, target_version_id=governing_id,
                actual=actual, progress=progress, status=status,
                semantic_identity=semantic, period_or_horizon_id=horizon_id,
                publication_date=str(governing["header"].get("publication_date")) if governing else None,
                knowledge_dates=status_knowledge, source_occurrence_ids=status_occurrences,
                source_document_ids=status_documents, method_id=status.assessment_rule_id,
                lineage_state="needs-review" if status.review_state == "needs_review" else "accepted",
                parity_exception_ids=(STATIC_STATUS_EXCEPTION_ID,), **row_common,
            ),
            _field(
                field_role="notes_source", display_value=DisplayValue("qualitative", notes, notes),
                canonical_record_ids=(entity_id, *record_ids, *actual.canonical_observation_ids, *(progress.canonical_input_ids if progress else ())),
                target_version_id=governing_id, semantic_identity=semantic,
                period_or_horizon_id=horizon_id, knowledge_dates=status_knowledge,
                source_occurrence_ids=status_occurrences, source_document_ids=status_documents,
                method_id="projection:promise-progress:compact-source-note@1",
                lineage_state="needs-review" if status.review_state == "needs_review" else "accepted",
                parity_exception_ids=(GENERIC_SOURCE_EXCEPTION_ID,), **row_common,
            ),
        ]
    )
    return _row(
        product_id=acc.product_id,
        block_id=ANNUAL_BLOCK_ID,
        business_key=entity_id,
        row_variant="annual_guidance_series",
        business_order=business_order,
        visible_sheet_row=row_number,
        ui_as_of_date=cutoff,
        canonical_series_or_program_id=str(entity["payload"].get("program_id") or entity_id),
        canonical_version_id=str(governing["header"]["record_id"]) if governing else None,
        fields=fields,
        review_issue_ids=issues,
        parity_exception_ids=exceptions,
    )


def _coverage_gap_row(acc: _ProductAccumulator, gap: Mapping[str, Any], business_order: int, row_number: int) -> ProductRow:
    cutoff = str(acc.config["ui_as_of_date"])
    label = str(gap["metric_label"])
    business_key = f"diagnostic-gap:{label}"
    row_id = _row_id(acc.product_id, ANNUAL_BLOCK_ID, business_key)
    exceptions = (ACTUAL_COVERAGE_EXCEPTION_ID, STATIC_STATUS_EXCEPTION_ID, GENERIC_SOURCE_EXCEPTION_ID)
    common = {
        "product_id": acc.product_id,
        "block_id": ANNUAL_BLOCK_ID,
        "row_id": row_id,
        "row_number": row_number,
        "ui_as_of_date": cutoff,
        "method_id": "projection:promise-progress:diagnostic-coverage-gap@1",
        "lineage_state": "diagnostic",
    }
    fields = tuple(
        _field(
            field_role=role,
            display_value=(
                DisplayValue("qualitative", label, label)
                if role == "metric"
                else DisplayValue("qualitative", "Needs Review", "needs_review")
                if role == "status"
                else DisplayValue("qualitative", str(gap.get("note", "Accepted source-native coverage is unavailable.")), str(gap.get("note", "Accepted source-native coverage is unavailable.")))
                if role == "notes_source"
                else MISSING_DISPLAY
            ),
            parity_exception_ids=(
                (ACTUAL_COVERAGE_EXCEPTION_ID, STATIC_STATUS_EXCEPTION_ID)
                if role == "status"
                else (ACTUAL_COVERAGE_EXCEPTION_ID, GENERIC_SOURCE_EXCEPTION_ID)
                if role == "notes_source"
                else (ACTUAL_COVERAGE_EXCEPTION_ID,)
                if role == "actual"
                else ()
            ),
            **common,
        )
        for role, _, _ in BLOCK_FIELD_LAYOUT[ANNUAL_BLOCK_ID]
    )
    return _row(
        product_id=acc.product_id,
        block_id=ANNUAL_BLOCK_ID,
        business_key=business_key,
        row_variant="diagnostic_coverage_gap",
        business_order=business_order,
        visible_sheet_row=row_number,
        ui_as_of_date=cutoff,
        canonical_series_or_program_id=None,
        canonical_version_id=None,
        fields=fields,
        review_issue_ids=(),
        parity_exception_ids=exceptions,
    )


def _annual_block(acc: _ProductAccumulator) -> AnnualGuidanceProgressionBlock:
    index, config = acc.index, acc.config
    years = {int(value) for value in config.get("annual_fiscal_years", ())}
    order_map = _configured_order(index, config)
    bindings = _config_bindings(index, config)
    items: list[tuple[int, str, str, Mapping[str, Any], Mapping[str, Any] | None]] = []
    for entity_id, entity in index.entities.items():
        kind = entity.get("payload", {}).get("kind")
        if kind == "GuidanceSeries" and years and _period_contains_annual(index, entity, years):
            items.append((order_map.get(entity_id, 10_000), entity_id, "guidance", entity, bindings.get(entity_id)))
        elif kind == "Promise" and entity_id in bindings and bool(bindings[entity_id].get("include_in_annual", False)):
            items.append((order_map.get(entity_id, 10_000), entity_id, "promise", entity, bindings[entity_id]))
    for gap_index, gap in enumerate(config.get("coverage_gaps", ()), start=1):
        items.append((int(gap.get("business_order", 20_000 + gap_index)), f"gap:{gap['metric_label']}", "gap", gap, None))
    items.sort(key=lambda item: (item[0], item[1]))
    if len(items) > len(ANNUAL_DATA_ROWS):
        raise PromiseProgressProjectionError("Annual projection exceeds the locked first-shadow template row capacity.")
    rows: list[ProductRow] = []
    for position, (configured_order, identity, kind, entity, binding) in enumerate(items):
        row_number = ANNUAL_DATA_ROWS[position]
        business_order = configured_order if configured_order < 10_000 else position + 1
        if kind == "guidance":
            rows.append(_annual_guidance_row(acc, entity_id=identity, entity=entity, binding=binding, business_order=business_order, row_number=row_number))
        elif kind == "promise":
            assert binding is not None
            rows.append(_annual_promise_row(acc, entity_id=identity, entity=entity, binding=binding, business_order=business_order, row_number=row_number))
        else:
            rows.append(_coverage_gap_row(acc, entity, business_order, row_number))
    return AnnualGuidanceProgressionBlock(tuple(rows))


def _is_open_period(index: _Indexes, period_id: str | None, cutoff: str) -> bool:
    if not period_id:
        return True
    period = index.periods.get(period_id)
    if period is None:
        return True
    return _parse_date(str(period["end_date"]), label="horizon end") >= _parse_date(cutoff, label="product cutoff")


def _open_row(
    acc: _ProductAccumulator,
    *,
    entity_id: str,
    entity: Mapping[str, Any],
    binding: Mapping[str, Any] | None,
    business_order: int,
    row_number: int,
) -> ProductRow:
    index, cutoff = acc.index, str(acc.config["ui_as_of_date"])
    kind = str(entity["payload"]["kind"])
    versions = _series_versions(index, entity_id) if kind == "GuidanceSeries" else _promise_versions(index, entity_id)
    terminal = _terminal_versions(index, versions, cutoff)
    governing = terminal[0] if len(terminal) == 1 else None
    target = None
    if governing:
        target = governing["payload"].get("value") if kind == "GuidanceSeries" else governing["payload"].get("target")
    if kind == "GuidanceSeries":
        semantic = _semantic_from_payload(index, entity["payload"], str(entity["payload"].get("dimension_set_id", "")))
        horizon_id = str(entity["payload"].get("horizon_period_id", "")) or None
        currency = entity["payload"].get("currency")
    else:
        if binding is None:
            raise PromiseProgressProjectionError("Every projected Promise requires a declarative semantic binding.")
        semantic = _semantic_from_config(index, binding["target_semantic_identity"])
        deadline = governing["payload"].get("deadline") if governing else entity["payload"].get("original_deadline")
        horizon_id = str(deadline.get("value")) if deadline and deadline.get("kind") == "period" else None
        currency = binding.get("currency")
    actual = acc.retain_actual(
        _actual_from_binding(index, product_id=acc.product_id, business_key=entity_id, binding=_binding_actual_for_cutoff(binding, cutoff), cutoff=cutoff, default_semantic=semantic, default_period_id=horizon_id)
    )
    governing_id = str(governing["header"]["record_id"]) if governing else None
    progress = acc.retain_progress(
        _progress_from_binding(
            index,
            product_id=acc.product_id,
            business_key=entity_id,
            binding=_binding_progress_for_cutoff(binding, cutoff),
            cutoff=cutoff,
            governing_target_version_id=governing_id,
            target_value=target,
            target_semantic=semantic,
            target_period_or_horizon_id=horizon_id,
        )
    )
    record_ids = tuple(str(version["header"]["record_id"]) for version in versions if _eligible(version, cutoff))
    issues = _issues_for(index, (entity_id, *record_ids, *actual.canonical_observation_ids, *(progress.canonical_input_ids if progress else ())), binding.get("review_issue_rule_ids", ()) if binding else ())
    rule_id = str(binding.get("status_rule_id")) if binding and binding.get("status_rule_id") else STATUS_OPEN_ID
    if governing is None:
        rule_id = STATUS_REVIEW_ID
    status = acc.retain_status(
        assess_status(
            product_id=acc.product_id,
            row_key=f"open:{entity_id}",
            rule_id=rule_id,
            target_version_id=governing_id,
            target_value=target,
            actual=actual,
            progress=progress,
            ui_as_of_date=cutoff,
            horizon_closed=not _is_open_period(index, horizon_id, cutoff),
            target_period_or_horizon_id=horizon_id,
            review_issue_ids=issues,
            withdrawn=bool(governing and governing["payload"].get("version_kind") == "withdrawal"),
            favorable_direction=str(binding["favorable_direction"]) if binding and binding.get("favorable_direction") else None,
        )
    )
    value = display_value_from_spec(target, unit=index.unit_catalog.get(str(semantic.unit_id)), currency=currency)
    if binding and binding.get("target_display_suffix") and value.value_form != "missing":
        value = DisplayValue(value.value_form, f"{value.display_text} {binding['target_display_suffix']}", value.machine_value)
    occurrences = _sorted_unique(
        occurrence for version in versions if _eligible(version, cutoff) for occurrence in version["header"].get("evidence_occurrence_ids", ())
    )
    occurrences = _sorted_unique((*occurrences, *actual.source_occurrence_ids, *(progress.source_occurrence_ids if progress else ())))
    occurrences, documents = _source_ids(index, occurrences)
    knowledge = _sorted_unique(
        [str(version["header"]["knowledge_date"]) for version in versions if _eligible(version, cutoff)]
        + ([actual.knowledge_date] if actual.knowledge_date else [])
        + (list(progress.knowledge_dates) if progress else [])
    )
    note_suffixes = [status.explanation]
    if progress:
        note_suffixes.append(f"Progress: {progress.display_value.display_text}.")
    if binding:
        note_suffixes.extend(str(value) for value in binding.get("note_suffixes", ()))
    notes = _notes(index, occurrences, suffixes=note_suffixes)
    exceptions = (GENERIC_SOURCE_EXCEPTION_ID, STATIC_STATUS_EXCEPTION_ID)
    row_id = _row_id(acc.product_id, OPEN_BLOCK_ID, entity_id)
    row_common = {
        "product_id": acc.product_id,
        "block_id": OPEN_BLOCK_ID,
        "row_id": row_id,
        "row_number": row_number,
        "ui_as_of_date": cutoff,
        "review_issue_ids": issues,
    }
    governing_id = str(governing["header"]["record_id"]) if governing else None
    governing_occurrences, governing_documents = _record_source(index, governing) if governing else ((), ())
    governing_knowledge = ((str(governing["header"]["knowledge_date"]),) if governing else ())
    fields = (
        _field(
            field_role="metric", display_value=DisplayValue("qualitative", _metric_label(index, entity, binding), _metric_label(index, entity, binding)),
            canonical_record_ids=(entity_id,), semantic_identity=semantic,
            period_or_horizon_id=horizon_id, method_id="selection:promise-progress:typed-economic-label@1",
            **row_common,
        ),
        _field(
            field_role="current_guide", display_value=value,
            canonical_record_ids=(entity_id, *((governing_id,) if governing_id else ())),
            target_version_id=governing_id, semantic_identity=semantic,
            period_or_horizon_id=horizon_id,
            publication_date=str(governing["header"].get("publication_date")) if governing else None,
            knowledge_dates=governing_knowledge, source_occurrence_ids=governing_occurrences,
            source_document_ids=governing_documents,
            method_id="selection:promise-progress:explicit-terminal-history@1",
            lineage_state="accepted" if governing else "blocked", **row_common,
        ),
        _field(
            field_role="horizon",
            display_value=DisplayValue("qualitative", _horizon_display(index, horizon_id, str(binding.get("horizon_display")) if binding and binding.get("horizon_display") else None), horizon_id),
            canonical_record_ids=(entity_id, *((governing_id,) if governing_id else ())),
            target_version_id=governing_id, semantic_identity=semantic,
            period_or_horizon_id=horizon_id, knowledge_dates=governing_knowledge,
            source_occurrence_ids=governing_occurrences, source_document_ids=governing_documents,
            method_id="selection:promise-progress:source-backed-horizon@1", **row_common,
        ),
        _field(
            field_role="status", display_value=DisplayValue("qualitative", status.visible_label, status.status_code),
            canonical_record_ids=status.canonical_input_ids, target_version_id=governing_id,
            actual=actual, progress=progress, status=status, semantic_identity=semantic,
            period_or_horizon_id=horizon_id,
            publication_date=str(governing["header"].get("publication_date")) if governing else None,
            knowledge_dates=knowledge, source_occurrence_ids=occurrences, source_document_ids=documents,
            method_id=status.assessment_rule_id,
            lineage_state="needs-review" if status.review_state == "needs_review" else "accepted",
            parity_exception_ids=(STATIC_STATUS_EXCEPTION_ID,), **row_common,
        ),
        _field(
            field_role="notes_source", display_value=DisplayValue("qualitative", notes, notes),
            canonical_record_ids=(entity_id, *record_ids, *actual.canonical_observation_ids, *(progress.canonical_input_ids if progress else ())),
            target_version_id=governing_id, actual=actual, progress=progress, status=status,
            semantic_identity=semantic, period_or_horizon_id=horizon_id,
            knowledge_dates=knowledge, source_occurrence_ids=occurrences, source_document_ids=documents,
            method_id="projection:promise-progress:compact-source-note@1",
            lineage_state="needs-review" if status.review_state == "needs_review" else "accepted",
            parity_exception_ids=(GENERIC_SOURCE_EXCEPTION_ID,), **row_common,
        ),
    )
    return _row(
        product_id=acc.product_id,
        block_id=OPEN_BLOCK_ID,
        business_key=entity_id,
        row_variant="open_guidance",
        business_order=business_order,
        visible_sheet_row=row_number,
        ui_as_of_date=cutoff,
        canonical_series_or_program_id=str(entity["payload"].get("program_id") or entity_id),
        canonical_version_id=str(governing["header"]["record_id"]) if governing else None,
        fields=fields,
        review_issue_ids=issues,
        parity_exception_ids=exceptions,
    )


def _open_block(acc: _ProductAccumulator) -> OpenGuidanceBlock:
    index, cutoff = acc.index, str(acc.config["ui_as_of_date"])
    order_map = _configured_order(index, acc.config)
    bindings = _config_bindings(index, acc.config)
    items: list[tuple[int, str, Mapping[str, Any], Mapping[str, Any] | None]] = []
    for entity_id, entity in index.entities.items():
        kind = entity.get("payload", {}).get("kind")
        if kind == "GuidanceSeries":
            horizon_id = str(entity["payload"].get("horizon_period_id", "")) or None
            if _is_open_period(index, horizon_id, cutoff):
                items.append((order_map.get(entity_id, 10_000), entity_id, entity, bindings.get(entity_id)))
        elif kind == "Promise" and entity_id in bindings and bool(bindings[entity_id].get("include_in_open", True)):
            versions = _promise_versions(index, entity_id)
            terminal = _terminal_versions(index, versions, cutoff)
            governing = terminal[0] if len(terminal) == 1 else None
            deadline = governing["payload"].get("deadline") if governing else entity["payload"].get("original_deadline")
            horizon_id = str(deadline.get("value")) if deadline and deadline.get("kind") == "period" else None
            if _is_open_period(index, horizon_id, cutoff):
                items.append((order_map.get(entity_id, 10_000), entity_id, entity, bindings[entity_id]))
    items.sort(key=lambda item: (item[0], item[1]))
    if len(items) > len(OPEN_DATA_ROWS):
        raise PromiseProgressProjectionError("Open projection exceeds the locked first-shadow template row capacity.")
    rows = tuple(
        _open_row(
            acc,
            entity_id=entity_id,
            entity=entity,
            binding=binding,
            business_order=(configured if configured < 10_000 else position + 1),
            row_number=OPEN_DATA_ROWS[position],
        )
        for position, (configured, entity_id, entity, binding) in enumerate(items)
    )
    return OpenGuidanceBlock(rows)


def _timeline_progress(
    acc: _ProductAccumulator,
    entity_id: str,
    binding: Mapping[str, Any] | None,
    cutoff: str,
    target_version_id: str,
    target_value: Mapping[str, Any] | None,
    target_semantic: SemanticIdentity,
    target_period_or_horizon_id: str | None,
) -> ProgressSelection | None:
    return acc.retain_progress(
        _progress_from_binding(
            acc.index,
            product_id=acc.product_id,
            business_key=f"{entity_id}:{cutoff}",
            binding=_binding_progress_for_cutoff(binding, cutoff),
            cutoff=cutoff,
            governing_target_version_id=target_version_id,
            target_value=target_value,
            target_semantic=target_semantic,
            target_period_or_horizon_id=target_period_or_horizon_id,
        )
    )


def _timeline_actual(
    acc: _ProductAccumulator,
    entity_id: str,
    binding: Mapping[str, Any] | None,
    cutoff: str,
    semantic: SemanticIdentity,
    horizon_id: str | None,
) -> ActualSelection:
    return acc.retain_actual(
        _actual_from_binding(acc.index, product_id=acc.product_id, business_key=f"{entity_id}:{cutoff}", binding=_binding_actual_for_cutoff(binding, cutoff), cutoff=cutoff, default_semantic=semantic, default_period_id=horizon_id)
    )


def _timeline_row(
    acc: _ProductAccumulator,
    *,
    entity_id: str,
    entity: Mapping[str, Any],
    version: Mapping[str, Any],
    versions: Sequence[Mapping[str, Any]],
    binding: Mapping[str, Any] | None,
    business_order: int,
    row_number: int,
) -> ProductRow:
    index = acc.index
    cutoff = str(version["header"]["knowledge_date"])
    predecessor = _relation_predecessor(index, version, versions)
    kind = str(entity["payload"]["kind"])
    if kind == "GuidanceSeries":
        semantic = _semantic_from_payload(index, entity["payload"], str(entity["payload"].get("dimension_set_id", "")))
        horizon_id = str(entity["payload"].get("horizon_period_id", "")) or None
        currency = entity["payload"].get("currency")
    else:
        if binding is None:
            raise PromiseProgressProjectionError("Timeline Promise is missing its declarative semantic binding.")
        semantic = _semantic_from_config(index, binding["target_semantic_identity"])
        deadline = version["payload"].get("deadline") or entity["payload"].get("original_deadline")
        horizon_id = str(deadline.get("value")) if deadline and deadline.get("kind") == "period" else None
        currency = binding.get("currency")
    current_spec = version["payload"].get("value") if kind == "GuidanceSeries" else version["payload"].get("target")
    previous_spec = None
    if predecessor:
        previous_spec = predecessor["payload"].get("value") if kind == "GuidanceSeries" else predecessor["payload"].get("target")
    current_display = display_value_from_spec(current_spec, unit=index.unit_catalog.get(str(semantic.unit_id)), currency=currency)
    previous_display = display_value_from_spec(previous_spec, unit=index.unit_catalog.get(str(semantic.unit_id)), currency=currency)
    if binding and binding.get("target_display_suffix"):
        suffix = str(binding["target_display_suffix"])
        if current_display.value_form != "missing":
            current_display = DisplayValue(current_display.value_form, f"{current_display.display_text} {suffix}", current_display.machine_value)
        if previous_display.value_form != "missing":
            previous_display = DisplayValue(previous_display.value_form, f"{previous_display.display_text} {suffix}", previous_display.machine_value)
    version_id = str(version["header"]["record_id"])
    actual = _timeline_actual(acc, entity_id, binding, cutoff, semantic, horizon_id)
    progress = _timeline_progress(
        acc,
        entity_id,
        binding,
        cutoff,
        version_id,
        current_spec,
        semantic,
        horizon_id,
    )
    canonical_ids = (entity_id, version_id) + ((str(predecessor["header"]["record_id"]),) if predecessor else ())
    issues = _issues_for(index, (*canonical_ids, *actual.canonical_observation_ids, *(progress.canonical_input_ids if progress else ())), binding.get("review_issue_rule_ids", ()) if binding else ())
    rule_id = str(binding.get("status_rule_id")) if binding and binding.get("status_rule_id") else STATUS_REVIEW_ID
    if kind == "GuidanceSeries" and rule_id == STATUS_REVIEW_ID:
        rule_id = STATUS_OPEN_ID
    status = acc.retain_status(
        assess_status(
            product_id=acc.product_id,
            row_key=f"timeline:{version_id}",
            rule_id=rule_id,
            target_version_id=version_id,
            target_value=current_spec,
            actual=actual,
            progress=progress,
            ui_as_of_date=cutoff,
            horizon_closed=not _is_open_period(index, horizon_id, cutoff),
            target_period_or_horizon_id=horizon_id,
            review_issue_ids=issues,
            withdrawn=_change_type(
                version,
                predecessor,
                current_semantic=semantic,
                previous_semantic=semantic,
            ) == "withdrawal",
            favorable_direction=str(binding["favorable_direction"]) if binding and binding.get("favorable_direction") else None,
        )
    )
    occurrences = _sorted_unique(
        tuple(version["header"].get("evidence_occurrence_ids", ()))
        + tuple(predecessor["header"].get("evidence_occurrence_ids", ()) if predecessor else ())
        + actual.source_occurrence_ids
        + (progress.source_occurrence_ids if progress else ())
    )
    occurrences, documents = _source_ids(index, occurrences)
    knowledge = _sorted_unique(
        [str(version["header"]["knowledge_date"])]
        + ([str(predecessor["header"]["knowledge_date"])] if predecessor else [])
        + ([actual.knowledge_date] if actual.knowledge_date else [])
        + (list(progress.knowledge_dates) if progress else [])
    )
    notes = _notes(index, occurrences, suffixes=(("Run rate is Progress, not realized savings." if progress and progress.progress_role_id == PROGRESS_RUN_RATE_ID else ""), status.explanation))
    exceptions = [GENERIC_SOURCE_EXCEPTION_ID, STATIC_STATUS_EXCEPTION_ID]
    temporal_exception_active = TEMPORAL_EXCEPTION_ID in set(acc.config.get("parity_exception_ids", ()))
    if actual.selection_state == "missing_by_cutoff" and temporal_exception_active:
        exceptions.append(TEMPORAL_EXCEPTION_ID)
    row_id = _row_id(acc.product_id, TIMELINE_BLOCK_ID, version_id)
    row_common = {
        "product_id": acc.product_id,
        "block_id": TIMELINE_BLOCK_ID,
        "row_id": row_id,
        "row_number": row_number,
        "ui_as_of_date": cutoff,
        "review_issue_ids": issues,
    }
    change_type = _change_type(
        version,
        predecessor,
        current_semantic=semantic,
        previous_semantic=semantic,
    )
    visible_change = change_type.replace("-", " ").title()
    version_occurrences, version_documents = _record_source(index, version)
    predecessor_occurrences, predecessor_documents = _record_source(index, predecessor) if predecessor else ((), ())
    version_knowledge = (str(version["header"]["knowledge_date"]),)
    predecessor_knowledge = ((str(predecessor["header"]["knowledge_date"]),) if predecessor else ())
    temporal_exceptions = (
        (TEMPORAL_EXCEPTION_ID,)
        if actual.selection_state == "missing_by_cutoff" and temporal_exception_active
        else ()
    )
    fields = (
        _field(
            field_role="metric", display_value=DisplayValue("qualitative", _metric_label(index, entity, binding), _metric_label(index, entity, binding)),
            canonical_record_ids=(entity_id,), semantic_identity=semantic,
            period_or_horizon_id=horizon_id, method_id="selection:promise-progress:typed-economic-label@1", **row_common,
        ),
        _field(
            field_role="previous_guide", display_value=previous_display,
            canonical_record_ids=((entity_id, str(predecessor["header"]["record_id"])) if predecessor else (entity_id,)),
            target_version_id=str(predecessor["header"]["record_id"]) if predecessor else None,
            semantic_identity=semantic, period_or_horizon_id=horizon_id,
            publication_date=str(predecessor["header"].get("publication_date")) if predecessor else None,
            knowledge_dates=predecessor_knowledge, source_occurrence_ids=predecessor_occurrences,
            source_document_ids=predecessor_documents,
            method_id="selection:promise-progress:explicit-history-predecessor@1",
            lineage_state="accepted" if predecessor else "missing", **row_common,
        ),
        _field(
            field_role="current_guide", display_value=current_display,
            canonical_record_ids=(entity_id, version_id), target_version_id=version_id,
            semantic_identity=semantic, period_or_horizon_id=horizon_id,
            publication_date=str(version["header"].get("publication_date")),
            knowledge_dates=version_knowledge, source_occurrence_ids=version_occurrences,
            source_document_ids=version_documents, method_id="selection:promise-progress:material-version@1", **row_common,
        ),
        _field(
            field_role="change_type", display_value=DisplayValue("qualitative", visible_change, change_type),
            canonical_record_ids=canonical_ids, target_version_id=version_id,
            semantic_identity=semantic, period_or_horizon_id=horizon_id,
            publication_date=str(version["header"].get("publication_date")),
            knowledge_dates=_sorted_unique((*version_knowledge, *predecessor_knowledge)),
            source_occurrence_ids=_sorted_unique((*version_occurrences, *predecessor_occurrences)),
            source_document_ids=_sorted_unique((*version_documents, *predecessor_documents)),
            method_id="classification:promise-progress:version-change@1", **row_common,
        ),
        _field(
            field_role="actual", display_value=actual.display_value,
            canonical_record_ids=actual.canonical_observation_ids, actual=actual,
            semantic_identity=actual.semantic_identity,
            period_or_horizon_id=actual.effective_or_fiscal_period_id,
            publication_date=actual.publication_date,
            knowledge_dates=((actual.knowledge_date,) if actual.knowledge_date else ()),
            source_occurrence_ids=actual.source_occurrence_ids, source_document_ids=actual.source_document_ids,
            method_id=actual.selection_method_id, lineage_state=actual.lineage_state,
            parity_exception_ids=temporal_exceptions, **row_common,
        ),
        _field(
            field_role="progress", display_value=progress.display_value if progress else MISSING_DISPLAY,
            canonical_record_ids=(progress.canonical_input_ids if progress else ()), progress=progress,
            semantic_identity=(progress.semantic_identity if progress else semantic),
            period_or_horizon_id=(progress.period_or_horizon_id if progress else horizon_id),
            publication_date=(max(progress.publication_dates) if progress and progress.publication_dates else None),
            knowledge_dates=(progress.knowledge_dates if progress else ()),
            source_occurrence_ids=(progress.source_occurrence_ids if progress else ()),
            source_document_ids=(progress.source_document_ids if progress else ()),
            method_id=progress.method_id if progress else "selection:promise-progress:no-eligible-progress@1",
            lineage_state="accepted" if progress else "missing", **row_common,
        ),
        _field(
            field_role="status", display_value=DisplayValue("qualitative", status.visible_label, status.status_code),
            canonical_record_ids=status.canonical_input_ids, target_version_id=version_id,
            actual=actual, progress=progress, status=status, semantic_identity=semantic,
            period_or_horizon_id=horizon_id, publication_date=str(version["header"].get("publication_date")),
            knowledge_dates=knowledge, source_occurrence_ids=occurrences, source_document_ids=documents,
            method_id=status.assessment_rule_id,
            lineage_state="needs-review" if status.review_state == "needs_review" else "accepted",
            parity_exception_ids=_sorted_unique((STATIC_STATUS_EXCEPTION_ID, *temporal_exceptions)), **row_common,
        ),
        _field(
            field_role="horizon", display_value=DisplayValue("qualitative", _horizon_display(index, horizon_id, str(binding.get("horizon_display")) if binding and binding.get("horizon_display") else None), horizon_id),
            canonical_record_ids=(entity_id, version_id), target_version_id=version_id,
            semantic_identity=semantic, period_or_horizon_id=horizon_id,
            knowledge_dates=version_knowledge, source_occurrence_ids=version_occurrences,
            source_document_ids=version_documents, method_id="selection:promise-progress:source-backed-horizon@1", **row_common,
        ),
        _field(
            field_role="stated_in", display_value=DisplayValue("qualitative", _stated_in(index, version), str(version["header"].get("effective_period_id"))),
            canonical_record_ids=(version_id,), target_version_id=version_id,
            semantic_identity=semantic, period_or_horizon_id=str(version["header"].get("effective_period_id")),
            knowledge_dates=version_knowledge, source_occurrence_ids=version_occurrences,
            source_document_ids=version_documents, method_id="selection:promise-progress:effective-period-label@1", **row_common,
        ),
        _field(
            field_role="source_date", display_value=DisplayValue("date", str(version["header"].get("publication_date")), str(version["header"].get("publication_date"))),
            canonical_record_ids=(version_id,), target_version_id=version_id,
            publication_date=str(version["header"].get("publication_date")),
            knowledge_dates=version_knowledge, source_occurrence_ids=version_occurrences,
            source_document_ids=version_documents, method_id="selection:promise-progress:source-publication-date@1", **row_common,
        ),
        _field(
            field_role="source_note", display_value=DisplayValue("qualitative", notes, notes),
            canonical_record_ids=(*canonical_ids, *actual.canonical_observation_ids, *(progress.canonical_input_ids if progress else ())),
            target_version_id=version_id, actual=actual, progress=progress, status=status,
            semantic_identity=semantic, period_or_horizon_id=horizon_id,
            publication_date=str(version["header"].get("publication_date")),
            knowledge_dates=knowledge, source_occurrence_ids=occurrences, source_document_ids=documents,
            method_id="projection:promise-progress:compact-source-note@1",
            lineage_state="needs-review" if status.review_state == "needs_review" else "accepted",
            parity_exception_ids=_sorted_unique((GENERIC_SOURCE_EXCEPTION_ID, *temporal_exceptions)), **row_common,
        ),
    )
    return _row(
        product_id=acc.product_id,
        block_id=TIMELINE_BLOCK_ID,
        business_key=version_id,
        row_variant="guidance_version" if kind == "GuidanceSeries" else "promise_version",
        business_order=business_order,
        visible_sheet_row=row_number,
        ui_as_of_date=cutoff,
        canonical_series_or_program_id=str(entity["payload"].get("program_id") or entity_id),
        canonical_version_id=version_id,
        fields=fields,
        review_issue_ids=issues,
        parity_exception_ids=exceptions,
    )


def _timeline_block(acc: _ProductAccumulator) -> QuarterlyRevisionTimelineBlock:
    index, cutoff = acc.index, str(acc.config["ui_as_of_date"])
    order_map = _configured_order(index, acc.config)
    bindings = _config_bindings(index, acc.config)
    items: list[tuple[int, str, str, Mapping[str, Any], Mapping[str, Any], tuple[Mapping[str, Any], ...], Mapping[str, Any] | None]] = []
    for entity_id, entity in index.entities.items():
        kind = entity.get("payload", {}).get("kind")
        if kind == "GuidanceSeries":
            versions = _series_versions(index, entity_id)
            binding = bindings.get(entity_id)
        elif kind == "Promise" and entity_id in bindings:
            versions = _promise_versions(index, entity_id)
            binding = bindings[entity_id]
        else:
            continue
        for version in versions:
            if _eligible(version, cutoff):
                items.append((order_map.get(entity_id, 10_000), entity_id, str(version["header"]["record_id"]), entity, version, versions, binding))
    items.sort(key=lambda item: (item[0], _version_sort_key(item[4]), item[1], item[2]))
    if len(items) > len(TIMELINE_DATA_ROWS):
        raise PromiseProgressProjectionError("Timeline projection exceeds the locked first-shadow template row capacity.")
    rows = tuple(
        _timeline_row(
            acc,
            entity_id=entity_id,
            entity=entity,
            version=version,
            versions=versions,
            binding=binding,
            business_order=position + 1,
            row_number=TIMELINE_DATA_ROWS[position],
        )
        for position, (_, entity_id, _, entity, version, versions, binding) in enumerate(items)
    )
    return QuarterlyRevisionTimelineBlock(rows)


def _validate_config(index: _Indexes, config: Mapping[str, Any]) -> None:
    required = {"template_oracle_sha256", "ui_as_of_date", "annual_fiscal_years"}
    missing = required - set(config)
    if missing:
        raise PromiseProgressProjectionError(f"Projection plan is missing required fields: {sorted(missing)!r}.")
    _require_sha(str(config["template_oracle_sha256"]), label="template_oracle_sha256")
    cutoff = _parse_date(str(config["ui_as_of_date"]), label="ui_as_of_date")
    package_cutoff = _parse_date(str(index.package.get("knowledge_cutoff")), label="package knowledge_cutoff")
    if cutoff > package_cutoff:
        raise PromiseProgressProjectionError("Product UI as-of date cannot exceed the accepted package knowledge cutoff.")
    unknown = set(config.get("parity_exception_ids", ())) - CLOSED_PARITY_EXCEPTION_IDS
    if unknown:
        raise PromiseProgressProjectionError(f"Projection plan contains unregistered parity exceptions: {sorted(unknown)!r}.")
    _configured_order(index, config)
    _config_bindings(index, config)


def build_promise_progress_product(
    package: Mapping[str, Any],
    plan: Mapping[str, Any],
) -> PromiseProgressProduct:
    """Build the one source-native product and its field-level shadow matrix."""

    index = _build_indexes(package)
    _validate_config(index, plan)
    ui_as_of_date = str(plan["ui_as_of_date"])
    product_id = f"promise-progress-product:{index.company_id}:{ui_as_of_date}@1"
    source_package_sha = hashlib.sha256(serialize_package(package)).hexdigest()
    source_package_id = str(index.package.get("normalized_package_ref", {}).get("semantic_snapshot_id") or f"longitudinal-package:{source_package_sha}@1")
    acc = _ProductAccumulator(index=index, config=plan, product_id=product_id)
    blocks: tuple[ProductBlock, ...] = (
        _scorecard_block(acc),
        _annual_block(acc),
        _open_block(acc),
        _timeline_block(acc),
    )
    field_applied = _sorted_unique(
        exception_id
        for block in blocks
        for row in block.rows
        for exception_id in row.parity_exception_ids
    )
    structural_applied = _sorted_unique(
        set(plan.get("parity_exception_ids", ())) & _STRUCTURAL_PARITY_EXCEPTION_IDS
    )
    legacy_parity_oracle = plan.get("legacy_parity")
    accepted_parity_differences = plan.get("legacy_parity_accepted_differences")
    independent_digest_plan_keys = {
        "legacy_parity_capture_manifest_sha256": "capture_manifest_sha256",
        "legacy_parity_source_scope_manifest_sha256": "source_scope_manifest_sha256",
        "legacy_parity_row_disposition_graph_sha256": "row_disposition_graph_sha256",
        "legacy_parity_structural_observation_set_sha256": "structural_observation_set_sha256",
        "legacy_parity_structural_binding_set_sha256": "structural_binding_set_sha256",
    }
    supplied_independent_digest_keys = set(independent_digest_plan_keys) & set(plan)
    if legacy_parity_oracle is None:
        if supplied_independent_digest_keys:
            raise PromiseProgressProjectionError(
                "Independent parity-scope digests require a frozen declared legacy oracle."
            )
    else:
        if supplied_independent_digest_keys != set(independent_digest_plan_keys):
            raise PromiseProgressProjectionError(
                "Frozen legacy parity requires every independent scope/completeness digest."
            )
        if legacy_parity_oracle.get("independent_expected_digests") is not None:
            raise PromiseProgressProjectionError(
                "Independent parity-scope digests must have one owner outside the frozen oracle payload."
            )
        independent_expected_digests = {
            oracle_key: str(plan[plan_key])
            for plan_key, oracle_key in independent_digest_plan_keys.items()
        }
        for key, value in independent_expected_digests.items():
            _require_sha(value, label=f"independent parity {key}")
        legacy_parity_oracle = {
            **dict(legacy_parity_oracle),
            "independent_expected_digests": independent_expected_digests,
        }
    binding_applied: tuple[str, ...] = ()
    if accepted_parity_differences is not None:
        if legacy_parity_oracle is None:
            raise PromiseProgressProjectionError(
                "Accepted parity differences require a frozen declared legacy field oracle."
            )
        if legacy_parity_oracle.get("accepted_differences"):
            raise PromiseProgressProjectionError(
                "Accepted parity differences must have one declarative owner."
            )
        legacy_parity_oracle = {
            **dict(legacy_parity_oracle),
            "accepted_differences": accepted_parity_differences,
        }
        if not isinstance(accepted_parity_differences, (list, tuple)):
            raise PromiseProgressProjectionError("Accepted parity differences must be a closed reviewed list.")
        binding_applied = _sorted_unique(
            str(binding.get("exception_id"))
            for binding in accepted_parity_differences
            if isinstance(binding, Mapping) and binding.get("exception_id")
        )
        undeclared_binding_exceptions = set(binding_applied) - set(plan.get("parity_exception_ids", ()))
        if undeclared_binding_exceptions:
            raise PromiseProgressProjectionError(
                "Accepted parity bindings use exception policies not activated by the reviewed plan."
            )
    applied = _sorted_unique((*field_applied, *structural_applied, *binding_applied))
    product = PromiseProgressProduct(
        product_id=product_id,
        company_id=index.company_id,
        ui_as_of_date=ui_as_of_date,
        knowledge_cutoff=str(index.package["knowledge_cutoff"]),
        source_package_id=source_package_id,
        source_package_sha256=source_package_sha,
        template_oracle_sha256=str(plan["template_oracle_sha256"]),
        blocks=blocks,
        actuals=tuple(sorted(acc.actuals.values(), key=lambda value: value.actual_id)),
        progress_values=tuple(sorted(acc.progress_values.values(), key=lambda value: value.progress_id)),
        status_assessments=tuple(sorted(acc.statuses.values(), key=lambda value: value.status_assessment_id)),
        structural_parity_exception_ids=structural_applied,
        applied_parity_exception_ids=applied,
        legacy_parity_oracle=legacy_parity_oracle,
        source_reference_catalog={
            "canonical_record_ids": _sorted_unique((*index.entities, *index.observations)),
            "series_or_program_ids": _sorted_unique(
                (
                    *index.entities,
                    *(
                        str(entity.get("payload", {}).get("program_id"))
                        for entity in index.entities.values()
                        if entity.get("payload", {}).get("program_id")
                    ),
                )
            ),
            "source_document_ids": _sorted_unique(index.documents),
            "source_occurrence_ids": _sorted_unique(index.occurrences),
            "review_issue_ids": _sorted_unique(
                str(issue["issue_id"]) for issue in index.review_issues
            ),
            "period_or_horizon_ids": _sorted_unique(index.periods),
            "metric_ids": _sorted_unique(index.catalog_ids["metrics"]),
            "definition_ids": _sorted_unique(index.catalog_ids["definitions"]),
            "basis_ids": _sorted_unique(index.catalog_ids["bases"]),
            "unit_ids": _sorted_unique(index.catalog_ids["units"]),
            "axis_ids": _sorted_unique(index.catalog_ids["dimensions"]),
            "member_ids": _sorted_unique(index.catalog_ids["dimension_members"]),
        },
        validation_results=(),
    )
    parity = product.parity_report()
    if parity["comparison_scope"]["state"] == "declared":
        if parity["unregistered_difference_count"]:
            raise PromiseProgressProjectionError(
                "Legacy/source-native parity comparison contains unregistered differences."
            )
        if parity["unused_accepted_difference_bindings"]:
            raise PromiseProgressProjectionError(
                "Legacy/source-native parity comparison contains unused accepted-difference bindings."
            )
        if parity["unused_registered_exception_ids"]:
            raise PromiseProgressProjectionError(
                "Legacy/source-native parity comparison contains unused exception-policy definitions."
            )
    issues = validate_promise_progress_product(product, package=package, plan=plan, replay=False)
    if issues:
        raise PromiseProgressProjectionError(f"Projected product failed internal validation: {issues!r}.")
    return product


def validate_promise_progress_product(
    product: PromiseProgressProduct,
    *,
    package: Mapping[str, Any] | None = None,
    plan: Mapping[str, Any] | None = None,
    replay: bool = True,
) -> list[dict[str, Any]]:
    """Validate cross-references and optionally replay all selections from source."""

    issues: list[dict[str, Any]] = list(validate_shadow_matrix(product.shadow_matrix()))
    rows = product.ordered_rows
    fields = product.fields
    field_ids = {field.field_id for field in fields}
    row_ids = {row.row_id for row in rows}
    actual_ids = {actual.actual_id for actual in product.actuals}
    progress_ids = {progress.progress_id for progress in product.progress_values}
    status_ids = {status.status_assessment_id for status in product.status_assessments}
    rows_by_id = {row.row_id: row for row in rows}
    package_index = _build_indexes(package) if package is not None else None
    if len(field_ids) != len(fields):
        issues.append({"rule_id": "promise_progress_duplicate_field", "message": "Field identities are not unique."})
    if len(row_ids) != len(rows):
        issues.append({"rule_id": "promise_progress_duplicate_row", "message": "Row identities are not unique."})
    if any(set(row.to_shadow_dict()["field_ids"]) - field_ids for row in rows):
        issues.append({"rule_id": "promise_progress_missing_field", "message": "A row references a missing field."})
    for field_value in fields:
        row = rows_by_id.get(field_value.row_id)
        if row is not None:
            layout = {role: columns for role, *columns in BLOCK_FIELD_LAYOUT[field_value.block_id]}
            anchor_column, display_columns = layout[field_value.field_role]
            expected_anchor = f"{anchor_column}{row.visible_sheet_row}"
            expected_range = (
                f"{display_columns}{row.visible_sheet_row}"
                if ":" not in display_columns
                else f"{display_columns.split(':')[0]}{row.visible_sheet_row}:{display_columns.split(':')[1]}{row.visible_sheet_row}"
            )
            if field_value.anchor_cell != expected_anchor or field_value.display_range != expected_range:
                issues.append(
                    {
                        "rule_id": "promise_progress_destination_mapping",
                        "message": f"Field {field_value.field_id} differs from the locked block destination mapping.",
                    }
                )
        if field_value.actual_selection_id and field_value.actual_selection_id not in actual_ids:
            issues.append({"rule_id": "promise_progress_actual_reference", "message": f"Field {field_value.field_id} references an unowned Actual selection."})
        if field_value.progress_selection_id and field_value.progress_selection_id not in progress_ids:
            issues.append({"rule_id": "promise_progress_progress_reference", "message": f"Field {field_value.field_id} references an unowned Progress selection."})
        if field_value.status_assessment_id and field_value.status_assessment_id not in status_ids:
            issues.append({"rule_id": "promise_progress_status_reference", "message": f"Field {field_value.field_id} references an unowned Status assessment."})
        if any(_parse_date(value, label="field knowledge_date") > _parse_date(field_value.ui_as_of_date, label="field cutoff") for value in field_value.knowledge_dates):
            issues.append({"rule_id": "promise_progress_temporal_leakage", "message": f"Field {field_value.field_id} leaks later knowledge."})
        if package_index is not None:
            cutoff = _parse_date(field_value.ui_as_of_date, label="field cutoff")
            unknown_records = set(field_value.canonical_record_ids) - (
                set(package_index.entities) | set(package_index.observations)
            )
            if unknown_records:
                issues.append(
                    {
                        "rule_id": "promise_progress_missing_canonical_input",
                        "message": f"Field {field_value.field_id} references unknown canonical records: {sorted(unknown_records)!r}.",
                    }
                )
            try:
                _, replayed_document_ids = _source_ids(package_index, field_value.source_occurrence_ids)
                if replayed_document_ids != field_value.source_document_ids:
                    issues.append(
                        {
                            "rule_id": "promise_progress_source_lineage",
                            "message": f"Field {field_value.field_id} source documents do not replay from its occurrences.",
                        }
                    )
            except PromiseProgressProjectionError as exc:
                issues.append({"rule_id": "promise_progress_source_lineage", "message": str(exc)})
            for document_id in field_value.source_document_ids:
                document = package_index.documents.get(document_id)
                if document is None:
                    issues.append({"rule_id": "promise_progress_missing_source", "message": f"Field {field_value.field_id} references a missing source document."})
                    continue
                publication_date = document.get("publication_date")
                if publication_date and _parse_date(str(publication_date), label="source publication_date") > cutoff:
                    issues.append({"rule_id": "promise_progress_temporal_leakage", "message": f"Field {field_value.field_id} uses a source document published after its row as-of date."})
        if set(field_value.parity_exception_ids) - CLOSED_PARITY_EXCEPTION_IDS:
            issues.append({"rule_id": "promise_progress_unknown_exception", "message": f"Field {field_value.field_id} uses an unregistered parity exception."})
        for exception_id in field_value.parity_exception_ids:
            if (field_value.block_id, field_value.field_role) not in _PARITY_FIELD_SCOPES[exception_id]:
                issues.append(
                    {
                        "rule_id": "promise_progress_exception_scope",
                        "message": f"Field {field_value.field_id} applies parity exception {exception_id!r} outside its closed field scope.",
                    }
                )
        recalculated = _selection_lineage(
            "field",
            {
                "product_id": field_value.product_id,
                "block_id": field_value.block_id,
                "row_id": field_value.row_id,
                "field_id": field_value.field_id,
                "field_role": field_value.field_role,
                "anchor_cell": field_value.anchor_cell,
                "display_range": field_value.display_range,
                "display": field_value.display_value.to_dict(),
                "canonical_record_ids": field_value.canonical_record_ids,
                "target_version_id": field_value.target_version_id,
                "actual_id": field_value.actual_selection_id,
                "progress_id": field_value.progress_selection_id,
                "status_id": field_value.status_assessment_id,
                "semantic_identity": field_value.semantic_identity.to_dict(),
                "period_or_horizon_id": field_value.period_or_horizon_id,
                "ui_as_of_date": field_value.ui_as_of_date,
                "knowledge_dates": field_value.knowledge_dates,
                "source_occurrence_ids": field_value.source_occurrence_ids,
                "source_document_ids": field_value.source_document_ids,
                "method_id": field_value.selection_or_calculation_method_id,
                "review_issue_ids": field_value.review_issue_ids,
                "parity_exception_ids": field_value.parity_exception_ids,
            },
        )
        if recalculated != field_value.lineage_digest:
            issues.append({"rule_id": "promise_progress_lineage_digest", "message": f"Field {field_value.field_id} has a stale lineage digest."})
    field_exception_usage = {item for field_value in fields for item in field_value.parity_exception_ids}
    parity = product.parity_report()
    authorized_exception_usage = {
        row["exception_id"]
        for row in parity["field_comparisons"]
        if row["classification"] == "registered-authorized-exception"
        and row["exception_id"] is not None
    }
    expected_exception_usage = (
        field_exception_usage
        | set(product.structural_parity_exception_ids)
        | authorized_exception_usage
    )
    if set(product.applied_parity_exception_ids) != expected_exception_usage:
        issues.append({"rule_id": "promise_progress_exception_usage", "message": "Product parity exception summary differs from field-level usage."})
    if set(product.structural_parity_exception_ids) - _STRUCTURAL_PARITY_EXCEPTION_IDS:
        issues.append({"rule_id": "promise_progress_structural_exception", "message": "Product uses an invalid structural parity exception."})
    if plan is not None:
        declared_exceptions = set(plan.get("parity_exception_ids", ()))
        undeclared = set(product.applied_parity_exception_ids) - declared_exceptions
        if undeclared:
            issues.append(
                {
                    "rule_id": "promise_progress_exception_not_activated",
                    "message": f"Product applies parity exceptions not activated by its reviewed plan: {sorted(undeclared)!r}.",
                }
            )
    if package is not None and plan is not None and replay:
        rebuilt = build_promise_progress_product(package, plan)
        if serialize_promise_progress_product(rebuilt) != serialize_promise_progress_product(product):
            issues.append({"rule_id": "promise_progress_semantic_replay", "message": "Stored product differs from independent source/package replay."})
    return issues
