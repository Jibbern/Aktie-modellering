"""Ticker-neutral investor product for source-native Operating Drivers.

The module deliberately contains no ticker-specific economic branches.  A
declarative profile supplies accepted observations, display selection, and
source references; this shared layer validates continuity, computes only
compatible comparisons, and emits the frozen three-section investor product.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
import hashlib
import json
from typing import Any, Mapping, Sequence


PRODUCT_CONTRACT = "operating-drivers-cross-ticker-investor-product@1"
PRESENTATION_CONTRACT = "operating-drivers-anf-frozen-visible-contract@1"
COMPARISON_CONTRACT = "typed-compatible-period-comparison@1"
SAFE_SUM_CONTRACT = "complete-compatible-quarter-sum@1"


class OperatingDriverCrossTickerError(ValueError):
    """Raised when a declarative profile violates the accepted boundaries."""


def _digest(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=lambda item: format(item, "f") if isinstance(item, Decimal) else str(item),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


@dataclass(frozen=True)
class SourceDocument:
    source_id: str
    source_type: str
    period_label: str
    source_location: str
    source_url: str | None
    local_path: str | None
    official: bool
    review_disposition: str


@dataclass(frozen=True)
class DriverObservation:
    observation_id: str
    driver_id: str
    period_label: str
    period_basis: str
    value: Decimal | None
    display_value: str
    precision: str
    status: str
    unit: str
    definition_id: str
    source_ids: tuple[str, ...]
    derivation_id: str | None = None
    lineage_references: tuple[str, ...] = ()


@dataclass(frozen=True)
class CoreDriverView:
    core_id: str
    group_label: str
    label: str
    driver_id: str
    latest_value: Decimal | None
    latest_display: str
    qoq_value: Decimal | None
    qoq_display: str
    yoy_value: Decimal | None
    yoy_display: str
    qoq_status: str
    yoy_status: str
    unit: str
    comparison_kind: str
    broader_trend: str
    why_it_matters: str
    source_references: tuple[str, ...]


@dataclass(frozen=True)
class HistoryRowView:
    group_label: str
    driver_id: str
    label: str
    unit: str
    points: tuple[DriverObservation, ...]


@dataclass(frozen=True)
class OverviewStatement:
    statement_id: str
    subsection: str
    text: str
    source_references: tuple[str, ...]


@dataclass(frozen=True)
class DriverGuideTerm:
    term: str
    meaning: str
    economic_role: str
    definition_authority: str
    source_references: tuple[str, ...]


@dataclass(frozen=True)
class SafeDerivation:
    derivation_id: str
    driver_id: str
    result_period_label: str
    result_value: Decimal | None
    result_status: str
    contract_version: str
    input_observation_ids: tuple[str, ...]


@dataclass(frozen=True)
class CrossTickerOperatingDriverPackage:
    ticker: str
    company_name: str
    latest_period_label: str
    quarter_labels: tuple[str, ...]
    product_contract: str
    presentation_contract: str
    overview: tuple[OverviewStatement, ...]
    core_drivers: tuple[CoreDriverView, ...]
    history_rows: tuple[HistoryRowView, ...]
    guide_terms: tuple[DriverGuideTerm, ...]
    source_documents: tuple[SourceDocument, ...]
    driver_registry: tuple[Mapping[str, Any], ...]
    observations: tuple[DriverObservation, ...]
    safe_derivations: tuple[SafeDerivation, ...]
    package_sha256: str

    def to_dict(self) -> dict[str, Any]:
        def normalize(value: Any) -> Any:
            if isinstance(value, Decimal):
                return format(value, "f")
            if isinstance(value, tuple):
                return [normalize(item) for item in value]
            if isinstance(value, list):
                return [normalize(item) for item in value]
            if isinstance(value, dict):
                return {key: normalize(item) for key, item in value.items()}
            return value

        return normalize(asdict(self))


def _observation_from_mapping(item: Mapping[str, Any]) -> DriverObservation:
    return DriverObservation(
        observation_id=str(item["observation_id"]),
        driver_id=str(item["driver_id"]),
        period_label=str(item["period_label"]),
        period_basis=str(item.get("period_basis", "QUARTER_ACTUAL")),
        value=_decimal(item.get("value")),
        display_value=str(item.get("display_value", "")),
        precision=str(item.get("precision", "EXACT")),
        status=str(item.get("status", "AVAILABLE")),
        unit=str(item["unit"]),
        definition_id=str(item["definition_id"]),
        source_ids=tuple(str(value) for value in item.get("source_ids", ())),
        derivation_id=(None if item.get("derivation_id") is None else str(item["derivation_id"])),
        lineage_references=tuple(str(value) for value in item.get("lineage_references", ())),
    )


def _compatible_delta(
    current: DriverObservation,
    prior: DriverObservation | None,
    *,
    comparison_kind: str,
) -> tuple[Decimal | None, str, str]:
    if prior is None or prior.status != "AVAILABLE" or prior.value is None:
        return None, "", "UNAVAILABLE_MISSING_PERIOD"
    if current.definition_id != prior.definition_id or current.unit != prior.unit:
        return None, "", "UNAVAILABLE_DEFINITION_BREAK"
    if current.value is None or current.status != "AVAILABLE":
        return None, "", "UNAVAILABLE_CURRENT"
    delta = current.value - prior.value
    if comparison_kind == "PERCENTAGE_POINT":
        display = f"{delta:+g} pp"
    elif comparison_kind == "COUNT":
        display = f"{delta:+g}"
    elif comparison_kind == "AMOUNT":
        display = f"{delta:+g}"
    else:
        display = f"{delta:+g}"
    return delta, display, "AVAILABLE"


def _complete_sum(
    driver_id: str,
    period_label: str,
    inputs: Sequence[DriverObservation],
    *,
    expected_count: int,
) -> SafeDerivation:
    derivation_id = f"derivation:{driver_id}:{period_label}:complete-quarter-sum@1"
    valid = (
        len(inputs) == expected_count
        and all(item.driver_id == driver_id for item in inputs)
        and all(item.value is not None and item.status == "AVAILABLE" and item.precision == "EXACT" for item in inputs)
        and len({item.unit for item in inputs}) == 1
        and len({item.definition_id for item in inputs}) == 1
    )
    return SafeDerivation(
        derivation_id=derivation_id,
        driver_id=driver_id,
        result_period_label=period_label,
        result_value=(sum((item.value for item in inputs if item.value is not None), Decimal("0")) if valid else None),
        result_status="AVAILABLE" if valid else "UNAVAILABLE_INCOMPLETE_PERIOD_SET",
        contract_version=SAFE_SUM_CONTRACT,
        input_observation_ids=tuple(item.observation_id for item in inputs),
    )


def build_cross_ticker_operating_driver_package(
    profile: Mapping[str, Any],
) -> CrossTickerOperatingDriverPackage:
    """Build and validate a frozen investor product from a declarative profile."""

    quarter_labels = tuple(str(value) for value in profile["quarter_labels"])
    if len(quarter_labels) != 12 or len(set(quarter_labels)) != 12:
        raise OperatingDriverCrossTickerError("The shared history contract requires 12 distinct fiscal quarters.")
    latest_period = str(profile["latest_period_label"])
    if quarter_labels[-1] != latest_period:
        raise OperatingDriverCrossTickerError("Latest period must be the final typed history quarter.")

    sources = tuple(SourceDocument(**item) for item in profile["source_documents"])
    source_ids = {item.source_id for item in sources}
    if len(source_ids) != len(sources) or any(not item.official for item in sources):
        raise OperatingDriverCrossTickerError("Source identities must be unique and official in this pass.")

    observations = tuple(_observation_from_mapping(item) for item in profile["observations"])
    observation_keys = {(item.driver_id, item.period_label) for item in observations}
    if len(observation_keys) != len(observations):
        raise OperatingDriverCrossTickerError("Duplicate driver/period observation identity.")
    if any(set(item.source_ids) - source_ids for item in observations):
        raise OperatingDriverCrossTickerError("An observation references an uncensused source.")
    if any(item.precision == "EXACT" and item.status == "AVAILABLE" and item.value is None for item in observations):
        raise OperatingDriverCrossTickerError("Available exact observations must carry numeric values.")
    if any(item.precision != "EXACT" and item.value is not None for item in observations):
        raise OperatingDriverCrossTickerError("Approximate or qualitative observations may not carry exact numbers.")

    by_key = {(item.driver_id, item.period_label): item for item in observations}
    history_rows: list[HistoryRowView] = []
    for row in profile["history_rows"]:
        points: list[DriverObservation] = []
        for period_label in quarter_labels:
            point = by_key.get((str(row["driver_id"]), period_label))
            if point is None:
                point = DriverObservation(
                    observation_id=f"missing:{row['driver_id']}:{period_label}",
                    driver_id=str(row["driver_id"]),
                    period_label=period_label,
                    period_basis="QUARTER_ACTUAL",
                    value=None,
                    display_value="",
                    precision="EXACT",
                    status="NOT_DISCLOSED",
                    unit=str(row["unit"]),
                    definition_id=str(row["definition_id"]),
                    source_ids=(),
                )
            points.append(point)
        history_rows.append(
            HistoryRowView(
                group_label=str(row["group_label"]),
                driver_id=str(row["driver_id"]),
                label=str(row["label"]),
                unit=str(row["unit"]),
                points=tuple(points),
            )
        )

    prior_quarter = quarter_labels[-2]
    prior_year = quarter_labels[-5]
    core_drivers: list[CoreDriverView] = []
    for spec in profile["core_drivers"]:
        driver_id = str(spec["driver_id"])
        current = by_key.get((driver_id, latest_period))
        if current is None:
            if not bool(spec.get("allow_missing_latest", False)):
                raise OperatingDriverCrossTickerError(f"Core driver {driver_id!r} lacks a latest observation.")
            historical = tuple(item for item in observations if item.driver_id == driver_id)
            if not historical:
                raise OperatingDriverCrossTickerError(
                    f"Fail-closed Core driver {driver_id!r} has no source-backed history."
                )
            unit = str(spec.get("unit", historical[-1].unit))
            source_references = tuple(
                dict.fromkeys(source_id for item in historical for source_id in item.source_ids)
            )
            core_drivers.append(
                CoreDriverView(
                    core_id=str(spec["core_id"]),
                    group_label=str(spec["group_label"]),
                    label=str(spec["label"]),
                    driver_id=driver_id,
                    latest_value=None,
                    latest_display=str(spec.get("latest_display", "")),
                    qoq_value=None,
                    qoq_display="",
                    yoy_value=None,
                    yoy_display="",
                    qoq_status="UNAVAILABLE_CURRENT",
                    yoy_status="UNAVAILABLE_CURRENT",
                    unit=unit,
                    comparison_kind=str(spec.get("comparison_kind", "NONE")),
                    broader_trend=str(spec["broader_trend"]),
                    why_it_matters=str(spec["why_it_matters"]),
                    source_references=source_references,
                )
            )
            continue
        comparison_kind = str(spec.get("comparison_kind", "NONE"))
        if current.precision == "EXACT":
            qoq_value, qoq_display, qoq_status = _compatible_delta(
                current,
                by_key.get((driver_id, prior_quarter)),
                comparison_kind=comparison_kind,
            )
            yoy_value, yoy_display, yoy_status = _compatible_delta(
                current,
                by_key.get((driver_id, prior_year)),
                comparison_kind=comparison_kind,
            )
        else:
            qoq_value = yoy_value = None
            qoq_display = str(spec.get("qoq_display", ""))
            yoy_display = str(spec.get("yoy_display", ""))
            qoq_status = str(spec.get("qoq_status", "UNAVAILABLE_NON_NUMERIC"))
            yoy_status = str(spec.get("yoy_status", "UNAVAILABLE_NON_NUMERIC"))
        core_drivers.append(
            CoreDriverView(
                core_id=str(spec["core_id"]),
                group_label=str(spec["group_label"]),
                label=str(spec["label"]),
                driver_id=driver_id,
                latest_value=current.value,
                latest_display=current.display_value,
                qoq_value=qoq_value,
                qoq_display=qoq_display,
                yoy_value=yoy_value,
                yoy_display=yoy_display,
                qoq_status=qoq_status,
                yoy_status=yoy_status,
                unit=current.unit,
                comparison_kind=comparison_kind,
                broader_trend=str(spec["broader_trend"]),
                why_it_matters=str(spec["why_it_matters"]),
                source_references=current.source_ids,
            )
        )

    safe_derivations: list[SafeDerivation] = []
    for spec in profile.get("safe_sum_derivations", ()):
        inputs = [
            by_key[(str(spec["driver_id"]), str(period))]
            for period in spec["input_periods"]
            if (str(spec["driver_id"]), str(period)) in by_key
        ]
        safe_derivations.append(
            _complete_sum(
                str(spec["driver_id"]),
                str(spec["result_period_label"]),
                inputs,
                expected_count=len(spec["input_periods"]),
            )
        )

    overview = tuple(OverviewStatement(**item) for item in profile["overview"])
    allowed_subsections = {"OPERATING INTERPRETATION", "LATEST QUARTER", "BROADER TREND"}
    if {item.subsection for item in overview} != allowed_subsections:
        raise OperatingDriverCrossTickerError("Overview must retain the frozen three-subsection contract.")
    forbidden = ("management said", "management expects", "we expect", "guidance implies")
    if any(token in item.text.lower() for token in forbidden for item in overview):
        raise OperatingDriverCrossTickerError("Management commentary or forecast language leaked into the product.")

    guide_terms = tuple(DriverGuideTerm(**item) for item in profile.get("guide_terms", ()))
    if any(not item.meaning.strip() or not item.economic_role.strip() for item in guide_terms):
        raise OperatingDriverCrossTickerError("Visible guide rows require meaning and economic role.")

    payload = {
        "ticker": str(profile["ticker"]),
        "company_name": str(profile["company_name"]),
        "latest_period_label": latest_period,
        "quarter_labels": list(quarter_labels),
        "product_contract": PRODUCT_CONTRACT,
        "presentation_contract": PRESENTATION_CONTRACT,
        "overview": [asdict(item) for item in overview],
        "core_drivers": [asdict(item) for item in core_drivers],
        "history_rows": [asdict(item) for item in history_rows],
        "guide_terms": [asdict(item) for item in guide_terms],
        "source_documents": [asdict(item) for item in sources],
        "driver_registry": list(profile["driver_registry"]),
        "observations": [asdict(item) for item in observations],
        "safe_derivations": [asdict(item) for item in safe_derivations],
    }
    return CrossTickerOperatingDriverPackage(
        ticker=str(profile["ticker"]),
        company_name=str(profile["company_name"]),
        latest_period_label=latest_period,
        quarter_labels=quarter_labels,
        product_contract=PRODUCT_CONTRACT,
        presentation_contract=PRESENTATION_CONTRACT,
        overview=overview,
        core_drivers=tuple(core_drivers),
        history_rows=tuple(history_rows),
        guide_terms=guide_terms,
        source_documents=sources,
        driver_registry=tuple(dict(item) for item in profile["driver_registry"]),
        observations=observations,
        safe_derivations=tuple(safe_derivations),
        package_sha256=_digest(payload),
    )


__all__ = [
    "COMPARISON_CONTRACT",
    "CrossTickerOperatingDriverPackage",
    "DriverGuideTerm",
    "DriverObservation",
    "HistoryRowView",
    "OperatingDriverCrossTickerError",
    "PRESENTATION_CONTRACT",
    "PRODUCT_CONTRACT",
    "SAFE_SUM_CONTRACT",
    "SafeDerivation",
    "build_cross_ticker_operating_driver_package",
]
