"""ANF investor presentation over the accepted Operating Drivers layers.

The V4 three-section composition is retained. This module consumes the
corrected canonical shadow, analytics, context semantics, story selection, and
typed period-repair census. It owns presentation language only: Quarter Notes
still owns management commentary and Investment Case still owns forward
assumptions.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
import hashlib
import json
from typing import Any, Iterable, Mapping, Sequence


INVESTOR_LANGUAGE_CONTRACT = "operating-drivers-investor-language@3"
ANF_UI_CONTRACT = "operating-drivers-anf-blank-surface-v4@7"
STORE_COUNT_ROLL_FORWARD_CONTRACT = "store-count-roll-forward@1"
STORE_COUNT_PERIOD_COMPARISON_CONTRACT = "store-count-period-comparison@1"
APPROXIMATE_RANGE_DIRECTION_CONTRACT = "approximate-range-direction@1"
FOOTPRINT_DEFINITION_CONTRACT = "footprint-definitions-and-economics@1"
FOOTPRINT_ECONOMIC_SUPPORT_CONTRACT = "footprint-economic-support@1"
FOOTPRINT_CONTEXT_RELATIONSHIP_CONTRACT = "footprint-context-relationships@1"
PLAN_ORIGIN = "BLANK_SURFACE_V4"
VISIBLE_MAJOR_SECTIONS = (
    "Operating Drivers Overview",
    "Core Drivers",
    "Quarterly Driver History",
)
LATEST_SUBSECTION = "LATEST QUARTER"
BROADER_SUBSECTION = "BROADER TREND"
INTERPRETATION_SUBSECTION = "OPERATING INTERPRETATION"
QUARTER_COUNT = 12

_TOTAL_COMPANY = "member:operating-driver:total-company@1"
_ABERCROMBIE = "member:operating-driver:abercrombie@1"
_HOLLISTER = "member:operating-driver:hollister@1"
_AMERICAS = "member:operating-driver:americas@1"
_EMEA = "member:operating-driver:emea@1"
_APAC = "member:operating-driver:apac@1"

_PERCENT_UNIT = "unit:core:percent@1"
_STORE_UNIT = "unit:operating-driver:stores@1"
_USD_MILLION_UNIT = "unit:core:usd-million@1"

_ANF_FY2025_10K = "https://www.sec.gov/Archives/edgar/data/1018840/000101884026000012/anf-20260131.htm"
_ANF_Q1_2026_10Q = "https://www.sec.gov/Archives/edgar/data/1018840/000101884026000036/anf-20260502.htm"
_ANF_FY2024_10K = "https://www.sec.gov/Archives/edgar/data/1018840/000101884025000013/anf-20250201.htm"
_ANF_FY2019_10K = "https://www.sec.gov/Archives/edgar/data/1018840/000101884020000021/a201910-k.htm"
_ANF_FY2017_10K = "https://www.sec.gov/Archives/edgar/data/1018840/000101884018000018/a201710-k.htm"
_ANF_2018_INVESTOR_DAY = "https://www.sec.gov/Archives/edgar/data/1018840/000101884018000023/anfusqtranscript20180425.htm"


class OperatingDriverAnfUIV4Error(ValueError):
    """Raised when accepted source-native inputs cannot support the V4 UI."""


def _digest(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class InvestorStatement:
    statement_id: str
    subsection: str
    text: str
    constructs: tuple[str, ...]
    source_references: tuple[str, ...]


@dataclass(frozen=True)
class HistoryPoint:
    period_label: str
    period_id: str
    value: str | None
    display_value: str
    source_observation_id: str | None
    source_evidence_id: str | None
    derivation_id: str | None = None
    lineage_references: tuple[str, ...] = ()
    precision: str | None = None


@dataclass(frozen=True)
class StoreCountRollForward:
    period_label: str
    value: str
    derivation_id: str
    prior_period_label: str
    prior_store_reference: str
    new_store_fact_id: str
    closed_store_fact_id: str
    direct_store_fact_id: str | None
    direct_anchor_match: bool | None
    lineage_references: tuple[str, ...]


@dataclass(frozen=True)
class CoreDriver:
    core_id: str
    group_label: str
    driver_id: str
    dimension_member_id: str
    definition_id: str
    unit_id: str
    label: str
    latest_period_label: str
    latest_value: str | None
    latest_display: str
    qoq_value: str | None
    qoq_display: str
    yoy_value: str | None
    yoy_display: str
    trend_disposition: str
    trend_fallback_display: str
    sparkline_eligible: bool
    sparkline_point_count: int
    why_it_matters: str
    source_references: tuple[str, ...]
    qoq_status: str = "AVAILABLE"
    yoy_status: str = "AVAILABLE"
    qoq_lineage_references: tuple[str, ...] = ()
    yoy_lineage_references: tuple[str, ...] = ()
    qoq_comparison_contract: str | None = None
    yoy_comparison_contract: str | None = None


@dataclass(frozen=True)
class ApproximateRangeComparison:
    contract_version: str
    metric_label: str
    current_period_label: str
    prior_period_label: str
    current_category: str
    prior_category: str
    direction: str
    display_text: str
    source_references: tuple[str, ...]


@dataclass(frozen=True)
class CombinedStoreActivityEvidence:
    fact_id: str
    period_label: str
    period_basis: str
    actual_or_guidance: str
    precision: str
    display_value: str
    definition_id: str
    source_document_id: str


@dataclass(frozen=True)
class FootprintDefinition:
    term: str
    meaning: str
    authority: str
    measurement: str
    measurement_authorities: tuple[str, ...]
    economic_role: str
    economic_role_authority: str
    economic_role_type: str
    driver_id: str
    definition_id: str
    source_references: tuple[str, ...]


@dataclass(frozen=True)
class FootprintEconomicSupport:
    support_id: str
    terms: tuple[str, ...]
    period_label: str
    support_type: str
    evidence_summary: str
    authority: str
    source_document_id: str
    source_url: str
    source_location: str
    current_period_metric_owner: bool = False


@dataclass(frozen=True)
class FootprintContextRelationship:
    relationship_id: str
    trigger_terms: tuple[str, ...]
    interpretation: str
    semantic_type: str
    source_references: tuple[str, ...]


@dataclass(frozen=True)
class HistoryRow:
    group_label: str
    driver_id: str
    dimension_member_id: str
    definition_id: str
    unit_id: str
    label: str
    display_role: str
    points: tuple[HistoryPoint, ...]


@dataclass(frozen=True)
class OperatingDriverAnfUIV4Package:
    ticker: str
    contract_version: str
    language_contract: str
    plan_origin: str
    major_sections: tuple[str, ...]
    latest_period_label: str
    quarter_labels: tuple[str, ...]
    overview: tuple[InvestorStatement, ...]
    core_drivers: tuple[CoreDriver, ...]
    history_rows: tuple[HistoryRow, ...]
    footprint_definitions: tuple[FootprintDefinition, ...]
    footprint_economic_support: tuple[FootprintEconomicSupport, ...]
    footprint_context_relationships: tuple[FootprintContextRelationship, ...]
    store_count_roll_forward_note: str
    store_count_roll_forward_note_sources: tuple[str, ...]
    source_contracts: Mapping[str, str]
    source_identity_receipts: Mapping[str, str]
    package_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class _Observation:
    driver_id: str
    member_ids: tuple[str, ...]
    definition_id: str
    definition_version: int
    unit_id: str
    fiscal_year: int
    fiscal_quarter: int
    period_id: str
    period_label: str
    value: Decimal
    observation_id: str
    evidence_id: str


def _member_ids(raw: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(sorted(str(item["member_id"]) for item in raw.get("dimensions", ())))


def _quarter_label(year: int, quarter: int) -> str:
    return f"{year}-Q{quarter}"


def _previous_quarter(year: int, quarter: int) -> tuple[int, int]:
    return (year - 1, 4) if quarter == 1 else (year, quarter - 1)


def _next_quarter(year: int, quarter: int) -> tuple[int, int]:
    return (year + 1, 1) if quarter == 4 else (year, quarter + 1)


def _quarter_window(year: int, quarter: int, count: int) -> tuple[tuple[int, int], ...]:
    result: list[tuple[int, int]] = []
    current = (year, quarter)
    for _ in range(count):
        result.append(current)
        current = _previous_quarter(*current)
    return tuple(reversed(result))


def _load_observations(source: Mapping[str, Any]) -> tuple[_Observation, ...]:
    result: list[_Observation] = []
    for raw in source["shadow"]["observations"]:
        evidence = raw["evidence"]
        driver = evidence["driver"]
        period = evidence["period"]
        if evidence.get("availability") != "AVAILABLE" or evidence.get("value_kind") != "NUMERIC":
            continue
        if period.get("period_kind") != "FISCAL_QUARTER":
            continue
        value = evidence.get("normalized_value")
        if value is None:
            continue
        result.append(
            _Observation(
                driver_id=str(driver["driver_id"]),
                member_ids=_member_ids(driver),
                definition_id=str(driver["definition_id"]),
                definition_version=int(driver["definition_version"]),
                unit_id=str(driver["unit_id"]),
                fiscal_year=int(period["fiscal_year"]),
                fiscal_quarter=int(period["fiscal_quarter"]),
                period_id=str(period["period_id"]),
                period_label=_quarter_label(int(period["fiscal_year"]), int(period["fiscal_quarter"])),
                value=Decimal(str(value)),
                observation_id=str(raw["observation_id"]),
                evidence_id=str(evidence["evidence_id"]),
            )
        )
    return tuple(result)


def _series(
    observations: Sequence[_Observation], driver_id: str, member_id: str
) -> tuple[_Observation, ...]:
    candidates = [
        item for item in observations
        if item.driver_id == driver_id and member_id in item.member_ids
    ]
    by_period: dict[tuple[int, int], _Observation] = {}
    for item in candidates:
        key = (item.fiscal_year, item.fiscal_quarter)
        existing = by_period.get(key)
        if existing is not None and (
            existing.value != item.value
            or existing.definition_id != item.definition_id
            or existing.unit_id != item.unit_id
        ):
            raise OperatingDriverAnfUIV4Error(
                f"Conflicting observations for {driver_id} {member_id} {item.period_label}."
            )
        by_period[key] = item
    ordered = tuple(by_period[key] for key in sorted(by_period))
    if ordered and len(
        {(item.definition_id, item.definition_version, item.unit_id) for item in ordered}
    ) != 1:
        raise OperatingDriverAnfUIV4Error(
            f"Definition or unit break requires a presentation boundary: {driver_id} {member_id}."
        )
    return ordered


def _analysis(
    source: Mapping[str, Any], collection: str, observation: _Observation
) -> Mapping[str, Any] | None:
    candidates = [
        item for item in source["analytics"].get(collection, ())
        if item.get("driver_id") == observation.driver_id
        and item.get("as_of_period_id") == observation.period_id
        and tuple(sorted(str(member["member_id"]) for member in item.get("dimensions", ())))
        == observation.member_ids
    ]
    if len(candidates) > 1:
        raise OperatingDriverAnfUIV4Error(
            f"Ambiguous {collection} analysis for {observation.driver_id} {observation.period_id}."
        )
    return candidates[0] if candidates else None


def _smart_decimal(value: Decimal) -> str:
    """Apply investor-facing precision without changing the source value."""

    rounded = value.quantize(Decimal("0.1"))
    if rounded == rounded.to_integral_value():
        return format(rounded.quantize(Decimal("1")), "f")
    return format(rounded.normalize(), "f")


def _display_number(value: Decimal, unit_id: str, *, signed: bool = False) -> str:
    if unit_id == _PERCENT_UNIT:
        prefix = "+" if signed and value > 0 else ""
        return f"{prefix}{_smart_decimal(value)}%"
    if unit_id == _STORE_UNIT:
        prefix = "+" if signed and value > 0 else ""
        rounded = value.quantize(Decimal("1"))
        unit = "store" if abs(rounded) == Decimal("1") else "stores"
        return f"{prefix}{rounded:,} {unit}"
    if unit_id == _USD_MILLION_UNIT:
        sign = "+" if signed and value > 0 else ""
        minus = "-" if value < 0 else ""
        return f"{sign}{minus}${_smart_decimal(abs(value))}m"
    prefix = "+" if signed and value > 0 else ""
    return f"{prefix}{value.normalize()}"


def _comparison_display(analysis: Mapping[str, Any] | None, unit_id: str) -> str:
    value = _comparison_value(analysis, unit_id)
    if value is None:
        return "—"
    if unit_id == _PERCENT_UNIT:
        prefix = "+" if value > 0 else ""
        return f"{prefix}{_smart_decimal(value)} pp"
    return _display_number(value, unit_id, signed=True)


def _comparison_value(
    analysis: Mapping[str, Any] | None, unit_id: str
) -> Decimal | None:
    if analysis is None or analysis.get("availability") != "AVAILABLE":
        return None
    key = "percentage_point_change" if unit_id == _PERCENT_UNIT else "native_unit_change"
    raw = analysis.get(key)
    return None if raw is None else Decimal(str(raw))


def _validated_fact_index(
    source: Mapping[str, Any],
    *,
    metric_label: str,
    driver_id: str,
    definition_id: str,
    period_basis: str,
) -> dict[tuple[int, int], Mapping[str, Any]]:
    """Return one exact, actual, total-company stores fact per fiscal quarter."""

    result: dict[tuple[int, int], Mapping[str, Any]] = {}
    for fact in _facts(source, metric_label):
        if fact.get("fiscal_year") is None or fact.get("fiscal_quarter") is None:
            continue
        if fact.get("period_basis") != period_basis:
            continue
        expected = {
            "actual_or_guidance": "ACTUAL",
            "canonical_driver_id": driver_id,
            "definition_id": definition_id,
            "definition_version": 1,
            "precision": "EXACT",
            "status": "AVAILABLE",
            "unit_id": _STORE_UNIT,
        }
        mismatches = {
            key: (fact.get(key), value)
            for key, value in expected.items()
            if fact.get(key) != value
        }
        if tuple(fact.get("dimension_member_ids", ())) != (_TOTAL_COMPANY,):
            mismatches["dimension_member_ids"] = (
                tuple(fact.get("dimension_member_ids", ())),
                (_TOTAL_COMPANY,),
            )
        if fact.get("value") is None:
            mismatches["value"] = (None, "exact numeric")
        if mismatches:
            raise OperatingDriverAnfUIV4Error(
                f"Incompatible {metric_label} fact for {fact.get('period_label')}: {mismatches}."
            )
        key = (int(fact["fiscal_year"]), int(fact["fiscal_quarter"]))
        existing = result.get(key)
        if existing is not None and (
            str(existing["value"]) != str(fact["value"])
            or str(existing["fact_id"]) != str(fact["fact_id"])
        ):
            raise OperatingDriverAnfUIV4Error(
                f"Conflicting {metric_label} facts for {_quarter_label(*key)}."
            )
        result[key] = fact
    return result


def derive_company_owned_store_roll_forward(
    source: Mapping[str, Any],
) -> tuple[StoreCountRollForward, ...]:
    """Reconcile company-owned ending stores through exact openings and closures.

    The derivation is accepted only when every quarter has compatible exact
    activity and every disclosed ending-store anchor matches the roll-forward.
    Direct ending-store observations remain authoritative.
    """

    direct = _validated_fact_index(
        source,
        metric_label="Company-owned stores, end",
        driver_id="driver:operating:company-owned-stores-end@1",
        definition_id="definition:operating-driver:company-owned-stores-end@1",
        period_basis="INSTANT_ACTUAL",
    )
    new_stores = _validated_fact_index(
        source,
        metric_label="New stores",
        driver_id="driver:operating:new-stores@1",
        definition_id="definition:operating-driver:new-stores@1",
        period_basis="QUARTER_ACTUAL",
    )
    closed_stores = _validated_fact_index(
        source,
        metric_label="Closed stores",
        driver_id="driver:operating:closed-stores@1",
        definition_id="definition:operating-driver:closed-stores@1",
        period_basis="QUARTER_ACTUAL",
    )
    if len(direct) < 2:
        raise OperatingDriverAnfUIV4Error(
            "STORE_COUNT_ROLL_FORWARD requires at least two direct ending-store anchors."
        )
    start = min(direct)
    end = max(direct)
    prior_value = Decimal(str(direct[start]["value"]))
    prior_reference = str(direct[start]["fact_id"])
    prior_period = start
    current = _next_quarter(*start)
    records: list[StoreCountRollForward] = []
    while current <= end:
        if current not in new_stores or current not in closed_stores:
            raise OperatingDriverAnfUIV4Error(
                f"STORE_COUNT_ROLL_FORWARD lacks exact activity for {_quarter_label(*current)}."
            )
        opened = new_stores[current]
        closed = closed_stores[current]
        implied = prior_value + Decimal(str(opened["value"])) - Decimal(str(closed["value"]))
        anchor = direct.get(current)
        anchor_match = None if anchor is None else implied == Decimal(str(anchor["value"]))
        if anchor_match is False:
            raise OperatingDriverAnfUIV4Error(
                f"STORE_COUNT_ROLL_FORWARD anchor mismatch for {_quarter_label(*current)}: "
                f"derived {implied} versus direct {anchor['value']}."
            )
        payload = {
            "contract": STORE_COUNT_ROLL_FORWARD_CONTRACT,
            "period": _quarter_label(*current),
            "prior_period": _quarter_label(*prior_period),
            "prior_reference": prior_reference,
            "new_store_fact_id": str(opened["fact_id"]),
            "closed_store_fact_id": str(closed["fact_id"]),
            "value": format(implied, "f"),
        }
        derivation_id = f"derivation:anf:company-owned-store-roll-forward:{_digest(payload)[:32]}"
        lineage = tuple(
            sorted(
                {
                    prior_reference,
                    str(opened["fact_id"]),
                    str(opened["source_document_id"]),
                    str(closed["fact_id"]),
                    str(closed["source_document_id"]),
                    *(
                        ()
                        if anchor is None
                        else (str(anchor["fact_id"]), str(anchor["source_document_id"]))
                    ),
                }
            )
        )
        records.append(
            StoreCountRollForward(
                period_label=_quarter_label(*current),
                value=format(implied, "f"),
                derivation_id=derivation_id,
                prior_period_label=_quarter_label(*prior_period),
                prior_store_reference=prior_reference,
                new_store_fact_id=str(opened["fact_id"]),
                closed_store_fact_id=str(closed["fact_id"]),
                direct_store_fact_id=None if anchor is None else str(anchor["fact_id"]),
                direct_anchor_match=anchor_match,
                lineage_references=lineage,
            )
        )
        prior_value = Decimal(str(anchor["value"])) if anchor is not None else implied
        prior_reference = str(anchor["fact_id"]) if anchor is not None else derivation_id
        prior_period = current
        current = _next_quarter(*current)
    return tuple(records)


def _company_owned_store_core(
    source: Mapping[str, Any],
    observations: Sequence[_Observation],
    quarter_window: Sequence[tuple[int, int]],
    *,
    latest_year: int,
    latest_quarter: int,
) -> CoreDriver:
    """Build typed quarter and prior-year store-count comparisons.

    Direct period-end observations remain authoritative. Missing compatible
    quarter-end observations may be supplied only by the already accepted,
    fully anchored store-count roll-forward contract.
    """

    driver_id = "driver:operating:company-owned-stores-end@1"
    direct = {
        (item.fiscal_year, item.fiscal_quarter): item
        for item in _series(observations, driver_id, _TOTAL_COMPANY)
    }
    reconciliations = {
        tuple(int(value) for value in record.period_label.replace("-Q", "-").split("-")): record
        for record in derive_company_owned_store_roll_forward(source)
    }

    def resolved(period: tuple[int, int]) -> tuple[Decimal, tuple[str, ...]]:
        direct_observation = direct.get(period)
        reconciliation = reconciliations.get(period)
        if direct_observation is None and reconciliation is None:
            raise OperatingDriverAnfUIV4Error(
                f"Company-owned store comparison lacks compatible exact evidence for {_quarter_label(*period)}."
            )
        if direct_observation is not None:
            references = {
                direct_observation.observation_id,
                direct_observation.evidence_id,
            }
            if reconciliation is not None:
                references.add(reconciliation.derivation_id)
                references.update(reconciliation.lineage_references)
            return direct_observation.value, tuple(sorted(references))
        assert reconciliation is not None
        return Decimal(reconciliation.value), tuple(
            sorted({reconciliation.derivation_id, *reconciliation.lineage_references})
        )

    current_period = (latest_year, latest_quarter)
    prior_period = _previous_quarter(*current_period)
    year_ago_period = (latest_year - 1, latest_quarter)
    current_value, current_refs = resolved(current_period)
    prior_value, prior_refs = resolved(prior_period)
    year_ago_value, year_ago_refs = resolved(year_ago_period)
    qoq_value = current_value - prior_value
    yoy_value = current_value - year_ago_value
    qoq_lineage = tuple(sorted({*current_refs, *prior_refs}))
    yoy_lineage = tuple(sorted({*current_refs, *year_ago_refs}))
    latest = direct.get(current_period)
    if latest is None:
        raise OperatingDriverAnfUIV4Error(
            "Latest company-owned store level must be a direct authoritative observation."
        )
    return CoreDriver(
        core_id="company-owned-stores",
        group_label="Store Footprint",
        driver_id=driver_id,
        dimension_member_id=_TOTAL_COMPANY,
        definition_id=latest.definition_id,
        unit_id=_STORE_UNIT,
        label="Company-owned stores",
        latest_period_label=latest.period_label,
        latest_value=format(current_value, "f"),
        latest_display=_display_number(current_value, _STORE_UNIT),
        qoq_value=format(qoq_value, "f"),
        qoq_display=_display_number(qoq_value, _STORE_UNIT, signed=True),
        yoy_value=format(yoy_value, "f"),
        yoy_display=_display_number(yoy_value, _STORE_UNIT, signed=True),
        trend_disposition="PLAIN_LANGUAGE",
        trend_fallback_display="Expanding",
        sparkline_eligible=False,
        sparkline_point_count=sum(period in quarter_window for period in {*direct, *reconciliations}),
        why_it_matters="Tracks the size of the physical selling footprint.",
        source_references=tuple(sorted({*current_refs, *prior_refs, *year_ago_refs})),
        qoq_status="AVAILABLE",
        yoy_status="AVAILABLE",
        qoq_lineage_references=qoq_lineage,
        yoy_lineage_references=yoy_lineage,
        qoq_comparison_contract=STORE_COUNT_PERIOD_COMPARISON_CONTRACT,
        yoy_comparison_contract=STORE_COUNT_PERIOD_COMPARISON_CONTRACT,
    )


def _history_points(
    series: Sequence[_Observation], quarter_window: Sequence[tuple[int, int]]
) -> tuple[HistoryPoint, ...]:
    by_period = {(item.fiscal_year, item.fiscal_quarter): item for item in series}
    result: list[HistoryPoint] = []
    for year, quarter in quarter_window:
        item = by_period.get((year, quarter))
        result.append(
            HistoryPoint(
                period_label=_quarter_label(year, quarter),
                period_id=f"period:anf:{year}-q{quarter}@1",
                value=None if item is None else format(item.value, "f"),
                display_value=(
                    "" if item is None
                    else _display_number(item.value, item.unit_id, signed=item.unit_id == _PERCENT_UNIT)
                ),
                source_observation_id=None if item is None else item.observation_id,
                source_evidence_id=None if item is None else item.evidence_id,
            )
        )
    return tuple(result)


def _core_driver(
    source: Mapping[str, Any],
    observations: Sequence[_Observation],
    quarter_window: Sequence[tuple[int, int]],
    *,
    core_id: str,
    group_label: str,
    driver_id: str,
    member_id: str,
    label: str,
    trend: str,
    why_it_matters: str,
) -> CoreDriver:
    values = _series(observations, driver_id, member_id)
    if not values:
        raise OperatingDriverAnfUIV4Error(f"Selected ANF core driver has no history: {core_id}.")
    latest = values[-1]
    qoq = _analysis(source, "qoq_analytics", latest)
    yoy = _analysis(source, "yoy_analytics", latest)
    qoq_value = _comparison_value(qoq, latest.unit_id)
    yoy_value = _comparison_value(yoy, latest.unit_id)
    references = [latest.observation_id, latest.evidence_id]
    for item in (qoq, yoy):
        if item is not None:
            references.append(str(item["analysis_id"]))
            references.extend(str(value) for value in item.get("source_evidence_ids", ()))
    point_count = sum(item.value is not None for item in _history_points(values, quarter_window))
    return CoreDriver(
        core_id=core_id,
        group_label=group_label,
        driver_id=driver_id,
        dimension_member_id=member_id,
        definition_id=latest.definition_id,
        unit_id=latest.unit_id,
        label=label,
        latest_period_label=latest.period_label,
        latest_value=format(latest.value, "f"),
        latest_display=_display_number(latest.value, latest.unit_id, signed=latest.unit_id == _PERCENT_UNIT),
        qoq_value=None if qoq_value is None else format(qoq_value, "f"),
        qoq_display=_comparison_display(qoq, latest.unit_id),
        yoy_value=None if yoy_value is None else format(yoy_value, "f"),
        yoy_display=_comparison_display(yoy, latest.unit_id),
        trend_disposition="PLAIN_LANGUAGE",
        trend_fallback_display=trend,
        sparkline_eligible=False,
        sparkline_point_count=point_count,
        why_it_matters=why_it_matters,
        source_references=tuple(sorted(set(references))),
    )


def _fact_layer(source: Mapping[str, Any]) -> Mapping[str, Any]:
    if "completeness" in source:
        return source["completeness"]
    if "period_repair" in source:
        return source["period_repair"]
    raise OperatingDriverAnfUIV4Error("ANF UI source is missing its accepted fact layer.")


def _facts(source: Mapping[str, Any], metric_label: str) -> tuple[Mapping[str, Any], ...]:
    fact_layer = _fact_layer(source)
    return tuple(
        sorted(
            (
                item for item in fact_layer["facts"]
                if item["metric_label"] == metric_label
            ),
            key=lambda item: (
                -1 if item.get("fiscal_year") is None else int(item["fiscal_year"]),
                -1 if item.get("fiscal_quarter") is None else int(item["fiscal_quarter"]),
                str(item["fact_id"]),
            ),
        )
    )


def _one_fact(
    source: Mapping[str, Any], metric_label: str, *, period_label: str
) -> Mapping[str, Any]:
    matches = [item for item in _facts(source, metric_label) if item["period_label"] == period_label]
    if len(matches) != 1:
        raise OperatingDriverAnfUIV4Error(
            f"Expected one {metric_label} fact for {period_label}, found {len(matches)}."
        )
    return matches[0]


def _context_inventory_core(
    source: Mapping[str, Any], *, latest_period: str, prior_period: str, year_ago_period: str
) -> CoreDriver:
    latest = _one_fact(source, "Inventory at cost", period_label=latest_period)
    prior = _one_fact(source, "Inventory at cost", period_label=prior_period)
    year_ago = _one_fact(source, "Inventory at cost", period_label=year_ago_period)
    current = Decimal(str(latest["value"]))
    references = tuple(
        sorted(
            {
                str(value)
                for item in (latest, prior, year_ago)
                for value in (item["fact_id"], item["source_document_id"])
            }
        )
    )
    return CoreDriver(
        core_id="inventory-at-cost",
        group_label="Inventory",
        driver_id="metric:summary-bs:inventory-at-cost@1",
        dimension_member_id=_TOTAL_COMPANY,
        definition_id=str(latest["definition_id"]),
        unit_id=_USD_MILLION_UNIT,
        label="Inventory at cost",
        latest_period_label=latest_period,
        latest_value=str(latest["value"]),
        latest_display=_display_number(current, _USD_MILLION_UNIT),
        qoq_value=format(current - Decimal(str(prior["value"])), "f"),
        qoq_display=_display_number(current - Decimal(str(prior["value"])), _USD_MILLION_UNIT, signed=True),
        yoy_value=format(current - Decimal(str(year_ago["value"])), "f"),
        yoy_display=_display_number(current - Decimal(str(year_ago["value"])), _USD_MILLION_UNIT, signed=True),
        trend_disposition="PLAIN_LANGUAGE",
        trend_fallback_display="Cost pressure easing",
        sparkline_eligible=False,
        sparkline_point_count=len(_facts(source, "Inventory at cost")),
        why_it_matters="Shows working-capital exposure and inventory discipline.",
        source_references=references,
    )


def _context_rate_core(
    source: Mapping[str, Any],
    *,
    core_id: str,
    group_label: str,
    metric_label: str,
    driver_id: str,
    definition_id: str,
    label: str,
    latest_period: str,
    prior_period: str,
    year_ago_period: str,
    trend: str,
    why_it_matters: str,
) -> CoreDriver:
    latest = _one_fact(source, metric_label, period_label=latest_period)
    prior = _one_fact(source, metric_label, period_label=prior_period)
    year_ago = _one_fact(source, metric_label, period_label=year_ago_period)
    for fact in (latest, prior, year_ago):
        if fact.get("period_basis") != "QUARTER_ACTUAL" or fact.get("value") is None:
            raise OperatingDriverAnfUIV4Error(
                f"{metric_label} requires exact compatible quarterly observations."
            )
    current = Decimal(str(latest["value"]))
    previous = Decimal(str(prior["value"]))
    year_ago_value = Decimal(str(year_ago["value"]))
    references = _fact_sources(latest, prior, year_ago)
    return CoreDriver(
        core_id=core_id,
        group_label=group_label,
        driver_id=driver_id,
        dimension_member_id=_TOTAL_COMPANY,
        definition_id=definition_id,
        unit_id=_PERCENT_UNIT,
        label=label,
        latest_period_label=latest_period,
        latest_value=str(latest["value"]),
        latest_display=_display_number(current, _PERCENT_UNIT, signed=True),
        qoq_value=format(current - previous, "f"),
        qoq_display=f"{_display_number(current - previous, _PERCENT_UNIT, signed=True)[:-1]} pp",
        yoy_value=format(current - year_ago_value, "f"),
        yoy_display=f"{_display_number(current - year_ago_value, _PERCENT_UNIT, signed=True)[:-1]} pp",
        trend_disposition="PLAIN_LANGUAGE",
        trend_fallback_display=trend,
        sparkline_eligible=False,
        sparkline_point_count=sum(
            fact.get("period_basis") == "QUARTER_ACTUAL" and fact.get("value") is not None
            for fact in _facts(source, metric_label)
        ),
        why_it_matters=why_it_matters,
        source_references=references,
    )


def _approximate_inventory_category(fact: Mapping[str, Any]) -> tuple[str, int]:
    if (
        fact.get("metric_label") != "Inventory unit growth"
        or fact.get("period_basis") != "APPROXIMATE_RANGE"
        or fact.get("actual_or_guidance") != "ACTUAL"
        or fact.get("precision") != "APPROXIMATE"
        or fact.get("status") != "AVAILABLE_APPROXIMATE_TEXT"
        or fact.get("value") is not None
        or tuple(fact.get("dimension_member_ids", ())) != (_TOTAL_COMPANY,)
    ):
        raise OperatingDriverAnfUIV4Error(
            f"Inventory-unit ordinal comparison rejects incompatible evidence for {fact.get('period_label')}."
        )
    normalized = str(fact.get("display_value", "")).casefold().replace("–", "-").replace("—", "-")
    if "around 1%" in normalized:
        return "AROUND_ONE_PERCENT", 0
    if "low single" in normalized:
        return "LOW_SINGLE_DIGIT", 1
    if "mid-single" in normalized or "mid single" in normalized:
        return "MID_SINGLE_DIGIT", 2
    raise OperatingDriverAnfUIV4Error(
        f"Unsupported approximate inventory-unit category for {fact.get('period_label')}."
    )


def derive_inventory_approximate_range_comparison(
    source: Mapping[str, Any], *, current_period: str, prior_period: str
) -> ApproximateRangeComparison:
    """Compare compatible source phrases ordinally without emitting a number."""

    current = _one_fact(source, "Inventory unit growth", period_label=current_period)
    prior = _one_fact(source, "Inventory unit growth", period_label=prior_period)
    if (
        current.get("definition_id") != prior.get("definition_id")
        or current.get("definition_version") != prior.get("definition_version")
        or current.get("unit_id") != prior.get("unit_id")
        or tuple(current.get("dimension_member_ids", ()))
        != tuple(prior.get("dimension_member_ids", ()))
    ):
        raise OperatingDriverAnfUIV4Error(
            "Inventory-unit ordinal comparison requires identical definition, unit, and dimensions."
        )
    current_category, current_rank = _approximate_inventory_category(current)
    prior_category, prior_rank = _approximate_inventory_category(prior)
    if current_rank < prior_rank:
        direction = "MODERATING"
    elif current_rank > prior_rank:
        direction = "ACCELERATING"
    else:
        direction = "STABLE_CATEGORY"
    prior_display = {
        "AROUND_ONE_PERCENT": "around 1%",
        "LOW_SINGLE_DIGIT": "low-single-digit",
        "MID_SINGLE_DIGIT": "mid-single-digit",
    }[prior_category]
    display_text = {
        "MODERATING": f"Down from {prior_display}",
        "ACCELERATING": f"Up from {prior_display}",
        "STABLE_CATEGORY": f"Similar to {prior_display}",
    }[direction]
    return ApproximateRangeComparison(
        contract_version=APPROXIMATE_RANGE_DIRECTION_CONTRACT,
        metric_label="Inventory unit growth",
        current_period_label=current_period,
        prior_period_label=prior_period,
        current_category=current_category,
        prior_category=prior_category,
        direction=direction,
        display_text=display_text,
        source_references=_fact_sources(current, prior),
    )


def _context_inventory_units_core(
    source: Mapping[str, Any], *, latest_period: str, prior_period: str
) -> CoreDriver:
    fact = _one_fact(source, "Inventory unit growth", period_label=latest_period)
    comparison = derive_inventory_approximate_range_comparison(
        source, current_period=latest_period, prior_period=prior_period
    )
    return CoreDriver(
        core_id="inventory-unit-growth",
        group_label="Inventory",
        driver_id="driver:operating:inventory-unit-growth@1",
        dimension_member_id=_TOTAL_COMPANY,
        definition_id=str(fact["definition_id"]),
        unit_id=_PERCENT_UNIT,
        label="Inventory units",
        latest_period_label=latest_period,
        latest_value=None,
        latest_display="Approx. low-single-digit YoY",
        qoq_value=None,
        qoq_display=comparison.display_text,
        yoy_value=None,
        yoy_display="—",
        trend_disposition="PLAIN_LANGUAGE",
        trend_fallback_display="Recent growth moderating",
        sparkline_eligible=False,
        sparkline_point_count=0,
        why_it_matters="Tests whether unit growth is aligned with demand.",
        source_references=tuple(
            sorted({str(fact["fact_id"]), str(fact["source_document_id"]), *comparison.source_references})
        ),
        qoq_status="AVAILABLE_ORDINAL",
        yoy_status="UNAVAILABLE_NOT_DISCLOSED",
        qoq_lineage_references=comparison.source_references,
        qoq_comparison_contract=APPROXIMATE_RANGE_DIRECTION_CONTRACT,
    )


def assess_combined_store_activity_evidence(
    source: Mapping[str, Any],
) -> tuple[CombinedStoreActivityEvidence, ...]:
    """Inventory accepted combined remodel/right-size evidence without splitting it."""

    records: list[CombinedStoreActivityEvidence] = []
    for fact in _facts(source, "Remodels and right-sizes guidance"):
        if fact.get("actual_or_guidance") != "GUIDANCE" or fact.get("period_basis") != "GUIDANCE":
            raise OperatingDriverAnfUIV4Error(
                "Accepted combined remodel/right-size evidence unexpectedly contains an actual observation."
            )
        records.append(
            CombinedStoreActivityEvidence(
                fact_id=str(fact["fact_id"]),
                period_label=str(fact["period_label"]),
                period_basis=str(fact["period_basis"]),
                actual_or_guidance=str(fact["actual_or_guidance"]),
                precision=str(fact["precision"]),
                display_value=str(fact["display_value"]),
                definition_id=str(fact["definition_id"]),
                source_document_id=str(fact["source_document_id"]),
            )
        )
    return tuple(records)


def _history_row(
    observations: Sequence[_Observation],
    quarter_window: Sequence[tuple[int, int]],
    *,
    group_label: str,
    driver_id: str,
    member_id: str,
    label: str,
    display_role: str,
) -> HistoryRow:
    values = _series(observations, driver_id, member_id)
    if not values:
        raise OperatingDriverAnfUIV4Error(f"History row has no source-native series: {label}.")
    return HistoryRow(
        group_label=group_label,
        driver_id=driver_id,
        dimension_member_id=member_id,
        definition_id=values[-1].definition_id,
        unit_id=values[-1].unit_id,
        label=label,
        display_role=display_role,
        points=_history_points(values, quarter_window),
    )


def _inventory_history(
    source: Mapping[str, Any], quarter_window: Sequence[tuple[int, int]]
) -> HistoryRow:
    by_period = {
        (int(item["fiscal_year"]), int(item["fiscal_quarter"])): item
        for item in _facts(source, "Inventory at cost")
        if item.get("fiscal_year") is not None and item.get("fiscal_quarter") is not None
    }
    points: list[HistoryPoint] = []
    for year, quarter in quarter_window:
        fact = by_period.get((year, quarter))
        points.append(
            HistoryPoint(
                period_label=f"{year}-Q{quarter}",
                period_id=f"period:anf:{year}-q{quarter}@1",
                value=None if fact is None else str(fact["value"]),
                display_value="" if fact is None else f"${Decimal(str(fact['value'])):,.1f}m",
                source_observation_id=None if fact is None else str(fact["fact_id"]),
                source_evidence_id=None if fact is None else str(fact["source_document_id"]),
            )
        )
    return HistoryRow(
        group_label="Inventory",
        driver_id="metric:summary-bs:inventory-at-cost@1",
        dimension_member_id=_TOTAL_COMPANY,
        definition_id="definition:summary-bs:inventory-at-cost@1",
        unit_id=_USD_MILLION_UNIT,
        label="Inventory at cost ($m)",
        display_role="OWNER_ELSEWHERE_CONTEXT",
        points=tuple(points),
    )


def _inventory_growth_history(
    source: Mapping[str, Any], quarter_window: Sequence[tuple[int, int]]
) -> HistoryRow:
    by_period = {
        (int(item["fiscal_year"]), int(item["fiscal_quarter"])): item
        for item in _facts(source, "Inventory cost growth")
        if item.get("fiscal_year") is not None
        and item.get("fiscal_quarter") is not None
        and item.get("period_basis") == "QUARTER_ACTUAL"
        and item.get("value") is not None
    }
    points: list[HistoryPoint] = []
    for year, quarter in quarter_window:
        fact = by_period.get((year, quarter))
        value = None if fact is None else Decimal(str(fact["value"]))
        points.append(
            HistoryPoint(
                period_label=f"{year}-Q{quarter}",
                period_id=f"period:anf:{year}-q{quarter}@1",
                value=None if value is None else format(value, "f"),
                display_value=(
                    ""
                    if value is None
                    else _display_number(value, _PERCENT_UNIT, signed=True)
                ),
                source_observation_id=None if fact is None else str(fact["fact_id"]),
                source_evidence_id=None if fact is None else str(fact["source_document_id"]),
            )
        )
    return HistoryRow(
        group_label="Inventory",
        driver_id="driver:operating:inventory-cost-growth@1",
        dimension_member_id=_TOTAL_COMPANY,
        definition_id="definition:operating-driver:inventory-cost-growth@1",
        unit_id=_PERCENT_UNIT,
        label="Inventory cost growth (YoY)",
        display_role="PRIMARY",
        points=tuple(points),
    )


def _fact_rate_history(
    source: Mapping[str, Any],
    quarter_window: Sequence[tuple[int, int]],
    *,
    metric_label: str,
    group_label: str,
    driver_id: str,
    definition_id: str,
    label: str,
    display_role: str,
) -> HistoryRow:
    by_period = {
        (int(item["fiscal_year"]), int(item["fiscal_quarter"])): item
        for item in _facts(source, metric_label)
        if item.get("fiscal_year") is not None
        and item.get("fiscal_quarter") is not None
        and item.get("period_basis") == "QUARTER_ACTUAL"
        and item.get("value") is not None
    }
    points: list[HistoryPoint] = []
    for year, quarter in quarter_window:
        fact = by_period.get((year, quarter))
        value = None if fact is None else Decimal(str(fact["value"]))
        points.append(
            HistoryPoint(
                period_label=f"{year}-Q{quarter}",
                period_id=f"period:anf:{year}-q{quarter}@1",
                value=None if value is None else format(value, "f"),
                display_value="" if value is None else _display_number(value, _PERCENT_UNIT, signed=True),
                source_observation_id=None if fact is None else str(fact["fact_id"]),
                source_evidence_id=None if fact is None else str(fact["source_document_id"]),
            )
        )
    return HistoryRow(
        group_label=group_label,
        driver_id=driver_id,
        dimension_member_id=_TOTAL_COMPANY,
        definition_id=definition_id,
        unit_id=_PERCENT_UNIT,
        label=label,
        display_role=display_role,
        points=tuple(points),
    )


def _inventory_units_history(
    source: Mapping[str, Any], quarter_window: Sequence[tuple[int, int]]
) -> HistoryRow:
    """Expose exact values numerically and source-supported ranges as concise text."""

    by_period = {
        (int(item["fiscal_year"]), int(item["fiscal_quarter"])): item
        for item in _facts(source, "Inventory unit growth")
        if item.get("fiscal_year") is not None and item.get("fiscal_quarter") is not None
    }

    def approximate_label(raw: str) -> str:
        normalized = raw.casefold().replace("–", "-").replace("—", "-")
        if "low single" in normalized:
            return "Up low-single"
        if "mid-single" in normalized:
            return "Up mid-single"
        if "around 1%" in normalized:
            return "Up ~1%"
        raise OperatingDriverAnfUIV4Error(
            f"Unsupported approximate inventory-unit presentation: {raw!r}."
        )

    points: list[HistoryPoint] = []
    for year, quarter in quarter_window:
        fact = by_period.get((year, quarter))
        if fact is None:
            points.append(
                HistoryPoint(
                    period_label=f"{year}-Q{quarter}",
                    period_id=f"period:anf:{year}-q{quarter}@1",
                    value=None,
                    display_value="",
                    source_observation_id=None,
                    source_evidence_id=None,
                )
            )
            continue
        if fact.get("precision") == "EXACT" and fact.get("value") is not None:
            value = Decimal(str(fact["value"]))
            points.append(
                HistoryPoint(
                    period_label=f"{year}-Q{quarter}",
                    period_id=f"period:anf:{year}-q{quarter}@1",
                    value=format(value, "f"),
                    display_value=_display_number(value, _PERCENT_UNIT, signed=True),
                    source_observation_id=str(fact["fact_id"]),
                    source_evidence_id=str(fact["source_document_id"]),
                    precision="EXACT",
                )
            )
            continue
        if (
            fact.get("precision") == "APPROXIMATE"
            and fact.get("status") == "AVAILABLE_APPROXIMATE_TEXT"
            and fact.get("value") is None
        ):
            points.append(
                HistoryPoint(
                    period_label=f"{year}-Q{quarter}",
                    period_id=f"period:anf:{year}-q{quarter}@1",
                    value=None,
                    display_value=approximate_label(str(fact["display_value"])),
                    source_observation_id=str(fact["fact_id"]),
                    source_evidence_id=str(fact["source_document_id"]),
                    precision="APPROXIMATE",
                )
            )
            continue
        raise OperatingDriverAnfUIV4Error(
            f"Inventory-unit evidence has an unsupported precision/status for {year}-Q{quarter}."
        )
    return HistoryRow(
        group_label="Inventory",
        driver_id="driver:operating:inventory-unit-growth@1",
        dimension_member_id=_TOTAL_COMPANY,
        definition_id="definition:operating-driver:inventory-unit-growth@1",
        unit_id=_PERCENT_UNIT,
        label="Inventory units (YoY)",
        display_role="CONTEXT_EXACT_AND_APPROXIMATE_TEXT",
        points=tuple(points),
    )


def _company_owned_store_count_history(
    source: Mapping[str, Any],
    observations: Sequence[_Observation],
    quarter_window: Sequence[tuple[int, int]],
) -> HistoryRow:
    direct_series = _series(
        observations,
        "driver:operating:company-owned-stores-end@1",
        _TOTAL_COMPANY,
    )
    direct = {
        (item.fiscal_year, item.fiscal_quarter): item
        for item in direct_series
    }
    reconciliations = {
        record.period_label: record
        for record in derive_company_owned_store_roll_forward(source)
    }
    points: list[HistoryPoint] = []
    for year, quarter in quarter_window:
        period_label = _quarter_label(year, quarter)
        direct_observation = direct.get((year, quarter))
        reconciliation = reconciliations.get(period_label)
        if direct_observation is None and reconciliation is None:
            points.append(
                HistoryPoint(
                    period_label=period_label,
                    period_id=f"period:anf:{year}-q{quarter}@1",
                    value=None,
                    display_value="",
                    source_observation_id=None,
                    source_evidence_id=None,
                )
            )
            continue
        value = (
            direct_observation.value
            if direct_observation is not None
            else Decimal(str(reconciliation.value))
        )
        points.append(
            HistoryPoint(
                period_label=period_label,
                period_id=f"period:anf:{year}-q{quarter}@1",
                value=format(value, "f"),
                display_value=_display_number(value, _STORE_UNIT),
                source_observation_id=(
                    direct_observation.observation_id
                    if direct_observation is not None
                    else reconciliation.derivation_id
                ),
                source_evidence_id=(
                    direct_observation.evidence_id
                    if direct_observation is not None
                    else STORE_COUNT_ROLL_FORWARD_CONTRACT
                ),
                derivation_id=None if reconciliation is None else reconciliation.derivation_id,
                lineage_references=(
                    () if reconciliation is None else reconciliation.lineage_references
                ),
                precision="EXACT",
            )
        )
    return HistoryRow(
        group_label="Store Footprint",
        driver_id="driver:operating:company-owned-stores-end@1",
        dimension_member_id=_TOTAL_COMPANY,
        definition_id="definition:operating-driver:company-owned-stores-end@1",
        unit_id=_STORE_UNIT,
        label="Company-owned stores",
        display_role="PRIMARY_DIRECT_OR_RECONCILED_ROLL_FORWARD",
        points=tuple(points),
    )


def build_operating_driver_anf_ui_source_from_completeness(
    completeness: Any,
) -> dict[str, Any]:
    """Adapt the accepted completeness package without changing its economics."""

    return {
        "shadow": completeness.registry.to_dict(),
        "analytics": completeness.analytics.to_dict(),
        "semantics": completeness.semantics.to_dict(),
        "selection": completeness.selection.to_dict(),
        "completeness": {
            "contract_version": completeness.contract_version,
            "facts": [item.to_dict() for item in completeness.observation_registry],
            "driver_registry": [dict(item) for item in completeness.driver_registry],
            "reconciliation": dict(completeness.reconciliation),
            "sha256": completeness.sha256,
        },
    }


def _latest_observation(
    observations: Sequence[_Observation], driver_id: str, member_id: str
) -> _Observation:
    values = _series(observations, driver_id, member_id)
    if not values:
        raise OperatingDriverAnfUIV4Error(f"Missing observation for {driver_id} {member_id}.")
    return values[-1]


def _observation_at(
    observations: Sequence[_Observation],
    driver_id: str,
    member_id: str,
    year: int,
    quarter: int,
) -> _Observation:
    matches = [
        item for item in _series(observations, driver_id, member_id)
        if (item.fiscal_year, item.fiscal_quarter) == (year, quarter)
    ]
    if len(matches) != 1:
        raise OperatingDriverAnfUIV4Error(
            f"Expected one observation for {driver_id} {member_id} {year}-Q{quarter}."
        )
    return matches[0]


def _statement_sources(*items: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted(set(value for group in items for value in group if value)))


def _fact_sources(*facts: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(
        sorted(
            value
            for item in facts
            for value in (str(item["fact_id"]), str(item["source_document_id"]))
        )
    )


def _footprint_definitions(
    source: Mapping[str, Any], observations: Sequence[_Observation]
) -> tuple[FootprintDefinition, ...]:
    registry = {
        str(item["driver_id"]): item
        for item in _fact_layer(source).get("driver_registry", ())
    }
    specs = (
        {
            "term": "Company-owned stores",
            "meaning": "Company-owned stores open at fiscal period end; franchise stores are separate.",
            "authority": "SOURCE_DEFINED",
            "measurement": "Period-end count; direct where reported, otherwise prior count + openings - closures.",
            "measurement_authorities": ("SOURCE_DEFINED", "SAFE_DERIVATION"),
            "economic_role": "Shows the scale of the physical selling footprint and local omnichannel reach.",
            "economic_role_authority": "SOURCE_SUPPORTED_INTERPRETATION",
            "economic_role_type": "PERIOD_END_CAPACITY",
            "driver_id": "driver:operating:company-owned-stores-end@1",
            "source_urls": (_ANF_FY2025_10K, _ANF_Q1_2026_10Q),
        },
        {
            "term": "New stores",
            "meaning": "Company-owned store locations opened during the fiscal period.",
            "authority": "SOURCE_DEFINED",
            "measurement": "Period activity count, disclosed directly or derived from compatible cumulative actuals.",
            "measurement_authorities": ("SOURCE_DEFINED", "SAFE_DERIVATION"),
            "economic_role": "Adds physical reach and capacity; economics depend on location, demand and return.",
            "economic_role_authority": "SOURCE_SUPPORTED_INTERPRETATION",
            "economic_role_type": "CAPACITY_GROWTH",
            "driver_id": "driver:operating:new-stores@1",
            "source_urls": (_ANF_FY2025_10K, _ANF_Q1_2026_10Q, _ANF_2018_INVESTOR_DAY),
        },
        {
            "term": "Remodeled",
            "meaning": "Existing company-owned stores updated during the fiscal period.",
            "authority": "SOURCE_SUPPORTED_INTERPRETATION",
            "measurement": "Period activity count, disclosed directly or derived from compatible cumulative actuals.",
            "measurement_authorities": ("SOURCE_DEFINED", "SAFE_DERIVATION"),
            "economic_role": "Reinvests in existing stores to improve experience and support store productivity.",
            "economic_role_authority": "SOURCE_SUPPORTED_INTERPRETATION",
            "economic_role_type": "PRODUCTIVITY_INVESTMENT",
            "driver_id": "driver:operating:remodeled-stores@1",
            "source_urls": (_ANF_FY2025_10K, _ANF_Q1_2026_10Q, _ANF_2018_INVESTOR_DAY),
        },
        {
            "term": "Right-sized",
            "meaning": "Existing stores adjusted to a smaller or better-aligned selling footprint during the period.",
            "authority": "SOURCE_SUPPORTED_INTERPRETATION",
            "measurement": "Period activity count, disclosed directly or derived from compatible cumulative actuals.",
            "measurement_authorities": ("SOURCE_DEFINED", "SAFE_DERIVATION"),
            "economic_role": "Aligns selling space with demand and digital penetration to support footprint efficiency.",
            "economic_role_authority": "SOURCE_SUPPORTED_INTERPRETATION",
            "economic_role_type": "FOOTPRINT_EFFICIENCY",
            "driver_id": "driver:operating:right-sized-stores@1",
            "source_urls": (_ANF_FY2025_10K, _ANF_FY2017_10K, _ANF_FY2019_10K),
        },
        {
            "term": "Closed",
            "meaning": "Company-owned store locations closed during the fiscal period.",
            "authority": "SOURCE_DEFINED",
            "measurement": "Period activity count, disclosed directly or derived from compatible cumulative actuals.",
            "measurement_authorities": ("SOURCE_DEFINED", "SAFE_DERIVATION"),
            "economic_role": "Removes physical capacity and can rationalize legacy or underproductive locations.",
            "economic_role_authority": "SOURCE_SUPPORTED_INTERPRETATION",
            "economic_role_type": "FOOTPRINT_RATIONALIZATION",
            "driver_id": "driver:operating:closed-stores@1",
            "source_urls": (_ANF_FY2025_10K, _ANF_FY2024_10K, _ANF_FY2019_10K),
        },
    )
    result: list[FootprintDefinition] = []
    for spec in specs:
        driver_id = str(spec["driver_id"])
        entry = registry.get(driver_id)
        if entry is None:
            raise OperatingDriverAnfUIV4Error(
                f"Footprint definition is missing accepted registry metadata for {driver_id}."
            )
        latest = _latest_observation(observations, driver_id, _TOTAL_COMPANY)
        if latest.definition_id != entry.get("definition_id"):
            raise OperatingDriverAnfUIV4Error(
                f"Footprint definition identity mismatch for {driver_id}."
            )
        result.append(
            FootprintDefinition(
                term=str(spec["term"]),
                meaning=str(spec["meaning"]),
                authority=str(spec["authority"]),
                measurement=str(spec["measurement"]),
                measurement_authorities=tuple(spec["measurement_authorities"]),
                economic_role=str(spec["economic_role"]),
                economic_role_authority=str(spec["economic_role_authority"]),
                economic_role_type=str(spec["economic_role_type"]),
                driver_id=driver_id,
                definition_id=str(entry["definition_id"]),
                source_references=tuple(
                    sorted(
                        {
                            str(entry["definition_id"]),
                            latest.observation_id,
                            latest.evidence_id,
                            *spec["source_urls"],
                        }
                    )
                ),
            )
        )
    return tuple(result)


def _footprint_economic_support() -> tuple[FootprintEconomicSupport, ...]:
    """Historical source support for mechanism language, never current KPI ownership."""

    return (
        FootprintEconomicSupport(
            support_id="support:anf:footprint:current-network-optimization@1",
            terms=("Remodeled", "Right-sized", "Closed"),
            period_label="FY2025",
            support_type="NETWORK_PRODUCTIVITY_MECHANISM",
            evidence_summary=(
                "ANF describes store-network optimization as remodeling, right-sizing or relocating "
                "stores to smaller footprints, and closing legacy stores to optimize store productivity."
            ),
            authority="SOURCE_SUPPORTED_INTERPRETATION",
            source_document_id="anf-fy2025-form-10-k",
            source_url=_ANF_FY2025_10K,
            source_location="Store Operations; Risk Factors; Global store network modernization and growth",
        ),
        FootprintEconomicSupport(
            support_id="support:anf:footprint:remodel-historical-lift@1",
            terms=("Remodeled",),
            period_label="2018 Investor Day",
            support_type="HISTORICAL_REMODEL_RETURN_EVIDENCE",
            evidence_summary=(
                "Hollister remodels were reported to have delivered a sustained high-single-digit "
                "sales-comparison lift versus control stores; this is historical support, not a current assumption."
            ),
            authority="SOURCE_SUPPORTED_INTERPRETATION",
            source_document_id="anf-2018-investor-day",
            source_url=_ANF_2018_INVESTOR_DAY,
            source_location="Investor Day transcript, remodel return discussion",
        ),
        FootprintEconomicSupport(
            support_id="support:anf:footprint:rightsize-efficiency@1",
            terms=("Right-sized", "Closed"),
            period_label="FY2017-FY2019",
            support_type="HISTORICAL_FOOTPRINT_EFFICIENCY_EVIDENCE",
            evidence_summary=(
                "ANF linked smaller formats, right-sizing and selected closures with higher square-foot "
                "productivity, lower square footage and store-occupancy efficiency."
            ),
            authority="SOURCE_SUPPORTED_INTERPRETATION",
            source_document_id="anf-fy2017-fy2019-footprint-materials",
            source_url=_ANF_FY2019_10K,
            source_location="Global Store Network Optimization; 2018 Investor Day real-estate discussion",
        ),
    )


def _footprint_context_relationships(
    definitions: Sequence[FootprintDefinition],
) -> tuple[FootprintContextRelationship, ...]:
    sources = {
        item.term: item.source_references
        for item in definitions
    }
    specs = (
        (
            "relationship:anf:footprint:capacity-sales-spread@1",
            ("New stores", "Company-owned stores"),
            "Footprint expansion can help reported sales remain positive even when comparable demand is softer.",
            "CAPACITY_AND_DEMAND_INTERACTION",
        ),
        (
            "relationship:anf:footprint:remodel-reinvestment@1",
            ("Remodeled",),
            "Higher remodeling activity indicates active reinvestment in the existing store fleet.",
            "PRODUCTIVITY_INVESTMENT",
        ),
        (
            "relationship:anf:footprint:rightsize-efficiency@1",
            ("Right-sized",),
            "Right-sizing activity indicates an effort to align selling space with demand and digital penetration.",
            "FOOTPRINT_EFFICIENCY",
        ),
        (
            "relationship:anf:footprint:fleet-reshaping@1",
            ("New stores", "Closed"),
            "Openings and closures together describe fleet reshaping; neither is directionally good or bad alone.",
            "FOOTPRINT_RATIONALIZATION",
        ),
    )
    return tuple(
        FootprintContextRelationship(
            relationship_id=relationship_id,
            trigger_terms=terms,
            interpretation=interpretation,
            semantic_type=semantic_type,
            source_references=tuple(sorted({value for term in terms for value in sources[term]})),
        )
        for relationship_id, terms, interpretation, semantic_type in specs
    )


def build_operating_driver_anf_ui_v4(
    source: Mapping[str, Any], *, source_identity_receipts: Mapping[str, str]
) -> OperatingDriverAnfUIV4Package:
    """Build the corrected ANF V4 presentation from accepted lower layers."""

    fact_layer = _fact_layer(source)
    fact_contract_name = "completeness" if "completeness" in source else "period_repair"
    contracts = {
        "analytics": str(source["analytics"]["contract_version"]),
        "selection": str(source["selection"]["contract_version"]),
        "semantics": str(source["semantics"]["contract_version"]),
        "shadow": str(source["shadow"]["contract_version"]),
        fact_contract_name: str(fact_layer["contract_version"]),
    }
    reconciliation = fact_layer["reconciliation"]
    if reconciliation.get("status") != "PASS":
        raise OperatingDriverAnfUIV4Error("Accepted ANF fact package did not pass reconciliation.")
    if str(source["analytics"].get("ticker", "ANF")).upper() != "ANF":
        raise OperatingDriverAnfUIV4Error("ANF V4 refuses non-ANF source packages.")
    if any(int(source[layer].get("forecast_number_emission_count", 0)) for layer in ("analytics", "semantics")):
        raise OperatingDriverAnfUIV4Error("Operating Drivers cannot emit forecast numbers.")
    if int(source["semantics"].get("qualitative_to_numeric_count", 0)) != 0:
        raise OperatingDriverAnfUIV4Error("Qualitative evidence may not become numeric UI values.")

    observations = _load_observations(source)
    latest_year, latest_quarter = max(
        (item.fiscal_year, item.fiscal_quarter)
        for item in observations
        if item.driver_id == "driver:operating:comparable-sales@1"
    )
    latest_period = _quarter_label(latest_year, latest_quarter)
    prior_year, prior_quarter = _previous_quarter(latest_year, latest_quarter)
    prior_period = _quarter_label(prior_year, prior_quarter)
    year_ago_period = _quarter_label(latest_year - 1, latest_quarter)
    latest_fy = latest_year if latest_quarter == 4 else latest_year - 1
    quarter_window = _quarter_window(latest_year, latest_quarter, QUARTER_COUNT)
    quarter_labels = tuple(_quarter_label(*value) for value in quarter_window)

    core = (
        _core_driver(source, observations, quarter_window, core_id="comp-total", group_label="Demand / Sales", driver_id="driver:operating:comparable-sales@1", member_id=_TOTAL_COMPANY, label="Total company", trend="Slowing", why_it_matters="Primary read on underlying customer demand."),
        _context_rate_core(source, core_id="net-sales-growth", group_label="Demand / Sales", metric_label="Net sales growth", driver_id="metric:financial:net-sales-growth@1", definition_id="definition:financial:net-sales-growth@1", label="Net sales growth", latest_period=latest_period, prior_period=prior_period, year_ago_period=year_ago_period, trend="Slowing", why_it_matters="Shows reported sales momentum alongside comparable demand."),
        _core_driver(source, observations, quarter_window, core_id="comp-emea", group_label="Demand / Sales", driver_id="driver:operating:comparable-sales@1", member_id=_EMEA, label="EMEA", trend="Contracting", why_it_matters="Shows the current regional demand pressure."),
        _core_driver(source, observations, quarter_window, core_id="comp-apac", group_label="Demand / Sales", driver_id="driver:operating:comparable-sales@1", member_id=_APAC, label="APAC", trend="Accelerating", why_it_matters="Shows the strongest regional demand divergence."),
        _company_owned_store_core(
            source,
            observations,
            quarter_window,
            latest_year=latest_year,
            latest_quarter=latest_quarter,
        ),
        _context_inventory_core(source, latest_period=latest_period, prior_period=prior_period, year_ago_period=year_ago_period),
        _context_rate_core(source, core_id="inventory-cost-growth", group_label="Inventory", metric_label="Inventory cost growth", driver_id="driver:operating:inventory-cost-growth@1", definition_id="definition:operating-driver:inventory-cost-growth@1", label="Inventory cost growth", latest_period=latest_period, prior_period=prior_period, year_ago_period=year_ago_period, trend="Cost pressure easing", why_it_matters="Shows whether inventory cost pressure is building or easing."),
        _context_inventory_units_core(
            source, latest_period=latest_period, prior_period=prior_period
        ),
    )

    history_specs = (
        ("Demand / Sales", "driver:operating:comparable-sales@1", _TOTAL_COMPANY, "Total company", "PRIMARY"),
        ("Demand / Sales", "driver:operating:comparable-sales@1", _ABERCROMBIE, "Abercrombie", "CONTEXT"),
        ("Demand / Sales", "driver:operating:comparable-sales@1", _HOLLISTER, "Hollister", "CONTEXT"),
        ("Demand / Sales", "driver:operating:comparable-sales@1", _AMERICAS, "Americas", "CONTEXT"),
        ("Demand / Sales", "driver:operating:comparable-sales@1", _EMEA, "EMEA", "CONTEXT"),
        ("Demand / Sales", "driver:operating:comparable-sales@1", _APAC, "APAC", "CONTEXT"),
    )
    history = [
        _fact_rate_history(
            source,
            quarter_window,
            metric_label="Net sales growth",
            group_label="Demand / Sales",
            driver_id="metric:financial:net-sales-growth@1",
            definition_id="definition:financial:net-sales-growth@1",
            label="Net sales growth",
            display_role="OWNER_ELSEWHERE_CONTEXT",
        ),
        *(
        _history_row(
            observations,
            quarter_window,
            group_label=group,
            driver_id=driver,
            member_id=member,
            label=label,
            display_role=role,
        )
        for group, driver, member, label, role in history_specs
        ),
    ]
    history.append(_inventory_history(source, quarter_window))
    history.append(_inventory_growth_history(source, quarter_window))
    history.append(_inventory_units_history(source, quarter_window))
    history.append(_company_owned_store_count_history(source, observations, quarter_window))
    history.extend(
        _history_row(
            observations,
            quarter_window,
            group_label="Store Footprint",
            driver_id=driver,
            member_id=_TOTAL_COMPANY,
            label=label,
            display_role=role,
        )
        for driver, label, role in (
            ("driver:operating:new-stores@1", "New stores", "CONTEXT"),
            ("driver:operating:remodeled-stores@1", "Remodeled", "CONTEXT"),
            ("driver:operating:right-sized-stores@1", "Right-sized", "CONTEXT"),
            ("driver:operating:closed-stores@1", "Closed", "CONTEXT"),
        )
    )
    combined_store_activity_evidence = assess_combined_store_activity_evidence(source)
    if not combined_store_activity_evidence:
        raise OperatingDriverAnfUIV4Error(
            "Accepted ANF package unexpectedly lacks the combined guidance evidence under review."
        )
    footprint_definitions = _footprint_definitions(source, observations)
    footprint_economic_support = _footprint_economic_support()
    footprint_context_relationships = _footprint_context_relationships(footprint_definitions)
    store_count_roll_forward_note = (
        "Store-count bridge: prior period-end company-owned stores + new stores - closed stores = "
        "current period-end company-owned stores. Remodels and right-sizes update existing locations, "
        "so they do not change store count."
    )
    store_count_roll_forward_note_sources = tuple(
        sorted(
            {
                STORE_COUNT_ROLL_FORWARD_CONTRACT,
                _ANF_FY2025_10K,
                _ANF_Q1_2026_10Q,
                *(value for item in derive_company_owned_store_roll_forward(source) for value in item.lineage_references),
            }
        )
    )

    comp_total = _latest_observation(observations, "driver:operating:comparable-sales@1", _TOTAL_COMPANY)
    comp_americas = _latest_observation(observations, "driver:operating:comparable-sales@1", _AMERICAS)
    comp_emea = _latest_observation(observations, "driver:operating:comparable-sales@1", _EMEA)
    comp_apac = _latest_observation(observations, "driver:operating:comparable-sales@1", _APAC)
    comp_aber = _latest_observation(observations, "driver:operating:comparable-sales@1", _ABERCROMBIE)
    comp_hollister = _latest_observation(observations, "driver:operating:comparable-sales@1", _HOLLISTER)
    comp_aber_q4 = _observation_at(observations, "driver:operating:comparable-sales@1", _ABERCROMBIE, prior_year, prior_quarter)
    comp_hollister_q4 = _observation_at(observations, "driver:operating:comparable-sales@1", _HOLLISTER, prior_year, prior_quarter)
    comp_2024_q1 = _observation_at(observations, "driver:operating:comparable-sales@1", _TOTAL_COMPANY, 2024, 1)
    comp_2025_q4 = _observation_at(observations, "driver:operating:comparable-sales@1", _TOTAL_COMPANY, 2025, 4)
    stores_q4 = _observation_at(observations, "driver:operating:company-owned-stores-end@1", _TOTAL_COMPANY, prior_year, prior_quarter)
    stores_q1 = _latest_observation(observations, "driver:operating:company-owned-stores-end@1", _TOTAL_COMPANY)
    q1_store_activity = {
        label: _latest_observation(observations, driver, _TOTAL_COMPANY)
        for label, driver in (
            ("openings", "driver:operating:new-stores@1"),
            ("remodels", "driver:operating:remodeled-stores@1"),
            ("right-sizes", "driver:operating:right-sized-stores@1"),
            ("closures", "driver:operating:closed-stores@1"),
        )
    }
    net_sales = _one_fact(source, "Net sales growth", period_label=latest_period)
    inventory_q1 = _one_fact(source, "Inventory at cost", period_label=latest_period)
    inventory_yoy = _one_fact(source, "Inventory at cost", period_label=year_ago_period)
    inventory_growth_q1 = _one_fact(source, "Inventory cost growth", period_label=latest_period)
    inventory_units = _one_fact(source, "Inventory unit growth", period_label=latest_period)
    fy_new = _one_fact(source, "New stores", period_label=f"FY{latest_fy}")
    fy_remodel = _one_fact(source, "Remodeled stores", period_label=f"FY{latest_fy}")
    fy_right = _one_fact(source, "Right-sized stores", period_label=f"FY{latest_fy}")
    fy_closed = _one_fact(source, "Closed stores", period_label=f"FY{latest_fy}")
    inventory_yoy_percent = Decimal(str(inventory_growth_q1["value"]))

    overview = (
        InvestorStatement(
            "anf-interpretation-demand",
            INTERPRETATION_SUBSECTION,
            "Underlying demand has slowed sharply from the 2024 peak. Reported sales remained positive in 2026-Q1, while the company-owned store base continued to expand.",
            ("SYNTHESIS", "DEMAND_AND_FOOTPRINT_SPREAD"),
            _statement_sources(
                (comp_2024_q1.observation_id, comp_2024_q1.evidence_id),
                (comp_total.observation_id, comp_total.evidence_id),
                _fact_sources(net_sales),
                (stores_q4.observation_id, stores_q4.evidence_id, stores_q1.observation_id, stores_q1.evidence_id),
            ),
        ),
        InvestorStatement(
            "anf-interpretation-inventory",
            INTERPRETATION_SUBSECTION,
            "Inventory cost pressure eased, but units were still up approximately low single digits.",
            ("SYNTHESIS", "CONTEXT_DEPENDENT_DIVERGENCE", "APPROXIMATE_EVIDENCE"),
            _statement_sources(_fact_sources(inventory_q1, inventory_yoy, inventory_growth_q1, inventory_units)),
        ),
        InvestorStatement(
            "anf-interpretation-regions",
            INTERPRETATION_SUBSECTION,
            "Regional demand remained uneven, with APAC strength offset by pronounced EMEA weakness.",
            ("SYNTHESIS", "DIVERGENCE"),
            _statement_sources(
                (comp_apac.observation_id, comp_apac.evidence_id),
                (comp_emea.observation_id, comp_emea.evidence_id),
            ),
        ),
        InvestorStatement(
            "anf-latest-demand",
            LATEST_SUBSECTION,
            f"Net sales rose {_smart_decimal(Decimal(str(net_sales['value'])))}% in {latest_period}, while comparable sales fell 1%.",
            ("CURRENT_LEVEL", "DEMAND_SPREAD"),
            _statement_sources(_fact_sources(net_sales), (comp_total.observation_id, comp_total.evidence_id)),
        ),
        InvestorStatement(
            "anf-latest-regional-divergence",
            LATEST_SUBSECTION,
            "Americas stayed positive at 1% and APAC accelerated to 15%, but EMEA weakened sharply to -11%. Demand diverged by region.",
            ("CURRENT_LEVEL", "DIVERGENCE"),
            _statement_sources(
                (comp_americas.observation_id, comp_americas.evidence_id),
                (comp_apac.observation_id, comp_apac.evidence_id),
                (comp_emea.observation_id, comp_emea.evidence_id),
            ),
        ),
        InvestorStatement(
            "anf-latest-footprint",
            LATEST_SUBSECTION,
            "The store base increased from 829 to 834. The latest quarter included 6 openings, 24 remodels, 2 right-sizes and 1 closure.",
            ("CURRENT_LEVEL", "FOOTPRINT_ACTIVITY"),
            _statement_sources(
                (stores_q4.observation_id, stores_q4.evidence_id, stores_q1.observation_id, stores_q1.evidence_id),
                tuple(value for item in q1_store_activity.values() for value in (item.observation_id, item.evidence_id)),
            ),
        ),
        InvestorStatement(
            "anf-latest-inventory",
            LATEST_SUBSECTION,
            (
                f"Inventory at cost declined {_smart_decimal(abs(inventory_yoy_percent))}% year over year to {_display_number(Decimal(str(inventory_q1['value'])), _USD_MILLION_UNIT)}, "
                "while units were up low single digits. Inventory cost and units moved in different directions."
            ),
            ("CURRENT_LEVEL", "CONTEXT_DEPENDENT_DIVERGENCE", "APPROXIMATE_EVIDENCE"),
            _statement_sources(_fact_sources(inventory_q1, inventory_yoy, inventory_units)),
        ),
        InvestorStatement(
            "anf-broader-demand",
            BROADER_SUBSECTION,
            "Comparable sales slowed from 21% in 2024-Q1 to 1% in 2025-Q4 and -1% in 2026-Q1.",
            ("BROADER_DIRECTION", "DECELERATION"),
            _statement_sources(
                (comp_2024_q1.observation_id, comp_2024_q1.evidence_id),
                (comp_2025_q4.observation_id, comp_2025_q4.evidence_id),
                (comp_total.observation_id, comp_total.evidence_id),
            ),
        ),
        InvestorStatement(
            "anf-broader-brand-divergence",
            BROADER_SUBSECTION,
            "Abercrombie improved from -1% to flat in the latest quarter, but Hollister slowed from 3% to -2%.",
            ("BROADER_DIRECTION", "DIVERGENCE"),
            _statement_sources(
                (comp_aber_q4.observation_id, comp_aber_q4.evidence_id, comp_aber.observation_id, comp_aber.evidence_id),
                (comp_hollister_q4.observation_id, comp_hollister_q4.evidence_id, comp_hollister.observation_id, comp_hollister.evidence_id),
            ),
        ),
        InvestorStatement(
            "anf-broader-store-investment",
            BROADER_SUBSECTION,
            f"Store investment remains active: FY{latest_fy} included 62 openings, 47 remodels, 11 right-sizes and 22 closures, followed by 24 remodels and 2 right-sizes in {latest_period}.",
            ("BROADER_DIRECTION", "FOOTPRINT_ACTIVITY"),
            _statement_sources(
                _fact_sources(fy_new, fy_remodel, fy_right, fy_closed),
                (q1_store_activity["remodels"].observation_id, q1_store_activity["remodels"].evidence_id),
                (q1_store_activity["right-sizes"].observation_id, q1_store_activity["right-sizes"].evidence_id),
            ),
        ),
    )

    payload = {
        "ticker": "ANF",
        "contract_version": ANF_UI_CONTRACT,
        "language_contract": INVESTOR_LANGUAGE_CONTRACT,
        "plan_origin": PLAN_ORIGIN,
        "major_sections": VISIBLE_MAJOR_SECTIONS,
        "latest_period_label": latest_period,
        "quarter_labels": quarter_labels,
        "overview": [asdict(item) for item in overview],
        "core_drivers": [asdict(item) for item in core],
        "history_rows": [asdict(item) for item in history],
        "footprint_definitions": [asdict(item) for item in footprint_definitions],
        "footprint_economic_support": [asdict(item) for item in footprint_economic_support],
        "footprint_context_relationships": [asdict(item) for item in footprint_context_relationships],
        "store_count_roll_forward_note": store_count_roll_forward_note,
        "store_count_roll_forward_note_sources": store_count_roll_forward_note_sources,
        "source_contracts": contracts,
        "source_identity_receipts": dict(source_identity_receipts),
    }
    return OperatingDriverAnfUIV4Package(
        ticker="ANF",
        contract_version=ANF_UI_CONTRACT,
        language_contract=INVESTOR_LANGUAGE_CONTRACT,
        plan_origin=PLAN_ORIGIN,
        major_sections=VISIBLE_MAJOR_SECTIONS,
        latest_period_label=latest_period,
        quarter_labels=quarter_labels,
        overview=overview,
        core_drivers=core,
        history_rows=tuple(history),
        footprint_definitions=footprint_definitions,
        footprint_economic_support=footprint_economic_support,
        footprint_context_relationships=footprint_context_relationships,
        store_count_roll_forward_note=store_count_roll_forward_note,
        store_count_roll_forward_note_sources=store_count_roll_forward_note_sources,
        source_contracts=contracts,
        source_identity_receipts=dict(source_identity_receipts),
        package_sha256=_digest(payload),
    )


__all__ = [
    "ANF_UI_CONTRACT",
    "APPROXIMATE_RANGE_DIRECTION_CONTRACT",
    "ApproximateRangeComparison",
    "BROADER_SUBSECTION",
    "CombinedStoreActivityEvidence",
    "CoreDriver",
    "FOOTPRINT_CONTEXT_RELATIONSHIP_CONTRACT",
    "FOOTPRINT_DEFINITION_CONTRACT",
    "FOOTPRINT_ECONOMIC_SUPPORT_CONTRACT",
    "FootprintContextRelationship",
    "FootprintDefinition",
    "FootprintEconomicSupport",
    "HistoryPoint",
    "HistoryRow",
    "INTERPRETATION_SUBSECTION",
    "INVESTOR_LANGUAGE_CONTRACT",
    "InvestorStatement",
    "LATEST_SUBSECTION",
    "OperatingDriverAnfUIV4Error",
    "OperatingDriverAnfUIV4Package",
    "PLAN_ORIGIN",
    "QUARTER_COUNT",
    "STORE_COUNT_PERIOD_COMPARISON_CONTRACT",
    "STORE_COUNT_ROLL_FORWARD_CONTRACT",
    "StoreCountRollForward",
    "VISIBLE_MAJOR_SECTIONS",
    "assess_combined_store_activity_evidence",
    "build_operating_driver_anf_ui_source_from_completeness",
    "build_operating_driver_anf_ui_v4",
    "derive_company_owned_store_roll_forward",
    "derive_inventory_approximate_range_comparison",
]
