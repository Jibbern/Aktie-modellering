"""Safe, explicit change derivation from selected numerical observations."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any, Mapping, Sequence

from .calendar_rules import IncomparablePeriodError, compare_periods
from .identity import change_observation_identity, identity_digest
from .types import canonical_decimal


class IncompatibleFactError(ValueError):
    """Raised when numerical facts differ on a semantic comparison axis."""


COMPARISON_FIELDS = (
    "metric_id",
    "definition_id",
    "basis_id",
    "unit_id",
    "currency",
)


def _period_dates(period: Mapping[str, Any]) -> tuple[date, date]:
    try:
        return date.fromisoformat(str(period["start_date"])), date.fromisoformat(
            str(period["end_date"])
        )
    except (KeyError, ValueError) as exc:
        raise IncomparablePeriodError(
            "Fiscal periods require source-backed ISO start and end dates."
        ) from exc


def _exact_value(record: Mapping[str, Any]) -> Decimal:
    value = record.get("payload", {}).get("value", {})
    if value.get("kind") != "exact":
        raise IncompatibleFactError("Change derivation requires selected exact NumericalFacts.")
    return Decimal(str(value.get("value")))


def validate_fact_compatibility(earlier: Mapping[str, Any], later: Mapping[str, Any]) -> None:
    if earlier.get("header", {}).get("record_type") != "NumericalFact" or later.get("header", {}).get("record_type") != "NumericalFact":
        raise IncompatibleFactError("Only NumericalFact records can produce numeric changes.")
    earlier_payload, later_payload = earlier.get("payload", {}), later.get("payload", {})
    differing = [field for field in COMPARISON_FIELDS if earlier_payload.get(field) != later_payload.get(field)]
    if earlier.get("header", {}).get("dimension_set_id") != later.get("header", {}).get("dimension_set_id"):
        differing.append("dimension_set_id")
    if earlier.get("header", {}).get("company_id") != later.get("header", {}).get("company_id"):
        differing.append("company_id")
    if differing:
        raise IncompatibleFactError(f"Facts differ on comparison axes: {', '.join(differing)}.")


def validate_percentage_point_rule_binding(
    change: Mapping[str, Any],
    earlier: Mapping[str, Any],
    later: Mapping[str, Any],
    *,
    rule: Mapping[str, Any],
    units: Mapping[str, Mapping[str, Any]],
) -> None:
    """Bind a derived percentage-point record to its catalog rule and later fact."""

    validate_fact_compatibility(earlier, later)
    payload = change.get("payload", {})
    header = change.get("header", {})
    change_kind = str(payload.get("change_kind", ""))
    if rule.get("change_kind") != change_kind:
        raise IncompatibleFactError("Change rule does not permit the stored change_kind.")
    if rule.get("input_unit_kind") != "percent":
        raise IncompatibleFactError("Percentage-point rules require percent input units.")
    input_unit_id = str(earlier.get("payload", {}).get("unit_id", ""))
    input_unit = units.get(input_unit_id)
    if input_unit is None or input_unit.get("unit_kind") != "percent":
        raise IncompatibleFactError("Percentage-point change inputs must use a percent unit.")
    output_unit_id = str(payload.get("unit_id", ""))
    if output_unit_id != rule.get("output_unit_id"):
        raise IncompatibleFactError("Change output unit does not match the rule-defined output unit.")
    output_unit = units.get(output_unit_id)
    if output_unit is None or output_unit.get("unit_kind") != "percentage-point":
        raise IncompatibleFactError("Percentage-point change output must use a percentage-point unit.")

    later_header = later.get("header", {})
    for field in (
        "company_id",
        "subject_id",
        "knowledge_date",
        "effective_period_id",
        "fiscal_period_id",
        "period_type",
        "dimension_set_id",
    ):
        if header.get(field) != later_header.get(field):
            raise IncompatibleFactError(f"Derived change header {field} must match the later selected fact.")
    if header.get("assertion_mode") != "derived" or header.get("publication_date") is not None:
        raise IncompatibleFactError("Derived change header must use derived assertion mode and no publication date.")
    expected_evidence = sorted(
        set(str(value) for value in earlier.get("header", {}).get("evidence_occurrence_ids", ()))
        | set(str(value) for value in later_header.get("evidence_occurrence_ids", ()))
    )
    if list(header.get("evidence_occurrence_ids", ())) != expected_evidence:
        raise IncompatibleFactError("Derived change evidence must equal the sorted union of input evidence.")


def derive_percentage_point_change(
    earlier: Mapping[str, Any],
    later: Mapping[str, Any],
    *,
    earlier_period: Mapping[str, Any],
    later_period: Mapping[str, Any],
    earlier_calendar: Mapping[str, Any],
    later_calendar: Mapping[str, Any],
    change_kind: str,
    rule_id: str,
    change_unit_id: str,
) -> dict[str, Any]:
    validate_fact_compatibility(earlier, later)
    comparability = compare_periods(
        earlier_period,
        later_period,
        earlier_calendar=earlier_calendar,
        later_calendar=later_calendar,
        change_kind=change_kind,
    )
    earlier_value, later_value = _exact_value(earlier), _exact_value(later)
    earlier_id = str(earlier["header"]["record_id"])
    later_id = str(later["header"]["record_id"])
    company_id = str(later["header"]["company_id"])
    record_id = change_observation_identity(
        company_id=company_id,
        change_kind=change_kind,
        from_record_id=earlier_id,
        to_record_id=later_id,
        rule_id=rule_id,
    )
    evidence_ids = sorted(
        set(earlier["header"].get("evidence_occurrence_ids", ()))
        | set(later["header"].get("evidence_occurrence_ids", ()))
    )
    return {
        "header": {
            "record_id": record_id,
            "identity_digest": identity_digest(record_id),
            "record_type": "ChangeObservation",
            "schema_version": "1.0.0",
            "company_id": company_id,
            "subject_id": str(later["header"].get("subject_id", "")),
            "publication_date": None,
            "knowledge_date": str(later["header"]["knowledge_date"]),
            "effective_period_id": str(later["header"]["effective_period_id"]),
            "fiscal_period_id": str(later["header"]["fiscal_period_id"]),
            "period_type": str(later["header"]["period_type"]),
            "dimension_set_id": str(later["header"]["dimension_set_id"]),
            "assertion_mode": "derived",
            "evidence_occurrence_ids": evidence_ids,
            "review_state": "accepted",
            "confidence": None,
        },
        "payload": {
            "kind": "ChangeObservation",
            "change_kind": change_kind,
            "from_record_id": earlier_id,
            "to_record_id": later_id,
            "input_record_ids": sorted([earlier_id, later_id]),
            "rule_id": rule_id,
            "comparability": comparability,
            "value": {"kind": "exact", "value": canonical_decimal(later_value - earlier_value)},
            "unit_id": change_unit_id,
        },
    }


def percentage_change(earlier_value: str, later_value: str) -> str:
    denominator = Decimal(earlier_value)
    if denominator == 0:
        raise IncompatibleFactError("Percentage change is undefined for a zero denominator.")
    return canonical_decimal((Decimal(later_value) - denominator) / abs(denominator) * Decimal("100"))


def validate_complete_ttm(
    periods: Sequence[Mapping[str, Any]], *, calendar: Mapping[str, Any]
) -> None:
    if len(periods) != 4:
        raise IncomparablePeriodError("TTM requires exactly four complete fiscal quarters.")
    ordered = sorted(periods, key=lambda row: int(row.get("fiscal_ordinal", -1)))
    for earlier, later in zip(ordered, ordered[1:]):
        compare_periods(
            earlier,
            later,
            earlier_calendar=calendar,
            later_calendar=calendar,
            change_kind="qoq-percentage-point",
        )


def derive_quarter_from_ytd(
    current_ytd_value: str,
    prior_ytd_value: str,
    *,
    current_period: Mapping[str, Any],
    prior_period: Mapping[str, Any],
) -> str:
    if current_period.get("period_type") != "ytd" or prior_period.get("period_type") != "ytd":
        raise IncomparablePeriodError("YTD subtraction requires two YTD periods.")
    if current_period.get("calendar_id") != prior_period.get("calendar_id") or current_period.get("fiscal_year") != prior_period.get("fiscal_year"):
        raise IncomparablePeriodError("YTD inputs must share fiscal calendar and year.")
    current_quarter, prior_quarter = current_period.get("fiscal_quarter"), prior_period.get("fiscal_quarter")
    if not isinstance(current_quarter, int) or not isinstance(prior_quarter, int) or current_quarter != prior_quarter + 1:
        raise IncomparablePeriodError("YTD subtraction requires adjacent cumulative fiscal quarters.")
    current_start, current_end = _period_dates(current_period)
    prior_start, prior_end = _period_dates(prior_period)
    if current_start != prior_start or current_end <= prior_end:
        raise IncomparablePeriodError("YTD inputs do not form a safe cumulative sequence.")
    current_weeks, prior_weeks = current_period.get("week_count"), prior_period.get("week_count")
    if not isinstance(current_weeks, int) or not isinstance(prior_weeks, int) or current_weeks - prior_weeks not in {13, 14}:
        raise IncomparablePeriodError("YTD duration difference is not one source-backed fiscal quarter.")
    return canonical_decimal(Decimal(current_ytd_value) - Decimal(prior_ytd_value))
