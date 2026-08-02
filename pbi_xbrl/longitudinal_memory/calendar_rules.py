"""Closed fiscal-calendar rules and deterministic period compatibility."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from types import MappingProxyType
from typing import Any, Literal, Mapping

from .identity import IdentityError, validate_semantic_id


CALENDAR_YEAR_RULE_ID = "rule:core:calendar-year-fiscal@1"
SOURCE_LABELLED_52_53_WEEK_RULE_ID = (
    "rule:core:source-labelled-52-53-week-fiscal@1"
)


class FiscalCalendarRuleError(ValueError):
    """Raised when a calendar or period cannot satisfy one closed rule."""


class IncomparablePeriodError(ValueError):
    """Raised rather than converting or guessing unsafe fiscal periods."""


@dataclass(frozen=True, slots=True)
class FiscalCalendarRule:
    rule_id: str
    comparison_mode: Literal["calendar-year", "equal-duration"]
    quarter_week_counts: frozenset[int]
    annual_week_counts: frozenset[int]


FISCAL_CALENDAR_RULES: Mapping[str, FiscalCalendarRule] = MappingProxyType(
    {
        CALENDAR_YEAR_RULE_ID: FiscalCalendarRule(
            rule_id=CALENDAR_YEAR_RULE_ID,
            comparison_mode="calendar-year",
            quarter_week_counts=frozenset(),
            annual_week_counts=frozenset(),
        ),
        SOURCE_LABELLED_52_53_WEEK_RULE_ID: FiscalCalendarRule(
            rule_id=SOURCE_LABELLED_52_53_WEEK_RULE_ID,
            comparison_mode="equal-duration",
            quarter_week_counts=frozenset({13, 14}),
            annual_week_counts=frozenset({52, 53}),
        ),
    }
)


def fiscal_calendar_rule(calendar: Mapping[str, Any]) -> FiscalCalendarRule:
    """Resolve one explicit versioned rule without inspecting issuer or dates."""

    raw_rule_id = calendar.get("calendar_rule_id")
    if raw_rule_id is None:
        raise FiscalCalendarRuleError("Fiscal calendar rule identity is missing.")
    try:
        rule_id = validate_semantic_id(raw_rule_id, prefix="rule")
    except IdentityError as exc:
        raise FiscalCalendarRuleError(str(exc)) from exc
    rule = FISCAL_CALENDAR_RULES.get(rule_id)
    if rule is None:
        raise FiscalCalendarRuleError(
            f"Unsupported fiscal calendar rule {rule_id!r}."
        )
    return rule


def _period_dates(period: Mapping[str, Any]) -> tuple[date, date]:
    try:
        start = date.fromisoformat(str(period["start_date"]))
        end = date.fromisoformat(str(period["end_date"]))
    except (KeyError, ValueError) as exc:
        raise FiscalCalendarRuleError(
            "Fiscal periods require source-backed ISO start and end dates."
        ) from exc
    if start > end:
        raise FiscalCalendarRuleError("Fiscal period boundaries are reversed.")
    return start, end


def _exact_calendar_quarter_bounds(fiscal_year: int, quarter: int) -> tuple[date, date]:
    if quarter == 1:
        return date(fiscal_year, 1, 1), date(fiscal_year, 3, 31)
    if quarter == 2:
        return date(fiscal_year, 4, 1), date(fiscal_year, 6, 30)
    if quarter == 3:
        return date(fiscal_year, 7, 1), date(fiscal_year, 9, 30)
    if quarter == 4:
        return date(fiscal_year, 10, 1), date(fiscal_year, 12, 31)
    raise FiscalCalendarRuleError("Fiscal quarter must be an integer from 1 through 4.")


def validate_period_for_calendar_rule(
    period: Mapping[str, Any], calendar: Mapping[str, Any]
) -> FiscalCalendarRule:
    """Validate source-backed period fields under their canonical calendar rule."""

    calendar_id = str(calendar.get("calendar_id", ""))
    if not calendar_id or str(period.get("calendar_id", "")) != calendar_id:
        raise FiscalCalendarRuleError(
            "Fiscal period does not resolve to the supplied canonical calendar."
        )
    rule = fiscal_calendar_rule(calendar)
    start, end = _period_dates(period)
    actual_days = (end - start).days + 1
    if period.get("day_count") != actual_days:
        raise FiscalCalendarRuleError(
            "Fiscal-period day_count does not match its exact inclusive dates."
        )

    period_type = period.get("period_type")
    fiscal_year = period.get("fiscal_year")
    fiscal_quarter = period.get("fiscal_quarter")
    week_count = period.get("week_count")

    if rule.comparison_mode == "calendar-year":
        if period.get("is_53_week_year") is True:
            raise FiscalCalendarRuleError(
                "Calendar-year periods cannot claim a 53-week fiscal year."
            )
        if week_count is not None:
            raise FiscalCalendarRuleError(
                "Calendar-year periods use exact dates and must not invent week_count."
            )
        if not isinstance(fiscal_year, int):
            raise FiscalCalendarRuleError("Calendar-year periods require fiscal_year.")
        if period_type == "quarter":
            if not isinstance(fiscal_quarter, int):
                raise FiscalCalendarRuleError(
                    "Calendar-year quarters require an explicit fiscal quarter."
                )
            expected_start, expected_end = _exact_calendar_quarter_bounds(
                fiscal_year, fiscal_quarter
            )
            if (start, end) != (expected_start, expected_end):
                raise FiscalCalendarRuleError(
                    "Calendar-year quarter boundaries are not exact."
                )
        elif period_type == "annual":
            if fiscal_quarter is not None:
                raise FiscalCalendarRuleError(
                    "Calendar-year annual periods cannot carry a fiscal quarter."
                )
            if (start, end) != (date(fiscal_year, 1, 1), date(fiscal_year, 12, 31)):
                raise FiscalCalendarRuleError(
                    "Calendar-year annual boundaries are not exact."
                )
    elif period_type == "quarter":
        if not isinstance(week_count, int) or week_count not in rule.quarter_week_counts:
            raise FiscalCalendarRuleError(
                "Source-labelled 52/53-week quarters require 13 or 14 exact weeks."
            )
        if not isinstance(period.get("is_53_week_year"), bool):
            raise FiscalCalendarRuleError(
                "Source-labelled fiscal quarters require an explicit 52/53-week year classification."
            )
        if actual_days != week_count * 7:
            raise FiscalCalendarRuleError(
                "Source-labelled fiscal-quarter dates do not match week_count."
            )
    elif period_type == "annual":
        if not isinstance(week_count, int) or week_count not in rule.annual_week_counts:
            raise FiscalCalendarRuleError(
                "Source-labelled fiscal years require 52 or 53 exact weeks."
            )
        if actual_days != week_count * 7:
            raise FiscalCalendarRuleError(
                "Source-labelled fiscal-year dates do not match week_count."
            )
        if bool(period.get("is_53_week_year")) != (week_count == 53):
            raise FiscalCalendarRuleError(
                "Fiscal-year 52/53-week flag does not match week_count."
            )
    return rule


def _base_checks(
    earlier: Mapping[str, Any],
    later: Mapping[str, Any],
    earlier_calendar: Mapping[str, Any],
    later_calendar: Mapping[str, Any],
) -> dict[str, bool]:
    return {
        "same_calendar": bool(earlier.get("calendar_id"))
        and earlier.get("calendar_id") == later.get("calendar_id")
        and earlier.get("calendar_id") == earlier_calendar.get("calendar_id")
        and later.get("calendar_id") == later_calendar.get("calendar_id")
        and earlier_calendar.get("calendar_id") == later_calendar.get("calendar_id"),
        "quarter_periods": earlier.get("period_type")
        == later.get("period_type")
        == "quarter",
        "same_duration": earlier.get("day_count") == later.get("day_count")
        and earlier.get("week_count") == later.get("week_count"),
    }


def _quarter_sequence_is_next(earlier: Mapping[str, Any], later: Mapping[str, Any]) -> bool:
    earlier_quarter = earlier.get("fiscal_quarter")
    later_quarter = later.get("fiscal_quarter")
    earlier_year = earlier.get("fiscal_year")
    later_year = later.get("fiscal_year")
    if not all(isinstance(value, int) for value in (earlier_quarter, later_quarter, earlier_year, later_year)):
        return False
    expected_quarter = 1 if earlier_quarter == 4 else earlier_quarter + 1
    expected_year = earlier_year + 1 if earlier_quarter == 4 else earlier_year
    return later_quarter == expected_quarter and later_year == expected_year


def evaluate_period_compatibility(
    earlier: Mapping[str, Any],
    later: Mapping[str, Any],
    *,
    earlier_calendar: Mapping[str, Any],
    later_calendar: Mapping[str, Any],
    change_kind: str,
) -> dict[str, Any]:
    """Return deterministic compatibility state without trusting stored results."""

    checks = _base_checks(earlier, later, earlier_calendar, later_calendar)
    try:
        if not checks["same_calendar"]:
            raise FiscalCalendarRuleError(
                "Periods must resolve to the same canonical fiscal calendar."
            )
        if (
            earlier_calendar.get("reconciliation_state") != "reconciled"
            or later_calendar.get("reconciliation_state") != "reconciled"
            or earlier.get("reconciliation_state") != "reconciled"
            or later.get("reconciliation_state") != "reconciled"
        ):
            raise FiscalCalendarRuleError(
                "Period compatibility requires reconciled periods and calendars."
            )
        company_ids = {
            str(value)
            for value in (
                earlier.get("company_id"),
                later.get("company_id"),
                earlier_calendar.get("company_id"),
                later_calendar.get("company_id"),
            )
            if value is not None
        }
        if len(company_ids) != 1:
            raise FiscalCalendarRuleError(
                "Periods and calendars must share one company identity."
            )
        if not checks["quarter_periods"]:
            raise FiscalCalendarRuleError(
                "Percentage-point QoQ/YoY rules require two fiscal quarters."
            )
        earlier_rule = validate_period_for_calendar_rule(earlier, earlier_calendar)
        later_rule = validate_period_for_calendar_rule(later, later_calendar)
        if earlier_rule.rule_id != later_rule.rule_id:
            raise FiscalCalendarRuleError(
                "Periods use different fiscal calendar rules."
            )
        if earlier_rule.comparison_mode == "equal-duration" and not checks["same_duration"]:
            raise FiscalCalendarRuleError(
                "Source-labelled 52/53-week periods require identical duration."
            )
        if (
            earlier_rule.comparison_mode == "equal-duration"
            and earlier.get("is_53_week_year") != later.get("is_53_week_year")
        ):
            raise FiscalCalendarRuleError(
                "Source-labelled fiscal-year-length classification differs."
            )

        earlier_start, earlier_end = _period_dates(earlier)
        later_start, _ = _period_dates(later)
        if earlier_end >= later_start or earlier_start >= later_start:
            raise FiscalCalendarRuleError(
                "Fiscal period boundaries overlap or are reversed."
            )

        if change_kind == "qoq-percentage-point":
            checks["adjacent_ordinal"] = (
                isinstance(earlier.get("fiscal_ordinal"), int)
                and isinstance(later.get("fiscal_ordinal"), int)
                and later["fiscal_ordinal"] == earlier["fiscal_ordinal"] + 1
            )
            checks["adjacent_dates"] = later_start == earlier_end + timedelta(days=1)
            if (
                not checks["adjacent_ordinal"]
                or not checks["adjacent_dates"]
                or not _quarter_sequence_is_next(earlier, later)
            ):
                raise FiscalCalendarRuleError(
                    "QoQ requires adjacent fiscal quarters, dates and ordinals."
                )
        elif change_kind == "yoy-percentage-point":
            checks["same_fiscal_quarter"] = (
                isinstance(earlier.get("fiscal_quarter"), int)
                and earlier.get("fiscal_quarter") == later.get("fiscal_quarter")
            )
            checks["next_fiscal_year"] = (
                isinstance(earlier.get("fiscal_year"), int)
                and isinstance(later.get("fiscal_year"), int)
                and later["fiscal_year"] == earlier["fiscal_year"] + 1
            )
            ordinal_delta_is_four = (
                isinstance(earlier.get("fiscal_ordinal"), int)
                and isinstance(later.get("fiscal_ordinal"), int)
                and later["fiscal_ordinal"] == earlier["fiscal_ordinal"] + 4
            )
            if (
                not checks["same_fiscal_quarter"]
                or not checks["next_fiscal_year"]
                or not ordinal_delta_is_four
            ):
                raise FiscalCalendarRuleError(
                    "YoY requires the same fiscal quarter in the next fiscal year with ordinal difference four."
                )
        else:
            raise FiscalCalendarRuleError(
                f"Unsupported change rule {change_kind!r}."
            )
    except FiscalCalendarRuleError as exc:
        return {"comparable": False, "reason": str(exc), "checks": checks}

    reason = (
        "calendar-year quarter relationship is exact"
        if earlier_rule.comparison_mode == "calendar-year"
        else "filing-backed quarter relationship is exact"
    )
    return {"comparable": True, "reason": reason, "checks": checks}


def compare_periods(
    earlier: Mapping[str, Any],
    later: Mapping[str, Any],
    *,
    earlier_calendar: Mapping[str, Any],
    later_calendar: Mapping[str, Any],
    change_kind: str,
) -> dict[str, Any]:
    """Require one compatible result for construction and semantic replay."""

    result = evaluate_period_compatibility(
        earlier,
        later,
        earlier_calendar=earlier_calendar,
        later_calendar=later_calendar,
        change_kind=change_kind,
    )
    if not result["comparable"]:
        raise IncomparablePeriodError(str(result["reason"]))
    return result
