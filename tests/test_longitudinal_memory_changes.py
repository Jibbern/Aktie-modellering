from __future__ import annotations

import inspect
from copy import deepcopy

import pytest

from pbi_xbrl.longitudinal_memory.calendar_rules import (
    CALENDAR_YEAR_RULE_ID,
    SOURCE_LABELLED_52_53_WEEK_RULE_ID,
    evaluate_period_compatibility,
)
import pbi_xbrl.longitudinal_memory.calendar_rules as calendar_rules_module
from pbi_xbrl.longitudinal_memory.changes import (
    IncomparablePeriodError,
    IncompatibleFactError,
    derive_percentage_point_change,
    derive_quarter_from_ytd,
    percentage_change,
    validate_complete_ttm,
)
from pbi_xbrl.longitudinal_memory.identity import build_identity, identity_digest


def _calendar(
    rule_id=SOURCE_LABELLED_52_53_WEEK_RULE_ID,
    *,
    calendar_id="calendar:test:fiscal@1",
):
    return {
        "calendar_id": calendar_id,
        "calendar_rule_id": rule_id,
        "company_id": "TEST",
        "reconciliation_state": "reconciled",
    }


def _period(
    key,
    year,
    quarter,
    start,
    end,
    ordinal,
    *,
    weeks=13,
    days=91,
    period_type="quarter",
    calendar_id="calendar:test:fiscal@1",
):
    return {
        "period_id": f"period:test:{key}@1",
        "calendar_id": calendar_id,
        "company_id": "TEST",
        "fiscal_year": year,
        "fiscal_quarter": quarter,
        "period_type": period_type,
        "start_date": start,
        "end_date": end,
        "day_count": days,
        "week_count": weeks,
        "fiscal_ordinal": ordinal,
        "is_53_week_year": False,
        "reconciliation_state": "reconciled",
    }


def _fact(key, period, value="1"):
    record_id = build_identity("fact", (("key", key),))
    business_key = build_identity("business-fact", (("key", "comp-sales"),))
    return {
        "header": {
            "record_id": record_id,
            "identity_digest": identity_digest(record_id),
            "record_type": "NumericalFact",
            "company_id": "TEST",
            "subject_id": "comparable-sales",
            "knowledge_date": "2026-03-04",
            "effective_period_id": period["period_id"],
            "fiscal_period_id": period["period_id"],
            "period_type": "quarter",
            "dimension_set_id": "dimset:v1|members=total",
            "evidence_occurrence_ids": [build_identity("occ", (("key", key),))],
        },
        "payload": {
            "kind": "NumericalFact",
            "business_key": business_key,
            "metric_id": "metric:core:comparable-sales@1",
            "definition_id": "definition:core:reported@1",
            "basis_id": "basis:core:reported@1",
            "unit_id": "unit:core:percent@1",
            "currency": None,
            "value": {"kind": "exact", "value": value},
        },
    }


Q3 = _period("fy2025-q3", 2025, 3, "2025-08-03", "2025-11-01", 103)
Q4 = _period("fy2025-q4", 2025, 4, "2025-11-02", "2026-01-31", 104)
PRIOR_Q4 = _period("fy2024-q4", 2024, 4, "2024-11-03", "2025-02-01", 100)
ANF_CALENDAR = _calendar()
CALENDAR_YEAR_CALENDAR = _calendar(
    CALENDAR_YEAR_RULE_ID, calendar_id="calendar:test:calendar-year@1"
)
CALENDAR_Q4_2025 = _period(
    "calendar-fy2025-q4",
    2025,
    4,
    "2025-10-01",
    "2025-12-31",
    200,
    weeks=None,
    days=92,
    calendar_id=CALENDAR_YEAR_CALENDAR["calendar_id"],
)
CALENDAR_Q1_2026 = _period(
    "calendar-fy2026-q1",
    2026,
    1,
    "2026-01-01",
    "2026-03-31",
    201,
    weeks=None,
    days=90,
    calendar_id=CALENDAR_YEAR_CALENDAR["calendar_id"],
)
CALENDAR_Q2_2026 = _period(
    "calendar-fy2026-q2",
    2026,
    2,
    "2026-04-01",
    "2026-06-30",
    202,
    weeks=None,
    days=91,
    calendar_id=CALENDAR_YEAR_CALENDAR["calendar_id"],
)
CALENDAR_Q3_2026 = _period(
    "calendar-fy2026-q3",
    2026,
    3,
    "2026-07-01",
    "2026-09-30",
    203,
    weeks=None,
    days=92,
    calendar_id=CALENDAR_YEAR_CALENDAR["calendar_id"],
)


def test_safe_qoq_and_yoy_percentage_point_changes_retain_inputs_and_rule():
    q3_fact, q4_fact, prior_q4_fact = _fact("q3", Q3, "3"), _fact("q4", Q4, "1"), _fact("prior-q4", PRIOR_Q4, "14")
    qoq = derive_percentage_point_change(
        q3_fact,
        q4_fact,
        earlier_period=Q3,
        later_period=Q4,
        earlier_calendar=ANF_CALENDAR,
        later_calendar=ANF_CALENDAR,
        change_kind="qoq-percentage-point",
        rule_id="rule:core:qoq-percentage-point@1",
        change_unit_id="unit:core:percentage-point@1",
    )
    yoy = derive_percentage_point_change(
        prior_q4_fact,
        q4_fact,
        earlier_period=PRIOR_Q4,
        later_period=Q4,
        earlier_calendar=ANF_CALENDAR,
        later_calendar=ANF_CALENDAR,
        change_kind="yoy-percentage-point",
        rule_id="rule:core:yoy-percentage-point@1",
        change_unit_id="unit:core:percentage-point@1",
    )
    assert qoq["payload"]["value"] == {"kind": "exact", "value": "-2"}
    assert yoy["payload"]["value"] == {"kind": "exact", "value": "-13"}
    assert qoq["payload"]["comparability"]["comparable"] is True
    assert qoq["payload"]["input_record_ids"] == sorted([q3_fact["header"]["record_id"], q4_fact["header"]["record_id"]])
    assert qoq["payload"]["rule_id"].endswith("@1")


@pytest.mark.parametrize(
    ("change_kind", "earlier_template"),
    [
        ("qoq-percentage-point", Q3),
        ("yoy-percentage-point", PRIOR_Q4),
    ],
)
@pytest.mark.parametrize(
    ("earlier_is_53_week_year", "later_is_53_week_year"),
    [(False, True), (True, False)],
)
def test_source_labelled_quarter_year_classification_mismatch_fails_closed(
    change_kind,
    earlier_template,
    earlier_is_53_week_year,
    later_is_53_week_year,
):
    earlier = deepcopy(earlier_template)
    later = deepcopy(Q4)
    earlier["is_53_week_year"] = earlier_is_53_week_year
    later["is_53_week_year"] = later_is_53_week_year
    result = evaluate_period_compatibility(
        earlier,
        later,
        earlier_calendar=ANF_CALENDAR,
        later_calendar=ANF_CALENDAR,
        change_kind=change_kind,
    )
    assert result["comparable"] is False
    assert result["reason"] == "Source-labelled fiscal-year-length classification differs."


@pytest.mark.parametrize(
    ("change_kind", "earlier_template"),
    [
        ("qoq-percentage-point", Q3),
        ("yoy-percentage-point", PRIOR_Q4),
    ],
)
@pytest.mark.parametrize("is_53_week_year", [False, True])
def test_source_labelled_quarters_with_matching_year_classification_remain_compatible(
    change_kind, earlier_template, is_53_week_year
):
    earlier = deepcopy(earlier_template)
    later = deepcopy(Q4)
    earlier["is_53_week_year"] = is_53_week_year
    later["is_53_week_year"] = is_53_week_year
    result = evaluate_period_compatibility(
        earlier,
        later,
        earlier_calendar=ANF_CALENDAR,
        later_calendar=ANF_CALENDAR,
        change_kind=change_kind,
    )
    assert result["comparable"] is True


def test_calendar_year_qoq_constructs_three_percentage_points_and_replays_exactly():
    earlier = _fact("calendar-q1", CALENDAR_Q1_2026, "-8")
    later = _fact("calendar-q2", CALENDAR_Q2_2026, "-5")
    change = derive_percentage_point_change(
        earlier,
        later,
        earlier_period=CALENDAR_Q1_2026,
        later_period=CALENDAR_Q2_2026,
        earlier_calendar=CALENDAR_YEAR_CALENDAR,
        later_calendar=CALENDAR_YEAR_CALENDAR,
        change_kind="qoq-percentage-point",
        rule_id="rule:core:qoq-percentage-point@1",
        change_unit_id="unit:core:percentage-point@1",
    )
    assert change["payload"]["value"] == {"kind": "exact", "value": "3"}
    assert change["payload"]["comparability"] == evaluate_period_compatibility(
        CALENDAR_Q1_2026,
        CALENDAR_Q2_2026,
        earlier_calendar=CALENDAR_YEAR_CALENDAR,
        later_calendar=CALENDAR_YEAR_CALENDAR,
        change_kind="qoq-percentage-point",
    )
    assert change["payload"]["comparability"]["checks"]["same_duration"] is False


def test_calendar_year_qoq_supports_year_rollover_and_natural_quarter_lengths():
    rollover = evaluate_period_compatibility(
        CALENDAR_Q4_2025,
        CALENDAR_Q1_2026,
        earlier_calendar=CALENDAR_YEAR_CALENDAR,
        later_calendar=CALENDAR_YEAR_CALENDAR,
        change_kind="qoq-percentage-point",
    )
    q2_q3 = evaluate_period_compatibility(
        CALENDAR_Q2_2026,
        CALENDAR_Q3_2026,
        earlier_calendar=CALENDAR_YEAR_CALENDAR,
        later_calendar=CALENDAR_YEAR_CALENDAR,
        change_kind="qoq-percentage-point",
    )
    assert rollover["comparable"] is True
    assert q2_q3["comparable"] is True
    assert q2_q3["checks"]["same_duration"] is False


def test_calendar_year_yoy_accepts_natural_leap_year_duration_difference():
    leap_q1 = _period(
        "calendar-fy2024-q1",
        2024,
        1,
        "2024-01-01",
        "2024-03-31",
        193,
        weeks=None,
        days=91,
        calendar_id=CALENDAR_YEAR_CALENDAR["calendar_id"],
    )
    normal_q1 = _period(
        "calendar-fy2025-q1",
        2025,
        1,
        "2025-01-01",
        "2025-03-31",
        197,
        weeks=None,
        days=90,
        calendar_id=CALENDAR_YEAR_CALENDAR["calendar_id"],
    )
    result = evaluate_period_compatibility(
        leap_q1,
        normal_q1,
        earlier_calendar=CALENDAR_YEAR_CALENDAR,
        later_calendar=CALENDAR_YEAR_CALENDAR,
        change_kind="yoy-percentage-point",
    )
    assert result["comparable"] is True
    assert result["checks"]["same_duration"] is False


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("start_date", "2026-01-02", "day_count"),
        ("end_date", "2026-03-30", "day_count"),
        ("day_count", 89, "day_count"),
        ("week_count", 13, "week_count"),
        ("fiscal_year", 2025, "boundaries"),
        ("fiscal_quarter", 2, "boundaries"),
        ("fiscal_ordinal", 199, "adjacent"),
    ],
)
def test_calendar_year_period_mutations_fail_closed(field, value, reason):
    earlier = deepcopy(CALENDAR_Q1_2026)
    earlier[field] = value
    result = evaluate_period_compatibility(
        earlier,
        CALENDAR_Q2_2026,
        earlier_calendar=CALENDAR_YEAR_CALENDAR,
        later_calendar=CALENDAR_YEAR_CALENDAR,
        change_kind="qoq-percentage-point",
    )
    assert result["comparable"] is False
    assert reason in result["reason"]


def test_missing_unknown_different_and_misapplied_calendar_rules_fail_closed():
    missing_rule = {key: value for key, value in CALENDAR_YEAR_CALENDAR.items() if key != "calendar_rule_id"}
    unknown_rule = {**CALENDAR_YEAR_CALENDAR, "calendar_rule_id": "rule:core:unknown-calendar@1"}
    different_calendar = {
        **CALENDAR_YEAR_CALENDAR,
        "calendar_id": "calendar:test:other@1",
    }
    later_other = {**CALENDAR_Q2_2026, "calendar_id": different_calendar["calendar_id"]}
    cases = [
        (CALENDAR_Q1_2026, CALENDAR_Q2_2026, missing_rule, missing_rule),
        (CALENDAR_Q1_2026, CALENDAR_Q2_2026, unknown_rule, unknown_rule),
        (CALENDAR_Q1_2026, later_other, CALENDAR_YEAR_CALENDAR, different_calendar),
        (Q3, Q4, CALENDAR_YEAR_CALENDAR, CALENDAR_YEAR_CALENDAR),
        (CALENDAR_Q1_2026, CALENDAR_Q2_2026, ANF_CALENDAR, ANF_CALENDAR),
    ]
    for earlier, later, earlier_calendar, later_calendar in cases:
        assert evaluate_period_compatibility(
            earlier,
            later,
            earlier_calendar=earlier_calendar,
            later_calendar=later_calendar,
            change_kind="qoq-percentage-point",
        )["comparable"] is False

    different_rule = {
        **CALENDAR_YEAR_CALENDAR,
        "calendar_rule_id": SOURCE_LABELLED_52_53_WEEK_RULE_ID,
    }
    assert evaluate_period_compatibility(
        CALENDAR_Q1_2026,
        CALENDAR_Q2_2026,
        earlier_calendar=CALENDAR_YEAR_CALENDAR,
        later_calendar=different_rule,
        change_kind="qoq-percentage-point",
    )["comparable"] is False


def test_calendar_rule_is_not_inferred_and_unresolved_state_fails_closed():
    exact_but_untyped = {
        **CALENDAR_YEAR_CALENDAR,
        "calendar_id": "calendar:test:looks-like-calendar-year@1",
    }
    exact_but_untyped.pop("calendar_rule_id")
    periods = [
        {**CALENDAR_Q1_2026, "calendar_id": exact_but_untyped["calendar_id"]},
        {**CALENDAR_Q2_2026, "calendar_id": exact_but_untyped["calendar_id"]},
    ]
    assert evaluate_period_compatibility(
        *periods,
        earlier_calendar=exact_but_untyped,
        later_calendar=exact_but_untyped,
        change_kind="qoq-percentage-point",
    )["comparable"] is False

    unresolved = {**CALENDAR_YEAR_CALENDAR, "reconciliation_state": "needs_review"}
    assert evaluate_period_compatibility(
        CALENDAR_Q1_2026,
        CALENDAR_Q2_2026,
        earlier_calendar=unresolved,
        later_calendar=unresolved,
        change_kind="qoq-percentage-point",
    )["comparable"] is False

    runtime = inspect.getsource(calendar_rules_module).casefold()
    assert all(token not in runtime for token in ("anf", "pbi", "gpre"))


def test_annual_calendar_year_change_is_not_broadened_beyond_c1_change_types():
    prior = _period(
        "calendar-fy2024",
        2024,
        None,
        "2024-01-01",
        "2024-12-31",
        196,
        weeks=None,
        days=366,
        period_type="annual",
        calendar_id=CALENDAR_YEAR_CALENDAR["calendar_id"],
    )
    current = _period(
        "calendar-fy2025",
        2025,
        None,
        "2025-01-01",
        "2025-12-31",
        200,
        weeks=None,
        days=365,
        period_type="annual",
        calendar_id=CALENDAR_YEAR_CALENDAR["calendar_id"],
    )
    result = evaluate_period_compatibility(
        prior,
        current,
        earlier_calendar=CALENDAR_YEAR_CALENDAR,
        later_calendar=CALENDAR_YEAR_CALENDAR,
        change_kind="yoy-percentage-point",
    )
    assert result["comparable"] is False
    assert "require two fiscal quarters" in result["reason"]


def test_wrong_calendar_year_sequence_change_type_and_yoy_ordinal_fail_closed():
    nonadjacent = deepcopy(CALENDAR_Q3_2026)
    result = evaluate_period_compatibility(
        CALENDAR_Q1_2026,
        nonadjacent,
        earlier_calendar=CALENDAR_YEAR_CALENDAR,
        later_calendar=CALENDAR_YEAR_CALENDAR,
        change_kind="qoq-percentage-point",
    )
    assert result["comparable"] is False

    wrong_kind = evaluate_period_compatibility(
        CALENDAR_Q1_2026,
        CALENDAR_Q2_2026,
        earlier_calendar=CALENDAR_YEAR_CALENDAR,
        later_calendar=CALENDAR_YEAR_CALENDAR,
        change_kind="yoy-percentage-point",
    )
    assert wrong_kind["comparable"] is False

    prior = {**CALENDAR_Q1_2026, "fiscal_year": 2025, "start_date": "2025-01-01", "end_date": "2025-03-31", "fiscal_ordinal": 198}
    later = {**CALENDAR_Q1_2026, "fiscal_ordinal": 201}
    wrong_ordinal = evaluate_period_compatibility(
        prior,
        later,
        earlier_calendar=CALENDAR_YEAR_CALENDAR,
        later_calendar=CALENDAR_YEAR_CALENDAR,
        change_kind="yoy-percentage-point",
    )
    assert wrong_ordinal["comparable"] is False
    assert "ordinal difference four" in wrong_ordinal["reason"]


@pytest.mark.parametrize("field", ["metric_id", "definition_id", "basis_id", "unit_id", "currency"])
def test_unit_basis_definition_currency_and_metric_incompatibility_fails(field):
    earlier, later = _fact("q3", Q3), _fact("q4", Q4)
    later["payload"][field] = "different"
    with pytest.raises(IncompatibleFactError, match=field):
        derive_percentage_point_change(
            earlier,
            later,
            earlier_period=Q3,
            later_period=Q4,
            earlier_calendar=ANF_CALENDAR,
            later_calendar=ANF_CALENDAR,
            change_kind="qoq-percentage-point",
            rule_id="rule:core:qoq-percentage-point@1",
            change_unit_id="unit:core:percentage-point@1",
        )


def test_dimension_and_nonexact_values_fail_closed():
    earlier, later = _fact("q3", Q3), _fact("q4", Q4)
    later["header"]["dimension_set_id"] = "dimset:v1|members=other"
    with pytest.raises(IncompatibleFactError, match="dimension_set_id"):
        derive_percentage_point_change(earlier, later, earlier_period=Q3, later_period=Q4, earlier_calendar=ANF_CALENDAR, later_calendar=ANF_CALENDAR, change_kind="qoq-percentage-point", rule_id="rule:core:qoq-percentage-point@1", change_unit_id="unit:core:percentage-point@1")
    later = _fact("q4", Q4)
    later["payload"]["value"] = {"kind": "approximate", "value": "1", "qualifier": "around", "tolerance": None}
    with pytest.raises(IncompatibleFactError, match="exact"):
        derive_percentage_point_change(earlier, later, earlier_period=Q3, later_period=Q4, earlier_calendar=ANF_CALENDAR, later_calendar=ANF_CALENDAR, change_kind="qoq-percentage-point", rule_id="rule:core:qoq-percentage-point@1", change_unit_id="unit:core:percentage-point@1")


def test_nonadjacent_qoq_wrong_quarter_yoy_and_52_53_week_mismatch_fail():
    nonadjacent = deepcopy(Q4); nonadjacent["fiscal_ordinal"] = 105
    with pytest.raises(IncomparablePeriodError, match="adjacent"):
        derive_percentage_point_change(_fact("q3", Q3), _fact("q4", nonadjacent), earlier_period=Q3, later_period=nonadjacent, earlier_calendar=ANF_CALENDAR, later_calendar=ANF_CALENDAR, change_kind="qoq-percentage-point", rule_id="rule:core:qoq-percentage-point@1", change_unit_id="unit:core:percentage-point@1")

    wrong_quarter = deepcopy(Q4); wrong_quarter["fiscal_quarter"] = 3
    with pytest.raises(IncomparablePeriodError, match="same fiscal quarter"):
        derive_percentage_point_change(_fact("prior", PRIOR_Q4), _fact("later", wrong_quarter), earlier_period=PRIOR_Q4, later_period=wrong_quarter, earlier_calendar=ANF_CALENDAR, later_calendar=ANF_CALENDAR, change_kind="yoy-percentage-point", rule_id="rule:core:yoy-percentage-point@1", change_unit_id="unit:core:percentage-point@1")

    week_53 = deepcopy(Q4); week_53["start_date"] = "2025-10-26"; week_53["week_count"] = 14; week_53["day_count"] = 98
    with pytest.raises(IncomparablePeriodError, match="52/53"):
        derive_percentage_point_change(_fact("prior", PRIOR_Q4), _fact("later", week_53), earlier_period=PRIOR_Q4, later_period=week_53, earlier_calendar=ANF_CALENDAR, later_calendar=ANF_CALENDAR, change_kind="yoy-percentage-point", rule_id="rule:core:yoy-percentage-point@1", change_unit_id="unit:core:percentage-point@1")


def test_zero_denominator_missing_ttm_and_unsafe_ytd_fail_closed():
    with pytest.raises(IncompatibleFactError, match="zero denominator"):
        percentage_change("0", "1")
    with pytest.raises(IncomparablePeriodError, match="exactly four"):
        validate_complete_ttm([Q3, Q4], calendar=ANF_CALENDAR)

    first_ytd = _period("fy2025-h1", 2025, 2, "2025-02-02", "2025-08-02", 102, weeks=26, days=182, period_type="ytd")
    nine_month = _period("fy2025-9m", 2025, 3, "2025-02-02", "2025-11-01", 103, weeks=39, days=273, period_type="ytd")
    assert derive_quarter_from_ytd("30", "20", current_period=nine_month, prior_period=first_ytd) == "10"
    unsafe = deepcopy(first_ytd); unsafe["start_date"] = "2025-02-03"
    with pytest.raises(IncomparablePeriodError, match="cumulative"):
        derive_quarter_from_ytd("30", "20", current_period=nine_month, prior_period=unsafe)


def test_explicit_zero_is_a_value_not_missing_or_unavailable():
    zero = _fact("zero", Q4, "0")
    assert zero["payload"]["value"] == {"kind": "exact", "value": "0"}
    unavailable = {
        "payload": {
            "kind": "AvailabilityObservation",
            "availability_state": "not-disclosed",
            "reason": "source explicitly says not disclosed",
        }
    }
    assert unavailable["payload"]["kind"] != zero["payload"]["kind"]
    missing = None
    assert missing is None
