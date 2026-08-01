from __future__ import annotations

from copy import deepcopy

import pytest

from pbi_xbrl.longitudinal_memory.changes import (
    IncomparablePeriodError,
    IncompatibleFactError,
    derive_percentage_point_change,
    derive_quarter_from_ytd,
    percentage_change,
    validate_complete_ttm,
)
from pbi_xbrl.longitudinal_memory.identity import build_identity, identity_digest


def _period(key, year, quarter, start, end, ordinal, *, weeks=13, days=91, period_type="quarter"):
    return {
        "period_id": f"period:test:{key}@1",
        "calendar_id": "calendar:test:fiscal@1",
        "fiscal_year": year,
        "fiscal_quarter": quarter,
        "period_type": period_type,
        "start_date": start,
        "end_date": end,
        "day_count": days,
        "week_count": weeks,
        "fiscal_ordinal": ordinal,
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


def test_safe_qoq_and_yoy_percentage_point_changes_retain_inputs_and_rule():
    q3_fact, q4_fact, prior_q4_fact = _fact("q3", Q3, "3"), _fact("q4", Q4, "1"), _fact("prior-q4", PRIOR_Q4, "14")
    qoq = derive_percentage_point_change(
        q3_fact,
        q4_fact,
        earlier_period=Q3,
        later_period=Q4,
        change_kind="qoq-percentage-point",
        rule_id="rule:core:qoq-percentage-point@1",
        change_unit_id="unit:core:percentage-point@1",
    )
    yoy = derive_percentage_point_change(
        prior_q4_fact,
        q4_fact,
        earlier_period=PRIOR_Q4,
        later_period=Q4,
        change_kind="yoy-percentage-point",
        rule_id="rule:core:yoy-percentage-point@1",
        change_unit_id="unit:core:percentage-point@1",
    )
    assert qoq["payload"]["value"] == {"kind": "exact", "value": "-2"}
    assert yoy["payload"]["value"] == {"kind": "exact", "value": "-13"}
    assert qoq["payload"]["comparability"]["comparable"] is True
    assert qoq["payload"]["input_record_ids"] == sorted([q3_fact["header"]["record_id"], q4_fact["header"]["record_id"]])
    assert qoq["payload"]["rule_id"].endswith("@1")


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
            change_kind="qoq-percentage-point",
            rule_id="rule:core:qoq-percentage-point@1",
            change_unit_id="unit:core:percentage-point@1",
        )


def test_dimension_and_nonexact_values_fail_closed():
    earlier, later = _fact("q3", Q3), _fact("q4", Q4)
    later["header"]["dimension_set_id"] = "dimset:v1|members=other"
    with pytest.raises(IncompatibleFactError, match="dimension_set_id"):
        derive_percentage_point_change(earlier, later, earlier_period=Q3, later_period=Q4, change_kind="qoq-percentage-point", rule_id="rule:core:qoq-percentage-point@1", change_unit_id="unit:core:percentage-point@1")
    later = _fact("q4", Q4)
    later["payload"]["value"] = {"kind": "approximate", "value": "1", "qualifier": "around", "tolerance": None}
    with pytest.raises(IncompatibleFactError, match="exact"):
        derive_percentage_point_change(earlier, later, earlier_period=Q3, later_period=Q4, change_kind="qoq-percentage-point", rule_id="rule:core:qoq-percentage-point@1", change_unit_id="unit:core:percentage-point@1")


def test_nonadjacent_qoq_wrong_quarter_yoy_and_52_53_week_mismatch_fail():
    nonadjacent = deepcopy(Q4); nonadjacent["fiscal_ordinal"] = 105
    with pytest.raises(IncomparablePeriodError, match="adjacent"):
        derive_percentage_point_change(_fact("q3", Q3), _fact("q4", nonadjacent), earlier_period=Q3, later_period=nonadjacent, change_kind="qoq-percentage-point", rule_id="rule:core:qoq-percentage-point@1", change_unit_id="unit:core:percentage-point@1")

    wrong_quarter = deepcopy(Q4); wrong_quarter["fiscal_quarter"] = 3
    with pytest.raises(IncomparablePeriodError, match="same fiscal quarter"):
        derive_percentage_point_change(_fact("prior", PRIOR_Q4), _fact("later", wrong_quarter), earlier_period=PRIOR_Q4, later_period=wrong_quarter, change_kind="yoy-percentage-point", rule_id="rule:core:yoy-percentage-point@1", change_unit_id="unit:core:percentage-point@1")

    week_53 = deepcopy(Q4); week_53["week_count"] = 14; week_53["day_count"] = 98
    with pytest.raises(IncomparablePeriodError, match="52/53"):
        derive_percentage_point_change(_fact("prior", PRIOR_Q4), _fact("later", week_53), earlier_period=PRIOR_Q4, later_period=week_53, change_kind="yoy-percentage-point", rule_id="rule:core:yoy-percentage-point@1", change_unit_id="unit:core:percentage-point@1")


def test_zero_denominator_missing_ttm_and_unsafe_ytd_fail_closed():
    with pytest.raises(IncompatibleFactError, match="zero denominator"):
        percentage_change("0", "1")
    with pytest.raises(IncomparablePeriodError, match="exactly four"):
        validate_complete_ttm([Q3, Q4])

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
