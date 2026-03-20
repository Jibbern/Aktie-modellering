from __future__ import annotations

import datetime as dt

import pandas as pd

from pbi_xbrl.metrics import MetricSpec
from pbi_xbrl.period_resolver import choose_best_tag, derive_quarter_from_ytd, quarter_ends_for_fy


def test_choose_best_tag_prefers_real_3m_coverage() -> None:
    spec = MetricSpec(
        name="demo_metric",
        tags=["NoQuarterTag", "QuarterTag"],
        kind="duration",
        unit="USD",
        prefer_forms=["10-Q", "10-K"],
    )
    df = pd.DataFrame(
        [
            {
                "tag": "NoQuarterTag",
                "start_d": dt.date(2025, 1, 1),
                "end_d": dt.date(2025, 6, 30),
            },
            {
                "tag": "QuarterTag",
                "start_d": dt.date(2025, 4, 1),
                "end_d": dt.date(2025, 6, 30),
            },
        ]
    )

    assert choose_best_tag(df, spec) == "QuarterTag"


def test_quarter_ends_for_fy_handles_non_calendar_year_end() -> None:
    result = quarter_ends_for_fy(dt.date(2025, 9, 30))

    assert result == {
        "Q1": dt.date(2024, 12, 31),
        "Q2": dt.date(2025, 3, 31),
        "Q3": dt.date(2025, 6, 30),
        "FY": dt.date(2025, 9, 30),
    }


def test_derive_quarter_from_ytd_prefers_direct_and_rejects_stale_ytd_pairs() -> None:
    fy_map = {
        (2025, "FY"): dt.date(2025, 12, 31),
        (2025, "Q1"): dt.date(2025, 3, 31),
        (2025, "Q2"): dt.date(2025, 6, 30),
        (2025, "Q3"): dt.date(2025, 9, 30),
    }
    direct_facts = pd.DataFrame(
        [
            {
                "tag": "Revenue",
                "start_d": dt.date(2025, 4, 1),
                "end_d": dt.date(2025, 6, 30),
                "val": 40.0,
                "accn": "0001",
                "form": "10-Q",
                "filed_d": dt.date(2025, 8, 5),
                "unit": "USD",
                "fy": 2025,
            },
            {
                "tag": "Revenue",
                "start_d": dt.date(2025, 1, 1),
                "end_d": dt.date(2025, 6, 30),
                "val": 70.0,
                "accn": "0001",
                "form": "10-Q",
                "filed_d": dt.date(2025, 8, 5),
                "unit": "USD",
                "fy": 2025,
            },
            {
                "tag": "Revenue",
                "start_d": dt.date(2025, 1, 1),
                "end_d": dt.date(2025, 3, 31),
                "val": 30.0,
                "accn": "0000",
                "form": "10-Q",
                "filed_d": dt.date(2025, 5, 1),
                "unit": "USD",
                "fy": 2025,
            },
        ]
    )

    direct_pick = derive_quarter_from_ytd(
        direct_facts,
        end=dt.date(2025, 6, 30),
        quarter_index=2,
        fy_fp_to_end=fy_map,
        prefer_forms=["10-Q", "10-K"],
    )

    assert direct_pick is not None
    assert direct_pick.source == "direct"
    assert direct_pick.value == 40.0

    stale_facts = pd.DataFrame(
        [
            {
                "tag": "Revenue",
                "start_d": dt.date(2025, 1, 1),
                "end_d": dt.date(2025, 9, 30),
                "val": 90.0,
                "accn": "1000",
                "form": "10-Q",
                "filed_d": dt.date(2025, 11, 10),
                "unit": "USD",
                "fy": 2025,
            },
            {
                "tag": "Revenue",
                "start_d": dt.date(2025, 1, 1),
                "end_d": dt.date(2025, 6, 30),
                "val": 60.0,
                "accn": "0900",
                "form": "10-Q",
                "filed_d": dt.date(2025, 4, 1),
                "unit": "USD",
                "fy": 2025,
            },
        ]
    )

    stale_pick = derive_quarter_from_ytd(
        stale_facts,
        end=dt.date(2025, 9, 30),
        quarter_index=3,
        fy_fp_to_end=fy_map,
        prefer_forms=["10-Q", "10-K"],
    )

    assert stale_pick is None
