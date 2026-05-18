from __future__ import annotations

import datetime as dt

import pandas as pd

from pbi_xbrl.metrics import MetricSpec
from pbi_xbrl.pipeline import build_qa_checks
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


def test_qsum_vs_fy_uses_actual_non_calendar_fiscal_quarters() -> None:
    fiscal_years = {
        2023: [
            (dt.date(2022, 4, 30), "Q1", 800.0),
            (dt.date(2022, 7, 30), "Q2", 810.0),
            (dt.date(2022, 10, 29), "Q3", 880.0),
            (dt.date(2023, 1, 28), "FY", 1_200.0),
        ],
        2024: [
            (dt.date(2023, 4, 29), "Q1", 835.0),
            (dt.date(2023, 7, 29), "Q2", 935.0),
            (dt.date(2023, 10, 28), "Q3", 1_056.0),
            (dt.date(2024, 2, 3), "FY", 1_453.0),
        ],
        2025: [
            (dt.date(2024, 5, 4), "Q1", 1_021.0),
            (dt.date(2024, 8, 3), "Q2", 1_134.0),
            (dt.date(2024, 11, 2), "Q3", 1_209.0),
            (dt.date(2025, 2, 1), "FY", 1_585.0),
        ],
        2026: [
            (dt.date(2025, 5, 3), "Q1", 1_097.0),
            (dt.date(2025, 8, 2), "Q2", 1_209.0),
            (dt.date(2025, 11, 1), "Q3", 1_291.0),
            (dt.date(2026, 1, 31), "FY", 1_670.0),
        ],
    }
    metric_tags = {
        "revenue": "Revenues",
        "gross_profit": "GrossProfit",
        "op_income": "OperatingIncomeLoss",
        "net_income": "NetIncomeLoss",
        "cfo": "NetCashProvidedByUsedInOperatingActivities",
        "capex": "PaymentsToAcquirePropertyPlantAndEquipment",
    }

    hist_rows = [
        {
            "quarter": dt.date(2022, 1, 29),
            "revenue": 1_161.0,
            "gross_profit": 677.0,
            "op_income": 98.0,
            "net_income": 65.0,
            "cfo": 143.0,
            "capex": 35.0,
        }
    ]
    fact_rows = [
        {
            "tag": "Revenues",
            "start_d": dt.date(2021, 10, 31),
            "end_d": dt.date(2022, 1, 29),
            "val": 1_161.0,
            "unit": "USD",
            "form": "10-K",
            "filed_d": dt.date(2022, 3, 28),
            "fy": 2023,
            "fp": "FY",
        }
    ]

    for fy, rows in fiscal_years.items():
        fy_end = rows[-1][0]
        fy_start = rows[0][0] - dt.timedelta(days=89)
        for qd, fp, rev in rows:
            row = {"quarter": qd}
            for metric in metric_tags:
                row[metric] = rev * {"revenue": 1.0, "gross_profit": 0.6, "op_income": 0.12, "net_income": 0.09, "cfo": 0.18, "capex": 0.04}[metric]
            hist_rows.append(row)
            fact_rows.append(
                {
                    "tag": "Revenues",
                    "start_d": qd - dt.timedelta(days=89),
                    "end_d": qd,
                    "val": rev,
                    "unit": "USD",
                    "form": "10-Q" if fp != "FY" else "10-K",
                    "filed_d": qd + dt.timedelta(days=40),
                    "fy": fy,
                    "fp": fp,
                }
            )
        fy_totals = {metric: sum(row[2] * {"revenue": 1.0, "gross_profit": 0.6, "op_income": 0.12, "net_income": 0.09, "cfo": 0.18, "capex": 0.04}[metric] for row in rows) for metric in metric_tags}
        for metric, tag in metric_tags.items():
            fact_rows.append(
                {
                    "tag": tag,
                    "start_d": fy_start,
                    "end_d": fy_end,
                    "val": fy_totals[metric],
                    "unit": "USD",
                    "form": "10-K",
                    "filed_d": fy_end + dt.timedelta(days=45),
                    "fy": fy,
                    "fp": "FY",
                }
            )

    qa = build_qa_checks(pd.DataFrame(fact_rows), pd.DataFrame(hist_rows), audit=pd.DataFrame())
    qsum = qa[qa["check"].astype(str).eq("qsum_vs_fy")].copy()
    actionable = qsum[qsum["status"].astype(str).isin(["fail", "warn"])]

    assert actionable.empty
    for fy_end in [rows[-1][0] for rows in fiscal_years.values()]:
        for metric in metric_tags:
            match = qsum[(pd.to_datetime(qsum["quarter"]).dt.date == fy_end) & (qsum["metric"].eq(metric))]
            assert not match.empty
            assert set(match["status"]) == {"pass"}
