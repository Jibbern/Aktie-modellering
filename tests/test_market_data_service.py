from __future__ import annotations

import pandas as pd

from pbi_xbrl.market_data.service import (
    PARSED_SCHEMA_COLUMNS,
    _build_export_rows,
    _build_quarterly_rows,
    _dedupe_parsed_df,
    _standardize_parsed_df,
)


def _parsed_row(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "observation_date": "2025-06-15",
        "quarter": "2025-06-30",
        "aggregation_level": "observation",
        "publication_date": "2025-06-16",
        "source": "demo",
        "report_type": "provider_demo",
        "source_type": "provider_demo",
        "market_family": "corn_price",
        "series_key": "corn_cash_demo",
        "instrument": "Corn",
        "location": "Iowa",
        "region": "midwest",
        "tenor": "",
        "price_value": "4.10",
        "unit": "$/bushel",
        "quality": "high",
        "source_file": "demo.csv",
        "parsed_note": "demo",
        "origin": "provider_raw",
        "_priority": 40,
        "_obs_count": 1,
    }
    base.update(overrides)
    return base


def test_standardize_parsed_df_normalizes_schema_and_obs_count_alias() -> None:
    raw = pd.DataFrame(
        [
            {
                **_parsed_row(),
                "obs_count": "3",
                "_obs_count": None,
                "_priority": "50",
            }
        ]
    )

    out = _standardize_parsed_df(raw)

    assert list(out.columns) == PARSED_SCHEMA_COLUMNS
    assert out.loc[0, "_obs_count"] == 3
    assert out.loc[0, "_priority"] == 50
    assert str(out.loc[0, "quarter"].date()) == "2025-06-30"


def test_dedupe_parsed_df_keeps_highest_priority_and_latest_record() -> None:
    df = pd.DataFrame(
        [
            _parsed_row(_priority=20, publication_date="2025-06-10", price_value=4.00),
            _parsed_row(_priority=50, publication_date="2025-06-20", price_value=4.25),
        ]
    )

    out = _dedupe_parsed_df(_standardize_parsed_df(df))

    assert len(out) == 1
    assert out.loc[0, "price_value"] == 4.25
    assert out.loc[0, "_priority"] == 50


def test_build_quarterly_rows_preserves_dedupe_precedence_and_order() -> None:
    obs_df = pd.DataFrame(columns=PARSED_SCHEMA_COLUMNS)
    bootstrap_df = _standardize_parsed_df(
        pd.DataFrame(
            [
                _parsed_row(
                    aggregation_level="quarter_avg",
                    source="shared_source",
                    source_type="bootstrap_demo",
                    report_type="bootstrap_demo",
                    price_value=4.00,
                    _priority=10,
                )
            ]
        )
    )
    provider_df = _standardize_parsed_df(
        pd.DataFrame(
            [
                _parsed_row(
                    aggregation_level="quarter_avg",
                    source="shared_source",
                    source_type="provider_demo",
                    report_type="provider_demo",
                    price_value=4.35,
                    _priority=50,
                ),
                _parsed_row(
                    aggregation_level="quarter_end",
                    series_key="ethanol_demo",
                    market_family="ethanol_price",
                    instrument="Ethanol",
                    price_value=2.15,
                    _priority=45,
                ),
            ]
        )
    )

    out = _build_quarterly_rows(obs_df, bootstrap_df, provider_df)

    assert list(out["series_key"]) == ["corn_cash_demo", "ethanol_demo"]
    assert list(out["aggregation_level"]) == ["quarter_avg", "quarter_end"]
    assert out.loc[out["series_key"] == "corn_cash_demo", "price_value"].iloc[0] == 4.35


def test_build_export_rows_preserves_deterministic_sorting_and_fallback_source_type() -> None:
    quarterly_df = _standardize_parsed_df(
        pd.DataFrame(
            [
                _parsed_row(
                    aggregation_level="quarter_avg",
                    observation_date="2025-06-30",
                    publication_date="2025-07-01",
                    source_type=None,
                    report_type="provider_quarterly",
                )
            ]
        )
    )
    observations_df = _standardize_parsed_df(
        pd.DataFrame(
            [
                _parsed_row(observation_date="2025-06-10", publication_date="2025-06-11", series_key="ethanol_demo"),
                _parsed_row(observation_date="2025-06-05", publication_date="2025-06-06", series_key="corn_cash_demo"),
            ]
        )
    )

    out = _build_export_rows(quarterly_df, observations_df)

    assert list(out["series_key"]) == ["corn_cash_demo", "corn_cash_demo", "ethanol_demo"]
    assert list(out["aggregation_level"]) == ["observation", "quarter_avg", "observation"]
    assert out.loc[out["aggregation_level"] == "quarter_avg", "source_type"].iloc[0] == "provider_quarterly"
