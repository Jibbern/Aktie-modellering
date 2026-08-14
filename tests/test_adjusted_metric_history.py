from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from pbi_xbrl.adjusted_metric_history import (
    AdjustedMetricHistoryError,
    AdjustedMetricSourceRole,
    build_adjusted_metric_history_selection,
    load_registered_issuer_recast_adjusted_metric_history,
    select_adjusted_metric_history,
)
from pbi_xbrl.doc_intel import _build_non_gaap_cred
from pbi_xbrl.non_gaap import parse_adjusted_from_ex99
from tests.workbook_test_resources import registered_data_root, registered_ticker_dir


def _fact_row(
    period: str,
    metric: str,
    value: float,
    *,
    period_type: str = "quarter",
    definition: str = "definition:test-adjusted-metric@1",
    scope: str = "test_scope",
    authority_rank: int = 300,
    occurrence: str | None = None,
    source_scale: str = "thousands",
) -> dict[str, object]:
    occurrence_id = occurrence or f"occurrence:{metric}:{period}:{period_type}:{value}"
    return {
        "quarter": pd.Timestamp(period),
        metric: value,
        "source": "ex99",
        "confidence": "high",
        "period_type": period_type,
        f"{metric}_metric_id": metric,
        f"{metric}_period_type": period_type,
        f"{metric}_basis": "adjusted_non_gaap",
        f"{metric}_scope": scope,
        f"{metric}_definition_id": definition,
        f"{metric}_source_role": "direct",
        f"{metric}_source_authority": "issuer_direct_period_release",
        f"{metric}_authority_rank": authority_rank,
        f"{metric}_source_document_id": "document:test",
        f"{metric}_source_occurrence_id": occurrence_id,
        f"{metric}_source_locator": f"table:test;metric:{metric};period:{period}",
        f"{metric}_source_scale": source_scale,
        f"{metric}_canonical_usd_millions": value / 1_000_000.0,
    }


def _metric_value(selected: pd.DataFrame, period: str, metric: str) -> float:
    rows = selected[
        (selected["quarter"] == pd.Timestamp(period))
        & (selected["metric_id"] == metric)
    ]
    assert len(rows) == 1
    return float(rows.iloc[0]["value"])


def test_mixed_metric_rows_remain_independently_owned_and_source_order_independent() -> None:
    rows = [
        _fact_row("2023-03-31", "adj_ebit", 68_028_000.0),
        _fact_row("2023-03-31", "adj_ebitda", 96_460_000.0),
        _fact_row("2023-03-31", "adj_fcf", -39_714_000.0),
    ]
    forward = select_adjusted_metric_history(pd.DataFrame(rows))
    reverse = select_adjusted_metric_history(pd.DataFrame(list(reversed(rows))))

    assert _metric_value(forward, "2023-03-31", "adj_ebit") == 68_028_000.0
    assert _metric_value(forward, "2023-03-31", "adj_ebitda") == 96_460_000.0
    assert _metric_value(forward, "2023-03-31", "adj_fcf") == -39_714_000.0
    pd.testing.assert_frame_equal(forward, reverse, check_dtype=False)


def test_unrelated_fcf_or_ebit_rows_cannot_suppress_another_metric() -> None:
    selected = select_adjusted_metric_history(
        pd.DataFrame(
            [
                _fact_row("2023-06-30", "adj_ebit", 69_313_000.0),
                _fact_row("2023-06-30", "adj_ebitda", 97_313_000.0),
                _fact_row("2023-06-30", "adj_fcf", 1.0),
            ]
        )
    )
    assert set(selected["metric_id"]) == {"adj_ebit", "adj_ebitda", "adj_fcf"}
    assert _metric_value(selected, "2023-06-30", "adj_ebit") == 69_313_000.0
    assert _metric_value(selected, "2023-06-30", "adj_ebitda") == 97_313_000.0


def test_exact_duplicate_corroborates_but_conflicting_same_owner_fails_closed() -> None:
    duplicate = _fact_row(
        "2023-09-30",
        "adj_ebit",
        84_044_000.0,
        occurrence="occurrence:one",
    )
    corroborating = dict(duplicate)
    corroborating["adj_ebit_source_occurrence_id"] = "occurrence:two"
    selected = select_adjusted_metric_history(pd.DataFrame([duplicate, corroborating]))
    assert int(selected.iloc[0]["corroboration_count"]) == 2

    conflicting = _fact_row(
        "2023-09-30",
        "adj_ebit",
        84_045_000.0,
        occurrence="occurrence:conflict",
    )
    with pytest.raises(AdjustedMetricHistoryError, match="Conflicting adjusted-metric values"):
        select_adjusted_metric_history(pd.DataFrame([duplicate, conflicting]))


def test_missing_metric_remains_missing_instead_of_borrowing_another_metric() -> None:
    selected = select_adjusted_metric_history(
        pd.DataFrame([_fact_row("2023-09-30", "adj_fcf", 25_305_000.0)])
    )
    assert "adj_ebit" not in set(selected["metric_id"])
    assert "adj_ebitda" not in set(selected["metric_id"])


def test_ytd_cannot_masquerade_as_quarter_but_exact_residual_can_derive_one() -> None:
    rows = [
        _fact_row(
            "2024-03-31",
            "adj_ebit",
            10_000_000.0,
            period_type="ytd",
            occurrence="occurrence:q1-ytd",
        ),
        _fact_row(
            "2024-06-30",
            "adj_ebit",
            25_000_000.0,
            period_type="ytd",
            occurrence="occurrence:q2-ytd",
        ),
    ]
    selected = select_adjusted_metric_history(pd.DataFrame(rows))
    q2 = selected[
        (selected["quarter"] == pd.Timestamp("2024-06-30"))
        & (selected["metric_id"] == "adj_ebit")
    ].iloc[0]
    assert q2["period_type"] == "quarter"
    assert q2["source_role"] == AdjustedMetricSourceRole.DERIVED_EXACT.value
    assert q2["value"] == 15_000_000.0
    assert q2["derivation_rule"] == "derivation:adjusted-metric-ytd-minus-prior-ytd@1"
    assert tuple(q2["derivation_input_occurrence_ids"]) == (
        "occurrence:q1-ytd",
        "occurrence:q2-ytd",
    )


def test_incompatible_definition_cannot_be_spliced_into_ttm() -> None:
    rows = [
        _fact_row("2022-12-31", "adj_ebit", 49_267_000.0, definition="definition:reported@1"),
        _fact_row("2023-03-31", "adj_ebit", 68_028_000.0, definition="definition:recast@1"),
        _fact_row("2023-06-30", "adj_ebit", 69_313_000.0, definition="definition:recast@1"),
        _fact_row("2023-09-30", "adj_ebit", 84_044_000.0, definition="definition:recast@1"),
    ]
    result = build_adjusted_metric_history_selection(
        pd.DataFrame(rows),
        pd.to_datetime(["2022-12-31", "2023-03-31", "2023-06-30", "2023-09-30"]),
    )
    assert result.ttm_values["adj_ebit"][pd.Timestamp("2023-09-30")] is None


def test_ttm_requires_four_consecutive_compatible_quarters() -> None:
    rows = [
        _fact_row(period, "adj_ebit", value)
        for period, value in (
            ("2023-03-31", 68_028_000.0),
            ("2023-06-30", 69_313_000.0),
            ("2023-09-30", 84_044_000.0),
            ("2023-12-31", 86_334_000.0),
        )
    ]
    quarters = pd.to_datetime([row["quarter"] for row in rows])
    result = build_adjusted_metric_history_selection(pd.DataFrame(rows), quarters)
    assert result.ttm_values["adj_ebit"][pd.Timestamp("2023-12-31")] == 307_719_000.0

    missing = build_adjusted_metric_history_selection(pd.DataFrame(rows[:-1]), quarters)
    assert missing.ttm_values["adj_ebit"][pd.Timestamp("2023-12-31")] is None


def test_metric_selection_preserves_source_unit_and_applies_no_scale() -> None:
    selected = select_adjusted_metric_history(
        pd.DataFrame([_fact_row("2022-12-31", "adj_ebit", 49_267_000.0)])
    )
    row = selected.iloc[0]
    assert row["value"] == 49_267_000.0
    assert row["source_unit"] == "thousands"
    assert row["canonical_usd_millions"] == pytest.approx(49.267)


def test_registered_issuer_recast_is_direct_current_presentation_source() -> None:
    history = load_registered_issuer_recast_adjusted_metric_history(
        registered_ticker_dir("PBI") / "historical_segment"
    )
    expected = {
        pd.Timestamp("2023-03-31"): (68.028, 96.460),
        pd.Timestamp("2023-06-30"): (69.313, 97.313),
        pd.Timestamp("2023-09-30"): (84.044, 112.113),
        pd.Timestamp("2023-12-31"): (86.334, 114.558),
    }
    for period, (ebit_m, ebitda_m) in expected.items():
        row = history[history["quarter"] == period].iloc[0]
        assert row["adj_ebit"] == pytest.approx(ebit_m * 1_000_000.0)
        assert row["adj_ebitda"] == pytest.approx(ebitda_m * 1_000_000.0)
        assert row["adj_ebit_source_role"] == "direct"
        assert row["adj_ebitda_source_role"] == "direct"
        assert row["adj_ebit_scope"] == "continuing_operations_current_presentation"
        assert row["adj_ebit_source_occurrence_id"]
        assert row["adj_ebitda_source_occurrence_id"]


@pytest.mark.parametrize(
    ("filename", "period", "expected_ebit_m", "expected_ebitda_m"),
    [
        ("doc_000115752323000123_a53293633ex_991.htm", "2022-12-31", 49.267, 88.331),
        ("doc_000115752323000721_a53393339ex99_1.htm", "2023-03-31", 33.021, 72.918),
        ("doc_000115752323001240_a53504349_ex991.htm", "2023-06-30", 32.085, 71.958),
        ("doc_000115752323001616_a53738241_ex991.htm", "2023-09-30", 43.469, 83.731),
    ],
)
def test_registered_contemporaneous_release_series_is_source_direct_but_distinct_scope(
    filename: str,
    period: str,
    expected_ebit_m: float,
    expected_ebitda_m: float,
) -> None:
    path = registered_data_root() / "sec_cache" / "PBI" / filename
    assert path.is_file(), f"Required registered issuer release is unavailable: {path}"
    metadata: dict[str, dict[str, object]] = {}
    ebit, ebitda, _eps, _adjustments, status, _column = parse_adjusted_from_ex99(
        path.read_bytes(),
        pd.Timestamp(period),
        mode="strict",
        extraction_metadata=metadata,
    )
    assert status == "ok"
    assert ebit == pytest.approx(expected_ebit_m * 1_000_000.0)
    assert ebitda == pytest.approx(expected_ebitda_m * 1_000_000.0)
    assert metadata["adj_ebit"]["source_locator"]
    assert metadata["adj_ebitda"]["source_locator"]


def test_registered_sources_make_2023_q3_splice_invalid_and_q4_recast_ttm_exact() -> None:
    recast = load_registered_issuer_recast_adjusted_metric_history(
        registered_ticker_dir("PBI") / "historical_segment"
    )
    reported_q4 = pd.DataFrame(
        [
            _fact_row(
                "2022-12-31",
                "adj_ebit",
                49_267_000.0,
                definition="definition:issuer-reported-consolidated-adjusted-ebit@1",
                scope="reported_consolidated_at_period",
            ),
            _fact_row(
                "2022-12-31",
                "adj_ebitda",
                88_331_000.0,
                definition="definition:issuer-reported-consolidated-adjusted-ebitda@1",
                scope="reported_consolidated_at_period",
            ),
        ]
    )
    combined = pd.concat([reported_q4, recast], ignore_index=True, sort=False)
    quarters = pd.to_datetime(
        ["2022-12-31", "2023-03-31", "2023-06-30", "2023-09-30", "2023-12-31"]
    )
    result = build_adjusted_metric_history_selection(combined, quarters)

    assert result.ttm_values["adj_ebit"][pd.Timestamp("2023-09-30")] is None
    assert result.ttm_values["adj_ebitda"][pd.Timestamp("2023-09-30")] is None
    assert result.ttm_values["adj_ebit"][pd.Timestamp("2023-12-31")] == 307_719_000.0
    assert result.ttm_values["adj_ebitda"][pd.Timestamp("2023-12-31")] == 420_444_000.0


def test_non_gaap_credibility_consumes_metric_owner_instead_of_last_fcf_row() -> None:
    recast = load_registered_issuer_recast_adjusted_metric_history(
        registered_ticker_dir("PBI") / "historical_segment"
    )
    q1 = recast[recast["quarter"] == pd.Timestamp("2023-03-31")]
    fcf_only = _fact_row("2023-03-31", "adj_fcf", -39_714_000.0)
    combined = pd.concat([q1, pd.DataFrame([fcf_only])], ignore_index=True, sort=False)
    hist = pd.DataFrame(
        {
            "quarter": [pd.Timestamp("2023-03-31")],
            "op_income": [20_000_000.0],
            "revenue": [800_000_000.0],
        }
    )
    credibility = _build_non_gaap_cred(hist, combined, pd.DataFrame())
    assert len(credibility) == 1
    row = credibility.iloc[0]
    assert row["adj_ebit"] == 68_028_000.0
    assert row["adj_ebitda"] == 96_460_000.0
    assert row["source_kind"] == "issuer_recast_direct"
    assert row["units_detected"] == "millions"
    assert not bool(row["qa_fallback_source"])
