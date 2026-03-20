from __future__ import annotations

import datetime as dt

import pandas as pd

from pbi_xbrl.non_gaap import (
    _find_header_dates,
    find_ex99_docs,
    infer_quarter_end_from_text,
    normalize_number_spacing,
    parse_adjusted_from_plain_text,
)


def test_infer_quarter_end_from_text_handles_common_headlines() -> None:
    assert infer_quarter_end_from_text("Green Plains Reports Second Quarter 2025 Results") == dt.date(2025, 6, 30)
    assert infer_quarter_end_from_text("Q3 2024 earnings release") == dt.date(2024, 9, 30)


def test_normalize_number_spacing_repairs_ocr_like_spacing() -> None:
    assert normalize_number_spacing("Adjusted EBITDA was 1 ,234 this quarter.") == "Adjusted EBITDA was 1,234 this quarter."
    assert normalize_number_spacing("Revenue reached 2 345,678 in the period.") == "Revenue reached 2345,678 in the period."


def test_find_header_dates_resolves_three_month_multirow_headers() -> None:
    df = pd.DataFrame(
        [
            ["Three Months Ended", "", ""],
            ["Metric", "June 30, 2025", "June 30, 2024"],
            ["Adjusted EBITDA", "12", "10"],
        ]
    )

    cols, header_row_idx, col_dates, table_hint = _find_header_dates(df)

    assert cols == ["Metric", "June 30, 2025", "June 30, 2024"]
    assert header_row_idx == 1
    assert col_dates == {
        1: dt.date(2025, 6, 30),
        2: dt.date(2024, 6, 30),
    }
    assert table_hint == "3M"


def test_parse_adjusted_from_plain_text_preserves_scale_and_shape() -> None:
    txt = """
    Green Plains Inc.
    Three Months Ended June 30, 2025
    Reconciliation of reported net income to adjusted EBITDA (in millions, except per share)
    Adjusted EBIT 10
    Adjusted EBITDA 12
    Adjusted diluted EPS 0.42
    """

    adj_ebit, adj_ebitda, adj_eps, adjustments, status, source = parse_adjusted_from_plain_text(
        txt,
        quarter_end=pd.Timestamp("2025-06-30"),
        mode="relaxed",
    )

    assert adj_ebit == 10_000_000.0
    assert adj_ebitda == 12_000_000.0
    assert adj_eps == 0.42
    assert adjustments == {}
    assert status == "ok_relaxed_ocr"
    assert source == "ocr"


def test_parse_adjusted_from_plain_text_does_not_treat_adjusted_earnings_before_interest_as_ebitda() -> None:
    txt = """
    Pitney Bowes Inc.
    Three Months Ended March 31, 2025
    Reconciliation of reported net income to adjusted results (in millions)
    Adjusted earnings before interest and taxes (Adjusted EBIT) 35
    Adjusted diluted EPS 0.19
    """

    adj_ebit, adj_ebitda, adj_eps, adjustments, status, source = parse_adjusted_from_plain_text(
        txt,
        quarter_end=pd.Timestamp("2025-03-31"),
        mode="relaxed",
    )

    assert adj_ebit == 35_000_000.0
    assert adj_ebitda is None
    assert adj_eps == 0.19
    assert adjustments == {}
    assert status == "ok_relaxed_ocr"
    assert source == "ocr"


def test_parse_adjusted_from_plain_text_does_not_fill_adj_ebit_from_adjusted_ebitda_only_line() -> None:
    txt = """
    Green Plains Inc.
    Three Months Ended December 31, 2025
    Reconciliation of reported net income to adjusted EBITDA (in millions)
    Adjusted EBITDA 49.1
    Adjusted diluted EPS 0.42
    """

    adj_ebit, adj_ebitda, adj_eps, adjustments, status, source = parse_adjusted_from_plain_text(
        txt,
        quarter_end=pd.Timestamp("2025-12-31"),
        mode="relaxed",
    )

    assert adj_ebit is None
    assert adj_ebitda == 49_100_000.0
    assert adj_eps == 0.42
    assert adjustments == {}
    assert status == "ok_relaxed_ocr"
    assert source == "ocr"


def test_find_ex99_docs_detects_earnings_release_and_ceo_letter_filenames_without_ex99_token() -> None:
    index_json = {
        "directory": {
            "item": [
                {"name": "q32025earningspressrelea.htm"},
                {"name": "q32025earningsceoletter.htm"},
                {"name": "plain8k.htm"},
            ]
        }
    }

    docs = find_ex99_docs(index_json)

    assert "q32025earningspressrelea.htm" in docs
    assert "q32025earningsceoletter.htm" in docs
    assert "plain8k.htm" not in docs
