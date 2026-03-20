from __future__ import annotations

import datetime as dt
import io

import pandas as pd
import pytest
import pbi_xbrl.debt_parser as debt_parser_module

from pbi_xbrl.debt_parser import (
    _parse_header_dates_from_table,
    coerce_number,
    parse_debt_tranches_from_primary_doc,
    parse_scheduled_debt_repayments_from_primary_doc,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("150,000(a)", 150000.0),
        ("(25,000)", -25000.0),
        ("1,234*", 1234.0),
        ("--", None),
    ],
)
def test_coerce_number_strips_markers_and_parentheses(raw: str, expected: float | None) -> None:
    assert coerce_number(raw) == expected


def test_parse_header_dates_from_table_handles_multirow_as_of_headers() -> None:
    df = pd.DataFrame(
        [
            ["As of", "December 31, 2025", "December 31, 2024"],
            ["Senior notes due 2028", "150,000", "140,000"],
        ],
        columns=["Description", "Current", "Prior"],
    )

    assert _parse_header_dates_from_table(df) == {
        1: dt.date(2025, 12, 31),
        2: dt.date(2024, 12, 31),
    }


def test_read_html_tables_any_wraps_html_in_stringio(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, object] = {}

    def fake_read_html(arg, *args, **kwargs):
        seen["arg"] = arg
        return [pd.DataFrame([["demo"]], columns=["Debt instrument"])]

    monkeypatch.setattr(debt_parser_module.pd, "read_html", fake_read_html)

    out = debt_parser_module.read_html_tables_any(b"<table><tr><td>demo</td></tr></table>")

    assert len(out) == 1
    assert isinstance(seen["arg"], io.StringIO)


def test_parse_debt_tranches_from_primary_doc_parses_small_html_fixture() -> None:
    html = b"""
    <html>
      <body>
        <table>
          <tr><th>Debt instrument</th><th>December 31, 2025</th><th>December 31, 2024</th></tr>
          <tr><td>Senior notes due 2028</td><td>150,000</td><td>140,000</td></tr>
          <tr><td>Term loan due 2027</td><td>50,000</td><td>45,000</td></tr>
          <tr><td>Total debt</td><td>200,000</td><td>185,000</td></tr>
        </table>
      </body>
    </html>
    """

    rows, score, total_debt, total_ltd, total_label, scale, period_match = parse_debt_tranches_from_primary_doc(
        html,
        quarter_end=dt.date(2025, 12, 31),
    )

    assert score >= 2
    assert period_match is True
    assert total_debt == 200000.0
    assert total_ltd is None
    assert total_label == "Total debt"
    assert scale == 1.0
    assert [row["name"] for row in rows] == ["Senior notes due 2028", "Term loan due 2027"]
    assert [row["amount"] for row in rows] == [150000.0, 50000.0]
    assert all(row["parse_quality"] == "asof_matched" for row in rows)


def test_parse_scheduled_debt_repayments_from_primary_doc_parses_maturity_ladder() -> None:
    html = b"""
    <html>
      <body>
        <table>
          <tr><th>Scheduled debt repayments</th><th>Amount</th></tr>
          <tr><td>2026</td><td>50,000</td></tr>
          <tr><td>2027</td><td>75,000</td></tr>
          <tr><td>Thereafter</td><td>100,000</td></tr>
          <tr><td>Total</td><td>225,000</td></tr>
        </table>
      </body>
    </html>
    """

    rows = parse_scheduled_debt_repayments_from_primary_doc(
        html,
        quarter_end=dt.date(2025, 12, 31),
    )

    assert rows == [
        {
            "quarter": dt.date(2025, 12, 31),
            "maturity_year": 2026,
            "maturity_label": "2026",
            "amount_total": 50000.0,
            "source_kind": "scheduled_repayments_fallback",
            "row_text": "2026 50,000",
        },
        {
            "quarter": dt.date(2025, 12, 31),
            "maturity_year": 2027,
            "maturity_label": "2027",
            "amount_total": 75000.0,
            "source_kind": "scheduled_repayments_fallback",
            "row_text": "2027 75,000",
        },
        {
            "quarter": dt.date(2025, 12, 31),
            "maturity_year": None,
            "maturity_label": "Thereafter",
            "amount_total": 100000.0,
            "source_kind": "scheduled_repayments_fallback",
            "row_text": "Thereafter 100,000",
        },
    ]
