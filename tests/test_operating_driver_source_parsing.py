from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from openpyxl import Workbook
import pytest

from pbi_xbrl.longitudinal_memory.operating_driver_source_parsing import (
    OperatingDriverSourceParsingError,
    derive_additive_quarter_actuals,
    parse_html_table_terminal_number,
    parse_inline_xbrl_instant_facts,
    parse_quarterly_history_table,
    parse_retail_activity_snapshot,
)


ANF_DATA = Path(r"C:\Users\Jibbe\Aktier\StockModelData\tickers\ANF")
COMP_ALIASES = {
    "total": ("Comparable sales",),
    "abercrombie": ("Abercrombie comparable sales",),
    "hollister": ("Hollister comparable sales",),
    "americas": ("Americas comparable sales",),
    "emea": ("EMEA comparable sales",),
    "apac": ("APAC comparable sales",),
}


def _quarterly_fixture(path: Path, *, conflicting_duplicate: bool) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "History"
    sheet["A1"] = "Fiscal 2023"
    sheet["B2"] = "Q1"
    sheet["C2"] = "Q2"
    rows = (
        ("Comparable sales", 0.03, 0.04),
        ("Abercrombie comparable sales", 0.14, 0.15),
        ("Hollister comparable sales", -0.06, -0.02),
        ("Americas comparable sales", "Not provided", 0.05),
        ("EMEA comparable sales", "Not provided", -0.03),
        ("APAC comparable sales", "Not provided", 0.07),
        ("Comparable sales", 0.04 if conflicting_duplicate else 0.03, 0.04),
    )
    for row_number, row in enumerate(rows, start=3):
        for column_number, value in enumerate(row, start=1):
            sheet.cell(row_number, column_number, value)
    workbook.save(path)
    workbook.close()


def test_quarterly_parser_accepts_only_equivalent_duplicate_total_rows(tmp_path: Path) -> None:
    source = tmp_path / "history.xlsx"
    _quarterly_fixture(source, conflicting_duplicate=False)
    observations = parse_quarterly_history_table(
        source,
        sheet_name="History",
        metric_aliases=COMP_ALIASES,
    )
    index = {(item.metric_key, item.fiscal_quarter): item for item in observations}
    assert len(observations) == 12
    assert index[("total", 1)].value == Decimal("0.03")
    assert index[("americas", 1)].value is None
    assert index[("americas", 1)].source_state == "NOT_DISCLOSED"


def test_quarterly_parser_rejects_conflicting_duplicate_total_rows(tmp_path: Path) -> None:
    source = tmp_path / "history.xlsx"
    _quarterly_fixture(source, conflicting_duplicate=True)
    with pytest.raises(OperatingDriverSourceParsingError, match="Conflicting duplicate rows"):
        parse_quarterly_history_table(
            source,
            sheet_name="History",
            metric_aliases=COMP_ALIASES,
        )


def test_inline_xbrl_parser_recovers_dimensionless_inventory_in_source_units() -> None:
    source = (
        ANF_DATA
        / "financial_statement"
        / "ANF_Q2_2023_10Q_2023-04-29_financial_statement.htm"
    )
    facts = parse_inline_xbrl_instant_facts(
        source,
        concept_names=("us-gaap:InventoryNet",),
    )
    index = {(item.concept_name, item.instant_date): item for item in facts}
    assert index[("us-gaap:InventoryNet", "2023-04-29")].value == Decimal("447806000")
    assert index[("us-gaap:InventoryNet", "2023-04-29")].unit_ref == "usd"


def test_html_table_parser_uses_declared_store_section_not_square_footage() -> None:
    source = (
        ANF_DATA
        / "financial_statement"
        / "ANF_Q2_2023_10Q_2023-04-29_financial_statement.htm"
    )
    value = parse_html_table_terminal_number(
        source,
        required_table_text="Total Number of stores:",
        row_label="April 29, 2023",
        section_label="Number of stores:",
    )
    assert value == Decimal("758")


def test_retail_activity_parser_keeps_combined_actual_components_separate() -> None:
    snapshot = parse_retail_activity_snapshot(
        "During Fiscal 2024, the Company opened 65 new store locations, "
        "remodeled 48 store locations, right-sized an additional 12 store "
        "locations and closed 41 stores."
    )
    assert snapshot.new_stores == 65
    assert snapshot.remodeled_stores == 48
    assert snapshot.right_sized_stores == 12
    assert snapshot.closed_stores == 41


def test_additive_quarter_derivation_differences_only_adjacent_actuals() -> None:
    results = derive_additive_quarter_actuals(
        fiscal_year=2024,
        cumulative_actuals={2: 23, 3: 30, 4: 48},
    )
    assert [(item.fiscal_quarter, item.value) for item in results] == [
        (3, Decimal("7")),
        (4, Decimal("18")),
    ]


def test_additive_quarter_derivation_never_bridges_a_missing_predecessor() -> None:
    results = derive_additive_quarter_actuals(
        fiscal_year=2024,
        cumulative_actuals={1: 1, 3: 30},
    )
    assert [(item.fiscal_quarter, item.value) for item in results] == [
        (1, Decimal("1")),
    ]
