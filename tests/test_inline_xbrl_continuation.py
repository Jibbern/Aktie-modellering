from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd
import pytest
from bs4 import BeautifulSoup

from pbi_xbrl.inline_xbrl_text import (
    INLINE_XBRL_FACT_TEXT_CONTRACT_ID,
    InlineXbrlContinuationError,
    reconstruct_inline_xbrl_fact_text,
)
from pbi_xbrl.path_config import resolve_effective_data_root_from_ancestors
from pbi_xbrl.pipeline_orchestration import (
    FinancialStatementDocumentPeriodError,
    _document_period_date,
    _inline_xbrl_document_identity,
    resolve_financial_statement_document_period,
)


def _soup(body: str) -> BeautifulSoup:
    return BeautifulSoup(
        f'<html xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"><body>{body}</body></html>',
        "lxml",
    )


def _fact_text(body: str, *, fact_id: str = "fact"):
    soup = _soup(body)
    fact = soup.find(id=fact_id)
    assert fact is not None
    return reconstruct_inline_xbrl_fact_text(soup, fact)


def _document_html(*, date_body: str, form: str = "10-K", body: str = "") -> str:
    return f"""
    <html xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"><body>
      <ix:nonnumeric id="form" name="dei:DocumentType" contextref="doc">{form}</ix:nonnumeric>
      {date_body}
      <ix:nonnumeric id="fy" name="dei:DocumentFiscalYearFocus" contextref="doc">2020</ix:nonnumeric>
      <ix:nonnumeric id="fp" name="dei:DocumentFiscalPeriodFocus" contextref="doc">FY</ix:nonnumeric>
      {body}
    </body></html>
    """


def test_single_continuation_reconstructs_document_date() -> None:
    result = _fact_text(
        '<ix:nonnumeric id="fact" continuedat="date-tail">December 31</ix:nonnumeric>'
        '<ix:continuation id="date-tail">, 2019</ix:continuation>'
    )
    assert result.contract_id == INLINE_XBRL_FACT_TEXT_CONTRACT_ID
    assert result.text == "December 31, 2019"
    assert result.continuation_ids == ("date-tail",)


def test_multi_step_continuation_follows_explicit_chain() -> None:
    result = _fact_text(
        '<ix:nonnumeric id="fact" continuedat="part-1">Alpha</ix:nonnumeric>'
        '<ix:continuation id="part-1" continuedat="part-2"> beta</ix:continuation>'
        '<ix:continuation id="part-2"> gamma</ix:continuation>'
    )
    assert result.text == "Alpha beta gamma"
    assert result.continuation_ids == ("part-1", "part-2")


def test_missing_continuation_target_fails_closed() -> None:
    with pytest.raises(InlineXbrlContinuationError, match="matched 0 nodes") as exc_info:
        _fact_text('<ix:nonnumeric id="fact" continuedat="missing">Alpha</ix:nonnumeric>')
    assert exc_info.value.code == "target_cardinality"


def test_continuation_cycle_fails_closed() -> None:
    with pytest.raises(InlineXbrlContinuationError, match="contains a cycle") as exc_info:
        _fact_text(
            '<ix:nonnumeric id="fact" continuedat="part-1">Alpha</ix:nonnumeric>'
            '<ix:continuation id="part-1" continuedat="part-2"> beta</ix:continuation>'
            '<ix:continuation id="part-2" continuedat="part-1"> gamma</ix:continuation>'
        )
    assert exc_info.value.code == "cycle"


def test_duplicate_continuation_target_id_fails_closed() -> None:
    with pytest.raises(InlineXbrlContinuationError, match="matched 2 nodes") as exc_info:
        _fact_text(
            '<ix:nonnumeric id="fact" continuedat="duplicate">Alpha</ix:nonnumeric>'
            '<ix:continuation id="duplicate"> beta</ix:continuation>'
            '<ix:continuation id="duplicate"> gamma</ix:continuation>'
        )
    assert exc_info.value.code == "target_cardinality"


def test_source_dom_order_does_not_change_explicit_chain() -> None:
    first = _fact_text(
        '<ix:nonnumeric id="fact" continuedat="a">One</ix:nonnumeric>'
        '<ix:continuation id="a" continuedat="b"> two</ix:continuation>'
        '<ix:continuation id="b"> three</ix:continuation>'
    )
    second = _fact_text(
        '<ix:continuation id="b"> three</ix:continuation>'
        '<ix:continuation id="a" continuedat="b"> two</ix:continuation>'
        '<ix:nonnumeric id="fact" continuedat="a">One</ix:nonnumeric>'
    )
    assert first.text == second.text == "One two three"


def test_surrounding_dom_text_is_not_concatenated() -> None:
    result = _fact_text(
        '<p>Before</p>'
        '<ix:nonnumeric id="fact" continuedat="tail">Owned</ix:nonnumeric>'
        '<p>Neighbor</p>'
        '<ix:continuation id="tail"> source</ix:continuation>'
        '<p>After</p>'
    )
    assert result.text == "Owned source"


def test_whitespace_and_escaped_text_are_normalized_deterministically() -> None:
    result = _fact_text(
        '<ix:nonnumeric id="fact" continuedat="tail">  A&nbsp; &amp;\n B </ix:nonnumeric>'
        '<ix:continuation id="tail">  C\t D  </ix:continuation>'
    )
    assert result.text == "A & B C D"


def test_excessive_chain_fails_instead_of_truncating() -> None:
    soup = _soup(
        '<ix:nonnumeric id="fact" continuedat="one">A</ix:nonnumeric>'
        '<ix:continuation id="one" continuedat="two"> B</ix:continuation>'
        '<ix:continuation id="two"> C</ix:continuation>'
    )
    fact = soup.find(id="fact")
    assert fact is not None
    with pytest.raises(InlineXbrlContinuationError, match="exceeds 1 links") as exc_info:
        reconstruct_inline_xbrl_fact_text(soup, fact, max_hops=1)
    assert exc_info.value.code == "excessive_chain"


def test_reconstructed_date_is_complete_and_yearless_date_is_ineligible() -> None:
    assert _document_period_date("December 31, 2019") == dt.date(2019, 12, 31)
    assert _document_period_date("December 31") is None
    assert _document_period_date("2019") is None


def test_manifest_inline_and_filename_document_authorities_agree(tmp_path: Path) -> None:
    path = tmp_path / "GPRE_FY2020_10K_2020-12-31_financial_statement.htm"
    path.write_text(
        _document_html(
            date_body=(
                '<ix:nonnumeric id="date" name="dei:DocumentPeriodEndDate" '
                'contextref="doc" continuedat="date-tail">December 31</ix:nonnumeric>'
                '<ix:continuation id="date-tail">, 2020</ix:continuation>'
            )
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [{"ticker": "GPRE", "form": "10-K", "reportDate": "2020-12-31", "materialized_path": str(path)}]
    ).to_csv(tmp_path / "GPRE_financial_statement_manifest.csv", index=False)
    identity = resolve_financial_statement_document_period(path)
    assert identity.reporting_date == dt.date(2020, 12, 31)
    assert {candidate.source for candidate in identity.candidates} == {
        "registered_manifest",
        "inline_xbrl_dei",
        "registered_canonical_filename",
    }


def test_completed_continuation_date_conflict_still_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "GPRE_FY2020_10K_2020-12-31_financial_statement.htm"
    path.write_text(
        _document_html(
            date_body=(
                '<ix:nonnumeric id="date" name="dei:DocumentPeriodEndDate" '
                'contextref="doc" continuedat="date-tail">September 30</ix:nonnumeric>'
                '<ix:continuation id="date-tail">, 2020</ix:continuation>'
            )
        ),
        encoding="utf-8",
    )
    with pytest.raises(FinancialStatementDocumentPeriodError, match="conflicting document reporting periods"):
        resolve_financial_statement_document_period(path)


def test_real_gpre_fy2019_continuation_resolves_registered_identity() -> None:
    resolution = resolve_effective_data_root_from_ancestors(Path(__file__).resolve(), env={})
    assert resolution.data_root is not None, (*resolution.errors, *resolution.warnings)
    path = (
        resolution.data_root
        / "tickers"
        / "GPRE"
        / "financial_statement"
        / "GPRE_FY2019_10K_2019-12-31_financial_statement.htm"
    )
    inline = _inline_xbrl_document_identity(path)
    identity = resolve_financial_statement_document_period(path)
    assert inline == {
        "reporting_date": dt.date(2019, 12, 31),
        "form": "10-K",
        "raw_document_types": ["10-K"],
        "period_contexts": ["Duration_1_1_2019_To_12_31_2019"],
        "fiscal_years": ["2019"],
        "fiscal_periods": ["FY"],
    }
    assert identity.ticker == "GPRE"
    assert identity.form == "10-K"
    assert identity.reporting_date == dt.date(2019, 12, 31)
    assert len({candidate.reporting_date for candidate in identity.candidates}) == 1
