from __future__ import annotations

import datetime as dt
import math
import re
from pathlib import Path

import pandas as pd
import pytest

from pbi_xbrl.debt_detail_lineage import (
    DEBT_DETAIL_LINEAGE_DISPOSITION_COLUMN,
    DebtDetailLineageContractError,
    DebtDetailLineageDisposition,
    normalize_debt_detail_lineage_dispositions,
    require_debt_detail_lineage_disposition,
)
from pbi_xbrl.excel_writer import WorkbookInputs
from pbi_xbrl.excel_writer_context import build_writer_context
from pbi_xbrl.excel_writer_core import ensure_valuation_inputs
from pbi_xbrl.excel_writer_valuation_debt_support import (
    ValuationDebtSupportDeps,
    source_backed_debt_tranches_from_slides,
)
from pbi_xbrl.path_config import resolve_effective_data_root_from_ancestors
from pbi_xbrl.pipeline_orchestration import (
    DEBT_FACT_PERIOD_OWNERSHIP_CONTRACT_ID,
    DEBT_TABLE_PERIOD_OWNERSHIP_VERSION,
    FINANCIAL_STATEMENT_DOCUMENT_PERIOD_CONTRACT_ID,
    FINANCIAL_STATEMENT_DOCUMENT_PERIOD_VERSION,
    DebtTableCellState,
    DebtTablePeriodSelectionError,
    FinancialStatementDocumentPeriodError,
    _parse_financial_statement_debt_table_html,
    is_owned_financial_statement_document,
    resolve_financial_statement_document_period,
)
from pbi_xbrl.pipeline_types import PipelineArtifacts
from pbi_xbrl.post_quarter_capital_events import apply_pbi_current_debt_overlay
from pbi_xbrl.workbook_gap_audit import load_pipeline_bundle_map


def _registered_data_root() -> Path:
    resolution = resolve_effective_data_root_from_ancestors(Path(__file__).resolve(), env={})
    assert resolution.data_root is not None, (*resolution.errors, *resolution.warnings)
    return resolution.data_root


def _pbi_q2_debt_source() -> Path:
    return (
        _registered_data_root()
        / "tickers"
        / "PBI"
        / "financial_statement"
        / "PBI_Q2_2026_10Q_2026-06-30_financial_statement.htm"
    )


def _pbi_fy2019_debt_source() -> Path:
    return (
        _registered_data_root()
        / "tickers"
        / "PBI"
        / "financial_statement"
        / "PBI_FY2019_10K_2019-12-31_financial_statement.htm"
    )


def _pbi_fy2020_debt_source() -> Path:
    return (
        _registered_data_root()
        / "tickers"
        / "PBI"
        / "financial_statement"
        / "PBI_FY2020_10K_2020-12-31_financial_statement.htm"
    )


def _document_identity_html(
    *,
    document_date: str,
    form: str,
    fiscal_year: str,
    fiscal_period: str,
    body: str = "",
) -> str:
    return f"""
    <html xmlns:ix="http://www.xbrl.org/2013/inlineXBRL">
      <body>
        <ix:nonnumeric name="dei:DocumentType" contextref="c-document">{form}</ix:nonnumeric>
        <ix:nonnumeric name="dei:DocumentPeriodEndDate" contextref="c-document">{document_date}</ix:nonnumeric>
        <ix:nonnumeric name="dei:DocumentFiscalYearFocus" contextref="c-document">{fiscal_year}</ix:nonnumeric>
        <ix:nonnumeric name="dei:DocumentFiscalPeriodFocus" contextref="c-document">{fiscal_period}</ix:nonnumeric>
        {body}
      </body>
    </html>
    """


def _inline_debt_html(
    *,
    current_text: str | None,
    prior_text: str | None,
    current_format: str = "ixt:num-dot-decimal",
    current_date: str = "2026-06-30",
    prior_date: str = "2025-12-31",
    reorder_columns: bool = False,
    duplicate_current_header: bool = False,
    omit_visual_dates: bool = False,
    reverse_fact_source_order: bool = False,
    duplicate_current_fact: bool = False,
    duplicate_current_value: str | None = None,
) -> str:
    def fact(fact_id: str, context: str, text: str | None, format_id: str) -> str:
        if text is None:
            return ""
        return (
            f'<ix:nonfraction id="{fact_id}" '
            'name="us-gaap:DebtInstrumentCarryingAmount" '
            f'contextref="{context}" unitref="usd" scale="3" format="{format_id}">'
            f"{text}</ix:nonfraction>"
        )

    current_cell = fact("f-current", "c-current", current_text, current_format)
    prior_cell = fact("f-prior", "c-prior", prior_text, "ixt:num-dot-decimal")
    if duplicate_current_fact:
        duplicate_text = current_text if duplicate_current_value is None else duplicate_current_value
        current_cell += fact("f-current-duplicate", "c-current", duplicate_text, current_format)
    headers = [
        ("Current balance" if omit_visual_dates else "June 30, 2026", current_cell),
        ("Comparative balance" if omit_visual_dates else "December 31, 2025", prior_cell),
    ]
    if reorder_columns:
        headers.reverse()
    if duplicate_current_header:
        headers.append(("June 30, 2026", current_cell))
    header_cells = "".join(f"<th>{header}</th>" for header, _ in headers)
    value_cells = "".join(f"<td>{value}</td>" for _, value in headers)
    contexts = [
        f'<xbrli:context id="c-current"><xbrli:period><xbrli:instant>{current_date}</xbrli:instant></xbrli:period>'
        '<xbrli:scenario><xbrldi:explicitmember dimension="pbi:DebtInstrumentAxis">pbi:DebtDueMarch2027Member</xbrldi:explicitmember></xbrli:scenario>'
        '</xbrli:context>',
        f'<xbrli:context id="c-prior"><xbrli:period><xbrli:instant>{prior_date}</xbrli:instant></xbrli:period>'
        '<xbrli:scenario><xbrldi:explicitmember dimension="pbi:DebtInstrumentAxis">pbi:DebtDueMarch2027Member</xbrldi:explicitmember></xbrli:scenario>'
        '</xbrli:context>',
    ]
    if reverse_fact_source_order:
        contexts.reverse()
    context_markup = "".join(contexts)
    return f"""
    <html xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"
          xmlns:xbrli="http://www.xbrl.org/2003/instance"
          xmlns:xbrldi="http://xbrl.org/2006/xbrldi">
      <body>
        {context_markup}
        <div>10. Debt</div>
        <table>
          <tr><th>Instrument</th>{header_cells}</tr>
          <tr><td>Notes due March 2027</td>{value_cells}</tr>
        </table>
        <div>23</div><hr style="page-break-after:always"/>
      </body>
    </html>
    """


def _parse_synthetic(tmp_path: Path, html: str, requested: dt.date) -> dict[str, object]:
    path = tmp_path / "debt.htm"
    path.write_text(html, encoding="utf-8")
    rows = _parse_financial_statement_debt_table_html(path, requested)
    assert len(rows) == 1
    return rows[0]


@pytest.mark.parametrize(
    ("current", "prior", "current_format", "expected", "state"),
    [
        ("125", "346.7", "ixt:num-dot-decimal", 125_000.0, "reported_numeric"),
        ("0", "346.7", "ixt:num-dot-decimal", 0.0, "explicit_zero"),
        ("—", "346.7", "ixt:fixed-zero", 0.0, "explicit_zero"),
        (None, "346.7", "ixt:num-dot-decimal", None, "missing"),
    ],
)
def test_debt_table_current_period_owns_selected_cell_before_value_filtering(
    tmp_path: Path,
    current: str | None,
    prior: str,
    current_format: str,
    expected: float | None,
    state: str,
) -> None:
    row = _parse_synthetic(
        tmp_path,
        _inline_debt_html(
            current_text=current,
            prior_text=prior,
            current_format=current_format,
        ),
        dt.date(2026, 6, 30),
    )
    assert row["amount"] == expected
    assert row["selected_cell_state"] == state
    assert row["reporting_date"] == "2026-06-30"
    assert row["comparative_amount"] == pytest.approx(346_700.0)


def test_debt_table_prior_request_selects_only_prior_context(tmp_path: Path) -> None:
    row = _parse_synthetic(
        tmp_path,
        _inline_debt_html(current_text="0", prior_text="346.7"),
        dt.date(2025, 12, 31),
    )
    assert row["amount"] == pytest.approx(346_700.0)
    assert row["reporting_date"] == "2025-12-31"
    assert row["source_fact_id"] == "f-prior"


def test_debt_table_source_column_reorder_does_not_change_period_result(tmp_path: Path) -> None:
    first = _parse_synthetic(
        tmp_path,
        _inline_debt_html(current_text="0", prior_text="346.7"),
        dt.date(2026, 6, 30),
    )
    second = _parse_synthetic(
        tmp_path,
        _inline_debt_html(
            current_text="0",
            prior_text="346.7",
            reorder_columns=True,
        ),
        dt.date(2026, 6, 30),
    )
    assert first["amount"] == second["amount"] == 0.0
    assert first["source_fact_id"] == second["source_fact_id"] == "f-current"


def test_debt_table_ambiguous_or_mismatched_period_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "debt.htm"
    path.write_text(
        _inline_debt_html(
            current_text="0",
            prior_text="346.7",
            duplicate_current_header=True,
        ),
        encoding="utf-8",
    )
    with pytest.raises(DebtTablePeriodSelectionError, match="ambiguous duplicate visual period"):
        _parse_financial_statement_debt_table_html(path, dt.date(2026, 6, 30))
    path.write_text(
        _inline_debt_html(current_text="0", prior_text="346.7"),
        encoding="utf-8",
    )
    with pytest.raises(DebtTablePeriodSelectionError, match="no visual or inline-XBRL context owner"):
        _parse_financial_statement_debt_table_html(path, dt.date(2026, 9, 30))


def test_debt_table_visual_date_and_matching_context_are_jointly_owned(tmp_path: Path) -> None:
    row = _parse_synthetic(
        tmp_path,
        _inline_debt_html(current_text="125", prior_text="100"),
        dt.date(2026, 6, 30),
    )
    assert row["period_ownership_contract_id"] == DEBT_FACT_PERIOD_OWNERSHIP_CONTRACT_ID
    assert row["period_ownership_basis"] == "visual_xbrl_agree"
    assert row["visual_reporting_date"] == "2026-06-30"
    assert row["context_reporting_date"] == "2026-06-30"


def test_debt_table_absent_visual_date_uses_fact_context_period(tmp_path: Path) -> None:
    row = _parse_synthetic(
        tmp_path,
        _inline_debt_html(
            current_text="125",
            prior_text="100",
            omit_visual_dates=True,
        ),
        dt.date(2026, 6, 30),
    )
    assert row["amount"] == 125_000.0
    assert row["source_fact_id"] == "f-current"
    assert row["source_column_index"] is None
    assert row["period_ownership_basis"] == "inline_xbrl_context_fallback"
    assert row["visual_reporting_date"] is None
    assert row["context_reporting_date"] == "2026-06-30"


def test_debt_table_visual_and_context_period_conflict_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "debt.htm"
    path.write_text(
        _inline_debt_html(
            current_text="125",
            prior_text="100",
            current_date="2026-03-31",
        ),
        encoding="utf-8",
    )
    with pytest.raises(DebtTablePeriodSelectionError, match="visual period 2026-06-30"):
        _parse_financial_statement_debt_table_html(path, dt.date(2026, 3, 31))


def test_debt_table_missing_context_and_visual_date_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "debt.htm"
    html = _inline_debt_html(
        current_text="125",
        prior_text="100",
        omit_visual_dates=True,
    ).replace('contextref="c-current"', 'contextref="missing-context"', 1)
    path.write_text(html, encoding="utf-8")
    with pytest.raises(DebtTablePeriodSelectionError, match="no registered XBRL context"):
        _parse_financial_statement_debt_table_html(path, dt.date(2026, 6, 30))


def test_debt_table_equivalent_duplicate_fact_resolves_deterministically(tmp_path: Path) -> None:
    row = _parse_synthetic(
        tmp_path,
        _inline_debt_html(
            current_text="125",
            prior_text="100",
            duplicate_current_fact=True,
        ),
        dt.date(2026, 6, 30),
    )
    assert row["source_fact_id"] == "f-current"
    assert row["related_fact_ids"] == ("f-current-duplicate",)


def test_debt_table_incompatible_duplicate_fact_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "debt.htm"
    path.write_text(
        _inline_debt_html(
            current_text="125",
            prior_text="100",
            duplicate_current_fact=True,
            duplicate_current_value="126",
        ),
        encoding="utf-8",
    )
    with pytest.raises(DebtTablePeriodSelectionError, match="ambiguous duplicate facts"):
        _parse_financial_statement_debt_table_html(path, dt.date(2026, 6, 30))


def test_debt_table_context_source_order_does_not_change_selection(tmp_path: Path) -> None:
    first = _parse_synthetic(
        tmp_path,
        _inline_debt_html(
            current_text="125",
            prior_text="100",
            omit_visual_dates=True,
        ),
        dt.date(2026, 6, 30),
    )
    second = _parse_synthetic(
        tmp_path,
        _inline_debt_html(
            current_text="125",
            prior_text="100",
            omit_visual_dates=True,
            reverse_fact_source_order=True,
        ),
        dt.date(2026, 6, 30),
    )
    assert first["amount"] == second["amount"] == 125_000.0
    assert first["source_fact_id"] == second["source_fact_id"] == "f-current"


def test_live_pbi_fy2019_comparative_debt_facts_use_distinct_context_dates() -> None:
    current = _parse_financial_statement_debt_table_html(
        _pbi_fy2019_debt_source(),
        dt.date(2019, 12, 31),
    )
    prior = _parse_financial_statement_debt_table_html(
        _pbi_fy2019_debt_source(),
        dt.date(2018, 12, 31),
    )
    assert len(current) == len(prior) == 7
    current_by_name = {row["issuer_instrument_label"]: row for row in current}
    prior_by_name = {row["issuer_instrument_label"]: row for row in prior}
    for name in sorted(current_by_name):
        current_row = current_by_name[name]
        prior_row = prior_by_name[name]
        assert current_row["reporting_date"] == current_row["context_reporting_date"] == "2019-12-31"
        assert prior_row["reporting_date"] == prior_row["context_reporting_date"] == "2018-12-31"
        assert current_row["source_context_id"].startswith("FI2019Q4")
        assert prior_row["source_context_id"].startswith("FI2018Q4")
        assert current_row["period_ownership_basis"] == "inline_xbrl_context_fallback"
        assert prior_row["period_ownership_basis"] == "inline_xbrl_context_fallback"
    september_current = current_by_name["Notes due September 2020"]
    september_prior = prior_by_name["Notes due September 2020"]
    assert september_current["amount"] is None
    assert september_current["selected_cell_state"] == DebtTableCellState.NOT_APPLICABLE.value
    assert september_prior["amount"] == 300_000_000.0


def test_debt_period_ownership_version_is_explicit() -> None:
    assert DEBT_TABLE_PERIOD_OWNERSHIP_VERSION == "v1_visual_xbrl_context"


def test_financial_statement_document_period_version_is_explicit() -> None:
    assert FINANCIAL_STATEMENT_DOCUMENT_PERIOD_VERSION == "v2_registered_document_identity"


def test_live_pbi_debt_table_period_and_convertible_identity_are_source_owned() -> None:
    rows = _parse_financial_statement_debt_table_html(
        _pbi_q2_debt_source(),
        dt.date(2026, 6, 30),
    )
    by_name = {row["issuer_instrument_label"]: row for row in rows}
    march = by_name["Notes due March 2027"]
    assert march["amount"] == 0.0
    assert march["comparative_amount"] == 346_700_000.0
    assert march["selected_cell_state"] == DebtTableCellState.EXPLICIT_ZERO.value
    assert march["source_fact_id"] == "f-1245"
    assert march["source_context_id"] == "c-305"
    convertible = by_name["Convertible Notes due August 2030"]
    assert convertible["amount"] == 230_000_000.0
    assert convertible["rate_value"] == 0.015
    assert convertible["rate_display"] == "1.50%"
    assert convertible["page"] == 23
    assert convertible["source_fact_id"] == "f-1251"
    assert convertible["rate_fact_id"] == "f-1250"
    assert convertible["source_context_id"] == "c-309"
    assert convertible["source_member"] == "pbi:ConvertibleSeniorNotesDueAugust2030Member"
    assert "printed-page:23" in convertible["source_locator"]
    assert "page:8" not in convertible["source_locator"]
    assert not convertible["issuer_instrument_label"].startswith("1.50%")


def _live_source_backed_debt_rows() -> pd.DataFrame:
    root = _registered_data_root()
    bundle = load_pipeline_bundle_map(root, "PBI")
    return source_backed_debt_tranches_from_slides(
        ValuationDebtSupportDeps(runtime={"pd": pd, "re": re}),
        bundle["slides_debt"],
        "2026-06-30",
        "PBI",
    )


def test_stale_registered_bundle_is_reconciled_before_projection() -> None:
    root = _registered_data_root()
    bundle = load_pipeline_bundle_map(root, "PBI")
    stale = bundle["slides_debt"]
    stale_march = stale[
        stale["quarter"].astype(str).str.startswith("2026-06-30")
        & stale["tranche"].astype(str).eq("Notes due March 2027")
    ].iloc[0]
    assert stale_march["amount"] == 346_700_000.0
    resolved = _live_source_backed_debt_rows()
    march = resolved[resolved["tranche_name"].eq("Notes due March 2027")].iloc[0]
    assert march["amount_principal"] == 0.0
    assert march["source_fact_id"] == "f-1245"
    assert march[DEBT_DETAIL_LINEAGE_DISPOSITION_COLUMN] is DebtDetailLineageDisposition.VALID


def test_convertible_projection_preserves_separate_rate_and_stable_lineage() -> None:
    row = _live_source_backed_debt_rows().loc[
        lambda frame: frame["tranche_name"].eq("Convertible Notes due August 2030")
    ].iloc[0]
    assert row["amount_principal"] == 230_000_000.0
    assert row["coupon_pct"] == 0.015
    assert row["issuer_instrument_label"] == "Convertible Notes due August 2030"
    assert row["source_page"] == 23
    assert row["source_fact_id"] == "f-1251"
    assert row["rate_fact_id"] == "f-1250"
    assert row["source_context_id"] == "c-309"
    assert row["source_member"] == "pbi:ConvertibleSeniorNotesDueAugust2030Member"
    assert str(row["source_document_id"]).startswith("debt-source-document:v1|")
    assert str(row["source_occurrence_id"]).startswith("debt-source-occurrence:v1|")
    assert str(row["economic_id"]).startswith("debt-instrument:v1|")
    assert "printed-page%3A23" in row["source_occurrence_id"]
    assert not re.search(r"(?:cell=)?\$?[A-Z]{1,3}\$?[1-9][0-9]*", row["economic_id"])


def test_post_quarter_overlay_normalizes_exact_failing_row_to_not_applicable() -> None:
    event = {
        "ticker": "PBI",
        "event_type": "refinancing_redemption",
        "principal_redeemed": 346_700_000.0,
        "term_loan_total": 302_000_000.0,
    }
    current = apply_pbi_current_debt_overlay(_live_source_backed_debt_rows(), event)
    term = current[current["tranche_name"].eq("Term Loan A")]
    assert list(term.index) == [6]
    row = term.iloc[0]
    assert row["source_kind"] == "PostQuarter_Capital_Events"
    assert row["source_basis"] == "current_principal_overlay"
    assert row[DEBT_DETAIL_LINEAGE_DISPOSITION_COLUMN] is DebtDetailLineageDisposition.NOT_APPLICABLE
    assert pd.isna(row.get("source_document_id"))


@pytest.mark.parametrize("invalid", [pd.NA, None, math.nan, "True", "False", 1, 0, object()])
def test_renderer_contract_rejects_nullable_or_untyped_lineage_state(invalid: object) -> None:
    with pytest.raises(DebtDetailLineageContractError, match="expected one of"):
        require_debt_detail_lineage_disposition(
            {
                "tranche_name": "Term Loan A",
                DEBT_DETAIL_LINEAGE_DISPOSITION_COLUMN: invalid,
            }
        )


def test_mixed_frame_normalization_has_no_nullable_dispositions() -> None:
    valid = _live_source_backed_debt_rows().iloc[[0]].copy()
    legacy = pd.DataFrame(
        [{"tranche_name": "Independent legacy debt", "source_kind": "Debt_Tranches_Latest"}]
    )
    mixed = normalize_debt_detail_lineage_dispositions(
        pd.concat([valid, legacy], ignore_index=True, sort=False)
    )
    assert list(mixed[DEBT_DETAIL_LINEAGE_DISPOSITION_COLUMN]) == [
        DebtDetailLineageDisposition.VALID,
        DebtDetailLineageDisposition.NOT_APPLICABLE,
    ]
    assert mixed[DEBT_DETAIL_LINEAGE_DISPOSITION_COLUMN].notna().all()


@pytest.mark.parametrize(
    "missing_field",
    ["source_document_id", "source_occurrence_id", "source_locator"],
)
def test_incomplete_source_backed_lineage_becomes_invalid(missing_field: str) -> None:
    valid = _live_source_backed_debt_rows().iloc[[0]].copy()
    valid[missing_field] = None
    normalized = normalize_debt_detail_lineage_dispositions(valid)
    assert normalized.iloc[0][DEBT_DETAIL_LINEAGE_DISPOSITION_COLUMN] is DebtDetailLineageDisposition.INVALID


def test_invalid_source_overlay_cannot_relabel_independent_row() -> None:
    independent = pd.DataFrame(
        [{"tranche_name": "Independent debt", "source_kind": "Debt_Tranches_Latest"}]
    )
    invalid = _live_source_backed_debt_rows().iloc[[0]].copy()
    invalid["source_document_id"] = None
    mixed = normalize_debt_detail_lineage_dispositions(
        pd.concat([independent, invalid], ignore_index=True, sort=False)
    )
    assert mixed.iloc[0][DEBT_DETAIL_LINEAGE_DISPOSITION_COLUMN] is DebtDetailLineageDisposition.NOT_APPLICABLE
    assert mixed.iloc[1][DEBT_DETAIL_LINEAGE_DISPOSITION_COLUMN] is DebtDetailLineageDisposition.INVALID


def test_source_row_order_does_not_change_projection_identity() -> None:
    root = _registered_data_root()
    bundle = load_pipeline_bundle_map(root, "PBI")
    source = bundle["slides_debt"]
    forward = source_backed_debt_tranches_from_slides(
        ValuationDebtSupportDeps(runtime={"pd": pd, "re": re}),
        source,
        "2026-06-30",
        "PBI",
    )
    reverse = source_backed_debt_tranches_from_slides(
        ValuationDebtSupportDeps(runtime={"pd": pd, "re": re}),
        source.iloc[::-1].reset_index(drop=True),
        "2026-06-30",
        "PBI",
    )
    columns = ["economic_id", "source_occurrence_id", "amount_principal"]
    pd.testing.assert_frame_equal(
        forward.sort_values("economic_id")[columns].reset_index(drop=True),
        reverse.sort_values("economic_id")[columns].reset_index(drop=True),
    )


def test_live_pbi_writer_renders_verified_lineage_without_nullable_state(tmp_path: Path) -> None:
    data_root = _registered_data_root()
    artifacts = PipelineArtifacts(**load_pipeline_bundle_map(data_root, "PBI"))
    inputs = WorkbookInputs.from_artifacts(
        artifacts,
        out_path=tmp_path / "pbi-live-lineage.xlsx",
        ticker="PBI",
        cache_dir=data_root / "sec_cache" / "PBI",
    )
    context = build_writer_context(inputs)
    ensure_valuation_inputs(context)
    context.callbacks.write_valuation_sheet()
    assert not inputs.out_path.exists()
    ws = context.wb["Valuation"]
    debt_row = next(
        row
        for row in range(1, ws.max_row + 1)
        if ws.cell(row=row, column=1).value == "Convertible Notes due August 2030"
    )
    assert ws.cell(debt_row, 2).value == 230.0
    assert ws.cell(debt_row, 4).value == 0.015
    comment = ws.cell(debt_row, 17).comment
    assert comment is not None
    text = str(comment.text or "")
    assert "printed-page:23" in text
    assert "principal-fact:f-1251" in text
    assert "rate-fact:f-1250" in text
    assert "page:8" not in text
    term_row = next(
        row
        for row in range(1, ws.max_row + 1)
        if ws.cell(row=row, column=1).value == "Term Loan A"
    )
    assert ws.cell(term_row, 17).comment is None


def test_fy2020_document_identity_ignores_internal_q1_phrase() -> None:
    identity = resolve_financial_statement_document_period(_pbi_fy2020_debt_source())
    assert identity.contract_id == FINANCIAL_STATEMENT_DOCUMENT_PERIOD_CONTRACT_ID
    assert identity.version == FINANCIAL_STATEMENT_DOCUMENT_PERIOD_VERSION
    assert identity.reporting_date == dt.date(2020, 12, 31)
    assert identity.form == "10-K"
    assert identity.ticker == "PBI"
    assert {candidate.source for candidate in identity.candidates} == {
        "inline_xbrl_dei",
        "registered_canonical_filename",
    }


def test_fy2019_registered_annual_document_identity_remains_year_end() -> None:
    identity = resolve_financial_statement_document_period(_pbi_fy2019_debt_source())
    assert identity.reporting_date == dt.date(2019, 12, 31)
    assert identity.form == "10-K"


def test_registered_10q_document_uses_quarter_end_despite_internal_periods(tmp_path: Path) -> None:
    path = tmp_path / "PBI_Q2_2026_10Q_2026-06-30_financial_statement.htm"
    path.write_text(
        _document_identity_html(
            document_date="June 30, 2026",
            form="10-Q",
            fiscal_year="2026",
            fiscal_period="Q2",
            body=(
                "<p>Three Months Ended March 31, 2026</p>"
                "<p>Comparative balance at December 31, 2025</p>"
            ),
        ),
        encoding="utf-8",
    )
    identity = resolve_financial_statement_document_period(path)
    assert identity.reporting_date == dt.date(2026, 6, 30)
    assert identity.form == "10-Q"


def test_document_identity_is_independent_of_incidental_source_order(tmp_path: Path) -> None:
    filename = "PBI_FY2020_10K_2020-12-31_financial_statement.htm"
    bodies = (
        "<p>Three Months Ended March 31, 2020</p><p>December 31, 2019 comparative</p>",
        "<p>December 31, 2019 comparative</p><p>Three Months Ended March 31, 2020</p>",
    )
    resolved = []
    for index, body in enumerate(bodies):
        folder = tmp_path / str(index)
        folder.mkdir()
        path = folder / filename
        path.write_text(
            _document_identity_html(
                document_date="December 31, 2020",
                form="10-K",
                fiscal_year="2020",
                fiscal_period="FY",
                body=body,
            ),
            encoding="utf-8",
        )
        resolved.append(resolve_financial_statement_document_period(path).reporting_date)
    assert resolved == [dt.date(2020, 12, 31), dt.date(2020, 12, 31)]


def test_conflicting_registered_manifest_identity_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "PBI_FY2020_10K_2020-12-31_financial_statement.htm"
    path.write_text(
        _document_identity_html(
            document_date="December 31, 2020",
            form="10-K",
            fiscal_year="2020",
            fiscal_period="FY",
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "ticker": "PBI",
                "form": "10-K",
                "reportDate": "2020-09-30",
                "materialized_path": str(path),
            }
        ]
    ).to_csv(tmp_path / "PBI_financial_statement_manifest.csv", index=False)
    with pytest.raises(FinancialStatementDocumentPeriodError, match="conflicting document reporting periods"):
        resolve_financial_statement_document_period(path)


def test_missing_registered_filename_identity_uses_inline_xbrl_dei(tmp_path: Path) -> None:
    path = tmp_path / "statement.htm"
    path.write_text(
        _document_identity_html(
            document_date="December 31, 2020",
            form="10-K",
            fiscal_year="2020",
            fiscal_period="FY",
            body="<p>Three Months Ended March 31, 2020</p>",
        ),
        encoding="utf-8",
    )
    identity = resolve_financial_statement_document_period(path)
    assert identity.reporting_date == dt.date(2020, 12, 31)
    assert identity.selected_authority == "inline_xbrl_dei"


def test_filename_and_inline_xbrl_document_period_conflict_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "PBI_FY2020_10K_2020-12-31_financial_statement.htm"
    path.write_text(
        _document_identity_html(
            document_date="September 30, 2020",
            form="10-K",
            fiscal_year="2020",
            fiscal_period="FY",
        ),
        encoding="utf-8",
    )
    with pytest.raises(FinancialStatementDocumentPeriodError, match="conflicting document reporting periods"):
        resolve_financial_statement_document_period(path)


def test_document_period_does_not_override_debt_fact_period(tmp_path: Path) -> None:
    path = tmp_path / "PBI_FY2020_10K_2020-12-31_financial_statement.htm"
    debt_html = _inline_debt_html(
        current_text="125",
        prior_text="100",
        current_date="2019-12-31",
        prior_date="2018-12-31",
        omit_visual_dates=True,
    )
    document_facts = """
        <ix:nonnumeric name="dei:DocumentType" contextref="c-document">10-K</ix:nonnumeric>
        <ix:nonnumeric name="dei:DocumentPeriodEndDate" contextref="c-document">December 31, 2020</ix:nonnumeric>
        <ix:nonnumeric name="dei:DocumentFiscalYearFocus" contextref="c-document">2020</ix:nonnumeric>
        <ix:nonnumeric name="dei:DocumentFiscalPeriodFocus" contextref="c-document">FY</ix:nonnumeric>
    """
    path.write_text(
        debt_html.replace("</body>", document_facts + "</body>"),
        encoding="utf-8",
    )
    assert resolve_financial_statement_document_period(path).reporting_date == dt.date(2020, 12, 31)
    row = _parse_financial_statement_debt_table_html(path, dt.date(2019, 12, 31))[0]
    assert row["reporting_date"] == "2019-12-31"
    assert row["context_reporting_date"] == "2019-12-31"


def test_document_identity_without_registered_or_dei_authority_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "statement.htm"
    path.write_text("<html><body>Three Months Ended March 31, 2020</body></html>", encoding="utf-8")
    with pytest.raises(FinancialStatementDocumentPeriodError, match="no registered or inline-XBRL"):
        resolve_financial_statement_document_period(path)


def test_colocated_8k_is_not_owned_by_financial_statement_document_route(tmp_path: Path) -> None:
    path = tmp_path / "unregistered-8k.htm"
    path.write_text(
        _document_identity_html(
            document_date="June 23, 2026",
            form="8-K",
            fiscal_year="2026",
            fiscal_period="Q2",
        ),
        encoding="utf-8",
    )
    assert not is_owned_financial_statement_document(path)
    with pytest.raises(FinancialStatementDocumentPeriodError, match="unsupported inline-XBRL DocumentType"):
        resolve_financial_statement_document_period(path)
