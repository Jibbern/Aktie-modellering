from __future__ import annotations

import datetime as dt
import json
from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest

from pbi_xbrl.non_gaap import (
    ADJUSTED_METRIC_SOURCE_UNIT_CONTRACT,
    NON_GAAP_ADJUSTMENT_DOMAIN_CONTRACT,
    NON_GAAP_ADJUSTMENT_DOMAIN_VERSION,
    AdjustmentTableRole,
    CanonicalAdjustmentFact,
    CanonicalSourceAmount,
    MeasureDomain,
    SourceAmountScale,
    SourceAmountUnit,
    SourceUnitContractError,
    _adjustment_row_domain,
    _find_header_dates,
    build_non_gaap_tier3,
    classify_adjustment_table,
    detect_source_amount_unit,
    find_ex99_docs,
    infer_quarter_end_from_text,
    normalize_source_amount,
    normalize_number_spacing,
    parse_adjusted_from_ex99,
    parse_adjusted_from_plain_text,
    reconcile_adjustment_facts,
)
from pbi_xbrl.path_config import resolve_effective_data_root_from_ancestors
from pbi_xbrl.pipeline_orchestration import (
    DEBT_TABLE_PERIOD_OWNERSHIP_VERSION,
    _tier3_non_gaap_stage_cache_key,
)


def _registered_pbi_q4_2022_release() -> Path:
    resolution = resolve_effective_data_root_from_ancestors(Path(__file__).resolve(), env={})
    assert resolution.data_root is not None, (*resolution.errors, *resolution.warnings)
    source_path = (
        resolution.data_root
        / "tickers"
        / "PBI"
        / "earnings_release"
        / "PBI_Q4_2022_earnings_release.htm"
    )
    assert source_path.is_file()
    return source_path


def _registered_pbi_sec_document(name: str) -> Path:
    resolution = resolve_effective_data_root_from_ancestors(Path(__file__).resolve(), env={})
    assert resolution.data_root is not None, (*resolution.errors, *resolution.warnings)
    source_path = resolution.data_root / "sec_cache" / "PBI" / name
    assert source_path.is_file()
    return source_path


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


def test_parse_adjusted_from_plain_text_skips_pbi_reconciliation_heading_before_adj_ebitda() -> None:
    txt = """
    Pitney Bowes Inc.
    Reconciliation of Reported Consolidated Results to Adjusted Results
    (Unaudited; in thousands, except per share amounts)
    Three Months Ended
    March 31,
    2026 2025
    Reconciliation of net income to adjusted net income,
    adjusted EBIT and adjusted EBITDA
    Net income - GAAP $58,138 $35,422
    Adjusted net income $68,942 $61,691
    Adjusted income before tax $94,802 $81,804
    Interest expense, including financing interest 35,575 37,885
    Adjusted EBIT 130,377 119,689
    Depreciation and amortization 25,641 28,324
    Adjusted EBITDA $156,018 $148,013
    Adjusted diluted earnings per share $0.47 $0.33
    """

    adj_ebit, adj_ebitda, adj_eps, adjustments, status, source = parse_adjusted_from_plain_text(
        txt,
        quarter_end=pd.Timestamp("2026-03-31"),
        mode="relaxed",
    )

    assert adj_ebit == 130_377_000.0
    assert adj_ebitda == 156_018_000.0
    assert adj_eps == 0.47
    assert adjustments == {}
    assert status == "ok_relaxed_ocr"
    assert source == "ocr"


def test_parse_adjusted_from_ex99_reads_pbi_colspan_consolidated_adjusted_table() -> None:
    html = """
    <html><body>
    <table>
      <tr><td>Pitney Bowes Inc.</td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td></tr>
      <tr><td>Reconciliation of Reported Consolidated Results to Adjusted Results</td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td></tr>
      <tr><td>(Unaudited; in thousands, except per share amounts)</td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td></tr>
      <tr><td></td><td></td><td>Three months ended June 30,</td><td>Three months ended June 30,</td><td></td><td></td><td>Three months ended June 30,</td><td></td><td>Six months ended June 30,</td><td>Six months ended June 30,</td><td></td><td></td><td>Six months ended June 30,</td></tr>
      <tr><td></td><td></td><td></td><td>2023</td><td></td><td></td><td>2022</td><td></td><td></td><td>2023</td><td></td><td></td><td>2022</td></tr>
      <tr><td>Net (loss) income</td><td></td><td>$</td><td>(141,535)</td><td></td><td>$</td><td>4,336</td><td></td><td>$</td><td>(149,272)</td><td></td><td>$</td><td>25,157</td></tr>
      <tr><td>Restructuring charges</td><td></td><td></td><td>22,443</td><td></td><td></td><td>4,224</td><td></td><td></td><td>26,042</td><td></td><td></td><td>8,408</td></tr>
      <tr><td>Adjusted EBIT</td><td></td><td></td><td>32,085</td><td></td><td></td><td>38,830</td><td></td><td></td><td>65,106</td><td></td><td></td><td>91,507</td></tr>
      <tr><td>Depreciation and amortization</td><td></td><td></td><td>39,873</td><td></td><td></td><td>43,470</td><td></td><td></td><td>79,770</td><td></td><td></td><td>85,472</td></tr>
      <tr><td>Adjusted EBITDA</td><td></td><td>$</td><td>71,958</td><td></td><td>$</td><td>82,300</td><td></td><td>$</td><td>144,876</td><td></td><td>$</td><td>176,979</td></tr>
    </table>
    </body></html>
    """

    adj_ebit, adj_ebitda, adj_eps, adjustments, status, source = parse_adjusted_from_ex99(
        html.encode("utf-8"),
        quarter_end=pd.Timestamp("2023-06-30"),
        mode="relaxed",
    )

    assert adj_ebit == 32_085_000.0
    assert adj_ebitda == 71_958_000.0
    assert adj_eps is None
    assert adjustments
    assert status == "ok_relaxed"
    assert "2023" in str(source)


@pytest.mark.parametrize(
    ("raw_value", "scale", "expected_usd_millions"),
    [
        ("49,267", SourceAmountScale.THOUSANDS, 49.267),
        ("88,331", SourceAmountScale.THOUSANDS, 88.331),
        ("49.267", SourceAmountScale.MILLIONS, 49.267),
        ("0", SourceAmountScale.THOUSANDS, 0.0),
        ("(1,250)", SourceAmountScale.THOUSANDS, -1.250),
    ],
)
def test_source_amount_normalization_applies_explicit_scale_once(
    raw_value: str,
    scale: SourceAmountScale,
    expected_usd_millions: float,
) -> None:
    unit = SourceAmountUnit(currency="USD", scale=scale, declaration=f"in {scale.value}")

    amount = normalize_source_amount(raw_value, unit)

    assert amount.canonical_usd_millions == pytest.approx(expected_usd_millions)


def test_already_normalized_usd_amount_is_not_scaled_again() -> None:
    unit = SourceAmountUnit(currency="USD", scale=SourceAmountScale.THOUSANDS, declaration="in thousands")
    amount = normalize_source_amount("49,267", unit)

    assert normalize_source_amount(amount, unit) is amount
    with pytest.raises(SourceUnitContractError, match="different source unit"):
        normalize_source_amount(
            amount,
            SourceAmountUnit(currency="USD", scale=SourceAmountScale.MILLIONS, declaration="in millions"),
        )


def test_conflicting_source_unit_scope_fails_closed() -> None:
    with pytest.raises(SourceUnitContractError, match="conflicting amount scales"):
        detect_source_amount_unit("first table in thousands; another table in millions")


def test_table_local_unit_owns_adjusted_metrics_when_document_contains_other_scales() -> None:
    html = """
    <html><body>
      <p>Revenue discussion presented in millions.</p>
      <table>
        <tr><td>Reconciliation of Reported Consolidated Results to Adjusted Results</td><td></td><td></td><td></td></tr>
        <tr><td>(Unaudited; in thousands, except per share amounts)</td><td></td><td></td><td></td></tr>
        <tr><td>Three months ended December 31,</td><td></td><td></td><td>2022</td></tr>
        <tr><td>Net income - GAAP</td><td></td><td>$</td><td>1,000</td></tr>
        <tr><td>Adjusted EBIT</td><td></td><td></td><td>49,267</td></tr>
        <tr><td>Depreciation and amortization</td><td></td><td></td><td>39,064</td></tr>
        <tr><td>Adjusted EBITDA</td><td></td><td>$</td><td>88,331</td></tr>
      </table>
    </body></html>
    """
    metadata: dict[str, dict[str, object]] = {}

    adj_ebit, adj_ebitda, _adj_eps, _adjustments, status, _source = parse_adjusted_from_ex99(
        html.encode("utf-8"),
        quarter_end=pd.Timestamp("2022-12-31"),
        mode="relaxed",
        extraction_metadata=metadata,
    )

    assert status == "ok_relaxed"
    assert adj_ebit == 49_267_000.0
    assert adj_ebitda == 88_331_000.0
    assert metadata["adj_ebit"]["source_scale"] == "thousands"
    assert metadata["adj_ebit"]["raw_source_scalar"] == 49_267.0


def test_registered_pbi_q4_2022_release_retains_table_local_unit_lineage() -> None:
    source_path = _registered_pbi_q4_2022_release()
    metadata: dict[str, dict[str, object]] = {}

    adj_ebit, adj_ebitda, _adj_eps, _adjustments, status, _source = parse_adjusted_from_ex99(
        source_path.read_bytes(),
        quarter_end=pd.Timestamp("2022-12-31"),
        mode="strict",
        extraction_metadata=metadata,
    )

    assert status == "ok"
    assert adj_ebit == 49_267_000.0
    assert adj_ebitda == 88_331_000.0
    assert metadata["adj_ebit"] == {
        "raw_source_scalar": 49_267.0,
        "source_currency": "USD",
        "source_scale": "thousands",
        "source_scale_factor": 1_000.0,
        "source_unit_declaration": "(Unaudited; in thousands, except per share amounts)",
        "measure_domain": "monetary_amount",
        "source_unit_row_index": 2,
        "canonical_currency": "USD",
        "canonical_unit": "USD",
        "canonical_unit_id": "unit:core:currency@1",
        "canonical_value": 49_267_000.0,
        "canonical_usd_millions": 49.267,
        "source_table_index": 13,
        "source_row_index": 18,
        "source_column_index": 3,
        "source_column_label": "2022 3M consolidated table",
        "source_locator": "html-table:13;row:18;column:3",
    }
    assert metadata["adj_ebitda"]["raw_source_scalar"] == 88_331.0
    assert metadata["adj_ebitda"]["canonical_usd_millions"] == 88.331


def test_tier3_bundle_rows_serialize_canonical_unit_and_occurrence_lineage() -> None:
    source_bytes = _registered_pbi_q4_2022_release().read_bytes()

    class _Sec:
        @staticmethod
        def accession_index_json(_cik: int, _accn: str) -> dict[str, object]:
            return {"directory": {"item": [{"name": "a53293633ex_991.htm"}]}}

        @staticmethod
        def download_document(_cik: int, _accn: str, _doc: str) -> bytes:
            return source_bytes

        @staticmethod
        def download_html_assets(*_args: object, **_kwargs: object) -> None:
            return None

        @staticmethod
        def download_index_images(*_args: object, **_kwargs: object) -> None:
            return None

    metrics, _breakdown, _files = build_non_gaap_tier3(
        _Sec(),
        78814,
        {
            "filings": {
                "recent": {
                    "form": ["8-K"],
                    "accessionNumber": ["0001157523-23-000123"],
                    "filingDate": ["2023-01-31"],
                    "reportDate": ["2022-12-31"],
                }
            }
        },
        max_quarters=8,
        mode="strict",
    )

    row = metrics.iloc[0]
    assert row["adj_ebit"] == 49_267_000.0
    assert row["adj_ebitda"] == 88_331_000.0
    assert row["source_lineage_contract"] == ADJUSTED_METRIC_SOURCE_UNIT_CONTRACT
    assert row["adj_ebit_raw_source_scalar"] == 49_267.0
    assert row["adj_ebit_source_scale"] == "thousands"
    assert row["adj_ebit_canonical_value"] == 49_267_000.0
    assert str(row["source_document_id"]).startswith("sec-document:v1|")
    assert str(row["adj_ebit_source_occurrence_id"]).startswith("non-gaap-occurrence:v1|")


def test_tier3_cache_key_owns_adjusted_metric_unit_semantics(monkeypatch) -> None:
    import pbi_xbrl.pipeline_orchestration as orchestration

    key = _tier3_non_gaap_stage_cache_key(
        sec_source_identity="sec-source",
        submissions_signature="submissions-sha",
        mode_name="strict",
        max_quarters=80,
    )

    assert key.startswith("v1_canonical_json_sha256:pipeline-stage:tier3_non_gaap:")
    assert not key.endswith(":none")

    for name in (
        "TIER3_NON_GAAP_STAGE_VERSION",
        "ADJUSTED_METRIC_UNIT_NORMALIZATION_VERSION",
        "NON_GAAP_ADJUSTMENT_DOMAIN_VERSION",
    ):
        monkeypatch.setattr(orchestration, name, f"mutation-{name}")
        mutated = orchestration._tier3_non_gaap_stage_cache_key(
            sec_source_identity="sec-source",
            submissions_signature="submissions-sha",
            mode_name="strict",
            max_quarters=80,
        )
        assert mutated != key
        monkeypatch.undo()


def test_local_bundle_semantics_own_debt_fact_period_resolution() -> None:
    assert DEBT_TABLE_PERIOD_OWNERSHIP_VERSION == "v1_visual_xbrl_context"


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


@pytest.mark.parametrize(
    "declaration",
    [
        "($ millions)",
        "($ in millions)",
        "$ millions",
        "in millions",
        "(in millions)",
        "(Dollars in millions)",
        "(amounts in millions)",
    ],
)
def test_observed_millions_declarations_resolve_to_monetary_millions(declaration: str) -> None:
    unit = detect_source_amount_unit(declaration)

    assert unit == SourceAmountUnit(
        currency="USD",
        scale=SourceAmountScale.MILLIONS,
        declaration=unit.declaration,
        measure_domain=MeasureDomain.MONETARY_AMOUNT,
    )


@pytest.mark.parametrize("declaration", ["($ thousands)", "in thousands", "(in thousands)", "amounts in thousands"])
def test_observed_thousands_declarations_remain_monetary_thousands(declaration: str) -> None:
    unit = detect_source_amount_unit(declaration)

    assert unit is not None
    assert unit.scale is SourceAmountScale.THOUSANDS
    assert unit.measure_domain is MeasureDomain.MONETARY_AMOUNT


def test_table_local_millions_does_not_leak_into_separate_thousands_table() -> None:
    millions = pd.DataFrame([["($ millions)", None], ["Adjusted EBIT", "65"]])
    thousands = pd.DataFrame([["(in thousands)", None], ["Adjusted EBIT", "65"]])

    millions_role = classify_adjustment_table(millions)
    thousands_role = classify_adjustment_table(thousands)

    assert millions_role.source_unit is not None
    assert thousands_role.source_unit is not None
    assert millions_role.source_unit.scale is SourceAmountScale.MILLIONS
    assert thousands_role.source_unit.scale is SourceAmountScale.THOUSANDS


def test_document_level_amount_scale_cannot_own_an_unclassified_adjustment_table() -> None:
    table = pd.DataFrame(
        [["Reconciliation to Adjusted EBIT", None], ["Adjusted EBIT", "65"]]
    )

    classification = classify_adjustment_table(
        table,
        document_default=SourceAmountUnit(
            "USD",
            SourceAmountScale.MILLIONS,
            "document-global:in millions",
        ),
    )

    assert classification.role is AdjustmentTableRole.UNRESOLVED
    assert classification.evidence == "amount_reconciliation_missing_source_unit"


def test_registered_millions_fcf_table_preserves_exact_amount_domain_and_lineage() -> None:
    source = _registered_pbi_sec_document("doc_000093041316008161_c86095_ex99-1.htm")
    facts: list[CanonicalAdjustmentFact] = []

    _ebit, _ebitda, _eps, adjustments, status, _column = parse_adjusted_from_ex99(
        source.read_bytes(),
        pd.Timestamp("2016-09-12"),
        mode="relaxed",
        adjustment_facts=facts,
    )

    expected = {
        "restructuring payments": 65_000_000.0,
        "pension plan contribution": 36_000_000.0,
        "tax and other payments on sale of businesses and leveraged lease assets": 21_000_000.0,
    }
    assert status == "ok_relaxed"
    assert {key: adjustments[key] for key in expected} == expected
    selected = {fact.source_label: fact for fact in facts}
    assert set(expected).issubset(selected)
    for label, value in expected.items():
        fact = selected[label]
        assert fact.measure_domain is MeasureDomain.MONETARY_AMOUNT
        assert fact.table_role is AdjustmentTableRole.AMOUNT_RECONCILIATION
        assert fact.amount.source_unit.declaration == "($ millions)"
        assert fact.amount.canonical_value == value
        assert fact.amount.canonical_unit == "USD"


def test_registered_millions_fcf_table_normalizes_endpoint_and_breakdown_once() -> None:
    source_bytes = _registered_pbi_sec_document(
        "doc_000093041316008161_c86095_ex99-1.htm"
    ).read_bytes()

    class _Sec:
        @staticmethod
        def accession_index_json(_cik: int, _accn: str) -> dict[str, object]:
            return {"directory": {"item": [{"name": "c86095_ex99-1.htm"}]}}

        @staticmethod
        def download_document(_cik: int, _accn: str, _doc: str) -> bytes:
            return source_bytes

        @staticmethod
        def download_html_assets(*_args: object, **_kwargs: object) -> None:
            return None

        @staticmethod
        def download_index_images(*_args: object, **_kwargs: object) -> None:
            return None

    metrics, breakdown, _files = build_non_gaap_tier3(
        _Sec(),
        78814,
        {
            "filings": {
                "recent": {
                    "form": ["8-K"],
                    "accessionNumber": ["0000930413-16-008161"],
                    "filingDate": ["2016-09-12"],
                    "reportDate": ["2016-09-12"],
                }
            }
        },
        max_quarters=8,
        mode="relaxed",
    )

    assert metrics.iloc[0]["adj_fcf"] == 433_000_000.0
    selected = breakdown.set_index("label")
    assert selected.loc["restructuring payments", "value"] == 65_000_000.0
    assert selected.loc["pension plan contribution", "value"] == 36_000_000.0
    assert (
        selected.loc[
            "tax and other payments on sale of businesses and leveraged lease assets",
            "value",
        ]
        == 21_000_000.0
    )
    assert set(selected["raw_source_unit_text"]) == {"($ millions)"}
    assert set(selected["normalized_source_scale"]) == {"millions"}
    assert set(selected["measure_domain"]) == {"monetary_amount"}


def test_registered_eps_reconciliation_retains_per_share_evidence_outside_amount_projection() -> None:
    source = _registered_pbi_sec_document("doc_000119312525169331_d98441dex991.htm")
    facts: list[CanonicalAdjustmentFact] = []

    _ebit, _ebitda, eps, adjustments, status, _column = parse_adjusted_from_ex99(
        source.read_bytes(),
        pd.Timestamp("2025-06-30"),
        mode="relaxed",
        adjustment_facts=facts,
    )

    assert status == "ok_relaxed"
    assert eps == 0.27
    assert adjustments == {}
    selected = {fact.source_label: fact for fact in facts}
    assert selected["restructuring charges"].amount.canonical_value == 0.06
    assert selected["foreign currency loss on intercompany loans"].amount.canonical_value == 0.07
    assert selected["benefit in connection with ecommerce restructuring"].amount.canonical_value == 0.03
    for fact in selected.values():
        assert fact.measure_domain is MeasureDomain.PER_SHARE_AMOUNT
        assert fact.table_role is AdjustmentTableRole.EPS_RECONCILIATION
        assert fact.amount.canonical_unit == "USD/share"
        assert fact.amount.canonical_unit_id == "unit:core:currency-per-share@1"


def test_amount_normalization_preserves_negative_and_zero_millions() -> None:
    unit = SourceAmountUnit("USD", SourceAmountScale.MILLIONS, "($ millions)")

    assert normalize_source_amount("(65)", unit).canonical_value == -65_000_000.0
    assert normalize_source_amount("0", unit).canonical_value == 0.0


def test_small_monetary_adjustment_is_not_reclassified_by_magnitude() -> None:
    facts: list[CanonicalAdjustmentFact] = []
    html = b"""
    <table><tr><td>Reconciliation to Adjusted EBIT</td><td>$</td></tr>
    <tr><td>GAAP operating income</td><td>1.00</td></tr>
    <tr><td>Restructuring charges</td><td>0.06</td></tr>
    <tr><td>Adjusted EBIT</td><td>1.06</td></tr></table>
    """

    _ebit, _ebitda, _eps, adjustments, status, _column = parse_adjusted_from_ex99(
        html,
        pd.Timestamp("2025-06-30"),
        mode="relaxed",
        adjustment_facts=facts,
    )

    assert status == "ok_relaxed"
    assert adjustments["restructuring charges"] == 0.06
    fact = next(item for item in facts if item.source_label == "restructuring charges")
    assert fact.measure_domain is MeasureDomain.MONETARY_AMOUNT
    assert fact.amount.canonical_unit == "USD"


def test_large_per_share_adjustment_is_not_reclassified_by_magnitude() -> None:
    facts: list[CanonicalAdjustmentFact] = []
    html = b"""
    <table><tr><td>GAAP EPS</td><td>$</td><td>5</td></tr>
    <tr><td>Restructuring charges</td><td>$</td><td>65</td></tr>
    <tr><td>Adjusted EPS</td><td>$</td><td>70</td></tr></table>
    """

    _ebit, _ebitda, _eps, adjustments, status, _column = parse_adjusted_from_ex99(
        html,
        pd.Timestamp("2025-06-30"),
        mode="relaxed",
        adjustment_facts=facts,
    )

    assert status == "ok_relaxed"
    assert adjustments == {}
    fact = next(item for item in facts if item.source_label == "restructuring charges")
    assert fact.measure_domain is MeasureDomain.PER_SHARE_AMOUNT
    assert fact.amount.canonical_value == 65.0
    assert fact.amount.canonical_unit == "USD/share"


def test_per_share_amount_cannot_be_converted_to_usd_millions_or_rescaled() -> None:
    unit = SourceAmountUnit(
        "USD",
        SourceAmountScale.ONES,
        "table-role:gaap-eps-to-adjusted-eps;unit:USD/share",
        MeasureDomain.PER_SHARE_AMOUNT,
    )
    amount = normalize_source_amount("0.06", unit)

    assert amount.canonical_value == 0.06
    assert amount.canonical_unit == "USD/share"
    with pytest.raises(SourceUnitContractError, match="not defined for per-share"):
        _ = amount.canonical_usd_millions


def test_same_adjustment_keyword_in_amount_and_eps_tables_remains_domain_distinct() -> None:
    amount_html = b"""
    <table><tr><td>Reconciliation to Adjusted EBIT</td><td></td></tr>
    <tr><td>($ millions)</td><td></td></tr>
    <tr><td>GAAP operating income</td><td>5</td></tr>
    <tr><td>Restructuring charges</td><td>2</td></tr>
    <tr><td>Adjusted EBIT</td><td>7</td></tr></table>
    """
    eps_html = b"""
    <table><tr><td>GAAP EPS</td><td>$</td><td>0.10</td></tr>
    <tr><td>Restructuring charges</td><td>$</td><td>0.06</td></tr>
    <tr><td>Adjusted EPS</td><td>$</td><td>0.16</td></tr></table>
    """
    amount_facts: list[CanonicalAdjustmentFact] = []
    eps_facts: list[CanonicalAdjustmentFact] = []
    parse_adjusted_from_ex99(amount_html, pd.Timestamp("2025-06-30"), mode="relaxed", adjustment_facts=amount_facts)
    parse_adjusted_from_ex99(eps_html, pd.Timestamp("2025-06-30"), mode="relaxed", adjustment_facts=eps_facts)
    combined = reconcile_adjustment_facts(
        [
            fact
            for fact in amount_facts + eps_facts
            if fact.source_label == "restructuring charges"
        ]
    )

    assert {fact.measure_domain for fact in combined} == {
        MeasureDomain.MONETARY_AMOUNT,
        MeasureDomain.PER_SHARE_AMOUNT,
    }
    assert len({fact.semantic_key for fact in combined}) == 2


def test_registered_mixed_reconciliation_keeps_amount_and_per_share_sections_distinct() -> None:
    facts: list[CanonicalAdjustmentFact] = []

    _ebit, _ebitda, _eps, adjustments, status, _column = parse_adjusted_from_ex99(
        _registered_pbi_q4_2022_release().read_bytes(),
        pd.Timestamp("2022-12-31"),
        mode="strict",
        adjustment_facts=facts,
    )

    assert status == "ok"
    restructuring = [
        fact for fact in facts if fact.source_label == "restructuring charges"
    ]
    assert {
        (fact.table_role, fact.measure_domain, fact.amount.canonical_value)
        for fact in restructuring
    } == {
        (
            AdjustmentTableRole.MIXED_RECONCILIATION,
            MeasureDomain.MONETARY_AMOUNT,
            6_043_000.0,
        ),
        (
            AdjustmentTableRole.MIXED_RECONCILIATION,
            MeasureDomain.PER_SHARE_AMOUNT,
            0.03,
        ),
    }
    assert adjustments["restructuring charges"] == 6_043_000.0
    assert 0.03 not in adjustments.values()


def test_adjustment_fact_reconciliation_is_order_independent_and_accepts_exact_corroboration() -> None:
    source = _registered_pbi_sec_document("doc_000093041316008161_c86095_ex99-1.htm")
    facts: list[CanonicalAdjustmentFact] = []
    parse_adjusted_from_ex99(
        source.read_bytes(),
        pd.Timestamp("2016-09-12"),
        mode="relaxed",
        adjustment_facts=facts,
    )
    fact = next(item for item in facts if item.source_label == "restructuring payments")
    corroboration = replace(fact, source_table_index=99, source_row_index=1)

    forward = reconcile_adjustment_facts([fact, corroboration])
    reverse = reconcile_adjustment_facts([corroboration, fact])

    assert forward == reverse == [fact]


def test_conflicting_same_domain_adjustment_facts_fail_closed() -> None:
    source = _registered_pbi_sec_document("doc_000093041316008161_c86095_ex99-1.htm")
    facts: list[CanonicalAdjustmentFact] = []
    parse_adjusted_from_ex99(
        source.read_bytes(),
        pd.Timestamp("2016-09-12"),
        mode="relaxed",
        adjustment_facts=facts,
    )
    fact = next(item for item in facts if item.source_label == "restructuring payments")
    conflicting = replace(
        fact,
        amount=normalize_source_amount("66", fact.amount.source_unit),
        source_table_index=99,
    )

    with pytest.raises(SourceUnitContractError, match="Conflicting same-domain adjustment facts"):
        reconcile_adjustment_facts([fact, conflicting])


def test_conflicting_table_local_units_fail_closed() -> None:
    table = pd.DataFrame(
        [
            ["Reconciliation to Adjusted EBIT", None],
            ["(in thousands)", "($ millions)"],
            ["Adjusted EBIT", "65"],
        ]
    )

    with pytest.raises(SourceUnitContractError, match="conflicting amount scales"):
        classify_adjustment_table(table)


def test_unclassified_adjustment_table_role_fails_closed_before_row_materialization() -> None:
    classification = classify_adjustment_table(
        pd.DataFrame([["Restructuring charges", "$", "0.06"]])
    )

    assert classification.role is AdjustmentTableRole.UNRESOLVED
    with pytest.raises(SourceUnitContractError, match="unresolved table role"):
        _adjustment_row_domain(classification, row_label="restructuring charges")


def test_tier3_retains_per_share_evidence_but_excludes_it_from_amount_breakdown() -> None:
    source_bytes = _registered_pbi_sec_document(
        "doc_000119312525169331_d98441dex991.htm"
    ).read_bytes()

    class _Sec:
        @staticmethod
        def accession_index_json(_cik: int, _accn: str) -> dict[str, object]:
            return {"directory": {"item": [{"name": "d98441dex991.htm"}]}}

        @staticmethod
        def download_document(_cik: int, _accn: str, _doc: str) -> bytes:
            return source_bytes

        @staticmethod
        def download_html_assets(*_args: object, **_kwargs: object) -> None:
            return None

        @staticmethod
        def download_index_images(*_args: object, **_kwargs: object) -> None:
            return None

    metrics, breakdown, _files = build_non_gaap_tier3(
        _Sec(),
        78814,
        {
            "filings": {
                "recent": {
                    "form": ["8-K"],
                    "accessionNumber": ["0001193125-25-169331"],
                    "filingDate": ["2025-07-30"],
                    "reportDate": ["2025-06-30"],
                }
            }
        },
        max_quarters=8,
        mode="relaxed",
    )

    assert breakdown.empty
    row = metrics.iloc[0]
    assert row["adjustment_domain_contract"] == NON_GAAP_ADJUSTMENT_DOMAIN_CONTRACT
    assert row["per_share_adjustment_fact_count"] == 3
    evidence = json.loads(row["adjustment_evidence_json"])
    assert {item["canonical_value"] for item in evidence} == {0.03, 0.06, 0.07}
    assert {item["measure_domain"] for item in evidence} == {"per_share_amount"}
