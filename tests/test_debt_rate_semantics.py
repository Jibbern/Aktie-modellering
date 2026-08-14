from __future__ import annotations

import datetime as dt
import inspect
from pathlib import Path

import pytest

from pbi_xbrl.cache_semantics import DEBT_RATE_SEMANTIC_OWNERSHIP_VERSION
from pbi_xbrl.debt_rate_semantics import (
    DEBT_RATE_OWNERSHIP_CONTRACT_ID,
    DebtRateAuthority,
    DebtRateConflictError,
    DebtRateFactCandidate,
    DebtRateRole,
    classify_debt_rate_concept,
    display_coupon_rate,
    resolve_debt_rate_facts,
    select_debt_detail_rate,
)
from pbi_xbrl.path_config import resolve_effective_data_root_from_ancestors
from pbi_xbrl.pipeline_orchestration import _parse_financial_statement_debt_table_html


REPORTING_DATE = dt.date(2019, 12, 31)


def _candidate(
    fact_id: str,
    *,
    role: DebtRateRole = DebtRateRole.COUPON_STATED_RATE,
    value: float = 0.04125,
    reporting_date: dt.date = REPORTING_DATE,
    instrument: str = "instrument:convertible-notes-2022",
    authority: DebtRateAuthority = DebtRateAuthority.STRUCTURED_DIRECT,
    basis_scope: str = "consolidated",
    direct: bool = True,
) -> DebtRateFactCandidate:
    return DebtRateFactCandidate(
        instrument_identity=instrument,
        reporting_date=reporting_date,
        role=role,
        value=value,
        rendered_text=f"{value * 100:g}%",
        raw_scalar=f"{value * 100:g}",
        concept=f"test:{role.value}",
        unit_ref="pure",
        canonical_unit="ratio",
        context_id=f"context:{reporting_date.isoformat()}:{fact_id}",
        fact_id=fact_id,
        basis_scope=basis_scope,
        source_locator=f"table:debt;fact:{fact_id}",
        authority=authority,
        direct=direct,
    )


def _registered_gpre_fy2019() -> Path:
    resolution = resolve_effective_data_root_from_ancestors(Path(__file__).resolve(), env={})
    assert resolution.data_root is not None, (*resolution.errors, *resolution.warnings)
    return (
        resolution.data_root
        / "tickers"
        / "GPRE"
        / "financial_statement"
        / "GPRE_FY2019_10K_2019-12-31_financial_statement.htm"
    )


def test_distinct_coupon_effective_rate_and_spread_roles_remain_addressable() -> None:
    candidates = (
        _candidate("coupon"),
        _candidate("effective", role=DebtRateRole.EFFECTIVE_INTEREST_RATE, value=0.052),
        _candidate("spread", role=DebtRateRole.SPREAD_MARGIN, value=0.0225),
    )
    resolved = resolve_debt_rate_facts(candidates, requested_reporting_date=REPORTING_DATE)
    assert {fact.selected.role for fact in resolved} == {
        DebtRateRole.COUPON_STATED_RATE,
        DebtRateRole.EFFECTIVE_INTEREST_RATE,
        DebtRateRole.SPREAD_MARGIN,
    }
    assert select_debt_detail_rate(resolved).selected.fact_id == "coupon"


def test_identical_same_role_facts_corroborate_and_retain_lineage() -> None:
    resolved = resolve_debt_rate_facts(
        (_candidate("z-source"), _candidate("a-source")),
        requested_reporting_date=REPORTING_DATE,
    )
    assert len(resolved) == 1
    assert resolved[0].selected.fact_id == "a-source"
    assert resolved[0].evidence_fact_ids == ("a-source", "z-source")
    assert resolved[0].as_record()["rate_ownership_contract_id"] == DEBT_RATE_OWNERSHIP_CONTRACT_ID


def test_conflicting_same_authority_coupon_facts_fail_closed() -> None:
    with pytest.raises(DebtRateConflictError, match="Conflicting same-authority"):
        resolve_debt_rate_facts(
            (_candidate("coupon-a", value=0.04125), _candidate("coupon-b", value=0.05)),
            requested_reporting_date=REPORTING_DATE,
        )


def test_direct_structured_coupon_beats_weaker_display_derivation() -> None:
    candidates = (
        _candidate("display", value=0.05, authority=DebtRateAuthority.DERIVED_DISPLAY, direct=False),
        _candidate("structured", value=0.04125),
    )
    resolved = resolve_debt_rate_facts(candidates, requested_reporting_date=REPORTING_DATE)
    assert resolved[0].selected.fact_id == "structured"
    assert resolved[0].selected.value == 0.04125


def test_candidate_source_order_does_not_change_owner() -> None:
    candidates = [_candidate("source-b"), _candidate("source-a")]
    forward = resolve_debt_rate_facts(candidates, requested_reporting_date=REPORTING_DATE)
    reverse = resolve_debt_rate_facts(reversed(candidates), requested_reporting_date=REPORTING_DATE)
    assert forward == reverse


@pytest.mark.parametrize(
    "label",
    [
        "Convertible notes due 2022 (2)",
        "Convertible notes due 2022 principal 149,256",
        "Conversion premium 25% — not an instrument coupon",
        "Footnote (2) includes $2.0 million costs",
    ],
)
def test_footnote_maturity_principal_and_unrelated_percent_are_not_coupons(label: str) -> None:
    assert display_coupon_rate(label) is None


def test_explicit_instrument_description_coupon_is_parsed_without_footnote_leakage() -> None:
    assert display_coupon_rate("4.125 % convertible notes due 2022 (2)") == (
        0.04125,
        "4.125%",
    )


def test_unrelated_percent_concepts_do_not_become_coupon() -> None:
    assert (
        classify_debt_rate_concept("us-gaap:DebtConversionConvertedInstrumentRate")
        is DebtRateRole.CONVERSION_RELATED_RATE
    )
    assert (
        classify_debt_rate_concept("us-gaap:EffectiveIncomeTaxRateContinuingOperations")
        is DebtRateRole.NOT_A_RATE
    )


def test_current_period_never_borrows_prior_rate_and_missing_stays_missing() -> None:
    prior = _candidate("prior", reporting_date=dt.date(2018, 12, 31))
    assert resolve_debt_rate_facts((prior,), requested_reporting_date=REPORTING_DATE) == ()
    assert resolve_debt_rate_facts((), requested_reporting_date=REPORTING_DATE) == ()


def test_explicit_zero_rate_is_preserved() -> None:
    resolved = resolve_debt_rate_facts(
        (_candidate("zero", value=0.0),), requested_reporting_date=REPORTING_DATE
    )
    assert resolved[0].selected.value == 0.0


def test_instrument_label_cannot_override_incompatible_structured_role() -> None:
    assert display_coupon_rate("4.125% convertible notes due 2022") == (0.04125, "4.125%")
    conversion = resolve_debt_rate_facts(
        (_candidate("conversion", role=DebtRateRole.CONVERSION_RELATED_RATE, value=0.04125),),
        requested_reporting_date=REPORTING_DATE,
    )
    assert select_debt_detail_rate(conversion) is None


def test_different_instruments_and_basis_scopes_remain_separate() -> None:
    resolved = resolve_debt_rate_facts(
        (
            _candidate("instrument-a"),
            _candidate("instrument-b", instrument="instrument:other"),
            _candidate("segment", basis_scope="segment"),
        ),
        requested_reporting_date=REPORTING_DATE,
    )
    assert len(resolved) == 3


def test_real_gpre_fy2019_rate_is_period_owned_and_footnote_is_not_a_rate() -> None:
    rows = _parse_financial_statement_debt_table_html(_registered_gpre_fy2019(), REPORTING_DATE)
    row = next(
        item
        for item in rows
        if item["issuer_instrument_label"] == "4.125 % convertible notes due 2022 (2)"
    )
    assert row["amount"] == 149_256_000.0
    assert row["comparative_amount"] == 142_708_000.0
    assert row["rate_value"] == 0.04125
    assert row["rate_display"] == "4.125%"
    assert row["rate_role"] == DebtRateRole.COUPON_STATED_RATE.value
    assert row["rate_reporting_date"] == "2019-12-31"
    assert row["rate_fact_id"] == "ct-nonFraction-1d2f4ccc-dafa-4e8a-8450-07ca46e53792"
    assert "ct-nonFraction-c1d2eb3e-67f2-43fe-84e3-8a971473d8f1" not in row["rate_fact_ids"]
    assert all(record["rate_value"] != 0.02 for record in row["debt_rate_facts"])


def test_debt_rate_semantics_are_generic_and_versioned() -> None:
    import pbi_xbrl.debt_rate_semantics as module

    source = inspect.getsource(module).lower()
    assert DEBT_RATE_SEMANTIC_OWNERSHIP_VERSION == "v1_role_period_authority"
    assert 'ticker == "gpre"' not in source
    assert "fy2019" not in source
    assert "convertiblenotes4.125due2022" not in source
