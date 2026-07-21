from __future__ import annotations

import ast
import copy
from pathlib import Path

import pytest

from pbi_xbrl.new_ticker_debt_scope import (
    DebtResolutionError,
    dispositions_to_package_section,
    normalize_debt_currency_to_millions,
    resolve_debt_collections,
    resolve_debt_facilities,
)


ROOT = Path(__file__).resolve().parents[1]
SCOPE_MODULE = ROOT / "pbi_xbrl" / "new_ticker_debt_scope.py"


def _amount(
    value: float | None,
    as_of_date: str,
    *,
    status: str = "populated",
    derived: bool = False,
) -> dict[str, object]:
    return {
        "value": value,
        "status": status,
        "currency": "USD",
        "unit": "$m",
        "source_value": None if value is None or derived else value * 1_000,
        "source_unit": "USD",
        "source_scale": "not_applicable" if value is None or derived else "thousands",
        "as_of_date": as_of_date,
        "source_ref": f"fixture.htm#amount-{as_of_date}",
        "source_row_ref": f"table[1]:{as_of_date}",
        "evidence_refs": [f"fixture.htm#amount-{as_of_date}"],
        "evidence_classification": "source_backed_calculation" if derived else "unavailable" if value is None else "source_backed_fact",
        "derivation": "cash_and_equivalents + net_availability" if derived else "",
        "reason": "Source did not report this amount." if value is None else "",
        "core": False,
    }


def _facility(as_of_date: str = "2026-05-02", *, role: str = "current") -> dict[str, object]:
    return {
        "facility_id": "test_revolver",
        "facility_name": "Test revolver",
        "facility_type": "asset_based_revolver",
        "borrower": "Test borrower",
        "currency": "USD",
        "as_of_date": as_of_date,
        "publication_date": "2026-06-05" if as_of_date == "2026-05-02" else "2026-03-26",
        "period_role": role,
        "source_status": "accepted",
        "source_table_scope": "borrowings_capacity_table",
        "aggregation_role": "liquidity_capacity",
        "commitment": _amount(500.0, as_of_date),
        "loan_cap": _amount(500.0, as_of_date),
        "drawn_balance": _amount(0.0, as_of_date),
        "drawn_status": "reported_zero",
        "letters_of_credit": _amount(0.469, as_of_date),
        "gross_capacity": _amount(499.531, as_of_date),
        "minimum_excess_availability": _amount(50.0, as_of_date),
        "net_availability": _amount(449.531, as_of_date),
        "cash_and_equivalents": _amount(594.08, as_of_date),
        "restricted_cash": _amount(7.336, as_of_date),
        "same_date_liquidity": _amount(1043.611, as_of_date, derived=True),
        "facility_expiry_date": "2029-08-02",
        "evidence_key": f"test_revolver_{as_of_date.replace('-', '_')}",
        "evidence_refs": [f"fixture.htm#facility-{as_of_date}"],
        "source_refs": [f"fixture.htm#facility-{as_of_date}"],
        "source_row_ref": "table[1]:rows[2:6]",
        "source_document_sha256": "a" * 64,
        "reason": "Source-backed fixture.",
    }


def _instrument(
    *,
    instrument_id: str = "term_loan_a",
    instrument_type: str = "term_loan",
    aggregation_role: str = "core_debt",
) -> dict[str, object]:
    as_of_date = "2026-05-02"
    return {
        "instrument_id": instrument_id,
        "instrument_name": "Term loan A" if instrument_type != "operating_lease_liability" else "Operating leases",
        "instrument_type": instrument_type,
        "issuer": "Test issuer",
        "currency": "USD",
        "as_of_date": as_of_date,
        "publication_date": "2026-06-05",
        "period_role": "current",
        "source_status": "accepted",
        "source_table_scope": "debt_note_table",
        "aggregation_role": aggregation_role,
        "balance": _amount(100.0, as_of_date),
        "current_balance": _amount(10.0, as_of_date),
        "noncurrent_balance": _amount(90.0, as_of_date),
        "rate_type": "fixed",
        "reference_rate": "",
        "spread_bps": None,
        "effective_rate": 6.25,
        "maturity_date": "2030-05-02",
        "secured_status": "secured",
        "seniority": "senior",
        "evidence_key": "test_term_loan_a_2026_05_02",
        "evidence_refs": ["fixture.htm#term-loan"],
        "source_refs": ["fixture.htm#term-loan"],
        "source_row_ref": "table[2]:row[3]",
        "source_document_sha256": "b" * 64,
        "reason": "Source-backed fixture.",
    }


def _maturity(instrument_id: str = "term_loan_a") -> dict[str, object]:
    as_of_date = "2026-05-02"
    return {
        "maturity_id": "term_loan_a_2030",
        "instrument_id": instrument_id,
        "maturity_type": "contractual_principal",
        "due_date": "2030-05-02",
        "maturity_bucket": "fy2030",
        "currency": "USD",
        "as_of_date": as_of_date,
        "publication_date": "2026-06-05",
        "period_role": "current",
        "source_status": "accepted",
        "source_table_scope": "debt_maturity_table",
        "aggregation_role": "core_debt_maturity",
        "amount": _amount(100.0, as_of_date),
        "evidence_key": "test_term_loan_a_2030",
        "evidence_refs": ["fixture.htm#maturity"],
        "source_refs": ["fixture.htm#maturity"],
        "source_row_ref": "table[3]:row[2]",
        "source_document_sha256": "c" * 64,
        "reason": "Source-backed fixture.",
    }


def test_explicit_source_scale_controls_currency_normalization() -> None:
    assert normalize_debt_currency_to_millions(500_000, source_unit="USD", source_scale="thousands") == 500.0
    assert normalize_debt_currency_to_millions(500, source_unit="USD", source_scale="millions") == 500.0
    with pytest.raises(DebtResolutionError, match="source scale"):
        normalize_debt_currency_to_millions(500_000, source_unit="USD", source_scale="")


def test_facility_resolution_is_row_order_independent() -> None:
    historical = _facility("2026-01-31", role="historical")
    current = _facility()
    forward = [row.to_dict() for row in resolve_debt_facilities([historical, current])]
    reverse = [row.to_dict() for row in resolve_debt_facilities([current, historical])]
    assert forward == reverse
    assert [row["period_role"] for row in forward] == ["historical", "current"]


def test_canonical_duplicate_facility_aliases_fail_closed_with_lineage() -> None:
    first = _facility()
    second = copy.deepcopy(first)
    first["facility_id"] = "Test Facility"
    second["facility_id"] = "test-facility"
    second["source_row_ref"] = "table[1]:row[99]"
    with pytest.raises(DebtResolutionError) as caught:
        resolve_debt_facilities([first, second])
    assert caught.value.rule_id == "duplicate_debt_business_identity"
    assert caught.value.context["canonical_id"] == "test_facility"
    assert caught.value.context["business_key"] == "facility|test_facility|2026-05-02"
    assert caught.value.context["first_source_row_ref"] == "table[1]:rows[2:6]"
    assert caught.value.context["conflicting_source_row_ref"] == "table[1]:row[99]"


def test_zero_and_unavailable_drawn_states_remain_distinct() -> None:
    unreported = _facility()
    unreported["drawn_status"] = "not_reported"
    unreported["drawn_balance"] = _amount(None, "2026-05-02", status="missing_source")
    resolved = resolve_debt_facilities([unreported])[0]
    assert resolved.drawn_balance.value is None
    assert resolved.drawn_status == "not_reported"

    leaked_zero = copy.deepcopy(unreported)
    leaked_zero["drawn_balance"]["value"] = 0.0  # type: ignore[index]
    with pytest.raises(DebtResolutionError, match="cannot carry"):
        resolve_debt_facilities([leaked_zero])

    false_zero = copy.deepcopy(unreported)
    false_zero["drawn_status"] = "reported_zero"
    with pytest.raises(DebtResolutionError, match="reported-zero"):
        resolve_debt_facilities([false_zero])


def test_restricted_cash_is_excluded_from_same_date_liquidity() -> None:
    facility = _facility()
    resolved = resolve_debt_facilities([facility])[0]
    assert resolved.same_date_liquidity.value == resolved.cash_and_equivalents.value + resolved.net_availability.value
    assert resolved.restricted_cash.value == 7.336

    includes_restricted_cash = copy.deepcopy(facility)
    includes_restricted_cash["same_date_liquidity"] = _amount(1050.947, "2026-05-02", derived=True)
    with pytest.raises(DebtResolutionError, match="Same-date liquidity"):
        resolve_debt_facilities([includes_restricted_cash])


def test_pbi_style_tranche_and_maturity_resolve_as_core_debt() -> None:
    section = {
        "facilities": [],
        "instruments": [_instrument()],
        "maturities": [_maturity()],
        "credit_notes": [],
    }
    resolved = resolve_debt_collections(section)
    assert resolved["instruments"][0].aggregation_role == "core_debt"
    assert resolved["maturities"][0].instrument_id == "term_loan_a"
    package_rows = dispositions_to_package_section(resolved)
    assert package_rows["maturities"][0]["amount"]["value"] == 100.0


def test_operating_lease_and_facility_expiry_cannot_become_core_maturity() -> None:
    lease = _instrument(
        instrument_id="operating_lease_liabilities",
        instrument_type="operating_lease_liability",
        aggregation_role="excluded_from_core_debt",
    )
    maturity = _maturity("operating_lease_liabilities")
    section = {"facilities": [_facility()], "instruments": [lease], "maturities": [maturity], "credit_notes": []}
    with pytest.raises(DebtResolutionError) as caught:
        resolve_debt_collections(section)
    assert caught.value.rule_id == "debt_maturity_non_core_instrument"

    facility_expiry = _maturity("test_revolver")
    facility_expiry["maturity_id"] = "test_revolver_expiry"
    section = {"facilities": [_facility()], "instruments": [], "maturities": [facility_expiry], "credit_notes": []}
    with pytest.raises(DebtResolutionError) as caught:
        resolve_debt_collections(section)
    assert caught.value.rule_id == "debt_maturity_instrument_missing"


def test_gpre_style_failed_facility_reconciliation_blocks() -> None:
    facility = _facility()
    facility["gross_capacity"] = _amount(499.0, "2026-05-02")
    with pytest.raises(DebtResolutionError) as caught:
        resolve_debt_facilities([facility])
    assert caught.value.rule_id == "debt_facility_gross_capacity_mismatch"


def test_revolver_only_and_no_debt_fixtures_do_not_infer_core_debt() -> None:
    revolver_only = resolve_debt_collections(
        {"facilities": [_facility()], "instruments": [], "maturities": [], "credit_notes": []}
    )
    assert len(revolver_only["facilities"]) == 1
    assert revolver_only["instruments"] == ()
    assert resolve_debt_collections({"facilities": [], "instruments": [], "maturities": [], "credit_notes": []}) == {
        "facilities": (),
        "instruments": (),
        "maturities": (),
        "credit_notes": (),
    }


def test_missing_lineage_and_unknown_instrument_metadata_fail_closed() -> None:
    facility = _facility()
    facility["evidence_refs"] = []
    with pytest.raises(DebtResolutionError) as caught:
        resolve_debt_facilities([facility])
    assert caught.value.rule_id == "debt_lineage_missing"

    instrument = _instrument()
    instrument["rate_type"] = "probably floating"
    with pytest.raises(DebtResolutionError) as caught:
        resolve_debt_collections({"facilities": [], "instruments": [instrument], "maturities": [], "credit_notes": []})
    assert caught.value.rule_id == "unsupported_debt_vocabulary"


def test_generic_debt_scope_has_no_ticker_branch_or_items_zero_selection() -> None:
    source = SCOPE_MODULE.read_text(encoding="utf-8")
    assert "items.0" not in source
    module = ast.parse(source)
    assert all("ticker" not in ast.unparse(node.test).casefold() for node in ast.walk(module) if isinstance(node, ast.If))
