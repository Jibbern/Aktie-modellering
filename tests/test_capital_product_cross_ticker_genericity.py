from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import pytest

from pbi_xbrl.json_schema_validation import load_json_strict
from pbi_xbrl.longitudinal_memory.capital_allocation_return_product_expansion import (
    CAPITAL_ALLOCATION_OWNER_ROUTES,
    CAPITAL_RETURN_ACTIVITY_FAMILIES,
    build_capital_allocation_return_investor_product,
    capital_allocation_owner_routing_review,
    capital_return_activity_family_contract,
)
from pbi_xbrl.longitudinal_memory.valuation_guidance_net_share_polish import (
    derive_net_share_percentage_records,
)


ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = Path(r"C:\Users\Jibbe\Aktier\StockModelData")
PACKAGE_PATH = (
    DATA_ROOT
    / "outputs"
    / "stress_tests"
    / "ANF_new_ticker_engine"
    / "ANF_normalized_data_package.json"
)
BS_PRODUCT_PATH = ROOT / "tests" / "fixtures" / "summary_bs" / "anf_bs_segment_product.v1.json"
BS_SHADOW_PATH = ROOT / "tests" / "fixtures" / "summary_bs" / "anf_bs_segment_shadow.v1.json"


@pytest.fixture(scope="module")
def source_inputs() -> tuple[dict, dict, dict]:
    missing = [
        str(path)
        for path in (PACKAGE_PATH, BS_PRODUCT_PATH, BS_SHADOW_PATH)
        if not path.exists()
    ]
    if missing:
        pytest.skip(f"Accepted generic-product inputs are unavailable: {missing!r}")
    return (
        load_json_strict(PACKAGE_PATH),
        load_json_strict(BS_PRODUCT_PATH),
        load_json_strict(BS_SHADOW_PATH),
    )


def _profile(package: dict, metrics: set[str], *, activate_dividends: bool = False) -> dict:
    changed = deepcopy(package)
    records = [
        deepcopy(row)
        for row in changed["capital_returns"]["records"]
        if row["metric_id"] in metrics
    ]
    if activate_dividends:
        for row in records:
            if row["metric_id"] != "dividends_paid":
                continue
            row["value"] = {
                "quarter": 2.0,
                "year_to_date": 6.0,
                "ttm": 8.0,
                "annual": 7.0,
            }[row["period_type"]]
            row["status"] = "populated"
            row["source_classification"] = "source_native_numeric"
            row["reason"] = ""
    changed["capital_returns"]["records"] = records
    return changed


def _product(package: dict, product: dict, shadow: dict):
    return build_capital_allocation_return_investor_product(
        package=package,
        balance_sheet_product=product,
        balance_sheet_shadow=shadow,
    )


def _rows(rows) -> dict[str, dict]:
    return {str(row["row_key"]): row for row in rows}


def _net_share_support(package: dict) -> tuple[dict, dict]:
    by_identity = {
        (row["metric_id"], row["fiscal_period"], row["period_type"]): row
        for row in package["capital_returns"]["records"]
    }

    def support(section: str, periods: tuple[tuple[str, str], ...]) -> dict:
        bindings = []
        for index, (period, period_type) in enumerate(periods, start=1):
            record = by_identity[("net_share_reduction", period, period_type)]
            bindings.append(
                {
                    "display_period": period,
                    "period": period,
                    "source_identity": record["record_id"],
                    "source_ref": record["evidence_ref"],
                    "target_cell": f"Valuation!A{index}",
                    "value": record["value"],
                }
            )
        return {"bindings": bindings, "metric_id": "net_share_reduction", "section": section}

    return (
        support(
            "capital_return_summary",
            (
                ("2026-Q1", "quarter"),
                ("TTM through 2026-Q1", "ttm"),
                ("2025-FY", "annual"),
            ),
        ),
        support(
            "annual_capital_return_history",
            (("2024-FY", "annual"), ("2025-FY", "annual")),
        ),
    )


def _reticker(value, *, old: str = "anf", new: str = "xyz"):
    text = json.dumps(value, ensure_ascii=False)
    text = text.replace(old.upper(), new.upper()).replace(old, new)
    return json.loads(text)


def test_owner_routing_is_declarative_and_capital_return_independent(
    source_inputs: tuple[dict, dict, dict],
) -> None:
    package, bs_product, bs_shadow = deepcopy(source_inputs)
    package.pop("capital_returns", None)
    product = _product(package, bs_product, bs_shadow)
    allocation = _rows(product.capital_allocation_summary)
    assert set(allocation) == {"free_cash_flow", "capital_expenditures", "ending_net_cash"}
    assert [value["value"] for value in allocation["free_cash_flow"]["values"]] == pytest.approx(
        [-17.085, 416.047, 378.368]
    )
    assert [value["value"] for value in allocation["capital_expenditures"]["values"]] == pytest.approx(
        [61.341, 251.351, 240.774]
    )
    assert product.capital_return_summary == ()
    assert len(CAPITAL_ALLOCATION_OWNER_ROUTES) == 8
    routing = {row["metric_id"]: row for row in capital_allocation_owner_routing_review(package)}
    assert routing["free_cash_flow"]["classification"] == "CANONICAL_OWNER_AVAILABLE"
    assert routing["capital_expenditures"]["classification"] == "CANONICAL_OWNER_AVAILABLE"


def test_buyback_only_profile_remains_valid(source_inputs: tuple[dict, dict, dict]) -> None:
    package, bs_product, bs_shadow = source_inputs
    profile = _profile(package, set(CAPITAL_RETURN_ACTIVITY_FAMILIES["BUYBACK"]))
    product = _product(profile, bs_product, bs_shadow)
    assert product.annual_return_periods == ("2024-FY", "2025-FY")
    assert "repurchase_cash_program" in _rows(product.annual_capital_return_history)
    assert "dividends_paid" not in _rows(product.capital_return_summary)
    profile["capital_returns"]["records"] = [
        row
        for row in profile["capital_returns"]["records"]
        if not (
            row["metric_id"] == "cash_per_program_share"
            and row["period_type"] == "quarter"
        )
    ]
    assert _product(profile, bs_product, bs_shadow).annual_return_periods == (
        "2024-FY",
        "2025-FY",
    )


def test_dividend_only_profile_does_not_require_buyback_price(
    source_inputs: tuple[dict, dict, dict],
) -> None:
    package, bs_product, bs_shadow = source_inputs
    profile = _profile(package, {"dividends_paid"}, activate_dividends=True)
    product = _product(profile, bs_product, bs_shadow)
    assert product.derivation_review["annual_average_price"] == []
    assert product.annual_return_periods == ("2024-FY", "2025-FY")
    assert set(_rows(product.annual_capital_return_history)) == {"dividends_paid"}
    assert set(_rows(product.capital_return_summary)) == {"dividends_paid"}
    families = {row["activity_family"]: row for row in capital_return_activity_family_contract(profile)}
    assert families["DIVIDEND"]["is_available"] is True
    assert families["BUYBACK"]["is_relevant"] is False


def test_issuance_only_profile_drives_annual_history(
    source_inputs: tuple[dict, dict, dict],
) -> None:
    package, bs_product, bs_shadow = source_inputs
    profile = _profile(package, {"share_issuance_sbc"})
    product = _product(profile, bs_product, bs_shadow)
    assert product.annual_return_periods == ("2024-FY", "2025-FY")
    assert set(_rows(product.annual_capital_return_history)) == {"share_issuance_sbc"}
    assert set(_rows(product.capital_return_summary)) == {"share_issuance_sbc"}


def test_no_activity_does_not_fabricate_shareholder_return(
    source_inputs: tuple[dict, dict, dict],
) -> None:
    package, bs_product, bs_shadow = deepcopy(source_inputs)
    package.pop("capital_returns", None)
    product = _product(package, bs_product, bs_shadow)
    assert product.capital_return_summary == ()
    assert product.quarterly_capital_return_history == ()
    assert product.annual_capital_return_history == ()
    assert all(
        row["is_available"] is False
        for row in capital_return_activity_family_contract(package)
    )


def test_mixed_activity_families_compose_independently(
    source_inputs: tuple[dict, dict, dict],
) -> None:
    package, bs_product, bs_shadow = source_inputs
    profile = _profile(
        package,
        set(CAPITAL_RETURN_ACTIVITY_FAMILIES["BUYBACK"])
        | set(CAPITAL_RETURN_ACTIVITY_FAMILIES["DIVIDEND"])
        | {"share_issuance_sbc"},
        activate_dividends=True,
    )
    product = _product(profile, bs_product, bs_shadow)
    summary = _rows(product.capital_return_summary)
    assert {"repurchase_cash_program", "dividends_paid", "share_issuance_sbc"} <= set(summary)
    assert summary["dividends_paid"]["values"][0]["value"] == pytest.approx(2.0)


def test_generic_non_anf_share_period_resolution(
    source_inputs: tuple[dict, dict, dict],
) -> None:
    package, bs_product, bs_shadow = deepcopy(source_inputs)
    generic_product = _reticker(bs_product)
    generic_shadow = _reticker(bs_shadow)
    summary, annual = derive_net_share_percentage_records(
        support_records=_net_share_support(package),
        package=package,
        balance_sheet_product=generic_product,
        balance_sheet_shadow=generic_shadow,
    )
    bindings = [*summary["bindings"], *annual["bindings"]]
    by_period = {row["period"]: row for row in bindings}
    assert by_period["2026-Q1"]["denominator_period"] == "2025-Q4"
    assert by_period["TTM through 2026-Q1"]["denominator_period"] == "2025-Q1"
    assert by_period["2025-FY"]["denominator_period"] == "2024-Q4"
    assert by_period["2026-Q1"]["value"] == pytest.approx(0.574 / 45.005)
    assert by_period["TTM through 2026-Q1"]["value"] == pytest.approx(3.212 / 47.643)
    assert by_period["2025-FY"]["value"] == pytest.approx(4.730 / 49.735)


def test_missing_generic_opening_share_fails_closed(
    source_inputs: tuple[dict, dict, dict],
) -> None:
    package, bs_product, bs_shadow = deepcopy(source_inputs)
    generic_product = _reticker(bs_product)
    generic_shadow = _reticker(bs_shadow)
    generic_product["fields"] = [
        row
        for row in generic_product["fields"]
        if not (
            row.get("metric_key") == "shares_outstanding"
            and row.get("period_id") == "period:xyz:fy2025-q4@1"
        )
    ]
    summary, _ = derive_net_share_percentage_records(
        support_records=_net_share_support(package),
        package=package,
        balance_sheet_product=generic_product,
        balance_sheet_shadow=generic_shadow,
    )
    binding = next(row for row in summary["bindings"] if row["period"] == "2026-Q1")
    assert binding["status"] == "unavailable"
    assert binding["value"] is None


def test_no_ticker_specific_economic_branch_or_anf_period_literal() -> None:
    paths = (
        ROOT / "pbi_xbrl" / "new_ticker_capital_return.py",
        ROOT / "pbi_xbrl" / "longitudinal_memory" / "capital_allocation_return_product_expansion.py",
        ROOT / "pbi_xbrl" / "longitudinal_memory" / "valuation_guidance_net_share_polish.py",
    )
    source = "\n".join(path.read_text(encoding="utf-8") for path in paths)
    assert "period:anf:" not in source.casefold()
    assert 'ticker == "ANF"' not in source
    assert "ticker == 'ANF'" not in source
