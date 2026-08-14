from __future__ import annotations

import copy
import dataclasses
import hashlib
import json
import re
import shutil
from pathlib import Path

import pytest
import pandas as pd

import pbi_xbrl.anf_debt_source_adapter as debt_adapter
from pbi_xbrl.anf_debt_source_adapter import (
    ANF_DEBT_EVIDENCE_ADAPTER_ID,
    DebtSourceFactMissing,
    _abl_subsection,
    _borrowings_note,
    _context_dates,
    _ix_amount_fact,
    _soup,
    anf_debt_extraction_to_legacy_revolver_history,
    build_anf_legacy_revolver_history,
    build_anf_debt_collections,
    parse_anf_debt_filing,
)
from pbi_xbrl.company_profiles import get_company_profile
import pbi_xbrl.debt_source_registry as debt_source_registry
from pbi_xbrl.debt_source_registry import (
    DebtEvidenceAdapter,
    DebtEvidenceRoutingError,
    merge_source_native_revolver_history,
    resolve_profile_debt_revolver_history,
)
from pbi_xbrl.debt_sheet_visibility import DEBT_PROFILE_ECONOMIC_VALIDATION_ATTR
from pbi_xbrl.new_ticker_debt_scope import (
    DebtResolutionError,
    resolve_debt_facilities,
    select_latest_debt_facilities,
)
from pbi_xbrl.new_ticker_style_planner import reproduce_style_plan
from pbi_xbrl.normalized_company_data_validation import (
    validate_normalized_company_data,
    validate_normalized_company_data_schema,
)
from scripts.build_anf_shadow_normalized_package import (
    _default_data_root,
    _default_workbook_path,
    build_anf_normalized_package,
)


ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = _default_data_root()
SEC_CACHE = DATA_ROOT / "sec_cache" / "ANF"
MAY_SOURCE = SEC_CACHE / "doc_000101884026000036_anf-20260502.htm"
JAN_SOURCE = SEC_CACHE / "doc_000101884026000012_anf-20260131.htm"
MAY_CONTEXT_REF = "c-4"
RESTRICTED_CASH_CONCEPT = "us-gaap:RestrictedCashAndCashEquivalentsNoncurrent"
RESTRICTED_CASH_ALIASES = (
    RESTRICTED_CASH_CONCEPT,
    "us-gaap:RestrictedCashEquivalentsNoncurrent",
)


@pytest.fixture(scope="module")
def extraction():
    return build_anf_debt_collections(SEC_CACHE)


@pytest.fixture(scope="module")
def package():
    return build_anf_normalized_package(
        data_root=DATA_ROOT,
        workbook_path=_default_workbook_path(DATA_ROOT),
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _facility(extraction, period: str):
    matches = [row for row in extraction.facilities if row["as_of_date"] == period]
    assert len(matches) == 1
    return matches[0]


def _instrument(extraction, period: str):
    matches = [row for row in extraction.instruments if row["as_of_date"] == period]
    assert len(matches) == 1
    return matches[0]


def _debt_disclosure_fixture(*, toc_after: bool = False, duplicate_fact: bool = False) -> bytes:
    toc = "<div>4. BORROWINGS</div>"
    fact = (
        '<div><span>10. <ix:nonnumeric id="debt-fact" '
        'name="us-gaap:DebtDisclosureTextBlock" continuedat="debt-cont">'
        "BORROWINGS</ix:nonnumeric></span></div>"
    )
    if duplicate_fact:
        fact += (
            '<div><span>11. <ix:nonnumeric id="duplicate-debt-fact" '
            'name="us-gaap:DebtDisclosureTextBlock" continuedat="duplicate-debt-cont">'
            "BORROWINGS</ix:nonnumeric></span></div>"
            '<ix:continuation id="duplicate-debt-cont">'
            "ABL Facility The duplicate facility. "
            "Representations, warranties and covenants"
            "</ix:continuation>"
        )
    continuation = (
        '<ix:continuation id="debt-cont">'
        "The ABL Facility is described elsewhere in this note. "
        "ABL Facility The Credit Agreement provides for a $500 million revolving credit facility. "
        "Representations, warranties and covenants The remainder is outside the section."
        "</ix:continuation>"
    )
    body = fact + continuation + toc if toc_after else toc + fact + continuation
    return f"<html><body>{body}</body></html>".encode("utf-8")


def _concept_fact_pattern(concept: str) -> re.Pattern[str]:
    return re.compile(
        rf"<ix:nonfraction\b(?=[^>]*\bname=[\"']{re.escape(concept)}[\"'])[^>]*>.*?</ix:nonfraction>",
        re.I | re.S,
    )


def _fact_text(tag: str) -> str:
    open_end = tag.find(">")
    close_start = tag.casefold().rfind("</ix:nonfraction>")
    assert open_end >= 0 and close_start > open_end
    return tag[open_end + 1 : close_start]


def _replace_fact_text(tag: str, value: str) -> str:
    open_end = tag.find(">")
    close_start = tag.casefold().rfind("</ix:nonfraction>")
    assert open_end >= 0 and close_start > open_end
    return tag[: open_end + 1] + value + tag[close_start:]


def _replace_fact_attribute(tag: str, attribute: str, value: str) -> str:
    pattern = re.compile(rf"\b{re.escape(attribute)}\s*=\s*([\"']).*?\1", re.I)
    assert len(pattern.findall(tag)) == 1
    return pattern.sub(f'{attribute}="{value}"', tag, count=1)


def _exact_fact(html: str, concept: str, *, context_ref: str = MAY_CONTEXT_REF) -> str:
    context_pattern = re.compile(rf"\bcontextref\s*=\s*[\"']{re.escape(context_ref)}[\"']", re.I)
    matches = [match.group(0) for match in _concept_fact_pattern(concept).finditer(html) if context_pattern.search(match.group(0))]
    assert len(matches) == 1
    return matches[0]


def _temporary_source(tmp_path: Path, html: str) -> Path:
    output = tmp_path / MAY_SOURCE.name
    output.write_bytes(html.encode("utf-8"))
    accession = MAY_SOURCE.name.split("_")[1]
    index_source = MAY_SOURCE.with_name(f"index_{accession}.json")
    shutil.copyfile(index_source, tmp_path / index_source.name)
    return output


def _parse_rule(tmp_path: Path, html: str) -> DebtResolutionError:
    with pytest.raises(DebtResolutionError) as exc_info:
        parse_anf_debt_filing(_temporary_source(tmp_path, html))
    return exc_info.value


def test_primary_source_hashes_are_exact() -> None:
    assert _sha256(MAY_SOURCE) == "4bb925d6957c71e2760bc9d6e09bd88d43253a8199c0259b0480d223ed2e3079"
    assert _sha256(JAN_SOURCE) == "2024444bc693471e53fb6c5c464c60eaeba7b3e797d75e1ffed4eb801af176fd"


def test_borrowings_note_uses_unique_xbrl_identity_not_toc_source_order() -> None:
    before_number, before_note = _borrowings_note(
        _soup(_debt_disclosure_fixture()),
        source_path=Path("toc-before.htm"),
    )
    after_number, after_note = _borrowings_note(
        _soup(_debt_disclosure_fixture(toc_after=True)),
        source_path=Path("toc-after.htm"),
    )
    assert before_number == after_number == 10
    assert before_note == after_note
    assert before_note.startswith("10. BORROWINGS")


def test_duplicate_debt_disclosure_facts_fail_closed_before_section_selection() -> None:
    with pytest.raises(DebtResolutionError) as exc_info:
        _borrowings_note(
            _soup(_debt_disclosure_fixture(duplicate_fact=True)),
            source_path=Path("duplicate-fact.htm"),
        )
    assert exc_info.value.rule_id == "anf_debt_note_identity_conflict"
    assert exc_info.value.context["matching_fact_count"] == 2
    assert exc_info.value.context["source_path"] == "duplicate-fact.htm"


def test_abl_subsection_ignores_prose_mentions_and_requires_one_canonical_heading() -> None:
    _, note = _borrowings_note(
        _soup(_debt_disclosure_fixture()),
        source_path=Path("unique-heading.htm"),
    )
    subsection = _abl_subsection(note, source_path=Path("unique-heading.htm"))
    assert subsection.startswith("ABL Facility The Credit Agreement")
    assert "described elsewhere" not in subsection
    assert "Representations, warranties and covenants" not in subsection

    duplicate_heading = note.replace(
        "Representations, warranties and covenants",
        "ABL Facility On amendment, the duplicate section remained active. "
        "Representations, warranties and covenants",
    )
    with pytest.raises(DebtResolutionError) as exc_info:
        _abl_subsection(duplicate_heading, source_path=Path("duplicate-heading.htm"))
    assert exc_info.value.rule_id == "anf_debt_abl_section_conflict"
    assert exc_info.value.context["matching_heading_count"] == 2
    assert exc_info.value.context["source_path"] == "duplicate-heading.htm"


def test_parse_boundary_demotes_only_a_genuinely_absent_optional_companion(tmp_path: Path) -> None:
    html = MAY_SOURCE.read_text(encoding="utf-8")
    for concept in RESTRICTED_CASH_ALIASES:
        html = _concept_fact_pattern(concept).sub(lambda match: _fact_text(match.group(0)), html)
    soup = _soup(html.encode("utf-8"))
    with pytest.raises(DebtSourceFactMissing) as exc_info:
        _ix_amount_fact(
            soup,
            concepts=RESTRICTED_CASH_ALIASES,
            as_of_date="2026-05-02",
            source_path=Path("missing-restricted-cash.htm"),
        )
    assert exc_info.value.rule_id == "anf_debt_xbrl_fact_missing"

    parsed = parse_anf_debt_filing(_temporary_source(tmp_path, html))
    facility = parsed["facility"]
    assert facility["source_status"] == "accepted"
    assert facility["restricted_cash"]["value"] is None
    assert facility["restricted_cash"]["status"] == "missing_source"
    assert facility["cash_and_equivalents"]["status"] == "populated"
    assert facility["net_availability"]["value"] == 449.531


def test_parse_boundary_missing_draw_disclosure_remains_not_reported(tmp_path: Path) -> None:
    html = MAY_SOURCE.read_text(encoding="utf-8")
    original = "did not have any borrowings outstanding"
    assert html.count(original) >= 1
    html = html.replace(original, "did not disclose borrowings outstanding")
    facility = parse_anf_debt_filing(_temporary_source(tmp_path, html))["facility"]
    assert facility["source_status"] == "accepted"
    assert facility["drawn_status"] == "not_reported"
    assert facility["drawn_balance"]["value"] is None
    assert facility["drawn_balance"]["status"] == "missing_source"
    assert facility["net_availability"]["value"] == 449.531


def test_parse_boundary_conflicting_scale_blocks_instead_of_demoting(tmp_path: Path) -> None:
    html = MAY_SOURCE.read_text(encoding="utf-8")
    fact = _exact_fact(html, RESTRICTED_CASH_CONCEPT)
    conflicting = _replace_fact_attribute(fact, "scale", "6")
    error = _parse_rule(tmp_path, html.replace(fact, fact + conflicting, 1))
    assert type(error) is DebtResolutionError
    assert error.rule_id == "anf_debt_xbrl_scale_conflict"
    assert error.context["scale"] == "6"


def test_parse_boundary_incompatible_unit_blocks_instead_of_demoting(tmp_path: Path) -> None:
    html = MAY_SOURCE.read_text(encoding="utf-8")
    fact = _exact_fact(html, RESTRICTED_CASH_CONCEPT)
    incompatible = _replace_fact_attribute(fact, "unitref", "shares")
    error = _parse_rule(tmp_path, html.replace(fact, incompatible, 1))
    assert type(error) is DebtResolutionError
    assert error.rule_id == "anf_debt_xbrl_unit_conflict"
    assert error.context["unit_ref"] == "shares"


def test_parse_boundary_malformed_numeric_fact_blocks(tmp_path: Path) -> None:
    html = MAY_SOURCE.read_text(encoding="utf-8")
    fact = _exact_fact(html, RESTRICTED_CASH_CONCEPT)
    malformed = _replace_fact_text(fact, "not-a-number")
    error = _parse_rule(tmp_path, html.replace(fact, malformed, 1))
    assert type(error) is DebtResolutionError
    assert error.rule_id == "anf_debt_xbrl_numeric_malformed"
    assert error.context["raw_text"] == "not-a-number"


def test_parse_boundary_conflicting_numeric_facts_block(tmp_path: Path) -> None:
    html = MAY_SOURCE.read_text(encoding="utf-8")
    fact = _exact_fact(html, RESTRICTED_CASH_CONCEPT)
    conflicting = _replace_fact_text(fact, "8,336")
    error = _parse_rule(tmp_path, html.replace(fact, fact + conflicting, 1))
    assert type(error) is DebtResolutionError
    assert error.rule_id == "anf_debt_xbrl_fact_conflict"
    assert error.context["values"] == [7336.0, 8336.0]


def test_parse_boundary_distinct_compatible_context_identity_blocks(tmp_path: Path) -> None:
    html = MAY_SOURCE.read_text(encoding="utf-8")
    context_pattern = re.compile(
        rf"<xbrli:context\b(?=[^>]*\bid=[\"']{MAY_CONTEXT_REF}[\"'])[^>]*>.*?</xbrli:context>",
        re.I | re.S,
    )
    contexts = context_pattern.findall(html)
    assert len(contexts) == 1
    duplicate_context = re.sub(
        rf"\bid=[\"']{MAY_CONTEXT_REF}[\"']",
        'id="duplicate-current-context"',
        contexts[0],
        count=1,
        flags=re.I,
    )
    html = html.replace(contexts[0], contexts[0] + duplicate_context, 1)
    fact = _exact_fact(html, RESTRICTED_CASH_CONCEPT)
    duplicate_fact = _replace_fact_attribute(fact, "contextref", "duplicate-current-context")
    error = _parse_rule(tmp_path, html.replace(fact, fact + duplicate_fact, 1))
    assert type(error) is DebtResolutionError
    assert error.rule_id == "anf_debt_xbrl_duplicate_identity"
    assert len(error.context["candidate_identities"]) == 2


def test_parse_boundary_incompatible_period_context_blocks(tmp_path: Path) -> None:
    html = MAY_SOURCE.read_text(encoding="utf-8")
    contexts = _context_dates(_soup(html.encode("utf-8")), source_path=MAY_SOURCE)
    prior_contexts = sorted(
        context_ref
        for context_ref, (instant, has_dimensions) in contexts.items()
        if instant != "2026-05-02" and not has_dimensions
    )
    assert prior_contexts
    fact = _exact_fact(html, RESTRICTED_CASH_CONCEPT)
    wrong_period = _replace_fact_attribute(fact, "contextref", prior_contexts[0])
    error = _parse_rule(tmp_path, html.replace(fact, wrong_period, 1))
    assert type(error) is DebtResolutionError
    assert error.rule_id == "anf_debt_xbrl_context_conflict"
    assert error.context["as_of_date"] == "2026-05-02"


def test_parse_boundary_is_independent_of_fact_traversal_order(monkeypatch: pytest.MonkeyPatch) -> None:
    baseline = parse_anf_debt_filing(MAY_SOURCE)
    original_soup = debt_adapter._soup

    def reversed_soup(raw_html: bytes):
        soup = original_soup(raw_html)
        original_find_all = soup.find_all

        def reversed_find_all(*args, **kwargs):
            return list(reversed(original_find_all(*args, **kwargs)))

        soup.find_all = reversed_find_all
        return soup

    monkeypatch.setattr(debt_adapter, "_soup", reversed_soup)
    assert parse_anf_debt_filing(MAY_SOURCE) == baseline


@pytest.mark.parametrize(
    ("period", "expected"),
    [
        (
            "2026-05-02",
            {
                "commitment": 500.0,
                "loan_cap": 500.0,
                "drawn_balance": 0.0,
                "letters_of_credit": 0.469,
                "gross_capacity": 499.531,
                "minimum_excess_availability": 50.0,
                "net_availability": 449.531,
                "cash_and_equivalents": 594.08,
                "restricted_cash": 7.336,
                "same_date_liquidity": 1043.611,
            },
        ),
        (
            "2026-01-31",
            {
                "commitment": 500.0,
                "loan_cap": 500.0,
                "drawn_balance": 0.0,
                "letters_of_credit": 0.454,
                "gross_capacity": 499.546,
                "minimum_excess_availability": 50.0,
                "net_availability": 449.546,
                "cash_and_equivalents": 759.54,
                "restricted_cash": 7.376,
                "same_date_liquidity": 1209.086,
            },
        ),
    ],
)
def test_exact_facility_cash_and_liquidity_oracles(extraction, period: str, expected: dict[str, float]) -> None:
    facility = _facility(extraction, period)
    assert {key: facility[key]["value"] for key in expected} == expected
    assert facility["drawn_status"] == "reported_zero"
    assert facility["same_date_liquidity"]["derivation"] == (
        "cash_and_equivalents + net_availability; restricted cash excluded"
    )


@pytest.mark.parametrize(
    ("period", "current", "noncurrent", "total"),
    [
        ("2026-05-02", 262.316, 1030.161, 1292.477),
        ("2026-01-31", 241.265, 926.83, 1168.095),
    ],
)
def test_exact_operating_lease_oracles(extraction, period: str, current: float, noncurrent: float, total: float) -> None:
    instrument = _instrument(extraction, period)
    assert instrument["current_balance"]["value"] == current
    assert instrument["noncurrent_balance"]["value"] == noncurrent
    assert instrument["balance"]["value"] == total
    assert instrument["aggregation_role"] == "excluded_from_core_debt"


def test_all_thirteen_abl_periods_reconcile_and_latest_twelve_are_exact(extraction) -> None:
    expected = {
        "2023-04-29": (400.0, 345.995, 0.0, 0.610, 345.385, 34.600, 310.785),
        "2023-07-29": (400.0, 397.087, 0.0, 0.435, 396.652, 39.709, 356.943),
        "2023-10-28": (400.0, 400.0, 0.0, 0.422, 399.578, 40.0, 359.578),
        "2024-02-03": (400.0, 332.891, 0.0, 0.440, 332.451, 33.289, 299.162),
        "2024-05-04": (400.0, 325.648, 0.0, 0.430, 325.218, 32.565, 292.653),
        "2024-08-03": (500.0, 478.787, None, 0.427, 478.360, 47.878, 430.482),
        "2024-11-02": (500.0, 500.0, None, 0.443, 499.557, 50.0, 449.557),
        "2025-02-01": (500.0, 500.0, 0.0, 0.423, 499.577, 50.0, 449.577),
        "2025-05-03": (500.0, 477.358, 0.0, 0.415, 476.943, 47.736, 429.207),
        "2025-08-02": (500.0, 500.0, 0.0, 0.452, 499.548, 50.0, 449.548),
        "2025-11-01": (500.0, 500.0, 0.0, 0.454, 499.546, 50.0, 449.546),
        "2026-01-31": (500.0, 500.0, 0.0, 0.454, 499.546, 50.0, 449.546),
        "2026-05-02": (500.0, 500.0, 0.0, 0.469, 499.531, 50.0, 449.531),
    }
    actual = {
        row["as_of_date"]: (
            row["commitment"]["value"],
            row["loan_cap"]["value"],
            row["drawn_balance"]["value"],
            row["letters_of_credit"]["value"],
            row["gross_capacity"]["value"],
            row["minimum_excess_availability"]["value"],
            row["net_availability"]["value"],
        )
        for row in extraction.facilities
    }
    assert actual == expected
    assert [row.as_of_date for row in select_latest_debt_facilities(extraction.facilities)] == list(expected)[1:]
    assert all(
        row["drawn_balance"]["value"] is None
        for row in extraction.facilities
        if row["drawn_status"] == "not_reported"
    )


def test_resolved_history_is_independent_of_source_row_order(extraction) -> None:
    forward = [row.to_dict() for row in resolve_debt_facilities(list(extraction.facilities))]
    reverse = [row.to_dict() for row in resolve_debt_facilities(list(reversed(extraction.facilities)))]
    assert forward == reverse


def test_source_table_scale_period_publication_and_lineage_are_explicit(extraction) -> None:
    may = _facility(extraction, "2026-05-02")
    jan = _facility(extraction, "2026-01-31")
    assert may["loan_cap"]["source_value"] == 500_000.0
    assert may["loan_cap"]["source_scale"] == "thousands"
    assert may["commitment"]["source_scale"] == "millions"
    assert may["publication_date"] == "2026-06-05"
    assert jan["publication_date"] == "2026-03-26"
    assert may["source_document_sha256"] == _sha256(MAY_SOURCE)
    assert jan["source_document_sha256"] == _sha256(JAN_SOURCE)
    assert may["source_table_scope"] == "borrowings_capacity_table"
    assert may["net_availability"]["source_row_ref"] == "table[87]:row[6]"
    assert all(row["evidence_key"] and row["evidence_refs"] and row["source_refs"] for row in extraction.facilities)


def test_credit_notes_are_exact_bounded_source_sentences(extraction) -> None:
    assert extraction.credit_notes
    assert {row["source_table_scope"] for row in extraction.credit_notes} == {"borrowings_note"}
    assert all(row["text"].endswith(".") for row in extraction.credit_notes)
    assert all(row["reason"] == "Exact bounded BORROWINGS-note sentence; no narrative scoring applied." for row in extraction.credit_notes)
    may_notes = [row for row in extraction.credit_notes if row["as_of_date"] == "2026-05-02"]
    assert [row["note_type"] for row in may_notes] == ["covenant_compliance", "facility_draw_status"]
    assert all("#note[10]:sentence[" in row["source_ref"] for row in may_notes)


def test_package_builder_preserves_scalars_and_corrects_only_q1_revolver_identity(package) -> None:
    q1 = next(row for row in package["quarterly_financials"]["rows"] if row["period"] == "2026-Q1")
    assert q1["revolver_availability"]["value"] == 449.531
    assert q1["revolver_availability"]["as_of_date"] == "2026-05-02"
    assert q1["revolver_availability"]["source_scale"] == "thousands"
    assert q1["revolver_availability"]["source_document_sha256"] == _sha256(MAY_SOURCE)
    assert package["debt_liquidity"]["revolver_availability"]["value"] == 449.546
    assert package["debt_liquidity"]["revolver_availability"]["period"] == "2026-01-31"
    assert package["debt_liquidity"]["total_liquidity"]["value"] == 1209.086
    assert package["debt_liquidity"]["total_liquidity"]["period"] == "2026-01-31"
    assert validate_normalized_company_data_schema(package) == []
    assert validate_normalized_company_data(package) == []


def _digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def test_exact_plan_and_style_regression_reconciles_3a2_additions(package) -> None:
    binding_payload = json.loads((ROOT / "docs" / "workbook_binding_map.json").read_text(encoding="utf-8"))
    manifest = json.loads((ROOT / "docs" / "standard_template_shell_manifest.json").read_text(encoding="utf-8"))
    value_plan, style_plan = reproduce_style_plan(
        package,
        binding_payload=binding_payload,
        manifest=manifest,
        shell_path=ROOT / "templates" / "standard_stock_model_template.xlsx",
    )
    plan = value_plan.to_dict()
    styles = style_plan.to_dict()
    assert plan["status"] == "PASS"
    assert plan["planned_write_count"] == 23_613
    assert plan["structured_skip_count"] == 2_006
    assert plan["overflow_count"] == 0
    assert plan["issue_ledger"]["summary"]["canonical_unique_issue_count"] == 755
    assert plan["issue_ledger"]["summary"]["detailed_occurrence_count"] == 2_311
    assert plan["issue_ledger"]["summary"]["blocking_issue_count"] == 0
    m95 = [
        row
        for row in plan["planned_writes"]
        if row["target_sheet"] == "Valuation" and row["target_cell"] == "M95"
    ]
    assert len(m95) == 1
    assert m95[0]["value"] == 449.531
    assert "doc_000101884026000036_anf-20260502.htm" in m95[0]["source_ref"]
    debt_product_sheets = {
        "Debt_Profile",
        "Revolver_History",
        "Leverage_Liquidity",
        "Debt_Credit_Notes",
        "Debt_Maturity_Ladder",
        "Debt_Tranches_Latest",
        "Debt_Tranches_Q",
        "Debt_Buckets",
        "Debt_Recon",
    }
    capital_return_binding_ids = {
        "valuation_capital_return_latest_quarter_header",
        "valuation_capital_return_ttm_header",
        "valuation_capital_return_annual_header",
        "valuation_capital_return_product_rows",
        "valuation_capital_return_support_rows",
    }
    capital_return_writes = [
        row for row in plan["planned_writes"] if row["binding_id"] in capital_return_binding_ids
    ]
    hidden_value_binding_ids = {
        "hidden_value_base_rows",
        "hidden_value_audit_rows",
        "hidden_value_recompute_rows",
        "hidden_value_flags_rows",
        "hidden_value_valuation_rows",
    }
    hidden_value_writes = [
        row for row in plan["planned_writes"] if row["binding_id"] in hidden_value_binding_ids
    ]
    unaffected = [
        row
        for row in plan["planned_writes"]
        if row["target_sheet"] not in debt_product_sheets
        and row["binding_id"] != "ic_product_projection_rows"
        and row["binding_id"] not in capital_return_binding_ids
        and row["binding_id"] not in hidden_value_binding_ids
        and not (row["target_sheet"] == "Valuation" and row["target_cell"] == "M95")
    ]
    assert len(unaffected) == 20_239
    assert len(capital_return_writes) == 240
    assert len(hidden_value_writes) == 1_983
    assert _digest(unaffected) == "1c81bac2ac94ed0c44fbf8d19ef9761d362889b99753acab9754754975c37ab2"
    assert _digest(hidden_value_writes) == "4aba88a2839defcc7f1c58867ae7b80ca7198132592e57f89d7a16cc3383a6c6"
    debt_product_writes = [
        row for row in plan["planned_writes"] if row["target_sheet"] in debt_product_sheets
    ]
    assert len(debt_product_writes) == 389
    assert _digest(debt_product_writes) == "774cc923de372f915599414a60dcd10d52832c746f6c6d73e3275caa7fcef57f"
    investment_case_writes = [
        row for row in plan["planned_writes"] if row["binding_id"] == "ic_product_projection_rows"
    ]
    assert len(investment_case_writes) == 761
    assert _digest(investment_case_writes) == "24837ba4d3a5c297a053838ea5ed234a266b9682a5a5f299210864ee098d27b6"
    assert _digest(plan["issue_ledger"]) == "6371c550feb51c5aea91f32bec18d393a65316788cf304351e500ad89799e8d6"
    assert styles["action_count"] == 770
    assert styles["decision_count"] == 1_298
    debt_policy_ids = {
        "debt_profile_product_state",
        "revolver_history_product_state",
        "leverage_liquidity_product_state",
        "debt_credit_notes_product_state",
        "debt_maturity_product_state",
    }
    hidden_value_policy_ids = {"hidden_value_audit_candidate_state"}
    prior_actions = [
        row
        for row in styles["actions"]
        if row["policy_id"] not in debt_policy_ids | hidden_value_policy_ids
    ]
    prior_decisions = [
        row
        for row in styles["decisions"]
        if row["policy_id"] not in debt_policy_ids | hidden_value_policy_ids
    ]
    assert _digest({"actions": prior_actions, "decisions": prior_decisions}) == (
        "e79336c75c282544b8cd8e210c1d6b9e5a351c18c8d04d06f1ca49517a719161"
    )
    hidden_value_actions = [
        row for row in styles["actions"] if row["policy_id"] in hidden_value_policy_ids
    ]
    hidden_value_decisions = [
        row for row in styles["decisions"] if row["policy_id"] in hidden_value_policy_ids
    ]
    assert (len(hidden_value_actions), len(hidden_value_decisions)) == (6, 7)
    assert _digest(
        {"actions": hidden_value_actions, "decisions": hidden_value_decisions}
    ) == "e4d555e505a2fc16406f5392e8538ee6645179b9f159a152a676f5ddfa4415e4"
    debt_actions = [row for row in styles["actions"] if row["policy_id"] in debt_policy_ids]
    debt_decisions = [row for row in styles["decisions"] if row["policy_id"] in debt_policy_ids]
    assert len(debt_actions) == 17
    assert len(debt_decisions) == 37
    assert _digest({"actions": debt_actions, "decisions": debt_decisions}) == (
        "f21653ede9603e06d05d76ba6e2c99e390be5d6f5dcc625d66004023f2e1fd43"
    )


def test_duplicate_debt_identity_blocks_public_package_validation(package) -> None:
    mutated = copy.deepcopy(package)
    duplicate = copy.deepcopy(mutated["debt_liquidity"]["facilities"][-1])
    duplicate["facility_id"] = "ANF ABL-Facility"
    duplicate["source_row_ref"] = "table[87]:duplicate-row"
    mutated["debt_liquidity"]["facilities"][-1]["facility_id"] = "anf abl facility"
    mutated["debt_liquidity"]["facilities"].append(duplicate)
    issues = validate_normalized_company_data(mutated, validate_schema=False)
    matches = [issue for issue in issues if issue.rule_id == "duplicate_debt_business_identity"]
    assert len(matches) == 1
    assert matches[0].business_row_key == "facility|anf_abl_facility|2026-05-02"
    assert "table[87]:duplicate-row" in matches[0].message


def test_anf_adapter_is_local_only_and_does_not_use_items_zero_selection() -> None:
    source = (ROOT / "pbi_xbrl" / "anf_debt_source_adapter.py").read_text(encoding="utf-8")
    assert "items.0" not in source
    assert "except DebtResolutionError:" not in source
    assert "requests" not in source
    assert "urlopen" not in source
    assert "httpx" not in source


def test_anf_legacy_bridge_preserves_exact_usd_values_and_per_field_lineage(extraction) -> None:
    history = anf_debt_extraction_to_legacy_revolver_history(extraction)
    assert len(history) == 13
    jan = history.loc[history["quarter"].eq(pd.Timestamp("2026-01-31"))].iloc[0]
    may = history.loc[history["quarter"].eq(pd.Timestamp("2026-05-02"))].iloc[0]
    assert (
        jan["revolver_commitment"],
        jan["revolver_facility_size"],
        jan["revolver_drawn"],
        jan["revolver_letters_of_credit"],
        jan["revolver_availability"],
    ) == (500_000_000.0, 500_000_000.0, 0.0, 454_000.0, 449_546_000.0)
    assert (
        may["revolver_commitment"],
        may["revolver_facility_size"],
        may["revolver_drawn"],
        may["revolver_letters_of_credit"],
        may["revolver_availability"],
    ) == (500_000_000.0, 500_000_000.0, 0.0, 469_000.0, 449_531_000.0)
    assert may["source_document_accession"] == "0001018840-26-000036"
    assert may["source_document_sha256"] == _sha256(MAY_SOURCE)
    assert "key=sec-accession-0001018840-26-000036" in may["source_document_id"]
    assert may["publication_date"] == "2026-06-05"
    assert may["commitment_source_type"] == "text"
    assert may["facility_source_type"] == "table"
    assert may["drawn_source_type"] == "text"
    assert may["lc_source_type"] == "table"
    assert may["availability_source_type"] == "table"
    assert may["commitment_evidence_classification"] == "source_backed_fact"
    assert may["availability_evidence_classification"] == "source_backed_fact"
    assert may["liquidity_evidence_classification"] == "source_backed_calculation"
    assert may["drawn_status"] == "reported_zero"
    validation = history.attrs[DEBT_PROFILE_ECONOMIC_VALIDATION_ATTR]
    assert validation.passed
    assert validation.subject_ids == ("anf_abl_facility",)
    assert validation.as_of_date == "2026-05-02"


def test_anf_full_local_legacy_builder_resolves_complete_current_evidence() -> None:
    history = build_anf_legacy_revolver_history(SEC_CACHE)
    assert list(history["quarter"].dt.strftime("%Y-%m-%d")) == list(
        debt_adapter.ANF_EXPECTED_ABL_PERIODS
    )
    latest = history.iloc[-1]
    assert latest["revolver_letters_of_credit"] == 469_000.0
    assert latest["revolver_availability"] == 449_531_000.0
    assert history.attrs[DEBT_PROFILE_ECONOMIC_VALIDATION_ATTR].passed


def test_profile_owned_debt_adapter_isolated_from_other_and_unknown_tickers(extraction) -> None:
    history = anf_debt_extraction_to_legacy_revolver_history(extraction)
    adapter = DebtEvidenceAdapter(ANF_DEBT_EVIDENCE_ADAPTER_ID, lambda _root: history.copy())
    assert get_company_profile("ANF").debt_evidence_adapter_id == ANF_DEBT_EVIDENCE_ADAPTER_ID
    resolved = resolve_profile_debt_revolver_history(
        ticker="ANF", cache_root=SEC_CACHE, adapters=(adapter,)
    )
    assert len(resolved) == 13
    for ticker in ("PBI", "GPRE", "FRESHCO"):
        assert get_company_profile(ticker).debt_evidence_adapter_id == ""
        assert resolve_profile_debt_revolver_history(
            ticker=ticker, cache_root=SEC_CACHE, adapters=(adapter,)
        ).empty


def test_unknown_and_duplicate_debt_adapter_ownership_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    unknown_profile = dataclasses.replace(
        get_company_profile("ANF"), debt_evidence_adapter_id="debt-source-adapter:unknown@1"
    )
    monkeypatch.setattr(debt_source_registry, "get_company_profile", lambda _ticker: unknown_profile)
    with pytest.raises(DebtEvidenceRoutingError, match="unknown debt evidence adapter"):
        resolve_profile_debt_revolver_history(ticker="ANF", cache_root=tmp_path)

    duplicate = DebtEvidenceAdapter(ANF_DEBT_EVIDENCE_ADAPTER_ID, lambda _root: pd.DataFrame())
    with pytest.raises(DebtEvidenceRoutingError, match="Duplicate debt evidence adapter"):
        resolve_profile_debt_revolver_history(
            ticker="ANF",
            cache_root=tmp_path,
            adapters=(duplicate, duplicate),
        )


def test_source_native_revolver_merge_is_order_independent_and_preserves_validation(extraction) -> None:
    overlay = anf_debt_extraction_to_legacy_revolver_history(extraction)
    base = pd.DataFrame(
        [
            {
                "quarter": pd.Timestamp("2026-01-31"),
                "revolver_commitment": 1.0,
                "revolver_availability": 2.0,
                "unrelated_direct_metric": 7.0,
            },
            {
                "quarter": pd.Timestamp("2022-12-31"),
                "revolver_commitment": 300_000_000.0,
                "unrelated_direct_metric": 8.0,
            },
        ]
    )
    forward = merge_source_native_revolver_history(base, overlay)
    reverse_overlay = overlay.iloc[::-1].copy()
    reverse_overlay.attrs.update(overlay.attrs)
    reverse = merge_source_native_revolver_history(base, reverse_overlay)
    pd.testing.assert_frame_equal(forward, reverse)
    jan = forward.loc[forward["quarter"].eq(pd.Timestamp("2026-01-31"))].iloc[0]
    assert jan["revolver_commitment"] == 500_000_000.0
    assert jan["revolver_availability"] == 449_546_000.0
    assert jan["unrelated_direct_metric"] == 7.0
    assert forward.attrs[DEBT_PROFILE_ECONOMIC_VALIDATION_ATTR].passed


def test_generic_runtime_contains_no_anf_abl_literal_or_missing_to_zero_fallback() -> None:
    sources = {
        path.name: path.read_text(encoding="utf-8")
        for path in (
            ROOT / "pbi_xbrl" / "pipeline_orchestration.py",
            ROOT / "pbi_xbrl" / "excel_writer_core.py",
            ROOT / "pbi_xbrl" / "excel_writer_valuation_history_grid_render.py",
        )
    }
    joined = "\n".join(sources.values())
    for forbidden in (
        "anf_abl_row",
        "anf_abl_q",
        "449_546_000.0",
        "454_000.0",
        'debt_core_map[q] = 0.0',
        '"10-K debt note"',
    ):
        assert forbidden not in joined
    assert "resolve_profile_debt_revolver_history" in sources["pipeline_orchestration.py"]
    assert "merge_source_native_revolver_history" in sources["pipeline_orchestration.py"]
