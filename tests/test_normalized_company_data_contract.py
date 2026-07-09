from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

REQUIRED_SECTIONS = [
    "ticker_metadata",
    "company_profile",
    "quarterly_financials",
    "annual_financials",
    "debt_liquidity",
    "capital_returns",
    "normalized_guidance",
    "segments",
    "operating_drivers",
    "quarter_notes",
    "investment_case",
    "source_coverage",
    "mapping_gaps",
    "manual_review_flags",
]

FIELD_STATUSES = {
    "populated",
    "missing_source",
    "missing_mapping",
    "not_applicable",
    "manual_review_required",
    "parser_conflict",
}


def test_normalized_contract_docs_and_schema_cover_required_sections() -> None:
    contract_doc = ROOT / "docs" / "normalized_company_data_contract.md"
    schema_path = ROOT / "docs" / "normalized_company_data.schema.json"

    contract_text = contract_doc.read_text(encoding="utf-8").lower()
    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    properties = schema["properties"]
    required = set(schema["required"])
    for section in REQUIRED_SECTIONS:
        assert section in properties
        assert section in required
        assert section.replace("_", " ") in contract_text or section in contract_text

    status_enum = set(schema["$defs"]["fieldStatus"]["enum"])
    assert status_enum == FIELD_STATUSES


def test_normalized_schema_has_explicit_core_structures() -> None:
    schema = json.loads((ROOT / "docs" / "normalized_company_data.schema.json").read_text(encoding="utf-8"))

    explicit_sections = [
        "ticker_metadata",
        "company_profile",
        "quarterly_financials",
        "annual_financials",
        "debt_liquidity",
        "normalized_guidance",
        "investment_case",
    ]
    for section in explicit_sections:
        assert "$ref" not in schema["properties"][section]

    assert "rows" in schema["properties"]["quarterly_financials"]["properties"]
    assert "rows" in schema["properties"]["annual_financials"]["properties"]
    assert "items" in schema["properties"]["normalized_guidance"]["properties"]
    assert "x-normalized-fields" in schema
    assert "quarterly_financials.rows.revenue" in schema["x-normalized-fields"]


def test_new_ticker_engine_audit_names_gtx_paths_that_must_not_be_copied() -> None:
    audit_path = ROOT / "docs" / "new_ticker_engine_audit.md"
    audit_text = audit_path.read_text(encoding="utf-8")

    required_phrases = [
        "gtx_content_quality.py",
        "workbook_template_scaffold.py",
        "post-render",
        "source/content failure modes",
        "must not be copied",
    ]
    for phrase in required_phrases:
        assert phrase in audit_text
