from __future__ import annotations

import json
from pathlib import Path

from pbi_xbrl.json_schema_validation import (
    DuplicateJsonKeyError,
    SUPPORTED_SCHEMA_KEYWORDS,
    load_json_strict,
    schema_keywords,
    validate_json_schema,
)
from pbi_xbrl.new_ticker_issue_ledger import build_canonical_issue_ledger
from pbi_xbrl.normalized_company_data_validation import validate_normalized_company_data_schema


ROOT = Path(__file__).resolve().parents[1]
PACKAGE_PATH = ROOT / "tests" / "fixtures" / "normalized_packages" / "TEST_minimal_valid.json"
NORMALIZED_SCHEMA = ROOT / "docs" / "normalized_company_data.schema.json"
LEDGER_SCHEMA = ROOT / "docs" / "new_ticker_issue_ledger.schema.json"
MANIFEST = ROOT / "docs" / "standard_template_shell_manifest.json"
MANIFEST_SCHEMA = ROOT / "docs" / "standard_template_shell_manifest.schema.json"
BINDING_MAP = ROOT / "docs" / "workbook_binding_map.json"
BINDING_SCHEMA = ROOT / "docs" / "workbook_binding_map.schema.json"
PLAN_SCHEMA = ROOT / "docs" / "new_ticker_binding_plan.schema.json"
AUDIT_RECEIPT_SCHEMA = ROOT / "docs" / "standard_template_audit_receipt.schema.json"
MODULE_MANIFEST = ROOT / "docs" / "workbook_module_manifest.json"
MODULE_SCHEMA = ROOT / "docs" / "workbook_module_manifest.schema.json"


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_every_checked_in_schema_keyword_is_supported() -> None:
    for path in (
        NORMALIZED_SCHEMA,
        LEDGER_SCHEMA,
        MANIFEST_SCHEMA,
        BINDING_SCHEMA,
        PLAN_SCHEMA,
        AUDIT_RECEIPT_SCHEMA,
        MODULE_SCHEMA,
    ):
        used = schema_keywords(_load(path))
        unsupported = {keyword for keyword in used if keyword not in SUPPORTED_SCHEMA_KEYWORDS and not keyword.startswith("x-")}
        assert not unsupported, (path, sorted(unsupported))


def test_checked_in_manifest_and_binding_contracts_match_their_schemas() -> None:
    assert validate_json_schema(_load(MANIFEST), _load(MANIFEST_SCHEMA)) == []
    assert validate_json_schema(_load(BINDING_MAP), _load(BINDING_SCHEMA)) == []
    assert validate_json_schema(_load(MODULE_MANIFEST), _load(MODULE_SCHEMA)) == []


def test_strict_json_loader_rejects_duplicate_keys(tmp_path: Path) -> None:
    path = tmp_path / "duplicate.json"
    path.write_text('{"status":"PASS","status":"FAIL"}', encoding="utf-8")

    try:
        load_json_strict(path)
    except DuplicateJsonKeyError:
        pass
    else:  # pragma: no cover - explicit fail-closed assertion
        raise AssertionError("Duplicate JSON keys were accepted.")


def test_normalized_schema_enforces_minimum_unique_items_and_real_dates() -> None:
    package = _load(PACKAGE_PATH)
    guidance = package["normalized_guidance"]["items"][0]
    guidance["display_priority"] = 0
    guidance["supersedes_evidence_keys"] = ["duplicate-evidence", "duplicate-evidence"]
    guidance["publication_date"] = "2026-99-99"
    guidance["source_date"] = "2026-02-31"

    rules = {issue.rule_id for issue in validate_normalized_company_data_schema(package)}

    assert "normalized_schema_minimum" in rules
    assert "normalized_schema_uniqueItems" in rules
    assert "normalized_schema_format" in rules


def test_issue_ledger_schema_enforces_const_and_nonnegative_counts() -> None:
    ledger = build_canonical_issue_ledger()
    ledger["version"] = "9.9.9"
    ledger["summary"]["canonical_unique_issue_count"] = -1

    rules = {issue.rule_id for issue in validate_normalized_company_data_schema(ledger, schema_path=LEDGER_SCHEMA)}

    assert "normalized_schema_const" in rules
    assert "normalized_schema_minimum" in rules


def test_composition_bounds_lengths_and_formats_are_enforced() -> None:
    schema = {
        "type": "object",
        "required": ["n", "items", "text", "stamp", "uri", "choice"],
        "properties": {
            "n": {
                "type": "number",
                "minimum": 1,
                "maximum": 9,
                "exclusiveMinimum": 1,
                "exclusiveMaximum": 9,
            },
            "items": {"type": "array", "minItems": 1, "maxItems": 2, "uniqueItems": True},
            "text": {"type": "string", "minLength": 2, "maxLength": 4, "pattern": "^[A-Z]+$"},
            "stamp": {"type": "string", "format": "date-time"},
            "uri": {"type": "string", "format": "uri"},
            "choice": {
                "allOf": [{"type": "integer"}, {"minimum": 1}],
                "oneOf": [{"const": 1}, {"const": 2}],
                "not": {"const": 3},
            },
        },
        "additionalProperties": False,
    }
    bad = {
        "n": 1,
        "items": ["x", "x", "z"],
        "text": "abcde",
        "stamp": "2026-07-11 12:00:00",
        "uri": "not a uri",
        "choice": 3,
        "extra": True,
    }

    keywords = {keyword for _path, keyword, _message in validate_json_schema(bad, schema)}

    assert {
        "exclusiveMinimum",
        "maxItems",
        "uniqueItems",
        "maxLength",
        "pattern",
        "format",
        "oneOf",
        "not",
        "additionalProperties",
    } <= keywords


def test_anyof_and_maximum_accept_valid_contract() -> None:
    schema = {
        "anyOf": [{"type": "integer", "maximum": 5}, {"type": "string", "maxLength": 3}],
    }

    assert validate_json_schema(5, schema) == []
    assert validate_json_schema("abc", schema) == []
    assert any(keyword == "anyOf" for _path, keyword, _message in validate_json_schema(6, schema))


def test_all_numeric_boundary_keywords_reject_adversarial_values() -> None:
    schema = {
        "type": "object",
        "properties": {
            "below": {"type": "number", "minimum": 1},
            "above": {"type": "number", "maximum": 9},
            "at_low_edge": {"type": "number", "exclusiveMinimum": 1},
            "at_high_edge": {"type": "number", "exclusiveMaximum": 9},
        },
    }
    failures = validate_json_schema(
        {"below": 0, "above": 10, "at_low_edge": 1, "at_high_edge": 9},
        schema,
    )

    assert {keyword for _path, keyword, _message in failures} == {
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
    }
