from __future__ import annotations

from pbi_xbrl.normalized_company_data_validation import (
    build_normalized_text_quality_audit,
    validate_normalized_company_data,
)


def _field(value, *, status: str = "populated", source_ref: str = "fixture", core: bool = False, reason: str = ""):
    out = {"value": value, "status": status, "source_ref": source_ref, "core": core}
    if reason:
        out["reason"] = reason
    return out


def _base_package() -> dict:
    return {
        "ticker_metadata": {"ticker": _field("TEST", core=True)},
        "company_profile": {
            "company_name": _field("Test Co", core=True),
            "sector": _field("Industrial", core=True),
            "allowed_sector_terms": [],
        },
        "quarterly_financials": {
            "rows": [
                {
                    "period": "2026-Q1",
                    "revenue": _field(100.0, core=True),
                    "diluted_shares": _field(50.0, core=True),
                },
                {
                    "period": "2026-Q2",
                    "revenue": _field(110.0, core=True),
                    "diluted_shares": _field(51.0, core=True),
                },
            ]
        },
        "annual_financials": {"rows": []},
        "debt_liquidity": {"net_debt": _field(10.0, core=True)},
        "capital_returns": {"buybacks": _field(0.0, core=False)},
        "normalized_guidance": {"items": []},
        "segments": {"items": []},
        "operating_drivers": {"items": []},
        "quarter_notes": {"items": []},
        "investment_case": {"summary": _field("Source-backed differentiated case.", core=True)},
        "source_coverage": {"sources": []},
        "mapping_gaps": [],
        "manual_review_flags": [],
    }


def _rule_ids(package: dict, **kwargs) -> set[str]:
    return {issue.rule_id for issue in validate_normalized_company_data(package, **kwargs)}


def test_validation_catches_guidance_metric_misclassification() -> None:
    package = _base_package()
    package["normalized_guidance"]["items"].append(
        {
            "metric": _field("Net sales", core=True),
            "value": _field("$100m net income", core=True),
            "source_excerpt": "Management guided net income to approximately $100m.",
        }
    )

    assert "guidance_metric_misclassification" in _rule_ids(package)


def test_validation_catches_boilerplate_guidance() -> None:
    package = _base_package()
    package["normalized_guidance"]["items"].append(
        {
            "metric": _field("Revenue", core=True),
            "value": _field("", status="manual_review_required", core=True),
            "source_excerpt": "Forward-looking statements may differ materially and no duty to update.",
        }
    )

    assert "boilerplate_guidance" in _rule_ids(package)


def test_validation_catches_parser_noise_snippets() -> None:
    package = _base_package()
    package["quarter_notes"]["items"].append(
        {"period": "2026-Q1", "note": _field("Guidance signal in filing text: raw_json source_txt_file", core=True)}
    )

    assert "parser_noise_snippet" in _rule_ids(package)


def test_validation_catches_compensation_governance_noise_in_quarter_notes() -> None:
    package = _base_package()
    package["quarter_notes"]["items"].append(
        {
            "period": "2026-Q1",
            "note": _field(
                "The Company’s compensation offerings include cash-",
                source_ref="ANF_10K.htm",
                core=True,
            ),
        }
    )

    assert "visible_text_quality_compensation_or_governance_noise" in _rule_ids(package)


def test_validation_catches_legal_boilerplate_in_visible_quarter_notes() -> None:
    package = _base_package()
    package["quarter_notes"]["items"].append(
        {
            "period": "2026-Q1",
            "note": _field(
                "Risks related to the timing and implementation of changes to existing tariff programs.",
                source_ref="ANF_10K.htm",
                core=True,
            ),
        }
    )

    assert "visible_text_quality_boilerplate_or_legal" in _rule_ids(package)


def test_validation_catches_formula_definitions_as_operating_drivers() -> None:
    package = _base_package()
    package["operating_drivers"]["items"].append(
        {
            "driver": _field("Gross margin", core=True),
            "current_read": _field("Gross profit divided by reported net sales.", source_ref="internal_metric", core=True),
        }
    )

    assert "visible_text_quality_accounting_policy_or_definition" in _rule_ids(package)


def test_validation_catches_release_headers_as_segment_notes() -> None:
    package = _base_package()
    package["segments"]["items"].append(
        {
            "segment": _field("Americas", core=True),
            "metric": "revenue",
            "note": "Document ABERCROMBIE & FITCH CO. REPORTS THIRD QUARTER FISCAL 2025 RESULTS",
        }
    )

    assert "visible_text_quality_release_header_or_source_title" in _rule_ids(package)


def test_validation_catches_broken_visible_text_fragments() -> None:
    package = _base_package()
    package["quarter_notes"]["items"].append(
        {"period": "2026-Q1", "note": _field("Comparable sales improved because of", core=True)}
    )

    assert "visible_text_quality_fragmented_sentence" in _rule_ids(package)


def test_text_quality_audit_and_validation_stay_in_sync() -> None:
    package = _base_package()
    package["operating_drivers"]["items"].append(
        {
            "driver": _field("Operating margin", core=True),
            "current_read": _field("Operating income divided by reported net sales.", core=True),
        }
    )

    audit = build_normalized_text_quality_audit(package)
    assert audit["non_clean_visible_count"] > 0
    assert validate_normalized_company_data(package)


def test_validation_catches_unexplained_empty_core_fields() -> None:
    package = _base_package()
    package["company_profile"]["company_name"] = _field("", status="populated", core=True)

    assert "unexplained_empty_core_field" in _rule_ids(package)


def test_validation_catches_share_count_outliers() -> None:
    package = _base_package()
    package["quarterly_financials"]["rows"][1]["diluted_shares"] = _field(650.0, core=True)

    assert "share_count_outlier" in _rule_ids(package)


def test_validation_catches_valuation_mapping_gaps() -> None:
    package = _base_package()
    binding_map = [
        {
            "sheet": "Valuation",
            "section": "Core valuation",
            "normalized_field": "quarterly_financials.rows.0.ebitda",
            "required": True,
        }
    ]

    assert "valuation_core_mapping_gap" in _rule_ids(package, binding_map=binding_map)


def test_validation_catches_placeholder_investment_case_during_promotion() -> None:
    package = _base_package()
    package["investment_case"]["summary"] = _field("Placeholder generic investment case.", core=True)

    assert "placeholder_investment_case" in _rule_ids(package, promotion_requested=True)


def test_validation_catches_unsupported_sector_specific_leakage() -> None:
    package = _base_package()
    package["operating_drivers"]["items"].append({"driver": _field("45Z crush margin support", core=True)})

    assert "unsupported_sector_specific_leakage" in _rule_ids(package)


def test_validation_issue_shape_is_structured() -> None:
    package = _base_package()
    package["company_profile"]["company_name"] = _field("", status="populated", core=True)

    issue = validate_normalized_company_data(package)[0].to_dict()

    assert {"severity", "rule_id", "field", "message", "source_ref", "suggested_action"} <= set(issue)
