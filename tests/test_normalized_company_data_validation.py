from __future__ import annotations

from pbi_xbrl.normalized_company_data_validation import (
    build_normalized_text_quality_audit,
    validate_normalized_company_data,
)
from pbi_xbrl.new_ticker_guidance_scope import normalize_guidance_scope


def _field(value, *, status: str = "populated", source_ref: str = "fixture", core: bool = False, reason: str = "", unit: str = "", period: str = ""):
    out = {"value": value, "status": status, "source_ref": source_ref, "core": core}
    if reason:
        out["reason"] = reason
    if unit:
        out["unit"] = unit
    if period:
        out["period"] = period
    return out


def _base_package() -> dict:
    calculation_history = []
    for ordinal in range(8097, 8106):
        year, quarter_index = divmod(ordinal, 4)
        period = f"{year}-Q{quarter_index + 1}"
        revenue = 100.0 if period == "2026-Q1" else 110.0 if period == "2026-Q2" else float(80 + ordinal - 8097)
        calculation_history.append(
            {"period": period, "period_ordinal": ordinal, "metric": "revenue", "value": revenue, "unit": "$m", "source_ref": f"fixture:{period}:revenue", "status": "populated"}
        )
    calculation_history.extend(
        [
            {"period": "2026-Q1", "period_ordinal": 8104, "metric": "diluted_shares", "value": 50.0, "unit": "m shares", "source_ref": "fixture:2026-Q1:diluted_shares", "status": "populated"},
            {"period": "2026-Q2", "period_ordinal": 8105, "metric": "diluted_shares", "value": 51.0, "unit": "m shares", "source_ref": "fixture:2026-Q2:diluted_shares", "status": "populated"},
        ]
    )
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
                    "fiscal_year": 2026,
                    "fiscal_quarter": 1,
                    "revenue": _field(100.0, core=True, unit="$m", period="2026-Q1"),
                    "diluted_shares": _field(50.0, core=True, unit="m shares", period="2026-Q1"),
                },
                {
                    "period": "2026-Q2",
                    "fiscal_year": 2026,
                    "fiscal_quarter": 2,
                    "revenue": _field(110.0, core=True, unit="$m", period="2026-Q2"),
                    "diluted_shares": _field(51.0, core=True, unit="m shares", period="2026-Q2"),
                },
            ]
        },
        "calculation_history": {"quarterly_items": calculation_history},
        "annual_financials": {
            "rows": [
                {
                    "period": "2025-FY",
                    "fiscal_year": 2025,
                    "revenue": _field(400.0, core=True, unit="$m", period="2025-FY"),
                }
            ]
        },
        "debt_liquidity": {"net_debt": _field(10.0, core=True)},
        "capital_returns": {"buybacks": _field(0.0, core=False)},
        "normalized_guidance": {"items": []},
        "promise_progress": {
            "items": [],
            "scorecard_items": [],
            "scorecard_disposition": "No source-backed scorecard is available in this fixture.",
        },
        "segments": {"items": []},
        "operating_drivers": {"items": []},
        "quarter_notes": {"items": []},
        "investment_case": {
            "summary": _field("Source-backed differentiated case.", core=True),
            "key_debate": _field("Whether execution sustains durable cash generation.", core=True),
        },
        "valuation_outputs": {"items": []},
        "source_coverage": {"sources": []},
        "mapping_gaps": [],
        "manual_review_flags": [],
    }


def _quarter_note(commentary: str, *, source_ref: str = "fixture:quarter-note") -> dict:
    return {
        "theme": _field("Test theme", source_ref=source_ref),
        "quarter": _field("2026-Q1", source_ref=source_ref),
        "metric": _field("Narrative", source_ref=source_ref),
        "commentary": _field(commentary, source_ref=source_ref, core=True),
        "why_it_matters": _field("This is a validation fixture.", source_ref=source_ref),
        "model_implication": _field("Review the source-backed implication.", source_ref=source_ref),
        "source_display": _field("Validation fixture", source_ref=source_ref),
        "source": source_ref,
        "evidence_refs": [source_ref],
        "evidence_key": "fixture-quarter-note",
        "display_role": "current_note",
        "display_priority": 1,
        "review_state": "accepted",
    }


def _operating_driver(current_read: str, *, source_ref: str = "fixture:driver") -> dict:
    return {
        "topic": _field("Operating margin", source_ref=source_ref),
        "driver": _field("Operating margin", source_ref=source_ref),
        "driver_type": "margin",
        "period": "2026-Q1",
        "current_read": _field(current_read, source_ref=source_ref, core=True),
        "source": source_ref,
        "why_it_matters": _field("Margin quality affects earnings durability.", source_ref=source_ref),
        "evidence_key": "fixture-operating-driver",
        "display_role": "current_watchlist",
        "display_priority": 1,
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
    package["quarter_notes"]["items"].append(_quarter_note("Guidance signal in filing text: raw_json source_txt_file"))

    assert "parser_noise_snippet" in _rule_ids(package)


def test_validation_catches_compensation_governance_noise_in_quarter_notes() -> None:
    package = _base_package()
    package["quarter_notes"]["items"].append(
        _quarter_note(
            "The Company's compensation offerings include cash awards.",
            source_ref="ANF_10K.htm",
        )
    )

    assert "visible_text_quality_compensation_or_governance_noise" in _rule_ids(package)


def test_validation_catches_legal_boilerplate_in_visible_quarter_notes() -> None:
    package = _base_package()
    package["quarter_notes"]["items"].append(
        _quarter_note(
            "Risks related to the timing and implementation of changes to existing tariff programs.",
            source_ref="ANF_10K.htm",
        )
    )

    assert "visible_text_quality_boilerplate_or_legal" in _rule_ids(package)


def test_validation_catches_formula_definitions_as_operating_drivers() -> None:
    package = _base_package()
    package["operating_drivers"]["items"].append(
        _operating_driver("Gross profit divided by reported net sales.", source_ref="internal_metric")
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
    package["quarter_notes"]["items"].append(_quarter_note("Comparable sales improved because of"))

    assert "visible_text_quality_fragmented_sentence" in _rule_ids(package)


def test_text_quality_audit_and_validation_stay_in_sync() -> None:
    package = _base_package()
    package["operating_drivers"]["items"].append(
        _operating_driver("Operating income divided by reported net sales.")
    )

    audit = build_normalized_text_quality_audit(package)
    assert audit["non_clean_visible_count"] > 0
    assert validate_normalized_company_data(package)


def test_validation_catches_unexplained_empty_core_fields() -> None:
    package = _base_package()
    package["company_profile"]["company_name"] = _field("", status="populated", core=True)

    assert "unexplained_empty_core_field" in _rule_ids(package)


def test_validation_requires_distinct_strategic_context_and_numeric_revenue_mix() -> None:
    package = _base_package()
    package["company_profile"]["business_description"] = _field("Same narrative", core=True)
    package["company_profile"]["strategic_context"] = _field("Same narrative", core=True)
    package["company_profile"]["revenue_streams"] = [
        {
            "member": _field("Primary stream", core=True),
            "mix": _field("Narrative revenue model", core=True),
            "unit": "%",
            "period": "2025-FY",
            "source_ref": "fixture",
            "display_order": 1,
        }
    ]

    rules = _rule_ids(package)

    assert "strategic_context_not_distinct" in rules
    assert "revenue_stream_mix_not_numeric" in rules


def test_validation_rejects_cash_only_or_unreconciled_total_liquidity() -> None:
    package = _base_package()
    package["debt_liquidity"] = {
        "cash": _field(80.0, core=True, unit="$m", period="2026-03-31"),
        "total_debt": _field(None, status="missing_source", core=True, reason="Missing debt evidence."),
        "net_debt": _field(70.0, core=True, unit="$m", period="2026-03-31"),
        "revolver_availability": _field(None, status="missing_source", core=True, reason="Missing revolver evidence."),
        "liquidity_cash": _field(80.0, core=True, unit="$m", period="2026-03-31"),
        "other_available_liquidity": _field(None, status="not_applicable", reason="No other component."),
        "total_liquidity": _field(180.0, core=True, unit="$m", period="2026-03-31"),
        "liquidity_definition": _field("Cash plus revolver availability.", core=True),
        "as_of_date": _field("2026-03-31", core=True),
    }

    rules = _rule_ids(package)

    assert "cash_only_total_liquidity" in rules
    assert "total_liquidity_not_reconciled" in rules


def test_liquidity_freshness_contract_covers_current_stale_mixed_and_incomplete() -> None:
    package = _base_package()
    package["debt_liquidity"] = {
        "cash": _field(90.0, core=True, unit="$m", period="2026-05-02"),
        "total_debt": _field(20.0, core=True, unit="$m", period="2026-05-02"),
        "net_debt": _field(-70.0, core=True, unit="$m", period="2026-05-02"),
        "revolver_availability": _field(100.0, core=True, unit="$m", period="2026-03-31"),
        "liquidity_cash": _field(80.0, core=True, unit="$m", period="2026-03-31"),
        "other_available_liquidity": _field(None, status="not_applicable", reason="No other component."),
        "total_liquidity": _field(180.0, core=True, unit="$m", period="2026-03-31"),
        "liquidity_definition": _field("Cash plus undrawn revolver availability.", core=True),
        "as_of_date": _field("2026-03-31", core=True),
        "summary_as_of_date": _field("2026-05-02", core=True),
        "summary_liquidity_display": _field(180.0, core=True, unit="$m", period="2026-03-31"),
        "summary_liquidity_as_of_display": _field("As of 2026-03-31 (stale)", core=True, period="2026-03-31"),
        "liquidity_freshness": {
            "disposition": "stale_but_displayable_with_date",
            "summary_as_of": "2026-05-02",
            "liquidity_as_of": "2026-03-31",
            "component_as_of": {"cash": "2026-03-31", "revolver": "2026-03-31"},
            "mixed_date_components": False,
            "reason": "Older than SUMMARY but visibly dated.",
            "source_ref": "fixture",
        },
    }

    stale_rules = _rule_ids(package)
    assert "stale_liquidity_date_not_visible" not in stale_rules
    assert "liquidity_current_freshness_mismatch" not in stale_rules

    package["debt_liquidity"]["summary_liquidity_as_of_display"]["value"] = ""
    assert "stale_liquidity_date_not_visible" in _rule_ids(package)

    package["debt_liquidity"]["summary_liquidity_as_of_display"]["value"] = "As of 2026-03-31 (stale)"
    package["debt_liquidity"]["liquidity_freshness"]["mixed_date_components"] = True
    assert "liquidity_mixed_date_components" in _rule_ids(package)

    package["debt_liquidity"]["total_liquidity"] = _field(
        None,
        status="missing_source",
        core=True,
        reason="Same-date components unavailable.",
    )
    package["debt_liquidity"]["summary_liquidity_display"] = _field(
        None,
        status="missing_source",
        core=True,
        reason="No displayable total.",
    )
    package["debt_liquidity"]["summary_liquidity_as_of_display"] = _field(
        None,
        status="missing_source",
        core=True,
        reason="No displayable total date.",
    )
    package["debt_liquidity"]["liquidity_freshness"].update(
        disposition="incomplete_components",
        mixed_date_components=False,
    )
    incomplete_rules = _rule_ids(package)
    assert "incomplete_liquidity_is_populated" not in incomplete_rules
    assert "non_displayable_liquidity_has_visible_value" not in incomplete_rules


def test_liquidity_freshness_cross_field_dates_fail_closed() -> None:
    def package_for(
        *,
        summary_as_of: str = "2026-05-02",
        liquidity_as_of: str = "2026-03-31",
        disposition: str = "stale_but_displayable_with_date",
        visible_date: str | None = None,
        cash_as_of: str | None = None,
        revolver_as_of: str | None = None,
    ) -> dict:
        package = _base_package()
        cash_date = cash_as_of or liquidity_as_of
        revolver_date = revolver_as_of or liquidity_as_of
        display_date = visible_date or liquidity_as_of
        package["debt_liquidity"] = {
            "cash": _field(90.0, core=True, unit="$m", period=summary_as_of),
            "total_debt": _field(20.0, core=True, unit="$m", period=summary_as_of),
            "net_debt": _field(-70.0, core=True, unit="$m", period=summary_as_of),
            "revolver_availability": _field(100.0, core=True, unit="$m", period=revolver_date),
            "liquidity_cash": _field(80.0, core=True, unit="$m", period=cash_date),
            "other_available_liquidity": _field(None, status="not_applicable", reason="No other component."),
            "total_liquidity": _field(180.0, core=True, unit="$m", period=liquidity_as_of),
            "liquidity_definition": _field("Cash plus undrawn revolver availability.", core=True),
            "as_of_date": _field(liquidity_as_of, core=True),
            "summary_as_of_date": _field(summary_as_of, core=True),
            "summary_liquidity_display": _field(180.0, core=True, unit="$m", period=liquidity_as_of),
            "summary_liquidity_as_of_display": _field(
                f"As of {display_date}", core=True, period=liquidity_as_of
            ),
            "liquidity_freshness": {
                "disposition": disposition,
                "summary_as_of": summary_as_of,
                "liquidity_as_of": liquidity_as_of,
                "component_as_of": {"cash": cash_date, "revolver": revolver_date},
                "mixed_date_components": cash_date != revolver_date,
                "reason": "Fixture freshness contract.",
                "source_ref": "fixture",
            },
        }
        return package

    valid_current = package_for(
        summary_as_of="2026-05-02",
        liquidity_as_of="2026-05-02",
        disposition="current",
    )
    assert not {
        "liquidity_current_freshness_mismatch",
        "liquidity_future_date_invalid",
        "liquidity_visible_date_mismatch",
    } & _rule_ids(valid_current)

    current_without_visible_date = package_for(
        summary_as_of="2026-05-02",
        liquidity_as_of="2026-05-02",
        disposition="current",
    )
    current_without_visible_date["debt_liquidity"]["summary_liquidity_as_of_display"] = _field(
        None,
        status="missing_source",
        core=True,
        reason="Visible display unavailable.",
    )
    assert "current_liquidity_date_not_visible" in _rule_ids(current_without_visible_date)

    valid_stale = package_for()
    assert not {
        "liquidity_current_freshness_mismatch",
        "liquidity_future_date_invalid",
        "liquidity_visible_date_mismatch",
        "stale_liquidity_date_not_visible",
    } & _rule_ids(valid_stale)

    future_stale = package_for(liquidity_as_of="2026-06-01")
    assert "liquidity_future_date_invalid" in _rule_ids(future_stale)

    stale_marked_current = package_for(disposition="current")
    assert "liquidity_current_freshness_mismatch" in _rule_ids(stale_marked_current)

    wrong_visible_date = package_for(visible_date="2026-02-28")
    assert "liquidity_visible_date_mismatch" in _rule_ids(wrong_visible_date)

    mixed_dates = package_for(cash_as_of="2026-03-31", revolver_as_of="2026-02-28")
    assert "liquidity_mixed_date_components" in _rule_ids(mixed_dates)

    conflicting_authoritative_date = package_for()
    conflicting_authoritative_date["debt_liquidity"]["liquidity_freshness"]["liquidity_as_of"] = "2026-02-28"
    assert "liquidity_as_of_conflict" in _rule_ids(conflicting_authoritative_date)


def test_validation_rejects_latest_current_horizon_guidance_marked_history() -> None:
    package = _base_package()
    package["normalized_guidance"]["items"] = [
        {
            "metric": _field("Capex", core=True),
            "value": _field("$100 million", core=True),
            "horizon": _field("2026 year", core=True),
            "source_excerpt": "Capital expenditures are expected to be approximately $100 million.",
            "source_date": "2026-01-31",
            "publication_date": "2026-03-04",
            "stated_in_period": "2025-Q4",
            "classification": "normalized_outlook",
            "evidence_key": "fixture-guidance-capex",
            "display_role": "history",
            "display_priority": 999,
            "update_stage": "initial",
        }
    ]

    assert "current_guidance_visibility_misclassified" in _rule_ids(package)


def test_validation_rejects_superseded_old_guidance_marked_current() -> None:
    package = _base_package()
    package["normalized_guidance"]["items"] = [
        {
            "metric": _field("Revenue", core=True),
            "value": _field("Up low-single digits", core=True),
            "horizon": _field("FY2025", core=True),
            "source_excerpt": "Revenue was expected to increase in fiscal 2025.",
            "source_date": "2024-12-31",
            "publication_date": "2025-02-20",
            "stated_in_period": "2024-Q4",
            "classification": "normalized_outlook",
            "evidence_key": "fixture-guidance-fy2025",
            "display_role": "current_primary",
            "display_priority": 1,
            "update_stage": "initial",
        },
        {
            "metric": _field("Revenue", core=True),
            "value": _field("Up mid-single digits", core=True),
            "horizon": _field("FY2026", core=True),
            "source_excerpt": "Revenue is expected to increase in fiscal 2026.",
            "source_date": "2025-12-31",
            "publication_date": "2026-03-04",
            "stated_in_period": "2025-Q4",
            "classification": "normalized_outlook",
            "evidence_key": "fixture-guidance-fy2026",
            "display_role": "current_primary",
            "display_priority": 1,
            "update_stage": "initial",
        },
    ]

    assert "stale_guidance_visibility_misclassified" in _rule_ids(package)


def test_guidance_alias_horizons_share_one_scope_and_old_row_cannot_remain_current() -> None:
    package = _base_package()
    older = {
        "metric": _field("Revenue", core=True),
        "value": _field("Up low-single digits", core=True),
        "horizon": _field("2026 year", core=True),
        "source_excerpt": "Revenue was expected to increase in fiscal 2026.",
        "source_date": "2025-09-30",
        "publication_date": "2025-11-20",
        "stated_in_period": "2025-Q3",
        "classification": "normalized_outlook",
        "evidence_key": "fixture-guidance-old-2026",
        "display_role": "current_primary",
        "display_priority": 1,
        "update_stage": "initial",
    }
    newer = {
        **older,
        "value": _field("Up mid-single digits", core=True),
        "horizon": _field("FY2026", core=True),
        "source_date": "2025-12-31",
        "publication_date": "2026-03-04",
        "stated_in_period": "2025-Q4",
        "evidence_key": "fixture-guidance-new-2026",
        "update_stage": "update",
    }
    package["normalized_guidance"]["items"] = [older, newer]

    assert normalize_guidance_scope(older).scope_key == normalize_guidance_scope(newer).scope_key
    assert normalize_guidance_scope(older).horizon == "FY2026"
    assert "stale_guidance_visibility_misclassified" in _rule_ids(package)


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
