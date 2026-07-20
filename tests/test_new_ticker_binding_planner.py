from __future__ import annotations

import ast
from copy import deepcopy
import json
from pathlib import Path

import pytest

import pbi_xbrl.new_ticker_binding_planner as planner_module
import pbi_xbrl.normalized_company_data_validation as validation_module
from pbi_xbrl.new_ticker_binding_planner import (
    BindingPlan,
    BindingPlanReproductionError,
    plan_standard_template_writes,
    reproduce_binding_plan_snapshot,
    compare_binding_plan_snapshot,
)
from pbi_xbrl.normalized_company_data_validation import (
    validate_normalized_company_data,
    validate_normalized_company_data_schema,
)
from pbi_xbrl.standard_template_shell_identity import compute_shell_identity, verify_shell_identity


ROOT = Path(__file__).resolve().parents[1]
PACKAGE_PATH = ROOT / "tests" / "fixtures" / "normalized_packages" / "TEST_minimal_valid.json"
BINDING_MAP_PATH = ROOT / "docs" / "workbook_binding_map.json"
MANIFEST_PATH = ROOT / "docs" / "standard_template_shell_manifest.json"
SHELL_PATH = ROOT / "templates" / "standard_stock_model_template.xlsx"


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _package() -> dict:
    return _load(PACKAGE_PATH)


def _bindings() -> dict:
    return _load(BINDING_MAP_PATH)


def _manifest() -> dict:
    return _load(MANIFEST_PATH)


def _plan(package: dict | None = None, *, bindings: dict | None = None, manifest: dict | None = None) -> BindingPlan:
    effective_bindings = bindings or _bindings()
    effective_manifest = manifest or _manifest()
    if bindings is not None or manifest is not None:
        effective_manifest = deepcopy(effective_manifest)
        effective_manifest["shell_identity"] = compute_shell_identity(
            SHELL_PATH,
            manifest=effective_manifest,
            binding_payload=effective_bindings,
        )
    identity_report = verify_shell_identity(
        SHELL_PATH,
        manifest=effective_manifest,
        binding_payload=effective_bindings,
    )
    return plan_standard_template_writes(
        package or _package(),
        binding_payload=effective_bindings,
        manifest=effective_manifest,
        shell_identity_report=identity_report,
    )


def test_real_manifest_requires_shell_identity_verification() -> None:
    plan = plan_standard_template_writes(
        _package(),
        binding_payload=_bindings(),
        manifest=_manifest(),
    )

    assert plan.status == "FAIL"
    assert "shell_identity_not_verified" in _rule_ids(plan)


def test_unknown_scenario_route_alias_blocks_before_any_binding_is_planned() -> None:
    package = _package()
    package["investment_case"]["scenario_items"] = [
        {
            "scenario_id": "base",
            "assumption_id": "Revenue Momentum",
            "metric": "Revenue growth",
            "value_kind": "point",
            "value": 0.10,
            "low_value": None,
            "high_value": None,
            "unit": "%",
            "horizon": "FY2026",
            "dimension_id": "total_company",
            "member": "total_company",
            "profile_pack_id": None,
            "as_of_date": None,
            "source_classification": "user_input",
            "validation": {"minimum": None, "maximum": None},
            "propagation_rule": "selected_growth_route",
            "status": "populated",
            "source_ref": "fixture:unknown-route-alias",
            "source_refs": ["fixture:unknown-route-alias"],
            "reason": "",
        }
    ]

    plan = _plan(package)

    assert plan.status == "FAIL"
    assert not plan.planned_writes
    assert "scenario_route_token_unknown" in _rule_ids(plan)


def test_accepted_route_aliases_are_canonical_before_support_binding_planning() -> None:
    package = _package()
    package["investment_case"]["scenario_items"] = [
        {
            "scenario_id": "Base",
            "assumption_id": "Revenue Growth",
            "metric": "Revenue growth",
            "value_kind": "point",
            "value": 0.10,
            "low_value": None,
            "high_value": None,
            "unit": "percent",
            "horizon": "FY-2026",
            "dimension_id": "Total Company",
            "member": "",
            "profile_pack_id": None,
            "as_of_date": None,
            "source_classification": "user_input",
            "validation": {"minimum": None, "maximum": None},
            "propagation_rule": "Selected Growth Route",
            "status": "populated",
            "source_ref": "fixture:accepted-route-alias",
            "source_refs": ["fixture:accepted-route-alias"],
            "reason": "",
        }
    ]

    plan = _plan(package)

    assert plan.status == "PASS"
    writes = {
        write.target_cell: write.value
        for write in plan.planned_writes
        if write.binding_id == "ic_bull_base_bear_rows"
    }
    assert writes["A2"] == "base"
    assert writes["B2"] == "revenue_growth"
    assert writes["H2"] == "%"
    assert writes["I2"] == "FY2026"
    assert writes["J2"] == "total_company"
    assert writes["K2"] == "total_company"
    assert writes["M2"] == "selected_growth_route"


def test_planner_canonicalizes_scenario_route_tokens_exactly_once(monkeypatch: pytest.MonkeyPatch) -> None:
    package = _package()
    package["investment_case"]["scenario_items"] = [
        {
            "scenario_id": "Base",
            "assumption_id": "Revenue Growth",
            "metric": "Revenue growth",
            "value_kind": "point",
            "value": 0.10,
            "low_value": None,
            "high_value": None,
            "unit": "percent",
            "horizon": "FY-2026",
            "dimension_id": "Total Company",
            "member": "default",
            "profile_pack_id": None,
            "as_of_date": None,
            "source_classification": "user_input",
            "validation": {"minimum": None, "maximum": None},
            "propagation_rule": "Selected Growth Route",
            "status": "populated",
            "source_ref": "fixture:single-boundary",
            "source_refs": ["fixture:single-boundary"],
            "reason": "",
        }
    ]
    original = validation_module.canonicalize_normalized_scenario_tokens
    calls = 0

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(planner_module, "canonicalize_normalized_scenario_tokens", counted)
    monkeypatch.setattr(validation_module, "canonicalize_normalized_scenario_tokens", counted)

    plan = _plan(package)

    assert plan.status == "PASS"
    assert calls == 1


def test_manifest_without_shell_identity_cannot_bypass_verification() -> None:
    manifest = _manifest()
    manifest.pop("shell_identity")

    plan = plan_standard_template_writes(
        _package(),
        binding_payload=_bindings(),
        manifest=manifest,
    )

    assert plan.status == "FAIL"
    assert not plan.planned_writes
    assert "shell_identity_missing" in _rule_ids(plan)


def test_verified_token_cannot_be_reused_with_mutated_manifest_or_binding_contract() -> None:
    manifest = _manifest()
    bindings = _bindings()
    token = verify_shell_identity(SHELL_PATH, manifest=manifest, binding_payload=bindings)

    drifted_manifest = deepcopy(manifest)
    drifted_manifest["semantic_contract_version"] = "9.9.9"
    manifest_plan = plan_standard_template_writes(
        _package(),
        binding_payload=bindings,
        manifest=drifted_manifest,
        shell_identity_report=token,
    )

    drifted_bindings = deepcopy(bindings)
    _binding(drifted_bindings, "summary_latest_revenue")["source_field"] = "net_income"
    binding_plan = plan_standard_template_writes(
        _package(),
        binding_payload=drifted_bindings,
        manifest=manifest,
        shell_identity_report=token,
    )

    version_bindings = deepcopy(bindings)
    version_bindings["binding_planner_contract_version"] = "9.9.9"
    version_plan = plan_standard_template_writes(
        _package(),
        binding_payload=version_bindings,
        manifest=manifest,
        shell_identity_report=token,
    )

    for plan in (manifest_plan, binding_plan, version_plan):
        assert plan.status == "FAIL"
        assert not plan.planned_writes
    assert "shell_identity_token_manifest_mismatch" in _rule_ids(manifest_plan)
    assert "shell_binding_contract_token_mismatch" in _rule_ids(binding_plan)
    assert "shell_binding_contract_token_mismatch" in _rule_ids(version_plan)


def test_binding_plan_snapshot_is_comparison_only_and_reproduced_exactly() -> None:
    package = _package()
    manifest = _manifest()
    bindings = _bindings()
    shell_token = verify_shell_identity(SHELL_PATH, manifest=manifest, binding_payload=bindings)

    plan, verified = reproduce_binding_plan_snapshot(
        package,
        binding_payload=bindings,
        manifest=manifest,
        shell_path=SHELL_PATH,
        shell_identity_report=shell_token,
    )
    assert plan.status == "PASS"
    assert not compare_binding_plan_snapshot(
        verified,
        normalized_package=package,
        manifest=manifest,
        binding_payload=bindings,
        shell_path=SHELL_PATH,
        shell_identity_report=shell_token,
    )

    fabricated = deepcopy(plan.to_dict())
    fabricated["planned_writes"][0]["value"] = "FORGED"
    with pytest.raises(BindingPlanReproductionError):
        reproduce_binding_plan_snapshot(
            package,
            binding_payload=bindings,
            manifest=manifest,
            shell_path=SHELL_PATH,
            shell_identity_report=shell_token,
            expected_plan=fabricated,
        )

    changed_package = deepcopy(package)
    changed_package["company_profile"]["business_description"]["value"] = "Changed after authorization"
    assert {
        issue["rule_id"]
        for issue in compare_binding_plan_snapshot(
            verified,
            normalized_package=changed_package,
            manifest=manifest,
            binding_payload=bindings,
            shell_path=SHELL_PATH,
            shell_identity_report=shell_token,
        )
    } == {"binding_plan_reproduction_mismatch"}


def test_stale_alias_horizon_guidance_is_blocked_before_visible_planning() -> None:
    package = _package()
    newer = package["normalized_guidance"]["items"][0]
    older = deepcopy(newer)
    older["horizon"]["value"] = "2026 year"
    older["publication_date"] = "2026-05-01"
    older["source_date"] = "2026-04-30"
    older["stated_in_period"] = "2025-Q4"
    older["evidence_key"] = "test-guidance-old-2026"
    older["value"]["value"] = "Revenue growth expected in the low-single-digit range."
    package["normalized_guidance"]["items"] = [older, newer]

    plan = _plan(package)

    assert plan.status == "FAIL"
    assert "stale_guidance_visibility_misclassified" in _rule_ids(plan)
    assert not plan.planned_writes


def _writes(plan: BindingPlan) -> dict[tuple[str, str], object]:
    return {(write.target_sheet, write.target_cell): write for write in plan.planned_writes}


def _binding(payload: dict, binding_id: str) -> dict:
    return next(item for item in payload["bindings"] if item["binding_id"] == binding_id)


def _rule_ids(plan: BindingPlan) -> set[str]:
    return {issue.rule_id for issue in plan.issues}


def _resolved_scalar_dispositions(package: dict) -> dict[str, object]:
    contracts, issues = planner_module._scalar_disposition_contracts(_bindings()["bindings"])
    assert issues == []
    return {
        disposition_id: planner_module._resolve_scalar_disposition(package, contract)
        for disposition_id, contract in contracts.items()
    }


def test_planner_is_json_only_and_does_not_import_excel_io() -> None:
    source = (ROOT / "pbi_xbrl" / "new_ticker_binding_planner.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    imported_modules = {
        alias.name
        for node in ast.walk(module)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module or ""
        for node in ast.walk(module)
        if isinstance(node, ast.ImportFrom)
    }

    assert not any(name.startswith("openpyxl") for name in imported_modules)
    assert "load_workbook(" not in source


def test_real_contract_plans_seven_business_flows_to_exact_cells() -> None:
    plan = _plan()

    assert plan.status == "PASS", [issue.to_dict() for issue in plan.issues]
    writes = _writes(plan)

    assert writes[("SUMMARY", "A3")].value.startswith("Test Systems provides")
    assert writes[("SUMMARY", "A5")].value.startswith("The current setup depends")
    assert writes[("SUMMARY", "A9")].value == "Subscriptions"
    assert writes[("SUMMARY", "B9")].value == 70.0
    assert writes[("SUMMARY", "A11")].value == "Support contracts"
    assert writes[("SUMMARY", "B11")].value == 10.0

    # Quarterly financials: period headers and metrics are distinct contracts.
    assert writes[("SUMMARY", "B26")].value == "2026-Q1"
    assert writes[("SUMMARY", "B28")].value == 120.5
    assert writes[("SUMMARY", "B30")].value == 13.1
    assert writes[("Valuation", "B6")].value == "2025-Q4"
    assert writes[("Valuation", "B9")].value == 118.0
    assert writes[("Valuation", "B9")].row_key == "2025-Q4"
    assert writes[("Valuation", "C6")].value == "2026-Q1"
    assert writes[("Valuation", "C9")].value == 120.5

    # Annual financials use their own period and value rows.
    assert writes[("BS_Segments", "B70")].value == "2025-FY"
    assert writes[("BS_Segments", "B71")].value == 470.0
    assert writes[("BS_Segments", "B71")].row_key == "2025-FY"
    assert writes[("BS_Segments", "B7")].value == "2025-Q4"
    assert writes[("BS_Segments", "C7")].value == "2026-Q1"

    # Guidance uses merge anchors and one resolved row disposition.
    guidance_key = "current_primary|1|revenue|FY2026|test-guidance-2026q1"
    assert writes[("Valuation", "O9")].value == "Revenue"
    assert writes[("Valuation", "Q9")].value == "2026-Q1"
    assert writes[("Valuation", "R9")].value == "FY2026"
    assert writes[("Valuation", "S9")].value.startswith("Revenue growth")
    assert writes[("Valuation", "AA9")].value == "current_primary / accepted"
    assert writes[("Valuation", "S9")].row_key == guidance_key
    assert ("Valuation", "T9") not in writes

    # Segment rows are a member-by-period pivot, not a sequential row dump.
    assert writes[("BS_Segments", "A66")].value == "Brand: Workflow software"
    assert writes[("BS_Segments", "C66")].value == 92.0
    assert writes[("BS_Segments", "B72")].value == 360.0
    assert writes[("BS_Segments", "C66")].row_key == "2026-Q1|brand|workflow software|revenue"
    assert plan.period_axes["bs_quarterly_periods"]["period_to_column"] == {
        "2025-Q4": "B",
        "2026-Q1": "C",
    }

    # Operating driver fields stay in the declared A/B/H cells.
    driver_key = "Demand|2026-Q1|operational|Retention and implementation velocity|test-driver-2026q1"
    assert writes[("Operating_Drivers", "A6")].value == "Demand"
    assert writes[("Operating_Drivers", "B6")].value.startswith("Renewal rates")
    assert writes[("Operating_Drivers", "H6")].value.startswith("Retention and implementation")
    assert writes[("Operating_Drivers", "B6")].row_key == driver_key
    assert writes[("Operating_Drivers", "B6")].source_ref == "synthetic_fixture:operating_drivers"

    # Quarter note commentary, implication, and source remain separate.
    note_key = "2026-Q1|Demand|Revenue|test-quarter-note-2026q1"
    assert writes[("Quarter_Notes_UI", "B3")].value.startswith("Recurring software demand")
    assert writes[("Quarter_Notes_UI", "A10")].value == "Demand"
    assert writes[("Quarter_Notes_UI", "C10")].value.startswith("Recurring software demand")
    assert writes[("Quarter_Notes_UI", "H10")].value.startswith("Growth durability")
    assert writes[("Quarter_Notes_UI", "M10")].value == "Synthetic quarterly evidence"
    assert writes[("Quarter_Notes_UI", "M10")].source_ref == "synthetic_fixture:quarter_notes"
    assert writes[("Quarter_Notes_UI", "C10")].row_key == note_key
    assert not any(sheet == "Quarter_Notes_UI" and cell.endswith("9") for sheet, cell in writes)

    # Promise Progress follows the real A:K header semantics.
    assert writes[("Promise_Progress_UI", "A61")].value == "Revenue"
    assert writes[("Promise_Progress_UI", "C61")].value.startswith("Revenue growth")
    assert ("Promise_Progress_UI", "B61") not in writes
    assert writes[("Promise_Progress_UI", "G61")].value == "open"
    assert writes[("Promise_Progress_UI", "H61")].value == "FY2026"
    assert writes[("Promise_Progress_UI", "I61")].value == "2026-Q1"
    assert writes[("Promise_Progress_UI", "J61")].value == "2026-07-07"

    # Investment-case fields target the actual B5/B7 merge anchors.
    assert writes[("TEST_Investment_Case", "B5")].value.startswith("The test case depends")
    assert writes[("TEST_Investment_Case", "B7")].value.startswith("Whether retention")

    # Liquidity and valuation inputs have exact business semantics.
    assert writes[("SUMMARY", "B41")].value == 0.7
    assert writes[("SUMMARY", "B41")].normalized_path == "debt_liquidity.net_leverage"
    assert writes[("SUMMARY", "B45")].value == 180.0
    assert writes[("SUMMARY", "B45")].normalized_path == "debt_liquidity.summary_liquidity_display"
    assert writes[("SUMMARY", "D45")].value == "As of 2026-03-31"
    assert writes[("SUMMARY", "B44")].value == 100.0
    assert writes[("SUMMARY", "D44")].value == "As of 2026-03-31"
    snapshot = {
        124: (80.0, "2026-03-31", "populated"),
        125: (100.0, "2026-03-31", "populated"),
        126: (180.0, "2026-03-31", "populated"),
        127: (30.0, "2026-03-31", "populated"),
        128: (150.0, "2026-03-31", "populated"),
        129: (70.0, "2026-03-31", "populated"),
        130: (0.7, "2026-03-31", "populated"),
    }
    for row, (value, period, status) in snapshot.items():
        assert writes[("Valuation", f"B{row}")].value == value
        assert writes[("Valuation", f"D{row}")].value == period
        assert writes[("Valuation", f"E{row}")].value == status
        assert writes[("Valuation", f"F{row}")].value == "synthetic_fixture:debt_liquidity"
        assert writes[("Valuation", f"F{row}")].source_ref == "synthetic_fixture:debt_liquidity"
    assert writes[("Valuation", "D202")].value == 76.2
    assert writes[("Valuation", "D202")].normalized_path == "valuation_inputs.operating_cash_flow_ttm"
    assert writes[("Valuation", "D195")].value == "2026-03-31"
    assert writes[("Valuation", "D196")].value == 42.0
    assert writes[("Valuation", "D197")].value == 42.3
    assert writes[("Valuation", "D198")].value == 70.0
    assert writes[("Valuation", "D200")].value == 101.4
    assert writes[("Valuation", "D203")].value == 470.8

    assert len(writes) == len(plan.planned_writes)
    assert all(write.binding_id and write.normalized_path and write.row_key for write in plan.planned_writes)
    assert all(write.target_role and write.target_type for write in plan.planned_writes)
    assert not any(issue.severity in {"P0", "P1"} for issue in plan.issues)


def test_required_or_blocked_if_missing_binding_creates_p1_and_fails_plan() -> None:
    package = _package()
    field = package["company_profile"]["business_description"]
    field.update(value=None, status="missing_source", reason="Synthetic source intentionally removed.")

    plan = _plan(package)

    assert plan.status == "FAIL"
    assert "required_binding_missing" in _rule_ids(plan)
    gap = next(item for item in plan.mapping_gaps if item["binding_id"] == "summary_company_description")
    assert gap["severity"] == "P1"
    assert gap["normalized_path"] == "company_profile.business_description"
    assert gap["row_key"] == "scalar"
    assert gap["expected_target"] == "A3"


def test_manifest_rejects_guidance_write_to_merge_non_anchor_t9() -> None:
    bindings = _bindings()
    guidance = _binding(bindings, "valuation_guidance_current_primary_rows")
    value_column = next(column for column in guidance["target_columns"] if column["source_field"] == "value")
    value_column["target_column"] = "T"

    plan = _plan(bindings=bindings)

    assert plan.status == "FAIL"
    assert "manifest_merge_non_anchor_target" in _rule_ids(plan)
    assert "manifest_exact_writable_cell_missing" in _rule_ids(plan)
    assert plan.planned_writes == []


def test_annual_axis_window_exclusion_is_explicit_and_shared() -> None:
    package = _package()
    seed = package["annual_financials"]["rows"][0]
    rows = []
    for year in range(2017, 2026):
        row = deepcopy(seed)
        row["period"] = f"{year}-FY"
        row["fiscal_year"] = year
        for name, value in row.items():
            if isinstance(value, dict) and "status" in value:
                value["period"] = f"{year}-FY"
                value["source_ref"] = f"synthetic_fixture:annual_financials:{year}"
        rows.append(row)
    package["annual_financials"]["rows"] = rows

    plan = _plan(package)
    header_report = next(item for item in plan.binding_reports if item["binding_id"] == "bs_annual_period_headers")
    value_report = next(item for item in plan.binding_reports if item["binding_id"] == "bs_annual_revenue_series")

    assert plan.status == "PASS", [issue.to_dict() for issue in plan.issues]
    for report in (header_report, value_report):
        excluded = next(item for item in report["skipped_rows"] if item["row_key"] == "2017-FY")
        assert excluded["reason"] == "period_axis_outside_visible_window"
        assert excluded["severity"] == "P2"
        assert not report["overflow_rows"]
    writes = _writes(plan)
    assert writes[("BS_Segments", "B70")].value == "2018-FY"
    assert writes[("BS_Segments", "I70")].value == "2025-FY"
    assert writes[("BS_Segments", "I71")].value == 470.0


def test_selected_row_with_missing_required_value_is_not_silently_skipped() -> None:
    package = _package()
    revenue = package["annual_financials"]["rows"][0]["revenue"]
    revenue.update(value=None, status="missing_source", reason="Revenue evidence intentionally removed.")

    plan = _plan(package)
    report = next(item for item in plan.binding_reports if item["binding_id"] == "bs_annual_revenue_series")

    assert plan.status == "FAIL"
    assert "required_row_value_missing" in _rule_ids(plan)
    skip = next(item for item in report["skipped_rows"] if item["row_key"] == "2025-FY")
    assert skip["severity"] == "P1"
    assert skip["normalized_path"].endswith(".revenue")
    assert skip["expected_target"] == "B71"
    assert any(gap["row_key"] == "2025-FY" and gap["severity"] == "P1" for gap in plan.mapping_gaps)
    ledger_issues = [
        issue
        for issue in plan.issue_ledger["issues"]
        if issue.get("binding_id") == "bs_annual_revenue_series"
        and issue.get("business_row_key") == "2025-FY"
        and issue.get("root_cause") == "missing_value"
    ]
    assert len(ledger_issues) == 1
    assert ledger_issues[0]["occurrence_count"] == 2
    occurrence_ids = set(ledger_issues[0]["occurrence_ids"])
    assert len(occurrence_ids) == 2
    assert sum(issue["occurrence_count"] for issue in plan.issue_ledger["issues"]) == len(plan.issue_ledger["occurrences"])


def test_duplicate_package_mapping_gaps_remain_as_distinct_occurrences() -> None:
    package = _package()
    gap = {
        "severity": "P2",
        "rule_id": "duplicate_fixture_gap",
        "field": "capital_returns.dividends",
        "normalized_path": "capital_returns.dividends",
        "message": "The same source gap was reported twice.",
        "source_ref": "synthetic_fixture:duplicate_gap",
        "visibility_disposition": "json_audit_only",
    }
    package["mapping_gaps"].extend([gap, deepcopy(gap)])

    plan = _plan(package)

    matching = [issue for issue in plan.issue_ledger["issues"] if issue["rule_id"] == "duplicate_fixture_gap"]
    assert len(matching) == 1
    assert matching[0]["occurrence_count"] == 2
    assert sum(issue["occurrence_count"] for issue in plan.issue_ledger["issues"]) == len(plan.issue_ledger["occurrences"])


def test_schema_and_semantics_reject_units_dimensions_and_core_lineage() -> None:
    bad_unit = _package()
    bad_unit["quarterly_financials"]["rows"][0]["revenue"]["unit"] = "bananas"
    assert "normalized_schema_enum" in {issue.rule_id for issue in validate_normalized_company_data_schema(bad_unit)}
    assert "invalid_unit" in {issue.rule_id for issue in validate_normalized_company_data(bad_unit)}

    bad_dimension = _package()
    bad_dimension["segments"]["items"][0]["dimension"] = "planet"
    assert "normalized_schema_enum" in {issue.rule_id for issue in validate_normalized_company_data_schema(bad_dimension)}
    assert "invalid_dimension" in {issue.rule_id for issue in validate_normalized_company_data(bad_dimension)}

    bad_lineage = _package()
    field = bad_lineage["annual_financials"]["rows"][0]["revenue"]
    field["core"] = True
    field["source_ref"] = ""
    assert "missing_source_ref" in {issue.rule_id for issue in validate_normalized_company_data(bad_lineage)}


def test_target_type_mismatch_and_invalid_sort_key_fail_before_writes() -> None:
    bindings = _bindings()
    _binding(bindings, "summary_latest_revenue")["target_type"] = "text"
    type_plan = _plan(bindings=bindings)
    assert type_plan.status == "FAIL"
    assert "manifest_target_type_mismatch" in _rule_ids(type_plan)
    assert type_plan.planned_writes == []

    bindings = _bindings()
    _binding(bindings, "qn_quarter_note_rows")["sort_order"] = ["missing_business_sort_key:asc"]
    sort_plan = _plan(bindings=bindings)
    assert sort_plan.status == "FAIL"
    assert "binding_sort_key_missing" in _rule_ids(sort_plan)


def test_schema_is_enforced_before_planning_and_required_row_columns_are_real() -> None:
    missing_section = _package()
    missing_section.pop("annual_financials")
    plan = _plan(missing_section)
    assert plan.status == "FAIL"
    assert any(issue.rule_id == "normalized_schema_required" for issue in plan.schema_issues)
    assert plan.planned_writes == []

    missing_row_column = _package()
    missing_row_column["quarter_notes"]["items"][0].pop("commentary")
    plan = _plan(missing_row_column)
    assert plan.status == "FAIL"
    assert any(issue.rule_id == "normalized_schema_required" for issue in plan.schema_issues)
    assert plan.planned_writes == []


def test_planner_source_has_no_sequential_dump_or_silent_capacity_slice() -> None:
    source = (ROOT / "pbi_xbrl" / "new_ticker_binding_planner.py").read_text(encoding="utf-8")

    assert "zip(" not in source
    assert "rows[:capacity]" not in source
    assert '" | ".join' not in source
    assert "for source_position, row in enumerate(rows):" in source


def test_historical_guidance_projects_only_through_its_resolved_rowset() -> None:
    package = _package()
    historical = deepcopy(package["normalized_guidance"]["items"][0])
    historical["source_date"] = "2025-03-01"
    historical["stated_in_period"] = "2024-Q4"
    historical["horizon"]["value"] = "FY2025"
    historical["evidence_key"] = "test-guidance-history"
    historical["display_role"] = "history"
    historical["display_priority"] = 99
    package["normalized_guidance"]["items"].append(historical)

    plan = _plan(package)

    assert plan.status == "PASS", [issue.to_dict() for issue in plan.issues]
    projected = [write for write in plan.planned_writes if write.row_key.endswith("test-guidance-history")]
    assert len(projected) == 8
    assert {write.binding_id for write in projected} == {"valuation_guidance_historical_rows"}
    assert {write.target_cell for write in projected} == {"O29", "Q29", "R29", "S29", "X29", "Y29", "Z29", "AA29"}
    assert not any(
        write.row_key.endswith("test-guidance-history")
        for write in plan.planned_writes
        if write.binding_id in {
            "valuation_guidance_current_primary_rows",
            "valuation_guidance_current_secondary_rows",
        }
    )


def test_pick_selector_reconciles_eligible_selected_and_structured_exclusions() -> None:
    package = _package()
    plan = _plan(package)

    assert plan.status == "PASS", [issue.to_dict() for issue in plan.issues]
    expected_eligible = {
        "summary_as_of_quarter": 2,
        "summary_latest_net_income": 2,
        "summary_latest_revenue": 2,
    }
    for binding_id, eligible_count in expected_eligible.items():
        report = next(row for row in plan.binding_reports if row["binding_id"] == binding_id)
        pick_exclusions = [
            row
            for row in report["skipped_rows"]
            if str(row.get("reason") or "").startswith("row_selector_pick_excluded:")
        ]
        assert eligible_count == report["capacity_used"] + len(pick_exclusions)
        assert all(row["selected_row_key"] and row["excluded_row_key"] for row in pick_exclusions)
        assert all(row["selector_rule"] in {"pick=first", "pick=latest"} for row in pick_exclusions)
        assert all(isinstance(row["period"], str) and not row["period"].startswith("{") for row in pick_exclusions)
        assert report["overflow_rows"] == []


def test_latest_summary_selector_is_typed_and_source_row_order_independent() -> None:
    original = _package()
    reordered = _package()
    reordered["quarterly_financials"]["rows"].reverse()
    bindings = _bindings()

    for binding_id in ("summary_as_of_quarter", "summary_latest_revenue", "summary_latest_net_income"):
        binding = _binding(bindings, binding_id)
        assert binding["row_selector"]["source_path"] == "quarterly_financials.rows"
        assert binding["row_selector"]["pick"] == "latest"
        assert ".rows.0" not in binding["normalized_field"]
    for binding in bindings["bindings"]:
        if binding["binding_id"].startswith(("summary_revolver_availability", "valuation_debt_snapshot")):
            serialized = json.dumps(binding, sort_keys=True)
            assert ".items.0" not in serialized
            assert ".rows.0" not in serialized

    first = _plan(original)
    second = _plan(reordered)

    assert first.status == second.status == "PASS"
    first_writes = _writes(first)
    second_writes = _writes(second)
    expected = {
        ("SUMMARY", "B26"): "2026-Q1",
        ("SUMMARY", "B28"): 120.5,
        ("SUMMARY", "B30"): 13.1,
    }
    for key, value in expected.items():
        assert first_writes[key].value == value
        assert second_writes[key].value == value
        assert first_writes[key].row_key == second_writes[key].row_key == "2026-Q1"

    planner_source = Path(planner_module.__file__).read_text(encoding="utf-8")
    for ticker in ("ANF", "PBI", "GPRE"):
        assert f'ticker == "{ticker}"' not in planner_source
        assert f"ticker == '{ticker}'" not in planner_source


def test_duplicate_or_incompatible_latest_quarter_fails_closed() -> None:
    duplicate = _package()
    repeated = deepcopy(
        next(
            row
            for row in duplicate["quarterly_financials"]["rows"]
            if row["period"] == "2026-Q1"
        )
    )
    repeated["revenue"]["source_ref"] = "synthetic_fixture:duplicate-latest"
    duplicate["quarterly_financials"]["rows"].append(repeated)
    duplicate_plan = _plan(duplicate)

    assert duplicate_plan.status == "FAIL"
    assert duplicate_plan.planned_writes == []
    assert "duplicate_business_row_key" in _rule_ids(duplicate_plan)

    invalid_period = _package()
    latest_row = next(
        row
        for row in invalid_period["quarterly_financials"]["rows"]
        if row["period"] == "2026-Q1"
    )
    latest_row["fiscal_quarter"] = 4
    invalid_period_plan = _plan(invalid_period)

    assert invalid_period_plan.status == "FAIL"
    assert invalid_period_plan.planned_writes == []
    assert "invalid_fiscal_quarter" in _rule_ids(invalid_period_plan)

    mismatched_unit_bindings = _bindings()
    _binding(mismatched_unit_bindings, "summary_latest_revenue")["row_selector"]["period_identity"]["unit"] = "m shares"
    mismatch_plan = _plan(bindings=mismatched_unit_bindings)

    assert mismatch_plan.status == "FAIL"
    assert ("SUMMARY", "B28") not in _writes(mismatch_plan)
    assert "binding_latest_quarter_metric_incompatible" in _rule_ids(mismatch_plan)


def test_scalar_snapshot_requires_exact_period_and_unit_without_reconstruction() -> None:
    missing_period = _package()
    missing_period["debt_liquidity"]["lease_liabilities"].pop("period")
    missing_period_plan = _plan(missing_period)
    missing_writes = _writes(missing_period_plan)

    assert missing_period_plan.status == "PASS"
    assert ("Valuation", "B127") not in missing_writes
    assert ("Valuation", "D127") not in missing_writes
    assert missing_writes[("Valuation", "E127")].value == "manual_review_required"
    assert missing_writes[("Valuation", "F127")].value == "synthetic_fixture:debt_liquidity"
    assert any(
        row.get("binding_id") == "valuation_debt_snapshot_leases_value"
        and row.get("reason") == "Scalar value requires populated companion path(s): debt_liquidity.lease_liabilities.period."
        for row in missing_period_plan.mapping_gaps
    )

    wrong_unit = _package()
    wrong_unit["debt_liquidity"]["cash"]["unit"] = "m shares"
    wrong_unit_plan = _plan(wrong_unit)

    assert wrong_unit_plan.status == "FAIL"
    assert ("Valuation", "B124") not in _writes(wrong_unit_plan)
    assert "binding_scalar_unit_mismatch" in _rule_ids(wrong_unit_plan)

    unsupported = _package()
    for metric in ("total_debt", "net_debt", "net_leverage"):
        field = unsupported["debt_liquidity"][metric]
        field.update(value=None, status="missing_source", reason=f"{metric} intentionally unavailable")
        field.pop("period", None)
    unsupported_plan = _plan(unsupported)
    unsupported_writes = _writes(unsupported_plan)

    assert unsupported_plan.status == "PASS"
    for row in range(128, 131):
        assert ("Valuation", f"B{row}") not in unsupported_writes
        assert ("Valuation", f"D{row}") not in unsupported_writes
        assert unsupported_writes[("Valuation", f"E{row}")].value == "missing_source"


def test_product_pass2a_scalar_columns_declare_one_shared_disposition() -> None:
    bindings = _bindings()["bindings"]
    expected_groups = {
        "debt_liquidity_cash": {
            "record_path": "debt_liquidity.cash",
            "binding_ids": {
                "valuation_debt_snapshot_cash_value",
                "valuation_debt_snapshot_cash_as_of",
                "valuation_debt_snapshot_cash_status",
                "valuation_debt_snapshot_cash_evidence",
            },
        },
        "debt_liquidity_revolver_availability": {
            "record_path": "debt_liquidity.revolver_availability",
            "binding_ids": {
                "summary_revolver_availability",
                "summary_revolver_availability_as_of",
                "valuation_debt_snapshot_revolver_value",
                "valuation_debt_snapshot_revolver_as_of",
                "valuation_debt_snapshot_revolver_status",
                "valuation_debt_snapshot_revolver_evidence",
            },
        },
        "debt_liquidity_total_liquidity": {
            "record_path": "debt_liquidity.total_liquidity",
            "binding_ids": {
                "summary_liquidity",
                "summary_liquidity_as_of",
                "valuation_debt_snapshot_liquidity_value",
                "valuation_debt_snapshot_liquidity_as_of",
                "valuation_debt_snapshot_liquidity_status",
                "valuation_debt_snapshot_liquidity_evidence",
            },
        },
        "debt_liquidity_lease_liabilities": {
            "record_path": "debt_liquidity.lease_liabilities",
            "binding_ids": {
                "valuation_debt_snapshot_leases_value",
                "valuation_debt_snapshot_leases_as_of",
                "valuation_debt_snapshot_leases_status",
                "valuation_debt_snapshot_leases_evidence",
            },
        },
        "debt_liquidity_total_debt": {
            "record_path": "debt_liquidity.total_debt",
            "binding_ids": {
                "valuation_debt_snapshot_core_debt_value",
                "valuation_debt_snapshot_core_debt_as_of",
                "valuation_debt_snapshot_core_debt_status",
                "valuation_debt_snapshot_core_debt_evidence",
            },
        },
        "debt_liquidity_net_debt": {
            "record_path": "debt_liquidity.net_debt",
            "binding_ids": {
                "valuation_debt_snapshot_net_debt_value",
                "valuation_debt_snapshot_net_debt_as_of",
                "valuation_debt_snapshot_net_debt_status",
                "valuation_debt_snapshot_net_debt_evidence",
            },
        },
        "debt_liquidity_net_leverage": {
            "record_path": "debt_liquidity.net_leverage",
            "binding_ids": {
                "valuation_debt_snapshot_net_leverage_value",
                "valuation_debt_snapshot_net_leverage_as_of",
                "valuation_debt_snapshot_net_leverage_status",
                "valuation_debt_snapshot_net_leverage_evidence",
            },
        },
    }

    grouped: dict[str, list[dict]] = {}
    for binding in bindings:
        disposition_id = str(binding.get("scalar_disposition_id") or "")
        if disposition_id:
            grouped.setdefault(disposition_id, []).append(binding)

    assert set(grouped) == set(expected_groups)
    for disposition_id, expected in expected_groups.items():
        rows = grouped[disposition_id]
        assert {row["binding_id"] for row in rows} == expected["binding_ids"]
        assert {row["scalar_disposition_field"] for row in rows} >= {"value", "status"}
        assert {
            str(row.get("scalar_disposition_path") or "")
            for row in rows
            if row.get("scalar_disposition_path")
        } == {expected["record_path"]}


@pytest.mark.parametrize(
    ("disposition_id", "field_name", "value", "unit", "period"),
    [
        ("debt_liquidity_cash", "cash", 80.0, "$m", "2026-03-31"),
        ("debt_liquidity_revolver_availability", "revolver_availability", 100.0, "$m", "2026-03-31"),
        ("debt_liquidity_total_liquidity", "total_liquidity", 180.0, "$m", "2026-03-31"),
        ("debt_liquidity_lease_liabilities", "lease_liabilities", 30.0, "$m", "2026-03-31"),
    ],
)
def test_scalar_disposition_resolves_complete_record_as_one_candidate(
    disposition_id: str,
    field_name: str,
    value: float,
    unit: str,
    period: str,
) -> None:
    disposition = _resolved_scalar_dispositions(_package())[disposition_id]

    assert disposition.metric_id == field_name
    assert disposition.value == value
    assert disposition.unit == unit
    assert disposition.period == period
    assert disposition.status == "populated"
    assert disposition.validity_code == "populated"
    assert disposition.source_ref == "synthetic_fixture:debt_liquidity"


@pytest.mark.parametrize(
    ("disposition_id", "field_name"),
    [
        ("debt_liquidity_cash", "cash"),
        ("debt_liquidity_revolver_availability", "revolver_availability"),
        ("debt_liquidity_total_liquidity", "total_liquidity"),
        ("debt_liquidity_lease_liabilities", "lease_liabilities"),
    ],
)
@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        ("missing_value", "scalar_value_invalid"),
        ("missing_period", "scalar_companion_missing"),
        ("missing_unit", "scalar_unit_mismatch"),
        ("incompatible_unit", "scalar_unit_mismatch"),
        ("rejected_status", "scalar_status_value_conflict"),
        ("missing_lineage", "missing_source_ref"),
    ],
)
def test_scalar_disposition_invalid_companion_fails_closed_across_columns(
    disposition_id: str,
    field_name: str,
    mutation: str,
    expected_code: str,
) -> None:
    package = _package()
    record = package["debt_liquidity"][field_name]
    if mutation == "missing_value":
        record.pop("value")
    elif mutation == "missing_period":
        record.pop("period")
    elif mutation == "missing_unit":
        record.pop("unit")
    elif mutation == "incompatible_unit":
        record["unit"] = "m shares"
    elif mutation == "rejected_status":
        record["status"] = "manual_review_required"
    elif mutation == "missing_lineage":
        record.pop("source_ref")

    disposition = _resolved_scalar_dispositions(package)[disposition_id]

    assert disposition.value is None
    assert disposition.unit is None
    assert disposition.period is None
    assert disposition.period_display is None
    assert disposition.status == "manual_review_required"
    assert disposition.validity_code == expected_code
    assert disposition.reason


def test_scalar_disposition_duplicate_identity_blocks_before_writes() -> None:
    bindings = _bindings()
    duplicate_rows = [
        deepcopy(row)
        for row in bindings["bindings"]
        if row.get("scalar_disposition_id") == "debt_liquidity_cash"
    ]
    for row in duplicate_rows:
        row["binding_id"] = f"duplicate_{row['binding_id']}"
        row["scalar_disposition_id"] = "duplicate_debt_liquidity_cash"
    bindings["bindings"].extend(duplicate_rows)

    plan = _plan(bindings=bindings)

    assert plan.status == "FAIL"
    assert plan.planned_writes == []
    assert "binding_scalar_disposition_duplicate_identity" in _rule_ids(plan)


def test_scalar_disposition_is_binding_order_independent() -> None:
    original_bindings = _bindings()
    reversed_bindings = _bindings()
    reversed_bindings["bindings"].reverse()

    original = _plan(bindings=original_bindings)
    reordered = _plan(bindings=reversed_bindings)

    assert original.status == reordered.status == "PASS"
    scalar_targets = {
        ("SUMMARY", "B44"),
        ("SUMMARY", "D44"),
        ("SUMMARY", "B45"),
        ("SUMMARY", "D45"),
        *(("Valuation", f"{column}{row}") for row in range(124, 131) for column in "BDEF"),
    }
    original_writes = _writes(original)
    reordered_writes = _writes(reordered)
    assert {
        key: original_writes[key].to_dict()
        for key in scalar_targets
        if key in original_writes
    } == {
        key: reordered_writes[key].to_dict()
        for key in scalar_targets
        if key in reordered_writes
    }


def test_stale_guidance_marked_current_fails_before_visible_planning() -> None:
    package = _package()
    stale = deepcopy(package["normalized_guidance"]["items"][0])
    stale["publication_date"] = "2025-02-20"
    stale["source_date"] = "2024-12-31"
    stale["stated_in_period"] = "2024-Q4"
    stale["horizon"]["value"] = "FY2025"
    stale["evidence_key"] = "test-guidance-stale-current"
    stale["display_role"] = "current_primary"
    package["normalized_guidance"]["items"].append(stale)

    plan = _plan(package)

    assert plan.status == "FAIL"
    assert "stale_guidance_visibility_misclassified" in _rule_ids(plan)
    assert plan.planned_writes == []


def test_future_quarter_note_is_audit_only_and_never_planned_visible() -> None:
    package = _package()
    future = deepcopy(package["quarter_notes"]["items"][0])
    future["quarter"]["value"] = "2027-Q1"
    future["quarter"]["period"] = "2027-Q1"
    future["evidence_key"] = "test-quarter-note-future"
    future["display_role"] = "audit_only"
    package["quarter_notes"]["items"].append(future)

    plan = _plan(package)

    assert plan.status == "PASS", [issue.to_dict() for issue in plan.issues]
    assert not any(write.row_key.endswith("test-quarter-note-future") for write in plan.planned_writes)
    report = next(item for item in plan.binding_reports if item["binding_id"] == "qn_quarter_note_rows")
    assert any(skip["row_key"].endswith("test-quarter-note-future") and skip["reason"].startswith("row_selector_excluded") for skip in report["skipped_rows"])


def test_qa_capacity_overflow_is_explicit_and_does_not_drop_json_detail() -> None:
    package = _package()
    package["manual_review_flags"].append(
        {
            "severity": "P2",
            "rule_id": "second_actionable_review",
            "field": "company_profile.industry",
            "message": "Second distinct actionable review.",
            "source_ref": "synthetic_fixture:second_review",
            "suggested_action": "Review the second synthetic issue.",
        }
    )
    bindings = _bindings()
    qa_binding = _binding(bindings, "qa_log_validation_rows")
    qa_binding["planner_target"] = "A2:L2"
    qa_binding["target"] = "A2:L2"
    qa_binding["capacity"] = 1

    plan = _plan(package, bindings=bindings)
    report = next(item for item in plan.binding_reports if item["binding_id"] == "qa_log_validation_rows")

    assert plan.status == "FAIL"
    assert plan.qa_snapshot_status == "failed"
    assert "qa_presentation_snapshot_unstable" in _rule_ids(plan)
    assert report["overflow_rows"]
    assert any(gap["binding_id"] == "qa_log_validation_rows" and gap["reason"] == "capacity_exceeded" for gap in plan.mapping_gaps)
    assert plan.issue_ledger["summary"]["detailed_occurrence_count"] >= len(package["manual_review_flags"])
    assert len(plan.issue_ledger["occurrences"]) == plan.issue_ledger["summary"]["detailed_occurrence_count"]
    assert not any(write.target_sheet in {"QA_Log", "Needs_Review", "QA_Checks"} for write in plan.planned_writes)


def test_final_canonical_ledger_is_the_only_blocking_gate() -> None:
    blocking_gap = _package()
    blocking_gap["mapping_gaps"].append(
        {
            "severity": "P1",
            "rule_id": "blocking_package_gap",
            "field": "debt_liquidity.total_debt",
            "message": "Required source is unresolved.",
            "source_ref": "synthetic_fixture:blocking_gap",
        }
    )
    gap_plan = _plan(blocking_gap)
    assert gap_plan.status == "FAIL"
    assert gap_plan.planned_writes == []
    assert any(issue["rule_id"] == "blocking_package_gap" for issue in gap_plan.issue_ledger["issues"])

    blocking_review = _package()
    blocking_review["manual_review_flags"].append(
        {
            "severity": "P2",
            "rule_id": "explicit_render_blocker",
            "field": "normalized_guidance.items",
            "message": "A P2 issue explicitly blocks rendering.",
            "source_ref": "synthetic_fixture:blocking_review",
            "render_blocking": True,
        }
    )
    review_plan = _plan(blocking_review)
    assert review_plan.status == "FAIL"
    assert review_plan.planned_writes == []

    audit_only = _package()
    audit_only["manual_review_flags"].append(
        {
            "severity": "P2",
            "rule_id": "nonblocking_audit_note",
            "field": "source_coverage",
            "message": "Audit-only lineage note.",
                "source_ref": "synthetic_fixture:audit",
                "suggested_action": "Retain in JSON audit only.",
                "visibility_disposition": "json_audit_only",
            "promotion_blocking": False,
            "render_blocking": False,
        }
    )
    audit_plan = _plan(audit_only)
    assert audit_plan.status == "PASS", [issue.to_dict() for issue in audit_plan.issues]


def test_caller_mapping_cannot_forge_shell_identity() -> None:
    plan = plan_standard_template_writes(
        _package(),
        binding_payload=_bindings(),
        manifest=_manifest(),
        shell_identity_report={"status": "PASS", "issues": []},  # type: ignore[arg-type]
    )

    assert plan.status == "FAIL"
    assert "shell_identity_not_verified" in _rule_ids(plan)
    assert plan.planned_writes == []


def test_verified_identity_token_cannot_be_reused_for_drifted_binding_semantics() -> None:
    manifest = _manifest()
    approved_bindings = _bindings()
    identity = verify_shell_identity(SHELL_PATH, manifest=manifest, binding_payload=approved_bindings)
    drifted_bindings = deepcopy(approved_bindings)
    _binding(drifted_bindings, "summary_latest_revenue")["source_field"] = "net_income"

    plan = plan_standard_template_writes(
        _package(),
        binding_payload=drifted_bindings,
        manifest=manifest,
        shell_identity_report=identity,
    )

    assert plan.status == "FAIL"
    assert "shell_binding_contract_token_mismatch" in _rule_ids(plan)
    assert plan.planned_writes == []
