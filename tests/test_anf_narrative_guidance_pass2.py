from __future__ import annotations

from collections import Counter
from copy import deepcopy
from pathlib import Path

from pbi_xbrl.json_schema_validation import load_json_strict
from pbi_xbrl.new_ticker_binding_planner import (
    DEFAULT_MANIFEST,
    DEFAULT_SHELL,
    reproduce_binding_plan,
)
from pbi_xbrl.normalized_company_data_validation import validate_normalized_company_data
from scripts.build_anf_new_ticker_parity_matrix import (
    _guidance_parity_entries,
    _guidance_signature,
    _promise_progress_parity_entries,
)
from scripts.build_anf_shadow_normalized_package import _progress_status, build_anf_normalized_package


ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = next(
    ancestor / "StockModelData"
    for ancestor in [ROOT, *ROOT.parents]
    if (ancestor / "StockModelData").exists()
)
ANF_WORKBOOK = DATA_ROOT / "outputs" / "Excel stock models" / "ANF_model.xlsx"
BINDING_MAP = ROOT / "docs" / "workbook_binding_map.json"


def _package() -> dict:
    return build_anf_normalized_package(data_root=DATA_ROOT, workbook_path=ANF_WORKBOOK)


def _plan(package: dict):
    return reproduce_binding_plan(
        package,
        binding_payload=load_json_strict(BINDING_MAP),
        manifest=load_json_strict(DEFAULT_MANIFEST),
        shell_path=DEFAULT_SHELL,
    )


def _guidance_item_for_legacy_row(package: dict, row_number: int) -> dict:
    marker = f"!Guidance_Normalized!row:{row_number}"
    return next(
        item
        for item in package["normalized_guidance"]["items"]
        if any(marker in str(source_ref) for source_ref in item.get("evidence_refs", []))
    )


def test_anf_guidance_routes_current_history_and_supersession_by_business_scope() -> None:
    package = _package()
    assert validate_normalized_company_data(package) == []

    items = package["normalized_guidance"]["items"]
    primary = sorted(
        (item for item in items if item["display_role"] == "current_primary"),
        key=lambda item: item["display_priority"],
    )
    secondary = sorted(
        (item for item in items if item["display_role"] == "current_secondary"),
        key=lambda item: item["display_priority"],
    )

    assert [(item["metric"]["value"], item["horizon"]["value"]) for item in primary] == [
        ("Revenue", "2026 year"),
        ("Revenue", "2026-Q1"),
        ("Operating margin", "2026 year"),
        ("Operating margin", "2026-Q1"),
        ("Adj EPS", "2026 year"),
        ("Adj EPS", "2026-Q1"),
        ("Real estate activity", "2026 year"),
    ]
    assert {(item["metric"]["value"], item["horizon"]["value"]) for item in secondary} == {
        ("Capex", "2026 year"),
        ("Diluted shares", "2026 year"),
        ("Diluted shares", "2026-Q1"),
        ("Real estate activity", "2026-Q1"),
        ("Share repurchases", "2026 year"),
        ("Share repurchases", "2026-Q1"),
    }
    assert all(item["publication_date"] == "2026-03-04" for item in [*primary, *secondary])
    assert all(item["stated_in_period"] == "2025-Q4" for item in [*primary, *secondary])
    assert all(item["source_date"] == "2026-01-31" for item in [*primary, *secondary])
    assert all(item["evidence_refs"] for item in [*primary, *secondary])
    assert any(item["display_role"] == "history" for item in items)
    assert any(item["display_role"] == "superseded" for item in items)
    assert not ({item["evidence_key"] for item in primary} & {item["evidence_key"] for item in secondary})


def test_anf_promise_progress_uses_only_explicit_status_rules() -> None:
    package = _package()
    rows = package["promise_progress"]["items"]
    visible_2025 = sorted(
        (row for row in rows if row["display_block"] == "fy2025" and row["visibility_disposition"] == "visible"),
        key=lambda row: row["display_priority"],
    )
    visible_2024 = sorted(
        (row for row in rows if row["display_block"] == "fy2024" and row["visibility_disposition"] == "visible"),
        key=lambda row: row["display_priority"],
    )

    assert [row["display_metric"]["value"] for row in visible_2025] == [
        "FY2025 Revenue",
        "FY2025 Operating margin",
        "FY2025 Adj EPS",
        "FY2025 Share repurchases",
        "FY2025 Diluted shares",
        "FY2025 Capex",
        "FY2025 Real estate activity",
        "FY2025 Tariffs",
    ]
    assert [row["display_metric"]["value"] for row in visible_2024] == [
        "FY2024 Revenue",
        "FY2024 Operating margin",
    ]
    allowed_rules = {"", "actual_within_published_range", "actual_meets_explicit_minimum"}
    assert {row["status_rule_id"] for row in rows} <= allowed_rules
    for row in rows:
        if not row["status_rule_id"]:
            assert row["progress_status"]["status"] == "manual_review_required"
            assert row["progress_status"]["value"] is None
        else:
            assert row["progress_status"]["status"] == "populated"
    assert not {"Hit", "Miss", "On Track", "At Risk"} & {
        str(row["progress_status"].get("value") or "") for row in rows
    }


def _comparison_guidance(*, metric: str, unit: str, horizon: str, low: float, high: float) -> dict:
    return {
        "value": {
            "value": f"{low}, {high}",
            "status": "populated",
            "source_ref": "source#guidance",
            "core": True,
            "unit": unit,
        },
        "comparison_contract": {
            "comparison_type": "range",
            "metric": metric,
            "low": low,
            "high": high,
            "unit": unit,
            "horizon": horizon,
            "source_ref": "source#guidance",
        },
        "evidence_refs": ["source#guidance"],
    }


def _comparison_actual(*, metric: str, unit: str, horizon: str, value: float) -> dict:
    return {
        "value": value,
        "status": "populated",
        "source_ref": "source#actual",
        "core": True,
        "unit": unit,
        "comparison_metric": metric,
        "comparison_horizon": horizon,
    }


def test_promise_status_cannot_infer_a_range_from_composite_prose() -> None:
    guidance = {
        "value": {
            "value": "60 openings, 20 closures; 40 remodels",
            "status": "populated",
            "source_ref": "source#composite",
            "core": True,
        },
        "comparison_contract": None,
        "evidence_refs": ["source#composite"],
    }
    status, rule_id = _progress_status(
        guidance,
        _comparison_actual(metric="real_estate_activity", unit="stores", horizon="FY2025", value=40),
    )

    assert rule_id == ""
    assert status["status"] == "manual_review_required"
    assert status["value"] is None


def test_promise_range_status_requires_same_metric_unit_and_horizon() -> None:
    guidance = _comparison_guidance(metric="revenue", unit="%", horizon="FY2025", low=5, high=7)
    valid, valid_rule = _progress_status(
        guidance,
        _comparison_actual(metric="revenue", unit="%", horizon="FY2025", value=6),
    )
    assert (valid["value"], valid_rule) == ("Within range", "actual_within_published_range")

    for actual in (
        _comparison_actual(metric="operating_margin", unit="%", horizon="FY2025", value=6),
        _comparison_actual(metric="revenue", unit="$m", horizon="FY2025", value=6),
        _comparison_actual(metric="revenue", unit="%", horizon="FY2024", value=6),
    ):
        status, rule_id = _progress_status(guidance, actual)
        assert rule_id == ""
        assert status["status"] == "manual_review_required"
        assert status["value"] is None


def test_anf_real_estate_promise_is_manual_review_while_valid_ranges_are_unchanged() -> None:
    rows = {
        row["display_metric"]["value"]: row
        for row in _package()["promise_progress"]["items"]
    }

    real_estate = rows["FY2025 Real estate activity"]
    assert real_estate["current_guidance"]["value"] == "~40 net store openings"
    assert real_estate["actual"]["value"] == 40.0
    assert real_estate["status_comparison"] == {}
    assert real_estate["progress_status"]["value"] is None
    assert real_estate["progress_status"]["status"] == "manual_review_required"
    assert real_estate["status_rule_id"] == ""

    assert rows["FY2025 Revenue"]["current_guidance"]["value"] == "at least 6%"
    assert rows["FY2025 Revenue"]["progress_status"]["value"] is None
    assert rows["FY2025 Revenue"]["progress_status"]["status"] == "manual_review_required"
    assert rows["FY2025 Revenue"]["status_rule_id"] == ""
    assert rows["FY2025 Adj EPS"]["progress_status"]["value"] == "Outside range"


def test_january_12_full_year_rows_keep_reporting_period_separate_from_horizon() -> None:
    package = _package()
    expected_full_year = {
        185: ("Revenue", "at least 6%"),
        186: ("Operating margin", "around 13%"),
        190: ("Diluted shares", "around 48 million"),
        191: ("Capex", "~ $245 million"),
        192: ("Real estate activity", "~40 net store openings"),
    }
    for row_number, (metric, value) in expected_full_year.items():
        item = _guidance_item_for_legacy_row(package, row_number)
        assert item["metric"]["value"] == metric
        assert item["value"]["value"] == value
        assert item["publication_date"] == "2026-01-12"
        assert item["stated_in_period"] == "2025-Q4"
        assert item["horizon"]["value"] == "FY2025"
        assert item["source_table_context"] == "Full Year Fiscal 2025 Outlook"

    expected_q4 = {
        193: ("Revenue", "around 5%"),
        194: ("Operating margin", "around 14%"),
        195: ("Adj EPS", "$3.50, $3.60"),
        196: ("Share repurchases", "around $100 million"),
        197: ("Diluted shares", "around 47 million"),
    }
    for row_number, (metric, value) in expected_q4.items():
        item = _guidance_item_for_legacy_row(package, row_number)
        assert item["metric"]["value"] == metric
        assert item["value"]["value"] == value
        assert item["publication_date"] == "2026-01-12"
        assert item["stated_in_period"] == "2025-Q4"
        assert item["horizon"]["value"] == "2025-Q4"
        assert "source_table_context" not in item


def test_january_12_full_year_updates_plan_to_exact_promise_q4_cells() -> None:
    package = _package()
    plan = _plan(package)
    writes = {(write.target_sheet, write.target_cell): write for write in plan.planned_writes}
    expected = {
        "F13": ("at least 6%", 185),
        "F14": ("around 13%", 186),
        "F17": ("around 48 million", 190),
        "F18": ("~ $245 million", 191),
        "F19": ("~40 net store openings", 192),
    }
    for cell, (value, row_number) in expected.items():
        write = writes[("Promise_Progress_UI", cell)]
        assert write.value == value
        assert f"!Guidance_Normalized!row:{row_number}" in write.source_ref

    assert writes[("Promise_Progress_UI", "B19")].value == "60 openings, 20 closures; 40 remodels/right-sizes"
    assert writes[("Promise_Progress_UI", "G19")].value == 40.0
    assert ("Promise_Progress_UI", "H19") not in writes
    assert ("Promise_Progress_UI", "H13") not in writes


def test_january_12_point_and_minimum_guidance_do_not_become_ranges() -> None:
    package = _package()
    for row_number in (185, 186, 190, 191, 192):
        item = _guidance_item_for_legacy_row(package, row_number)
        assert item["comparison_contract"] is None
        assert item["comparison_contract_disposition"] == "manual_review_required_no_compatible_typed_range"


def test_january_12_latest_fy2025_route_removal_is_an_explicit_parity_gap() -> None:
    package = _package()
    target = _guidance_item_for_legacy_row(package, 185)
    package["normalized_guidance"]["items"] = [
        item for item in package["normalized_guidance"]["items"] if item is not target
    ]
    entries = _guidance_parity_entries(package, {}, ANF_WORKBOOK)
    revenue = next(entry for entry in entries if entry["parity_id"] == "legacy-guidance:185:revenue:FY2025")

    assert revenue["comparison_result"] == "missing_normalized_guidance"
    assert revenue["current_status"] == "missing_or_explicitly_unavailable"
    assert revenue["normalized_package_path"] == "normalized_guidance.items[missing:185].value"


def test_semantic_validation_rejects_incompatible_populated_promise_range_status() -> None:
    package = _package()
    row = next(
        item
        for item in package["promise_progress"]["items"]
        if item["display_metric"]["value"] == "FY2025 Real estate activity"
    )
    row["status_comparison"] = {
        "comparison_type": "range",
        "metric": "real_estate_activity",
        "low": 60.0,
        "high": 20.0,
        "unit": "stores",
        "horizon": "FY2025",
        "source_ref": row["current_guidance"]["source_ref"],
    }
    row["progress_status"] = {
        "value": "Within range",
        "status": "populated",
        "source_ref": row["current_guidance"]["source_ref"],
        "core": False,
    }
    row["status_rule_id"] = "actual_within_published_range"

    rules = {issue.rule_id for issue in validate_normalized_company_data(package)}
    assert "promise_progress_range_contract_incompatible" in rules


def test_all_source_evidenced_legacy_promise_occurrences_have_explicit_dispositions() -> None:
    section = _package()["promise_progress"]
    rows = section["historical_evidence_items"]
    expected_keys = {
        "capital_expenditures:FY2020": 2,
        "capital_expenditures:FY2022": 5,
        "capital_expenditures:FY2023": 6,
        "operating_margin:FY2023": 1,
        "revenue:FY2019": 1,
        "revenue:FY2020": 1,
        "revenue:FY2022": 4,
        "tariffs:FY2019": 5,
        "tariffs:FY2020": 1,
    }

    assert len(rows) == 26
    assert Counter(row["business_key"] for row in rows) == Counter(expected_keys)
    assert Counter(row["disposition"] for row in rows) == {
        "audit_only_historical_evidence": 9,
        "duplicate_or_superseded_evidence": 5,
        "rejected_with_evidence": 12,
    }
    assert section["historical_evidence_summary"] == {
        "business_key_count": 9,
        "occurrence_count": 26,
        "disposition_counts": {
            "audit_only_historical_evidence": 9,
            "duplicate_or_superseded_evidence": 5,
            "rejected_with_evidence": 12,
        },
    }
    for row in rows:
        assert row["source_refs"] == [row["source_ref"]]
        assert row["source_document"]
        assert Path(row["source_document"]).is_file()
        assert row["source_excerpt"]
        assert row["disposition_reason"]
        assert row["disposition"] != "missing"


def test_removing_historical_promise_disposition_does_not_remove_legacy_parity_key() -> None:
    package = _package()
    original = _promise_progress_parity_entries(package, {}, ANF_WORKBOOK)
    target_id = "promise-progress:revenue:FY2020"
    assert any(row["parity_id"] == target_id for row in original)

    package["promise_progress"]["historical_evidence_items"] = [
        row
        for row in package["promise_progress"]["historical_evidence_items"]
        if row["business_key"] != "revenue:FY2020"
    ]
    mutated = _promise_progress_parity_entries(package, {}, ANF_WORKBOOK)
    target = next(row for row in mutated if row["parity_id"] == target_id)

    assert len(mutated) == len(original)
    assert target["current_status"] == "unavailable_without_adequate_evidence"
    assert target["dimensions"]["promise_parity_category"] == "unavailable_without_adequate_evidence"


def test_anf_visible_narrative_is_clean_and_source_backed() -> None:
    package = _package()
    notes = sorted(
        (item for item in package["quarter_notes"]["items"] if item["display_role"] == "current_note"),
        key=lambda item: item["display_priority"],
    )
    drivers = sorted(
        (item for item in package["operating_drivers"]["items"] if item["display_role"] == "current_watchlist"),
        key=lambda item: item["display_priority"],
    )

    assert len(notes) == 6
    assert len(drivers) == 4
    forbidden = ("operating_drivers shows", "adapter", "planner", "binding", "parser", "fact:", "read:", "{ticker}")
    for item in notes:
        assert item["evidence_refs"]
        assert item["commentary"]["source_ref"] in item["evidence_refs"]
        visible = " ".join(
            str(item[field]["value"])
            for field in ("theme", "commentary", "why_it_matters", "model_implication")
        ).casefold()
        assert not any(token in visible for token in forbidden)
    for item in drivers:
        assert item["current_read"]["source_ref"]
        visible = f"{item['topic']['value']} {item['current_read']['value']} {item['why_it_matters']['value']}".casefold()
        assert not any(token in visible for token in forbidden)


def test_anf_narrative_business_keys_plan_to_exact_cells() -> None:
    package = _package()
    plan = _plan(package)
    assert plan.status == "PASS"
    assert plan.to_dict()["overflow_count"] == 0
    assert not [issue for issue in plan.issues if issue.severity in {"P0", "P1"}]
    writes = {(write.target_sheet, write.target_cell): write for write in plan.planned_writes}

    notes = sorted(
        (item for item in package["quarter_notes"]["items"] if item["display_role"] == "current_note"),
        key=lambda item: item["display_priority"],
    )
    for row_number, item in enumerate(notes, 10):
        for column, field in (("A", "theme"), ("C", "commentary"), ("F", "why_it_matters"), ("H", "model_implication"), ("M", "source_display")):
            assert writes[("Quarter_Notes_UI", f"{column}{row_number}")].value == item[field]["value"]

    assert writes[("Operating_Drivers", "A6")].value == "Sales execution"
    assert writes[("Operating_Drivers", "B13")].normalized_path == "operating_drivers.current_outlook.current_actual_read"
    assert writes[("Operating_Drivers", "H15")].normalized_path == "operating_drivers.current_outlook.margin_bridge_use"
    assert writes[("SUMMARY", "A13")].value == "Americas"
    assert writes[("SUMMARY", "A17")].normalized_path.startswith("company_profile.key_dependencies.")
    assert writes[("SUMMARY", "A23")].normalized_path.startswith("investment_case.invalidators.")
    assert writes[("ANF_Investment_Case", "B5")].value == package["investment_case"]["summary"]["value"]
    assert writes[("ANF_Investment_Case", "B11")].value == package["investment_case"]["current_stance"]["value"]
    assert writes[("Promise_Progress_UI", "A13")].value == "FY2025 Revenue"
    assert writes[("Promise_Progress_UI", "A24")].value == "FY2024 Revenue"
    assert writes[("Promise_Progress_UI", "A39")].value == "Capex"
    assert writes[("Promise_Progress_UI", "A61")].value == "Revenue"


def test_guidance_parity_inventory_does_not_disappear_when_package_row_is_removed() -> None:
    package = _package()
    original_entries = _guidance_parity_entries(package, {}, ANF_WORKBOOK)
    target_entry = next(
        entry
        for entry in original_entries
        if entry["normalized_package_path"].startswith("normalized_guidance.items.")
    )
    package_index = int(target_entry["normalized_package_path"].split(".")[2])
    removed_signature = _guidance_signature(package["normalized_guidance"]["items"][package_index])
    package["normalized_guidance"]["items"] = [
        item
        for item in package["normalized_guidance"]["items"]
        if _guidance_signature(item) != removed_signature
    ]
    mutated_entries = _guidance_parity_entries(package, {}, ANF_WORKBOOK)

    assert len(mutated_entries) == len(original_entries)
    mutated_entry = next(entry for entry in mutated_entries if entry["parity_id"] == target_entry["parity_id"])
    assert mutated_entry["comparison_result"] == "missing_normalized_guidance"
    assert mutated_entry["current_status"] == "missing_or_explicitly_unavailable"


def test_visible_narrative_validation_rejects_internal_language_and_missing_lineage() -> None:
    package = _package()
    internal = deepcopy(package)
    internal["quarter_notes"]["items"][0]["commentary"]["value"] = "Operating_Drivers shows the parser result."
    missing_lineage = deepcopy(package)
    missing_lineage["quarter_notes"]["items"][0]["commentary"]["source_ref"] = ""
    missing_lineage["quarter_notes"]["items"][0]["commentary"]["evidence_refs"] = []
    missing_lineage["quarter_notes"]["items"][0]["evidence_refs"] = []

    internal_rules = {issue.rule_id for issue in validate_normalized_company_data(internal)}
    lineage_rules = {issue.rule_id for issue in validate_normalized_company_data(missing_lineage)}
    assert "visible_text_quality_internal_implementation_language" in internal_rules
    assert "visible_narrative_missing_evidence_refs" in lineage_rules
