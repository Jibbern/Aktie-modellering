from __future__ import annotations

import json
import re
from pathlib import Path

from pbi_xbrl.normalized_company_data_validation import (
    build_normalized_text_quality_audit,
    validate_normalized_company_data,
)
from scripts.build_anf_shadow_normalized_package import build_anf_shadow_outputs


ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = next(
    ancestor / "StockModelData"
    for ancestor in [ROOT, *ROOT.parents]
    if (ancestor / "StockModelData").exists()
)
ANF_WORKBOOK = DATA_ROOT / "outputs" / "Excel stock models" / "ANF_model.xlsx"


def test_anf_shadow_package_reports_are_built_from_read_only_legacy_artifacts(tmp_path: Path) -> None:
    output_dir = tmp_path / "ANF_new_ticker_engine"

    paths = build_anf_shadow_outputs(
        data_root=DATA_ROOT,
        workbook_path=ANF_WORKBOOK,
        output_dir=output_dir,
        docs_dir=tmp_path / "docs",
    )

    package_path = paths["package"]
    mapping_path = paths["mapping_gaps"]
    validation_path = paths["validation"]
    source_audit_path = paths["source_audit_json"]
    coverage_path = paths["binding_coverage_json"]
    text_quality_path = paths["text_quality_json"]

    for path in (package_path, mapping_path, validation_path, source_audit_path, coverage_path, text_quality_path):
        assert path.exists()

    package = json.loads(package_path.read_text(encoding="utf-8"))
    mapping = json.loads(mapping_path.read_text(encoding="utf-8"))
    validation = json.loads(validation_path.read_text(encoding="utf-8"))
    source_audit = json.loads(source_audit_path.read_text(encoding="utf-8"))
    coverage = json.loads(coverage_path.read_text(encoding="utf-8"))
    text_quality = json.loads(text_quality_path.read_text(encoding="utf-8"))

    assert package["ticker_metadata"]["ticker"]["value"] == "ANF"
    assert package["company_profile"]["company_name"]["value"] == "Abercrombie & Fitch Co."
    assert len(package["quarterly_financials"]["rows"]) >= 8
    assert package["quarterly_financials"]["rows"][-1]["revenue"]["status"] == "populated"
    assert len(package["annual_financials"]["rows"]) >= 3
    assert package["debt_liquidity"]["cash"]["status"] == "populated"
    assert len(package["normalized_guidance"]["items"]) >= 8
    assert len(package["segments"]["items"]) >= 6
    assert len(package["operating_drivers"]["items"]) >= 4
    assert len(package["quarter_notes"]["items"]) >= 8
    assert package["source_coverage"]["sources"]

    assert mapping["ticker"] == "ANF"
    assert isinstance(mapping["gaps"], list)
    assert validation["ticker"] == "ANF"
    assert not [issue for issue in validation["issues"] if issue["severity"] in {"P0", "P1"}]
    assert validate_normalized_company_data(package) == []

    audited_sections = {row["section"] for row in source_audit["sections"]}
    assert {
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
    } <= audited_sections

    binding_ids = {row["binding_id"] for row in coverage["bindings"]}
    expected_binding_ids = {
        entry["binding_id"]
        for entry in json.loads((ROOT / "docs" / "workbook_binding_map.json").read_text(encoding="utf-8"))["bindings"]
    }
    assert binding_ids == expected_binding_ids
    assert any(row["would_write_useful_output"] for row in coverage["bindings"])
    assert text_quality["non_clean_visible_count"] == 0
    assert not (output_dir / "ANF_model.xlsx").exists()


def test_anf_binding_coverage_reports_row_schema_capacity(tmp_path: Path) -> None:
    paths = build_anf_shadow_outputs(
        data_root=DATA_ROOT,
        workbook_path=ANF_WORKBOOK,
        output_dir=tmp_path / "ANF_new_ticker_engine",
        docs_dir=tmp_path / "docs",
    )

    coverage = json.loads(paths["binding_coverage_json"].read_text(encoding="utf-8"))
    rows = {row["binding_id"]: row for row in coverage["bindings"]}

    guidance = rows["pp_annual_guidance_rows"]
    quarter_notes = rows["qn_quarter_note_rows"]
    operating_drivers = rows["od_watchlist_rows"]
    qa_rows = rows["qa_checks_mapping_gap_rows"]

    assert set(guidance["row_schema_columns"]) >= {
        "metric",
        "initial_guide",
        "q1_update",
        "q2_update",
        "q3_update",
        "q4_update",
        "actual",
        "status",
        "notes_source",
    }
    assert guidance["number_of_values_available"] >= 1
    assert set(quarter_notes["row_schema_columns"]) >= {
        "theme",
        "quarter",
        "metric",
        "commentary",
        "model_implication",
        "source",
    }
    assert set(operating_drivers["row_schema_columns"]) >= {"topic", "current_read", "source", "why_it_matters"}
    assert set(qa_rows["row_schema_columns"]) >= {
        "severity",
        "rule_id",
        "field",
        "message",
        "source_ref",
        "suggested_action",
    }


def test_anf_shadow_package_demotes_noisy_visible_text(tmp_path: Path) -> None:
    paths = build_anf_shadow_outputs(
        data_root=DATA_ROOT,
        workbook_path=ANF_WORKBOOK,
        output_dir=tmp_path / "ANF_new_ticker_engine",
        docs_dir=tmp_path / "docs",
    )
    package = json.loads(paths["package"].read_text(encoding="utf-8"))
    text_quality = json.loads(paths["text_quality_json"].read_text(encoding="utf-8"))

    visible_blob = "\n".join(_visible_text_values(package))
    assert not re.search(r"compensation|governance|director|board|officer|restricted stock", visible_blob, re.I)
    assert not re.search(r"forward-looking|safe harbor|risk factors|trade policies or arrangements", visible_blob, re.I)
    assert "Gross profit divided by reported net sales" not in visible_blob
    assert "Operating income divided by reported net sales" not in visible_blob
    assert "REPORTS THIRD QUARTER" not in visible_blob
    assert not re.search(r"[-–]\s*$|\b(and|of|the|to|from|with)\s*$", visible_blob, re.I | re.M)

    demotions = package["source_coverage"].get("text_quality_demotions", [])
    assert demotions
    assert any(flag["rule_id"] == "text_quality_demoted" for flag in package["manual_review_flags"])
    assert text_quality["demotion_summary"]["total_demoted"] == len(demotions)
    assert build_normalized_text_quality_audit(package)["non_clean_visible_count"] == 0


def _visible_text_values(package: dict) -> list[str]:
    values: list[str] = []
    for item in package.get("quarter_notes", {}).get("items", []):
        for key in ("note", "commentary", "model_implication", "valuation_implication"):
            values.append(_field_text(item.get(key)))
    for item in package.get("operating_drivers", {}).get("items", []):
        for key in ("driver", "current_read", "why_it_matters"):
            values.append(_field_text(item.get(key)))
    for item in package.get("segments", {}).get("items", []):
        values.append(_field_text(item.get("note")))
    for item in package.get("normalized_guidance", {}).get("items", []):
        values.append(str(item.get("source_excerpt") or ""))
    return [value for value in values if value]


def _field_text(value) -> str:
    if isinstance(value, dict):
        return str(value.get("value") or "")
    return str(value or "")
