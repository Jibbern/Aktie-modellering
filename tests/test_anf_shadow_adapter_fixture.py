from __future__ import annotations

import ast
from pathlib import Path

import pytest

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
BUILDER = ROOT / "scripts" / "build_anf_shadow_normalized_package.py"


def _resolve_package_path(package: object, path: str) -> object:
    current = package
    for token in path.split("."):
        if isinstance(current, list):
            current = current[int(token)]
        elif isinstance(current, dict):
            current = current[token]
        else:
            raise AssertionError(f"Cannot resolve {path!r} through {type(current).__name__}")
    return current


def test_anf_shadow_builder_is_marked_as_legacy_adapter_fixture() -> None:
    source = BUILDER.read_text(encoding="utf-8")
    module = ast.parse(source)
    assignments = {
        target.id: ast.literal_eval(node.value)
        for node in module.body
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name)
        and target.id in {"LEGACY_WORKBOOK_ADAPTER_FIXTURE", "GENERIC_SOURCE_NATIVE_BUILDER"}
    }

    assert assignments["LEGACY_WORKBOOK_ADAPTER_FIXTURE"] is True
    assert assignments["GENERIC_SOURCE_NATIVE_BUILDER"] is False
    assert "migration fixture" in source.lower()
    assert "not the generic source-native package path" in source.lower()


def test_anf_legacy_adapter_builds_schema_valid_package_without_writing() -> None:
    data_root = _default_data_root()
    workbook_path = _default_workbook_path(data_root)
    if not workbook_path.exists():
        pytest.skip(f"Read-only ANF migration fixture is unavailable: {workbook_path}")

    package = build_anf_normalized_package(data_root=data_root, workbook_path=workbook_path)

    assert validate_normalized_company_data_schema(package) == []
    assert validate_normalized_company_data(package) == []
    assert all(row["period"].endswith("-FY") for row in package["annual_financials"]["rows"])
    assert all(isinstance(row["note"], dict) for row in package["segments"]["items"])
    assert package["source_coverage"]["legacy_adapter_truncations"]
    assert package["source_coverage"]["legacy_adapter_deduplications"]
    assert package["source_coverage"]["text_quality_demotions"]
    assert all(
        row["rule_id"] == "text_quality_demoted"
        for row in package["source_coverage"]["text_quality_demotions"]
    )
    assert len(package["source_coverage"]["text_quality_demotions"]) < len(
        package["manual_review_flags"]
    )
    detail_paths: list[str] = []
    lineage_ids: list[str] = []
    for record in package["source_coverage"]["legacy_adapter_truncations"]:
        assert {"collection", "input_rows", "retained_rows", "dropped_rows", "reason", "source_ref"} <= set(record)
        assert record["excluded_row_count"] == record["dropped_rows"] == len(record["excluded_rows"])
        assert all(
            {
                "collection",
                "section",
                "detail_path",
                "adapter_candidate_path",
                "lineage_id",
                "source_index",
                "business_row_key",
                "period",
                "source_ref",
                "source_refs",
                "truncation_rule",
                "reason",
            }
            <= set(row)
            for row in record["excluded_rows"]
        )
        assert all(row["business_row_key"] and row["source_ref"] for row in record["excluded_rows"])
        assert all("normalized_path" not in row for row in record["excluded_rows"])
        for row in record["excluded_rows"]:
            assert row["adapter_candidate_path"].startswith(
                f"legacy_adapter_candidates.{row['collection']}."
            )
            resolved = _resolve_package_path(package, row["detail_path"])
            assert resolved["lineage_id"] == row["lineage_id"]
            assert resolved["source_ref"] == row["source_ref"]
            detail_paths.append(row["detail_path"])
            lineage_ids.append(row["lineage_id"])

    truncations = package["source_coverage"]["legacy_adapter_truncations"]
    assert {record["collection"] for record in truncations} == {
        "quarterly_financials.rows",
        "operating_drivers.items",
    }
    dropped_total = sum(record["dropped_rows"] for record in truncations)
    assert dropped_total > 0
    assert sum(len(record["excluded_rows"]) for record in truncations) == dropped_total
    assert len(detail_paths) == len(set(detail_paths)) == dropped_total
    assert len(lineage_ids) == len(set(lineage_ids)) == dropped_total
    reviews = [row for row in package["manual_review_flags"] if row.get("rule_id") == "legacy_adapter_truncation"]
    assert sum(int(row["adapter_metadata"]["dropped_rows"]) for row in reviews) == dropped_total
    assert all(str(row["adapter_metadata"]["detail_ref"]).endswith(".excluded_rows") for row in reviews)
    assert all(isinstance(_resolve_package_path(package, row["adapter_metadata"]["detail_ref"]), list) for row in reviews)
