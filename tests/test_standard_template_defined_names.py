from __future__ import annotations

from pathlib import Path

from scripts.build_standard_template_defined_name_audit import build_audit


ROOT = Path(__file__).resolve().parents[1]


def test_standard_shell_defined_names_are_generic_and_contract_aligned() -> None:
    audit = build_audit(
        template_path=ROOT / "templates" / "standard_stock_model_template.xlsx",
        manifest_path=ROOT / "docs" / "standard_template_shell_manifest.json",
        binding_map_path=ROOT / "docs" / "workbook_binding_map.json",
    )

    assert audit["summary"]["target_mismatch_count"] == 0
    assert audit["summary"]["company_specific_count"] == 0
    assert "ThesisBaseAdjEBITDA_FY=815.59" in audit["removed_by_materializer"]
    assert audit["summary"]["classification_counts"].get("unreferenced_constant_or_alias", 0) == 0
