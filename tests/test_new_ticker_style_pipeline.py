from __future__ import annotations

from copy import copy
import json
from pathlib import Path
import shutil
from typing import Any

from openpyxl import load_workbook
from openpyxl.styles import PatternFill
import pytest

from pbi_xbrl.new_ticker_style_planner import load_style_policy_contract, reproduce_style_plan
from pbi_xbrl.new_ticker_value_filler import fill_standard_template_from_package
from pbi_xbrl.standard_template_shell_identity import verify_post_fill_structural_identity
from pbi_xbrl.workbook_modules import load_workbook_module_manifest


ROOT = Path(__file__).resolve().parents[1]
SHELL = ROOT / "templates" / "standard_stock_model_template.xlsx"
MANIFEST = ROOT / "docs" / "standard_template_shell_manifest.json"
BINDING_MAP = ROOT / "docs" / "workbook_binding_map.json"


def _json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _package_path() -> Path:
    for parent in (ROOT, *ROOT.parents):
        candidate = parent / "StockModelData" / "outputs" / "stress_tests" / "ANF_new_ticker_engine" / "ANF_normalized_data_package.json"
        if candidate.exists():
            return candidate
    pytest.skip("ANF normalized style fixture is unavailable.")


@pytest.fixture(scope="module")
def filled_anf_style_workbook(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    output = tmp_path_factory.mktemp("anf-style-fill") / "ANF_style_pipeline.xlsx"
    package_path = _package_path()
    result = fill_standard_template_from_package(package_path, output_path=output)
    package = _json(package_path)
    binding_payload = _json(BINDING_MAP)
    manifest = _json(MANIFEST)
    modules = load_workbook_module_manifest()
    styles = load_style_policy_contract(module_payload=modules, binding_payload=binding_payload)
    value_plan, style_plan = reproduce_style_plan(
        package,
        binding_payload=binding_payload,
        manifest=manifest,
        shell_path=SHELL,
        module_payload=modules,
        style_contract=styles,
    )
    return {
        "output": output,
        "result": result,
        "package": package,
        "binding": binding_payload,
        "manifest": manifest,
        "modules": modules,
        "styles": styles,
        "value_plan": value_plan,
        "style_plan": style_plan,
    }


def _non_fill_style(cell: Any) -> dict[str, Any]:
    return {
        "font": copy(cell.font),
        "border": copy(cell.border),
        "alignment": copy(cell.alignment),
        "protection": copy(cell.protection),
        "number_format": cell.number_format,
    }


def _conditional_formatting_signature(worksheet: Any) -> list[tuple[str, tuple[tuple[str, tuple[str, ...]], ...]]]:
    return [
        (
            str(key.sqref),
            tuple(
                (str(rule.type or ""), tuple(str(formula) for formula in (rule.formula or [])))
                for rule in worksheet.conditional_formatting[key]
            ),
        )
        for key in worksheet.conditional_formatting
    ]


def test_public_filler_applies_exact_reproduced_style_plan_after_values(
    filled_anf_style_workbook: dict[str, Any],
) -> None:
    artifacts = filled_anf_style_workbook
    result = artifacts["result"]
    assert result.written_cell_count == 22_824
    assert result.styled_cell_count == 824

    shell = load_workbook(SHELL, data_only=False, read_only=False)
    filled = load_workbook(artifacts["output"], data_only=False, read_only=False)
    try:
        assert filled["Valuation"]["B9"].fill.fgColor.rgb[-6:] == "2F80ED"
        assert filled["Valuation"]["B70"].fill.fill_type is None
        assert _non_fill_style(filled["Valuation"]["B9"]) == _non_fill_style(shell["Valuation"]["B9"])
        assert _conditional_formatting_signature(filled["Valuation"]) == _conditional_formatting_signature(
            shell["Valuation"]
        )
    finally:
        filled.close()
        shell.close()


def test_strict_post_fill_accepts_only_the_reproduced_style_plan(
    filled_anf_style_workbook: dict[str, Any],
) -> None:
    artifacts = filled_anf_style_workbook
    report = verify_post_fill_structural_identity(
        artifacts["output"],
        approved_shell_path=SHELL,
        manifest=artifacts["manifest"],
        binding_payload=artifacts["binding"],
        approved_plan=artifacts["value_plan"],
        normalized_package=artifacts["package"],
        module_payload=artifacts["modules"],
        style_contract=artifacts["styles"],
        approved_style_plan=artifacts["style_plan"],
    )

    assert report["status"] == "PASS", report["issues"][:10]
    assert report["reproduced_style_action_count"] == 824


def test_strict_post_fill_rejects_one_unplanned_style_mutation(
    filled_anf_style_workbook: dict[str, Any], tmp_path: Path
) -> None:
    artifacts = filled_anf_style_workbook
    drifted = tmp_path / "unplanned-style.xlsx"
    shutil.copyfile(artifacts["output"], drifted)
    workbook = load_workbook(drifted, data_only=False, read_only=False)
    try:
        workbook["Valuation"]["B70"].fill = PatternFill(fill_type="solid", fgColor="2F80ED")
        workbook.save(drifted)
    finally:
        workbook.close()

    report = verify_post_fill_structural_identity(
        drifted,
        approved_shell_path=SHELL,
        manifest=artifacts["manifest"],
        binding_payload=artifacts["binding"],
        approved_plan=artifacts["value_plan"],
        normalized_package=artifacts["package"],
        module_payload=artifacts["modules"],
        style_contract=artifacts["styles"],
        approved_style_plan=artifacts["style_plan"],
    )

    assert report["status"] == "FAIL"
    assert "post_fill_protected_cell_drift" in {row["rule_id"] for row in report["issues"]}
