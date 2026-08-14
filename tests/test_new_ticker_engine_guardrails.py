from __future__ import annotations

import ast
from pathlib import Path

from pbi_xbrl import workbook_validation_runner


ROOT = Path(__file__).resolve().parents[1]


def test_root_default_validation_tickers_remain_canonical_three() -> None:
    assert tuple(workbook_validation_runner.TICKERS) == ("PBI", "GPRE", "ANF")


def test_new_ticker_architecture_entrypoints_do_not_import_production_workbook_writers() -> None:
    """Enforce the lasting runtime boundary, independent of the caller's worktree.

    The retired test inspected every unrelated dirty path in ``git diff`` and
    therefore described one historical change set rather than the architecture.
    The public architecture contract is that the shell/value-only entrypoints do
    not import the legacy production writer, source pipeline, or ticker profile
    runtime.
    """

    architecture_paths = (
        ROOT / "scripts" / "fill_standard_template_from_normalized_package.py",
        ROOT / "scripts" / "fill_anf_shadow_workbook.py",
        ROOT / "pbi_xbrl" / "new_ticker_value_filler.py",
    )
    forbidden_modules = {
        "pbi_xbrl.excel_writer",
        "pbi_xbrl.pipeline",
        "pbi_xbrl.company_profiles",
        "pbi_xbrl.quarter_notes",
        "pbi_xbrl.sec_xbrl",
        "pbi_xbrl.summary_overview",
        "stock_models",
    }

    imported_by_path: dict[str, set[str]] = {}
    for path in architecture_paths:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imports = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imports.update(
            str(node.module or "")
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        )
        imported_by_path[path.relative_to(ROOT).as_posix()] = imports

    violations = {
        path: sorted(
            module
            for module in imports
            if any(module == forbidden or module.startswith(f"{forbidden}.") for forbidden in forbidden_modules)
        )
        for path, imports in imported_by_path.items()
    }
    assert violations == {path: [] for path in imported_by_path}
