from __future__ import annotations

import ast
import json
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
INVENTORY_PATH = ROOT / "tests" / "fixtures" / "native_excel_test_inventory.json"
EXPECTED_NATIVE_TEST_IDS = (
    "tests/test_hidden_value_workbook_formula_contract.py::test_excel_native_recompute_matches_independent_a_to_g_oracle_and_blocks_mutations",
    "tests/test_new_engine_excel_native.py::test_real_swedish_excel_investment_case_guards_unavailable_and_zero_domains",
    "tests/test_new_engine_excel_native.py::test_real_swedish_excel_roundtrip_uses_owned_process_and_leaves_no_workbook",
    "tests/test_new_engine_promotion_integration.py::test_real_swedish_excel_validates_isolated_rollback_source",
    "tests/test_new_ticker_style_pipeline.py::test_swedish_excel_native_recalculation_preserves_formula_and_protection_contracts",
    "tests/test_render_and_style_validation.py::test_real_excel_renders_protected_source_via_owned_scratch_workbook",
)


def _strict_json(path: Path) -> dict[str, Any]:
    duplicate_keys: list[str] = []

    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        duplicate_keys.extend(key for key, count in Counter(key for key, _value in pairs).items() if count > 1)
        return dict(pairs)

    payload = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=object_pairs)
    assert not duplicate_keys, f"Duplicate native-inventory JSON keys: {sorted(set(duplicate_keys))}"
    assert isinstance(payload, dict)
    return payload


def _decorator_name(node: ast.expr) -> str:
    if isinstance(node, ast.Call):
        return _decorator_name(node.func)
    if isinstance(node, ast.Attribute):
        prefix = _decorator_name(node.value)
        return f"{prefix}.{node.attr}" if prefix else node.attr
    if isinstance(node, ast.Name):
        return node.id
    return ""


def _native_marked_test_ids() -> tuple[str, ...]:
    marked: list[str] = []
    for path in sorted((ROOT / "tests").glob("test_*.py"), key=lambda item: item.as_posix()):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        relative = path.relative_to(ROOT).as_posix()
        for node in tree.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or not node.name.startswith("test_"):
                continue
            decorators = {_decorator_name(decorator) for decorator in node.decorator_list}
            if "pytest.mark.native_excel" in decorators:
                marked.append(f"{relative}::{node.name}")
    return tuple(sorted(marked))


def _direct_com_startup_test_ids() -> tuple[str, ...]:
    """Find tests that directly import/start desktop Excel, excluding fake COM unit tests."""

    direct: list[str] = []
    for path in sorted((ROOT / "tests").glob("test_*.py"), key=lambda item: item.as_posix()):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        relative = path.relative_to(ROOT).as_posix()
        for node in tree.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or not node.name.startswith("test_"):
                continue
            body = ast.get_source_segment(path.read_text(encoding="utf-8"), node) or ""
            if 'importorskip("win32com.client")' in body or "importorskip('win32com.client')" in body:
                direct.append(f"{relative}::{node.name}")
    return tuple(sorted(direct))


def test_native_excel_inventory_is_exact_unique_and_deterministic() -> None:
    payload = _strict_json(INVENTORY_PATH)
    test_ids = payload["test_ids"]
    assert payload["contract_id"] == "contract:repository-native-excel-test-boundary@1"
    assert payload["marker"] == "native_excel"
    assert isinstance(test_ids, list)
    assert test_ids == sorted(test_ids)
    assert len(test_ids) == len(set(test_ids)) == 6
    assert tuple(test_ids) == EXPECTED_NATIVE_TEST_IDS


def test_native_excel_marker_membership_matches_machine_readable_inventory() -> None:
    assert _native_marked_test_ids() == EXPECTED_NATIVE_TEST_IDS


def test_direct_com_startup_tests_are_declared_native_before_execution() -> None:
    direct_ids = _direct_com_startup_test_ids()
    assert direct_ids == (
        "tests/test_hidden_value_workbook_formula_contract.py::test_excel_native_recompute_matches_independent_a_to_g_oracle_and_blocks_mutations",
        "tests/test_new_ticker_style_pipeline.py::test_swedish_excel_native_recalculation_preserves_formula_and_protection_contracts",
    )
    assert set(direct_ids) <= set(EXPECTED_NATIVE_TEST_IDS)
