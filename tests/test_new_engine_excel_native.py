from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

from pbi_xbrl.new_engine_orchestration import render_shadow, run_plan


ROOT = Path(__file__).resolve().parents[1]


def _package_path() -> Path:
    for parent in (ROOT, *ROOT.parents):
        candidate = (
            parent
            / "StockModelData"
            / "outputs"
            / "stress_tests"
            / "ANF_new_ticker_engine"
            / "ANF_normalized_data_package.json"
        )
        if candidate.exists():
            return candidate
    pytest.fail("ANF normalized package is required for the Excel-native release-path test.")


@pytest.mark.skipif(sys.platform != "win32", reason="Desktop Excel release validation is Windows-only")
def test_real_swedish_excel_roundtrip_uses_owned_process_and_leaves_no_workbook(
    tmp_path: Path,
) -> None:
    package = _package_path()
    plan = run_plan(
        run_dir=tmp_path / "plan",
        package_path=package,
        ticker="ANF",
        profile_id="full_union",
    )
    rendered: dict[str, object] | None = None
    try:
        rendered = render_shadow(
            run_dir=tmp_path / "render",
            output_root=tmp_path / "output",
            version="native-test",
            plan_receipt_path=plan["receipt_path"],
            excel_native="required",
            required_locale_id=1053,
            package_path=package,
            ticker="ANF",
            profile_id="full_union",
        )
        receipt = json.loads(Path(rendered["receipt_path"]).read_text(encoding="utf-8"))
        excel = receipt["validations"]["excel_native"]
        assert excel["status"] == "PASS"
        assert excel["locale_id"] == 1053
        assert excel["formula_error_count"] == 0
        assert excel["owned_process_cleanup"] == "PASS"
        assert isinstance(excel["owned_process_forced_termination"], bool)
        assert excel["macro_part_count"] == 0
        assert excel["external_link_part_count"] == 0
        assert excel["recovery_part_count"] == 0
        assert receipt["validations"]["post_fill"]["status"] == "PASS"
        assert receipt["validations"]["saved_workbook"]["status"] == "PASS"
        formula = receipt["formula_inventory"]
        assert formula["cell_formula_count"] == 2_141
        assert formula["function_counts"]["MAXIFS"] == 324
        assert formula["function_counts"]["MINIFS"] == 324
        assert formula["function_counts"]["LET"] == 4
        assert formula["let_local_occurrences"] == 204
        assert formula["unprefixed_future_functions"] == {}
        assert formula["unsupported_functions"] == {}
    finally:
        if rendered is not None:
            Path(rendered["output_path"]).unlink(missing_ok=True)
            Path(rendered["receipt_path"]).unlink(missing_ok=True)
    assert not list(tmp_path.rglob("*.xlsx"))
