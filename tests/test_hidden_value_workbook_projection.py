from __future__ import annotations

from copy import deepcopy
import json
import random
from collections import Counter
import hashlib
from pathlib import Path
from typing import Any

import pytest

from pbi_xbrl.hidden_value_signal_economics import evaluate_hidden_value_signals
from pbi_xbrl.hidden_value_workbook_projection import (
    HiddenValueWorkbookProjectionError,
    WORKBOOK_PROJECTION_SCHEMA,
    build_hidden_value_workbook_projection,
)
from pbi_xbrl.json_schema_validation import load_json_strict, validate_json_schema
from pbi_xbrl.new_ticker_binding_planner import reproduce_binding_plan


ROOT = Path(__file__).resolve().parents[1]
PERIODS = [f"{year}-Q{quarter}" for year in (2024, 2025) for quarter in range(1, 5)]


def _source_row(period: str, metric: str, value: float, unit: str) -> dict[str, Any]:
    return {
        "period": period,
        "metric": metric,
        "value": value,
        "unit": unit,
        "status": "source_backed",
        "source_ref": f"fixture:{metric}:{period}",
    }


def _economic_package() -> dict[str, Any]:
    series = {
        "revenue": ([100.0] * 4 + [150.0] * 4, "$m"),
        "operating_income": ([10.0] * 4 + [13.0] * 4, "$m"),
        "base_ebitda": ([15.0] * 4 + [18.75] * 4, "$m"),
        "adjusted_ebitda": ([20.0] * 4 + [37.5] * 4, "$m"),
        "operating_cash_flow": ([20.0] * 4 + [40.0] * 4, "$m"),
        "capital_expenditures": ([5.0] * 8, "$m"),
        "interest_expense": ([4.0] * 4 + [3.0] * 4, "$m"),
        "shares_outstanding": ([100.0] * 4 + [99.5, 99.0, 98.0, 97.0], "m shares"),
        "market_cap": ([700.0] * 4 + [500.0] * 4, "$m"),
        "net_debt": ([300.0] * 4 + [280.0, 250.0, 225.0, 200.0], "$m"),
        "dividend_per_share": ([0.10] * 4 + [0.11, 0.11, 0.12, 0.12], "$/share"),
    }
    return {
        "ticker_metadata": {"ticker": "TEST"},
        "calculation_history": {
            "quarterly_items": [
                _source_row(period, metric, values[index], unit)
                for metric, (values, unit) in series.items()
                for index, period in enumerate(PERIODS)
            ]
        },
        "annual_financials": {"rows": []},
        "valuation_inputs": {
            "price": {
                "value": None,
                "unit": "$/share",
                "period": "2025-Q4",
                "status": "missing_source",
                "source_ref": "fixture:price:missing",
            }
        },
    }


def _projection(package: dict[str, Any] | None = None, *, profile_id: str = "anf") -> dict[str, Any]:
    evaluation = evaluate_hidden_value_signals(
        package or _economic_package(),
        profile_id=profile_id,
        ticker="TEST",
    )
    return build_hidden_value_workbook_projection(evaluation).to_dict()


def _digest(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def test_projection_is_schema_valid_typed_and_complete() -> None:
    payload = _projection()

    assert validate_json_schema(payload, load_json_strict(WORKBOOK_PROJECTION_SCHEMA)) == []
    assert len(payload["base_rows"]) == 59
    assert len(payload["audit_rows"]) == 7
    assert len(payload["recompute_rows"]) == 91
    assert len(payload["flags_rows"]) == 7
    assert Counter(row["record_type"] for row in payload["recompute_rows"]) == {
        "required_metric": 28,
        "predicate": 45,
        "score_component": 18,
    }
    assert [row["signal_id"] for row in payload["flags_rows"]] == ["C", "B", "D", "G", "E", "F", "A"]
    assert {row["signal_id"]: row["score"] for row in payload["flags_rows"]} == {
        "A": 41,
        "B": 82,
        "C": 92,
        "D": 71,
        "E": 61,
        "F": 49,
        "G": 69,
    }
    assert len({row["metric_key"] for row in payload["base_rows"]}) == 59
    assert len({row["record_key"] for row in payload["recompute_rows"]}) == 91
    assert all(
        json.loads(row["evidence_ids"]) or json.loads(row["formula_ids"])
        for row in payload["base_rows"]
    )
    assert all(json.loads(row["source_refs"]) for row in payload["audit_rows"])


def test_projection_is_independent_of_source_row_order() -> None:
    package = _economic_package()
    shuffled = deepcopy(package)
    random.Random(20260718).shuffle(shuffled["calculation_history"]["quarterly_items"])

    assert _projection(package) == _projection(shuffled)


def test_projection_rejects_duplicate_metric_keys_and_contract_drift() -> None:
    evaluation = evaluate_hidden_value_signals(_economic_package(), profile_id="anf", ticker="TEST")
    evaluation.base_projection.append(dict(evaluation.base_projection[0]))
    with pytest.raises(HiddenValueWorkbookProjectionError, match="Duplicate Hidden Value base metric key"):
        build_hidden_value_workbook_projection(evaluation)

    evaluation = evaluate_hidden_value_signals(_economic_package(), profile_id="anf", ticker="TEST")
    evaluation.contract_digest = "0" * 64
    with pytest.raises(HiddenValueWorkbookProjectionError, match="contract digest differs"):
        build_hidden_value_workbook_projection(evaluation)


def test_disabled_profile_has_no_hidden_value_workbook_projection_rows() -> None:
    payload = _projection(profile_id="core_only")

    assert payload["base_rows"] == []
    assert payload["audit_rows"] == []
    assert payload["recompute_rows"] == []
    assert payload["flags_rows"] == []


def test_workbook_projection_is_a_thin_non_economic_mapper() -> None:
    source = (ROOT / "pbi_xbrl" / "hidden_value_workbook_projection.py").read_text(encoding="utf-8")

    for forbidden in (
        "_evaluate_signal(",
        "_score_components(",
        "_normalized_score(",
        "openpyxl",
        "load_workbook",
        "if ticker",
    ):
        assert forbidden not in source.lower()


def test_current_anf_projection_fails_closed_without_triggered_flags() -> None:
    data_root = next(parent / "StockModelData" for parent in (ROOT, *ROOT.parents) if (parent / "StockModelData").exists())
    package_path = data_root / "outputs" / "stress_tests" / "ANF_new_ticker_engine" / "ANF_normalized_data_package.json"
    if not package_path.exists():
        pytest.skip(f"{package_path} is unavailable")

    evaluation = evaluate_hidden_value_signals(
        json.loads(package_path.read_text(encoding="utf-8")),
        profile_id="anf",
        ticker="ANF",
    )
    payload = build_hidden_value_workbook_projection(evaluation).to_dict()

    assert (len(payload["base_rows"]), len(payload["audit_rows"]), len(payload["recompute_rows"])) == (56, 7, 91)
    assert payload["flags_rows"] == []
    assert {row["signal_id"]: row["expected_state"] for row in payload["audit_rows"]} == {
        "A": "insufficient_evidence",
        "B": "not_triggered",
        "C": "insufficient_evidence",
        "D": "insufficient_evidence",
        "E": "insufficient_evidence",
        "F": "insufficient_evidence",
        "G": "unavailable",
    }


def test_anf_planner_adds_only_exact_hidden_value_support_writes() -> None:
    data_root = next(parent / "StockModelData" for parent in (ROOT, *ROOT.parents) if (parent / "StockModelData").exists())
    package_path = data_root / "outputs" / "stress_tests" / "ANF_new_ticker_engine" / "ANF_normalized_data_package.json"
    if not package_path.exists():
        pytest.skip(f"{package_path} is unavailable")
    package = json.loads(package_path.read_text(encoding="utf-8"))
    original_package = deepcopy(package)
    plan = reproduce_binding_plan(
        package,
        binding_payload=json.loads((ROOT / "docs" / "workbook_binding_map.json").read_text(encoding="utf-8")),
        manifest=json.loads((ROOT / "docs" / "standard_template_shell_manifest.json").read_text(encoding="utf-8")),
        shell_path=ROOT / "templates" / "standard_stock_model_template.xlsx",
        ticker_override="ANF",
    ).to_dict()
    hidden_bindings = {
        "hidden_value_base_rows",
        "hidden_value_audit_rows",
        "hidden_value_recompute_rows",
        "hidden_value_flags_rows",
    }
    hidden_writes = [row for row in plan["planned_writes"] if row["binding_id"] in hidden_bindings]
    accepted_writes = [row for row in plan["planned_writes"] if row["binding_id"] not in hidden_bindings]

    assert package == original_package
    assert "_derived_workbook" not in package
    assert plan["status"] == "PASS"
    assert plan["planned_write_count"] == 22_824
    assert plan["structured_skip_count"] == 2_413
    assert plan["overflow_count"] == 0
    assert Counter(row["binding_id"] for row in hidden_writes) == {
        "hidden_value_base_rows": 700,
        "hidden_value_audit_rows": 107,
        "hidden_value_recompute_rows": 1_176,
    }
    assert len(hidden_writes) == 1_983
    assert len(accepted_writes) == 20_841
    assert _digest(accepted_writes) == "11ad44f774f1d2ec2e40cfcaa694f0cba264389cb81bd586f83228d809d14a46"
    assert _digest(plan["issue_ledger"]) == "0d0b35dbe5e912088590bece30aa60196ac26933492de5adb420dd9b6e67c9aa"
    assert len(plan["derived_plans"]) == 1
    report = plan["derived_plans"][0]
    assert report["plan_id"] == "hidden_value_evaluation"
    assert report["status"] == "PASS"
    assert report["candidate_count"] == 7
    assert report["base_row_count"] == 56
    assert report["audit_row_count"] == 7
    assert report["recompute_row_count"] == 91
    assert report["flags_row_count"] == 0
    assert report["state_counts"] == {
        "insufficient_evidence": 5,
        "not_triggered": 1,
        "unavailable": 1,
    }
    assert all(len(report[field]) == 64 for field in (
        "contract_digest", "evaluation_plan_digest", "workbook_projection_digest",
    ))
