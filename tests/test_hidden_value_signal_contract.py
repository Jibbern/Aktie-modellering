from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from pbi_xbrl.hidden_value_signal_economics import (
    DEFAULT_HIDDEN_VALUE_CONTRACT,
    HIDDEN_VALUE_CONTRACT_SCHEMA,
    HiddenValueContractError,
    evaluate_hidden_value_signals,
    load_hidden_value_signal_contract,
    validate_hidden_value_signal_contract,
)
from pbi_xbrl.json_schema_validation import load_json_strict, validate_json_schema
from pbi_xbrl.workbook_modules import load_workbook_module_manifest


ROOT = Path(__file__).resolve().parents[1]


def _minimal_package() -> dict[str, object]:
    return {
        "ticker_metadata": {"ticker": "TEST"},
        "calculation_history": {
            "quarterly_items": [
                {
                    "period": "2025-Q4",
                    "metric": "revenue",
                    "value": 100.0,
                    "unit": "$m",
                    "status": "source_backed",
                    "source_ref": "fixture:revenue:2025-Q4",
                }
            ]
        },
        "annual_financials": {"rows": []},
        "valuation_inputs": {},
    }


def test_hidden_value_contract_is_schema_valid_and_complete() -> None:
    raw = load_json_strict(DEFAULT_HIDDEN_VALUE_CONTRACT)
    assert validate_json_schema(raw, load_json_strict(HIDDEN_VALUE_CONTRACT_SCHEMA)) == []

    contract = load_hidden_value_signal_contract()
    assert contract["contract_version"] == "1.0.0"
    assert [row["signal_id"] for row in contract["signals"]] == list("ABCDEFG")
    assert len(contract["metric_resolvers"]) == 40
    assert set(contract["states"]) == {
        "triggered",
        "near_miss",
        "not_triggered",
        "unavailable",
        "insufficient_evidence",
        "invalid_input",
    }


def test_contract_owns_signal_rules_without_production_writer_dependency() -> None:
    source = (ROOT / "pbi_xbrl" / "hidden_value_signal_economics.py").read_text(encoding="utf-8")
    assert "pbi_xbrl.signals" not in source
    assert "excel_writer_hidden_value" not in source
    assert "openpyxl" not in source and "load_workbook(" not in source and "data_only" not in source
    assert "if ticker" not in source.lower()
    assert "ANF" not in source and "PBI" not in source and "GPRE" not in source

    contract = load_hidden_value_signal_contract()
    signal_b = next(row for row in contract["signals"] if row["signal_id"] == "B")
    components = {row["component_id"]: row for row in signal_b["score_components"]}
    assert components["margin_streak_score"]["weight"] == 25
    signal_c = next(row for row in contract["signals"] if row["signal_id"] == "C")
    assert "positive_fcf_ttm_observations" in signal_c["required_metric_ids"]
    assert "positive-years" not in json.dumps(signal_c).lower()


def test_hidden_value_module_has_only_structural_dependencies() -> None:
    manifest = load_workbook_module_manifest()
    module = next(row for row in manifest["modules"] if row["module_id"] == "hidden_value_signals")
    assert module["dependencies"] == ["core_financial_history", "balance_cash_flow", "qa_lineage"]
    assert "debt_liquidity" not in module["dependencies"]
    signal_d = next(row for row in load_hidden_value_signal_contract()["signals"] if row["signal_id"] == "D")
    assert "debt_liquidity" in signal_d["required_modules"]


def test_contract_rejects_duplicate_signal_and_metric_ids() -> None:
    contract = load_hidden_value_signal_contract()
    manifest = load_workbook_module_manifest()

    duplicate_signal = copy.deepcopy(contract)
    duplicate_signal["signals"].append(copy.deepcopy(duplicate_signal["signals"][0]))
    issues = validate_hidden_value_signal_contract(duplicate_signal, manifest)
    assert any("Duplicate signal IDs" in issue for issue in issues)

    duplicate_metric = copy.deepcopy(contract)
    duplicate_metric["metric_resolvers"].append(copy.deepcopy(duplicate_metric["metric_resolvers"][0]))
    issues = validate_hidden_value_signal_contract(duplicate_metric, manifest)
    assert any("Duplicate metric resolver IDs" in issue for issue in issues)


def test_contract_rejects_unknown_metric_module_pack_and_cycle() -> None:
    contract = load_hidden_value_signal_contract()
    manifest = load_workbook_module_manifest()
    mutated = copy.deepcopy(contract)
    mutated["signals"][0]["required_modules"].append("missing_module")
    mutated["signals"][0]["profile_pack_ids"].append("missing_pack")
    mutated["signals"][0]["required_metric_ids"].append("missing_metric")
    mutated["metric_resolvers"][0]["resolver"] = {
        "kind": "lagged",
        "input_metric_id": "ebit_ttm",
        "lag": 1,
    }

    issues = validate_hidden_value_signal_contract(mutated, manifest)
    assert any("unknown modules" in issue for issue in issues)
    assert any("unknown profile packs" in issue for issue in issues)
    assert any("unknown metrics" in issue for issue in issues)
    assert any("dependency cycle" in issue for issue in issues)


def test_contract_rejects_incomplete_or_unknown_candidate_identity_fields() -> None:
    contract = load_hidden_value_signal_contract()
    manifest = load_workbook_module_manifest()
    contract["signals"][0]["deduplication_fields"] = ["signal_id", "unknown_field"]

    issues = validate_hidden_value_signal_contract(contract, manifest)

    assert any("deduplication must include" in issue for issue in issues)
    assert any("unknown deduplication fields" in issue for issue in issues)


def test_schema_valid_but_semantically_duplicate_contract_fails_loader(tmp_path: Path) -> None:
    contract = load_hidden_value_signal_contract()
    contract["signals"].append(copy.deepcopy(contract["signals"][0]))
    path = tmp_path / "duplicate.json"
    path.write_text(json.dumps(contract), encoding="utf-8")

    with pytest.raises(HiddenValueContractError, match="Duplicate signal IDs"):
        load_hidden_value_signal_contract(path)


def test_core_only_profile_produces_no_hidden_value_candidates() -> None:
    plan = evaluate_hidden_value_signals(_minimal_package(), profile_id="core_only", ticker="TEST")

    assert plan.status == "PASS"
    assert plan.candidates == []
    assert plan.audit_projection == []
    assert plan.flags_projection == []


def test_unknown_profile_fails_before_signal_planning() -> None:
    with pytest.raises(ValueError, match="Unknown workbook module profile"):
        evaluate_hidden_value_signals(_minimal_package(), profile_id="missing_profile")
