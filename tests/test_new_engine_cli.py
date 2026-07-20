from __future__ import annotations

import json
from pathlib import Path

import pytest

import pbi_xbrl.new_engine as cli
from pbi_xbrl.new_engine import main


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
    pytest.skip("ANF normalized package is unavailable for full CLI planning testing.")


def test_plan_cli_emits_machine_readable_pass_receipt(tmp_path: Path, capsys) -> None:
    exit_code = main(
        [
            "plan",
            "--package",
            str(_package_path()),
            "--ticker",
            "ANF",
            "--profile-id",
            "full_union",
            "--run-dir",
            str(tmp_path / "run"),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["status"] == "PASS"
    assert Path(payload["receipt_path"]).exists()


def test_plan_cli_reports_ticker_mismatch_without_traceback(tmp_path: Path, capsys) -> None:
    exit_code = main(
        [
            "plan",
            "--package",
            str(_package_path()),
            "--ticker",
            "WRONG",
            "--profile-id",
            "full_union",
            "--run-dir",
            str(tmp_path / "run"),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert payload["status"] == "FAIL"
    assert payload["reason"] == "NewEngineOrchestrationError"
    assert not (tmp_path / "run").exists()


def test_render_and_validate_cli_forward_release_arguments(
    tmp_path: Path, capsys, monkeypatch: pytest.MonkeyPatch
) -> None:
    observed: list[tuple[str, dict[str, object]]] = []

    def fake_render(**kwargs: object) -> dict[str, object]:
        observed.append(("render", kwargs))
        return {"status": "PASS", "output_path": tmp_path / "ANF_shadow_model_v8.xlsx"}

    def fake_validate(**kwargs: object) -> dict[str, object]:
        observed.append(("validate", kwargs))
        return {"status": "PASS", "receipt_path": tmp_path / "validate.json"}

    monkeypatch.setattr(cli, "render_shadow", fake_render)
    monkeypatch.setattr(cli, "validate_workbook_immutable", fake_validate)
    common = [
        "--package",
        str(ROOT / "package.json"),
        "--ticker",
        "ANF",
        "--profile-id",
        "full_union",
    ]
    assert main(
        [
            "render-shadow",
            *common,
            "--run-dir",
            str(tmp_path / "render"),
            "--plan-receipt",
            str(tmp_path / "plan.json"),
            "--output-root",
            str(tmp_path / "output"),
            "--version",
            "v8",
            "--excel-native",
            "required",
            "--excel-locale-id",
            "1053",
        ]
    ) == 0
    json.loads(capsys.readouterr().out)
    assert main(
        [
            "validate",
            *common,
            "--run-dir",
            str(tmp_path / "validate"),
            "--plan-receipt",
            str(tmp_path / "plan.json"),
            "--workbook",
            str(tmp_path / "input.xlsx"),
            "--excel-native",
            "off",
        ]
    ) == 0
    json.loads(capsys.readouterr().out)

    assert observed[0][0] == "render"
    assert observed[0][1]["version"] == "v8"
    assert observed[0][1]["excel_native"] == "required"
    assert observed[0][1]["required_locale_id"] == 1053
    assert observed[1][0] == "validate"
    assert observed[1][1]["excel_native"] == "off"


def test_cli_reports_operational_filesystem_errors_without_traceback(
    tmp_path: Path, capsys, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail_plan(**_kwargs: object) -> dict[str, object]:
        raise PermissionError("run directory is not writable")

    monkeypatch.setattr(cli, "run_plan", fail_plan)

    exit_code = main(
        [
            "plan",
            "--package",
            str(tmp_path / "package.json"),
            "--ticker",
            "TEST",
            "--profile-id",
            "full_union",
            "--run-dir",
            str(tmp_path / "run"),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert payload == {
        "message": "run directory is not writable",
        "reason": "PermissionError",
        "status": "FAIL",
    }
