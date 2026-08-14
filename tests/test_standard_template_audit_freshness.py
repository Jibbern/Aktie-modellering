from __future__ import annotations

import json
import hashlib
from pathlib import Path

import pytest

from pbi_xbrl.standard_template_audit_freshness import (
    AUDIT_FRESHNESS_VERSION,
    AUDIT_RECEIPT_VERSION,
    DEFAULT_AUDIT_CONTRACTS,
    AuditArtifactContract,
    AuditGeneratorContract,
    build_audit_freshness,
    build_unverified_stale_receipt,
    canonical_audit_content_sha256,
    _file_sha256,
    _portable_file_sha256,
    receipt_path_for_artifact,
    record_stale_audit_receipts,
    validate_audit_freshness,
)
from pbi_xbrl.standard_template_audit_runner import (
    GenerationRunResult,
    _run_generation_id,
    record_generation_receipts,
    run_audit_generator,
    verify_deterministic_audit_replay,
)


ROOT = Path(__file__).resolve().parents[1]
SHELL = ROOT / "templates" / "standard_stock_model_template.xlsx"
MANIFEST = ROOT / "docs" / "standard_template_shell_manifest.json"
BINDINGS = ROOT / "docs" / "workbook_binding_map.json"
FRESHNESS = ROOT / "docs" / "standard_template_audit_freshness.json"
RECEIPTS = ROOT / "docs" / "audit_receipts"


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _contract() -> AuditGeneratorContract:
    return AuditGeneratorContract(
        generator="scripts/generate_fixture_audit.py",
        contract_version="7.0.0",
        artifacts=(AuditArtifactContract("docs/fixture_audit.json", "json", "1.0.0"),),
        authoritative_inputs=("inputs/source.json",),
        dependencies=("helpers/audit_helper.py",),
        command_arguments=(
            "--input", "{root}/inputs/source.json",
            "--helper", "{root}/helpers/audit_helper.py",
            "--output", "{artifact:0}",
        ),
    )


def _fixture_tree(tmp_path: Path) -> tuple[AuditGeneratorContract, Path, Path]:
    contract = _contract()
    generator = '''
import argparse
import hashlib
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--input", type=Path, required=True)
parser.add_argument("--helper", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
source = json.loads(args.input.read_text(encoding="utf-8"))
helper_sha = hashlib.sha256(args.helper.read_bytes()).hexdigest()
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(
    json.dumps({"version": "1.0.0", "result": source["source"], "helper_sha": helper_sha}, indent=2) + "\\n",
    encoding="utf-8",
)
'''.lstrip()
    files = {
        "scripts/generate_fixture_audit.py": generator,
        "helpers/audit_helper.py": "# fixture dependency\n",
        "inputs/source.json": '{"source": 1}\n',
        "shell.xlsx": "fixture shell bytes",
    }
    for relative, content in files.items():
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    receipt_root = tmp_path / "docs" / "audit_receipts"
    run_audit_generator(
        tmp_path / contract.generator,
        root=tmp_path,
        data_root=tmp_path / "StockModelData",
        receipt_root=receipt_root,
        contracts=(contract,),
        generated_at_utc="2026-07-13T00:00:00+00:00",
    )
    return contract, tmp_path / "shell.xlsx", receipt_root


def _build_fixture_index(tmp_path: Path, contract: AuditGeneratorContract, shell: Path, receipt_root: Path) -> dict:
    return build_audit_freshness(
        shell_path=shell,
        manifest={"version": "fixture"},
        binding_payload={"binding_planner_contract_version": "fixture", "bindings": []},
        root=tmp_path,
        data_root=tmp_path / "StockModelData",
        receipt_root=receipt_root,
        contracts=(contract,),
    )


def test_checked_in_audit_freshness_matches_current_receipts() -> None:
    payload = _load(FRESHNESS)

    assert payload["version"] == AUDIT_FRESHNESS_VERSION
    assert not validate_audit_freshness(
        payload,
        shell_path=SHELL,
        manifest=_load(MANIFEST),
        binding_payload=_load(BINDINGS),
        root=ROOT,
        receipt_root=RECEIPTS,
    )
    current = {row["path"] for row in payload["artifacts"] if row["status"] == "current"}
    stale = {row["path"] for row in payload["artifacts"] if row["status"] == "stale"}
    assert len(current) == 14
    assert stale == {
        "docs/standard_template_shell_visual_gap_audit.json",
        "docs/standard_template_shell_visual_gap_audit.md",
    }
    for path in current:
        receipt = _load(receipt_path_for_artifact(path, receipt_root=RECEIPTS))
        assert receipt["verification"]["mode"] == "controlled_isolated_generator_run"
        assert receipt["verification"]["successful_completion"] is True


def test_audit_file_identity_normalizes_only_text_newlines(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lf_json = tmp_path / "lf.json"
    crlf_json = tmp_path / "crlf.json"
    mixed_json = tmp_path / "mixed.json"
    copied_json = tmp_path / "nested" / "copied.json"
    changed_json = tmp_path / "changed.json"
    trailing_content_json = tmp_path / "trailing_content.json"
    lf_json.write_bytes(b'{\n  "value": 1\n}\n')
    crlf_json.write_bytes(b'{\r\n  "value": 1\r\n}\r\n')
    mixed_json.write_bytes(b'{\r\n  "value": 1\n}\r\n')
    copied_json.parent.mkdir()
    copied_json.write_bytes(lf_json.read_bytes())
    changed_json.write_bytes(b'{\n  "value": 2\n}\n')
    trailing_content_json.write_bytes(b'{\n  "value": 1\n} \n')

    assert lf_json.read_bytes() != crlf_json.read_bytes()
    assert _file_sha256(lf_json) != _file_sha256(crlf_json)
    canonical = _portable_file_sha256(lf_json.resolve())
    assert canonical == _portable_file_sha256(crlf_json)
    assert canonical == _portable_file_sha256(mixed_json)
    assert canonical == _portable_file_sha256(copied_json.resolve())
    assert _portable_file_sha256(lf_json) != _portable_file_sha256(changed_json)
    assert _portable_file_sha256(lf_json) != _portable_file_sha256(trailing_content_json)

    other_cwd = tmp_path / "other_cwd"
    other_cwd.mkdir()
    monkeypatch.chdir(other_cwd)
    assert _portable_file_sha256(lf_json.resolve()) == canonical

    lf_binary = tmp_path / "lf.xlsx"
    crlf_binary = tmp_path / "crlf.xlsx"
    lf_binary.write_bytes(b"raw\nbytes")
    crlf_binary.write_bytes(b"raw\r\nbytes")
    assert _file_sha256(lf_binary) != _file_sha256(crlf_binary)
    assert _portable_file_sha256(lf_binary) != _portable_file_sha256(crlf_binary)


def test_actual_isolated_generator_run_records_metadata_and_replay_establishes_current(tmp_path: Path) -> None:
    contract, shell, receipt_root = _fixture_tree(tmp_path)
    payload = _build_fixture_index(tmp_path, contract, shell, receipt_root)
    receipt = _load(receipt_path_for_artifact(contract.artifacts[0].path, receipt_root=receipt_root))

    assert receipt["receipt_version"] == AUDIT_RECEIPT_VERSION
    assert receipt["status"] == "generated"
    assert receipt["verification"]["mode"] == "controlled_isolated_generator_run"
    assert receipt["verification"]["successful_completion"] is True
    assert len(receipt["verification"]["run_generation_id"]) == 64
    assert payload["status"] == "current"
    assert payload["artifacts"][0]["receipt_issues"] == []


def test_direct_receipt_builder_cannot_request_current(tmp_path: Path) -> None:
    contract, _shell, _receipt_root = _fixture_tree(tmp_path)
    with pytest.raises(TypeError):
        build_unverified_stale_receipt(
            contract,
            contract.artifacts[0],
            reason="Old artifact.",
            root=tmp_path,
            data_root=tmp_path / "StockModelData",
            status="current",  # type: ignore[call-arg]
        )


def test_stale_receipt_cannot_override_successful_replay(tmp_path: Path) -> None:
    contract, shell, receipt_root = _fixture_tree(tmp_path)
    record_stale_audit_receipts(
        tmp_path / contract.generator,
        reason="Generator has not been rerun for current inputs.",
        root=tmp_path,
        data_root=tmp_path / "StockModelData",
        receipt_root=receipt_root,
        contracts=(contract,),
    )
    payload = _build_fixture_index(tmp_path, contract, shell, receipt_root)
    assert payload["status"] == "current"
    assert payload["artifacts"][0]["receipt_issues"] == []


def test_caller_mapping_cannot_substitute_for_generation_run_result(tmp_path: Path) -> None:
    contract, _shell, receipt_root = _fixture_tree(tmp_path)
    with pytest.raises(TypeError, match="GenerationRunResult"):
        record_generation_receipts(
            {"successful_completion": True},  # type: ignore[arg-type]
            root=tmp_path,
            data_root=tmp_path / "StockModelData",
            receipt_root=receipt_root,
            contracts=(contract,),
        )

    forged = GenerationRunResult(
        _payload_json='{"successful_completion":true}',
        _generated_output_bytes=(),
    )
    with pytest.raises((ValueError, KeyError)):
        record_generation_receipts(
            forged,
            root=tmp_path,
            data_root=tmp_path / "StockModelData",
            receipt_root=receipt_root,
            contracts=(contract,),
        )


@pytest.mark.parametrize(
    ("relative_path", "replacement", "expected_issue"),
    [
        ("docs/fixture_audit.json", '{"version": "1.0.0", "result": "altered"}\n', "artifact digest drifted"),
        ("inputs/source.json", '{"source": 2}\n', "authoritative input declaration or digest drifted"),
        ("scripts/generate_fixture_audit.py", "# old generator replaced after generation\n", "generator implementation drifted"),
        ("helpers/audit_helper.py", "# changed dependency\n", "dependency declaration or digest drifted"),
    ],
)
def test_receipt_detects_output_input_generator_and_dependency_drift(
    tmp_path: Path,
    relative_path: str,
    replacement: str,
    expected_issue: str,
) -> None:
    contract, shell, receipt_root = _fixture_tree(tmp_path)
    (tmp_path / relative_path).write_text(replacement, encoding="utf-8")

    payload = _build_fixture_index(tmp_path, contract, shell, receipt_root)

    assert payload["status"] == "stale"
    assert any(expected_issue in issue for issue in payload["artifacts"][0]["receipt_issues"])


def test_deterministic_replay_matches_valid_artifact_without_writing(tmp_path: Path) -> None:
    contract, _shell, _receipt_root = _fixture_tree(tmp_path)
    artifact = tmp_path / contract.artifacts[0].path
    before = artifact.read_bytes()

    replay = verify_deterministic_audit_replay(
        tmp_path / contract.generator,
        root=tmp_path,
        data_root=tmp_path / "StockModelData",
        contracts=(contract,),
    )

    assert replay["status"] == "PASS"
    assert replay["artifacts"][0]["canonical_content_matches"] is True
    assert artifact.read_bytes() == before


def test_deterministic_replay_rejects_stale_artifact(tmp_path: Path) -> None:
    contract, _shell, _receipt_root = _fixture_tree(tmp_path)
    artifact = tmp_path / contract.artifacts[0].path
    artifact.write_text('{"version": "1.0.0", "result": "stale"}\n', encoding="utf-8")

    replay = verify_deterministic_audit_replay(
        tmp_path / contract.generator,
        root=tmp_path,
        data_root=tmp_path / "StockModelData",
        contracts=(contract,),
    )

    assert replay["status"] == "FAIL"


def test_recomputed_generation_result_and_receipt_cannot_establish_current(tmp_path: Path) -> None:
    contract, shell, receipt_root = _fixture_tree(tmp_path)
    result = run_audit_generator(
        tmp_path / contract.generator,
        root=tmp_path,
        data_root=tmp_path / "StockModelData",
        receipt_root=receipt_root,
        contracts=(contract,),
    )
    fabricated_bytes = b'{"version":"1.0.0","result":"FABRICATED"}\n'
    fabricated_path = tmp_path / contract.artifacts[0].path
    fabricated_path.write_bytes(fabricated_bytes)
    payload = result.generation_run.payload
    payload["generated_outputs"][0]["sha256"] = hashlib.sha256(fabricated_bytes).hexdigest()
    payload["generated_outputs"][0]["canonical_content_sha256"] = canonical_audit_content_sha256(
        fabricated_path,
        "json",
    )
    payload["run_generation_id"] = _run_generation_id(payload)
    forged = GenerationRunResult(
        _payload_json=json.dumps(payload, sort_keys=True, separators=(",", ":")),
        _generated_output_bytes=(fabricated_bytes,),
    )
    record_generation_receipts(
        forged,
        root=tmp_path,
        data_root=tmp_path / "StockModelData",
        receipt_root=receipt_root,
        contracts=(contract,),
    )

    freshness = _build_fixture_index(tmp_path, contract, shell, receipt_root)

    assert freshness["status"] == "stale"
    assert freshness["artifacts"][0]["replay_status"] == "FAIL"


def test_markdown_canonicalization_normalizes_only_timestamp_value(tmp_path: Path) -> None:
    first = tmp_path / "first.md"
    second = tmp_path / "second.md"
    failed = tmp_path / "failed.md"
    first.write_text("Generated at: 2026-07-13T01:02:03+00:00 | status PASS\nCount: 4\n", encoding="utf-8")
    second.write_text("Generated at: 2026-07-14T04:05:06+00:00 | status PASS\nCount: 4\n", encoding="utf-8")
    failed.write_text("Generated at: 2026-07-14T04:05:06+00:00 | status FAIL\nCount: 4\n", encoding="utf-8")

    assert canonical_audit_content_sha256(first, "text/markdown") == canonical_audit_content_sha256(
        second,
        "text/markdown",
    )
    assert canonical_audit_content_sha256(first, "text/markdown") != canonical_audit_content_sha256(
        failed,
        "text/markdown",
    )


def test_missing_receipt_is_diagnostic_only_and_caller_cannot_attest_current(tmp_path: Path) -> None:
    contract, shell, receipt_root = _fixture_tree(tmp_path)
    receipt_path_for_artifact(contract.artifacts[0].path, receipt_root=receipt_root).unlink()

    payload = _build_fixture_index(tmp_path, contract, shell, receipt_root)

    assert payload["status"] == "current"
    assert payload["artifacts"][0]["receipt_issues"] == ["audit generation receipt is missing"]
    with pytest.raises(TypeError):
        build_audit_freshness(
            shell_path=shell,
            manifest={"version": "fixture"},
            binding_payload={"bindings": []},
            root=tmp_path,
            contracts=(contract,),
            current_artifacts=[contract.artifacts[0].path],  # type: ignore[call-arg]
        )


def test_freshness_index_requires_exact_receipt_backed_artifact_set(tmp_path: Path) -> None:
    contract, shell, receipt_root = _fixture_tree(tmp_path)
    payload = _build_fixture_index(tmp_path, contract, shell, receipt_root)
    kwargs = {
        "shell_path": shell,
        "manifest": {"version": "fixture"},
        "binding_payload": {"binding_planner_contract_version": "fixture", "bindings": []},
        "root": tmp_path,
        "data_root": tmp_path / "StockModelData",
        "receipt_root": receipt_root,
        "contracts": (contract,),
    }

    empty = {**payload, "status": "current", "artifacts": []}
    assert "expected audit artifact is missing: docs/fixture_audit.json" in validate_audit_freshness(empty, **kwargs)
    duplicate = {**payload, "artifacts": [*payload["artifacts"], payload["artifacts"][0]]}
    assert "audit artifact must appear exactly once: docs/fixture_audit.json" in validate_audit_freshness(duplicate, **kwargs)
    unknown = {**payload, "artifacts": [*payload["artifacts"], {**payload["artifacts"][0], "path": "docs/unknown.json"}]}
    assert "unknown audit artifact: docs/unknown.json" in validate_audit_freshness(unknown, **kwargs)


def test_visual_gap_contract_declares_local_validation_dependency() -> None:
    visual = next(
        contract
        for contract in DEFAULT_AUDIT_CONTRACTS
        if contract.generator == "scripts/build_standard_template_shell_visual_gap_audit.py"
    )
    assert "scripts/validate_standard_template_shell.py" in visual.dependencies
    assert visual.replay_policy == "manual_visual"


def test_cli_has_no_blind_current_attestation_options() -> None:
    updater = (ROOT / "scripts" / "update_standard_template_audit_freshness.py").read_text(encoding="utf-8")
    runner = (ROOT / "scripts" / "run_standard_template_audit_generator.py").read_text(encoding="utf-8")
    combined = updater + runner
    assert "--all-current" not in combined
    assert "--current" not in combined
    assert "current_artifacts" not in combined
    assert "--generator" in runner
    assert "--replay-only" in runner
