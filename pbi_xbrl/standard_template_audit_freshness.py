"""Replay-backed freshness contract for generated standard-template audits."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from pbi_xbrl.json_schema_validation import load_json_strict, validate_json_schema
from pbi_xbrl.standard_template_shell_identity import (
    compute_binding_contract_signature,
    compute_manifest_contract_signature,
)


ROOT = Path(__file__).resolve().parents[1]
AUDIT_FRESHNESS_VERSION = "3.0.0"
AUDIT_RECEIPT_VERSION = "3.0.0"
GENERATOR_PROVENANCE_VERSION = "deterministic-replay-v1"
GENERATION_RUNNER_CONTRACT_VERSION = "2.0.0"
DEFAULT_FRESHNESS_PATH = ROOT / "docs" / "standard_template_audit_freshness.json"
DEFAULT_RECEIPT_ROOT = ROOT / "docs" / "audit_receipts"
RECEIPT_SCHEMA_PATH = ROOT / "docs" / "standard_template_audit_receipt.schema.json"


@dataclass(frozen=True)
class AuditArtifactContract:
    path: str
    output_schema: str
    output_version: str


@dataclass(frozen=True)
class AuditGeneratorContract:
    generator: str
    contract_version: str
    artifacts: tuple[AuditArtifactContract, ...]
    authoritative_inputs: tuple[str, ...]
    dependencies: tuple[str, ...] = ()
    command_arguments: tuple[str, ...] = ()
    replay_policy: str = "deterministic"


_SHELL = "templates/standard_stock_model_template.xlsx"
_LAB = "templates/lab/ANF_template_lab.xlsx"
_MANIFEST = "docs/standard_template_shell_manifest.json"
_BINDINGS = "docs/workbook_binding_map.json"
_MODULE_MANIFEST = "docs/workbook_module_manifest.json"
_FLOW = "docs/sheet_data_flow_map.json"
_SOURCE_MODELS = ("@stock_model:PBI", "@stock_model:GPRE", "@stock_model:ANF")


DEFAULT_AUDIT_CONTRACTS: tuple[AuditGeneratorContract, ...] = (
    AuditGeneratorContract(
        generator="scripts/build_standard_template_defined_name_audit.py",
        contract_version="1.0.0",
        artifacts=(
            AuditArtifactContract("docs/standard_template_defined_name_audit.json", "json", "1.0.0"),
            AuditArtifactContract("docs/standard_template_defined_name_audit.md", "text/markdown", "markdown-v1"),
        ),
        authoritative_inputs=(_SHELL, _MANIFEST, _BINDINGS),
        command_arguments=(
            "--template", "{root}/templates/standard_stock_model_template.xlsx",
            "--manifest", "{root}/docs/standard_template_shell_manifest.json",
            "--binding-map", "{root}/docs/workbook_binding_map.json",
            "--output-json", "{artifact:0}",
            "--output-md", "{artifact:1}",
        ),
    ),
    AuditGeneratorContract(
        generator="scripts/build_standard_template_sheet_inventory.py",
        contract_version="1.0.0",
        artifacts=(
            AuditArtifactContract("docs/standard_template_sheet_inventory.json", "json", "0.1.0"),
            AuditArtifactContract("docs/standard_template_sheet_inventory.md", "text/markdown", "markdown-v1"),
            AuditArtifactContract("docs/support_sheet_lifecycle_contract.json", "json", "0.1.0"),
            AuditArtifactContract("docs/support_sheet_lifecycle_contract.md", "text/markdown", "markdown-v1"),
        ),
        authoritative_inputs=(_SHELL, _MANIFEST, _BINDINGS, _MODULE_MANIFEST, *_SOURCE_MODELS),
        dependencies=("pbi_xbrl/workbook_modules.py", "pbi_xbrl/json_schema_validation.py"),
        command_arguments=(
            "--template", "{root}/templates/standard_stock_model_template.xlsx",
            "--manifest", "{root}/docs/standard_template_shell_manifest.json",
            "--binding-map", "{root}/docs/workbook_binding_map.json",
            "--module-manifest", "{root}/docs/workbook_module_manifest.json",
            "--data-root", "{data_root}",
            "--inventory-json", "{artifact:0}",
            "--inventory-md", "{artifact:1}",
            "--lifecycle-json", "{artifact:2}",
            "--lifecycle-md", "{artifact:3}",
        ),
    ),
    AuditGeneratorContract(
        generator="scripts/build_standard_template_shell_neutrality_audit.py",
        contract_version="1.0.0",
        artifacts=(
            AuditArtifactContract("docs/standard_template_shell_neutrality_audit.json", "json", "0.1.0"),
            AuditArtifactContract("docs/standard_template_shell_neutrality_audit.md", "text/markdown", "markdown-v1"),
        ),
        authoritative_inputs=(_SHELL, _MANIFEST, _MODULE_MANIFEST),
        dependencies=("pbi_xbrl/workbook_modules.py", "pbi_xbrl/json_schema_validation.py"),
        command_arguments=(
            "--template", "{root}/templates/standard_stock_model_template.xlsx",
            "--manifest", "{root}/docs/standard_template_shell_manifest.json",
            "--module-manifest", "{root}/docs/workbook_module_manifest.json",
            "--output-json", "{artifact:0}",
            "--output-md", "{artifact:1}",
        ),
    ),
    AuditGeneratorContract(
        generator="scripts/build_workbook_block_architecture.py",
        contract_version="1.0.0",
        artifacts=(
            AuditArtifactContract("docs/workbook_block_architecture.json", "json", "0.1.0"),
            AuditArtifactContract("docs/workbook_block_architecture.md", "text/markdown", "markdown-v1"),
            AuditArtifactContract("docs/workbook_block_coverage_matrix.json", "json", "0.1.0"),
            AuditArtifactContract("docs/workbook_block_coverage_matrix.md", "text/markdown", "markdown-v1"),
        ),
        authoritative_inputs=(_LAB, _MANIFEST, _BINDINGS, _FLOW, *_SOURCE_MODELS),
        dependencies=("pbi_xbrl/json_schema_validation.py",),
        command_arguments=(
            "--source-dir", "{data_root}/outputs/Excel stock models",
            "--lab-path", "{root}/templates/lab/ANF_template_lab.xlsx",
            "--reuse-existing-lab",
            "--architecture-json", "{artifact:0}",
            "--architecture-md", "{artifact:1}",
            "--coverage-json", "{artifact:2}",
            "--coverage-md", "{artifact:3}",
        ),
    ),
    AuditGeneratorContract(
        generator="scripts/build_standard_template_hidden_support_audit.py",
        contract_version="1.0.0",
        artifacts=(
            AuditArtifactContract("docs/standard_template_hidden_support_audit.json", "json", "0.1.0"),
            AuditArtifactContract("docs/standard_template_hidden_support_audit.md", "text/markdown", "markdown-v1"),
        ),
        authoritative_inputs=(_SHELL, _LAB, _MANIFEST, _MODULE_MANIFEST),
        dependencies=("pbi_xbrl/workbook_modules.py", "pbi_xbrl/json_schema_validation.py"),
        command_arguments=(
            "--template", "{root}/templates/standard_stock_model_template.xlsx",
            "--lab", "{root}/templates/lab/ANF_template_lab.xlsx",
            "--manifest", "{root}/docs/standard_template_shell_manifest.json",
            "--module-manifest", "{root}/docs/workbook_module_manifest.json",
            "--audit-json", "{artifact:0}",
            "--audit-md", "{artifact:1}",
        ),
    ),
    AuditGeneratorContract(
        generator="scripts/build_standard_template_shell_visual_gap_audit.py",
        contract_version="1.0.0",
        artifacts=(
            AuditArtifactContract("docs/standard_template_shell_visual_gap_audit.json", "json", "0.1.0"),
            AuditArtifactContract("docs/standard_template_shell_visual_gap_audit.md", "text/markdown", "markdown-v1"),
        ),
        authoritative_inputs=(_SHELL, _LAB, _MANIFEST),
        dependencies=("scripts/validate_standard_template_shell.py",),
        command_arguments=(
            "--template", "{root}/templates/standard_stock_model_template.xlsx",
            "--lab", "{root}/templates/lab/ANF_template_lab.xlsx",
            "--manifest", "{root}/docs/standard_template_shell_manifest.json",
            "--audit-json", "{artifact:0}",
            "--audit-md", "{artifact:1}",
            "--preview-dir", "{temp_root}/previews",
        ),
        replay_policy="manual_visual",
    ),
)

DEPENDENT_AUDITS: tuple[tuple[str, str], ...] = tuple(
    (artifact.path, contract.generator)
    for contract in DEFAULT_AUDIT_CONTRACTS
    for artifact in contract.artifacts
)


def build_unverified_stale_receipt(
    contract: AuditGeneratorContract,
    artifact: AuditArtifactContract,
    *,
    reason: str,
    root: Path = ROOT,
    data_root: Path | None = None,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    """Build a stale receipt without asserting that a generator ran.

    Generated-run receipts are metadata only. They never establish freshness;
    deterministic replay is the authoritative check.
    """

    if artifact not in contract.artifacts:
        raise ValueError(f"Artifact {artifact.path!r} does not belong to {contract.generator!r}.")
    if not reason.strip():
        raise ValueError("An unverified stale receipt requires a reason.")

    data_root = data_root or _default_data_root(root)
    artifact_path = _resolve_path(artifact.path, root=root, data_root=data_root)
    actual_output_version = _output_version(artifact_path, artifact.output_schema)
    receipt: dict[str, Any] = {
        "receipt_version": AUDIT_RECEIPT_VERSION,
        "status": "stale",
        "reason": reason.strip(),
        "artifact": {
            "path": artifact.path,
            "sha256": _artifact_file_sha256(artifact_path, artifact.output_schema),
            "output_schema": artifact.output_schema,
            "output_version": actual_output_version,
        },
        "authoritative_inputs": [
            _file_digest_record(path, root=root, data_root=data_root)
            for path in contract.authoritative_inputs
        ],
        "generator": {
            "path": contract.generator,
            "contract_version": contract.contract_version,
            "implementation_sha256": _portable_file_sha256(
                _resolve_path(contract.generator, root=root, data_root=data_root)
            ),
            "receipt_engine_sha256": _portable_file_sha256(Path(__file__).resolve()),
            "runner_implementation_sha256": _portable_file_sha256(_runner_module_path()),
            "execution_contract_sha256": compute_audit_generator_execution_contract_signature(contract),
            "dependencies": [
                _file_digest_record(path, root=root, data_root=data_root)
                for path in contract.dependencies
            ],
        },
        "verification": {
            "mode": "unverified_stale",
            "runner_contract_version": GENERATION_RUNNER_CONTRACT_VERSION,
            "successful_completion": False,
            "run_generation_id": _payload_sha256(
                {
                    "mode": "unverified_stale",
                    "generator": contract.generator,
                    "artifact": artifact.path,
                    "reason": reason.strip(),
                }
            ),
            "generated_outputs": [],
        },
        "generated_at_utc": generated_at_utc or _now(),
    }
    receipt["generation_id"] = _receipt_generation_id(receipt)
    return receipt


def record_stale_audit_receipts(
    generator_path: Path | str,
    *,
    reason: str,
    root: Path = ROOT,
    data_root: Path | None = None,
    receipt_root: Path | None = None,
    contracts: Sequence[AuditGeneratorContract] = DEFAULT_AUDIT_CONTRACTS,
    generated_at_utc: str | None = None,
) -> list[Path]:
    """Record stale-only receipts; this API cannot attest current status."""

    generator_label = _path_label(Path(generator_path), root=root)
    matches = [contract for contract in contracts if contract.generator == generator_label]
    if len(matches) != 1:
        raise ValueError(f"Expected one audit generator contract for {generator_label!r}; found {len(matches)}.")
    contract = matches[0]
    receipt_root = receipt_root or (root / "docs" / "audit_receipts")
    receipt_root.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for artifact in contract.artifacts:
        receipt = build_unverified_stale_receipt(
            contract,
            artifact,
            reason=reason,
            root=root,
            data_root=data_root,
            generated_at_utc=generated_at_utc,
        )
        path = receipt_path_for_artifact(artifact.path, receipt_root=receipt_root)
        path.write_text(json.dumps(receipt, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        paths.append(path)
    return paths


def validate_generation_receipt(
    receipt: Mapping[str, Any],
    *,
    contract: AuditGeneratorContract,
    artifact: AuditArtifactContract,
    root: Path = ROOT,
    data_root: Path | None = None,
    receipt_schema_path: Path = RECEIPT_SCHEMA_PATH,
) -> list[str]:
    data_root = data_root or _default_data_root(root)
    issues = [
        f"receipt schema {keyword} at {field}: {message}"
        for field, keyword, message in validate_json_schema(
            receipt,
            load_json_strict(receipt_schema_path),
        )
    ]
    expected_artifact_path = _resolve_path(artifact.path, root=root, data_root=data_root)
    artifact_row = receipt.get("artifact") if isinstance(receipt.get("artifact"), Mapping) else {}
    if str(artifact_row.get("path") or "") != artifact.path:
        issues.append("receipt artifact path mismatch")
    if expected_artifact_path.exists():
        if str(artifact_row.get("sha256") or "") != _artifact_file_sha256(
            expected_artifact_path, artifact.output_schema
        ):
            issues.append("receipt artifact digest drifted")
        actual_version = _output_version(expected_artifact_path, artifact.output_schema)
        if str(artifact_row.get("output_version") or "") != actual_version:
            issues.append("receipt artifact output version drifted")
    else:
        issues.append("receipt artifact is missing")
    if str(artifact_row.get("output_schema") or "") != artifact.output_schema:
        issues.append("receipt artifact output schema mismatch")
    if str(artifact_row.get("output_version") or "") != artifact.output_version:
        issues.append("receipt artifact contract version mismatch")

    expected_inputs = [
        _file_digest_record(path, root=root, data_root=data_root)
        for path in contract.authoritative_inputs
    ]
    if receipt.get("authoritative_inputs") != expected_inputs:
        issues.append("receipt authoritative input declaration or digest drifted")

    generator_row = receipt.get("generator") if isinstance(receipt.get("generator"), Mapping) else {}
    if str(generator_row.get("path") or "") != contract.generator:
        issues.append("receipt generator path mismatch")
    if str(generator_row.get("contract_version") or "") != contract.contract_version:
        issues.append("receipt generator contract version mismatch")
    generator_path = _resolve_path(contract.generator, root=root, data_root=data_root)
    if not generator_path.exists():
        issues.append("receipt generator is missing")
    elif str(generator_row.get("implementation_sha256") or "") != _portable_file_sha256(generator_path):
        issues.append("receipt generator implementation drifted")
    receipt_engine_path = Path(__file__).resolve()
    if str(generator_row.get("receipt_engine_sha256") or "") != _portable_file_sha256(receipt_engine_path):
        issues.append("receipt engine implementation drifted")
    runner_path = _runner_module_path()
    if not runner_path.exists():
        issues.append("audit generation runner is missing")
    elif str(generator_row.get("runner_implementation_sha256") or "") != _portable_file_sha256(runner_path):
        issues.append("audit generation runner implementation drifted")
    if str(generator_row.get("execution_contract_sha256") or "") != compute_audit_generator_execution_contract_signature(contract):
        issues.append("receipt generator execution contract drifted")
    expected_dependencies = [
        _file_digest_record(path, root=root, data_root=data_root)
        for path in contract.dependencies
    ]
    if generator_row.get("dependencies") != expected_dependencies:
        issues.append("receipt dependency declaration or digest drifted")

    if str(receipt.get("receipt_version") or "") != AUDIT_RECEIPT_VERSION:
        issues.append("unsupported audit receipt version")
    status = str(receipt.get("status") or "")
    if status not in {"generated", "stale"}:
        issues.append("invalid audit receipt status")
    if status == "stale" and not str(receipt.get("reason") or "").strip():
        issues.append("stale audit receipt has no reason")
    verification = receipt.get("verification") if isinstance(receipt.get("verification"), Mapping) else {}
    mode = str(verification.get("mode") or "")
    outputs = verification.get("generated_outputs") if isinstance(verification.get("generated_outputs"), list) else []
    if status == "generated":
        if str(receipt.get("reason") or ""):
            issues.append("generated audit receipt must not contain a stale reason")
        if mode != "controlled_isolated_generator_run":
            issues.append("generated audit receipt lacks controlled-run metadata")
        if verification.get("successful_completion") is not True:
            issues.append("generated audit receipt run did not complete successfully")
        if str(verification.get("runner_contract_version") or "") != GENERATION_RUNNER_CONTRACT_VERSION:
            issues.append("unsupported audit generation runner contract version")
        expected_run_id = generation_run_id_from_receipt(receipt)
        if str(verification.get("run_generation_id") or "") != expected_run_id:
            issues.append("audit generation run id mismatch")
        matching_outputs = [row for row in outputs if isinstance(row, Mapping) and row.get("path") == artifact.path]
        if len(matching_outputs) != 1:
            issues.append("generated audit receipt lacks exactly one generated output record")
        else:
            generated = matching_outputs[0]
            if str(generated.get("sha256") or "") != str(artifact_row.get("sha256") or ""):
                issues.append("generated output digest differs from promoted artifact")
            if expected_artifact_path.exists() and str(generated.get("canonical_content_sha256") or "") != canonical_audit_content_sha256(
                expected_artifact_path,
                artifact.output_schema,
            ):
                issues.append("generated output canonical content digest drifted")
    elif status == "stale":
        if mode != "unverified_stale":
            issues.append("stale audit receipt has an invalid verification mode")
        if verification.get("successful_completion") is not False:
            issues.append("stale audit receipt must not claim successful generation")
        if outputs:
            issues.append("stale audit receipt must not claim generated outputs")
    if str(receipt.get("generation_id") or "") != _receipt_generation_id(receipt):
        issues.append("audit receipt generation id mismatch")
    return sorted(set(issues))


def build_audit_freshness(
    *,
    shell_path: Path,
    manifest: Mapping[str, Any],
    binding_payload: Mapping[str, Any],
    root: Path = ROOT,
    data_root: Path | None = None,
    receipt_root: Path | None = None,
    contracts: Sequence[AuditGeneratorContract] = DEFAULT_AUDIT_CONTRACTS,
    receipt_schema_path: Path = RECEIPT_SCHEMA_PATH,
) -> dict[str, Any]:
    """Build freshness from isolated deterministic replay, never receipt claims."""

    data_root = data_root or _default_data_root(root)
    receipt_root = receipt_root or (root / "docs" / "audit_receipts")
    from pbi_xbrl.standard_template_audit_runner import verify_deterministic_audit_replay

    replay_by_generator: dict[str, dict[str, Any]] = {}
    for contract in contracts:
        if contract.replay_policy != "deterministic":
            replay_by_generator[contract.generator] = {
                "status": "SKIPPED",
                "reason": f"Replay policy is {contract.replay_policy!r}; deterministic replay was not performed.",
                "artifacts": [],
            }
            continue
        try:
            replay_by_generator[contract.generator] = verify_deterministic_audit_replay(
                contract.generator,
                root=root,
                data_root=data_root,
                contracts=contracts,
            )
        except Exception as exc:
            replay_by_generator[contract.generator] = {
                "status": "FAIL",
                "reason": f"Deterministic replay failed: {exc}",
                "artifacts": [],
            }

    artifacts: list[dict[str, Any]] = []
    receipt_hashes: list[dict[str, str]] = []
    for contract in contracts:
        for artifact in contract.artifacts:
            receipt_path = receipt_path_for_artifact(artifact.path, receipt_root=receipt_root)
            receipt_label = _path_label(receipt_path, root=root)
            receipt: Mapping[str, Any] = {}
            issues: list[str] = []
            if not receipt_path.exists():
                issues.append("audit generation receipt is missing")
                receipt_hash = ""
            else:
                receipt_hash = _portable_file_sha256(receipt_path)
                try:
                    loaded = load_json_strict(receipt_path)
                    if not isinstance(loaded, Mapping):
                        raise ValueError("receipt root must be an object")
                    receipt = loaded
                    issues.extend(
                        validate_generation_receipt(
                            receipt,
                            contract=contract,
                            artifact=artifact,
                            root=root,
                            data_root=data_root,
                            receipt_schema_path=receipt_schema_path,
                        )
                    )
                except Exception as exc:
                    issues.append(f"audit generation receipt is unreadable: {exc}")
            replay = replay_by_generator[contract.generator]
            replay_rows = [
                row
                for row in replay.get("artifacts") or []
                if isinstance(row, Mapping) and str(row.get("path") or "") == artifact.path
            ]
            replay_matches = (
                replay.get("status") == "PASS"
                and len(replay_rows) == 1
                and replay_rows[0].get("canonical_content_matches") is True
            )
            status = "current" if replay_matches else "stale"
            reason = "" if replay_matches else str(replay.get("reason") or "").strip()
            if not reason and status == "stale":
                reason = "Deterministic replay did not reproduce the checked-in artifact."
            artifacts.append(
                {
                    "path": artifact.path,
                    "generator": contract.generator,
                    "generator_contract_version": contract.contract_version,
                    "receipt_path": receipt_label,
                    "receipt_sha256": receipt_hash,
                    "status": status,
                    "reason": reason,
                    "receipt_issues": issues,
                    "freshness_basis": "deterministic_replay",
                    "replay_status": str(replay.get("status") or "FAIL"),
                    "replay_artifact": replay_rows[0] if len(replay_rows) == 1 else None,
                }
            )
            receipt_hashes.append({"path": receipt_label, "sha256": receipt_hash})
    return {
        "version": AUDIT_FRESHNESS_VERSION,
        "status": "current" if all(row["status"] == "current" for row in artifacts) else "stale",
        "source_identity": {
            "shell_sha256": _file_sha256(shell_path),
            "manifest_contract_signature": compute_manifest_contract_signature(manifest),
            "binding_contract_signature": compute_binding_contract_signature(binding_payload),
            "receipt_schema_sha256": _portable_file_sha256(receipt_schema_path),
            "receipt_engine_sha256": _portable_file_sha256(Path(__file__).resolve()),
            "generator_contract_signature": _generator_contract_signature(
                contracts,
                root=root,
                data_root=data_root,
            ),
            "receipt_set_signature": _payload_sha256(receipt_hashes),
            "freshness_basis": "isolated_deterministic_replay-v1",
        },
        "artifacts": artifacts,
    }


def write_audit_freshness(
    output_path: Path = DEFAULT_FRESHNESS_PATH,
    **kwargs: Any,
) -> Path:
    payload = build_audit_freshness(**kwargs)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return output_path


def validate_audit_freshness(
    payload: Mapping[str, Any],
    *,
    shell_path: Path,
    manifest: Mapping[str, Any],
    binding_payload: Mapping[str, Any],
    root: Path = ROOT,
    data_root: Path | None = None,
    receipt_root: Path | None = None,
    contracts: Sequence[AuditGeneratorContract] = DEFAULT_AUDIT_CONTRACTS,
    receipt_schema_path: Path = RECEIPT_SCHEMA_PATH,
) -> list[str]:
    issues: list[str] = []
    if str(payload.get("version") or "") != AUDIT_FRESHNESS_VERSION:
        issues.append("unsupported audit-freshness contract version")
    expected = build_audit_freshness(
        shell_path=shell_path,
        manifest=manifest,
        binding_payload=binding_payload,
        root=root,
        data_root=data_root,
        receipt_root=receipt_root,
        contracts=contracts,
        receipt_schema_path=receipt_schema_path,
    )
    if payload.get("source_identity") != expected["source_identity"]:
        issues.append("audit-freshness source identity differs from receipt metadata and contracts")

    raw_artifacts = payload.get("artifacts")
    if not isinstance(raw_artifacts, list):
        issues.append("audit-freshness artifacts must be a list")
        raw_artifacts = []
    rows_by_path: dict[str, list[Mapping[str, Any]]] = {}
    for index, row in enumerate(raw_artifacts):
        if not isinstance(row, Mapping):
            issues.append(f"audit-freshness artifact row {index} must be an object")
            continue
        rows_by_path.setdefault(str(row.get("path") or ""), []).append(row)
    expected_by_path = {str(row["path"]): row for row in expected["artifacts"]}
    for path, rows in sorted(rows_by_path.items()):
        if path not in expected_by_path:
            issues.append(f"unknown audit artifact: {path}")
        if len(rows) != 1:
            issues.append(f"audit artifact must appear exactly once: {path}")
    for path in sorted(set(expected_by_path) - set(rows_by_path)):
        issues.append(f"expected audit artifact is missing: {path}")
    for path, expected_row in expected_by_path.items():
        rows = rows_by_path.get(path) or []
        if len(rows) == 1 and rows[0] != expected_row:
            issues.append(f"audit freshness row differs from verified receipt state: {path}")
    if str(payload.get("status") or "") != str(expected["status"]):
        issues.append(f"audit-freshness top-level status is inconsistent: expected {expected['status']}")
    return sorted(set(issues))


def receipt_path_for_artifact(artifact_path: str, *, receipt_root: Path) -> Path:
    return receipt_root / f"{Path(artifact_path).name}.receipt.json"


def _generator_contract_signature(
    contracts: Sequence[AuditGeneratorContract],
    *,
    root: Path,
    data_root: Path,
) -> str:
    rows = []
    for contract in contracts:
        rows.append(
            {
                "generator": contract.generator,
                "contract_version": contract.contract_version,
                "implementation_sha256": _portable_file_sha256(
                    _resolve_path(contract.generator, root=root, data_root=data_root)
                ),
                "receipt_engine_sha256": _portable_file_sha256(Path(__file__).resolve()),
                "dependencies": [
                    _file_digest_record(path, root=root, data_root=data_root)
                    for path in contract.dependencies
                ],
                "authoritative_input_paths": [
                    _file_digest_record(path, root=root, data_root=data_root)["path"]
                    for path in contract.authoritative_inputs
                ],
                "artifacts": [artifact.__dict__ for artifact in contract.artifacts],
                "command_arguments": list(contract.command_arguments),
                "replay_policy": contract.replay_policy,
            }
        )
    return _payload_sha256(rows)


def compute_audit_generator_execution_contract_signature(contract: AuditGeneratorContract) -> str:
    """Digest every declared input/output/execution semantic for one audit generator."""

    return _payload_sha256(
        {
            "generator": contract.generator,
            "contract_version": contract.contract_version,
            "artifacts": [artifact.__dict__ for artifact in contract.artifacts],
            "authoritative_inputs": list(contract.authoritative_inputs),
            "dependencies": list(contract.dependencies),
            "command_arguments": list(contract.command_arguments),
            "replay_policy": contract.replay_policy,
        }
    )


def generation_run_id_from_receipt(receipt: Mapping[str, Any]) -> str:
    """Reproduce the deterministic run id embedded by the controlled runner."""

    generator = receipt.get("generator") if isinstance(receipt.get("generator"), Mapping) else {}
    verification = receipt.get("verification") if isinstance(receipt.get("verification"), Mapping) else {}
    return _payload_sha256(
        {
            "runner_contract_version": verification.get("runner_contract_version"),
            "runner_implementation_sha256": generator.get("runner_implementation_sha256"),
            "generator": {
                "path": generator.get("path"),
                "contract_version": generator.get("contract_version"),
                "implementation_sha256": generator.get("implementation_sha256"),
                "execution_contract_sha256": generator.get("execution_contract_sha256"),
                "dependencies": generator.get("dependencies"),
            },
            "authoritative_inputs": receipt.get("authoritative_inputs"),
            "generated_outputs": verification.get("generated_outputs"),
            "successful_completion": verification.get("successful_completion"),
        }
    )


def _receipt_generation_id(receipt: Mapping[str, Any]) -> str:
    signed = {
        key: value
        for key, value in receipt.items()
        if key not in {"generated_at_utc", "generation_id"}
    }
    return _payload_sha256(signed)


def canonical_audit_content_sha256(path: Path, output_schema: str) -> str:
    """Hash stable audit content while excluding run timestamps only."""

    if output_schema == "json":
        payload = load_json_strict(path)
        if not isinstance(payload, Mapping):
            raise ValueError(f"JSON audit root must be an object: {path}")
        stable = dict(payload)
        stable.pop("generated_at", None)
        stable.pop("generated_at_utc", None)
        return _payload_sha256(stable)
    text = path.read_text(encoding="utf-8")
    timestamp = re.compile(
        r"(generated at\s*:\s*`?)(\d{4}-\d{2}-\d{2}T[0-9:.]+(?:Z|[+-]\d{2}:?\d{2}))",
        flags=re.IGNORECASE,
    )
    stable_lines = [timestamp.sub(lambda match: f"{match.group(1)}<normalized>", line) for line in text.splitlines()]
    stable = "\n".join(stable_lines) + ("\n" if text.endswith("\n") else "")
    return hashlib.sha256(stable.encode("utf-8")).hexdigest()


def _file_digest_record(path_spec: str, *, root: Path, data_root: Path) -> dict[str, str]:
    path = _resolve_path(path_spec, root=root, data_root=data_root)
    label = path_spec if path_spec.startswith("@stock_model:") else _path_label(path, root=root)
    return {"path": label, "sha256": _portable_file_sha256(path)}


def _resolve_path(path_spec: str, *, root: Path, data_root: Path) -> Path:
    if path_spec.startswith("@stock_model:"):
        ticker = path_spec.split(":", 1)[1]
        output_dir = data_root / "outputs" / "Excel stock models"
        xlsx = output_dir / f"{ticker}_model.xlsx"
        xlsm = output_dir / f"{ticker}_model.xlsm"
        return xlsx if xlsx.exists() or not xlsm.exists() else xlsm
    path = Path(path_spec)
    return path if path.is_absolute() else root / path


def _path_label(path: Path, *, root: Path) -> str:
    resolved = path.expanduser().resolve()
    try:
        return resolved.relative_to(root.expanduser().resolve()).as_posix()
    except ValueError:
        return str(resolved)


def _output_version(path: Path, output_schema: str) -> str:
    if output_schema == "json":
        payload = load_json_strict(path)
        if not isinstance(payload, Mapping):
            raise ValueError(f"JSON audit root must be an object: {path}")
        return str(payload.get("version") or "")
    return "markdown-v1"


def _default_data_root(root: Path) -> Path:
    for parent in (root, *root.parents):
        candidate = parent / "StockModelData"
        if candidate.exists():
            return candidate
    return root.parent / "StockModelData"


def _runner_module_path() -> Path:
    return Path(__file__).with_name("standard_template_audit_runner.py")


_PORTABLE_TEXT_IDENTITY_SUFFIXES = frozenset(
    {
        ".csv",
        ".html",
        ".htm",
        ".json",
        ".md",
        ".py",
        ".toml",
        ".tsv",
        ".txt",
        ".xml",
        ".xsd",
        ".yaml",
        ".yml",
    }
)


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _portable_payload_sha256(payload: bytes) -> str:
    normalized = payload.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    return hashlib.sha256(normalized).hexdigest()


def _portable_file_sha256(path: Path) -> str:
    """Hash declared text contracts portably without weakening binary identity.

    Git may materialize declared text contracts with LF or CRLF in different
    worktrees.  Their audit identity therefore normalizes only newline encoding.
    Binary and unclassified artifacts retain exact raw-byte identity.
    """

    payload = path.read_bytes()
    if path.suffix.casefold() in _PORTABLE_TEXT_IDENTITY_SUFFIXES:
        return _portable_payload_sha256(payload)
    return hashlib.sha256(payload).hexdigest()


def _artifact_file_sha256(path: Path, output_schema: str) -> str:
    if output_schema == "json" or output_schema.startswith("text/"):
        return _portable_payload_sha256(path.read_bytes())
    return _file_sha256(path)


def _artifact_payload_sha256(payload: bytes, output_schema: str) -> str:
    if output_schema == "json" or output_schema.startswith("text/"):
        return _portable_payload_sha256(payload)
    return hashlib.sha256(payload).hexdigest()


def _payload_sha256(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
