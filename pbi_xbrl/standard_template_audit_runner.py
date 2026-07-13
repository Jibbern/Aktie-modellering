"""Controlled isolated execution for standard-template audit generators."""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

from pbi_xbrl.standard_template_audit_freshness import (
    AUDIT_RECEIPT_VERSION,
    DEFAULT_AUDIT_CONTRACTS,
    GENERATION_RUNNER_CONTRACT_VERSION,
    ROOT,
    AuditGeneratorContract,
    _default_data_root,
    _file_digest_record,
    _file_sha256,
    _now,
    _output_version,
    _path_label,
    _receipt_generation_id,
    _resolve_path,
    canonical_audit_content_sha256,
    compute_audit_generator_execution_contract_signature,
    generation_run_id_from_receipt,
    receipt_path_for_artifact,
)


_ARTIFACT_TOKEN = re.compile(r"\{artifact:(\d+)\}")


@dataclass(frozen=True)
class GenerationRunResult:
    """Immutable output of one isolated generator execution.

    This is a typed transport object, not cryptographic proof.  Audit freshness
    is established only by independently replaying the declared generator.
    """

    _payload_json: str = field(repr=False)
    _generated_output_bytes: tuple[bytes, ...] = field(repr=False)

    @property
    def payload(self) -> dict[str, Any]:
        return json.loads(self._payload_json)


@dataclass(frozen=True)
class AuditGenerationResult:
    generation_run: GenerationRunResult
    comparison_rows: tuple[dict[str, Any], ...]
    receipt_paths: tuple[Path, ...]
    stdout: str
    stderr: str


def run_audit_generator(
    generator_path: Path | str,
    *,
    root: Path = ROOT,
    data_root: Path | None = None,
    receipt_root: Path | None = None,
    contracts: Sequence[AuditGeneratorContract] = DEFAULT_AUDIT_CONTRACTS,
    generated_at_utc: str | None = None,
) -> AuditGenerationResult:
    """Run one declared generator in temp, promote outputs, and record metadata."""

    root = root.expanduser().resolve()
    data_root = (data_root or _default_data_root(root)).expanduser().resolve()
    receipt_root = (receipt_root or (root / "docs" / "audit_receipts")).expanduser().resolve()
    contract = _find_contract(generator_path, root=root, contracts=contracts)
    previous = _existing_artifact_state(contract, root=root, data_root=data_root)

    with tempfile.TemporaryDirectory(prefix="standard-template-audit-") as raw_temp:
        token, stdout, stderr = _execute_generator(
            contract,
            temp_root=Path(raw_temp),
            root=root,
            data_root=data_root,
        )
        comparisons = _comparison_rows(token, previous)
        _promote_generation_outputs(token, contract=contract, root=root, data_root=data_root)
        receipt_paths = record_generation_receipts(
            token,
            root=root,
            data_root=data_root,
            receipt_root=receipt_root,
            contracts=contracts,
            generated_at_utc=generated_at_utc,
        )
    return AuditGenerationResult(
        generation_run=token,
        comparison_rows=tuple(comparisons),
        receipt_paths=tuple(receipt_paths),
        stdout=stdout,
        stderr=stderr,
    )


def verify_deterministic_audit_replay(
    generator_path: Path | str,
    *,
    root: Path = ROOT,
    data_root: Path | None = None,
    contracts: Sequence[AuditGeneratorContract] = DEFAULT_AUDIT_CONTRACTS,
) -> dict[str, Any]:
    """Rerun an audit in temp and compare stable content without writing outputs."""

    root = root.expanduser().resolve()
    data_root = (data_root or _default_data_root(root)).expanduser().resolve()
    contract = _find_contract(generator_path, root=root, contracts=contracts)
    if contract.replay_policy != "deterministic":
        return {
            "status": "SKIPPED",
            "performed": False,
            "reason": f"Replay policy is {contract.replay_policy!r}.",
            "generator": contract.generator,
            "artifacts": [],
            "stdout": "",
            "stderr": "",
        }
    with tempfile.TemporaryDirectory(prefix="standard-template-audit-replay-") as raw_temp:
        token, stdout, stderr = _execute_generator(
            contract,
            temp_root=Path(raw_temp),
            root=root,
            data_root=data_root,
        )
        rows: list[dict[str, Any]] = []
        for generated in token.payload["generated_outputs"]:
            artifact = next(row for row in contract.artifacts if row.path == generated["path"])
            current_path = _resolve_path(artifact.path, root=root, data_root=data_root)
            current_exists = current_path.exists()
            current_canonical = (
                canonical_audit_content_sha256(current_path, artifact.output_schema)
                if current_exists
                else ""
            )
            rows.append(
                {
                    "path": artifact.path,
                    "current_exists": current_exists,
                    "canonical_content_matches": current_canonical == generated["canonical_content_sha256"],
                    "current_canonical_content_sha256": current_canonical,
                    "replayed_canonical_content_sha256": generated["canonical_content_sha256"],
                }
            )
    return {
        "status": "PASS" if rows and all(row["canonical_content_matches"] for row in rows) else "FAIL",
        "performed": True,
        "reason": "" if rows and all(row["canonical_content_matches"] for row in rows) else "Canonical replay output differs from the checked-in artifact.",
        "generator": contract.generator,
        "run_generation_id": token.payload["run_generation_id"],
        "artifacts": rows,
        "stdout": stdout,
        "stderr": stderr,
    }


def record_generation_receipts(
    generation_run: GenerationRunResult,
    *,
    root: Path = ROOT,
    data_root: Path | None = None,
    receipt_root: Path | None = None,
    contracts: Sequence[AuditGeneratorContract] = DEFAULT_AUDIT_CONTRACTS,
    generated_at_utc: str | None = None,
) -> list[Path]:
    """Record generation metadata for a completed isolated run.

    The receipt never establishes freshness; only deterministic replay does.
    """

    if not isinstance(generation_run, GenerationRunResult):
        raise TypeError("Generation receipts require a GenerationRunResult.")
    root = root.expanduser().resolve()
    data_root = (data_root or _default_data_root(root)).expanduser().resolve()
    receipt_root = (receipt_root or (root / "docs" / "audit_receipts")).expanduser().resolve()
    payload = generation_run.payload
    contract = _find_contract(payload.get("generator", {}).get("path", ""), root=root, contracts=contracts)
    _validate_generation_run_result(generation_run, contract=contract, root=root, data_root=data_root)

    receipt_root.mkdir(parents=True, exist_ok=True)
    generated_at = generated_at_utc or _now()
    paths: list[Path] = []
    for artifact in contract.artifacts:
        receipt = _build_generation_receipt(
            generation_run,
            contract=contract,
            artifact_path=artifact.path,
            generated_at_utc=generated_at,
        )
        path = receipt_path_for_artifact(artifact.path, receipt_root=receipt_root)
        path.write_text(json.dumps(receipt, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        paths.append(path)
    return paths


def _execute_generator(
    contract: AuditGeneratorContract,
    *,
    temp_root: Path,
    root: Path,
    data_root: Path,
) -> tuple[GenerationRunResult, str, str]:
    if not contract.command_arguments:
        raise ValueError(f"Audit generator has no controlled execution contract: {contract.generator}")
    inputs_before = [
        _file_digest_record(path, root=root, data_root=data_root)
        for path in contract.authoritative_inputs
    ]
    generator_before = _generator_identity(contract, root=root, data_root=data_root)
    output_paths = [temp_root / artifact.path for artifact in contract.artifacts]
    for path in output_paths:
        path.parent.mkdir(parents=True, exist_ok=True)
    arguments = [
        _render_argument(
            raw,
            root=root,
            data_root=data_root,
            temp_root=temp_root,
            output_paths=output_paths,
        )
        for raw in contract.command_arguments
    ]
    generator_path = _resolve_path(contract.generator, root=root, data_root=data_root)
    environment = dict(os.environ)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["STANDARD_TEMPLATE_AUDIT_ISOLATED_RUN"] = "1"
    process = subprocess.run(
        [sys.executable, "-B", str(generator_path), *arguments],
        cwd=root,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    if process.returncode != 0:
        raise RuntimeError(
            f"Audit generator failed ({contract.generator}, exit {process.returncode}):\n"
            f"{process.stdout}\n{process.stderr}"
        )
    inputs_after = [
        _file_digest_record(path, root=root, data_root=data_root)
        for path in contract.authoritative_inputs
    ]
    generator_after = _generator_identity(contract, root=root, data_root=data_root)
    if inputs_after != inputs_before:
        raise RuntimeError("Authoritative audit inputs changed during isolated generation.")
    if generator_after != generator_before:
        raise RuntimeError("Audit generator implementation or dependency changed during generation.")

    output_rows: list[dict[str, Any]] = []
    output_bytes: list[bytes] = []
    for artifact, path in zip(contract.artifacts, output_paths, strict=True):
        if not path.exists():
            raise RuntimeError(f"Audit generator did not create declared output: {artifact.path}")
        version = _output_version(path, artifact.output_schema)
        if version != artifact.output_version:
            raise RuntimeError(
                f"Generated audit version mismatch for {artifact.path}: "
                f"expected {artifact.output_version}, got {version}."
            )
        raw = path.read_bytes()
        output_bytes.append(raw)
        output_rows.append(
            {
                "path": artifact.path,
                "sha256": _file_sha256(path),
                "canonical_content_sha256": canonical_audit_content_sha256(path, artifact.output_schema),
                "output_schema": artifact.output_schema,
                "output_version": version,
            }
        )
    run_shell = {
        "runner_contract_version": GENERATION_RUNNER_CONTRACT_VERSION,
        "runner_implementation_sha256": _file_sha256(Path(__file__).resolve()),
        "generator": generator_before,
        "authoritative_inputs": inputs_before,
        "generated_outputs": output_rows,
        "successful_completion": True,
    }
    run_generation_id = _run_generation_id(run_shell)
    payload = {
        **run_shell,
        "run_generation_id": run_generation_id,
    }
    token = GenerationRunResult(
        _payload_json=json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
        _generated_output_bytes=tuple(output_bytes),
    )
    return token, process.stdout, process.stderr


def _validate_generation_run_result(
    token: GenerationRunResult,
    *,
    contract: AuditGeneratorContract,
    root: Path,
    data_root: Path,
) -> None:
    if not isinstance(token, GenerationRunResult):
        raise TypeError("Generation metadata requires a GenerationRunResult.")
    payload = token.payload
    current_generator = _generator_identity(contract, root=root, data_root=data_root)
    current_inputs = [
        _file_digest_record(path, root=root, data_root=data_root)
        for path in contract.authoritative_inputs
    ]
    if payload.get("generator") != current_generator:
        raise ValueError("Verified generation token no longer matches generator identity.")
    if payload.get("authoritative_inputs") != current_inputs:
        raise ValueError("Verified generation token no longer matches authoritative inputs.")
    if payload.get("successful_completion") is not True:
        raise ValueError("Verified generation token does not represent successful completion.")
    if payload.get("runner_contract_version") != GENERATION_RUNNER_CONTRACT_VERSION:
        raise ValueError("Verified generation token uses an unsupported runner contract.")
    if payload.get("runner_implementation_sha256") != _file_sha256(Path(__file__).resolve()):
        raise ValueError("Verified generation token no longer matches the runner implementation.")
    expected_run_id = _run_generation_id(payload)
    if payload.get("run_generation_id") != expected_run_id:
        raise ValueError("Verified generation token run id is invalid.")
    outputs = payload.get("generated_outputs") or []
    if len(outputs) != len(token._generated_output_bytes) or len(outputs) != len(contract.artifacts):
        raise ValueError("Verified generation token output cardinality is invalid.")
    for artifact, output, raw in zip(contract.artifacts, outputs, token._generated_output_bytes, strict=True):
        if output.get("path") != artifact.path or _payload_sha256_bytes(raw) != output.get("sha256"):
            raise ValueError("Verified generation token output bytes do not match its declaration.")


def _promote_generation_outputs(
    token: GenerationRunResult,
    *,
    contract: AuditGeneratorContract,
    root: Path,
    data_root: Path,
) -> None:
    payload = token.payload
    _validate_generation_run_result(token, contract=contract, root=root, data_root=data_root)
    for artifact, output, raw in zip(contract.artifacts, payload["generated_outputs"], token._generated_output_bytes, strict=True):
        target = _resolve_path(artifact.path, root=root, data_root=data_root)
        target.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=target.parent, prefix=f".{target.name}.", delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(raw)
        os.replace(temporary, target)
        if _file_sha256(target) != output["sha256"]:
            raise RuntimeError(f"Promoted audit digest mismatch: {artifact.path}")


def _build_generation_receipt(
    token: GenerationRunResult,
    *,
    contract: AuditGeneratorContract,
    artifact_path: str,
    generated_at_utc: str,
) -> dict[str, Any]:
    if not isinstance(token, GenerationRunResult):
        raise TypeError("Generation metadata requires a GenerationRunResult.")
    payload = token.payload
    output = next(row for row in payload["generated_outputs"] if row["path"] == artifact_path)
    receipt: dict[str, Any] = {
        "receipt_version": AUDIT_RECEIPT_VERSION,
        "status": "generated",
        "reason": "",
        "artifact": {
            "path": artifact_path,
            "sha256": output["sha256"],
            "output_schema": output["output_schema"],
            "output_version": output["output_version"],
        },
        "authoritative_inputs": payload["authoritative_inputs"],
        "generator": payload["generator"],
        "verification": {
            "mode": "controlled_isolated_generator_run",
            "runner_contract_version": payload["runner_contract_version"],
            "successful_completion": True,
            "run_generation_id": payload["run_generation_id"],
            "generated_outputs": payload["generated_outputs"],
        },
        "generated_at_utc": generated_at_utc,
    }
    if generation_run_id_from_receipt(receipt) != payload["run_generation_id"]:
        raise ValueError("Verified generation payload cannot be represented losslessly in a receipt.")
    receipt["generation_id"] = _receipt_generation_id(receipt)
    return receipt


def _generator_identity(
    contract: AuditGeneratorContract,
    *,
    root: Path,
    data_root: Path,
) -> dict[str, Any]:
    freshness_path = Path(__file__).with_name("standard_template_audit_freshness.py")
    return {
        "path": contract.generator,
        "contract_version": contract.contract_version,
        "implementation_sha256": _file_sha256(
            _resolve_path(contract.generator, root=root, data_root=data_root)
        ),
        "receipt_engine_sha256": _file_sha256(freshness_path),
        "runner_implementation_sha256": _file_sha256(Path(__file__).resolve()),
        "execution_contract_sha256": compute_audit_generator_execution_contract_signature(contract),
        "dependencies": [
            _file_digest_record(path, root=root, data_root=data_root)
            for path in contract.dependencies
        ],
    }


def _run_generation_id(payload: Mapping[str, Any]) -> str:
    receipt_view = {
        "generator": payload.get("generator"),
        "authoritative_inputs": payload.get("authoritative_inputs"),
        "verification": {
            "runner_contract_version": payload.get("runner_contract_version"),
            "generated_outputs": payload.get("generated_outputs"),
            "successful_completion": payload.get("successful_completion"),
        },
    }
    return generation_run_id_from_receipt(receipt_view)


def _find_contract(
    generator_path: Path | str,
    *,
    root: Path,
    contracts: Sequence[AuditGeneratorContract],
) -> AuditGeneratorContract:
    raw_label = str(generator_path).replace("\\", "/")
    label = raw_label if any(contract.generator == raw_label for contract in contracts) else _path_label(Path(generator_path), root=root)
    matches = [contract for contract in contracts if contract.generator == label]
    if len(matches) != 1:
        raise ValueError(f"Expected one audit generator contract for {label!r}; found {len(matches)}.")
    return matches[0]


def _render_argument(
    raw: str,
    *,
    root: Path,
    data_root: Path,
    temp_root: Path,
    output_paths: Sequence[Path],
) -> str:
    value = raw.replace("{root}", str(root)).replace("{data_root}", str(data_root)).replace("{temp_root}", str(temp_root))

    def replace_artifact(match: re.Match[str]) -> str:
        index = int(match.group(1))
        if index >= len(output_paths):
            raise ValueError(f"Audit execution contract references missing artifact index {index}.")
        return str(output_paths[index])

    return _ARTIFACT_TOKEN.sub(replace_artifact, value)


def _existing_artifact_state(
    contract: AuditGeneratorContract,
    *,
    root: Path,
    data_root: Path,
) -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    for artifact in contract.artifacts:
        path = _resolve_path(artifact.path, root=root, data_root=data_root)
        rows[artifact.path] = {
            "sha256": _file_sha256(path) if path.exists() else "",
            "canonical_content_sha256": (
                canonical_audit_content_sha256(path, artifact.output_schema) if path.exists() else ""
            ),
        }
    return rows


def _comparison_rows(
    token: GenerationRunResult,
    previous: Mapping[str, Mapping[str, str]],
) -> list[dict[str, Any]]:
    rows = []
    for output in token.payload["generated_outputs"]:
        before = previous.get(output["path"], {})
        rows.append(
            {
                "path": output["path"],
                "previous_sha256": str(before.get("sha256") or ""),
                "generated_sha256": output["sha256"],
                "byte_identical": before.get("sha256") == output["sha256"],
                "previous_canonical_content_sha256": str(before.get("canonical_content_sha256") or ""),
                "generated_canonical_content_sha256": output["canonical_content_sha256"],
                "canonical_content_identical": (
                    before.get("canonical_content_sha256") == output["canonical_content_sha256"]
                ),
            }
        )
    return rows


def _payload_sha256_bytes(raw: bytes) -> str:
    import hashlib

    return hashlib.sha256(raw).hexdigest()
