"""Repository-native golden contract for the ANF Summary/BS source-native product.

The golden freezes reviewed source-native products, shadows, a workbook surface map,
and the immutable binding plan.  Workbook replay remains a non-production operation:
the materializer writes only a caller-provided scratch output and the lifecycle stays
``target_not_wired``.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Mapping

from pbi_xbrl.longitudinal_memory.summary_bs_workbook_materialization import (
    CANONICAL_OOXML_HASH_CONTRACT,
    canonical_ooxml_sha256,
    load_materialization_plan,
    materialize_summary_bs_preview,
    sha256_file,
)


GOLDEN_MANIFEST_TYPE = "SummaryBSSourceNativeGoldenManifest@1"
GOLDEN_ID = "summary-bs-source-native:anf@1.0.0"
GOLDEN_WORKBOOK_ID = "summary-bs-source-native-workbook:anf@1.0.0"
GOLDEN_ACCEPTANCE_STATUS = "golden_accepted"
GOLDEN_LIFECYCLE = "target_not_wired"
GOLDEN_PRODUCTION_DEFAULT = False
GOLDEN_MANIFEST_VERSION = "1.0.0"
GOLDEN_FIXTURE_HASH_CONTRACT = "checkout-lf-normalized-file-sha256@1"
GOLDEN_SEMANTIC_HASH_CONTRACT = "workbook-semantic-snapshot-sha256@1"
GOLDEN_CANONICAL_OOXML_HASH_CONTRACT = CANONICAL_OOXML_HASH_CONTRACT
GOLDEN_FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "summary_bs"
GOLDEN_MANIFEST_PATH = GOLDEN_FIXTURE_ROOT / "anf_summary_bs_golden_manifest.v1.json"

_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_TOP_LEVEL_KEYS = {
    "acceptance",
    "acceptance_status",
    "binding",
    "checkpoint",
    "economic_product",
    "fixture_artifacts",
    "fixture_hash_contract",
    "generated_timestamp",
    "golden_id",
    "golden_version",
    "implementation_artifacts",
    "lifecycle",
    "manifest_digest",
    "manifest_type",
    "materialization",
    "production_default",
    "workbook_golden",
}


class SummaryBSGoldenContractError(ValueError):
    """Raised when a registered Summary/BS golden fails closed."""


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise SummaryBSGoldenContractError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def load_json_strict(path: Path | str) -> Any:
    return json.loads(
        Path(path).read_text(encoding="utf-8"),
        object_pairs_hook=_unique_object,
    )


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def fixture_bytes(path: Path | str) -> bytes:
    """Validate strict JSON and normalize only Git CRLF checkout conversion."""

    candidate = Path(path)
    load_json_strict(candidate)
    return candidate.read_bytes().replace(b"\r\n", b"\n")


def fixture_sha256(path: Path | str) -> str:
    return hashlib.sha256(fixture_bytes(path)).hexdigest()


def checkout_file_sha256(path: Path | str) -> str:
    """Hash text implementation content independently of Git CRLF checkout policy."""

    return hashlib.sha256(Path(path).read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def manifest_digest(manifest: Mapping[str, Any]) -> str:
    payload = dict(manifest)
    payload.pop("manifest_digest", None)
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _require_sha256(value: Any, *, label: str) -> str:
    normalized = str(value or "").lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise SummaryBSGoldenContractError(f"{label} is not a concrete SHA-256.")
    return normalized


def _resolve_relative(root: Path, relative_path: Any, *, label: str) -> Path:
    relative = Path(str(relative_path or ""))
    if relative.is_absolute() or ".." in relative.parts:
        raise SummaryBSGoldenContractError(f"{label} must be a contained relative path.")
    resolved = (root / relative).resolve()
    if resolved != root.resolve() and root.resolve() not in resolved.parents:
        raise SummaryBSGoldenContractError(f"{label} escapes its registered root.")
    return resolved


def verify_golden_manifest(
    manifest_path: Path | str = GOLDEN_MANIFEST_PATH,
) -> dict[str, Any]:
    """Validate the closed golden manifest and every registered content identity."""

    path = Path(manifest_path).resolve()
    manifest = load_json_strict(path)
    if not isinstance(manifest, dict) or set(manifest) != _TOP_LEVEL_KEYS:
        raise SummaryBSGoldenContractError("Golden manifest top-level keys are not closed.")
    if manifest.get("manifest_type") != GOLDEN_MANIFEST_TYPE:
        raise SummaryBSGoldenContractError("Unsupported Summary/BS golden manifest type.")
    if manifest.get("golden_id") != GOLDEN_ID:
        raise SummaryBSGoldenContractError("Unexpected Summary/BS golden ID.")
    if manifest.get("golden_version") != GOLDEN_MANIFEST_VERSION:
        raise SummaryBSGoldenContractError("Unexpected Summary/BS golden version.")
    if manifest.get("acceptance_status") != GOLDEN_ACCEPTANCE_STATUS:
        raise SummaryBSGoldenContractError("Summary/BS product is not golden accepted.")
    if manifest.get("lifecycle") != GOLDEN_LIFECYCLE:
        raise SummaryBSGoldenContractError("Summary/BS workbook bridge is not target_not_wired.")
    if manifest.get("production_default") is not GOLDEN_PRODUCTION_DEFAULT:
        raise SummaryBSGoldenContractError("Summary/BS golden cannot be a production default.")
    if manifest.get("generated_timestamp") is not None:
        raise SummaryBSGoldenContractError("Deterministic golden manifest cannot contain a timestamp.")
    if manifest.get("fixture_hash_contract") != GOLDEN_FIXTURE_HASH_CONTRACT:
        raise SummaryBSGoldenContractError("Unsupported golden fixture hash contract.")
    declared_manifest_digest = _require_sha256(
        manifest.get("manifest_digest"), label="manifest_digest"
    )
    if manifest_digest(manifest) != declared_manifest_digest:
        raise SummaryBSGoldenContractError("Golden manifest digest mismatch.")

    fixture_root = path.parent
    fixture_rows = manifest.get("fixture_artifacts")
    if not isinstance(fixture_rows, list) or not fixture_rows:
        raise SummaryBSGoldenContractError("Golden manifest has no fixture artifacts.")
    fixture_paths: list[str] = []
    verified_fixtures: list[dict[str, Any]] = []
    for row in fixture_rows:
        if not isinstance(row, dict) or set(row) != {"relative_path", "sha256"}:
            raise SummaryBSGoldenContractError("Golden fixture row is not closed.")
        relative = str(row["relative_path"])
        fixture_paths.append(relative)
        artifact = _resolve_relative(fixture_root, relative, label="fixture relative_path")
        if not artifact.is_file():
            raise SummaryBSGoldenContractError(f"Missing golden fixture: {relative}")
        actual = fixture_sha256(artifact)
        expected = _require_sha256(row["sha256"], label=f"fixture {relative}")
        if actual != expected:
            raise SummaryBSGoldenContractError(f"Golden fixture hash mismatch: {relative}")
        verified_fixtures.append(
            {"relative_path": relative, "sha256": actual, "size_bytes": artifact.stat().st_size}
        )
    if len(fixture_paths) != len(set(fixture_paths)):
        raise SummaryBSGoldenContractError("Golden fixture paths are duplicated.")

    repository_root = Path(__file__).resolve().parents[2]
    implementation_rows = manifest.get("implementation_artifacts")
    if not isinstance(implementation_rows, list) or not implementation_rows:
        raise SummaryBSGoldenContractError("Golden manifest has no implementation identities.")
    implementation_paths: list[str] = []
    verified_implementation: list[dict[str, Any]] = []
    for row in implementation_rows:
        if not isinstance(row, dict) or set(row) != {"repository_path", "sha256"}:
            raise SummaryBSGoldenContractError("Golden implementation row is not closed.")
        relative = str(row["repository_path"])
        implementation_paths.append(relative)
        artifact = _resolve_relative(repository_root, relative, label="implementation repository_path")
        if not artifact.is_file():
            raise SummaryBSGoldenContractError(f"Missing golden implementation: {relative}")
        actual = checkout_file_sha256(artifact)
        expected = _require_sha256(row["sha256"], label=f"implementation {relative}")
        if actual != expected:
            raise SummaryBSGoldenContractError(
                f"Golden implementation hash mismatch: {relative}"
            )
        verified_implementation.append(
            {"repository_path": relative, "sha256": actual, "size_bytes": artifact.stat().st_size}
        )
    if len(implementation_paths) != len(set(implementation_paths)):
        raise SummaryBSGoldenContractError("Golden implementation paths are duplicated.")

    binding = manifest.get("binding")
    if not isinstance(binding, dict):
        raise SummaryBSGoldenContractError("Golden manifest lacks binding identity.")
    plan_fixture = _resolve_relative(
        fixture_root, binding.get("plan_fixture"), label="binding plan fixture"
    )
    plan = load_materialization_plan(
        plan_fixture,
        expected_plan_digest=str(binding.get("plan_digest") or ""),
    )
    if len(plan["bindings"]) != 452 or plan["lifecycle"] != GOLDEN_LIFECYCLE:
        raise SummaryBSGoldenContractError("Golden binding plan is not the accepted 452-field plan.")

    workbook = manifest.get("workbook_golden")
    materialization = manifest.get("materialization")
    acceptance = manifest.get("acceptance")
    if not isinstance(workbook, dict) or workbook.get("workbook_id") != GOLDEN_WORKBOOK_ID:
        raise SummaryBSGoldenContractError("Golden workbook identity is missing or unsupported.")
    if not isinstance(materialization, dict):
        raise SummaryBSGoldenContractError("Golden materialization contract is missing.")
    if materialization.get("canonical_ooxml_hash_contract") != GOLDEN_CANONICAL_OOXML_HASH_CONTRACT:
        raise SummaryBSGoldenContractError("Golden canonical OOXML contract mismatch.")
    if workbook.get("semantic_hash_contract") != GOLDEN_SEMANTIC_HASH_CONTRACT:
        raise SummaryBSGoldenContractError("Golden semantic hash contract mismatch.")
    for key in ("raw_sha256", "semantic_sha256", "canonical_ooxml_sha256"):
        _require_sha256(workbook.get(key), label=f"workbook_golden.{key}")
    if not isinstance(acceptance, dict) or acceptance.get("passed") is not True:
        raise SummaryBSGoldenContractError("Golden acceptance receipt is not passed.")

    return {
        "fixture_artifacts": verified_fixtures,
        "golden_id": GOLDEN_ID,
        "implementation_artifacts": verified_implementation,
        "lifecycle": GOLDEN_LIFECYCLE,
        "manifest": manifest,
        "manifest_digest": declared_manifest_digest,
        "manifest_path": str(path),
        "passed": True,
        "production_default": GOLDEN_PRODUCTION_DEFAULT,
    }


def reproduce_registered_golden(
    *,
    base_workbook: Path | str,
    output_workbook: Path | str,
    manifest_path: Path | str = GOLDEN_MANIFEST_PATH,
) -> dict[str, Any]:
    """Replay the registered golden from committed fixtures into a scratch path."""

    verification = verify_golden_manifest(manifest_path)
    manifest = verification["manifest"]
    fixture_root = Path(manifest_path).resolve().parent
    workbook = manifest["workbook_golden"]
    base = Path(base_workbook)
    expected_base = _require_sha256(
        workbook.get("protected_oracle_sha256"), label="protected_oracle_sha256"
    )
    if sha256_file(base) != expected_base:
        raise SummaryBSGoldenContractError("Protected oracle workbook identity mismatch.")
    plan_path = _resolve_relative(
        fixture_root, manifest["binding"]["plan_fixture"], label="binding plan fixture"
    )
    plan = load_materialization_plan(
        plan_path,
        expected_plan_digest=manifest["binding"]["plan_digest"],
    )
    receipt = materialize_summary_bs_preview(
        base_workbook=base,
        output_workbook=output_workbook,
        plan=plan,
        expected_plan_digest=manifest["binding"]["plan_digest"],
    )
    if receipt["output_workbook_sha256"] != workbook["raw_sha256"]:
        raise SummaryBSGoldenContractError("Reproduced golden raw workbook hash mismatch.")
    if canonical_ooxml_sha256(output_workbook) != workbook["canonical_ooxml_sha256"]:
        raise SummaryBSGoldenContractError("Reproduced golden canonical OOXML hash mismatch.")
    return {
        **receipt,
        "acceptance_status": manifest["acceptance_status"],
        "golden_id": manifest["golden_id"],
        "golden_manifest_digest": manifest["manifest_digest"],
        "production_default": manifest["production_default"],
        "reproduced_from_committed_fixtures": True,
    }


__all__ = [
    "GOLDEN_ACCEPTANCE_STATUS",
    "GOLDEN_CANONICAL_OOXML_HASH_CONTRACT",
    "GOLDEN_FIXTURE_HASH_CONTRACT",
    "GOLDEN_FIXTURE_ROOT",
    "GOLDEN_ID",
    "GOLDEN_LIFECYCLE",
    "GOLDEN_MANIFEST_PATH",
    "GOLDEN_MANIFEST_TYPE",
    "GOLDEN_PRODUCTION_DEFAULT",
    "GOLDEN_SEMANTIC_HASH_CONTRACT",
    "GOLDEN_WORKBOOK_ID",
    "SummaryBSGoldenContractError",
    "canonical_json_bytes",
    "checkout_file_sha256",
    "fixture_bytes",
    "fixture_sha256",
    "load_json_strict",
    "manifest_digest",
    "reproduce_registered_golden",
    "verify_golden_manifest",
]
