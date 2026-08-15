"""Repository-native golden contract for the accepted ANF Valuation product.

The deterministic golden is the pre-native workbook produced from the accepted
Summary/BS golden plus the committed Valuation projection plan.  Native Excel
outputs are acceptance evidence because Excel-owned serialization is not a
deterministic product identity.  Production workbook routing remains unwired.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Mapping

from pbi_xbrl.longitudinal_memory.formula_aware_workbook_materialization import (
    CALCULATION_METADATA_CONTRACT,
    FORMULA_CACHE_POLICY,
    MATERIALIZER_CONTRACT,
    canonical_ooxml_sha256,
    materialize_formula_aware_mutations,
    sha256_file,
)
from pbi_xbrl.longitudinal_memory.valuation_source_native_projection import (
    BASE_SUMMARY_BS_GOLDEN_SHA256,
    CALCULATION_METADATA_POLICY_ID,
    PROJECTION_CONTRACT,
    VALUATION_CALCULATION_METADATA_POLICY,
    load_valuation_projection_plan,
)


GOLDEN_MANIFEST_TYPE = "ValuationSourceNativeGoldenManifest@1"
GOLDEN_ID = "valuation-source-native:anf@1.0.0"
GOLDEN_WORKBOOK_ID = "valuation-source-native-workbook:anf@1.0.0"
GOLDEN_ACCEPTANCE_STATUS = "golden_accepted"
GOLDEN_LIFECYCLE = "target_not_wired"
GOLDEN_PRODUCTION_DEFAULT = False
GOLDEN_MANIFEST_VERSION = "1.0.0"
GOLDEN_FIXTURE_HASH_CONTRACT = "checkout-lf-normalized-file-sha256@1"
GOLDEN_SEMANTIC_HASH_CONTRACT = "valuation-pre-native-semantic-snapshot-sha256@1"
GOLDEN_CANONICAL_OOXML_HASH_CONTRACT = "ordered-uncompressed-ooxml-members-sha256@1"
GOLDEN_FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "valuation"
GOLDEN_MANIFEST_PATH = GOLDEN_FIXTURE_ROOT / "anf_valuation_golden_manifest.v1.json"

_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_TOP_LEVEL_KEYS = {
    "acceptance",
    "acceptance_status",
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
    "projection",
    "workbook_golden",
}


class ValuationGoldenContractError(ValueError):
    """Raised when the registered Valuation golden fails closed."""


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValuationGoldenContractError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def load_json_strict(path: Path | str) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"), object_pairs_hook=_unique_object)


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def fixture_bytes(path: Path | str) -> bytes:
    candidate = Path(path)
    load_json_strict(candidate)
    return candidate.read_bytes().replace(b"\r\n", b"\n")


def fixture_sha256(path: Path | str) -> str:
    return hashlib.sha256(fixture_bytes(path)).hexdigest()


def checkout_file_sha256(path: Path | str) -> str:
    return hashlib.sha256(Path(path).read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def manifest_digest(manifest: Mapping[str, Any]) -> str:
    payload = dict(manifest)
    payload.pop("manifest_digest", None)
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _require_sha256(value: Any, *, label: str) -> str:
    normalized = str(value or "").casefold()
    if _SHA256_RE.fullmatch(normalized) is None:
        raise ValuationGoldenContractError(f"{label} is not a concrete SHA-256.")
    return normalized


def _resolve_relative(root: Path, relative_path: Any, *, label: str) -> Path:
    relative = Path(str(relative_path or ""))
    if relative.is_absolute() or ".." in relative.parts:
        raise ValuationGoldenContractError(f"{label} must be a contained relative path.")
    resolved = (root / relative).resolve()
    if resolved != root.resolve() and root.resolve() not in resolved.parents:
        raise ValuationGoldenContractError(f"{label} escapes its registered root.")
    return resolved


def verify_golden_manifest(
    manifest_path: Path | str = GOLDEN_MANIFEST_PATH,
) -> dict[str, Any]:
    """Validate the closed manifest and every committed content identity."""

    path = Path(manifest_path).resolve()
    manifest = load_json_strict(path)
    if not isinstance(manifest, dict) or set(manifest) != _TOP_LEVEL_KEYS:
        raise ValuationGoldenContractError("Valuation golden top-level keys are not closed.")
    if manifest.get("manifest_type") != GOLDEN_MANIFEST_TYPE:
        raise ValuationGoldenContractError("Valuation golden manifest type changed.")
    if manifest.get("golden_id") != GOLDEN_ID:
        raise ValuationGoldenContractError("Valuation golden ID changed.")
    if manifest.get("golden_version") != GOLDEN_MANIFEST_VERSION:
        raise ValuationGoldenContractError("Valuation golden version changed.")
    if manifest.get("acceptance_status") != GOLDEN_ACCEPTANCE_STATUS:
        raise ValuationGoldenContractError("Valuation golden is not accepted.")
    if manifest.get("lifecycle") != GOLDEN_LIFECYCLE:
        raise ValuationGoldenContractError("Valuation workbook bridge is not target_not_wired.")
    if manifest.get("production_default") is not GOLDEN_PRODUCTION_DEFAULT:
        raise ValuationGoldenContractError("Valuation golden cannot be a production default.")
    if manifest.get("generated_timestamp") is not None:
        raise ValuationGoldenContractError("Deterministic golden manifest cannot contain a timestamp.")
    if manifest.get("fixture_hash_contract") != GOLDEN_FIXTURE_HASH_CONTRACT:
        raise ValuationGoldenContractError("Unsupported Valuation fixture hash contract.")
    declared_manifest_digest = _require_sha256(
        manifest.get("manifest_digest"), label="manifest_digest"
    )
    if manifest_digest(manifest) != declared_manifest_digest:
        raise ValuationGoldenContractError("Valuation golden manifest digest mismatch.")

    fixture_root = path.parent
    fixture_rows = manifest.get("fixture_artifacts")
    if not isinstance(fixture_rows, list) or not fixture_rows:
        raise ValuationGoldenContractError("Valuation golden has no fixture artifacts.")
    fixture_paths: list[str] = []
    verified_fixtures: list[dict[str, Any]] = []
    for row in fixture_rows:
        if not isinstance(row, dict) or set(row) != {"relative_path", "sha256"}:
            raise ValuationGoldenContractError("Valuation fixture row is not closed.")
        relative = str(row["relative_path"])
        fixture_paths.append(relative)
        artifact = _resolve_relative(fixture_root, relative, label="fixture relative_path")
        if not artifact.is_file():
            raise ValuationGoldenContractError(f"Missing Valuation golden fixture: {relative}")
        actual = fixture_sha256(artifact)
        if actual != _require_sha256(row["sha256"], label=f"fixture {relative}"):
            raise ValuationGoldenContractError(f"Valuation fixture hash mismatch: {relative}")
        verified_fixtures.append(
            {"relative_path": relative, "sha256": actual, "size_bytes": artifact.stat().st_size}
        )
    if len(fixture_paths) != len(set(fixture_paths)):
        raise ValuationGoldenContractError("Valuation fixture paths are duplicated.")

    repository_root = Path(__file__).resolve().parents[2]
    implementation_rows = manifest.get("implementation_artifacts")
    if not isinstance(implementation_rows, list) or not implementation_rows:
        raise ValuationGoldenContractError("Valuation golden has no implementation identities.")
    implementation_paths: list[str] = []
    verified_implementation: list[dict[str, Any]] = []
    for row in implementation_rows:
        if not isinstance(row, dict) or set(row) != {"repository_path", "sha256"}:
            raise ValuationGoldenContractError("Valuation implementation row is not closed.")
        relative = str(row["repository_path"])
        implementation_paths.append(relative)
        artifact = _resolve_relative(repository_root, relative, label="implementation repository_path")
        if not artifact.is_file():
            raise ValuationGoldenContractError(f"Missing Valuation implementation: {relative}")
        actual = checkout_file_sha256(artifact)
        if actual != _require_sha256(row["sha256"], label=f"implementation {relative}"):
            raise ValuationGoldenContractError(f"Valuation implementation hash mismatch: {relative}")
        verified_implementation.append(
            {"repository_path": relative, "sha256": actual, "size_bytes": artifact.stat().st_size}
        )
    if len(implementation_paths) != len(set(implementation_paths)):
        raise ValuationGoldenContractError("Valuation implementation paths are duplicated.")

    projection = manifest.get("projection")
    if not isinstance(projection, dict):
        raise ValuationGoldenContractError("Valuation golden lacks projection identity.")
    plan_path = _resolve_relative(
        fixture_root, projection.get("plan_fixture"), label="projection plan fixture"
    )
    plan = load_valuation_projection_plan(
        plan_path,
        expected_projection_digest=str(projection.get("projection_digest") or ""),
        expected_formula_plan_digest=str(projection.get("formula_plan_digest") or ""),
        expected_defined_name_plan_digest=str(projection.get("defined_name_plan_digest") or ""),
    )
    if len(plan.cell_mutations) != 1105 or len(plan.defined_name_mutations) != 90:
        raise ValuationGoldenContractError("Valuation plan is not the accepted bounded plan.")

    materialization = manifest.get("materialization")
    workbook = manifest.get("workbook_golden")
    acceptance = manifest.get("acceptance")
    if not isinstance(materialization, dict):
        raise ValuationGoldenContractError("Valuation materialization contract is missing.")
    if materialization.get("materializer_contract") != MATERIALIZER_CONTRACT:
        raise ValuationGoldenContractError("Valuation materializer contract changed.")
    if materialization.get("calculation_metadata_contract") != CALCULATION_METADATA_CONTRACT:
        raise ValuationGoldenContractError("Valuation calculation-metadata contract changed.")
    if materialization.get("calculation_metadata_policy_id") != CALCULATION_METADATA_POLICY_ID:
        raise ValuationGoldenContractError("Valuation calculation-metadata owner changed.")
    if materialization.get("formula_cache_policy") != FORMULA_CACHE_POLICY:
        raise ValuationGoldenContractError("Valuation formula cache policy changed.")
    if materialization.get("canonical_ooxml_hash_contract") != GOLDEN_CANONICAL_OOXML_HASH_CONTRACT:
        raise ValuationGoldenContractError("Valuation canonical OOXML contract changed.")
    if not isinstance(workbook, dict) or workbook.get("workbook_id") != GOLDEN_WORKBOOK_ID:
        raise ValuationGoldenContractError("Valuation workbook golden identity is missing.")
    if workbook.get("semantic_hash_contract") != GOLDEN_SEMANTIC_HASH_CONTRACT:
        raise ValuationGoldenContractError("Valuation semantic hash contract changed.")
    for key in ("raw_sha256", "semantic_sha256", "canonical_ooxml_sha256"):
        _require_sha256(workbook.get(key), label=f"workbook_golden.{key}")
    if not isinstance(acceptance, dict) or acceptance.get("passed") is not True:
        raise ValuationGoldenContractError("Valuation golden acceptance receipt is not passed.")
    acceptance_fixture = _resolve_relative(
        fixture_root, acceptance.get("acceptance_fixture"), label="acceptance fixture"
    )
    accepted = load_json_strict(acceptance_fixture)
    if accepted.get("status") != "PASS" or accepted.get("native_acceptance", {}).get("passed") is not True:
        raise ValuationGoldenContractError("Valuation native acceptance is not passed.")

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
    """Replay the deterministic Valuation golden from committed fixtures."""

    verification = verify_golden_manifest(manifest_path)
    manifest = verification["manifest"]
    fixture_root = Path(manifest_path).resolve().parent
    workbook = manifest["workbook_golden"]
    base = Path(base_workbook)
    if sha256_file(base) != _require_sha256(
        workbook.get("base_summary_bs_golden_sha256"), label="base_summary_bs_golden_sha256"
    ):
        raise ValuationGoldenContractError("Summary/BS golden base identity mismatch.")
    if sha256_file(base) != BASE_SUMMARY_BS_GOLDEN_SHA256:
        raise ValuationGoldenContractError("Registered Summary/BS golden identity changed.")
    projection = manifest["projection"]
    plan = load_valuation_projection_plan(
        _resolve_relative(
            fixture_root, projection["plan_fixture"], label="projection plan fixture"
        ),
        expected_projection_digest=projection["projection_digest"],
        expected_formula_plan_digest=projection["formula_plan_digest"],
        expected_defined_name_plan_digest=projection["defined_name_plan_digest"],
    )
    result = materialize_formula_aware_mutations(
        base_workbook=base,
        output_workbook=output_workbook,
        cell_mutations=plan.cell_mutations,
        defined_name_mutations=plan.defined_name_mutations,
        merge_mutations=plan.merge_mutations,
        row_mutations=plan.row_mutations,
        calculation_metadata_policy=VALUATION_CALCULATION_METADATA_POLICY,
        expected_base_sha256=plan.base_workbook_sha256,
    )
    if result.output_workbook_sha256 != workbook["raw_sha256"]:
        raise ValuationGoldenContractError("Reproduced Valuation golden raw hash mismatch.")
    if canonical_ooxml_sha256(output_workbook) != workbook["canonical_ooxml_sha256"]:
        raise ValuationGoldenContractError("Reproduced Valuation canonical OOXML hash mismatch.")
    return {
        **result.as_dict(),
        "acceptance_status": GOLDEN_ACCEPTANCE_STATUS,
        "golden_id": GOLDEN_ID,
        "golden_manifest_digest": manifest["manifest_digest"],
        "production_default": GOLDEN_PRODUCTION_DEFAULT,
        "projection_digest": plan.projection_digest,
        "reproduced_from_committed_fixtures": True,
        "semantic_hash_contract": workbook["semantic_hash_contract"],
        "semantic_sha256": workbook["semantic_sha256"],
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
    "ValuationGoldenContractError",
    "canonical_json_bytes",
    "checkout_file_sha256",
    "fixture_bytes",
    "fixture_sha256",
    "load_json_strict",
    "manifest_digest",
    "reproduce_registered_golden",
    "verify_golden_manifest",
]
