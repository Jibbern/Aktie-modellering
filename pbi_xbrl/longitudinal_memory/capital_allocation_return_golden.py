"""Repository-native golden contract for ANF Capital Allocation / Return.

The economic product is versioned independently from the historical Valuation
golden.  Its workbook golden is a deterministic v2 successor reconstructed by
applying a closed, content-addressed OOXML member delta to the immutable
Valuation v1 workbook.  The delta performs no source selection or economic
calculation; the committed product/binding fixtures own those accepted facts.
Production workbook routing remains unwired.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Mapping
from zipfile import ZipFile

from pbi_xbrl.longitudinal_memory.capital_allocation_return_product_expansion import (
    CAPITAL_ALLOCATION_OWNER_ROUTES,
    CAPITAL_RETURN_ACTIVITY_FAMILIES,
    INVESTOR_PRODUCT_CONTRACT,
)
from pbi_xbrl.longitudinal_memory.summary_bs_workbook_materialization import (
    CANONICAL_OOXML_HASH_CONTRACT,
    canonical_ooxml_sha256,
    sha256_file,
)
from pbi_xbrl.longitudinal_memory.valuation_golden import (
    GOLDEN_ID as PREDECESSOR_GOLDEN_ID,
    GOLDEN_WORKBOOK_ID as PREDECESSOR_WORKBOOK_ID,
    verify_golden_manifest as verify_predecessor_manifest,
)
from pbi_xbrl.longitudinal_memory.valuation_guidance_net_share_polish import (
    NET_SHARE_PERCENTAGE_CONTRACT,
    POLISH_CONTRACT,
    SEMANTIC_SNAPSHOT_CONTRACT,
)


GOLDEN_MANIFEST_TYPE = "CapitalAllocationReturnSourceNativeGoldenManifest@1"
GOLDEN_ID = "capital-allocation-return-source-native:anf@1.0.0"
GOLDEN_WORKBOOK_ID = "valuation-source-native-workbook:anf@2.0.0"
GOLDEN_ACCEPTANCE_STATUS = "golden_accepted"
GOLDEN_LIFECYCLE = "target_not_wired"
GOLDEN_PRODUCTION_DEFAULT = False
GOLDEN_MANIFEST_VERSION = "1.0.0"
GOLDEN_FIXTURE_HASH_CONTRACT = "checkout-lf-normalized-file-sha256@1"
GOLDEN_SEMANTIC_HASH_CONTRACT = SEMANTIC_SNAPSHOT_CONTRACT
GOLDEN_CANONICAL_OOXML_HASH_CONTRACT = CANONICAL_OOXML_HASH_CONTRACT
GOLDEN_DELTA_CONTRACT = "valuation-capital-product-ooxml-delta-from-registered-predecessor@1"
GOLDEN_FIXTURE_ROOT = (
    Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "capital_allocation_return"
)
GOLDEN_MANIFEST_PATH = (
    GOLDEN_FIXTURE_ROOT / "anf_capital_allocation_return_golden_manifest.v1.json"
)

_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_TOP_LEVEL_KEYS = {
    "acceptance",
    "acceptance_status",
    "checkpoint",
    "cross_ticker_generality",
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
    "predecessor",
    "production_default",
    "projection",
    "workbook_golden",
}


class CapitalAllocationReturnGoldenContractError(ValueError):
    """Raised when the registered Capital Allocation / Return golden drifts."""


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise CapitalAllocationReturnGoldenContractError(f"duplicate JSON key: {key}")
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
        raise CapitalAllocationReturnGoldenContractError(
            f"{label} is not a concrete SHA-256."
        )
    return normalized


def _resolve_relative(root: Path, relative_path: Any, *, label: str) -> Path:
    relative = Path(str(relative_path or ""))
    if relative.is_absolute() or ".." in relative.parts:
        raise CapitalAllocationReturnGoldenContractError(
            f"{label} must be a contained relative path."
        )
    resolved = (root / relative).resolve()
    if resolved != root.resolve() and root.resolve() not in resolved.parents:
        raise CapitalAllocationReturnGoldenContractError(f"{label} escapes its root.")
    return resolved


def _binding_inventory(plan: Mapping[str, Any]) -> tuple[list[dict[str, Any]], str]:
    support_records = plan.get("support_records")
    if not isinstance(support_records, list) or not support_records:
        raise CapitalAllocationReturnGoldenContractError("Committed support records are missing.")
    bindings: list[dict[str, Any]] = []
    for record in support_records:
        if not isinstance(record, dict) or not isinstance(record.get("bindings"), list):
            raise CapitalAllocationReturnGoldenContractError("Support record is malformed.")
        bindings.extend(dict(binding) for binding in record["bindings"])
    digest = hashlib.sha256(canonical_json_bytes(bindings)).hexdigest()
    return bindings, digest


def _verify_projection_fixture(
    fixture_root: Path, projection: Mapping[str, Any]
) -> dict[str, Any]:
    plan_path = _resolve_relative(
        fixture_root, projection.get("plan_fixture"), label="projection plan fixture"
    )
    plan = load_json_strict(plan_path)
    if not isinstance(plan, dict) or plan.get("contract") != POLISH_CONTRACT:
        raise CapitalAllocationReturnGoldenContractError("Projection plan contract changed.")
    if plan.get("plan_digest") != projection.get("plan_digest"):
        raise CapitalAllocationReturnGoldenContractError("Projection plan digest changed.")
    if plan.get("binding_plan_digest") != projection.get("binding_plan_digest"):
        raise CapitalAllocationReturnGoldenContractError("Binding-plan digest changed.")
    if plan.get("source_package_sha256") != projection.get("source_package_sha256"):
        raise CapitalAllocationReturnGoldenContractError("Source-package identity changed.")
    expected_counts = {
        "new_binding_count": 145,
        "new_available_binding_count": 114,
        "new_unavailable_binding_count": 31,
    }
    if any(plan.get(key) != value for key, value in expected_counts.items()):
        raise CapitalAllocationReturnGoldenContractError("Accepted 145/114/31 plan changed.")
    bindings, binding_digest = _binding_inventory(plan)
    available = [row for row in bindings if row.get("status") == "available"]
    if len(bindings) != 145 or len(available) != 114:
        raise CapitalAllocationReturnGoldenContractError("Binding inventory changed.")
    if binding_digest != plan["binding_plan_digest"]:
        raise CapitalAllocationReturnGoldenContractError("Binding inventory digest mismatch.")
    targets = [str(row.get("target_cell") or "") for row in bindings]
    if len(targets) != len(set(targets)) or any(not target for target in targets):
        raise CapitalAllocationReturnGoldenContractError("Binding targets are missing or duplicated.")
    if any(
        not row.get("source_identity") or not row.get("source_ref")
        for row in available
    ):
        raise CapitalAllocationReturnGoldenContractError("Available binding lacks typed lineage.")
    net_share_rows = [
        row for row in bindings if row.get("metric_id") == "net_share_reduction_percentage"
    ]
    if len(net_share_rows) != 5:
        raise CapitalAllocationReturnGoldenContractError("Net-share percentage universe changed.")
    if any(row.get("section") == "quarterly_capital_return_history" for row in net_share_rows):
        raise CapitalAllocationReturnGoldenContractError(
            "Quarterly Capital Return unexpectedly displays net-share percentage."
        )
    return {
        "available_binding_count": len(available),
        "binding_count": len(bindings),
        "binding_plan_digest": binding_digest,
        "lineage_complete_count": len(available),
        "plan": plan,
        "plan_path": str(plan_path),
        "unavailable_binding_count": len(bindings) - len(available),
    }


def _verify_generic_contract(fixture_root: Path, manifest: Mapping[str, Any]) -> None:
    generic = manifest.get("cross_ticker_generality")
    if not isinstance(generic, dict):
        raise CapitalAllocationReturnGoldenContractError("Cross-ticker contract is missing.")
    path = _resolve_relative(
        fixture_root, generic.get("contract_fixture"), label="generic contract fixture"
    )
    contract = load_json_strict(path)
    if contract.get("investor_product_contract") != INVESTOR_PRODUCT_CONTRACT:
        raise CapitalAllocationReturnGoldenContractError("Investor-product contract changed.")
    expected_routes = [list(route) for route in CAPITAL_ALLOCATION_OWNER_ROUTES]
    if contract.get("capital_allocation_owner_routes") != expected_routes:
        raise CapitalAllocationReturnGoldenContractError("Declarative owner routing changed.")
    expected_families = {
        key: sorted(value) for key, value in CAPITAL_RETURN_ACTIVITY_FAMILIES.items()
    }
    if contract.get("capital_return_activity_families") != expected_families:
        raise CapitalAllocationReturnGoldenContractError("Activity-family contract changed.")
    if contract.get("pbi_workbook_state") != "binding_profile_required_not_wired":
        raise CapitalAllocationReturnGoldenContractError("PBI wiring state changed.")
    if contract.get("missing_is_never_zero") is not True:
        raise CapitalAllocationReturnGoldenContractError("Missing-to-zero contract changed.")


def _verify_delta(
    fixture_root: Path, materialization: Mapping[str, Any]
) -> dict[str, Any]:
    if materialization.get("delta_contract") != GOLDEN_DELTA_CONTRACT:
        raise CapitalAllocationReturnGoldenContractError("Workbook delta contract changed.")
    delta_path = _resolve_relative(
        fixture_root, materialization.get("delta_fixture"), label="delta fixture"
    )
    actual_delta_hash = sha256_file(delta_path)
    if actual_delta_hash != _require_sha256(
        materialization.get("delta_fixture_sha256"), label="delta fixture"
    ):
        raise CapitalAllocationReturnGoldenContractError("Workbook delta fixture hash mismatch.")
    rows = materialization.get("changed_members")
    if not isinstance(rows, list) or len(rows) != 17:
        raise CapitalAllocationReturnGoldenContractError("Workbook delta member count changed.")
    expected_names: list[str] = []
    with ZipFile(delta_path, "r") as delta:
        if delta.comment:
            raise CapitalAllocationReturnGoldenContractError("Delta fixture has a ZIP comment.")
        for row in rows:
            if not isinstance(row, dict) or set(row) != {"member", "sha256"}:
                raise CapitalAllocationReturnGoldenContractError("Delta member row is not closed.")
            name = str(row["member"])
            expected_names.append(name)
            if hashlib.sha256(delta.read(name)).hexdigest() != _require_sha256(
                row["sha256"], label=f"delta member {name}"
            ):
                raise CapitalAllocationReturnGoldenContractError(
                    f"Delta member hash mismatch: {name}"
                )
        if delta.namelist() != expected_names:
            raise CapitalAllocationReturnGoldenContractError(
                "Delta member order or inventory changed."
            )
    return {
        "delta_fixture": str(delta_path),
        "delta_fixture_sha256": actual_delta_hash,
        "member_count": len(expected_names),
        "members": expected_names,
    }


def verify_golden_manifest(
    manifest_path: Path | str = GOLDEN_MANIFEST_PATH,
) -> dict[str, Any]:
    """Validate the closed golden manifest and every committed identity."""

    path = Path(manifest_path).resolve()
    manifest = load_json_strict(path)
    if not isinstance(manifest, dict) or set(manifest) != _TOP_LEVEL_KEYS:
        raise CapitalAllocationReturnGoldenContractError("Golden top-level keys are not closed.")
    expected_scalars = {
        "manifest_type": GOLDEN_MANIFEST_TYPE,
        "golden_id": GOLDEN_ID,
        "golden_version": GOLDEN_MANIFEST_VERSION,
        "acceptance_status": GOLDEN_ACCEPTANCE_STATUS,
        "lifecycle": GOLDEN_LIFECYCLE,
        "fixture_hash_contract": GOLDEN_FIXTURE_HASH_CONTRACT,
    }
    if any(manifest.get(key) != value for key, value in expected_scalars.items()):
        raise CapitalAllocationReturnGoldenContractError("Golden identity or lifecycle changed.")
    if manifest.get("production_default") is not GOLDEN_PRODUCTION_DEFAULT:
        raise CapitalAllocationReturnGoldenContractError("Golden cannot be production default.")
    if manifest.get("generated_timestamp") is not None:
        raise CapitalAllocationReturnGoldenContractError("Deterministic manifest has a timestamp.")
    declared_digest = _require_sha256(manifest.get("manifest_digest"), label="manifest_digest")
    if manifest_digest(manifest) != declared_digest:
        raise CapitalAllocationReturnGoldenContractError("Golden manifest digest mismatch.")

    predecessor = manifest.get("predecessor")
    if not isinstance(predecessor, dict):
        raise CapitalAllocationReturnGoldenContractError("Predecessor identity is missing.")
    if predecessor.get("golden_id") != PREDECESSOR_GOLDEN_ID:
        raise CapitalAllocationReturnGoldenContractError("Valuation predecessor golden changed.")
    if predecessor.get("workbook_id") != PREDECESSOR_WORKBOOK_ID:
        raise CapitalAllocationReturnGoldenContractError("Valuation predecessor workbook changed.")
    predecessor_receipt = verify_predecessor_manifest()
    if predecessor_receipt["manifest_digest"] != predecessor.get("manifest_digest"):
        raise CapitalAllocationReturnGoldenContractError("Predecessor manifest digest changed.")

    fixture_root = path.parent
    fixture_rows = manifest.get("fixture_artifacts")
    if not isinstance(fixture_rows, list) or not fixture_rows:
        raise CapitalAllocationReturnGoldenContractError("Golden has no fixture artifacts.")
    verified_fixtures: list[dict[str, Any]] = []
    fixture_paths: list[str] = []
    for row in fixture_rows:
        if not isinstance(row, dict) or set(row) != {"relative_path", "sha256"}:
            raise CapitalAllocationReturnGoldenContractError("Fixture row is not closed.")
        relative = str(row["relative_path"])
        fixture_paths.append(relative)
        artifact = _resolve_relative(fixture_root, relative, label="fixture relative_path")
        actual = fixture_sha256(artifact)
        if actual != _require_sha256(row["sha256"], label=f"fixture {relative}"):
            raise CapitalAllocationReturnGoldenContractError(f"Fixture hash mismatch: {relative}")
        verified_fixtures.append(
            {"relative_path": relative, "sha256": actual, "size_bytes": artifact.stat().st_size}
        )
    if len(fixture_paths) != len(set(fixture_paths)):
        raise CapitalAllocationReturnGoldenContractError("Fixture paths are duplicated.")

    repository_root = Path(__file__).resolve().parents[2]
    implementation_rows = manifest.get("implementation_artifacts")
    if not isinstance(implementation_rows, list) or not implementation_rows:
        raise CapitalAllocationReturnGoldenContractError("Implementation identities are missing.")
    verified_implementation: list[dict[str, Any]] = []
    implementation_paths: list[str] = []
    for row in implementation_rows:
        if not isinstance(row, dict) or set(row) != {"repository_path", "sha256"}:
            raise CapitalAllocationReturnGoldenContractError("Implementation row is not closed.")
        relative = str(row["repository_path"])
        implementation_paths.append(relative)
        artifact = _resolve_relative(
            repository_root, relative, label="implementation repository_path"
        )
        actual = checkout_file_sha256(artifact)
        if actual != _require_sha256(row["sha256"], label=f"implementation {relative}"):
            raise CapitalAllocationReturnGoldenContractError(
                f"Implementation hash mismatch: {relative}"
            )
        verified_implementation.append(
            {"repository_path": relative, "sha256": actual, "size_bytes": artifact.stat().st_size}
        )
    if len(implementation_paths) != len(set(implementation_paths)):
        raise CapitalAllocationReturnGoldenContractError("Implementation paths are duplicated.")

    projection = manifest.get("projection")
    if not isinstance(projection, dict):
        raise CapitalAllocationReturnGoldenContractError("Projection identity is missing.")
    projection_receipt = _verify_projection_fixture(fixture_root, projection)
    _verify_generic_contract(fixture_root, manifest)
    materialization = manifest.get("materialization")
    if not isinstance(materialization, dict):
        raise CapitalAllocationReturnGoldenContractError("Materialization identity is missing.")
    if materialization.get("canonical_ooxml_hash_contract") != CANONICAL_OOXML_HASH_CONTRACT:
        raise CapitalAllocationReturnGoldenContractError("Canonical OOXML contract changed.")
    if materialization.get("artifact_tool_role") != "READ / INSPECTION / RENDER ONLY":
        raise CapitalAllocationReturnGoldenContractError("artifact-tool authoring role changed.")
    delta_receipt = _verify_delta(fixture_root, materialization)

    workbook = manifest.get("workbook_golden")
    if not isinstance(workbook, dict) or workbook.get("workbook_id") != GOLDEN_WORKBOOK_ID:
        raise CapitalAllocationReturnGoldenContractError("Workbook golden identity is missing.")
    if workbook.get("semantic_hash_contract") != GOLDEN_SEMANTIC_HASH_CONTRACT:
        raise CapitalAllocationReturnGoldenContractError("Semantic hash contract changed.")
    for key in ("raw_sha256", "semantic_sha256", "canonical_ooxml_sha256", "render_sha256"):
        _require_sha256(workbook.get(key), label=f"workbook_golden.{key}")
    acceptance = manifest.get("acceptance")
    if not isinstance(acceptance, dict) or acceptance.get("passed") is not True:
        raise CapitalAllocationReturnGoldenContractError("Golden acceptance is not passed.")
    accepted_path = _resolve_relative(
        fixture_root, acceptance.get("acceptance_fixture"), label="acceptance fixture"
    )
    accepted = load_json_strict(accepted_path)
    findings = accepted.get("product_findings")
    if (
        accepted.get("status") != "PASS"
        or not isinstance(findings, dict)
        or findings.get("p0") != 0
        or findings.get("p1") != 0
        or findings.get("p2") != 0
    ):
        raise CapitalAllocationReturnGoldenContractError("Acceptance fixture is not green.")
    return {
        "delta": delta_receipt,
        "fixture_artifacts": verified_fixtures,
        "golden_id": GOLDEN_ID,
        "implementation_artifacts": verified_implementation,
        "lifecycle": GOLDEN_LIFECYCLE,
        "manifest": manifest,
        "manifest_digest": declared_digest,
        "manifest_path": str(path),
        "passed": True,
        "production_default": GOLDEN_PRODUCTION_DEFAULT,
        "projection": projection_receipt,
    }


def reproduce_registered_golden(
    *,
    predecessor_workbook: Path | str,
    output_workbook: Path | str,
    manifest_path: Path | str = GOLDEN_MANIFEST_PATH,
) -> dict[str, Any]:
    """Replay v2 exactly from the immutable v1 golden and committed delta."""

    verification = verify_golden_manifest(manifest_path)
    manifest = verification["manifest"]
    predecessor = Path(predecessor_workbook)
    output = Path(output_workbook)
    if predecessor.resolve() == output.resolve():
        raise CapitalAllocationReturnGoldenContractError("Predecessor cannot be overwritten.")
    if output.exists():
        raise CapitalAllocationReturnGoldenContractError(f"Refusing to overwrite {output}.")
    if sha256_file(predecessor) != _require_sha256(
        manifest["predecessor"].get("raw_workbook_sha256"),
        label="predecessor raw workbook",
    ):
        raise CapitalAllocationReturnGoldenContractError("Predecessor workbook hash mismatch.")
    delta_path = Path(verification["delta"]["delta_fixture"])
    output.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(predecessor, "r") as source, ZipFile(delta_path, "r") as delta:
        source_names = source.namelist()
        delta_names = delta.namelist()
        new_names = [name for name in delta_names if name not in source_names]
        with ZipFile(output, "w") as target:
            target.comment = source.comment
            for info in source.infolist():
                raw = delta.read(info.filename) if info.filename in delta_names else source.read(info.filename)
                target.writestr(info, raw)
            for name in new_names:
                target.writestr(delta.getinfo(name), delta.read(name))
    workbook = manifest["workbook_golden"]
    raw_hash = sha256_file(output)
    canonical_hash = canonical_ooxml_sha256(output)
    if raw_hash != workbook["raw_sha256"]:
        raise CapitalAllocationReturnGoldenContractError("Replayed workbook raw hash mismatch.")
    if canonical_hash != workbook["canonical_ooxml_sha256"]:
        raise CapitalAllocationReturnGoldenContractError("Replayed canonical OOXML hash mismatch.")
    return {
        "acceptance_status": GOLDEN_ACCEPTANCE_STATUS,
        "binding_plan_digest": manifest["projection"]["binding_plan_digest"],
        "canonical_ooxml_contract": GOLDEN_CANONICAL_OOXML_HASH_CONTRACT,
        "canonical_ooxml_sha256": canonical_hash,
        "delta_fixture_sha256": verification["delta"]["delta_fixture_sha256"],
        "golden_id": GOLDEN_ID,
        "golden_manifest_digest": manifest["manifest_digest"],
        "output_workbook": str(output),
        "output_workbook_sha256": raw_hash,
        "production_default": GOLDEN_PRODUCTION_DEFAULT,
        "reproduced_from_committed_fixtures": True,
        "semantic_hash_contract": workbook["semantic_hash_contract"],
        "semantic_sha256": workbook["semantic_sha256"],
        "workbook_id": GOLDEN_WORKBOOK_ID,
    }


__all__ = [
    "CapitalAllocationReturnGoldenContractError",
    "GOLDEN_ACCEPTANCE_STATUS",
    "GOLDEN_CANONICAL_OOXML_HASH_CONTRACT",
    "GOLDEN_DELTA_CONTRACT",
    "GOLDEN_FIXTURE_HASH_CONTRACT",
    "GOLDEN_FIXTURE_ROOT",
    "GOLDEN_ID",
    "GOLDEN_LIFECYCLE",
    "GOLDEN_MANIFEST_PATH",
    "GOLDEN_MANIFEST_TYPE",
    "GOLDEN_PRODUCTION_DEFAULT",
    "GOLDEN_SEMANTIC_HASH_CONTRACT",
    "GOLDEN_WORKBOOK_ID",
    "canonical_json_bytes",
    "checkout_file_sha256",
    "fixture_bytes",
    "fixture_sha256",
    "load_json_strict",
    "manifest_digest",
    "reproduce_registered_golden",
    "verify_golden_manifest",
]
