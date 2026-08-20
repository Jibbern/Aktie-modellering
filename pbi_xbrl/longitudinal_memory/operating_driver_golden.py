"""Repository-native golden contract for cross-ticker Operating Drivers.

The source-native product is frozen independently from production routing.  Each
accepted workbook is replayed from its protected legacy shell plus a closed,
content-addressed OOXML delta.  The deltas own presentation only: source-native
observations, analytics, semantics, and selection remain owned by the committed
Operating Drivers product layers.  Production workbook routing stays unwired.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Mapping
from zipfile import ZipFile

from pbi_xbrl.longitudinal_memory.operating_driver_anf_full_completeness import (
    build_anf_operating_driver_full_completeness,
)
from pbi_xbrl.longitudinal_memory.operating_driver_anf_ui_v4 import (
    build_operating_driver_anf_ui_source_from_completeness,
    build_operating_driver_anf_ui_v4,
)
from pbi_xbrl.longitudinal_memory.operating_driver_anf_workbook_v4 import (
    WORKBOOK_SEMANTIC_HASH_CONTRACT as ANF_SEMANTIC_HASH_CONTRACT,
    operating_driver_anf_v4_semantic_sha256,
)
from pbi_xbrl.longitudinal_memory.operating_driver_cross_ticker_product import (
    PRODUCT_CONTRACT,
    build_cross_ticker_operating_driver_package,
)
from pbi_xbrl.longitudinal_memory.operating_driver_cross_ticker_profiles import (
    PROFILE_CONTRACT,
    PROFILES,
)
from pbi_xbrl.longitudinal_memory.operating_driver_cross_ticker_workbook import (
    SEMANTIC_HASH_CONTRACT as CROSS_TICKER_SEMANTIC_HASH_CONTRACT,
    cross_ticker_workbook_semantic_sha256,
)
from pbi_xbrl.longitudinal_memory.summary_bs_workbook_materialization import (
    CANONICAL_OOXML_HASH_CONTRACT,
    canonical_ooxml_sha256,
    sha256_file,
)


GOLDEN_MANIFEST_TYPE = "OperatingDriversSourceNativeGoldenManifest@1"
GOLDEN_ID = "operating-drivers-source-native:cross-ticker@1.0.0"
GOLDEN_VERSION = "1.0.0"
GOLDEN_ACCEPTANCE_STATUS = "golden_accepted"
GOLDEN_LIFECYCLE = "target_not_wired"
GOLDEN_PRODUCTION_DEFAULT = False
GOLDEN_FIXTURE_HASH_CONTRACT = "checkout-lf-normalized-file-sha256@1"
GOLDEN_DELTA_CONTRACT = "operating-drivers-ooxml-delta-from-protected-shell@1"
GOLDEN_FIXTURE_ROOT = (
    Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "operating_drivers"
)
GOLDEN_MANIFEST_PATH = GOLDEN_FIXTURE_ROOT / "operating_drivers_golden_manifest.v1.json"

WORKBOOK_IDS = {
    "ANF": "operating-drivers-source-native-workbook:anf@1.0.0",
    "PBI": "operating-drivers-source-native-workbook:pbi@1.0.0",
    "GPRE": "operating-drivers-source-native-workbook:gpre@1.0.0",
}

_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_TOP_LEVEL_KEYS = {
    "acceptance",
    "acceptance_status",
    "checkpoint",
    "fixture_artifacts",
    "fixture_hash_contract",
    "generated_timestamp",
    "golden_id",
    "golden_version",
    "implementation_artifacts",
    "lifecycle",
    "lifecycle_registration",
    "manifest_digest",
    "manifest_type",
    "ownership",
    "product",
    "production_default",
    "quality_gates",
    "workbook_goldens",
}


class OperatingDriverGoldenContractError(ValueError):
    """Raised when the registered Operating Drivers golden drifts."""


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise OperatingDriverGoldenContractError(f"duplicate JSON key: {key}")
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
        raise OperatingDriverGoldenContractError(f"{label} is not a concrete SHA-256.")
    return normalized


def _resolve_relative(root: Path, relative_path: Any, *, label: str) -> Path:
    relative = Path(str(relative_path or ""))
    if relative.is_absolute() or ".." in relative.parts:
        raise OperatingDriverGoldenContractError(f"{label} must be a contained relative path.")
    resolved = (root / relative).resolve()
    if resolved != root.resolve() and root.resolve() not in resolved.parents:
        raise OperatingDriverGoldenContractError(f"{label} escapes its root.")
    return resolved


def _semantic_sha256(ticker: str, workbook: Path | str) -> str:
    if ticker == "ANF":
        return operating_driver_anf_v4_semantic_sha256(workbook)
    return cross_ticker_workbook_semantic_sha256(workbook)


def _verify_product_packages(manifest: Mapping[str, Any]) -> dict[str, Any]:
    product = manifest.get("product")
    if not isinstance(product, dict):
        raise OperatingDriverGoldenContractError("Product identity is missing.")
    if product.get("product_contract") != PRODUCT_CONTRACT:
        raise OperatingDriverGoldenContractError("Cross-ticker product contract changed.")
    if product.get("ticker_profile_contract") != PROFILE_CONTRACT:
        raise OperatingDriverGoldenContractError("Declarative ticker-profile contract changed.")
    expected = product.get("package_identities")
    if not isinstance(expected, dict) or set(expected) != {"ANF", "PBI", "GPRE"}:
        raise OperatingDriverGoldenContractError("Package identity inventory changed.")

    completeness = build_anf_operating_driver_full_completeness()
    lower = {
        "analytics_sha256": completeness.analytics.sha256,
        "registry_sha256": completeness.registry.sha256,
        "selection_sha256": completeness.selection.sha256,
        "semantics_sha256": completeness.semantics.sha256,
    }
    anf_package = build_operating_driver_anf_ui_v4(
        build_operating_driver_anf_ui_source_from_completeness(completeness),
        source_identity_receipts={
            "full_data_completeness_sha256": completeness.sha256,
            **lower,
        },
    )
    actual = {
        "ANF": {
            "package_sha256": anf_package.package_sha256,
            "completeness_sha256": completeness.sha256,
            **lower,
        }
    }
    for ticker in ("PBI", "GPRE"):
        package = build_cross_ticker_operating_driver_package(PROFILES[ticker])
        actual[ticker] = {
            "package_sha256": package.package_sha256,
            "driver_count": len(package.driver_registry),
            "observation_count": len(package.observations),
            "safe_derivation_count": len(package.safe_derivations),
        }
    if actual != expected:
        raise OperatingDriverGoldenContractError("Source-native package identity changed.")
    return actual


def _verify_delta(fixture_root: Path, ticker: str, row: Mapping[str, Any]) -> dict[str, Any]:
    delta = row.get("delta")
    if not isinstance(delta, dict) or delta.get("contract") != GOLDEN_DELTA_CONTRACT:
        raise OperatingDriverGoldenContractError(f"{ticker} delta contract changed.")
    path = _resolve_relative(fixture_root, delta.get("fixture"), label=f"{ticker} delta")
    actual_hash = sha256_file(path)
    if actual_hash != _require_sha256(delta.get("fixture_sha256"), label=f"{ticker} delta"):
        raise OperatingDriverGoldenContractError(f"{ticker} delta hash mismatch.")
    members = delta.get("changed_members")
    if not isinstance(members, list) or not members:
        raise OperatingDriverGoldenContractError(f"{ticker} delta inventory is empty.")
    expected_names: list[str] = []
    with ZipFile(path, "r") as archive:
        if archive.comment:
            raise OperatingDriverGoldenContractError(f"{ticker} delta has a ZIP comment.")
        for member in members:
            if not isinstance(member, dict) or set(member) != {"member", "sha256"}:
                raise OperatingDriverGoldenContractError(f"{ticker} delta row is not closed.")
            name = str(member["member"])
            expected_names.append(name)
            if hashlib.sha256(archive.read(name)).hexdigest() != _require_sha256(
                member["sha256"], label=f"{ticker} delta member {name}"
            ):
                raise OperatingDriverGoldenContractError(f"{ticker} delta member changed: {name}")
        if archive.namelist() != expected_names:
            raise OperatingDriverGoldenContractError(f"{ticker} delta order changed.")
    return {"fixture": str(path), "fixture_sha256": actual_hash, "members": expected_names}


def verify_golden_manifest(
    manifest_path: Path | str = GOLDEN_MANIFEST_PATH,
    *,
    verify_packages: bool = True,
) -> dict[str, Any]:
    """Validate the closed golden manifest and every committed identity."""

    path = Path(manifest_path).resolve()
    manifest = load_json_strict(path)
    if not isinstance(manifest, dict) or set(manifest) != _TOP_LEVEL_KEYS:
        raise OperatingDriverGoldenContractError("Golden top-level keys are not closed.")
    expected_scalars = {
        "manifest_type": GOLDEN_MANIFEST_TYPE,
        "golden_id": GOLDEN_ID,
        "golden_version": GOLDEN_VERSION,
        "acceptance_status": GOLDEN_ACCEPTANCE_STATUS,
        "lifecycle": GOLDEN_LIFECYCLE,
        "fixture_hash_contract": GOLDEN_FIXTURE_HASH_CONTRACT,
    }
    if any(manifest.get(key) != value for key, value in expected_scalars.items()):
        raise OperatingDriverGoldenContractError("Golden identity or lifecycle changed.")
    if manifest.get("production_default") is not GOLDEN_PRODUCTION_DEFAULT:
        raise OperatingDriverGoldenContractError("Golden cannot be a production default.")
    if manifest.get("generated_timestamp") is not None:
        raise OperatingDriverGoldenContractError("Deterministic manifest has a timestamp.")
    declared_digest = _require_sha256(manifest.get("manifest_digest"), label="manifest_digest")
    if manifest_digest(manifest) != declared_digest:
        raise OperatingDriverGoldenContractError("Golden manifest digest mismatch.")

    fixture_root = path.parent
    fixture_rows = manifest.get("fixture_artifacts")
    if not isinstance(fixture_rows, list) or not fixture_rows:
        raise OperatingDriverGoldenContractError("Golden fixture inventory is missing.")
    verified_fixtures: list[dict[str, Any]] = []
    for row in fixture_rows:
        if not isinstance(row, dict) or set(row) != {"relative_path", "sha256"}:
            raise OperatingDriverGoldenContractError("Fixture row is not closed.")
        artifact = _resolve_relative(fixture_root, row["relative_path"], label="fixture")
        actual = fixture_sha256(artifact)
        if actual != _require_sha256(row["sha256"], label=f"fixture {row['relative_path']}"):
            raise OperatingDriverGoldenContractError(f"Fixture changed: {row['relative_path']}")
        verified_fixtures.append({"relative_path": row["relative_path"], "sha256": actual})

    repository_root = Path(__file__).resolve().parents[2]
    implementation_rows = manifest.get("implementation_artifacts")
    if not isinstance(implementation_rows, list) or not implementation_rows:
        raise OperatingDriverGoldenContractError("Implementation identities are missing.")
    verified_implementation: list[dict[str, Any]] = []
    for row in implementation_rows:
        if not isinstance(row, dict) or set(row) != {"repository_path", "sha256"}:
            raise OperatingDriverGoldenContractError("Implementation row is not closed.")
        artifact = _resolve_relative(repository_root, row["repository_path"], label="implementation")
        actual = checkout_file_sha256(artifact)
        if actual != _require_sha256(row["sha256"], label=f"implementation {row['repository_path']}"):
            raise OperatingDriverGoldenContractError(f"Implementation changed: {row['repository_path']}")
        verified_implementation.append({"repository_path": row["repository_path"], "sha256": actual})

    ownership = manifest.get("ownership")
    if not isinstance(ownership, dict) or ownership.get("duplicate_economic_owner_count") != 0:
        raise OperatingDriverGoldenContractError("Ownership reconciliation is not closed.")
    quality = manifest.get("quality_gates")
    if not isinstance(quality, dict) or any(value != 0 for value in quality.values()):
        raise OperatingDriverGoldenContractError("A golden quality gate is nonzero.")
    lifecycle = manifest.get("lifecycle_registration")
    if not isinstance(lifecycle, dict):
        raise OperatingDriverGoldenContractError("Lifecycle registration is missing.")
    if lifecycle.get("product_state") != "golden_accepted":
        raise OperatingDriverGoldenContractError("Product lifecycle changed.")
    if lifecycle.get("workbook_bridge") != GOLDEN_LIFECYCLE:
        raise OperatingDriverGoldenContractError("Workbook bridge lifecycle changed.")

    workbook_rows = manifest.get("workbook_goldens")
    if not isinstance(workbook_rows, list) or len(workbook_rows) != 3:
        raise OperatingDriverGoldenContractError("Three workbook goldens are required.")
    workbook_by_ticker: dict[str, dict[str, Any]] = {}
    delta_receipts: dict[str, dict[str, Any]] = {}
    for workbook in workbook_rows:
        ticker = str(workbook.get("ticker") or "")
        if ticker in workbook_by_ticker or ticker not in WORKBOOK_IDS:
            raise OperatingDriverGoldenContractError("Workbook ticker inventory changed.")
        if workbook.get("workbook_id") != WORKBOOK_IDS[ticker]:
            raise OperatingDriverGoldenContractError(f"{ticker} workbook ID changed.")
        expected_semantic_contract = (
            ANF_SEMANTIC_HASH_CONTRACT if ticker == "ANF" else CROSS_TICKER_SEMANTIC_HASH_CONTRACT
        )
        if workbook.get("semantic_hash_contract") != expected_semantic_contract:
            raise OperatingDriverGoldenContractError(f"{ticker} semantic hash contract changed.")
        if workbook.get("canonical_ooxml_hash_contract") != CANONICAL_OOXML_HASH_CONTRACT:
            raise OperatingDriverGoldenContractError(f"{ticker} canonical hash contract changed.")
        for key in (
            "base_workbook_sha256",
            "raw_sha256",
            "semantic_sha256",
            "canonical_ooxml_sha256",
            "render_sha256",
            "package_sha256",
        ):
            _require_sha256(workbook.get(key), label=f"{ticker}.{key}")
        if ticker == "GPRE":
            _require_sha256(workbook.get("vba_sha256"), label="GPRE.vba_sha256")
        elif workbook.get("vba_sha256") is not None:
            raise OperatingDriverGoldenContractError(f"{ticker} unexpectedly declares VBA.")
        delta_receipts[ticker] = _verify_delta(fixture_root, ticker, workbook)
        workbook_by_ticker[ticker] = dict(workbook)
    if set(workbook_by_ticker) != set(WORKBOOK_IDS):
        raise OperatingDriverGoldenContractError("Workbook ticker set changed.")

    acceptance = manifest.get("acceptance")
    if not isinstance(acceptance, dict) or acceptance.get("passed") is not True:
        raise OperatingDriverGoldenContractError("Golden acceptance is not passed.")
    acceptance_path = _resolve_relative(
        fixture_root, acceptance.get("acceptance_fixture"), label="acceptance fixture"
    )
    accepted = load_json_strict(acceptance_path)
    if accepted.get("status") != "PASS" or any(accepted.get(key) != 0 for key in ("p0", "p1", "p2")):
        raise OperatingDriverGoldenContractError("Acceptance fixture is not green.")

    packages = _verify_product_packages(manifest) if verify_packages else manifest["product"]["package_identities"]
    return {
        "acceptance_status": GOLDEN_ACCEPTANCE_STATUS,
        "delta_receipts": delta_receipts,
        "fixture_artifacts": verified_fixtures,
        "golden_id": GOLDEN_ID,
        "implementation_artifacts": verified_implementation,
        "lifecycle": GOLDEN_LIFECYCLE,
        "manifest": manifest,
        "manifest_digest": declared_digest,
        "manifest_path": str(path),
        "packages": packages,
        "passed": True,
        "production_default": GOLDEN_PRODUCTION_DEFAULT,
        "workbooks": workbook_by_ticker,
    }


def reproduce_registered_golden(
    *,
    ticker: str,
    base_workbook: Path | str,
    output_workbook: Path | str,
    manifest_path: Path | str = GOLDEN_MANIFEST_PATH,
) -> dict[str, Any]:
    """Replay one accepted workbook exactly from its protected shell and delta."""

    ticker = ticker.upper()
    verification = verify_golden_manifest(manifest_path, verify_packages=False)
    if ticker not in verification["workbooks"]:
        raise OperatingDriverGoldenContractError(f"Unsupported golden ticker: {ticker}")
    workbook = verification["workbooks"][ticker]
    source_path = Path(base_workbook)
    output_path = Path(output_workbook)
    if source_path.resolve() == output_path.resolve():
        raise OperatingDriverGoldenContractError("Protected shell cannot be overwritten.")
    if output_path.exists():
        raise OperatingDriverGoldenContractError(f"Refusing to overwrite {output_path}.")
    if sha256_file(source_path) != workbook["base_workbook_sha256"]:
        raise OperatingDriverGoldenContractError(f"{ticker} protected shell hash mismatch.")
    delta_path = Path(verification["delta_receipts"][ticker]["fixture"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(source_path, "r") as source, ZipFile(delta_path, "r") as delta:
        source_names = source.namelist()
        delta_names = delta.namelist()
        with ZipFile(output_path, "w") as target:
            target.comment = source.comment
            for info in source.infolist():
                raw = delta.read(info.filename) if info.filename in delta_names else source.read(info.filename)
                target.writestr(info, raw)
            for name in (name for name in delta_names if name not in source_names):
                target.writestr(delta.getinfo(name), delta.read(name))

    raw_hash = sha256_file(output_path)
    semantic_hash = _semantic_sha256(ticker, output_path)
    canonical_hash = canonical_ooxml_sha256(output_path)
    if raw_hash != workbook["raw_sha256"]:
        raise OperatingDriverGoldenContractError(f"{ticker} replay raw hash mismatch.")
    if semantic_hash != workbook["semantic_sha256"]:
        raise OperatingDriverGoldenContractError(f"{ticker} replay semantic hash mismatch.")
    if canonical_hash != workbook["canonical_ooxml_sha256"]:
        raise OperatingDriverGoldenContractError(f"{ticker} replay canonical hash mismatch.")
    vba_hash: str | None = None
    with ZipFile(output_path, "r") as archive:
        if "xl/vbaProject.bin" in archive.namelist():
            vba_hash = hashlib.sha256(archive.read("xl/vbaProject.bin")).hexdigest()
    if vba_hash != workbook.get("vba_sha256"):
        raise OperatingDriverGoldenContractError(f"{ticker} VBA identity changed.")
    return {
        "acceptance_status": GOLDEN_ACCEPTANCE_STATUS,
        "canonical_ooxml_sha256": canonical_hash,
        "golden_id": GOLDEN_ID,
        "manifest_digest": verification["manifest_digest"],
        "output_workbook": str(output_path),
        "output_workbook_sha256": raw_hash,
        "production_default": GOLDEN_PRODUCTION_DEFAULT,
        "reproduced_from_committed_fixtures": True,
        "semantic_sha256": semantic_hash,
        "ticker": ticker,
        "vba_sha256": vba_hash,
        "workbook_id": WORKBOOK_IDS[ticker],
    }


__all__ = [
    "GOLDEN_ACCEPTANCE_STATUS",
    "GOLDEN_DELTA_CONTRACT",
    "GOLDEN_FIXTURE_HASH_CONTRACT",
    "GOLDEN_FIXTURE_ROOT",
    "GOLDEN_ID",
    "GOLDEN_LIFECYCLE",
    "GOLDEN_MANIFEST_PATH",
    "GOLDEN_MANIFEST_TYPE",
    "GOLDEN_PRODUCTION_DEFAULT",
    "GOLDEN_VERSION",
    "OperatingDriverGoldenContractError",
    "WORKBOOK_IDS",
    "canonical_json_bytes",
    "checkout_file_sha256",
    "fixture_bytes",
    "fixture_sha256",
    "load_json_strict",
    "manifest_digest",
    "reproduce_registered_golden",
    "verify_golden_manifest",
]
