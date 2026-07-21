"""Canonical workbook promotion and workbook-specific rollback orchestration."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import uuid
from typing import Any, Mapping

from pbi_xbrl.json_schema_validation import load_json_strict
from pbi_xbrl.new_engine_orchestration import validate_workbook_immutable
from pbi_xbrl.new_engine_transaction import (
    NewEngineTransactionError,
    candidate_path_for,
    normalize_candidate_acl,
    replace_existing_atomic,
)


ROOT = Path(__file__).resolve().parents[1]
ROLLBACK_RECORD_VERSION = "new-engine-workbook-rollback/v1"
PROMOTION_RECEIPT_VERSION = "new-engine-promotion/v1"
ROLLBACK_RECEIPT_VERSION = "new-engine-rollback/v1"
_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
_GIT_HEAD_RE = re.compile(r"^[0-9a-fA-F]{40}$")


class NewEnginePromotionError(RuntimeError):
    """Raised when promotion or rollback cannot complete fail closed."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _operation_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{uuid.uuid4().hex[:12]}"


def _write_json_no_overwrite(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, indent=2)
            handle.write("\n")
    except FileExistsError as exc:
        raise NewEnginePromotionError(f"Artifact already exists: {path}") from exc


def _repository_state() -> dict[str, Any]:
    try:
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
            shell=False,
        ).stdout.strip()
        status = subprocess.run(
            ["git", "status", "--porcelain=v1", "--untracked-files=all"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
            shell=False,
        ).stdout.splitlines()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise NewEnginePromotionError(f"Could not verify repository state: {exc}") from exc
    return {"head": head, "clean": not status, "changes": status}


def _verify_release_gate(*, expected_head: str, product_approval_reference: str) -> dict[str, Any]:
    expected = str(expected_head or "").strip().lower()
    if not _GIT_HEAD_RE.fullmatch(expected):
        raise NewEnginePromotionError("expected HEAD must be one exact 40-character Git commit hash.")
    approval = str(product_approval_reference or "").strip()
    if not approval:
        raise NewEnginePromotionError("A non-empty product approval reference is required.")
    state = _repository_state()
    actual = str(state.get("head") or "").strip().lower()
    if actual != expected:
        raise NewEnginePromotionError(f"Repository HEAD {actual!r} differs from expected HEAD {expected!r}.")
    if not bool(state.get("clean")):
        raise NewEnginePromotionError(f"Repository is not clean: {list(state.get('changes') or [])!r}")
    return {"head": actual, "clean": True, "product_approval_reference": approval}


def _verify_canonical_target_identity(*, canonical: Path, ticker: str) -> str:
    ticker_id = str(ticker or "").strip().upper()
    if not ticker_id:
        raise NewEnginePromotionError("A non-empty ticker is required for canonical promotion.")
    expected_name = f"{ticker_id}_model.xlsx"
    if canonical.name.casefold() != expected_name.casefold():
        raise NewEnginePromotionError(
            f"Canonical workbook {canonical.name!r} does not match ticker {ticker_id!r}; "
            f"expected {expected_name!r}."
        )
    return ticker_id


def _status_at(payload: Mapping[str, Any], *path: str) -> str:
    current: Any = payload
    for part in path:
        if not isinstance(current, Mapping):
            return ""
        current = current.get(part)
    return str(current or "")


def _load_mapping(path: Path, label: str) -> Mapping[str, Any]:
    if not path.is_file():
        raise NewEnginePromotionError(f"{label} does not exist: {path}")
    try:
        payload = load_json_strict(path)
    except Exception as exc:
        raise NewEnginePromotionError(f"Could not read {label} {path}: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise NewEnginePromotionError(f"{label} must contain one JSON object: {path}")
    return payload


def _load_hash_bound_mapping(
    path: Path,
    label: str,
    *,
    expected_sha256: str,
) -> tuple[Mapping[str, Any], str]:
    expected = str(expected_sha256 or "").strip().lower()
    if not _SHA256_RE.fullmatch(expected):
        raise NewEnginePromotionError(f"expected {label} SHA-256 must be exactly 64 hexadecimal characters.")
    before = _sha256(path) if path.is_file() else ""
    if before != expected:
        raise NewEnginePromotionError(f"Actual {label} SHA-256 differs from the expected digest.")
    payload = _load_mapping(path, label)
    after = _sha256(path)
    if after != expected:
        raise NewEnginePromotionError(f"{label} changed while it was being strictly reloaded.")
    return payload, expected


def _exact_record_size(value: Any, *, label: str) -> int:
    try:
        size = int(value)
    except (TypeError, ValueError) as exc:
        raise NewEnginePromotionError(f"{label} size is missing or invalid.") from exc
    if size < 0:
        raise NewEnginePromotionError(f"{label} size is missing or invalid.")
    return size


def _verify_rollback_artifacts(
    *,
    record_path: Path,
    expected_record_sha256: str,
    canonical: Path,
    rollback_workbook: Path | None = None,
    shadow: Path | None = None,
    expected_previous_sha256: str | None = None,
    expected_promoted_sha256: str | None = None,
) -> dict[str, Any]:
    """Strictly reload and hash-bind one durable workbook rollback pair."""

    record, record_hash = _load_hash_bound_mapping(
        record_path,
        "rollback record",
        expected_sha256=expected_record_sha256,
    )
    if str(record.get("record_version") or "") != ROLLBACK_RECORD_VERSION:
        raise NewEnginePromotionError("Unsupported rollback record version.")
    canonical_record = record.get("canonical") if isinstance(record.get("canonical"), Mapping) else {}
    promoted_record = record.get("promoted") if isinstance(record.get("promoted"), Mapping) else {}
    rollback_record = record.get("rollback") if isinstance(record.get("rollback"), Mapping) else {}
    if Path(str(canonical_record.get("path") or "")).resolve() != canonical:
        raise NewEnginePromotionError("Rollback record canonical path does not match the requested workbook.")

    recorded_rollback = Path(str(rollback_record.get("path") or "")).resolve()
    if recorded_rollback == canonical:
        raise NewEnginePromotionError("Rollback workbook must be distinct from the canonical workbook.")
    if rollback_workbook is not None and recorded_rollback != rollback_workbook.resolve():
        raise NewEnginePromotionError("Rollback workbook path differs from the immutable rollback record.")
    if not recorded_rollback.is_file():
        raise NewEnginePromotionError(f"Recorded rollback workbook does not exist: {recorded_rollback}")
    rollback_hash = _sha256(recorded_rollback)
    recorded_rollback_hash = str(rollback_record.get("sha256") or "").lower()
    previous_hash = str(canonical_record.get("sha256") or "").lower()
    if rollback_hash != recorded_rollback_hash:
        raise NewEnginePromotionError("Recorded rollback workbook SHA-256 does not match its record.")
    if rollback_hash != previous_hash:
        raise NewEnginePromotionError("Rollback and previous canonical SHA-256 identities disagree.")
    if recorded_rollback.stat().st_size != _exact_record_size(
        rollback_record.get("size"), label="rollback workbook"
    ):
        raise NewEnginePromotionError("Recorded rollback workbook size does not match its record.")
    if recorded_rollback.stat().st_size != _exact_record_size(
        canonical_record.get("size"), label="previous canonical"
    ):
        raise NewEnginePromotionError("Rollback and previous canonical size identities disagree.")
    if expected_previous_sha256 is not None and rollback_hash != str(expected_previous_sha256).lower():
        raise NewEnginePromotionError("Rollback workbook differs from the expected previous canonical bytes.")

    promoted_hash = str(promoted_record.get("sha256") or "").lower()
    if not _SHA256_RE.fullmatch(promoted_hash):
        raise NewEnginePromotionError("Rollback record promoted SHA-256 is invalid.")
    promoted_size = _exact_record_size(promoted_record.get("size"), label="promoted shadow")
    if expected_promoted_sha256 is not None and promoted_hash != str(expected_promoted_sha256).lower():
        raise NewEnginePromotionError("Rollback record promoted SHA-256 differs from the accepted shadow.")
    if shadow is not None:
        if Path(str(promoted_record.get("shadow_path") or "")).resolve() != shadow.resolve():
            raise NewEnginePromotionError("Rollback record shadow path differs from the accepted shadow.")
        if not shadow.is_file() or _sha256(shadow) != promoted_hash:
            raise NewEnginePromotionError("Accepted shadow bytes differ from the immutable rollback record.")
        if shadow.stat().st_size != promoted_size:
            raise NewEnginePromotionError("Accepted shadow size differs from the immutable rollback record.")

    if _sha256(record_path) != record_hash or _sha256(recorded_rollback) != rollback_hash:
        raise NewEnginePromotionError("Rollback artifacts changed during identity verification.")
    return {
        "status": "PASS",
        "record": record,
        "record_sha256": record_hash,
        "rollback_workbook": recorded_rollback,
        "rollback_sha256": rollback_hash,
        "promoted_sha256": promoted_hash,
        "promoted_size": promoted_size,
    }


def _required_excel_locale(required_locale_id: int | None) -> int:
    if type(required_locale_id) is not int or required_locale_id <= 0:
        raise NewEnginePromotionError("A positive required Excel locale ID is required for release validation.")
    return required_locale_id


def _release_validation_context(
    *,
    plan_receipt_path: Path | str,
    required_locale_id: int,
    kwargs: Mapping[str, Any],
) -> dict[str, Any]:
    package = Path(str(kwargs.get("package_path") or "")).resolve()
    plan_receipt = Path(plan_receipt_path).resolve()
    for path, label in ((package, "normalized package"), (plan_receipt, "plan receipt")):
        if not path.is_file():
            raise NewEnginePromotionError(f"Required rollback {label} does not exist: {path}")
    return {
        "ticker": str(kwargs.get("ticker") or "").strip().upper(),
        "profile_id": str(kwargs.get("profile_id") or "").strip(),
        "required_locale_id": required_locale_id,
        "package": {"path": str(package), "size": package.stat().st_size, "sha256": _sha256(package)},
        "plan_receipt": {
            "path": str(plan_receipt),
            "size": plan_receipt.stat().st_size,
            "sha256": _sha256(plan_receipt),
        },
    }


def _verify_shadow_receipt(
    *,
    receipt_path: Path,
    shadow: Path,
    expected_head: str,
    expected_sha256: str,
    ticker: str,
    profile_id: str,
) -> dict[str, Any]:
    expected_hash = str(expected_sha256 or "").strip().lower()
    if not _SHA256_RE.fullmatch(expected_hash):
        raise NewEnginePromotionError("expected shadow SHA-256 must be exactly 64 hexadecimal characters.")
    if not shadow.is_file() or shadow.suffix.lower() != ".xlsx":
        raise NewEnginePromotionError(f"Accepted shadow workbook does not exist as .xlsx: {shadow}")
    actual_hash = _sha256(shadow)
    if actual_hash != expected_hash:
        raise NewEnginePromotionError(
            f"Actual shadow SHA-256 {actual_hash} differs from expected shadow SHA-256 {expected_hash}."
        )
    receipt = _load_mapping(receipt_path, "shadow receipt")
    checks = {
        "receipt_version": str(receipt.get("receipt_version") or "") == "new-engine-run/v1",
        "command": str(receipt.get("command") or "") == "render-shadow",
        "status": str(receipt.get("status") or "") == "PASS",
        "repo_head": str(receipt.get("repo_head") or "").lower() == expected_head.lower(),
        "ticker": _status_at(receipt, "contract_profile", "ticker").upper() == ticker.upper(),
        "profile_id": _status_at(receipt, "contract_profile", "profile_id") == profile_id,
    }
    output = receipt.get("output") if isinstance(receipt.get("output"), Mapping) else {}
    try:
        output_size: int | None = int(output.get("size"))
    except (TypeError, ValueError):
        output_size = None
    checks.update(
        {
            "output.path": Path(str(output.get("path") or "")).resolve() == shadow,
            "output.sha256": str(output.get("sha256") or "").lower() == actual_hash,
            "output.size": output_size == shadow.stat().st_size,
        }
    )
    for name in (
        "pre_fill",
        "post_fill",
        "saved_workbook",
        "excel_native",
        "acl",
        "formula_semantics",
    ):
        checks[f"validations.{name}"] = _status_at(receipt, "validations", name, "status") == "PASS"
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise NewEnginePromotionError(
            "Shadow receipt is not accepted for canonical promotion: " + ", ".join(failed)
        )
    return {
        "path": str(receipt_path),
        "sha256": _sha256(receipt_path),
        "shadow_sha256": actual_hash,
        "checks": checks,
    }


def _validate_release_workbook(
    workbook: Path,
    *,
    plan_receipt_path: Path | str,
    required_locale_id: int | None,
    kwargs: Mapping[str, Any],
) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="new-engine-release-validation-") as temp_dir:
        result = validate_workbook_immutable(
            workbook_path=workbook,
            run_dir=Path(temp_dir) / "run",
            plan_receipt_path=plan_receipt_path,
            excel_native="required",
            required_locale_id=required_locale_id,
            **kwargs,
        )
    receipt = result.get("receipt") if isinstance(result.get("receipt"), Mapping) else {}
    validations = receipt.get("validations") if isinstance(receipt.get("validations"), Mapping) else {}
    required = (
        "post_fill",
        "saved_workbook",
        "excel_native",
        "excel_post_fill",
        "excel_saved_workbook",
    )
    failed = [name for name in required if _status_at(validations, name, "status") != "PASS"]
    output = receipt.get("output") if isinstance(receipt.get("output"), Mapping) else {}
    actual_hash = _sha256(workbook)
    if str(result.get("status") or "") != "PASS" or failed:
        raise NewEnginePromotionError(f"Required release validation failed for {workbook}: {failed!r}")
    if str(output.get("sha256") or "").lower() != actual_hash or output.get("immutable_input") is not True:
        raise NewEnginePromotionError("Release validation did not prove immutable workbook byte identity.")
    return {
        "status": "PASS",
        "workbook_sha256": actual_hash,
        "validation_statuses": {name: "PASS" for name in required},
    }


def _copy_exact(source: Path, destination: Path) -> str:
    try:
        with source.open("rb") as reader, destination.open("xb") as writer:
            shutil.copyfileobj(reader, writer, length=1024 * 1024)
    except FileExistsError as exc:
        raise NewEnginePromotionError(f"Artifact already exists: {destination}") from exc
    copied_hash = _sha256(destination)
    source_hash = _sha256(source)
    if copied_hash != source_hash:
        destination.unlink(missing_ok=True)
        raise NewEnginePromotionError("Byte-exact workbook copy verification failed.")
    return copied_hash


def _create_rollback_record(
    *,
    canonical: Path,
    shadow: Path,
    shadow_receipt: Mapping[str, Any],
    rollback_dir: Path,
    gate: Mapping[str, Any],
    operation_id: str,
) -> tuple[Path, Path]:
    rollback_dir.mkdir(parents=True, exist_ok=True)
    rollback_workbook = rollback_dir / f"{canonical.stem}.{operation_id}.rollback.xlsx"
    rollback_record = rollback_dir / f"{canonical.stem}.{operation_id}.rollback.json"
    old_hash = _copy_exact(canonical, rollback_workbook)
    record = {
        "record_version": ROLLBACK_RECORD_VERSION,
        "operation_id": operation_id,
        "created_at_utc": _utc_now(),
        "repo_head": gate["head"],
        "product_approval_reference": gate["product_approval_reference"],
        "canonical": {
            "path": str(canonical),
            "sha256": old_hash,
            "size": canonical.stat().st_size,
        },
        "promoted": {
            "shadow_path": str(shadow),
            "sha256": _sha256(shadow),
            "size": shadow.stat().st_size,
        },
        "rollback": {
            "path": str(rollback_workbook),
            "sha256": old_hash,
            "size": rollback_workbook.stat().st_size,
        },
        "shadow_receipt": dict(shadow_receipt),
    }
    try:
        _write_json_no_overwrite(rollback_record, record)
    except Exception:
        rollback_workbook.unlink(missing_ok=True)
        if rollback_dir.exists() and not any(rollback_dir.iterdir()):
            rollback_dir.rmdir()
        raise
    return rollback_workbook, rollback_record


def _restore_after_failed_promotion(
    *,
    canonical: Path,
    recovery_candidate: Path,
    expected_hash: str,
    rollback_workbook: Path,
    rollback_record_path: Path,
    rollback_record_sha256: str,
    expected_promoted_hash: str,
) -> None:
    durable_error: Exception | None = None
    try:
        durable_proof = _verify_rollback_artifacts(
            record_path=rollback_record_path,
            expected_record_sha256=rollback_record_sha256,
            canonical=canonical,
            rollback_workbook=rollback_workbook,
            expected_previous_sha256=expected_hash,
            expected_promoted_sha256=expected_promoted_hash,
        )
    except Exception as exc:
        durable_proof = None
        durable_error = exc

    fallback_candidate: Path | None = None
    if recovery_candidate.is_file() and _sha256(recovery_candidate) == expected_hash:
        restore_candidate = recovery_candidate
    elif durable_proof is not None:
        fallback_candidate = candidate_path_for(canonical)
        _copy_exact(Path(durable_proof["rollback_workbook"]), fallback_candidate)
        restore_candidate = fallback_candidate
    else:
        raise NewEnginePromotionError(
            "Neither the prepared recovery candidate nor the durable rollback artifacts passed verification: "
            f"{durable_error}"
        )

    try:
        if _sha256(restore_candidate) != expected_hash:
            raise NewEnginePromotionError("Promotion recovery bytes failed verification before restoration.")
        normalize_candidate_acl(restore_candidate)
        if _sha256(restore_candidate) != expected_hash:
            raise NewEnginePromotionError("ACL normalization changed the promotion recovery bytes.")
        replace_existing_atomic(restore_candidate, canonical)
        if _sha256(canonical) != expected_hash:
            raise NewEnginePromotionError("Automatic restore produced the wrong canonical SHA-256.")
    finally:
        if fallback_candidate is not None:
            fallback_candidate.unlink(missing_ok=True)


def promote_workbook(
    *,
    run_dir: Path | str,
    shadow_workbook: Path | str,
    shadow_receipt_path: Path | str,
    canonical_workbook: Path | str,
    rollback_dir: Path | str,
    product_approval_reference: str,
    expected_head: str,
    expected_shadow_sha256: str,
    plan_receipt_path: Path | str,
    execute: bool = False,
    required_locale_id: int | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Dry-run or execute one validated canonical workbook promotion."""

    release_locale = _required_excel_locale(required_locale_id)
    gate = _verify_release_gate(
        expected_head=expected_head,
        product_approval_reference=product_approval_reference,
    )
    shadow = Path(shadow_workbook).resolve()
    canonical = Path(canonical_workbook).resolve()
    if not canonical.is_file() or canonical.suffix.lower() != ".xlsx":
        raise NewEnginePromotionError(f"Existing canonical .xlsx workbook is required: {canonical}")
    if shadow == canonical:
        raise NewEnginePromotionError("Shadow and canonical workbook paths must be different.")
    ticker = _verify_canonical_target_identity(
        canonical=canonical,
        ticker=str(kwargs.get("ticker") or ""),
    )
    profile_id = str(kwargs.get("profile_id") or "").strip()
    receipt_gate = _verify_shadow_receipt(
        receipt_path=Path(shadow_receipt_path).resolve(),
        shadow=shadow,
        expected_head=gate["head"],
        expected_sha256=expected_shadow_sha256,
        ticker=ticker,
        profile_id=profile_id,
    )
    preflight = _validate_release_workbook(
        shadow,
        plan_receipt_path=plan_receipt_path,
        required_locale_id=release_locale,
        kwargs=kwargs,
    )
    if not execute:
        return {
            "status": "PASS",
            "mode": "dry-run",
            "canonical_path": canonical,
            "shadow_path": shadow,
            "shadow_sha256": receipt_gate["shadow_sha256"],
            "validation": preflight,
            "canonical_unchanged": True,
        }

    _verify_release_gate(
        expected_head=expected_head,
        product_approval_reference=product_approval_reference,
    )
    run_path = Path(run_dir).resolve()
    promotion_receipt_path = run_path / "promotion_receipt.json"
    if promotion_receipt_path.exists():
        raise NewEnginePromotionError(f"Artifact already exists: {promotion_receipt_path}")
    operation_id = _operation_id()
    rollback_workbook, rollback_record_path = _create_rollback_record(
        canonical=canonical,
        shadow=shadow,
        shadow_receipt=receipt_gate,
        rollback_dir=Path(rollback_dir).resolve(),
        gate=gate,
        operation_id=operation_id,
    )
    expected_new_hash = str(receipt_gate["shadow_sha256"])
    initial_record_hash = _sha256(rollback_record_path)
    rollback_proof = _verify_rollback_artifacts(
        record_path=rollback_record_path,
        expected_record_sha256=initial_record_hash,
        canonical=canonical,
        rollback_workbook=rollback_workbook,
        shadow=shadow,
        expected_promoted_sha256=expected_new_hash,
    )
    old_hash = str(rollback_proof["rollback_sha256"])
    candidate = candidate_path_for(canonical)
    recovery_candidate = candidate_path_for(canonical)
    replaced = False
    completed = False
    try:
        _copy_exact(shadow, candidate)
        acl = normalize_candidate_acl(candidate)
        if _sha256(candidate) != expected_new_hash:
            raise NewEnginePromotionError("ACL-normalized candidate differs from the accepted shadow bytes.")
        staged_validation = _validate_release_workbook(
            candidate,
            plan_receipt_path=plan_receipt_path,
            required_locale_id=release_locale,
            kwargs=kwargs,
        )
        _copy_exact(canonical, recovery_candidate)
        recovery_acl = normalize_candidate_acl(recovery_candidate)
        if _sha256(recovery_candidate) != old_hash:
            raise NewEnginePromotionError(
                "Canonical workbook changed before promotion replacement while staging recovery bytes."
            )
        _verify_release_gate(
            expected_head=expected_head,
            product_approval_reference=product_approval_reference,
        )
        if _sha256(canonical) != old_hash:
            raise NewEnginePromotionError("Canonical workbook changed before promotion replacement.")
        pre_replacement_proof = _verify_rollback_artifacts(
            record_path=rollback_record_path,
            expected_record_sha256=initial_record_hash,
            canonical=canonical,
            rollback_workbook=rollback_workbook,
            shadow=shadow,
            expected_previous_sha256=old_hash,
            expected_promoted_sha256=expected_new_hash,
        )
        if _sha256(candidate) != expected_new_hash or _sha256(recovery_candidate) != old_hash:
            raise NewEnginePromotionError("Promotion candidates changed before canonical replacement.")
        replace_existing_atomic(candidate, canonical)
        replaced = True
        if _sha256(canonical) != expected_new_hash:
            raise NewEnginePromotionError("Promoted canonical SHA-256 differs from the accepted shadow.")
        canonical_validation = _validate_release_workbook(
            canonical,
            plan_receipt_path=plan_receipt_path,
            required_locale_id=release_locale,
            kwargs=kwargs,
        )
        receipt = {
            "receipt_version": PROMOTION_RECEIPT_VERSION,
            "status": "PASS",
            "mode": "execute",
            "operation_id": operation_id,
            "completed_at_utc": _utc_now(),
            "repo_head": gate["head"],
            "product_approval_reference": gate["product_approval_reference"],
            "canonical": {"path": str(canonical), "old_sha256": old_hash, "new_sha256": expected_new_hash},
            "shadow": dict(receipt_gate),
            "rollback": {
                "workbook_path": str(rollback_workbook),
                "record_path": str(rollback_record_path),
                "record_sha256": str(pre_replacement_proof["record_sha256"]),
                "sha256": old_hash,
            },
            "validations": {
                "shadow": preflight,
                "staged_candidate": staged_validation,
                "canonical": canonical_validation,
                "acl": acl,
                "recovery_acl": recovery_acl,
                "pre_replacement_rollback_artifacts": {
                    "status": "PASS",
                    "record_sha256": str(pre_replacement_proof["record_sha256"]),
                    "rollback_sha256": str(pre_replacement_proof["rollback_sha256"]),
                },
            },
        }
        _write_json_no_overwrite(promotion_receipt_path, receipt)
        completed = True
        return {
            "status": "PASS",
            "mode": "execute",
            "canonical_path": canonical,
            "canonical_sha256": expected_new_hash,
            "rollback_workbook": rollback_workbook,
            "rollback_record": rollback_record_path,
            "rollback_record_sha256": str(pre_replacement_proof["record_sha256"]),
            "receipt_path": promotion_receipt_path,
            "receipt": receipt,
        }
    except Exception as exc:
        if replaced:
            try:
                _restore_after_failed_promotion(
                    canonical=canonical,
                    recovery_candidate=recovery_candidate,
                    expected_hash=old_hash,
                    rollback_workbook=rollback_workbook,
                    rollback_record_path=rollback_record_path,
                    rollback_record_sha256=initial_record_hash,
                    expected_promoted_hash=expected_new_hash,
                )
            except Exception as restore_exc:
                raise NewEnginePromotionError(
                    f"Promotion failed and automatic canonical restore also failed: {exc}; restore={restore_exc}"
                ) from restore_exc
            raise NewEnginePromotionError(
                f"Promotion failed after replacement; the previous canonical workbook was restored: {exc}"
            ) from exc
        if isinstance(exc, NewEnginePromotionError):
            raise
        if isinstance(exc, NewEngineTransactionError):
            raise NewEnginePromotionError(str(exc)) from exc
        raise
    finally:
        if not completed:
            promotion_receipt_path.unlink(missing_ok=True)
            if run_path.exists() and not any(run_path.iterdir()):
                run_path.rmdir()
        candidate.unlink(missing_ok=True)
        recovery_candidate.unlink(missing_ok=True)


def rollback_workbook(
    *,
    run_dir: Path | str,
    canonical_workbook: Path | str,
    rollback_record_path: Path | str,
    expected_rollback_record_sha256: str,
    product_approval_reference: str,
    expected_head: str,
    plan_receipt_path: Path | str,
    required_locale_id: int | None,
    execute: bool = False,
    **kwargs: Any,
) -> dict[str, Any]:
    """Dry-run or execute one validated, byte-exact workbook-specific rollback."""

    release_locale = _required_excel_locale(required_locale_id)
    gate = _verify_release_gate(
        expected_head=expected_head,
        product_approval_reference=product_approval_reference,
    )
    canonical = Path(canonical_workbook).resolve()
    if not canonical.is_file() or canonical.suffix.lower() != ".xlsx":
        raise NewEnginePromotionError(f"Existing canonical .xlsx workbook is required: {canonical}")
    _verify_canonical_target_identity(
        canonical=canonical,
        ticker=str(kwargs.get("ticker") or ""),
    )
    record_path = Path(rollback_record_path).resolve()
    expected_record_hash = str(expected_rollback_record_sha256 or "").strip().lower()
    rollback_proof = _verify_rollback_artifacts(
        record_path=record_path,
        expected_record_sha256=expected_record_hash,
        canonical=canonical,
    )
    rollback_source = Path(rollback_proof["rollback_workbook"])
    rollback_hash = str(rollback_proof["rollback_sha256"])
    current_hash = _sha256(canonical)
    expected_current = str(rollback_proof["promoted_sha256"])
    if current_hash != expected_current or canonical.stat().st_size != int(rollback_proof["promoted_size"]):
        raise NewEnginePromotionError(
            f"Unexpected current canonical SHA-256 {current_hash}; expected {expected_current}."
        )
    source_validation = _validate_release_workbook(
        rollback_source,
        plan_receipt_path=plan_receipt_path,
        required_locale_id=release_locale,
        kwargs=kwargs,
    )
    validation_context = _release_validation_context(
        plan_receipt_path=plan_receipt_path,
        required_locale_id=release_locale,
        kwargs=kwargs,
    )
    if str(source_validation.get("workbook_sha256") or "") != rollback_hash:
        raise NewEnginePromotionError("Rollback source validation returned the wrong workbook identity.")
    post_validation_proof = _verify_rollback_artifacts(
        record_path=record_path,
        expected_record_sha256=expected_record_hash,
        canonical=canonical,
        rollback_workbook=rollback_source,
        expected_previous_sha256=rollback_hash,
        expected_promoted_sha256=current_hash,
    )
    if _sha256(canonical) != current_hash:
        raise NewEnginePromotionError("Canonical workbook changed during rollback dry-run validation.")
    if not execute:
        return {
            "status": "PASS",
            "mode": "dry-run",
            "canonical_path": canonical,
            "current_sha256": current_hash,
            "rollback_sha256": rollback_hash,
            "rollback_record_sha256": str(post_validation_proof["record_sha256"]),
            "validation_context": validation_context,
            "validation": source_validation,
            "canonical_unchanged": True,
        }

    _verify_release_gate(
        expected_head=expected_head,
        product_approval_reference=product_approval_reference,
    )
    rollback_proof = _verify_rollback_artifacts(
        record_path=record_path,
        expected_record_sha256=expected_record_hash,
        canonical=canonical,
        rollback_workbook=rollback_source,
        expected_previous_sha256=rollback_hash,
        expected_promoted_sha256=current_hash,
    )
    run_path = Path(run_dir).resolve()
    receipt_path = run_path / "rollback_receipt.json"
    if receipt_path.exists():
        raise NewEnginePromotionError(f"Artifact already exists: {receipt_path}")
    reapply_candidate = candidate_path_for(canonical)
    rollback_candidate: Path | None = None
    replaced = False
    completed = False
    try:
        _copy_exact(canonical, reapply_candidate)
        reapply_acl = normalize_candidate_acl(reapply_candidate)
        if _sha256(reapply_candidate) != current_hash:
            raise NewEnginePromotionError("Pre-rollback canonical changed while staging recovery bytes.")
        rollback_candidate = candidate_path_for(canonical)
        _copy_exact(rollback_source, rollback_candidate)
        acl = normalize_candidate_acl(rollback_candidate)
        if _sha256(rollback_candidate) != rollback_hash:
            raise NewEnginePromotionError("ACL-normalized rollback candidate differs from verified recovery bytes.")
        staged_validation = _validate_release_workbook(
            rollback_candidate,
            plan_receipt_path=plan_receipt_path,
            required_locale_id=release_locale,
            kwargs=kwargs,
        )
        if str(staged_validation.get("workbook_sha256") or "") != rollback_hash:
            raise NewEnginePromotionError("Staged rollback validation returned the wrong workbook identity.")
        if _release_validation_context(
            plan_receipt_path=plan_receipt_path,
            required_locale_id=release_locale,
            kwargs=kwargs,
        ) != validation_context:
            raise NewEnginePromotionError("Rollback validation context changed before canonical replacement.")
        _verify_release_gate(
            expected_head=expected_head,
            product_approval_reference=product_approval_reference,
        )
        pre_replacement_proof = _verify_rollback_artifacts(
            record_path=record_path,
            expected_record_sha256=expected_record_hash,
            canonical=canonical,
            rollback_workbook=rollback_source,
            expected_previous_sha256=rollback_hash,
            expected_promoted_sha256=current_hash,
        )
        if _sha256(canonical) != current_hash:
            raise NewEnginePromotionError("Canonical workbook changed before rollback replacement.")
        if _sha256(reapply_candidate) != current_hash or _sha256(rollback_candidate) != rollback_hash:
            raise NewEnginePromotionError("Rollback candidates changed before canonical replacement.")
        replace_existing_atomic(rollback_candidate, canonical)
        replaced = True
        if _sha256(canonical) != rollback_hash:
            raise NewEnginePromotionError("Restored canonical SHA-256 differs from the rollback record.")
        canonical_validation = _validate_release_workbook(
            canonical,
            plan_receipt_path=plan_receipt_path,
            required_locale_id=release_locale,
            kwargs=kwargs,
        )
        if str(canonical_validation.get("workbook_sha256") or "") != rollback_hash:
            raise NewEnginePromotionError("Restored canonical validation returned the wrong workbook identity.")
        if _release_validation_context(
            plan_receipt_path=plan_receipt_path,
            required_locale_id=release_locale,
            kwargs=kwargs,
        ) != validation_context:
            raise NewEnginePromotionError("Rollback validation context changed after canonical replacement.")
        receipt = {
            "receipt_version": ROLLBACK_RECEIPT_VERSION,
            "status": "PASS",
            "mode": "execute",
            "completed_at_utc": _utc_now(),
            "repo_head": gate["head"],
            "product_approval_reference": gate["product_approval_reference"],
            "validation_context": validation_context,
            "rollback_record": {
                "path": str(record_path),
                "sha256": str(pre_replacement_proof["record_sha256"]),
            },
            "canonical": {
                "path": str(canonical),
                "replaced_sha256": current_hash,
                "restored_sha256": rollback_hash,
            },
            "validations": {
                "rollback_source": source_validation,
                "staged_candidate": staged_validation,
                "canonical": canonical_validation,
                "acl": acl,
                "reapply_acl": reapply_acl,
                "pre_replacement_rollback_artifacts": {
                    "status": "PASS",
                    "record_sha256": str(pre_replacement_proof["record_sha256"]),
                    "rollback_sha256": str(pre_replacement_proof["rollback_sha256"]),
                },
            },
        }
        _write_json_no_overwrite(receipt_path, receipt)
        completed = True
        return {
            "status": "PASS",
            "mode": "execute",
            "canonical_path": canonical,
            "canonical_sha256": rollback_hash,
            "receipt_path": receipt_path,
            "receipt": receipt,
        }
    except Exception as exc:
        if replaced:
            try:
                if not reapply_candidate.is_file() or _sha256(reapply_candidate) != current_hash:
                    raise NewEnginePromotionError("Pre-rollback promoted recovery bytes failed verification.")
                normalize_candidate_acl(reapply_candidate)
                if _sha256(reapply_candidate) != current_hash:
                    raise NewEnginePromotionError("ACL normalization changed the promoted recovery bytes.")
                replace_existing_atomic(reapply_candidate, canonical)
                if _sha256(canonical) != current_hash:
                    raise NewEnginePromotionError("Could not reapply the pre-rollback canonical bytes.")
            except Exception as restore_exc:
                raise NewEnginePromotionError(
                    f"Rollback failed and reapplying the promoted canonical also failed: {exc}; restore={restore_exc}"
                ) from restore_exc
            raise NewEnginePromotionError(
                f"Rollback failed; the pre-rollback canonical workbook was restored: {exc}"
            ) from exc
        if isinstance(exc, NewEnginePromotionError):
            raise
        if isinstance(exc, NewEngineTransactionError):
            raise NewEnginePromotionError(str(exc)) from exc
        raise
    finally:
        if not completed:
            receipt_path.unlink(missing_ok=True)
            if run_path.exists() and not any(run_path.iterdir()):
                run_path.rmdir()
        if rollback_candidate is not None:
            rollback_candidate.unlink(missing_ok=True)
        reapply_candidate.unlink(missing_ok=True)
