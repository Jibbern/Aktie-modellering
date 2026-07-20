"""Filesystem transaction primitives for versioned new-engine workbooks."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import subprocess
import sys
import uuid
from typing import Any


class NewEngineTransactionError(RuntimeError):
    """Raised when a workbook cannot be published without weakening safety."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def candidate_path_for(final_path: Path | str) -> Path:
    """Return a unique same-directory candidate name without creating it."""

    final = Path(final_path)
    if final.suffix.lower() != ".xlsx":
        raise NewEngineTransactionError("Final workbook must be a macro-free .xlsx file.")
    for _ in range(16):
        candidate = final.with_name(f".{final.stem}.{uuid.uuid4().hex}.candidate.xlsx")
        if not candidate.exists():
            return candidate
    raise NewEngineTransactionError("Could not allocate a unique workbook candidate name.")


def normalize_candidate_acl(
    candidate_path: Path | str,
    *,
    platform: str = sys.platform,
) -> dict[str, Any]:
    """Reset one Windows candidate to inherited parent ACLs without changing bytes."""

    candidate = Path(candidate_path)
    if not candidate.is_file():
        raise NewEngineTransactionError(f"Workbook candidate does not exist: {candidate}")
    before = _sha256(candidate)
    if platform != "win32":
        return {
            "status": "NOT_APPLICABLE",
            "platform": platform,
            "sha256_before": before,
            "sha256_after": before,
        }

    for arguments in (
        ["icacls", str(candidate), "/inheritancelevel:e"],
        ["icacls", str(candidate), "/reset"],
    ):
        try:
            completed = subprocess.run(
                arguments,
                check=False,
                capture_output=True,
                text=True,
                shell=False,
            )
        except OSError as exc:
            raise NewEngineTransactionError(f"Could not execute icacls for {candidate}: {exc}") from exc
        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout or "unknown icacls error").strip()
            raise NewEngineTransactionError(f"ACL normalization failed for {candidate}: {detail}")

    try:
        import win32security

        descriptor = win32security.GetFileSecurity(str(candidate), win32security.DACL_SECURITY_INFORMATION)
        control, _revision = descriptor.GetSecurityDescriptorControl()
        inheritance_enabled = not bool(control & win32security.SE_DACL_PROTECTED)
        dacl = descriptor.GetSecurityDescriptorDacl()
        inherited_ace_count = 0
        if dacl is not None:
            for index in range(dacl.GetAceCount()):
                ace = dacl.GetAce(index)
                if int(ace[0][1]) & 0x10:  # INHERITED_ACE
                    inherited_ace_count += 1
    except Exception as exc:  # pragma: no cover - platform API failure details vary
        raise NewEngineTransactionError(f"Could not verify candidate DACL inheritance: {exc}") from exc

    after = _sha256(candidate)
    if before != after:
        raise NewEngineTransactionError("ACL normalization changed workbook bytes.")
    if not inheritance_enabled or inherited_ace_count <= 0:
        raise NewEngineTransactionError("Candidate DACL does not inherit effective ACEs from its parent.")
    if not os.access(candidate, os.R_OK):
        raise NewEngineTransactionError("Candidate is not readable after ACL normalization.")
    return {
        "status": "PASS",
        "platform": platform,
        "inheritance_enabled": True,
        "inherited_ace_count": inherited_ace_count,
        "sha256_before": before,
        "sha256_after": after,
        "readable": True,
    }


def publish_no_overwrite(
    candidate_path: Path | str,
    final_path: Path | str,
    *,
    platform: str = sys.platform,
) -> None:
    """Publish one same-directory candidate atomically without overwriting."""

    candidate = Path(candidate_path)
    final = Path(final_path)
    if candidate.parent.resolve() != final.parent.resolve():
        raise NewEngineTransactionError("Candidate and final workbook must be in the same directory.")
    if not candidate.is_file():
        raise NewEngineTransactionError(f"Workbook candidate does not exist: {candidate}")
    if final.exists():
        raise NewEngineTransactionError(f"Destination already exists: {final}")
    try:
        if platform == "win32":
            os.rename(candidate, final)
        else:
            os.link(candidate, final)
            candidate.unlink()
    except FileExistsError as exc:
        raise NewEngineTransactionError(f"Destination already exists: {final}") from exc
    except OSError as exc:
        if final.exists():
            raise NewEngineTransactionError(f"Destination already exists: {final}") from exc
        raise NewEngineTransactionError(
            f"No safe no-overwrite publication primitive succeeded for {candidate}: {exc}"
        ) from exc
