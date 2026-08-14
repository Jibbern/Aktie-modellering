"""Fail-closed validation and atomic publication for acquired source bytes."""
from __future__ import annotations

import hashlib
import io
import json
import os
import re
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from xml.etree import ElementTree as ET


class SourceAcquisitionError(RuntimeError):
    """Raised when acquired bytes cannot satisfy the source publication contract."""


@dataclass(frozen=True)
class PublishedSource:
    path: Path
    sha256: str
    size: int


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _decode_text(payload: bytes, *, path: Path) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            text = payload.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:  # pragma: no cover - latin-1 is total, retained as a defensive guard
        raise SourceAcquisitionError(f"source text is not decodable: {path}")
    if not text.strip():
        raise SourceAcquisitionError(f"source text is empty after decoding: {path}")
    printable = sum(1 for char in text if char.isprintable() or char in "\r\n\t")
    if printable / max(1, len(text)) < 0.85:
        raise SourceAcquisitionError(f"source text contains excessive binary/control data: {path}")
    return text


def _validate_zip_document(payload: bytes, *, path: Path) -> None:
    try:
        with zipfile.ZipFile(io.BytesIO(payload), "r") as archive:
            bad_member = archive.testzip()
            names = set(archive.namelist())
    except (OSError, zipfile.BadZipFile) as exc:
        raise SourceAcquisitionError(f"source ZIP container is invalid: {path}") from exc
    if bad_member:
        raise SourceAcquisitionError(f"source ZIP member is corrupt ({bad_member}): {path}")
    if "[Content_Types].xml" not in names:
        raise SourceAcquisitionError(f"OOXML source lacks [Content_Types].xml: {path}")


def validate_source_bytes(
    payload: bytes,
    *,
    path: Path,
    expected_size: int | None = None,
    expected_sha256: str | None = None,
    content_type: str | None = None,
) -> PublishedSource:
    """Validate acquired bytes using explicit file identity, never nonempty size alone."""

    destination = Path(path)
    if not isinstance(payload, (bytes, bytearray)):
        raise SourceAcquisitionError(f"source acquisition did not return bytes: {destination}")
    data = bytes(payload)
    if not data:
        raise SourceAcquisitionError(f"source acquisition returned zero bytes: {destination}")
    if expected_size is not None and int(expected_size) >= 0 and len(data) != int(expected_size):
        raise SourceAcquisitionError(
            f"source byte count mismatch for {destination}: expected={int(expected_size)} actual={len(data)}"
        )
    digest = _sha256_bytes(data)
    expected_digest = str(expected_sha256 or "").strip().lower()
    if expected_digest and digest.lower() != expected_digest:
        raise SourceAcquisitionError(
            f"source SHA-256 mismatch for {destination}: expected={expected_digest} actual={digest}"
        )

    suffix = destination.suffix.lower()
    media_type = str(content_type or "").split(";", 1)[0].strip().lower()
    if suffix == ".pdf":
        if media_type in {"text/html", "application/xhtml+xml"}:
            raise SourceAcquisitionError(f"PDF source returned HTML content: {destination}")
        if not data.startswith(b"%PDF-") or b"%%EOF" not in data[-8192:]:
            raise SourceAcquisitionError(f"PDF source is truncated or has an invalid signature: {destination}")
    elif suffix in {".htm", ".html"}:
        if media_type == "application/pdf":
            raise SourceAcquisitionError(f"HTML source returned PDF content: {destination}")
        text = _decode_text(data, path=destination)
        lowered = text.lower()
        if not re.search(r"<(?:!doctype\s+html|html\b|body\b|document\b|xbrl\b|ix:[a-z]+\b)", lowered):
            raise SourceAcquisitionError(f"HTML source lacks a recognized document root: {destination}")
        if not re.search(r"</(?:html|body|document|xbrl)\s*>", lowered):
            raise SourceAcquisitionError(f"HTML source appears truncated (no closing document tag): {destination}")
    elif suffix == ".json":
        text = _decode_text(data, path=destination)
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise SourceAcquisitionError(f"JSON source is invalid: {destination}") from exc
        if not isinstance(parsed, (dict, list)):
            raise SourceAcquisitionError(f"JSON source root must be an object or array: {destination}")
    elif suffix in {".xml", ".xsd"}:
        try:
            ET.fromstring(data)
        except ET.ParseError as exc:
            raise SourceAcquisitionError(f"XML source is invalid or truncated: {destination}") from exc
    elif suffix in {".txt", ".md", ".csv"}:
        _decode_text(data, path=destination)
    elif suffix in {".xlsx", ".xlsm", ".docx"}:
        _validate_zip_document(data, path=destination)
    elif suffix == ".xls":
        if not data.startswith(bytes.fromhex("D0CF11E0A1B11AE1")):
            raise SourceAcquisitionError(f"legacy Excel source has an invalid OLE signature: {destination}")
    elif suffix == ".parquet":
        if len(data) < 8 or not data.startswith(b"PAR1") or not data.endswith(b"PAR1"):
            raise SourceAcquisitionError(f"Parquet source is invalid or truncated: {destination}")
    else:
        raise SourceAcquisitionError(f"source file type is not supported for verified publication: {destination}")
    return PublishedSource(path=destination, sha256=digest, size=len(data))


def validate_published_source(
    path: Path,
    *,
    expected_size: int | None = None,
    expected_sha256: str | None = None,
    content_type: str | None = None,
) -> PublishedSource:
    source_path = Path(path)
    try:
        payload = source_path.read_bytes()
    except OSError as exc:
        raise SourceAcquisitionError(f"published source is unreadable: {source_path}") from exc
    return validate_source_bytes(
        payload,
        path=source_path,
        expected_size=expected_size,
        expected_sha256=expected_sha256,
        content_type=content_type,
    )


def atomic_publish_source_bytes(
    path: Path,
    payload: bytes,
    *,
    expected_size: int | None = None,
    expected_sha256: str | None = None,
    content_type: str | None = None,
) -> PublishedSource:
    """Validate, fsync, and atomically replace a final source on one filesystem."""

    final_path = Path(path)
    receipt = validate_source_bytes(
        payload,
        path=final_path,
        expected_size=expected_size,
        expected_sha256=expected_sha256,
        content_type=content_type,
    )
    final_path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary_name = tempfile.mkstemp(
        dir=final_path.parent,
        prefix=f".{final_path.name}.",
        suffix=".partial",
    )
    temporary_path = Path(temporary_name)
    primary_error: BaseException | None = None
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(bytes(payload))
            stream.flush()
            os.fsync(stream.fileno())
        staged = validate_source_bytes(
            temporary_path.read_bytes(),
            path=final_path,
            expected_size=receipt.size,
            expected_sha256=receipt.sha256,
            content_type=content_type,
        )
        os.replace(temporary_path, final_path)
        return PublishedSource(path=final_path, sha256=staged.sha256, size=staged.size)
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        if temporary_path.exists():
            try:
                temporary_path.unlink()
            except OSError as cleanup_exc:
                if primary_error is not None:
                    primary_error.add_note(
                        f"source staging cleanup also failed for {temporary_path}: "
                        f"{type(cleanup_exc).__name__}: {cleanup_exc}"
                    )
                else:
                    raise SourceAcquisitionError(
                        f"published source but could not clean staging path {temporary_path}: "
                        f"{type(cleanup_exc).__name__}: {cleanup_exc}"
                    ) from cleanup_exc


def atomic_publish_source_file(
    source_path: Path,
    final_path: Path,
    *,
    expected_sha256: str | None = None,
) -> PublishedSource:
    try:
        payload = Path(source_path).read_bytes()
    except OSError as exc:
        raise SourceAcquisitionError(f"source file is unreadable: {source_path}") from exc
    return atomic_publish_source_bytes(final_path, payload, expected_sha256=expected_sha256)


def atomic_publish_json(path: Path, payload: Mapping[str, Any], *, indent: int | None = None) -> PublishedSource:
    encoded = json.dumps(dict(payload), indent=indent).encode("utf-8")
    return atomic_publish_source_bytes(Path(path), encoded)
