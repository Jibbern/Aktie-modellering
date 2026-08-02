"""Replay reviewed transcript metadata as a locator/index, never as economics."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import PurePath
from types import MappingProxyType
from typing import Any, Mapping

from .types import DiscoveredDocument, LocatorError, SourceSet, text_sha256


@dataclass(frozen=True, slots=True)
class ReviewedMetadataRevision:
    document_key: str
    transcript_document_key: str
    revision: int
    review_date: str
    transcript_sha256: str
    metadata_sha256: str
    predecessor_metadata_sha256: str | None
    predecessor_bytes_available: str
    change_reason: str
    material_locators: tuple[Mapping[str, Any], ...]


def parse_reviewed_metadata_bytes(data: bytes) -> Mapping[str, Mapping[str, str]]:
    """Parse the closed section/key index and reject duplicate semantic keys."""

    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise LocatorError("Reviewed transcript metadata is not UTF-8 text.") from exc
    sections: dict[str, dict[str, str]] = {}
    section: str | None = None
    prose_ordinal = 0
    for line_number, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1].strip()
            if not section or section in sections:
                raise LocatorError(f"Reviewed metadata has duplicate or empty section at line {line_number}.")
            sections[section] = {}
            prose_ordinal = 0
            continue
        if section is None:
            raise LocatorError("Reviewed metadata has content before its first section.")
        if " = " in line:
            key, value = line.split(" = ", 1)
        else:
            prose_ordinal += 1
            key, value = f"__prose_{prose_ordinal}", line
        key = key.strip()
        value = value.strip()
        if not key or not value or key in sections[section]:
            raise LocatorError(f"Reviewed metadata has a duplicate or empty key at line {line_number}.")
        sections[section][key] = value
    if "METADATA" not in sections:
        raise LocatorError("Reviewed transcript metadata lacks [METADATA].")
    return MappingProxyType(
        {name: MappingProxyType(dict(values)) for name, values in sections.items()}
    )


def _transcript_lines(document: DiscoveredDocument) -> tuple[str, ...]:
    try:
        return tuple(document.verified_bytes.decode("utf-8-sig").splitlines())
    except UnicodeDecodeError as exc:
        raise LocatorError("Reviewed transcript is not UTF-8 text.") from exc


def _unquote(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] == '"':
        return value[1:-1]
    return value


def verify_reviewed_metadata_documents(
    source_set: SourceSet,
    documents: tuple[DiscoveredDocument, ...],
) -> tuple[ReviewedMetadataRevision, ...]:
    """Verify every reviewed-metadata revision against its raw transcript bytes."""

    by_key = {document.spec.document_key: document for document in documents}
    result: list[ReviewedMetadataRevision] = []
    for document in sorted(documents, key=lambda row: row.spec.document_key):
        if document.spec.role_id != "reviewed-transcript-metadata":
            continue
        metadata = parse_reviewed_metadata_bytes(document.verified_bytes)
        provenance = metadata["METADATA"]
        role = dict(document.spec.role_metadata or {})
        transcript_key = str(role.get("transcript_document_key") or "")
        transcript = by_key.get(transcript_key)
        if transcript is None or transcript.spec.role_id != "earnings-call-transcript":
            raise LocatorError("Reviewed metadata does not resolve one raw transcript origin.")
        transcript_sha = hashlib.sha256(transcript.verified_bytes).hexdigest()
        checks = {
            "metadata_revision": str(document.spec.revision),
            "metadata_review_state": "accepted",
            "metadata_review_date": document.spec.publication_date,
            "source_file": PurePath(transcript.spec.relative_path).name,
            "source_file_type": "txt",
            "source_file_sha256": transcript_sha,
            "supersedes_metadata_sha256": str(role.get("predecessor_metadata_sha256") or ""),
            "predecessor_bytes_available": str(role.get("predecessor_bytes_available") or ""),
            "metadata_change_reason": str(role.get("change_reason") or ""),
        }
        for key, expected in checks.items():
            if provenance.get(key) != expected:
                raise LocatorError(f"Reviewed metadata provenance field {key!r} disagrees with its authority.")
        if role.get("transcript_sha256") != transcript_sha:
            raise LocatorError("Reviewed metadata role carries the wrong transcript SHA-256.")
        if role.get("metadata_sha256") != document.content_sha256:
            raise LocatorError("Reviewed metadata role carries the wrong metadata SHA-256.")
        if role.get("review_date") != document.spec.publication_date:
            raise LocatorError("Reviewed metadata role carries the wrong review date.")

        guards = role.get("reviewed_field_guards")
        if not isinstance(guards, (list, tuple)) or not guards:
            raise LocatorError("Reviewed metadata requires closed semantic field guards.")
        seen_guards: set[tuple[str, str, str]] = set()
        for guard in sorted(
            guards,
            key=lambda row: (
                str(row["section"]),
                str(row["metadata_key"]),
                str(row["operator"]),
            ),
        ):
            section = str(guard["section"])
            key = str(guard["metadata_key"])
            operator = str(guard["operator"])
            identity = (section, key, operator)
            if identity in seen_guards:
                raise LocatorError("Reviewed metadata semantic field guard is duplicated.")
            seen_guards.add(identity)
            values = metadata.get(section)
            if values is None or key not in values:
                raise LocatorError(
                    f"Reviewed metadata guard references unknown field {section}.{key}."
                )
            actual = str(values[key])
            expected = str(guard["expected_value"])
            if operator == "equals":
                compatible = actual == expected
            elif operator == "not-contains":
                compatible = expected.casefold() not in actual.casefold()
            else:  # protected independently by the closed input schema
                raise LocatorError(
                    f"Reviewed metadata guard uses unsupported operator {operator!r}."
                )
            if not compatible:
                raise LocatorError(
                    f"Reviewed metadata field guard failed for {section}.{key}."
                )

        lines = _transcript_lines(transcript)
        locators = role.get("material_quote_locators")
        if not isinstance(locators, (list, tuple)) or not locators:
            raise LocatorError("Reviewed metadata requires exact material transcript locators.")
        replayed_locators: list[Mapping[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for locator in sorted(locators, key=lambda row: (str(row["section"]), str(row["metadata_key"]))):
            section = str(locator["section"])
            key = str(locator["metadata_key"])
            if (section, key) in seen:
                raise LocatorError("Reviewed metadata material locator is duplicated.")
            seen.add((section, key))
            values = metadata.get(section)
            if values is None or key not in values:
                raise LocatorError(f"Reviewed metadata locator references unknown field {section}.{key}.")
            start = int(locator["start_line"])
            end = int(locator["end_line"])
            if start < 1 or end < start or end > len(lines):
                raise LocatorError(f"Reviewed transcript locator for {section}.{key} has an invalid line range.")
            excerpt = "\n".join(lines[start - 1 : end])
            expected_excerpt = _unquote(str(values[key]))
            if expected_excerpt not in excerpt:
                raise LocatorError(f"Reviewed metadata value {section}.{key} is unsupported by raw transcript text.")
            if locator.get("line_digest") != text_sha256(excerpt):
                raise LocatorError(f"Reviewed transcript line digest changed for {section}.{key}.")
            replayed_locators.append(
                MappingProxyType(
                    {
                        "section": section,
                        "metadata_key": key,
                        "start_line": start,
                        "end_line": end,
                        "line_digest": str(locator["line_digest"]),
                    }
                )
            )
        result.append(
            ReviewedMetadataRevision(
                document_key=document.spec.document_key,
                transcript_document_key=transcript_key,
                revision=document.spec.revision,
                review_date=document.spec.publication_date,
                transcript_sha256=transcript_sha,
                metadata_sha256=document.content_sha256,
                predecessor_metadata_sha256=role.get("predecessor_metadata_sha256"),
                predecessor_bytes_available=str(role["predecessor_bytes_available"]),
                change_reason=str(role["change_reason"]),
                material_locators=tuple(replayed_locators),
            )
        )
    return tuple(result)
