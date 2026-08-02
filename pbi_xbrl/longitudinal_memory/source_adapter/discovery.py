"""Strict root-injected source discovery and immutable document creation."""
from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path, PureWindowsPath
from typing import Any, Mapping

from pbi_xbrl.json_schema_validation import load_json_strict, validate_json_schema
from pbi_xbrl.longitudinal_memory.identity import (
    canonical_company_id,
    source_document_identity,
    validate_semantic_id,
)

from .types import (
    DiscoveredDocument,
    DocumentSpec,
    SourceContractError,
    SourceDiscoveryError,
    SourceSet,
)


DEFAULT_SOURCE_SCHEMA_PATH = (
    Path(__file__).resolve().parents[3]
    / "docs"
    / "longitudinal_memory_source_adapter_input.schema.json"
)

@dataclass(frozen=True)
class DocumentRole:
    role_id: str
    source_family: str
    document_type: str
    authority_class: str
    publication_date_basis: str
    accession_required: bool
    embedded_date_locator_kind: str | None
    reviewed_link_type: str | None
    permitted_assertion_kinds: frozenset[str]


_ISSUER_RELEASE_ASSERTIONS = frozenset(
    {
        "numerical_fact",
        "guidance",
        "promise_version",
        "management_statement",
        "company_event",
        "period_evidence",
    }
)

DOCUMENT_ROLES = (
    DocumentRole(
        role_id="sec-filed-earnings-release-exhibit",
        source_family="sec-exhibit",
        document_type="earnings-release",
        authority_class="filed-exhibit",
        publication_date_basis="sec-filed-date",
        accession_required=True,
        embedded_date_locator_kind="html-dateline",
        reviewed_link_type=None,
        permitted_assertion_kinds=_ISSUER_RELEASE_ASSERTIONS,
    ),
    DocumentRole(
        role_id="issuer-earnings-release-pdf",
        source_family="issuer-pdf",
        document_type="earnings-release",
        authority_class="company-release",
        publication_date_basis="embedded-dateline",
        accession_required=False,
        embedded_date_locator_kind="pdf-dateline",
        reviewed_link_type=None,
        permitted_assertion_kinds=_ISSUER_RELEASE_ASSERTIONS,
    ),
    DocumentRole(
        role_id="issuer-business-update-pdf",
        source_family="issuer-pdf",
        document_type="business-update",
        authority_class="company-release",
        publication_date_basis="embedded-dateline",
        accession_required=False,
        embedded_date_locator_kind="pdf-dateline",
        reviewed_link_type=None,
        permitted_assertion_kinds=_ISSUER_RELEASE_ASSERTIONS,
    ),
    DocumentRole(
        role_id="issuer-earnings-history-workbook",
        source_family="issuer-spreadsheet",
        document_type="earnings-presentation-workbook",
        authority_class="company-presentation",
        publication_date_basis="reviewed-source-catalog",
        accession_required=False,
        embedded_date_locator_kind=None,
        reviewed_link_type=None,
        permitted_assertion_kinds=frozenset({"numerical_fact", "period_evidence"}),
    ),
    DocumentRole(
        role_id="earnings-call-transcript",
        source_family="issuer-transcript",
        document_type="earnings-transcript",
        authority_class="company-transcript",
        publication_date_basis="reviewed-same-event-link",
        accession_required=False,
        embedded_date_locator_kind=None,
        reviewed_link_type="same-event",
        permitted_assertion_kinds=frozenset(
            {"guidance", "promise_version", "management_statement", "company_event"}
        ),
    ),
)

ROLE_BY_TUPLE = {
    (
        role.source_family,
        role.document_type,
        role.authority_class,
        role.publication_date_basis,
    ): role
    for role in DOCUMENT_ROLES
}
KNOWN_SOURCE_FAMILIES = frozenset(role.source_family for role in DOCUMENT_ROLES)
_ACCESSION_PATTERN = re.compile(r"^[0-9]{10}-[0-9]{2}-[0-9]{6}$")


def _duplicates(values: list[str]) -> list[str]:
    seen: set[str] = set()
    repeated: set[str] = set()
    for value in values:
        if value in seen:
            repeated.add(value)
        seen.add(value)
    return sorted(repeated)


def _relative_parts(raw: str) -> tuple[str, ...]:
    if not raw or any(token in raw for token in ("*", "?", "[", "]")):
        raise SourceContractError(f"Source path must be one explicit relative file: {raw!r}.")
    candidate = PureWindowsPath(raw.replace("/", "\\"))
    if candidate.is_absolute() or candidate.drive or candidate.root:
        raise SourceContractError(f"Absolute source paths are forbidden: {raw!r}.")
    if not candidate.parts or any(part in {"", ".", ".."} for part in candidate.parts):
        raise SourceContractError(f"Source path traversal is forbidden: {raw!r}.")
    return tuple(candidate.parts)


def document_role(document: DocumentSpec) -> DocumentRole:
    key = (
        document.source_family,
        document.document_type,
        document.authority_class,
        document.publication_date_basis,
    )
    role = ROLE_BY_TUPLE.get(key)
    if role is None:
        raise SourceContractError(
            "Incoherent document role tuple for "
            f"{document.document_key!r}: family={document.source_family!r}, "
            f"type={document.document_type!r}, authority={document.authority_class!r}, "
            f"publication_basis={document.publication_date_basis!r}."
        )
    return role


def _validate_document_role(
    source_set: SourceSet,
    document: DocumentSpec,
    *,
    role: DocumentRole,
) -> None:
    profile_publisher = str(source_set.profile.get("publisher_id", ""))
    if document.publisher_id != profile_publisher:
        raise SourceContractError(
            f"Document {document.document_key!r} is not published by the declared profile publisher."
        )
    if role.accession_required:
        if document.accession is None or _ACCESSION_PATTERN.fullmatch(document.accession) is None:
            raise SourceContractError(
                f"SEC source {document.document_key!r} requires one valid accession."
            )
        if not document.accession.startswith(f"{source_set.profile.get('cik')}-"):
            raise SourceContractError(
                f"SEC source {document.document_key!r} accession is not owned by the profile CIK."
            )
        if document.report_date is None:
            raise SourceContractError(
                f"SEC source {document.document_key!r} requires its reported fiscal date."
            )
    elif document.accession is not None:
        raise SourceContractError(
            f"Non-SEC source {document.document_key!r} cannot claim an SEC accession."
        )

    locator = document.publication_date_locator
    embedded = document.embedded_publication_date
    if role.embedded_date_locator_kind is None:
        if embedded is not None or locator is not None:
            raise SourceContractError(
                f"Source role {role.role_id!r} cannot claim an embedded publication dateline."
            )
    else:
        if embedded is None or locator is None:
            raise SourceContractError(
                f"Source role {role.role_id!r} requires one reproducible embedded dateline."
            )
        try:
            embedded_date = date.fromisoformat(embedded)
        except ValueError as exc:
            raise SourceContractError(
                f"Source {document.document_key!r} has an invalid embedded dateline."
            ) from exc
        if locator.get("locator_kind") != role.embedded_date_locator_kind:
            raise SourceContractError(
                f"Source {document.document_key!r} uses the wrong dateline locator family."
            )
        if document.publication_date_basis == "embedded-dateline" and embedded != document.publication_date:
            raise SourceContractError(
                f"Embedded-dateline source {document.document_key!r} disagrees with publication_date."
            )
        if document.publication_date_basis == "sec-filed-date" and embedded_date > date.fromisoformat(
            document.publication_date
        ):
            raise SourceContractError(
                f"SEC source {document.document_key!r} predates its embedded issuer dateline."
            )


def _validate_semantics(source_set: SourceSet) -> None:
    try:
        if canonical_company_id(source_set.company_id) != source_set.company_id:
            raise SourceContractError("Source-set company_id must already be canonical uppercase.")
        date.fromisoformat(source_set.knowledge_cutoff)
        validate_semantic_id(source_set.sector_pack_id)
        validate_semantic_id(source_set.ticker_profile_id)
    except ValueError as exc:
        raise SourceContractError(str(exc)) from exc

    if source_set.profile.get("company_id") != source_set.company_id:
        raise SourceContractError("Ticker profile company differs from SourceSet company.")
    if source_set.normalized_package_ref.get("source_package_company_id") != source_set.company_id:
        raise SourceContractError("Normalized-package reference is cross-company.")

    document_keys = [row.document_key for row in source_set.documents]
    duplicate_documents = _duplicates(document_keys)
    if duplicate_documents:
        raise SourceContractError(f"Duplicate document keys: {duplicate_documents}.")
    known_documents = set(document_keys)
    roles_by_document: dict[str, DocumentRole] = {}
    for document in source_set.documents:
        _relative_parts(document.relative_path)
        if document.source_family not in KNOWN_SOURCE_FAMILIES:
            raise SourceContractError(f"Unknown source family {document.source_family!r}.")
        try:
            publication = date.fromisoformat(document.publication_date)
        except ValueError as exc:
            raise SourceContractError(
                f"Unresolved publication metadata for {document.document_key!r}."
            ) from exc
        if publication > date.fromisoformat(source_set.knowledge_cutoff):
            raise SourceContractError(
                f"Document {document.document_key!r} is after the knowledge cutoff."
            )
        if document.origin_document_key is not None and document.origin_document_key not in known_documents:
            raise SourceContractError(
                f"Unknown origin document {document.origin_document_key!r}."
            )
        role = document_role(document)
        _validate_document_role(source_set, document, role=role)
        roles_by_document[document.document_key] = role

    assertion_keys = [str(row.get("assertion_key", "")) for row in source_set.required_assertions]
    duplicate_assertions = _duplicates(assertion_keys)
    if duplicate_assertions:
        raise SourceContractError(f"Duplicate assertion keys: {duplicate_assertions}.")
    locator_keys = [str(row.get("locator", {}).get("locator_key", "")) for row in source_set.required_assertions]
    duplicate_locators = _duplicates(locator_keys)
    if duplicate_locators:
        raise SourceContractError(f"Duplicate locator keys: {duplicate_locators}.")
    for assertion in source_set.required_assertions:
        if assertion.get("document_key") not in known_documents:
            raise SourceContractError(
                f"Assertion {assertion.get('assertion_key')!r} references an unknown document."
            )
        role = roles_by_document[str(assertion["document_key"])]
        assertion_kind = str(assertion.get("assertion_kind"))
        if assertion_kind not in role.permitted_assertion_kinds:
            raise SourceContractError(
                f"Document role {role.role_id!r} cannot support assertion policy "
                f"{assertion_kind!r}."
            )
    period_keys = [str(row.get("period_key", "")) for row in source_set.periods]
    duplicate_periods = _duplicates(period_keys)
    if duplicate_periods:
        raise SourceContractError(f"Duplicate period keys: {duplicate_periods}.")
    assertion_set = set(assertion_keys)
    for period in source_set.periods:
        if period.get("evidence_assertion_key") not in assertion_set:
            raise SourceContractError(
                f"Period {period.get('period_key')!r} lacks a declared evidence assertion."
            )
    link_keys = [str(row.get("link_key", "")) for row in source_set.reviewed_links]
    if _duplicates(link_keys):
        raise SourceContractError("Reviewed link keys must be unique.")
    documents_by_key = {row.document_key: row for row in source_set.documents}
    for link in source_set.reviewed_links:
        if link.get("from_document_key") not in known_documents or link.get("to_document_key") not in known_documents:
            raise SourceContractError(f"Reviewed link {link.get('link_key')!r} has an unknown endpoint.")
        source = documents_by_key[str(link["from_document_key"])]
        target = documents_by_key[str(link["to_document_key"])]
        if source.document_key == target.document_key:
            raise SourceContractError(f"Reviewed link {link.get('link_key')!r} cannot be a self-link.")
        if link.get("relation_type") in {"same-event", "event-date-support"}:
            if source.publication_date != target.publication_date:
                raise SourceContractError(
                    f"Reviewed event link {link.get('link_key')!r} joins different publication dates."
                )
            if (
                source.report_date is not None
                and target.report_date is not None
                and source.report_date != target.report_date
            ):
                raise SourceContractError(
                    f"Reviewed event link {link.get('link_key')!r} joins different report dates."
                )
            knowledge_date = date.fromisoformat(str(link.get("knowledge_date")))
            if (
                knowledge_date < date.fromisoformat(source.publication_date)
                or knowledge_date > date.fromisoformat(source_set.knowledge_cutoff)
            ):
                raise SourceContractError(
                    f"Reviewed event link {link.get('link_key')!r} has an invalid knowledge date."
                )
    origin_by_document = {
        row.document_key: row.origin_document_key for row in source_set.documents
    }
    for document_key in origin_by_document:
        seen: set[str] = set()
        cursor: str | None = document_key
        while cursor is not None:
            if cursor in seen:
                raise SourceContractError("Source-document origin chains cannot contain cycles.")
            seen.add(cursor)
            cursor = origin_by_document[cursor]
    for document in source_set.documents:
        if document.origin_document_key is None:
            continue
        origin = documents_by_key[document.origin_document_key]
        if document_role(origin).role_id != roles_by_document[document.document_key].role_id:
            raise SourceContractError(
                f"Origin document for {document.document_key!r} has an incompatible source role."
            )
    for document in source_set.documents:
        role = roles_by_document[document.document_key]
        if role.reviewed_link_type is None:
            continue
        matches = [
            row
            for row in source_set.reviewed_links
            if row.get("relation_type") == role.reviewed_link_type
            and row.get("from_document_key") == document.document_key
            and date.fromisoformat(str(row.get("knowledge_date")))
            >= date.fromisoformat(document.publication_date)
            and date.fromisoformat(str(row.get("knowledge_date")))
            <= date.fromisoformat(source_set.knowledge_cutoff)
            and row.get("review_state") in {"accepted", "reviewed"}
        ]
        if len(matches) != 1:
            raise SourceContractError(
                f"Document {document.document_key!r} lacks one reviewed publication-date link."
            )


def load_source_set(
    path: Path | str,
    *,
    schema_path: Path | str = DEFAULT_SOURCE_SCHEMA_PATH,
) -> SourceSet:
    """Load the closed source contract with duplicate-key rejection."""

    raw = load_json_strict(path)
    schema = load_json_strict(schema_path)
    failures = validate_json_schema(raw, schema)
    if failures:
        formatted = "; ".join(f"{location} [{rule}] {message}" for location, rule, message in failures)
        raise SourceContractError(f"Source-set schema validation failed: {formatted}")
    if not isinstance(raw, Mapping):  # pragma: no cover - schema already guarantees this
        raise SourceContractError("Source set must be a JSON object.")
    result = SourceSet.from_mapping(raw)
    _validate_semantics(result)
    return result


def _is_reparse_point(path: Path) -> bool:
    info = path.lstat()
    attributes = getattr(info, "st_file_attributes", 0)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return bool(attributes & reparse_flag)


def _resolve_declared_file(root: Path, document: DocumentSpec) -> Path:
    parts = _relative_parts(document.relative_path)
    parent = root.joinpath(*parts[:-1])
    if not parent.is_dir():
        raise SourceDiscoveryError(f"Missing source parent for {document.document_key!r}.")
    matches = sorted(
        (entry for entry in parent.iterdir() if entry.name.casefold() == parts[-1].casefold()),
        key=lambda entry: entry.name,
    )
    if len(matches) != 1:
        raise SourceDiscoveryError(
            f"Expected exactly one file for {document.document_key!r}; found {len(matches)}."
        )
    candidate = matches[0]
    if candidate.is_symlink() or _is_reparse_point(candidate):
        raise SourceDiscoveryError(f"Unsafe symlink/reparse source {candidate}.")
    if not candidate.is_file():
        raise SourceDiscoveryError(f"Declared source is not one ordinary file: {candidate}.")
    resolved = candidate.resolve(strict=True)
    try:
        common = Path(os.path.commonpath((str(root), str(resolved))))
    except ValueError as exc:
        raise SourceDiscoveryError(f"Source escapes the injected root: {candidate}.") from exc
    if os.path.normcase(str(common)) != os.path.normcase(str(root)):
        raise SourceDiscoveryError(f"Source escapes the injected root: {candidate}.")
    return resolved


def _read_snapshot(path: Path) -> bytes:
    with path.open("rb") as handle:
        return handle.read()


def discover_sources(source_set: SourceSet, source_root: Path | str) -> tuple[DiscoveredDocument, ...]:
    """Resolve and hash only declared files below one injected source root."""

    root = Path(source_root)
    if not root.is_absolute() or not root.is_dir():
        raise SourceDiscoveryError("The injected source root must be one existing absolute directory.")
    if root.is_symlink() or _is_reparse_point(root):
        raise SourceDiscoveryError("The injected source root cannot be a symlink or reparse point.")
    root = root.resolve(strict=True)

    result: list[DiscoveredDocument] = []
    identity_bytes: dict[str, str] = {}
    for document in sorted(source_set.documents, key=lambda row: row.document_key):
        resolved = _resolve_declared_file(root, document)
        verified_bytes = _read_snapshot(resolved)
        actual_sha256 = hashlib.sha256(verified_bytes).hexdigest()
        if actual_sha256 != document.expected_sha256:
            raise SourceDiscoveryError(
                f"SHA-256 mismatch for {document.document_key!r}: expected "
                f"{document.expected_sha256}, received {actual_sha256}."
            )
        readable_id = source_document_identity(
            company_id=source_set.company_id,
            publisher_id=document.publisher_id,
            document_type=document.document_type,
            publication_date=document.publication_date,
            document_key=document.document_key,
            revision=document.revision,
        )
        prior = identity_bytes.get(readable_id)
        if prior is not None and prior != actual_sha256:
            raise SourceDiscoveryError(
                f"Readable SourceDocument identity {readable_id!r} maps to different bytes."
            )
        identity_bytes[readable_id] = actual_sha256
        result.append(
            DiscoveredDocument(
                spec=document,
                absolute_path=resolved,
                verified_bytes=verified_bytes,
                content_sha256=actual_sha256,
                source_document_id=readable_id,
            )
        )
    return tuple(sorted(result, key=lambda row: row.source_document_id))


def verify_reviewed_model_inputs(source_set: SourceSet, model_root: Path | str) -> None:
    """Replay reviewed model-input bytes and their recorded acceptance date."""

    root = Path(model_root)
    if not root.is_absolute() or not root.is_dir():
        raise SourceDiscoveryError(
            "The reviewed-model root must be one existing absolute directory."
        )
    root = root.resolve(strict=True)

    for model_input in source_set.reviewed_model_inputs:
        source_ref = str(model_input["source_ref"])
        relative, separator, fragment = source_ref.partition("#")
        if not separator or not fragment:
            raise SourceDiscoveryError("Reviewed model input requires a bounded source fragment.")
        path = root.joinpath(*_relative_parts(relative)).resolve(strict=True)
        common = Path(os.path.commonpath((str(root), str(path))))
        if os.path.normcase(str(common)) != os.path.normcase(str(root)) or not path.is_file():
            raise SourceDiscoveryError("Reviewed model input escapes its injected root.")
        verified_bytes = _read_snapshot(path)
        actual_sha256 = hashlib.sha256(verified_bytes).hexdigest()
        if actual_sha256 != model_input["source_content_sha256"]:
            raise SourceDiscoveryError("Reviewed model-input content hash changed.")
        try:
            raw = json.loads(
                verified_bytes.decode("utf-8"),
                object_pairs_hook=_unique_json_object,
                parse_constant=_reject_json_constant,
            )
        except (UnicodeDecodeError, ValueError) as exc:
            raise SourceDiscoveryError("Reviewed model input is not strict UTF-8 JSON.") from exc
        generated_at = raw.get("generated_at_utc") if isinstance(raw, Mapping) else None
        try:
            acceptance_date = datetime.fromisoformat(str(generated_at)).date().isoformat()
        except (TypeError, ValueError) as exc:
            raise SourceDiscoveryError(
                "Reviewed model input has no reproducible acceptance timestamp."
            ) from exc
        if acceptance_date != model_input["knowledge_date"]:
            raise SourceDiscoveryError(
                "Reviewed model-input knowledge date is backdated or differs from its accepted audit."
            )
        fragment_match = re.fullmatch(r"rows\[field=([^\]]+)\]", fragment)
        rows = raw.get("rows") if isinstance(raw, Mapping) else None
        if fragment_match is None or not isinstance(rows, list):
            raise SourceDiscoveryError(
                "Reviewed model input has an unsupported or missing audit-row locator."
            )
        matching_rows = [
            row
            for row in rows
            if isinstance(row, Mapping) and row.get("field") == fragment_match.group(1)
        ]
        if len(matching_rows) != 1:
            raise SourceDiscoveryError(
                "Reviewed model-input audit-row locator is missing or ambiguous."
            )
        audit_row = matching_rows[0]
        if (
            audit_row.get("text_excerpt") != model_input["interpretation"]
            or audit_row.get("is_clean_visible") is not True
            or audit_row.get("classification") != "clean_visible_ui"
        ):
            raise SourceDiscoveryError(
                "Reviewed model interpretation is not the uniquely accepted audit-row text."
            )


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON key {key!r}.")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"Invalid JSON constant {value!r}.")
