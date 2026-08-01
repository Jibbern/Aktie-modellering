"""Deterministic readable identities for longitudinal company memory.

Readable identities are the authority.  The compact digest is only an index
checksum and must never be used by itself for reconciliation or selection.
"""
from __future__ import annotations

import base64
import hashlib
import json
import re
import unicodedata
from collections.abc import Iterable, Mapping, Sequence
from typing import Any
from urllib.parse import quote


IDENTITY_CONTRACT_VERSION = "1"
_KEY_RE = re.compile(r"^[a-z][a-z0-9-]*$")
_KIND_RE = re.compile(r"^[a-z][a-z0-9-]*$")
_SEMANTIC_ID_RE = re.compile(r"^[a-z][a-z0-9-]*(?::[a-z0-9][a-z0-9-]*)+@[1-9][0-9]*$")


class IdentityError(ValueError):
    """Raised when an identity component is ambiguous or non-canonical."""


def normalize_text(value: Any) -> str:
    """Return the NFC-normalized string representation of an identity value."""

    text = unicodedata.normalize("NFC", str(value))
    if not text or text != text.strip():
        raise IdentityError(f"Identity values must be non-empty and trimmed, received {value!r}.")
    return text


def canonical_company_id(value: Any) -> str:
    company_id = normalize_text(value).upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9.-]{0,31}", company_id):
        raise IdentityError(f"Invalid company identifier {value!r}.")
    return company_id


def canonical_slug(value: Any) -> str:
    slug = normalize_text(value).lower()
    if not re.fullmatch(r"[a-z][a-z0-9-]*", slug):
        raise IdentityError(f"Expected a lowercase kebab-case slug, received {value!r}.")
    return slug


def validate_semantic_id(value: Any, *, prefix: str | None = None) -> str:
    semantic_id = normalize_text(value)
    if not _SEMANTIC_ID_RE.fullmatch(semantic_id):
        raise IdentityError(f"Invalid versioned semantic identifier {value!r}.")
    if prefix is not None and not semantic_id.startswith(f"{prefix}:"):
        raise IdentityError(f"Expected {prefix!r} identifier, received {semantic_id!r}.")
    return semantic_id


def build_identity(kind: str, components: Sequence[tuple[str, Any]]) -> str:
    """Build a fixed-order v1 identity with RFC 3986 encoded values."""

    kind = canonical_slug(kind)
    if not _KIND_RE.fullmatch(kind):  # defensive if slug rules evolve
        raise IdentityError(f"Invalid identity kind {kind!r}.")
    seen: set[str] = set()
    encoded: list[str] = []
    for key, raw_value in components:
        if not _KEY_RE.fullmatch(str(key)):
            raise IdentityError(f"Invalid identity component key {key!r}.")
        if key in seen:
            raise IdentityError(f"Duplicate identity component key {key!r}.")
        seen.add(key)
        value = normalize_text(raw_value)
        encoded.append(f"{key}={quote(value, safe='-._~')}")
    if not encoded:
        raise IdentityError("An identity requires at least one component.")
    return f"{kind}:v{IDENTITY_CONTRACT_VERSION}|" + "|".join(encoded)


def identity_digest(identity: str) -> str:
    """Return the required 160-bit compact SHA-256 index checksum."""

    readable = normalize_text(identity)
    raw = hashlib.sha256(readable.encode("utf-8")).digest()[:20]
    compact = base64.b32encode(raw).decode("ascii").lower().rstrip("=")
    return f"sha256-160:{compact}"


def sorted_reference_digest(values: Iterable[str]) -> str:
    """Digest a set-like collection independently of source order."""

    normalized = sorted({normalize_text(value) for value in values})
    canonical = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))
    return identity_digest(canonical)


def source_document_identity(
    *,
    company_id: str,
    publisher_id: str,
    document_type: str,
    publication_date: str,
    document_key: str,
    revision: int = 1,
) -> str:
    if revision < 1:
        raise IdentityError("Source-document revision must be positive.")
    return build_identity(
        "doc",
        (
            ("co", canonical_company_id(company_id)),
            ("publisher", canonical_slug(publisher_id)),
            ("type", canonical_slug(document_type)),
            ("pub", publication_date),
            ("key", canonical_slug(document_key)),
            ("rev", revision),
        ),
    )


def evidence_occurrence_identity(
    *,
    company_id: str,
    document_key: str,
    document_revision: int,
    locator_kind: str,
    locator_key: str,
    ordinal: int = 1,
) -> str:
    if document_revision < 1 or ordinal < 1:
        raise IdentityError("Evidence revisions and ordinals must be positive.")
    return build_identity(
        "occ",
        (
            ("co", canonical_company_id(company_id)),
            ("doc", canonical_slug(document_key)),
            ("rev", document_revision),
            ("loc", f"{canonical_slug(locator_kind)}:{normalize_text(locator_key)}"),
            ("n", ordinal),
        ),
    )


def dimension_set_identity(members: Mapping[str, str] | Iterable[tuple[str, str]]) -> str:
    pairs = members.items() if isinstance(members, Mapping) else members
    normalized: list[tuple[str, str]] = []
    seen_dimensions: set[str] = set()
    for dimension_id, member_id in pairs:
        dimension_id = validate_semantic_id(dimension_id, prefix="dimension")
        member_id = validate_semantic_id(member_id, prefix="member")
        if dimension_id in seen_dimensions:
            raise IdentityError(f"Dimension set contains multiple members for {dimension_id!r}.")
        seen_dimensions.add(dimension_id)
        normalized.append((dimension_id, member_id))
    if not normalized:
        raise IdentityError("Dimension sets must contain an explicit member, including total company.")
    canonical_members = ";".join(
        f"{dimension_id}={member_id}" for dimension_id, member_id in sorted(normalized)
    )
    return build_identity("dimset", (("members", canonical_members),))


def numerical_business_key(
    *,
    company_id: str,
    metric_id: str,
    definition_id: str,
    basis_id: str,
    period_id: str,
    dimension_set_id: str,
    unit_id: str,
    currency: str | None,
) -> str:
    return build_identity(
        "business-fact",
        (
            ("co", canonical_company_id(company_id)),
            ("metric", validate_semantic_id(metric_id, prefix="metric")),
            ("def", validate_semantic_id(definition_id, prefix="definition")),
            ("basis", validate_semantic_id(basis_id, prefix="basis")),
            ("period", period_id),
            ("dims", dimension_set_id),
            ("unit", validate_semantic_id(unit_id, prefix="unit")),
            ("ccy", currency or "na"),
        ),
    )


def numerical_fact_identity(*, provenance_key: str, **business: str | None) -> str:
    business_key = numerical_business_key(**business)
    return build_identity("fact", (("business", business_key), ("prov", provenance_key)))


def guidance_series_identity(
    *,
    company_id: str,
    metric_id: str,
    definition_id: str,
    basis_id: str,
    horizon_period_id: str,
    dimension_set_id: str,
    unit_id: str,
    currency: str | None,
) -> str:
    return build_identity(
        "gseries",
        (
            ("co", canonical_company_id(company_id)),
            ("metric", validate_semantic_id(metric_id, prefix="metric")),
            ("def", validate_semantic_id(definition_id, prefix="definition")),
            ("basis", validate_semantic_id(basis_id, prefix="basis")),
            ("horizon", horizon_period_id),
            ("dims", dimension_set_id),
            ("unit", validate_semantic_id(unit_id, prefix="unit")),
            ("ccy", currency or "na"),
        ),
    )


def guidance_version_identity(*, guidance_series_id: str, occurrence_id: str) -> str:
    return build_identity("gver", (("series", guidance_series_id), ("occ", occurrence_id)))


def promise_identity(*, company_id: str, subject_id: str, program_id: str | None, origin_occurrence_id: str) -> str:
    return build_identity(
        "promise",
        (
            ("co", canonical_company_id(company_id)),
            ("subject", normalize_text(subject_id)),
            ("program", program_id or "na"),
            ("origin", origin_occurrence_id),
        ),
    )


def promise_version_identity(*, promise_id: str, occurrence_id: str) -> str:
    return build_identity("pver", (("promise", promise_id), ("occ", occurrence_id)))


def management_statement_identity(
    *, company_id: str, statement_kind: str, topic_id: str, period_id: str, speaker_id: str, occurrence_id: str
) -> str:
    return build_identity(
        "statement",
        (
            ("co", canonical_company_id(company_id)),
            ("kind", canonical_slug(statement_kind)),
            ("topic", normalize_text(topic_id)),
            ("period", period_id),
            ("speaker", normalize_text(speaker_id)),
            ("occ", occurrence_id),
        ),
    )


def company_event_identity(
    *, company_id: str, event_type: str, event_subject_id: str, event_stage: str, effective_period_id: str, occurrence_id: str
) -> str:
    return build_identity(
        "event",
        (
            ("co", canonical_company_id(company_id)),
            ("type", canonical_slug(event_type)),
            ("subject", normalize_text(event_subject_id)),
            ("stage", canonical_slug(event_stage)),
            ("effective", effective_period_id),
            ("occ", occurrence_id),
        ),
    )


def model_interpretation_identity(
    *,
    company_id: str,
    interpretation_key: str,
    as_of_period_id: str,
    method_id: str,
    producer_id: str,
    input_record_ids: Iterable[str],
    revision: int,
) -> str:
    if revision < 1:
        raise IdentityError("Model-interpretation revision must be positive.")
    return build_identity(
        "interp",
        (
            ("co", canonical_company_id(company_id)),
            ("key", normalize_text(interpretation_key)),
            ("asof", as_of_period_id),
            ("method", validate_semantic_id(method_id, prefix="method")),
            ("producer", normalize_text(producer_id)),
            ("inputs", sorted_reference_digest(input_record_ids)),
            ("rev", revision),
        ),
    )


def availability_observation_identity(
    *, company_id: str, business_key: str, availability_state: str, occurrence_id: str
) -> str:
    return build_identity(
        "availability",
        (
            ("co", canonical_company_id(company_id)),
            ("business", business_key),
            ("state", canonical_slug(availability_state)),
            ("occ", occurrence_id),
        ),
    )


def relation_identity(*, relation_type: str, from_record_id: str, to_record_id: str, rule_id: str) -> str:
    return build_identity(
        "relation",
        (
            ("type", canonical_slug(relation_type)),
            ("from", from_record_id),
            ("to", to_record_id),
            ("rule", validate_semantic_id(rule_id, prefix="rule")),
        ),
    )


def canonical_resolution_identity(
    *, record_type: str, business_key: str, as_of_date: str, policy_id: str, candidate_record_ids: Iterable[str]
) -> str:
    return build_identity(
        "resolution",
        (
            ("type", canonical_slug(record_type)),
            ("business", business_key),
            ("asof", as_of_date),
            ("policy", validate_semantic_id(policy_id, prefix="policy")),
            ("candidates", sorted_reference_digest(candidate_record_ids)),
        ),
    )


def change_observation_identity(
    *, company_id: str, change_kind: str, from_record_id: str, to_record_id: str, rule_id: str
) -> str:
    return build_identity(
        "change",
        (
            ("co", canonical_company_id(company_id)),
            ("kind", canonical_slug(change_kind)),
            ("from", from_record_id),
            ("to", to_record_id),
            ("rule", validate_semantic_id(rule_id, prefix="rule")),
        ),
    )


def assert_identity_digest_pairs(pairs: Iterable[tuple[str, str]]) -> None:
    """Fail when a digest is wrong or maps to more than one readable identity."""

    by_digest: dict[str, str] = {}
    for readable, digest in pairs:
        expected = identity_digest(readable)
        if digest != expected:
            raise IdentityError(f"Digest {digest!r} does not match readable identity {readable!r}.")
        prior = by_digest.get(digest)
        if prior is not None and prior != readable:
            raise IdentityError(f"Identity digest collision between {prior!r} and {readable!r}.")
        by_digest[digest] = readable
