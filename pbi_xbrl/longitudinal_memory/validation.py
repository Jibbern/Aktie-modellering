"""Closed-schema and semantic validation for longitudinal memory packages."""
from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping

from pbi_xbrl.json_schema_validation import load_json_strict, validate_json_schema
from pbi_xbrl.new_ticker_issue_ledger import build_canonical_issue_ledger

from .calendar_rules import (
    FiscalCalendarRuleError,
    fiscal_calendar_rule,
    validate_period_for_calendar_rule,
)
from .changes import (
    IncomparablePeriodError,
    IncompatibleFactError,
    compare_periods,
    validate_percentage_point_rule_binding,
)
from .identity import (
    IdentityError,
    assert_identity_digest_pairs,
    availability_observation_identity,
    canonical_company_id,
    canonical_resolution_identity,
    change_observation_identity,
    company_event_identity,
    dimension_set_identity,
    evidence_occurrence_identity,
    guidance_series_identity,
    guidance_version_identity,
    identity_digest,
    management_statement_identity,
    model_interpretation_identity,
    numerical_business_key,
    numerical_fact_identity,
    promise_identity,
    promise_version_identity,
    relation_identity,
    source_document_identity,
    validate_semantic_id,
)
from .types import DomainValidationError, canonical_decimal
from .reconciliation import (
    ELIGIBLE_REVIEW_STATES,
    POLICY_ASSERTION_TYPES,
    POLICY_RECORD_TYPES,
    ReconciliationError,
    business_key,
    classify_promise_change,
    evidence_chain_eligible,
    infer_assertion_relations,
    resolve_observations,
)


DEFAULT_SCHEMA_PATH = Path(__file__).resolve().parents[2] / "docs" / "longitudinal_company_memory.schema.json"
BLOCKING_SEVERITIES = frozenset({"P0", "P1"})
RECORD_POLICY_IDS = {
    "NumericalFact": "policy:core:reported-numerical@1",
    "GuidanceVersion": "policy:core:guidance@1",
    "ManagementStatement": "policy:core:management-explanation@1",
    "CompanyEvent": "policy:core:company-event@1",
    "ModelInterpretation": "policy:core:model-interpretation@1",
}
HISTORY_RELATION_TYPES = frozenset({"corrects", "supersedes"})
FISCAL_ECONOMIC_PERIOD_TYPES = frozenset({"quarter", "ytd", "annual", "ttm"})
MANAGEMENT_SOURCE_AUTHORITIES = frozenset(
    {"audited-filing", "filed-exhibit", "company-release", "company-presentation", "company-transcript"}
)


class LongitudinalValidationError(ValueError):
    """Raised when a package violates a closed or lossless contract."""


@dataclass(frozen=True)
class ValidationIssue:
    severity: str
    rule_id: str
    normalized_path: str
    message: str
    suggested_action: str
    source_ref: str = ""
    evidence_key: str = ""
    affected_period: str = ""
    promotion_blocking: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "severity": self.severity,
            "rule_id": self.rule_id,
            "normalized_path": self.normalized_path,
            "message": self.message,
            "suggested_action": self.suggested_action,
            "source_ref": self.source_ref,
            "evidence_key": self.evidence_key,
            "affected_period": self.affected_period,
            "promotion_blocking": self.promotion_blocking or self.severity in BLOCKING_SEVERITIES,
        }


def _issue(
    severity: str,
    rule_id: str,
    path: str,
    message: str,
    action: str,
    *,
    source_ref: str = "",
    evidence_key: str = "",
    affected_period: str = "",
) -> ValidationIssue:
    return ValidationIssue(
        severity=severity,
        rule_id=rule_id,
        normalized_path=path,
        message=message,
        suggested_action=action,
        source_ref=source_ref,
        evidence_key=evidence_key,
        affected_period=affected_period,
        promotion_blocking=severity in BLOCKING_SEVERITIES,
    )


def load_package_strict(path: Path | str) -> dict[str, Any]:
    value = load_json_strict(path)
    if not isinstance(value, dict):
        raise LongitudinalValidationError("The longitudinal sidecar root must be a JSON object.")
    return value


def validate_package_schema(
    package: Mapping[str, Any], *, schema_path: Path | str = DEFAULT_SCHEMA_PATH
) -> list[ValidationIssue]:
    schema = load_json_strict(schema_path)
    failures = validate_json_schema(package, schema)
    return [
        _issue(
            "P1",
            "longitudinal_schema_closed",
            path,
            f"{keyword}: {message}",
            "Correct the package or version the checked-in schema explicitly.",
        )
        for path, keyword, message in failures
    ]


def _record_entries(package: Mapping[str, Any]) -> Iterable[tuple[str, str, str, Mapping[str, Any]]]:
    for index, record in enumerate(package.get("source_documents", ())):
        yield f"$.source_documents[{index}]", str(record.get("source_document_id", "")), str(record.get("identity_digest", "")), record
    for index, record in enumerate(package.get("evidence_occurrences", ())):
        yield f"$.evidence_occurrences[{index}]", str(record.get("evidence_occurrence_id", "")), str(record.get("identity_digest", "")), record
    for collection in ("entities", "observations"):
        id_key = "entity_id" if collection == "entities" else "record_id"
        for index, record in enumerate(package.get(collection, ())):
            header = record.get("header", {})
            yield f"$.{collection}[{index}]", str(header.get(id_key, "")), str(header.get("identity_digest", "")), record
    for collection, id_key in (("relations", "relation_id"), ("resolutions", "resolution_id")):
        for index, record in enumerate(package.get(collection, ())):
            yield f"$.{collection}[{index}]", str(record.get(id_key, "")), str(record.get("identity_digest", "")), record


def _catalog_ids(package: Mapping[str, Any]) -> dict[str, set[str]]:
    catalog = package.get("catalog", {})
    mapping = {
        "metrics": "metric_id",
        "definitions": "definition_id",
        "bases": "basis_id",
        "units": "unit_id",
        "dimensions": "dimension_id",
        "dimension_members": "member_id",
        "dimension_sets": "dimension_set_id",
        "policies": "policy_id",
        "change_rules": "rule_id",
        "methods": "method_id",
    }
    return {
        collection: {str(row.get(id_key, "")) for row in catalog.get(collection, ())}
        for collection, id_key in mapping.items()
    }


def _identity_issue(path: str, actual: str, expected: str) -> ValidationIssue | None:
    if actual == expected:
        return None
    return _issue(
        "P1",
        "identity_immutable_payload",
        path,
        f"Readable identity {actual!r} does not match immutable payload identity {expected!r}.",
        "Create a new deterministic identity from the immutable fields; never mutate an existing identity payload.",
    )


def _validate_value_spec(value: Any, path: str) -> list[ValidationIssue]:
    if value is None or not isinstance(value, Mapping):
        return []
    issues: list[ValidationIssue] = []
    kind = value.get("kind")
    decimal_fields = {
        "exact": ("value",),
        "approximate": ("value", "tolerance"),
        "range": ("low", "high"),
        "bound": ("value",),
    }.get(str(kind), ())
    for field in decimal_fields:
        raw = value.get(field)
        if raw is None and kind == "approximate" and field == "tolerance":
            continue
        try:
            if canonical_decimal(str(raw)) != raw:
                raise DomainValidationError("decimal string is not in canonical non-exponent form")
        except (DomainValidationError, TypeError) as exc:
            issues.append(_issue("P1", "decimal_canonical", f"{path}.{field}", f"Invalid canonical decimal: {exc}.", "Use a finite canonical decimal string without exponent or redundant trailing zeroes."))
    if kind == "range":
        try:
            if Decimal(str(value.get("low"))) > Decimal(str(value.get("high"))):
                raise ValueError("range low exceeds high")
        except (ArithmeticError, ValueError) as exc:
            issues.append(_issue("P1", "value_range_order", path, f"Invalid range: {exc}.", "Correct the source-backed lower and upper bounds."))
    if kind == "approximate" and value.get("tolerance") is not None:
        try:
            if Decimal(str(value.get("tolerance"))) < 0:
                raise ValueError("tolerance is negative")
        except (ArithmeticError, ValueError) as exc:
            issues.append(_issue("P1", "approximation_tolerance", path, f"Invalid approximation: {exc}.", "Use a non-negative source-supplied tolerance or null."))
    return issues


def _relation_business_identity(record: Mapping[str, Any]) -> str:
    """Return the immutable assertion/series identity used by history relations."""

    payload = record.get("payload", {})
    kind = str(payload.get("kind", ""))
    if kind == "PromiseVersion":
        return str(payload.get("promise_id", ""))
    return business_key(record)


def _duplicates(rows: Iterable[Mapping[str, Any]], key: str) -> list[str]:
    counts = Counter(str(row.get(key, "")) for row in rows)
    return sorted(value for value, count in counts.items() if value and count > 1)


def _has_cycle(edges: Iterable[tuple[str, str]]) -> bool:
    graph: dict[str, set[str]] = defaultdict(set)
    for source, target in edges:
        graph[source].add(target)

    def visit(node: str, visiting: set[str], visited: set[str]) -> bool:
        if node in visiting:
            return True
        if node in visited:
            return False
        visiting.add(node)
        if any(visit(child, visiting, visited) for child in graph.get(node, ())):
            return True
        visiting.remove(node)
        visited.add(node)
        return False

    visited: set[str] = set()
    return any(visit(node, set(), visited) for node in sorted(graph))


def _is_fiscal_economic_period(period: Mapping[str, Any] | None) -> bool:
    return bool(period) and period.get("period_type") in FISCAL_ECONOMIC_PERIOD_TYPES


def validate_package_semantics(package: Mapping[str, Any]) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    mandatory_conditions: set[tuple[str, str, tuple[str, ...]]] = set()

    def require_review(rule_id: str, business: str, candidates: Iterable[str] = ()) -> None:
        mandatory_conditions.add((rule_id, business, tuple(sorted(set(str(value) for value in candidates if value)))))

    def blocking(
        rule_id: str,
        path: str,
        message: str,
        action: str,
        *,
        business: str,
        candidates: Iterable[str] = (),
        affected_period: str = "",
    ) -> None:
        issues.append(_issue("P1", rule_id, path, message, action, affected_period=affected_period))
        require_review(rule_id, business, candidates)

    company_id = str(package.get("company_id", ""))
    try:
        if canonical_company_id(company_id) != company_id:
            raise IdentityError("Company ID is not canonical uppercase.")
    except IdentityError as exc:
        issues.append(_issue("P1", "company_identity", "$.company_id", str(exc), "Use the canonical uppercase company ID."))
    normalized_company = str(package.get("normalized_package_ref", {}).get("source_package_company_id", ""))
    if normalized_company != company_id:
        issues.append(_issue("P1", "normalized_snapshot_company", "$.normalized_package_ref.source_package_company_id", "Normalized snapshot company differs from the sidecar company.", "Link only a semantic snapshot for the same company."))
    try:
        knowledge_cutoff = date.fromisoformat(str(package.get("knowledge_cutoff")))
    except ValueError:
        knowledge_cutoff = date.min

    entries = list(_record_entries(package))
    identity_pairs = [(record_id, digest) for _, record_id, digest, _ in entries if record_id and digest]
    try:
        assert_identity_digest_pairs(identity_pairs)
    except IdentityError as exc:
        issues.append(_issue("P1", "identity_digest_integrity", "$", str(exc), "Repair the readable identity/digest pair; never select by digest alone."))

    grouped: dict[str, list[tuple[str, Mapping[str, Any]]]] = defaultdict(list)
    for path, record_id, _, record in entries:
        grouped[record_id].append((path, record))
    for record_id, rows in sorted(grouped.items()):
        if not record_id:
            continue
        canonical_payloads = {json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) for _, row in rows}
        if len(rows) > 1:
            detail = "different payloads" if len(canonical_payloads) > 1 else "duplicate records"
            issues.append(_issue("P1", "readable_identity_unique", rows[0][0], f"Readable identity {record_id!r} has {detail}.", "Keep exactly one immutable record per readable identity."))

    duplicate_collections = (
        ("fiscal_calendars", "calendar_id", "fiscal_calendar_identity_unique"),
        ("periods", "period_id", "period_identity_unique"),
        ("source_documents", "source_document_id", "source_document_identity_unique"),
        ("evidence_occurrences", "evidence_occurrence_id", "evidence_occurrence_identity_unique"),
        ("relations", "relation_id", "relation_identity_unique"),
        ("resolutions", "resolution_id", "resolution_identity_unique"),
        ("review_issues", "issue_id", "review_issue_identity_unique"),
    )
    for collection, id_key, rule_id in duplicate_collections:
        for duplicate in _duplicates(package.get(collection, ()), id_key):
            blocking(
                rule_id,
                f"$.{collection}",
                f"Duplicate {id_key} {duplicate!r} is not permitted.",
                "Keep exactly one row for every immutable identity; never use last-row-wins indexing.",
                business=duplicate,
                candidates=(duplicate,),
            )
    for collection, id_key, rule_id in (
        ("entities", "entity_id", "entity_identity_unique"),
        ("observations", "record_id", "observation_identity_unique"),
    ):
        rows = [row.get("header", {}) for row in package.get(collection, ())]
        for duplicate in _duplicates(rows, id_key):
            blocking(
                rule_id,
                f"$.{collection}",
                f"Duplicate {id_key} {duplicate!r} is not permitted.",
                "Keep exactly one immutable typed record per identity.",
                business=duplicate,
                candidates=(duplicate,),
            )

    catalog_ids = _catalog_ids(package)
    catalog_key_names = {
        "metrics": "metric_id", "definitions": "definition_id", "bases": "basis_id", "units": "unit_id",
        "dimensions": "dimension_id", "dimension_members": "member_id", "dimension_sets": "dimension_set_id",
        "policies": "policy_id", "change_rules": "rule_id", "methods": "method_id",
    }
    for collection, values in catalog_ids.items():
        id_key = catalog_key_names[collection]
        raw_values = [str(row.get(id_key, "")) for row in package.get("catalog", {}).get(collection, ())]
        counts = Counter(raw_values)
        if "" in values:
            issues.append(_issue("P1", "catalog_identity_required", f"$.catalog.{collection}", "A catalog row has an empty identity.", "Supply a versioned catalog identity."))
        for duplicate in sorted(value for value, count in counts.items() if value and count > 1):
            rule_id = "dimension_set_identity_unique" if collection == "dimension_sets" else "catalog_identity_unique"
            blocking(
                rule_id,
                f"$.catalog.{collection}",
                f"Duplicate catalog identity {duplicate!r}.",
                "Version semantic changes and retain one row per identity.",
                business=duplicate,
                candidates=(duplicate,),
            )
    catalog_prefixes = {
        "metrics": "metric", "definitions": "definition", "bases": "basis", "units": "unit",
        "dimensions": "dimension", "dimension_members": "member", "policies": "policy",
        "change_rules": "rule", "methods": "method",
    }
    for collection, prefix in catalog_prefixes.items():
        id_key = catalog_key_names[collection]
        for index, row in enumerate(package.get("catalog", {}).get(collection, ())):
            try:
                validate_semantic_id(row.get(id_key, ""), prefix=prefix)
            except IdentityError as exc:
                issues.append(_issue("P1", "catalog_identity_version", f"$.catalog.{collection}[{index}].{id_key}", str(exc), "Use the required independently versioned semantic-ID namespace."))
            supersedes_id = row.get("supersedes_id")
            if supersedes_id is not None and supersedes_id not in catalog_ids[collection]:
                issues.append(_issue("P1", "catalog_supersedes_reference", f"$.catalog.{collection}[{index}].supersedes_id", f"Unknown superseded catalog identity {supersedes_id!r}.", "Retain and reference the exact historical semantic identity."))
    for index, unit in enumerate(package.get("catalog", {}).get("units", ())):
        issues.extend(_validate_value_spec({"kind": "exact", "value": unit.get("scale")}, f"$.catalog.units[{index}].scale"))

    member_rows = {str(row.get("member_id")): row for row in package.get("catalog", {}).get("dimension_members", ())}
    for index, dimension_set in enumerate(package.get("catalog", {}).get("dimension_sets", ())):
        members = dimension_set.get("members", ())
        axes = [str(row.get("dimension_id", "")) for row in members]
        if not members:
            issues.append(_issue("P1", "dimension_total_explicit", f"$.catalog.dimension_sets[{index}].members", "A dimension set cannot be empty; total company must be explicit.", "Add an explicit total-company member."))
        if axes != sorted(axes) or len(axes) != len(set(axes)):
            issues.append(_issue("P1", "dimension_set_canonical", f"$.catalog.dimension_sets[{index}].members", "Dimension members must be sorted by dimension ID with at most one member per axis.", "Sort the members and remove axis ambiguity."))
        if members:
            try:
                expected_dimension_set_id = dimension_set_identity((str(row.get("dimension_id", "")), str(row.get("member_id", ""))) for row in members)
                mismatch = _identity_issue(f"$.catalog.dimension_sets[{index}].dimension_set_id", str(dimension_set.get("dimension_set_id", "")), expected_dimension_set_id)
                if mismatch:
                    issues.append(mismatch)
            except IdentityError as exc:
                issues.append(_issue("P1", "dimension_set_identity", f"$.catalog.dimension_sets[{index}]", str(exc), "Correct and deterministically identify the complete dimension-member mapping."))
        for member_index, member_ref in enumerate(members):
            member_id = str(member_ref.get("member_id", ""))
            dimension_id = str(member_ref.get("dimension_id", ""))
            member = member_rows.get(member_id)
            if member is None or str(member.get("dimension_id", "")) != dimension_id:
                issues.append(_issue("P1", "dimension_member_reference", f"$.catalog.dimension_sets[{index}].members[{member_index}]", f"Member {member_id!r} does not resolve on dimension {dimension_id!r}.", "Use a catalogued member on the same versioned dimension axis."))
        if members and not any(member_rows.get(str(ref.get("member_id", "")), {}).get("scope") == "company" for ref in members):
            issues.append(_issue("P1", "dimension_company_scope_explicit", f"$.catalog.dimension_sets[{index}].members", "Dimension set omits an explicit company-scope member.", "Add the explicit total-company scope rather than relying on an empty or implicit company dimension."))

    documents = {str(row.get("source_document_id")): row for row in package.get("source_documents", ())}
    occurrences = {str(row.get("evidence_occurrence_id")): row for row in package.get("evidence_occurrences", ())}
    for index, document in enumerate(package.get("source_documents", ())):
        path = f"$.source_documents[{index}]"
        if str(document.get("company_id", "")) != company_id:
            issues.append(_issue("P1", "source_company_scope", f"{path}.company_id", "Source document company differs from the package company.", "Keep source documents company-scoped."))
        try:
            expected = source_document_identity(
                company_id=str(document.get("company_id", "")),
                publisher_id=str(document.get("publisher_id", "")),
                document_type=str(document.get("document_type", "")),
                publication_date=str(document.get("publication_date", "")),
                document_key=str(document.get("document_key", "")),
                revision=int(document.get("revision", 0)),
            )
            mismatch = _identity_issue(f"{path}.source_document_id", str(document.get("source_document_id", "")), expected)
            if mismatch:
                issues.append(mismatch)
        except (IdentityError, TypeError, ValueError) as exc:
            issues.append(_issue("P1", "source_document_identity", path, str(exc), "Correct the immutable source-document identity fields."))
        try:
            if date.fromisoformat(str(document.get("publication_date"))) > knowledge_cutoff:
                issues.append(_issue("P1", "source_after_knowledge_cutoff", f"{path}.publication_date", "Source publication is after the package knowledge cutoff.", "Exclude future knowledge or advance the explicit cutoff."))
        except ValueError:
            pass
        origin_document_id = document.get("origin_document_id")
        if origin_document_id is not None and origin_document_id not in documents:
            blocking("source_origin_reference", f"{path}.origin_document_id", "Source origin document does not resolve.", "Retain the original document so mirrors are not treated as independent corroboration.", business=str(document.get("source_document_id", "")), candidates=(str(document.get("source_document_id", "")), str(origin_document_id)))
        elif origin_document_id is not None and str(origin_document_id) != str(document.get("source_document_id", "")):
            try:
                if date.fromisoformat(str(document.get("publication_date"))) < date.fromisoformat(str(documents[str(origin_document_id)].get("publication_date"))):
                    raise ValueError("source mirror predates its declared origin")
            except ValueError as exc:
                blocking("source_origin_time_direction", f"{path}.origin_document_id", str(exc), "Link a mirror only to an existing same-company source available no later than the mirror.", business=str(document.get("source_document_id", "")), candidates=(str(document.get("source_document_id", "")), str(origin_document_id)))
    source_origin_edges = [
        (str(row.get("source_document_id", "")), str(row.get("origin_document_id", "")))
        for row in package.get("source_documents", ())
        if row.get("origin_document_id") is not None and str(row.get("origin_document_id")) != str(row.get("source_document_id"))
    ]
    if _has_cycle(source_origin_edges):
        blocking("source_origin_cycle", "$.source_documents", "Source-document origin graph contains a cycle.", "Restore one acyclic mirror-to-origin provenance chain.", business="source-origin-graph", candidates=(value for edge in source_origin_edges for value in edge))
    for index, occurrence in enumerate(package.get("evidence_occurrences", ())):
        source_document_id = str(occurrence.get("source_document_id", ""))
        if source_document_id not in documents:
            issues.append(_issue("P1", "evidence_document_reference", f"$.evidence_occurrences[{index}].source_document_id", f"Unknown source document {source_document_id!r}.", "Add the lossless source-document record."))
            continue
        document = documents[source_document_id]
        if str(occurrence.get("company_id", "")) != company_id or str(document.get("company_id", "")) != company_id:
            issues.append(_issue("P1", "evidence_company_scope", f"$.evidence_occurrences[{index}].company_id", "Evidence occurrence company differs from its sidecar or source document.", "Keep every evidence occurrence company-scoped."))
        try:
            expected = evidence_occurrence_identity(
                company_id=str(occurrence.get("company_id", "")),
                document_key=str(document.get("document_key", "")),
                document_revision=int(document.get("revision", 0)),
                locator_kind=str(occurrence.get("locator_kind", "")),
                locator_key=str(occurrence.get("locator_key", "")),
                ordinal=int(occurrence.get("ordinal", 0)),
            )
            mismatch = _identity_issue(f"$.evidence_occurrences[{index}].evidence_occurrence_id", str(occurrence.get("evidence_occurrence_id", "")), expected)
            if mismatch:
                issues.append(mismatch)
        except (IdentityError, TypeError, ValueError) as exc:
            issues.append(_issue("P1", "evidence_occurrence_identity", f"$.evidence_occurrences[{index}]", str(exc), "Correct the immutable source locator and occurrence identity."))

    periods = {str(row.get("period_id")): row for row in package.get("periods", ())}
    calendars = {str(row.get("calendar_id")): row for row in package.get("fiscal_calendars", ())}

    def period_is_reconciled(period: Mapping[str, Any] | None) -> bool:
        if period is None or period.get("reconciliation_state") != "reconciled":
            return False
        calendar = calendars.get(str(period.get("calendar_id", "")))
        return bool(calendar) and calendar.get("reconciliation_state") == "reconciled"

    def enforce_typed_period_binding(
        *,
        header: Mapping[str, Any],
        typed_period_id: object,
        path: str,
        record_id: str,
        rule_id: str,
        label: str,
    ) -> None:
        """Bind an accepted typed payload period to its common observation header."""

        expected_period_id = str(typed_period_id or "")
        expected_period = periods.get(expected_period_id)
        effective_period_id = str(header.get("effective_period_id", ""))
        raw_fiscal_period_id = header.get("fiscal_period_id")
        fiscal_period_id = str(raw_fiscal_period_id) if raw_fiscal_period_id is not None else None
        failures: list[str] = []
        if expected_period is None:
            failures.append(f"typed {label} period {expected_period_id!r} does not resolve")
        else:
            if effective_period_id != expected_period_id:
                failures.append("effective_period_id differs from the typed period")
            if header.get("period_type") != expected_period.get("period_type"):
                failures.append("period_type differs from the typed period")
            if not period_is_reconciled(expected_period):
                failures.append("typed period or its fiscal calendar is not reconciled")
            if _is_fiscal_economic_period(expected_period) and fiscal_period_id != expected_period_id:
                failures.append("fiscal_period_id is missing or differs from the typed fiscal period")
        if failures:
            blocking(
                rule_id,
                f"{path}.header",
                f"{label} period binding is invalid: {'; '.join(failures)}.",
                "Bind the common header to the exact typed, reconciled economic period without a fiscal/effective fallback.",
                business=record_id,
                candidates=(record_id,),
                affected_period=expected_period_id,
            )

    for index, calendar in enumerate(package.get("fiscal_calendars", ())):
        calendar_id = str(calendar.get("calendar_id", ""))
        try:
            fiscal_calendar_rule(calendar)
        except FiscalCalendarRuleError as exc:
            blocking(
                "fiscal_calendar_rule",
                f"$.fiscal_calendars[{index}].calendar_rule_id",
                str(exc),
                "Use one supported reviewed versioned fiscal-calendar rule; never infer it from issuer or dates.",
                business=calendar_id,
                candidates=(calendar_id,),
            )
        if str(calendar.get("company_id", "")) != company_id:
            issues.append(_issue("P1", "fiscal_calendar_company", f"$.fiscal_calendars[{index}].company_id", "Fiscal calendar company differs from the sidecar company.", "Use the reconciled company calendar."))
        for occurrence_id in calendar.get("evidence_occurrence_ids", ()):
            if occurrence_id not in occurrences:
                blocking("fiscal_calendar_evidence", f"$.fiscal_calendars[{index}].evidence_occurrence_ids", f"Unknown calendar evidence {occurrence_id!r}.", "Attach source-backed fiscal-calendar evidence.", business=calendar_id, candidates=(calendar_id,))
        calendar_evidence = {"header": {"company_id": company_id, "evidence_occurrence_ids": list(calendar.get("evidence_occurrence_ids", ()))}}
        if not evidence_chain_eligible(calendar_evidence, policy_id=None, occurrences=occurrences, documents=documents):
            blocking("fiscal_calendar_evidence_eligibility", f"$.fiscal_calendars[{index}].evidence_occurrence_ids", "Fiscal calendar lacks an accepted company-scoped evidence chain.", "Attach accepted source evidence before treating the calendar as reconciled.", business=calendar_id, candidates=(calendar_id,))
        if calendar.get("reconciliation_state") == "needs_review":
            require_review("fiscal_calendar_needs_review", calendar_id, (calendar_id,))
    for index, period in enumerate(package.get("periods", ())):
        path = f"$.periods[{index}]"
        period_id = str(period.get("period_id", ""))
        if str(period.get("company_id", "")) != company_id:
            issues.append(_issue("P1", "fiscal_period_company", f"{path}.company_id", "Fiscal period company differs from the sidecar company.", "Use the company-scoped reconciled period."))
        for occurrence_id in period.get("evidence_occurrence_ids", ()):
            if occurrence_id not in occurrences:
                blocking("fiscal_period_evidence", f"{path}.evidence_occurrence_ids", f"Unknown period evidence {occurrence_id!r}.", "Attach filing/source evidence for actual boundaries or the reconciled expected horizon.", business=period_id, candidates=(period_id,))
        period_evidence = {"header": {"company_id": company_id, "evidence_occurrence_ids": list(period.get("evidence_occurrence_ids", ()))}}
        if not evidence_chain_eligible(period_evidence, policy_id=None, occurrences=occurrences, documents=documents):
            blocking("fiscal_period_evidence_eligibility", f"{path}.evidence_occurrence_ids", "Fiscal period lacks an accepted company-scoped evidence chain.", "Attach accepted filing/source evidence before treating the period as reconciled.", business=period_id, candidates=(period_id,))
        if period.get("reconciliation_state") == "needs_review":
            require_review("fiscal_period_needs_review", period_id, (period_id,))
        calendar_id = str(period.get("calendar_id", ""))
        if calendar_id not in calendars:
            blocking("fiscal_calendar_reference", f"{path}.calendar_id", f"Unknown fiscal calendar {calendar_id!r}.", "Reconcile the filing-backed period with an explicit calendar.", business=period_id, candidates=(period_id,))
        else:
            try:
                validate_period_for_calendar_rule(period, calendars[calendar_id])
            except FiscalCalendarRuleError as exc:
                blocking(
                    "fiscal_period_calendar_rule",
                    path,
                    str(exc),
                    "Correct the source-backed period or its explicit reviewed calendar rule.",
                    business=period_id,
                    candidates=(period_id, calendar_id),
                    affected_period=period_id,
                )
        try:
            start = date.fromisoformat(str(period.get("start_date")))
            end = date.fromisoformat(str(period.get("end_date")))
            actual_days = (end - start).days + 1
            if actual_days < 1 or actual_days != int(period.get("day_count", 0)):
                raise ValueError("day_count does not match inclusive source-backed dates")
            weeks = period.get("week_count")
            if weeks is not None and actual_days != int(weeks) * 7:
                raise ValueError("week_count does not match day_count")
            if period.get("period_type") == "annual" and weeks is not None and bool(period.get("is_53_week_year")) != (int(weeks) == 53):
                raise ValueError("annual 52/53-week flag does not match week_count")
            if period.get("period_type") == "quarter" and period.get("fiscal_quarter") not in {1, 2, 3, 4}:
                raise ValueError("quarter period has ambiguous fiscal quarter")
            if period.get("period_type") == "annual" and period.get("fiscal_quarter") is not None:
                raise ValueError("annual period cannot claim a fiscal quarter")
        except (TypeError, ValueError) as exc:
            blocking("fiscal_period_duration", path, f"Unsafe fiscal-period duration: {exc}.", "Correct start/end/duration from filing or source evidence.", business=period_id, candidates=(period_id,), affected_period=period_id)

    by_calendar: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for period in package.get("periods", ()):
        if period.get("period_type") == "quarter":
            by_calendar[str(period.get("calendar_id", ""))].append(period)
    for calendar_id, rows in by_calendar.items():
        ordered = sorted(rows, key=lambda row: (str(row.get("start_date", "")), str(row.get("period_id", ""))))
        for left, right in zip(ordered, ordered[1:]):
            if str(left.get("end_date", "")) >= str(right.get("start_date", "")):
                blocking("fiscal_period_overlap", "$.periods", f"Fiscal periods {left.get('period_id')!r} and {right.get('period_id')!r} overlap on {calendar_id!r}.", "Resolve the actual filing-backed period boundaries before comparisons.", business=calendar_id, candidates=(str(left.get("period_id", "")), str(right.get("period_id", ""))))
            left_ordinal, right_ordinal = left.get("fiscal_ordinal"), right.get("fiscal_ordinal")
            complete = calendars.get(calendar_id, {}).get("coverage_state") == "complete"
            if complete and isinstance(left_ordinal, int) and isinstance(right_ordinal, int) and right_ordinal != left_ordinal + 1:
                blocking("fiscal_period_gap", "$.periods", f"Complete fiscal calendar {calendar_id!r} has an ordinal gap.", "Add or correct filing-backed fiscal periods.", business=calendar_id, candidates=(str(left.get("period_id", "")), str(right.get("period_id", ""))))

    entity_ids = {str(row.get("header", {}).get("entity_id")): row for row in package.get("entities", ())}
    observation_ids = {str(row.get("header", {}).get("record_id")): row for row in package.get("observations", ())}
    all_record_ids = set(entity_ids) | set(observation_ids)
    for collection in ("entities", "observations"):
        for index, record in enumerate(package.get(collection, ())):
            header = record.get("header", {})
            payload = record.get("payload", {})
            path = f"$.{collection}[{index}]"
            if str(header.get("company_id", "")) != company_id:
                issues.append(_issue("P1", "record_company_scope", f"{path}.header.company_id", "Record company differs from the package company.", "Keep each sidecar company-scoped."))
            for occurrence_id in header.get("evidence_occurrence_ids", ()):
                if occurrence_id not in occurrences:
                    issues.append(_issue("P1", "record_evidence_reference", f"{path}.header.evidence_occurrence_ids", f"Unknown evidence occurrence {occurrence_id!r}.", "Add or correct the immutable evidence occurrence."))
            if collection == "observations":
                dimension_set_id = str(header.get("dimension_set_id", ""))
                if dimension_set_id not in catalog_ids["dimension_sets"]:
                    issues.append(_issue("P1", "record_dimension_reference", f"{path}.header.dimension_set_id", f"Unknown dimension set {dimension_set_id!r}.", "Use a deterministic catalogued dimension set."))
                if payload.get("kind") != header.get("record_type"):
                    issues.append(_issue("P1", "typed_union_discriminator", path, "Observation header and payload discriminators differ.", "Use one closed typed-record variant."))
                if header.get("assertion_mode") in {"reported", "guided", "stated"} and not header.get("evidence_occurrence_ids"):
                    issues.append(_issue("P1", "source_backed_assertion", path, "A reported, guided or stated observation has no evidence occurrence.", "Attach immutable source evidence."))
                for key, collection_name in (("metric_id", "metrics"), ("definition_id", "definitions"), ("basis_id", "bases"), ("unit_id", "units")):
                    if key in payload and str(payload.get(key)) not in catalog_ids[collection_name]:
                        issues.append(_issue("P1", "catalog_reference", f"{path}.payload.{key}", f"Unknown versioned catalog ID {payload.get(key)!r}.", "Add the semantic definition or correct the record reference."))
                raw_fiscal_period_id = header.get("fiscal_period_id")
                fiscal_period_id = str(raw_fiscal_period_id) if raw_fiscal_period_id is not None else None
                if fiscal_period_id is not None and fiscal_period_id not in periods:
                    issues.append(_issue("P1", "record_period_reference", f"{path}.header.fiscal_period_id", f"Unknown fiscal period {fiscal_period_id!r}.", "Use a filing-backed period identity."))
                effective_period_id = str(header.get("effective_period_id", ""))
                if effective_period_id not in periods:
                    issues.append(_issue("P1", "record_effective_period_reference", f"{path}.header.effective_period_id", f"Unknown effective period {effective_period_id!r}.", "Use an explicit filing/source-backed or reconciled expected period."))
                try:
                    publication = header.get("publication_date")
                    knowledge = date.fromisoformat(str(header.get("knowledge_date")))
                    if publication is not None and date.fromisoformat(str(publication)) > knowledge:
                        issues.append(_issue("P1", "observation_knowledge_order", f"{path}.header", "Observation knowledge date precedes publication date.", "Correct publication and knowledge dates without backdating."))
                    if knowledge > knowledge_cutoff:
                        issues.append(_issue("P1", "observation_after_knowledge_cutoff", f"{path}.header.knowledge_date", "Observation knowledge is after the package cutoff.", "Exclude future knowledge or advance the explicit cutoff."))
                except ValueError:
                    pass

                evidence_ids = list(header.get("evidence_occurrence_ids", ()))
                record_id = str(header.get("record_id", ""))
                kind = str(payload.get("kind", ""))
                accepted_record = header.get("review_state") in ELIGIBLE_REVIEW_STATES
                if accepted_record and not evidence_chain_eligible(
                    record,
                    policy_id=RECORD_POLICY_IDS.get(kind),
                    occurrences=occurrences,
                    documents=documents,
                    allowed_authority_classes=MANAGEMENT_SOURCE_AUTHORITIES if kind in {"PromiseVersion", "AvailabilityObservation"} else None,
                ):
                    blocking(
                        "accepted_record_source_ineligible",
                        f"{path}.header.evidence_occurrence_ids",
                        "An accepted observation has a missing, rejected, blocker-review, cross-company, or assertion-ineligible source/evidence chain.",
                        "Reject the observation or repair and review its complete occurrence-to-document lineage.",
                        business=record_id,
                        candidates=(record_id,),
                    )
                if accepted_record:
                    evidence_documents = [
                        documents.get(str(occurrences.get(str(occurrence_id), {}).get("source_document_id", "")))
                        for occurrence_id in evidence_ids
                    ]
                    try:
                        source_publications = [date.fromisoformat(str(row.get("publication_date"))) for row in evidence_documents if row is not None]
                        knowledge_date = date.fromisoformat(str(header.get("knowledge_date")))
                        if not source_publications or any(publication > knowledge_date for publication in source_publications):
                            raise ValueError("observation knowledge predates or omits its evidence publication")
                        if header.get("assertion_mode") != "derived" and date.fromisoformat(str(header.get("publication_date"))) != max(source_publications):
                            raise ValueError("observation publication_date differs from its latest immutable evidence document")
                    except ValueError as exc:
                        blocking(
                            "observation_source_time_lineage",
                            f"{path}.header.publication_date",
                            str(exc),
                            "Align publication and knowledge dates with the immutable evidence-document chain.",
                            business=record_id,
                            candidates=(record_id,),
                        )
                if accepted_record:
                    referenced_period_ids = sorted({effective_period_id, *((fiscal_period_id,) if fiscal_period_id is not None else ())})
                    for semantic_period_id in referenced_period_ids:
                        semantic_period = periods.get(semantic_period_id)
                        if semantic_period is None:
                            require_review("accepted_record_period_invalid", record_id, (record_id,))
                            continue
                        if semantic_period_id == effective_period_id and header.get("period_type") != semantic_period.get("period_type"):
                            blocking(
                                "accepted_record_period_type",
                                f"{path}.header.period_type",
                                "Accepted observation period_type disagrees with a referenced fiscal/effective period.",
                                "Use the exact source-backed period type; do not reinterpret the observation header.",
                                business=record_id,
                                candidates=(record_id,),
                                affected_period=semantic_period_id,
                            )
                        if not period_is_reconciled(semantic_period):
                            blocking(
                                "accepted_record_period_needs_review",
                                f"{path}.header.fiscal_period_id",
                                "Accepted observation depends on an ambiguous or unreconciled fiscal period/calendar.",
                                "Reconcile the source-backed calendar and period before accepting the observation.",
                                business=record_id,
                                candidates=(record_id,),
                                affected_period=semantic_period_id,
                            )
                expected: str | None = None
                try:
                    if kind == "NumericalFact":
                        issues.extend(_validate_value_spec(payload.get("value"), f"{path}.payload.value"))
                        effective_period = periods.get(effective_period_id)
                        if accepted_record:
                            enforce_typed_period_binding(
                                header=header,
                                typed_period_id=effective_period_id,
                                path=path,
                                record_id=record_id,
                                rule_id="numerical_fact_period_binding",
                                label="NumericalFact economic",
                            )
                        if _is_fiscal_economic_period(effective_period) or fiscal_period_id is None:
                            period_id = effective_period_id
                        else:
                            period_id = fiscal_period_id
                        expected_business = numerical_business_key(
                            company_id=company_id, metric_id=str(payload.get("metric_id", "")),
                            definition_id=str(payload.get("definition_id", "")), basis_id=str(payload.get("basis_id", "")),
                            period_id=period_id, dimension_set_id=dimension_set_id, unit_id=str(payload.get("unit_id", "")),
                            currency=payload.get("currency"),
                        )
                        if payload.get("business_key") != expected_business:
                            issues.append(_issue("P1", "numerical_business_key", f"{path}.payload.business_key", "Numerical business key does not match metric/definition/basis/period/dimension/unit/currency semantics.", "Rebuild the deterministic business key without changing the value."))
                        if len(evidence_ids) != 1:
                            issues.append(_issue("P1", "fact_origin_cardinality", f"{path}.header.evidence_occurrence_ids", "A NumericalFact must be anchored to exactly one immutable evidence occurrence.", "Emit one lossless fact per evidence occurrence and reconcile them explicitly."))
                        elif evidence_ids:
                            expected = numerical_fact_identity(
                                provenance_key=evidence_ids[0], company_id=company_id, metric_id=str(payload.get("metric_id", "")),
                                definition_id=str(payload.get("definition_id", "")), basis_id=str(payload.get("basis_id", "")),
                                period_id=period_id, dimension_set_id=dimension_set_id, unit_id=str(payload.get("unit_id", "")),
                                currency=payload.get("currency"),
                            )
                    elif kind == "GuidanceVersion":
                        issues.extend(_validate_value_spec(payload.get("value"), f"{path}.payload.value"))
                        series_id = str(payload.get("guidance_series_id", ""))
                        series = entity_ids.get(series_id)
                        if not series or series.get("payload", {}).get("kind") != "GuidanceSeries":
                            issues.append(_issue("P1", "guidance_series_reference", f"{path}.payload.guidance_series_id", "GuidanceVersion does not resolve to a GuidanceSeries entity.", "Add or correct the immutable series entity."))
                        else:
                            series_header = series.get("header", {})
                            series_payload = series.get("payload", {})
                            binding_mismatches = []
                            if header.get("company_id") != series_header.get("company_id"):
                                binding_mismatches.append("company")
                            if header.get("subject_id") != series_id:
                                binding_mismatches.append("subject/series identity")
                            if header.get("dimension_set_id") != series_payload.get("dimension_set_id"):
                                binding_mismatches.append("dimension set")
                            if accepted_record and binding_mismatches:
                                blocking(
                                    "guidance_series_binding",
                                    path,
                                    f"GuidanceVersion differs from its GuidanceSeries on: {', '.join(binding_mismatches)}.",
                                    "Use the immutable GuidanceSeries as the version's company, semantic-axis and dimension authority.",
                                    business=series_id,
                                    candidates=(record_id,),
                                )
                            if accepted_record:
                                enforce_typed_period_binding(
                                    header=header,
                                    typed_period_id=series_payload.get("horizon_period_id"),
                                    path=path,
                                    record_id=record_id,
                                    rule_id="guidance_period_binding",
                                    label="GuidanceVersion horizon",
                                )
                        if len(evidence_ids) == 1:
                            expected = guidance_version_identity(guidance_series_id=series_id, occurrence_id=evidence_ids[0])
                        else:
                            issues.append(_issue("P1", "guidance_version_origin", f"{path}.header.evidence_occurrence_ids", "GuidanceVersion requires exactly one origin occurrence.", "Emit separate evidence-anchored versions and reconcile repetitions."))
                    elif kind == "PromiseVersion":
                        for value_key in ("target", "baseline"):
                            issues.extend(_validate_value_spec(payload.get(value_key), f"{path}.payload.{value_key}"))
                        promise = entity_ids.get(str(payload.get("promise_id", "")))
                        if not promise or promise.get("payload", {}).get("kind") != "Promise":
                            issues.append(_issue("P1", "promise_reference", f"{path}.payload.promise_id", "PromiseVersion does not resolve to a Promise entity.", "Match exactly one historical Promise before creating a version."))
                        deadline = payload.get("deadline")
                        if accepted_record and isinstance(deadline, Mapping) and deadline.get("kind") == "period":
                            enforce_typed_period_binding(
                                header=header,
                                typed_period_id=deadline.get("value"),
                                path=path,
                                record_id=record_id,
                                rule_id="promise_deadline_period_binding",
                                label="PromiseVersion deadline",
                            )
                        if len(evidence_ids) == 1:
                            expected = promise_version_identity(promise_id=str(payload.get("promise_id", "")), occurrence_id=evidence_ids[0])
                        else:
                            issues.append(_issue("P1", "promise_version_origin", f"{path}.header.evidence_occurrence_ids", "PromiseVersion requires exactly one origin occurrence.", "Keep each later statement as one immutable evidence-anchored version."))
                    elif kind == "ManagementStatement":
                        if accepted_record:
                            enforce_typed_period_binding(
                                header=header,
                                typed_period_id=payload.get("statement_period_id"),
                                path=path,
                                record_id=record_id,
                                rule_id="management_statement_period_binding",
                                label="ManagementStatement payload",
                            )
                        if len(evidence_ids) == 1:
                            expected = management_statement_identity(company_id=company_id, statement_kind=str(payload.get("statement_kind", "")), topic_id=str(payload.get("topic_id", "")), period_id=str(payload.get("statement_period_id", "")), speaker_id=str(payload.get("speaker_id", "")), occurrence_id=evidence_ids[0])
                        else:
                            issues.append(_issue("P1", "statement_origin", f"{path}.header.evidence_occurrence_ids", "ManagementStatement requires exactly one occurrence.", "Keep repetitions as separate statement records."))
                    elif kind == "CompanyEvent":
                        precision = payload.get("effective_precision")
                        if precision == "month" and (payload.get("effective_month") is None or payload.get("effective_date") is not None):
                            issues.append(_issue("P1", "event_effective_precision", f"{path}.payload", "Month-precision event must have effective_month and no fabricated day.", "Retain the source precision exactly."))
                        if precision == "day" and (payload.get("effective_date") is None or payload.get("effective_month") is not None):
                            issues.append(_issue("P1", "event_effective_precision", f"{path}.payload", "Day-precision event must have effective_date and no competing month field.", "Retain the source precision exactly."))
                        if accepted_record and fiscal_period_id is not None:
                            effective_period = periods.get(effective_period_id)
                            fiscal_period = periods.get(fiscal_period_id)
                            event_period_failures: list[str] = []
                            if not _is_fiscal_economic_period(fiscal_period):
                                event_period_failures.append("fiscal_period_id does not identify a fiscal economic period")
                            elif effective_period is None:
                                event_period_failures.append("effective period does not resolve")
                            elif _is_fiscal_economic_period(effective_period):
                                if effective_period_id != fiscal_period_id:
                                    event_period_failures.append("fiscal effective and fiscal identities differ")
                            else:
                                same_context = (
                                    effective_period.get("company_id") == fiscal_period.get("company_id")
                                    and effective_period.get("calendar_id") == fiscal_period.get("calendar_id")
                                    and effective_period.get("fiscal_year") == fiscal_period.get("fiscal_year")
                                    and str(fiscal_period.get("start_date", "")) <= str(effective_period.get("start_date", ""))
                                    and str(effective_period.get("end_date", "")) <= str(fiscal_period.get("end_date", ""))
                                )
                                if not same_context:
                                    event_period_failures.append("effective period is outside the claimed fiscal context")
                            if event_period_failures:
                                blocking(
                                    "company_event_period_binding",
                                    f"{path}.header",
                                    f"CompanyEvent fiscal context is invalid: {'; '.join(event_period_failures)}.",
                                    "Retain an effective-only event or attach only a containing reconciled fiscal context.",
                                    business=record_id,
                                    candidates=(record_id,),
                                    affected_period=fiscal_period_id,
                                )
                        if len(evidence_ids) == 1:
                            expected = company_event_identity(company_id=company_id, event_type=str(payload.get("event_type", "")), event_subject_id=str(payload.get("event_subject_id", "")), event_stage=str(payload.get("event_stage", "")), effective_period_id=effective_period_id, occurrence_id=evidence_ids[0])
                        else:
                            issues.append(_issue("P1", "event_origin", f"{path}.header.evidence_occurrence_ids", "CompanyEvent requires exactly one occurrence.", "Keep later event stages as separate evidence-anchored records."))
                    elif kind == "ModelInterpretation":
                        if accepted_record:
                            enforce_typed_period_binding(
                                header=header,
                                typed_period_id=payload.get("as_of_period_id"),
                                path=path,
                                record_id=record_id,
                                rule_id="model_interpretation_period_binding",
                                label="ModelInterpretation as-of",
                            )
                        inputs = list(payload.get("input_record_ids", ()))
                        if inputs != sorted(set(inputs)):
                            issues.append(_issue("P1", "interpretation_input_set", f"{path}.payload.input_record_ids", "ModelInterpretation inputs must be unique and sorted.", "Canonicalize and retain the complete input record set."))
                        if any(input_id not in observation_ids for input_id in inputs):
                            issues.append(_issue("P1", "interpretation_input_reference", f"{path}.payload.input_record_ids", "ModelInterpretation input does not resolve to a typed observation.", "Retain every immutable interpretation input record."))
                        expected = model_interpretation_identity(company_id=company_id, interpretation_key=str(payload.get("interpretation_key", "")), as_of_period_id=str(payload.get("as_of_period_id", "")), method_id=str(payload.get("method_id", "")), producer_id=str(payload.get("producer_id", "")), input_record_ids=inputs, revision=int(payload.get("revision", 0)))
                    elif kind == "AvailabilityObservation":
                        if len(evidence_ids) == 1:
                            expected = availability_observation_identity(company_id=company_id, business_key=str(payload.get("business_key", "")), availability_state=str(payload.get("availability_state", "")), occurrence_id=evidence_ids[0])
                        else:
                            issues.append(_issue("P1", "availability_origin", f"{path}.header.evidence_occurrence_ids", "Explicit availability state requires exactly one source occurrence.", "Attach the explicit unavailable/not-disclosed/not-applicable evidence."))
                    elif kind == "ChangeObservation":
                        issues.extend(_validate_value_spec(payload.get("value"), f"{path}.payload.value"))
                        inputs = list(payload.get("input_record_ids", ()))
                        expected_inputs = sorted({str(payload.get("from_record_id", "")), str(payload.get("to_record_id", ""))})
                        if inputs != expected_inputs:
                            issues.append(_issue("P1", "change_input_set", f"{path}.payload.input_record_ids", "ChangeObservation inputs must equal its sorted from/to NumericalFact identities.", "Rebuild the derived record from exactly the two selected facts."))
                        if any(input_id not in observation_ids or observation_ids[input_id].get("payload", {}).get("kind") != "NumericalFact" for input_id in inputs):
                            issues.append(_issue("P1", "change_input_type", f"{path}.payload.input_record_ids", "ChangeObservation inputs must resolve to NumericalFact records.", "Derive only from compatible numerical facts."))
                        expected = change_observation_identity(company_id=company_id, change_kind=str(payload.get("change_kind", "")), from_record_id=str(payload.get("from_record_id", "")), to_record_id=str(payload.get("to_record_id", "")), rule_id=str(payload.get("rule_id", "")))
                    if expected is not None:
                        mismatch = _identity_issue(f"{path}.header.record_id", record_id, expected)
                        if mismatch:
                            issues.append(mismatch)
                except (IdentityError, TypeError, ValueError) as exc:
                    issues.append(_issue("P1", "observation_identity", path, str(exc), "Correct immutable typed-record fields and rebuild the readable identity."))

                if "unit_id" in payload:
                    unit = next((row for row in package.get("catalog", {}).get("units", ()) if row.get("unit_id") == payload.get("unit_id")), None)
                    if unit:
                        behavior = unit.get("currency_behavior")
                        currency = payload.get("currency")
                        if behavior == "required" and currency is None:
                            issues.append(_issue("P1", "currency_required", f"{path}.payload.currency", "Unit requires an explicit currency.", "Attach the ISO currency without converting the fact."))
                        if behavior == "forbidden" and currency is not None:
                            issues.append(_issue("P1", "currency_forbidden", f"{path}.payload.currency", "Unit forbids currency.", "Remove the incompatible currency or select the correct versioned unit."))
            elif payload.get("kind") == "GuidanceSeries":
                if header.get("entity_type") != payload.get("kind"):
                    issues.append(_issue("P1", "typed_union_discriminator", path, "Entity header and payload discriminators differ.", "Use one closed typed-entity variant."))
                dimension_set_id = str(payload.get("dimension_set_id", ""))
                if dimension_set_id not in catalog_ids["dimension_sets"]:
                    issues.append(_issue("P1", "record_dimension_reference", f"{path}.payload.dimension_set_id", f"Unknown dimension set {dimension_set_id!r}.", "Use a deterministic catalogued dimension set."))
                try:
                    expected = guidance_series_identity(company_id=company_id, metric_id=str(payload.get("metric_id", "")), definition_id=str(payload.get("definition_id", "")), basis_id=str(payload.get("basis_id", "")), horizon_period_id=str(payload.get("horizon_period_id", "")), dimension_set_id=dimension_set_id, unit_id=str(payload.get("unit_id", "")), currency=payload.get("currency"))
                    mismatch = _identity_issue(f"{path}.header.entity_id", str(header.get("entity_id", "")), expected)
                    if mismatch:
                        issues.append(mismatch)
                except IdentityError as exc:
                    issues.append(_issue("P1", "guidance_series_identity", path, str(exc), "Correct the immutable guidance-series semantic axes."))
                for key, collection_name in (("metric_id", "metrics"), ("definition_id", "definitions"), ("basis_id", "bases"), ("unit_id", "units")):
                    if str(payload.get(key, "")) not in catalog_ids[collection_name]:
                        issues.append(_issue("P1", "catalog_reference", f"{path}.payload.{key}", f"Unknown versioned catalog ID {payload.get(key)!r}.", "Add the semantic definition or correct the series reference."))
                if str(payload.get("horizon_period_id", "")) not in periods:
                    issues.append(_issue("P1", "guidance_horizon_reference", f"{path}.payload.horizon_period_id", "Guidance horizon does not resolve to an explicit period.", "Add or correct the reconciled guidance horizon."))
            elif payload.get("kind") == "Promise":
                if header.get("entity_type") != payload.get("kind"):
                    issues.append(_issue("P1", "typed_union_discriminator", path, "Entity header and payload discriminators differ.", "Use one closed typed-entity variant."))
                for value_key in ("original_target", "original_baseline"):
                    issues.extend(_validate_value_spec(payload.get(value_key), f"{path}.payload.{value_key}"))
                try:
                    expected = promise_identity(company_id=company_id, subject_id=str(payload.get("promise_subject_id", "")), program_id=payload.get("program_id"), origin_occurrence_id=str(payload.get("origin_occurrence_id", "")))
                    mismatch = _identity_issue(f"{path}.header.entity_id", str(header.get("entity_id", "")), expected)
                    if mismatch:
                        issues.append(mismatch)
                except IdentityError as exc:
                    issues.append(_issue("P1", "promise_identity", path, str(exc), "Anchor the Promise to its immutable origin occurrence."))
                origin_occurrence_id = str(payload.get("origin_occurrence_id", ""))
                if origin_occurrence_id not in occurrences:
                    issues.append(_issue("P1", "promise_origin_occurrence", f"{path}.payload.origin_occurrence_id", "Promise origin occurrence does not resolve.", "Retain the immutable original management statement evidence."))
                origin_version = observation_ids.get(str(payload.get("origin_version_id", "")))
                if not origin_version or origin_version.get("payload", {}).get("kind") != "PromiseVersion":
                    issues.append(_issue("P1", "promise_origin_version", f"{path}.payload.origin_version_id", "Promise origin version does not resolve to a PromiseVersion.", "Retain the immutable origin version."))
                else:
                    origin_payload = origin_version.get("payload", {})
                    immutable_pairs = (("original_wording", "wording"), ("original_target", "target"), ("original_baseline", "baseline"), ("original_deadline", "deadline"))
                    changed = [original for original, versioned in immutable_pairs if payload.get(original) != origin_payload.get(versioned)]
                    if changed:
                        issues.append(_issue("P1", "promise_origin_immutable", path, f"Promise original fields differ from its origin version: {', '.join(changed)}.", "Restore the immutable origin; later changes belong in later PromiseVersion records."))

    valid_history_relations: list[Mapping[str, Any]] = []
    valid_relation_keys: set[tuple[str, str, str]] = set()
    history_edges: list[tuple[str, str]] = []
    for index, relation in enumerate(package.get("relations", ())):
        path = f"$.relations[{index}]"
        source = str(relation.get("from_record_id", ""))
        target = str(relation.get("to_record_id", ""))
        relation_type = str(relation.get("relation_type", ""))
        relation_valid = True
        if source not in all_record_ids or target not in all_record_ids:
            blocking("relation_reference", path, "Relation endpoints must resolve to entity or observation records.", "Correct the relation endpoints.", business=str(relation.get("relation_id", "")), candidates=(source, target))
            relation_valid = False
        if source == target:
            blocking("relation_self_link", path, "A relation cannot link a record to itself.", "Remove the self-link and retain only source-backed directed history.", business=str(relation.get("relation_id", "")), candidates=(source,))
            relation_valid = False
        try:
            expected = relation_identity(relation_type=relation_type, from_record_id=source, to_record_id=target, rule_id=str(relation.get("rule_id", "")))
            mismatch = _identity_issue(f"{path}.relation_id", str(relation.get("relation_id", "")), expected)
            if mismatch:
                issues.append(mismatch)
                require_review("identity_immutable_payload", str(relation.get("relation_id", "")), (source, target))
                relation_valid = False
            if relation.get("identity_digest") != identity_digest(expected):
                require_review("identity_digest_integrity", str(relation.get("relation_id", "")), (source, target))
                relation_valid = False
        except IdentityError as exc:
            issues.append(_issue("P1", "relation_identity", path, str(exc), "Correct the explicit relation fields and rebuild its identity."))
            require_review("relation_identity", str(relation.get("relation_id", "")), (source, target))
            relation_valid = False
        source_record = entity_ids.get(source) or observation_ids.get(source)
        target_record = entity_ids.get(target) or observation_ids.get(target)
        if source_record and target_record:
            source_company = str(source_record.get("header", {}).get("company_id", ""))
            target_company = str(target_record.get("header", {}).get("company_id", ""))
            if source_company != target_company or source_company != company_id:
                blocking("relation_company_scope", path, "Relation endpoints are not scoped to the same package company.", "Keep relations within one company sidecar.", business=str(relation.get("relation_id", "")), candidates=(source, target))
                relation_valid = False

            source_kind = str(source_record.get("payload", {}).get("kind", ""))
            target_kind = str(target_record.get("payload", {}).get("kind", ""))
            if relation_type in HISTORY_RELATION_TYPES:
                if source not in observation_ids or target not in observation_ids or source_kind != target_kind:
                    blocking("history_relation_endpoint_type", path, "Correction/supersession endpoints must be observations of the same typed record kind.", "Link only compatible assertions within one typed history.", business=str(relation.get("relation_id", "")), candidates=(source, target))
                    relation_valid = False
                else:
                    try:
                        if _relation_business_identity(source_record) != _relation_business_identity(target_record):
                            raise ReconciliationError("history endpoints have different business identities")
                    except ReconciliationError as exc:
                        blocking("history_relation_business_identity", path, str(exc), "Do not correct or supersede across metric, definition, basis, dimension, period, or series identity.", business=str(relation.get("relation_id", "")), candidates=(source, target))
                        relation_valid = False
                source_header = source_record.get("header", {})
                target_header = target_record.get("header", {})
                try:
                    if date.fromisoformat(str(source_header.get("knowledge_date"))) < date.fromisoformat(str(target_header.get("knowledge_date"))):
                        raise ValueError("history relation points backward in knowledge time")
                    source_publication = source_header.get("publication_date")
                    target_publication = target_header.get("publication_date")
                    if source_publication is not None and target_publication is not None and date.fromisoformat(str(source_publication)) < date.fromisoformat(str(target_publication)):
                        raise ValueError("history relation points backward in publication time")
                except ValueError as exc:
                    blocking("history_relation_time_direction", path, str(exc), "Direct correction/supersession from the later source-backed assertion to the earlier assertion.", business=str(relation.get("relation_id", "")), candidates=(source, target))
                    relation_valid = False

            if relation_type in {"duplicate", "corroborates", "contradicts"}:
                try:
                    compatible_endpoints = source in observation_ids and target in observation_ids and source_kind == target_kind and _relation_business_identity(source_record) == _relation_business_identity(target_record)
                except ReconciliationError:
                    compatible_endpoints = False
                if not compatible_endpoints:
                    blocking("assertion_relation_endpoint_type", path, "Assertion relation endpoints must share typed record and business identity.", "Keep duplicate, corroboration, and contradiction relations within one assertion group.", business=str(relation.get("relation_id", "")), candidates=(source, target))
                    relation_valid = False
                else:
                    expected_relations = infer_assertion_relations(
                        [source_record, target_record],
                        source_documents=package.get("source_documents", ()),
                        evidence_occurrences=package.get("evidence_occurrences", ()),
                    )
                    expected_relation = expected_relations[0] if len(expected_relations) == 1 else None
                    semantic_fields = ("relation_id", "relation_type", "from_record_id", "to_record_id", "rule_id", "evidence_occurrence_ids")
                    if expected_relation is None or any(relation.get(field) != expected_relation.get(field) for field in semantic_fields):
                        blocking("assertion_relation_semantics", path, "Stored duplicate/corroboration/contradiction relation differs from deterministic evidence-and-assertion replay.", "Store only the exact inferred assertion relation; same-origin mirrors are duplicates and independent equivalent evidence corroborates.", business=str(relation.get("relation_id", "")), candidates=(source, target))
                        relation_valid = False
            elif relation_type == "reaffirms":
                if source_kind != target_kind or source_kind != "PromiseVersion" or source_record.get("payload", {}).get("promise_id") != target_record.get("payload", {}).get("promise_id"):
                    blocking("promise_relation_endpoint_type", path, "A reaffirmation must link two versions of the same Promise.", "Match one Promise and link the later reaffirmation to its previous version.", business=str(relation.get("relation_id", "")), candidates=(source, target))
                    relation_valid = False
                try:
                    source_header = source_record.get("header", {})
                    target_header = target_record.get("header", {})
                    if date.fromisoformat(str(source_header.get("knowledge_date"))) < date.fromisoformat(str(target_header.get("knowledge_date"))):
                        raise ValueError("promise reaffirmation points backward in knowledge time")
                    source_publication = source_header.get("publication_date")
                    target_publication = target_header.get("publication_date")
                    if source_publication is not None and target_publication is not None and date.fromisoformat(str(source_publication)) < date.fromisoformat(str(target_publication)):
                        raise ValueError("promise reaffirmation points backward in publication time")
                except ValueError as exc:
                    blocking("history_relation_time_direction", path, str(exc), "Direct reaffirmation from the later source-backed PromiseVersion to its previous version.", business=str(relation.get("relation_id", "")), candidates=(source, target))
                    relation_valid = False
            elif relation_type == "evidences":
                if source not in observation_ids or target_record.get("payload", {}).get("kind") != "Promise":
                    blocking("evidence_relation_endpoint_type", path, "An evidence relation must link an observation to a Promise entity.", "Use a source-backed observation as historical promise evidence.", business=str(relation.get("relation_id", "")), candidates=(source, target))
                    relation_valid = False
            elif relation_type == "explains" and (source_kind != "ManagementStatement" or target not in observation_ids):
                blocking("explanation_relation_endpoint_type", path, "An explanation relation must originate at a ManagementStatement and target an observation.", "Keep management explanation distinct from the fact it explains.", business=str(relation.get("relation_id", "")), candidates=(source, target))
                relation_valid = False
            elif relation_type == "interprets" and (source_kind != "ModelInterpretation" or target not in observation_ids):
                blocking("interpretation_relation_endpoint_type", path, "An interpretation relation must originate at a ModelInterpretation and target an observation.", "Keep reviewed model interpretation distinct from its inputs.", business=str(relation.get("relation_id", "")), candidates=(source, target))
                relation_valid = False

            relation_evidence = [str(value) for value in relation.get("evidence_occurrence_ids", ())]
            if relation_type in HISTORY_RELATION_TYPES | {"reaffirms"}:
                source_evidence = set(str(value) for value in source_record.get("header", {}).get("evidence_occurrence_ids", ()))
                evidence_record = {"header": {"company_id": source_company, "evidence_occurrence_ids": relation_evidence}}
                if not relation_evidence or not set(relation_evidence).issubset(source_evidence) or not evidence_chain_eligible(evidence_record, policy_id=None, occurrences=occurrences, documents=documents):
                    blocking("history_relation_evidence", path, "History-changing relation evidence is missing, rejected, or not owned by the later assertion.", "Attach accepted evidence from the later source-backed assertion.", business=str(relation.get("relation_id", "")), candidates=(source, target))
                    relation_valid = False

        if relation_type in HISTORY_RELATION_TYPES:
            history_edges.append((source, target))
            if relation_valid:
                valid_history_relations.append(relation)
        if relation_valid:
            valid_relation_keys.add((relation_type, source, target))

    if _has_cycle(history_edges):
        blocking("history_relation_cycle", "$.relations", "Correction/supersession graph contains a cycle.", "Resolve the directed history before canonical selection.", business="history-relation-graph", candidates=(value for edge in history_edges for value in edge))
        valid_history_relations = []
        valid_relation_keys = {key for key in valid_relation_keys if key[0] not in HISTORY_RELATION_TYPES}

    promise_entities = {
        entity_id: row
        for entity_id, row in entity_ids.items()
        if row.get("payload", {}).get("kind") == "Promise"
    }
    all_promise_versions = {
        record_id: row
        for record_id, row in observation_ids.items()
        if row.get("payload", {}).get("kind") == "PromiseVersion"
    }
    promise_versions = {
        record_id: row
        for record_id, row in all_promise_versions.items()
        if row.get("header", {}).get("review_state") in ELIGIBLE_REVIEW_STATES
    }
    for promise_id, promise in sorted(promise_entities.items()):
        promise_payload = promise.get("payload", {})
        versions = {
            record_id: row
            for record_id, row in promise_versions.items()
            if str(row.get("payload", {}).get("promise_id", "")) == promise_id
        }
        origin_id = str(promise_payload.get("origin_version_id", ""))
        origin = promise_versions.get(origin_id)
        if origin is None or str(origin.get("payload", {}).get("promise_id", "")) != promise_id:
            blocking("promise_origin_version_ownership", f"$.entities[{promise_id}]", "Promise origin_version_id does not resolve to a PromiseVersion owned by that Promise.", "Restore the immutable origin version and ownership link.", business=promise_id, candidates=(origin_id,))
            continue
        origin_payload = origin.get("payload", {})
        superseded_version_ids = {
            target_id
            for relation_type, _, target_id in valid_relation_keys
            if relation_type == "supersedes" and target_id in versions
        }
        origin_evidence = list(origin.get("header", {}).get("evidence_occurrence_ids", ()))
        promise_evidence = list(promise.get("header", {}).get("evidence_occurrence_ids", ()))
        if origin_evidence != [promise_payload.get("origin_occurrence_id")] or promise_evidence != [promise_payload.get("origin_occurrence_id")]:
            blocking("promise_origin_occurrence_mismatch", f"$.entities[{promise_id}]", "Promise origin version is not anchored to the Promise origin occurrence.", "Use the exact immutable origin evidence occurrence for both Promise and origin version.", business=promise_id, candidates=(origin_id,))
        expected_origin_state = "superseded" if origin_id in superseded_version_ids else "active"
        if origin_payload.get("change_kind") != "origin" or origin_payload.get("version_state") != expected_origin_state or origin_payload.get("previous_version_id") is not None:
            blocking("promise_origin_state", f"$.observations[{origin_id}]", "Promise origin must use change_kind origin, no previous version, and a state derived from explicit supersession.", "Restore the immutable origin state and create later changes as separate versions.", business=promise_id, candidates=(origin_id,))
        immutable_pairs = (("original_wording", "wording"), ("original_target", "target"), ("original_baseline", "baseline"), ("original_deadline", "deadline"))
        changed_origin_fields = [original for original, versioned in immutable_pairs if promise_payload.get(original) != origin_payload.get(versioned)]
        if changed_origin_fields:
            blocking("promise_origin_immutable", f"$.entities[{promise_id}]", f"Promise origin differs from immutable fields: {', '.join(changed_origin_fields)}.", "Restore the immutable original wording, target, baseline and deadline.", business=promise_id, candidates=(origin_id,))

        previous_edges: list[tuple[str, str]] = []
        for version_id, version in sorted(versions.items()):
            if version_id == origin_id:
                continue
            version_payload = version.get("payload", {})
            previous_id = str(version_payload.get("previous_version_id") or "")
            previous = versions.get(previous_id)
            if previous is None:
                blocking("promise_previous_version", f"$.observations[{version_id}].payload.previous_version_id", "Later PromiseVersion does not resolve to a previous version of the same Promise.", "Match exactly one Promise history before emitting a later version.", business=promise_id, candidates=(version_id, previous_id))
                matching_promises = [
                    candidate_id
                    for candidate_id, candidate in promise_entities.items()
                    if candidate.get("payload", {}).get("promise_subject_id") == promise_payload.get("promise_subject_id")
                    and candidate.get("payload", {}).get("program_id") == promise_payload.get("program_id")
                ]
                if len(matching_promises) != 1:
                    require_review("promise_match_cardinality", str(promise_payload.get("promise_subject_id", promise_id)), matching_promises)
                continue
            previous_edges.append((version_id, previous_id))
            try:
                if date.fromisoformat(str(version.get("header", {}).get("knowledge_date"))) < date.fromisoformat(str(previous.get("header", {}).get("knowledge_date"))):
                    raise ValueError("promise history points backward in knowledge time")
            except ValueError as exc:
                blocking("promise_history_time_direction", f"$.observations[{version_id}]", str(exc), "Link each later PromiseVersion to an earlier or same-day source-backed version.", business=promise_id, candidates=(version_id, previous_id))

            actual_change = classify_promise_change(previous.get("payload", {}), version_payload)
            declared_change = str(version_payload.get("change_kind", ""))
            if actual_change != declared_change:
                blocking("promise_change_kind", f"$.observations[{version_id}].payload.change_kind", f"Declared {declared_change!r} disagrees with actual semantic change {actual_change!r}.", "Classify target, deadline, wording reformulation, reaffirmation, or explicit withdrawal from the immutable previous version.", business=promise_id, candidates=(version_id, previous_id))
            intrinsic_state = {
                "reaffirmation": "reaffirmed",
                "target_update": "active",
                "deadline_update": "active",
                "reformulation": "active",
                "withdrawal": "withdrawn",
            }.get(declared_change)
            expected_state = "superseded" if version_id in superseded_version_ids and declared_change != "withdrawal" else intrinsic_state
            if expected_state is None or version_payload.get("version_state") != expected_state:
                blocking("promise_version_state", f"$.observations[{version_id}].payload.version_state", "Promise version_state is incompatible with change_kind and explicit supersession history.", "Use reaffirmed for a current reaffirmation, withdrawn only for explicit withdrawal, active for a current update/reformulation, and superseded only when targeted by a valid relation.", business=promise_id, candidates=(version_id, previous_id))
            expected_relation = "reaffirms" if declared_change == "reaffirmation" else "supersedes"
            if (expected_relation, version_id, previous_id) not in valid_relation_keys:
                blocking("promise_history_relation", "$.relations", "Promise history change lacks its valid source-backed reaffirmation or supersession relation.", "Add the explicit directed relation from the later version to its previous version.", business=promise_id, candidates=(version_id, previous_id))
        if _has_cycle(previous_edges):
            blocking("promise_history_cycle", "$.observations", "Promise previous_version_id chain contains a cycle.", "Restore one acyclic origin-to-latest promise history.", business=promise_id, candidates=versions)
        child_counts = Counter(target for _, target in previous_edges)
        for previous_id, count in sorted(child_counts.items()):
            if count > 1:
                blocking("promise_history_branch", "$.observations", f"Promise history branches into {count} later versions from {previous_id!r}.", "Resolve the chronology into one explicit immutable version chain.", business=promise_id, candidates=versions)

    for version_id, version in sorted(promise_versions.items()):
        promise_id = str(version.get("payload", {}).get("promise_id", ""))
        if promise_id not in promise_entities:
            blocking("promise_version_ownership", f"$.observations[{version_id}].payload.promise_id", "PromiseVersion does not belong to a valid Promise entity.", "Resolve exactly one Promise before emitting a version.", business=promise_id or version_id, candidates=(version_id,))

    promise_groups: dict[tuple[str, str], list[str]] = defaultdict(list)
    for promise_id, promise in promise_entities.items():
        payload = promise.get("payload", {})
        promise_groups[(str(payload.get("promise_subject_id", "")), str(payload.get("program_id") or ""))].append(promise_id)
    for (subject_id, program_id), matching_ids in sorted(promise_groups.items()):
        later_versions = [
            version_id
            for version_id, version in promise_versions.items()
            if str(version.get("payload", {}).get("promise_id", "")) in matching_ids
            and version.get("payload", {}).get("change_kind") != "origin"
        ]
        if len(matching_ids) > 1 and later_versions:
            blocking("promise_match_cardinality", "$.entities", "A later statement can match multiple Promises with the same subject/program identity.", "Do not emit a speculative PromiseVersion; retain a blocking review issue until one match is evidenced.", business=f"{subject_id}|{program_id}", candidates=matching_ids)
    for statement_id, statement in sorted(observation_ids.items()):
        statement_payload = statement.get("payload", {})
        if (
            statement_payload.get("kind") != "ManagementStatement"
            or statement_payload.get("statement_kind") != "commitment"
            or statement.get("header", {}).get("review_state") not in ELIGIBLE_REVIEW_STATES
        ):
            continue
        subject_id = str(statement_payload.get("topic_id", ""))
        matching_ids = sorted(
            promise_id
            for promise_id, promise in promise_entities.items()
            if str(promise.get("payload", {}).get("promise_subject_id", "")) == subject_id
        )
        if len(matching_ids) != 1:
            require_review("promise_match_cardinality", subject_id, matching_ids)
        else:
            statement_evidence = set(str(value) for value in statement.get("header", {}).get("evidence_occurrence_ids", ()))
            matching_versions = [
                version_id
                for version_id, version in promise_versions.items()
                if str(version.get("payload", {}).get("promise_id", "")) == matching_ids[0]
                and statement_evidence == set(str(value) for value in version.get("header", {}).get("evidence_occurrence_ids", ()))
            ]
            if len(matching_versions) != 1:
                blocking("promise_version_match_materialization", f"$.observations[{statement_id}]", "A uniquely matched source commitment is not represented by exactly one evidence-anchored PromiseVersion.", "Materialize one deterministically classified PromiseVersion or route matching ambiguity to Needs Review.", business=subject_id, candidates=matching_ids)

    policy_rows = {str(row.get("policy_id", "")): row for row in package.get("catalog", {}).get("policies", ())}
    stored_review_rows = {str(row.get("issue_id", "")): row for row in package.get("review_issues", ())}
    replayed_selected_record_ids: set[str] = set()
    resolution_business_keys: Counter[tuple[str, str, str, str]] = Counter()
    resolution_group_as_ofs: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for index, resolution in enumerate(package.get("resolutions", ())):
        path = f"$.resolutions[{index}]"
        candidates = list(resolution.get("candidate_record_ids", ()))
        eligible = list(resolution.get("eligible_candidate_ids", ()))
        maximal = list(resolution.get("maximal_candidate_ids", ()))
        reason_codes = list(resolution.get("reason_codes", ()))
        selected = resolution.get("selected_record_id")
        status = resolution.get("status")
        for field, values in (("candidate_record_ids", candidates), ("eligible_candidate_ids", eligible), ("maximal_candidate_ids", maximal), ("reason_codes", reason_codes)):
            if values != sorted(set(values)):
                blocking("resolution_ordered_set", f"{path}.{field}", f"Resolution {field} must be unique and sorted.", "Canonicalize the replayed deterministic set.", business=str(resolution.get("business_key", "")), candidates=candidates)
        if any(candidate not in observation_ids for candidate in candidates):
            blocking("resolution_candidate_reference", path, "A resolution candidate does not resolve to an observation.", "Retain the complete lossless candidate observation.", business=str(resolution.get("business_key", "")), candidates=candidates)
        valid = (status == "selected" and selected in eligible and int(resolution.get("selection_cardinality", -1)) == 1) or (status == "unresolved" and selected is None and int(resolution.get("selection_cardinality", -1)) == 0)
        if not valid:
            blocking("resolution_cardinality", path, "Resolution must select exactly one eligible candidate or explicitly remain unresolved.", "Route zero or multiple maximal matches to Needs Review.", business=str(resolution.get("business_key", "")), candidates=candidates)
        resolution_key = (str(resolution.get("record_type", "")), str(resolution.get("business_key", "")), str(resolution.get("as_of_date", "")), str(resolution.get("policy_id", "")))
        resolution_business_keys[resolution_key] += 1
        record_type, stored_business_key, as_of_date, policy_id = resolution_key
        resolution_group_as_ofs[(record_type, stored_business_key, policy_id)].add(as_of_date)
        policy_row = policy_rows.get(policy_id)
        if policy_id not in POLICY_RECORD_TYPES or record_type not in POLICY_RECORD_TYPES.get(policy_id, ()) or policy_row is None or policy_row.get("assertion_type") != POLICY_ASSERTION_TYPES.get(policy_id):
            blocking("resolution_policy_record_type", path, "Resolution policy, catalog assertion type, and candidate record type are inconsistent.", "Use the assertion-specific policy declared for this typed record union.", business=stored_business_key, candidates=candidates)

        expected_candidates: list[Mapping[str, Any]] = []
        for candidate in observation_ids.values():
            if candidate.get("header", {}).get("record_type") != record_type:
                continue
            try:
                if business_key(candidate) == stored_business_key:
                    expected_candidates.append(candidate)
            except ReconciliationError:
                continue
        for candidate_id in candidates:
            candidate = observation_ids.get(str(candidate_id))
            if candidate is None:
                continue
            try:
                if candidate.get("header", {}).get("record_type") != record_type or business_key(candidate) != stored_business_key:
                    blocking("resolution_candidate_business_identity", path, "Resolution candidate type or business identity differs from the declared resolution group.", "Group only one exact typed business identity per canonical resolution.", business=stored_business_key, candidates=candidates)
                    break
            except ReconciliationError as exc:
                blocking("resolution_candidate_business_identity", path, str(exc), "Use a typed observation with a deterministic business identity.", business=stored_business_key, candidates=candidates)
                break

        replay = None
        if policy_id in POLICY_RECORD_TYPES:
            try:
                replay = resolve_observations(
                    expected_candidates,
                    policy_id=policy_id,
                    as_of_date=as_of_date,
                    source_documents=package.get("source_documents", ()),
                    evidence_occurrences=package.get("evidence_occurrences", ()),
                    relations=valid_history_relations,
                )
            except ReconciliationError as exc:
                blocking("resolution_replay_invalid", path, str(exc), "Repair the candidate group or assertion-specific policy before storing a resolution.", business=stored_business_key, candidates=candidates)

        if replay is not None:
            recomputed = replay.resolution
            compared_fields = (
                "policy_id",
                "record_type",
                "business_key",
                "candidate_record_ids",
                "eligible_candidate_ids",
                "maximal_candidate_ids",
                "selected_record_id",
                "selection_cardinality",
                "status",
                "reason_codes",
                "review_issue_ids",
            )
            mismatches = [field for field in compared_fields if resolution.get(field) != recomputed.get(field)]
            if mismatches:
                blocking("resolution_replay_mismatch", path, f"Stored CanonicalResolution differs from deterministic replay in: {', '.join(mismatches)}.", "Replace stored outcome fields with the exact assertion-policy replay result.", business=stored_business_key, candidates=recomputed.get("candidate_record_ids", ()))
            replay_selected = recomputed.get("selected_record_id")
            if replay_selected is not None and as_of_date == str(package.get("knowledge_cutoff", "")):
                replayed_selected_record_ids.add(str(replay_selected))
            for replay_issue in replay.review_issues:
                require_review(str(replay_issue.get("rule_id", "")), str(replay_issue.get("business_key", stored_business_key)), replay_issue.get("candidate_record_ids", ()))
                if str(replay_issue.get("issue_id", "")) not in stored_review_rows:
                    issues.append(_issue("P1", "resolution_review_issue_missing", f"{path}.review_issue_ids", "Unresolved replay result lacks its deterministic root review issue.", "Serialize the exact blocking Needs Review issue returned by reconciliation."))
            if recomputed.get("status") == "unresolved" and not replay.review_issues:
                blocking("resolution_unresolved_without_reason", path, "Replayed unresolved resolution has no blocking reason.", "Fail closed with a deterministic cardinality/conflict reason and review issue.", business=stored_business_key, candidates=recomputed.get("maximal_candidate_ids", ()))

        try:
            expected = canonical_resolution_identity(record_type=record_type, business_key=stored_business_key, as_of_date=as_of_date, policy_id=policy_id, candidate_record_ids=eligible)
            mismatch = _identity_issue(f"{path}.resolution_id", str(resolution.get("resolution_id", "")), expected)
            if mismatch:
                issues.append(mismatch)
        except IdentityError as exc:
            issues.append(_issue("P1", "resolution_identity", path, str(exc), "Rebuild the resolution from its complete sorted eligible candidate set."))
    for resolution_key, count in resolution_business_keys.items():
        if count > 1:
            blocking("resolution_business_cardinality", "$.resolutions", f"Business resolution key {resolution_key!r} appears {count} times.", "Keep one canonical resolution per assertion key, cutoff, and policy.", business="|".join(resolution_key))
    current_cutoff = str(package.get("knowledge_cutoff", ""))
    for group, as_of_dates in sorted(resolution_group_as_ofs.items()):
        if current_cutoff not in as_of_dates:
            blocking("current_resolution_missing", "$.resolutions", f"Resolution group {group!r} has no canonical result at knowledge cutoff {current_cutoff!r}.", "Retain historical resolutions only alongside the required current-cutoff resolution.", business=group[1])

    change_rules = {str(row.get("rule_id", "")): row for row in package.get("catalog", {}).get("change_rules", ())}
    units = {str(row.get("unit_id", "")): row for row in package.get("catalog", {}).get("units", ())}
    for record_id, change in sorted(observation_ids.items()):
        if change.get("payload", {}).get("kind") != "ChangeObservation":
            continue
        payload = change.get("payload", {})
        path = f"$.observations[{record_id}]"
        rule_id = str(payload.get("rule_id", ""))
        rule = change_rules.get(rule_id)
        from_id = str(payload.get("from_record_id", ""))
        to_id = str(payload.get("to_record_id", ""))
        from_record, to_record = observation_ids.get(from_id), observation_ids.get(to_id)
        if rule is None:
            blocking("change_rule_reference", f"{path}.payload.rule_id", "ChangeObservation rule_id does not resolve in the change-rule catalog.", "Use a versioned closed change rule.", business=record_id, candidates=(from_id, to_id))
            continue
        if str(rule.get("output_unit_id", "")) not in units:
            blocking("change_rule_output_unit", "$.catalog.change_rules", "Change rule output_unit_id does not resolve to the unit catalog.", "Bind the rule to one versioned output unit.", business=rule_id, candidates=(record_id,))
        if from_record is None or to_record is None or from_record.get("payload", {}).get("kind") != "NumericalFact" or to_record.get("payload", {}).get("kind") != "NumericalFact":
            blocking("change_input_type", f"{path}.payload.input_record_ids", "ChangeObservation inputs must resolve to NumericalFact records.", "Derive only from two immutable numerical facts.", business=record_id, candidates=(from_id, to_id))
            continue
        if from_id not in replayed_selected_record_ids or to_id not in replayed_selected_record_ids:
            blocking("change_input_selection", f"{path}.payload.input_record_ids", "ChangeObservation uses an input that is not selected by deterministic canonical replay.", "Resolve both input facts canonically before deriving the change.", business=record_id, candidates=(from_id, to_id))
        try:
            validate_percentage_point_rule_binding(change, from_record, to_record, rule=rule, units=units)
            from_header = from_record.get("header", {})
            to_header = to_record.get("header", {})
            raw_from_period_id = from_header.get("fiscal_period_id")
            raw_to_period_id = to_header.get("fiscal_period_id")
            if raw_from_period_id is None or raw_to_period_id is None:
                raise IncomparablePeriodError("Change inputs require explicit fiscal_period_id values.")
            from_period_id = str(raw_from_period_id)
            to_period_id = str(raw_to_period_id)
            if from_header.get("effective_period_id") != from_period_id or to_header.get("effective_period_id") != to_period_id:
                raise IncomparablePeriodError("Change input effective and fiscal period identities differ.")
            if change.get("header", {}).get("fiscal_period_id") is None:
                raise IncomparablePeriodError("ChangeObservation requires the later selected fact's fiscal identity.")
            from_period = periods[from_period_id]
            to_period = periods[to_period_id]
            from_calendar = calendars[str(from_period.get("calendar_id", ""))]
            to_calendar = calendars[str(to_period.get("calendar_id", ""))]
            if from_period.get("reconciliation_state") != "reconciled" or to_period.get("reconciliation_state") != "reconciled" or from_calendar.get("reconciliation_state") != "reconciled" or to_calendar.get("reconciliation_state") != "reconciled":
                raise IncomparablePeriodError("Change inputs use a blocker-level needs-review period or calendar.")
            expected_comparability = compare_periods(
                from_period,
                to_period,
                earlier_calendar=from_calendar,
                later_calendar=to_calendar,
                change_kind=str(payload.get("change_kind", "")),
            )
            if payload.get("comparability") != expected_comparability:
                raise IncomparablePeriodError("Stored comparison state differs from exact fiscal-period replay.")
            from_value = from_record.get("payload", {}).get("value", {})
            to_value = to_record.get("payload", {}).get("value", {})
            if from_value.get("kind") != "exact" or to_value.get("kind") != "exact":
                raise IncompatibleFactError("Change inputs must be selected exact NumericalFacts.")
            expected_value = canonical_decimal(Decimal(str(to_value.get("value"))) - Decimal(str(from_value.get("value"))))
            if payload.get("value") != {"kind": "exact", "value": expected_value}:
                raise IncompatibleFactError("Stored change value does not equal later minus earlier selected fact.")
        except (IncomparablePeriodError, IncompatibleFactError, KeyError, ArithmeticError, DomainValidationError) as exc:
            blocking("change_semantic_binding", path, str(exc), "Re-derive the change from canonically selected, compatible exact facts and reconciled fiscal periods under the declared rule.", business=record_id, candidates=(from_id, to_id))

    methods = {str(row.get("method_id", "")): row for row in package.get("catalog", {}).get("methods", ())}
    for record_id, interpretation in sorted(observation_ids.items()):
        if interpretation.get("payload", {}).get("kind") != "ModelInterpretation":
            continue
        payload = interpretation.get("payload", {})
        header = interpretation.get("header", {})
        if header.get("review_state") not in ELIGIBLE_REVIEW_STATES:
            continue
        inputs = [str(value) for value in payload.get("input_record_ids", ())]
        method = methods.get(str(payload.get("method_id", "")))
        invalid_inputs = [
            input_id
            for input_id in inputs
            if input_id not in observation_ids
            or observation_ids[input_id].get("header", {}).get("review_state") not in ELIGIBLE_REVIEW_STATES
            or (
                observation_ids[input_id].get("header", {}).get("record_type") in RECORD_POLICY_IDS
                and input_id not in replayed_selected_record_ids
            )
        ]
        if header.get("review_state") != "reviewed" or method is None or method.get("producer_id") != payload.get("producer_id") or invalid_inputs:
            blocking("interpretation_review_input_state", f"$.observations[{record_id}]", "Accepted model interpretation lacks reviewed state, matching method/producer, or canonically selected accepted inputs.", "Review the interpretation and retain exact selected input lineage under the versioned method.", business=record_id, candidates=inputs)

    stored_blocking_conditions: set[tuple[str, str, tuple[str, ...]]] = set()
    for index, row in enumerate(package.get("review_issues", ())):
        severity = str(row.get("severity", "")).upper()
        if severity not in BLOCKING_SEVERITIES:
            continue
        if row.get("promotion_blocking") is not True:
            issues.append(_issue("P1", "blocking_review_promotion_flag", f"$.review_issues[{index}].promotion_blocking", "P0/P1 review issue must be promotion-blocking.", "Set promotion_blocking true for every mandatory P0/P1 issue."))
        stored_blocking_conditions.add(
            (
                str(row.get("rule_id", "")),
                str(row.get("business_key", "")),
                tuple(sorted(set(str(value) for value in row.get("candidate_record_ids", ()) if value))),
            )
        )

    missing_conditions = sorted(mandatory_conditions - stored_blocking_conditions)
    extra_conditions = sorted(stored_blocking_conditions - mandatory_conditions)
    for rule_id, business, candidates in missing_conditions:
        issues.append(_issue("P1", "mandatory_review_issue_missing", "$.review_issues", f"Mandatory P1 condition {rule_id!r} for {business!r} is not serialized with its exact candidate set {list(candidates)!r}.", "Serialize the independently derived blocking Needs Review condition."))
    for rule_id, business, candidates in extra_conditions:
        issues.append(_issue("P1", "blocking_review_issue_not_derived", "$.review_issues", f"Stored blocking issue {rule_id!r} for {business!r} is not reproduced by semantic validation for candidates {list(candidates)!r}.", "Remove stale blocking state or restore the independently verifiable condition."))

    artifact_state = str(package.get("artifact_state", ""))
    blocking_exists = bool(mandatory_conditions or stored_blocking_conditions)
    if blocking_exists and artifact_state != "needs_review":
        issues.append(_issue("P1", "artifact_state_fail_closed", "$.artifact_state", "A package with mandatory or stored P0/P1 conditions must be needs_review.", "Block acceptance until the exact mandatory issue set is resolved."))
    if artifact_state == "needs_review" and (not blocking_exists or mandatory_conditions != stored_blocking_conditions):
        issues.append(_issue("P1", "artifact_state_blocking_set", "$.artifact_state", "needs_review artifact state does not have an exact independently derived blocking issue set.", "Synchronize artifact state with the exact mandatory P0/P1 conditions."))
    return issues


def validate_package(
    package: Mapping[str, Any], *, schema_path: Path | str = DEFAULT_SCHEMA_PATH
) -> list[ValidationIssue]:
    return validate_package_schema(package, schema_path=schema_path) + validate_package_semantics(package)


def validate_or_raise(package: Mapping[str, Any], *, schema_path: Path | str = DEFAULT_SCHEMA_PATH) -> None:
    issues = validate_package(package, schema_path=schema_path)
    if issues:
        rendered = "\n".join(f"{row.severity} {row.normalized_path}: {row.message}" for row in issues)
        raise LongitudinalValidationError(rendered)


def build_review_ledger(issues: Iterable[ValidationIssue | Mapping[str, Any]]) -> dict[str, Any]:
    rows = [issue.to_dict() if hasattr(issue, "to_dict") else dict(issue) for issue in issues]
    return build_canonical_issue_ledger(manual_review_flags=rows)
