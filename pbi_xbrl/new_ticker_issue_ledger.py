"""Canonical issue ledger for the generic new-ticker engine.

The ledger keeps every source-level occurrence in JSON while presenting stable,
deduplicated issue summaries to workbook QA surfaces.  It has no Excel or
ticker-specific dependencies.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from typing import Any, Mapping, Sequence


ISSUE_LEDGER_VERSION = "1.0.0"
_BLOCKING_SEVERITIES = {"P0", "P1"}
_PERIOD_RE = re.compile(r"\b(?:20\d{2}-(?:Q[1-4]|FY)|FY20\d{2}|20\d{2}-\d{2}-\d{2})\b")
_INDEX_RE = re.compile(r"(?<=\.)\d+(?=\.|$)")
_SYNTHETIC_ROW_KEY_RE = re.compile(r"^(?:package_manual_review|package_mapping_gap|source_index|gap):", re.I)


def build_canonical_issue_ledger(
    *,
    manual_review_flags: Sequence[Mapping[str, Any]] = (),
    mapping_gaps: Sequence[Mapping[str, Any]] = (),
    validation_issues: Sequence[Any] = (),
    check_results: Sequence[Mapping[str, Any]] = (),
    trusted_canonical_issue_keys: Sequence[str] = (),
) -> dict[str, Any]:
    """Build stable summaries plus lossless source-level occurrences."""

    raw_items: list[tuple[str, Mapping[str, Any]]] = []
    raw_items.extend(("manual_review", item) for item in manual_review_flags if isinstance(item, Mapping))
    raw_items.extend(("mapping_gap", item) for item in mapping_gaps if isinstance(item, Mapping))
    for issue in validation_issues:
        payload = issue.to_dict() if hasattr(issue, "to_dict") else issue
        if isinstance(payload, Mapping):
            raw_items.append(("validation", payload))

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    issue_identity: dict[str, dict[str, Any]] = {}
    occurrences: list[dict[str, Any]] = []
    duplicate_ordinals: dict[str, int] = defaultdict(int)
    trusted_keys = {str(key) for key in trusted_canonical_issue_keys if key}
    for origin, raw in raw_items:
        normalized = _normalize_occurrence(raw, origin=origin, trusted_keys=trusted_keys)
        identity = _identity_payload(normalized)
        normalized.pop("_canonical_issue_key_trusted", None)
        issue_id = "ISS-" + _stable_hash(identity, length=20)
        normalized["issue_id"] = issue_id
        occurrence_fingerprint = _stable_hash(
            {"issue_id": issue_id, "origin": origin, "detail": normalized["detail"]},
            length=24,
        )
        duplicate_ordinal = duplicate_ordinals[occurrence_fingerprint]
        duplicate_ordinals[occurrence_fingerprint] += 1
        normalized["occurrence_id"] = "OCC-" + _stable_hash(
            {
                "issue_id": issue_id,
                "occurrence_fingerprint": occurrence_fingerprint,
                "duplicate_ordinal": duplicate_ordinal,
            },
            length=24,
        )
        grouped[issue_id].append(normalized)
        issue_identity[issue_id] = identity
        occurrences.append(normalized)

    issues = [
        _summarize_issue(issue_id, rows, issue_identity[issue_id])
        for issue_id, rows in grouped.items()
    ]
    issues.sort(key=_issue_sort_key)
    occurrences.sort(key=lambda row: (row["issue_id"], row["occurrence_id"]))
    checks = _build_rule_checks(issues, check_results=check_results)
    qa_log_rows = [_qa_log_row(issue) for issue in issues]
    needs_review_rows = [
        _needs_review_row(issue)
        for issue in issues
        if issue["visibility_disposition"] == "needs_review"
    ]
    qa_check_rows = [_qa_check_row(check) for check in checks]

    blocking_count = sum(
        1
        for issue in issues
        if issue["promotion_blocking"] or issue["render_blocking"]
    )
    actionable_count = len(needs_review_rows)
    summary = {
        "canonical_unique_issue_count": len(issues),
        "detailed_occurrence_count": len(occurrences),
        "actionable_issue_count": actionable_count,
        "audit_only_issue_count": len(issues) - actionable_count,
        "blocking_issue_count": blocking_count,
        "issue_type_counts": _count_values(issues, "issue_type"),
        "occurrence_type_counts": _count_values(occurrences, "issue_type"),
    }
    return {
        "version": ISSUE_LEDGER_VERSION,
        "summary": summary,
        "issues": issues,
        "occurrences": occurrences,
        "checks": checks,
        "qa_presentation": {
            "qa_log_rows": qa_log_rows,
            "needs_review_rows": needs_review_rows,
            "qa_check_rows": qa_check_rows,
        },
    }


def _normalize_occurrence(
    raw: Mapping[str, Any],
    *,
    origin: str,
    trusted_keys: set[str],
) -> dict[str, Any]:
    detail = _json_safe(raw)
    severity = str(raw.get("severity") or "P2").upper()
    rule_id = str(raw.get("rule_id") or _default_rule_id(origin))
    raw_field = str(raw.get("field") or raw.get("normalized_field") or "")
    issue_type = _issue_type(rule_id, origin=origin, raw=raw)
    normalized_path = str(raw.get("normalized_path") or raw_field)
    field = str(raw.get("field") or raw.get("normalized_field") or normalized_path)
    if issue_type in {"planner_mapping_gap", "planner_overflow"} and normalized_path:
        section = _section_from_path(normalized_path)
    else:
        section = str(raw.get("section") or _section_from_path(field or normalized_path))
    row_key = str(raw.get("business_row_key") or raw.get("row_key") or "")
    if _SYNTHETIC_ROW_KEY_RE.match(row_key):
        row_key = ""
    binding_id = str(raw.get("binding_id") or "")
    if _SYNTHETIC_ROW_KEY_RE.match(binding_id):
        binding_id = ""
    adapter_metadata = raw.get("adapter_metadata") if isinstance(raw.get("adapter_metadata"), Mapping) else {}
    evidence_key = str(raw.get("evidence_key") or adapter_metadata.get("evidence_key") or "")
    source_ref = str(raw.get("source_ref") or adapter_metadata.get("source_ref") or "")
    synthesis = raw.get("source_synthesis") if isinstance(raw.get("source_synthesis"), Mapping) else {}
    synthesis_id = str(synthesis.get("synthesis_id") or "")
    contributing_source_refs = sorted(
        {str(value) for value in synthesis.get("source_refs") or [] if str(value)}
    )
    message = _compact_text(raw.get("message") or raw.get("reason") or "Issue requires review.")
    root_cause = str(
        raw.get("root_cause")
        or raw.get("classification")
        or adapter_metadata.get("reason")
        or raw.get("reason")
        or rule_id.replace("_", " ")
    )
    suggested_action = _compact_text(raw.get("suggested_action") or "Review the detailed JSON occurrence.")
    period = str(raw.get("period") or raw.get("affected_period") or _first_period(detail) or "")
    canonical_issue_key = str(raw.get("canonical_issue_key") or "")
    disposition = _visibility_disposition(issue_type, severity=severity, raw=raw)
    promotion_blocking = bool(raw.get("promotion_blocking")) or severity in _BLOCKING_SEVERITIES
    render_blocking = bool(raw.get("render_blocking")) or promotion_blocking
    return {
        "severity": severity,
        "rule_id": rule_id,
        "issue_type": issue_type,
        "section": section,
        "normalized_path": normalized_path,
        "business_row_key": row_key,
        "binding_id": binding_id,
        "source_ref": source_ref,
        "evidence_key": evidence_key,
        "source_synthesis_id": synthesis_id,
        "contributing_source_refs": contributing_source_refs,
        "root_cause": _compact_text(root_cause),
        "message": message,
        "suggested_action": suggested_action,
        "affected_period": period,
        "visibility_disposition": disposition,
        "promotion_blocking": promotion_blocking,
        "render_blocking": render_blocking,
        "canonical_issue_key": canonical_issue_key,
        "_canonical_issue_key_trusted": canonical_issue_key in trusted_keys,
        "origin": origin,
        "detail": detail,
    }


def _identity_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    canonical_issue_key = str(row.get("canonical_issue_key") or "")
    issue_type = str(row["issue_type"])
    source_ref = str(row.get("source_ref") or "")
    evidence_key = str(row.get("evidence_key") or "")
    synthesis_id = str(row.get("source_synthesis_id") or "")
    contributing_source_refs = sorted(
        {str(value) for value in row.get("contributing_source_refs") or [] if str(value)}
    )
    detail = row.get("detail") if isinstance(row.get("detail"), Mapping) else {}
    if issue_type == "text_quality_demotion":
        excerpt = str(detail.get("original_excerpt") or "")
        evidence_key = evidence_key or _stable_hash(
            {"source_ref": source_ref.casefold(), "excerpt": _compact_text(excerpt).casefold()},
            length=20,
        )
    elif issue_type == "adapter_truncation_metadata":
        metadata = detail.get("adapter_metadata") if isinstance(detail.get("adapter_metadata"), Mapping) else {}
        evidence_key = evidence_key or str(metadata.get("collection") or row.get("normalized_path") or "")
    elif issue_type == "unit_normalization_review":
        evidence_key = evidence_key or f"{row.get('normalized_path') or ''}|{source_ref}"
    elif not evidence_key:
        evidence_key = source_ref

    business_row_key = str(row.get("business_row_key") or "")
    canonical_path = _INDEX_RE.sub("*", str(row.get("normalized_path") or ""))
    binding_id = str(row.get("binding_id") or "")
    affected_period = str(row.get("affected_period") or "")
    common_identity = {
        "canonical_issue_key": canonical_issue_key,
        "normalized_path": canonical_path,
        "business_row_key": business_row_key,
        "binding_id": binding_id,
        "affected_period": affected_period,
    }
    if canonical_issue_key and bool(row.get("_canonical_issue_key_trusted")) and _is_valid_planner_event_key(
        canonical_issue_key,
        binding_id=binding_id,
        normalized_path=str(row.get("normalized_path") or ""),
        business_row_key=business_row_key,
    ):
        # A planner event explicitly correlates the validation issue and mapping
        # gap emitted for one binding/path/business-row defect. Source evidence
        # remains losslessly attached to the child occurrences.
        return {**common_identity, "identity_contract": "validated_planner_event"}
    return {
        **common_identity,
        "rule_id": str(row["rule_id"]),
        "issue_type": issue_type,
        "section": str(row.get("section") or ""),
        "evidence_identity": evidence_key,
        "source_identity": (
            {
                "synthesis_id": synthesis_id,
                "source_refs": contributing_source_refs,
            }
            if synthesis_id and contributing_source_refs
            else source_ref.casefold()
        ),
        "root_cause": _compact_text(row.get("root_cause") or "").casefold(),
        "message": _compact_text(row.get("message") or "").casefold(),
    }


def _is_valid_planner_event_key(
    key: str,
    *,
    binding_id: str,
    normalized_path: str,
    business_row_key: str,
) -> bool:
    """Treat planner keys as correlation contracts only when their scope matches."""

    prefix = f"planner_event|{binding_id}|"
    suffix = f"|{normalized_path}|{business_row_key}"
    return bool(binding_id and normalized_path and business_row_key and key.startswith(prefix) and key.endswith(suffix))


def _summarize_issue(issue_id: str, rows: Sequence[Mapping[str, Any]], identity: Mapping[str, Any]) -> dict[str, Any]:
    first = rows[0]
    periods = sorted({str(row.get("affected_period") or "") for row in rows if row.get("affected_period")})
    source_refs = sorted(
        {
            source
            for row in rows
            for source in [str(row.get("source_ref") or ""), *(str(value) for value in row.get("contributing_source_refs") or [])]
            if source
        }
    )
    evidence_keys = sorted({str(row.get("evidence_key") or "") for row in rows if row.get("evidence_key")})
    severities = sorted({str(row.get("severity") or "P2") for row in rows}, key=_severity_rank)
    blocking = any(bool(row.get("promotion_blocking")) for row in rows)
    render_blocking = any(bool(row.get("render_blocking")) for row in rows)
    disposition = "needs_review" if any(row.get("visibility_disposition") == "needs_review" for row in rows) else "json_audit_only"
    return {
        "issue_id": issue_id,
        "severity": severities[0],
        "rule_id": str(first["rule_id"]),
        "issue_type": str(first["issue_type"]),
        "section": str(first.get("section") or ""),
        "normalized_path": str(identity.get("normalized_path") or first.get("normalized_path") or ""),
        "business_row_key": str(identity.get("business_row_key") or first.get("business_row_key") or ""),
        "binding_id": str(first.get("binding_id") or ""),
        "canonical_issue_key": str(first.get("canonical_issue_key") or ""),
        "source_refs": source_refs,
        "evidence_keys": evidence_keys,
        "root_cause": str(first.get("root_cause") or ""),
        "message": str(first.get("message") or ""),
        "suggested_action": str(first.get("suggested_action") or ""),
        "occurrence_count": len(rows),
        "first_affected_period": periods[0] if periods else "",
        "last_affected_period": periods[-1] if periods else "",
        "visibility_disposition": disposition,
        "promotion_blocking": blocking,
        "render_blocking": render_blocking,
        "occurrence_ids": [str(row["occurrence_id"]) for row in rows],
    }


def _build_rule_checks(
    issues: Sequence[Mapping[str, Any]],
    *,
    check_results: Sequence[Mapping[str, Any]] = (),
) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for issue in issues:
        grouped[str(issue["rule_id"])].append(issue)
    checks_by_rule: dict[str, dict[str, Any]] = {}
    for rule_id, rows in grouped.items():
        blocking_count = sum(
            1
            for row in rows
            if row["promotion_blocking"] or row["render_blocking"]
        )
        actionable_count = sum(1 for row in rows if row["visibility_disposition"] == "needs_review")
        status = "FAIL" if blocking_count else "REVIEW" if actionable_count else "INFO"
        occurrence_count = sum(int(row["occurrence_count"]) for row in rows)
        sections = sorted({str(row.get("section") or "") for row in rows if row.get("section")})
        checks_by_rule[rule_id] = {
            "rule_id": rule_id,
            "status": status,
            "unique_issue_count": len(rows),
            "occurrence_count": occurrence_count,
            "blocking_count": blocking_count,
            "actionable_count": actionable_count,
            "affected_sections": ", ".join(sections),
            "interpretation": _check_interpretation(status, len(rows), occurrence_count),
            "detail_ref": f"issue_ledger.checks[{rule_id}]",
        }

    for raw in check_results:
        if not isinstance(raw, Mapping):
            continue
        rule_id = str(raw.get("rule_id") or "").strip()
        if not rule_id or rule_id in checks_by_rule:
            continue
        status = str(raw.get("status") or "PASS").upper()
        if status not in {"FAIL", "REVIEW", "INFO", "PASS"}:
            status = "INFO"
        checks_by_rule[rule_id] = {
            "rule_id": rule_id,
            "status": status,
            "unique_issue_count": max(0, int(raw.get("unique_issue_count") or 0)),
            "occurrence_count": max(0, int(raw.get("occurrence_count") or 0)),
            "blocking_count": max(0, int(raw.get("blocking_count") or 0)),
            "actionable_count": max(0, int(raw.get("actionable_count") or 0)),
            "affected_sections": str(raw.get("affected_sections") or ""),
            "interpretation": _compact_text(raw.get("interpretation") or f"{rule_id} completed with status {status}."),
            "detail_ref": str(raw.get("detail_ref") or f"issue_ledger.checks[{rule_id}]"),
        }

    checks = list(checks_by_rule.values())
    checks.sort(key=lambda row: (_check_status_rank(str(row["status"])), str(row["rule_id"])))
    return checks


def _qa_log_row(issue: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "issue_id": issue["issue_id"],
        "severity": issue["severity"],
        "rule_id": issue["rule_id"],
        "issue_type": issue["issue_type"],
        "section": issue["section"],
        "root_cause": issue["root_cause"],
        "message": issue["message"],
        "suggested_action": issue["suggested_action"],
        "occurrence_count": issue["occurrence_count"],
        "visibility_disposition": issue["visibility_disposition"],
        "promotion_blocking": issue["promotion_blocking"],
        "detail_ref": f"issue_ledger.issues[{issue['issue_id']}]",
    }


def _needs_review_row(issue: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "issue_id": issue["issue_id"],
        "severity": issue["severity"],
        "rule_id": issue["rule_id"],
        "section": issue["section"],
        "normalized_path": issue["normalized_path"],
        "business_row_key": issue["business_row_key"],
        "message": issue["message"],
        "suggested_action": issue["suggested_action"],
        "occurrence_count": issue["occurrence_count"],
        "promotion_blocking": issue["promotion_blocking"],
        "detail_ref": f"issue_ledger.issues[{issue['issue_id']}]",
    }


def _qa_check_row(check: Mapping[str, Any]) -> dict[str, Any]:
    return dict(check)


def _issue_type(rule_id: str, *, origin: str, raw: Mapping[str, Any]) -> str:
    explicit = str(raw.get("issue_type") or "")
    if explicit:
        return explicit
    if rule_id == "text_quality_demoted":
        return "text_quality_demotion"
    if rule_id == "legacy_adapter_truncation":
        return "adapter_truncation_metadata"
    if rule_id == "legacy_adapter_exact_duplicate":
        return "duplicate_evidence"
    if rule_id == "legacy_adapter_unit_normalization":
        return "unit_normalization_review"
    if origin == "mapping_gap":
        reason = str(raw.get("reason") or raw.get("message") or "").casefold()
        return "planner_overflow" if "capacity" in reason or "overflow" in reason else "planner_mapping_gap"
    if origin == "validation":
        return "validation_failure"
    if str(raw.get("visibility_disposition") or "") == "json_audit_only":
        return "audit_only_information"
    return "actionable_exception"


def _visibility_disposition(issue_type: str, *, severity: str, raw: Mapping[str, Any]) -> str:
    explicit = str(raw.get("visibility_disposition") or "")
    if explicit in {"needs_review", "json_audit_only"}:
        return explicit
    if severity in _BLOCKING_SEVERITIES or bool(raw.get("promotion_blocking")) or bool(raw.get("render_blocking")):
        return "needs_review"
    if issue_type in {
        "text_quality_demotion",
        "adapter_truncation_metadata",
        "duplicate_evidence",
        "audit_only_information",
    }:
        return "json_audit_only"
    return "needs_review"


def _default_rule_id(origin: str) -> str:
    return {"mapping_gap": "planner_mapping_gap", "validation": "validation_failure"}.get(origin, "manual_review_required")


def _section_from_path(path: str) -> str:
    clean = path.lstrip("$.")
    return clean.split(".", 1)[0] if clean else "general"


def _first_period(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, default=str)
    match = _PERIOD_RE.search(text)
    return match.group(0) if match else ""


def _compact_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _stable_hash(value: Any, *, length: int) -> str:
    canonical = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:length]


def _json_safe(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False, default=str))


def _severity_rank(value: str) -> int:
    return {"P0": 0, "P1": 1, "P2": 2, "P3": 3}.get(value.upper(), 9)


def _issue_sort_key(issue: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        _severity_rank(str(issue["severity"])),
        0 if issue["visibility_disposition"] == "needs_review" else 1,
        str(issue["rule_id"]),
        str(issue["section"]),
        str(issue["issue_id"]),
    )


def _check_status_rank(value: str) -> int:
    return {"FAIL": 0, "REVIEW": 1, "INFO": 2, "PASS": 3}.get(value, 9)


def _check_interpretation(status: str, unique_count: int, occurrence_count: int) -> str:
    if status == "FAIL":
        return f"{unique_count} blocking issue(s) across {occurrence_count} detailed occurrence(s)."
    if status == "REVIEW":
        return f"{unique_count} actionable issue(s) require review; full detail remains in JSON."
    return f"{unique_count} audit-only issue(s) summarize {occurrence_count} retained occurrence(s)."


def _count_values(rows: Sequence[Mapping[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        counts[str(row.get(field) or "unknown")] += 1
    return dict(sorted(counts.items()))
