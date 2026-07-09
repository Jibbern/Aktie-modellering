"""Pre-render validation for normalized new-ticker data packages.

This module intentionally has no dependency on workbook writers.  It validates
the normalized data package before any Excel shell/fill step can run.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


FIELD_STATUSES = {
    "populated",
    "missing_source",
    "missing_mapping",
    "not_applicable",
    "manual_review_required",
    "parser_conflict",
}

_EMPTY_VALUES = (None, "")
_MISSING_STATUSES = {
    "missing_source",
    "missing_mapping",
    "manual_review_required",
    "parser_conflict",
}

_PARSER_NOISE_RE = re.compile(
    r"guidance signal in filing text|"
    r"revenue signal in filing text|"
    r"\bfcf guidance\s+1\s+to\s+1\b|"
    r"\bvolumes declined due to lower volumes(?:[\s.,|]|$)|"
    r"stable to positive partially offset by strong operating performance\s*\||"
    r"source_txt_file|source_txt|raw_json|"
    r"template placeholder|n/a - keep blank",
    re.I,
)
_BOILERPLATE_GUIDANCE_RE = re.compile(
    r"forward[- ]looking statements|"
    r"may differ materially|"
    r"no duty to update|"
    r"do not undertake|"
    r"safe harbor",
    re.I,
)
_NUMBER_RE = re.compile(r"[-+]?\$?\d+(?:\.\d+)?\s*(?:%|m|bn|billion|million)?", re.I)
_PLACEHOLDER_RE = re.compile(
    r"\bplaceholder\b|\bgeneric\b|\btbd\b|\bto be filled\b|\bneeds reviewed thesis\b|n/a - keep blank",
    re.I,
)
_SECTOR_TERMS = (
    "45Z",
    "RIN",
    "RVO",
    "crush margin",
    "ethanol",
    "Abercrombie",
    "Hollister",
    "Presort",
    "SendTech",
    "Pitney Bowes",
)
TEXT_QUALITY_CLASSES = {
    "clean_visible_ui",
    "clean_audit_only",
    "boilerplate_or_legal",
    "accounting_policy_or_definition",
    "compensation_or_governance_noise",
    "release_header_or_source_title",
    "fragmented_sentence",
    "too_long_unstructured",
    "missing_context",
    "manual_review_required",
}
_NON_CLEAN_VISIBLE_TEXT_CLASSES = TEXT_QUALITY_CLASSES - {"clean_visible_ui", "clean_audit_only"}
_COMPENSATION_GOVERNANCE_RE = re.compile(
    r"\b(compensation|governance|director|board|officer|proxy|restricted stock|stock award|equity award|cash-)\b",
    re.I,
)
_LEGAL_BOILERPLATE_RE = re.compile(
    r"forward[- ]looking statements|"
    r"safe harbor|"
    r"risk factors|"
    r"may differ materially|"
    r"do not undertake|"
    r"risks related to|"
    r"timing and implementation of changes to existing tariff programs|"
    r"trade policies or arrangements",
    re.I,
)
_ACCOUNTING_DEFINITION_RE = re.compile(
    r"gross profit divided by reported net sales|"
    r"operating income divided by reported net sales|"
    r"\bdivided by\b|"
    r"\bcalculated as\b|"
    r"\bcomputed as\b|"
    r"\bdefined as\b|"
    r"\bdefinition\b|"
    r"\bformula\b",
    re.I,
)
_RELEASE_HEADER_RE = re.compile(
    r"^document\s+.+\breports\s+(first|second|third|fourth)\b|"
    r"\breports\s+(first|second|third|fourth)\s+quarter\b|"
    r"\breports\s+fourth\s+quarter\s+and\s+full\s+year\b|"
    r"\bfiscal\s+\d{4}\s+results\b",
    re.I,
)
_FRAGMENTED_TEXT_RE = re.compile(r"[-–]$|\b(and|of|the|to|from|with|include|including)$", re.I)
_VISIBLE_TEXT_FIELD_SPECS = (
    ("quarter_notes.items", ("note", "commentary", "model_implication", "valuation_implication"), True),
    ("operating_drivers.items", ("driver", "current_read", "why_it_matters"), True),
    ("segments.items", ("note",), True),
    ("normalized_guidance.items", ("source_excerpt", "notes_source"), True),
    ("investment_case.source_evidence", ("source_ref", "section"), False),
)


@dataclass(frozen=True)
class NormalizedDataIssue:
    severity: str
    rule_id: str
    field: str
    message: str
    source_ref: str = ""
    suggested_action: str = ""

    def to_dict(self) -> Dict[str, str]:
        return {
            "severity": self.severity,
            "rule_id": self.rule_id,
            "field": self.field,
            "message": self.message,
            "source_ref": self.source_ref,
            "suggested_action": self.suggested_action,
        }


def validate_normalized_company_data(
    package: Mapping[str, Any],
    *,
    binding_map: Optional[Sequence[Mapping[str, Any]]] = None,
    promotion_requested: bool = False,
) -> List[NormalizedDataIssue]:
    """Return structured pre-render validation issues for a normalized package."""

    issues: List[NormalizedDataIssue] = []
    issues.extend(_validate_field_statuses_and_core_fields(package))
    issues.extend(_validate_guidance(package))
    issues.extend(_validate_parser_noise(package))
    issues.extend(_validate_visible_text_quality(package))
    issues.extend(_validate_share_count_outliers(package))
    issues.extend(_validate_binding_map_gaps(package, binding_map or ()))
    if promotion_requested:
        issues.extend(_validate_investment_case_for_promotion(package))
    issues.extend(_validate_sector_leakage(package))
    return _dedupe_issues(issues)


def classify_normalized_text_quality(
    text: str,
    *,
    field: str = "",
    visible_ui: bool = True,
) -> str:
    """Classify normalized text for visible workbook readiness."""

    clean = re.sub(r"\s+", " ", str(text or "")).strip()
    if not clean:
        return "missing_context"
    if not visible_ui:
        return "clean_audit_only"
    if _COMPENSATION_GOVERNANCE_RE.search(clean):
        return "compensation_or_governance_noise"
    if _LEGAL_BOILERPLATE_RE.search(clean):
        return "boilerplate_or_legal"
    if _ACCOUNTING_DEFINITION_RE.search(clean):
        return "accounting_policy_or_definition"
    if _RELEASE_HEADER_RE.search(clean):
        return "release_header_or_source_title"
    if _FRAGMENTED_TEXT_RE.search(clean):
        return "fragmented_sentence"
    if len(clean) > _visible_text_limit(field):
        return "too_long_unstructured"
    if len(clean.split()) < 3 and (
        "quarter_notes" in field
        or field.endswith(".current_read")
        or field.endswith(".why_it_matters")
    ):
        return "missing_context"
    return "clean_visible_ui"


def build_normalized_text_quality_audit(package: Mapping[str, Any]) -> Dict[str, Any]:
    """Return row-level text quality audit data for normalized visible text fields."""

    rows: List[Dict[str, Any]] = []
    for path, text, source_ref, visible_ui in _iter_audited_text_fields(package):
        classification = classify_normalized_text_quality(text, field=path, visible_ui=visible_ui)
        rows.append(
            {
                "field": path,
                "section": path.split(".", 1)[0],
                "visible_ui": visible_ui,
                "classification": classification,
                "is_clean_visible": classification == "clean_visible_ui",
                "text_length": len(text),
                "text_excerpt": _excerpt(text),
                "source_ref": source_ref,
                "suggested_action": _text_quality_action(classification, visible_ui=visible_ui),
            }
        )
    class_counts: Dict[str, int] = {}
    section_counts: Dict[str, Dict[str, int]] = {}
    for row in rows:
        classification = str(row["classification"])
        class_counts[classification] = class_counts.get(classification, 0) + 1
        section = str(row["section"])
        section_counts.setdefault(section, {})
        section_counts[section][classification] = section_counts[section].get(classification, 0) + 1
    non_clean_visible = [
        row
        for row in rows
        if row["visible_ui"] and row["classification"] in _NON_CLEAN_VISIBLE_TEXT_CLASSES
    ]
    return {
        "version": "0.1.0",
        "row_count": len(rows),
        "non_clean_visible_count": len(non_clean_visible),
        "classification_counts": class_counts,
        "section_classification_counts": section_counts,
        "rows": rows,
    }


def build_mapping_gap_report(
    package: Mapping[str, Any],
    binding_map: Sequence[Mapping[str, Any]],
    *,
    ticker: str = "",
) -> Dict[str, Any]:
    """Build a machine-readable report of required bindings not yet populated."""

    gaps: List[Dict[str, Any]] = []
    for entry in binding_map:
        normalized_field = str(entry.get("normalized_field") or "").strip()
        if not normalized_field:
            continue
        if bool(entry.get("required")) and not _field_is_populated(_path_get(package, normalized_field)):
            gaps.append(
                {
                    "binding_id": entry.get("binding_id", ""),
                    "sheet": entry.get("sheet", ""),
                    "section": entry.get("section", ""),
                    "target": entry.get("target", ""),
                    "shell_zone": entry.get("shell_zone", ""),
                    "anchor_label": entry.get("anchor_label", ""),
                    "named_range": entry.get("named_range", ""),
                    "row_family": entry.get("row_family", ""),
                    "normalized_field": normalized_field,
                    "value_shape": entry.get("value_shape", ""),
                    "source_policy": entry.get("source_policy", ""),
                    "missing_source_behavior": entry.get("missing_source_behavior", ""),
                    "promotion_requirement": entry.get("promotion_requirement", ""),
                    "validation_rule": entry.get("validation_rule", ""),
                }
            )
    return {
        "ticker": str(ticker or _ticker(package) or "").upper(),
        "gap_count": len(gaps),
        "gaps": gaps,
    }


def _validate_field_statuses_and_core_fields(package: Mapping[str, Any]) -> List[NormalizedDataIssue]:
    issues: List[NormalizedDataIssue] = []
    for path, node in _iter_field_nodes(package):
        status = str(node.get("status") or "").strip()
        value = node.get("value")
        source_ref = str(node.get("source_ref") or "")
        if status and status not in FIELD_STATUSES:
            issues.append(
                NormalizedDataIssue(
                    severity="P1",
                    rule_id="invalid_field_status",
                    field=path,
                    message=f"Field status {status!r} is not part of the normalized contract.",
                    source_ref=source_ref,
                    suggested_action="Use one of the documented normalized field statuses.",
                )
            )
        if not bool(node.get("core")):
            continue
        has_reason = bool(
            str(node.get("reason") or node.get("missing_reason") or node.get("suggested_action") or "").strip()
        )
        if _is_empty(value) and (status == "populated" or (status in _MISSING_STATUSES and not has_reason)):
            issues.append(
                NormalizedDataIssue(
                    severity="P1",
                    rule_id="unexplained_empty_core_field",
                    field=path,
                    message="Core normalized field is empty without an adequate missing-data reason.",
                    source_ref=source_ref,
                    suggested_action="Populate the field or set the correct missing/not-applicable status with a reason.",
                )
            )
    return issues


def _validate_guidance(package: Mapping[str, Any]) -> List[NormalizedDataIssue]:
    issues: List[NormalizedDataIssue] = []
    guidance = package.get("normalized_guidance") if isinstance(package, Mapping) else None
    items = guidance.get("items", []) if isinstance(guidance, Mapping) else []
    for idx, item in enumerate(items if isinstance(items, list) else []):
        if not isinstance(item, Mapping):
            continue
        metric = _field_text(item.get("metric"))
        value = _field_text(item.get("value"))
        excerpt = str(item.get("source_excerpt") or item.get("line") or item.get("text") or "")
        blob = " ".join(part for part in (metric, value, excerpt) if part)
        source_ref = _field_source_ref(item.get("metric")) or _field_source_ref(item.get("value"))
        field_path = f"normalized_guidance.items.{idx}"
        if _guidance_metric_misclassified(metric, blob):
            issues.append(
                NormalizedDataIssue(
                    severity="P1",
                    rule_id="guidance_metric_misclassification",
                    field=field_path,
                    message="Guidance metric label does not match the source/value language.",
                    source_ref=source_ref,
                    suggested_action="Reclassify the guidance row before it can feed Promise Progress or Valuation.",
                )
            )
        if _BOILERPLATE_GUIDANCE_RE.search(blob) and not _NUMBER_RE.search(blob):
            issues.append(
                NormalizedDataIssue(
                    severity="P1",
                    rule_id="boilerplate_guidance",
                    field=field_path,
                    message="Guidance candidate appears to be legal/boilerplate text rather than quantified guidance.",
                    source_ref=source_ref,
                    suggested_action="Keep the source as coverage evidence, but do not map it as normalized guidance.",
                )
            )
    return issues


def _validate_parser_noise(package: Mapping[str, Any]) -> List[NormalizedDataIssue]:
    issues: List[NormalizedDataIssue] = []
    for path, text, source_ref in _iter_text_values(package):
        if _PARSER_NOISE_RE.search(text):
            issues.append(
                NormalizedDataIssue(
                    severity="P1",
                    rule_id="parser_noise_snippet",
                    field=path,
                    message="Parser/scaffold noise is present in normalized content.",
                    source_ref=source_ref,
                    suggested_action="Route this to source coverage or manual review; do not render it visibly.",
                )
            )
    return issues


def _validate_visible_text_quality(package: Mapping[str, Any]) -> List[NormalizedDataIssue]:
    issues: List[NormalizedDataIssue] = []
    audit = build_normalized_text_quality_audit(package)
    for row in audit["rows"]:
        classification = str(row["classification"])
        if not row["visible_ui"] or classification not in _NON_CLEAN_VISIBLE_TEXT_CLASSES:
            continue
        field = str(row["field"])
        issues.append(
            NormalizedDataIssue(
                severity="P1",
                rule_id=f"visible_text_quality_{classification}",
                field=field,
                message=f"Visible normalized text is not render-ready: {classification}.",
                source_ref=str(row.get("source_ref") or ""),
                suggested_action=str(row.get("suggested_action") or ""),
            )
        )
    if audit["non_clean_visible_count"] and not issues:
        issues.append(
            NormalizedDataIssue(
                severity="P1",
                rule_id="visible_text_quality_audit_mismatch",
                field="normalized_text_quality",
                message="Text quality audit found non-clean visible rows but validation produced no row-level issue.",
                suggested_action="Keep the audit and validation rule paths in sync before rendering.",
            )
        )
    return issues


def _validate_share_count_outliers(package: Mapping[str, Any]) -> List[NormalizedDataIssue]:
    share_values: List[Tuple[str, float, str]] = []
    for path, node in _iter_field_nodes(package):
        label = path.lower()
        if not any(token in label for token in ("diluted_shares", "share_count", "shares_outstanding")):
            continue
        try:
            value = float(node.get("value"))
        except (TypeError, ValueError):
            continue
        if value > 0:
            share_values.append((path, value, str(node.get("source_ref") or "")))
    if len(share_values) < 2:
        return []
    values = [value for _path, value, _source in share_values]
    low = min(values)
    high = max(values)
    if low <= 0:
        return []
    if high / low < 5:
        return []
    low_path, low_value, low_source = min(share_values, key=lambda item: item[1])
    high_path, high_value, high_source = max(share_values, key=lambda item: item[1])
    return [
        NormalizedDataIssue(
            severity="P1",
            rule_id="share_count_outlier",
            field=f"{low_path};{high_path}",
            message=f"Diluted share count range is implausibly wide ({low_value:g} to {high_value:g}).",
            source_ref=low_source or high_source,
            suggested_action="Review period units and parser mapping before valuation rows are filled.",
        )
    ]


def _validate_binding_map_gaps(
    package: Mapping[str, Any],
    binding_map: Sequence[Mapping[str, Any]],
) -> List[NormalizedDataIssue]:
    issues: List[NormalizedDataIssue] = []
    for entry in binding_map:
        if not bool(entry.get("required")):
            continue
        if str(entry.get("sheet") or "") != "Valuation":
            continue
        normalized_field = str(entry.get("normalized_field") or "").strip()
        if not normalized_field:
            continue
        if _field_is_populated(_path_get(package, normalized_field)):
            continue
        issues.append(
            NormalizedDataIssue(
                severity="P1",
                rule_id="valuation_core_mapping_gap",
                field=normalized_field,
                message="Required Valuation binding has no populated normalized field.",
                source_ref="",
                suggested_action="Add source-backed data or record an explicit mapping gap before rendering.",
            )
        )
    return issues


def _validate_investment_case_for_promotion(package: Mapping[str, Any]) -> List[NormalizedDataIssue]:
    investment_case = package.get("investment_case") if isinstance(package, Mapping) else None
    if not isinstance(investment_case, Mapping):
        return [
            NormalizedDataIssue(
                severity="P1",
                rule_id="placeholder_investment_case",
                field="investment_case",
                message="Investment case section is missing while promotion is requested.",
                suggested_action="Populate a source-backed investment case before promotion.",
            )
        ]
    for path, text, source_ref in _iter_text_values(investment_case, prefix="investment_case"):
        if _PLACEHOLDER_RE.search(text):
            return [
                NormalizedDataIssue(
                    severity="P1",
                    rule_id="placeholder_investment_case",
                    field=path,
                    message="Investment case still contains placeholder/generic content while promotion is requested.",
                    source_ref=source_ref,
                    suggested_action="Replace with source-backed thesis/debate content or block promotion.",
                )
            ]
    for path, node in _iter_field_nodes(investment_case, prefix="investment_case"):
        if bool(node.get("core")) and str(node.get("status") or "") in _MISSING_STATUSES:
            return [
                NormalizedDataIssue(
                    severity="P1",
                    rule_id="placeholder_investment_case",
                    field=path,
                    message="Investment case core field is not promotion-ready.",
                    source_ref=str(node.get("source_ref") or ""),
                    suggested_action="Resolve manual-review and missing-source fields before promotion.",
                )
            ]
    return []


def _validate_sector_leakage(package: Mapping[str, Any]) -> List[NormalizedDataIssue]:
    allowed = _allowed_sector_terms(package)
    issues: List[NormalizedDataIssue] = []
    for path, text, source_ref in _iter_text_values(package):
        low = text.lower()
        for term in _SECTOR_TERMS:
            if term.lower() in allowed:
                continue
            if re.search(r"\b" + re.escape(term).replace(r"\ ", r"\s+") + r"\b", low, flags=re.I):
                issues.append(
                    NormalizedDataIssue(
                        severity="P1",
                        rule_id="unsupported_sector_specific_leakage",
                        field=path,
                        message=f"Unsupported sector-specific term leaked into normalized content: {term}.",
                        source_ref=source_ref,
                        suggested_action="Remove copied sector language unless the profile/source package explicitly allows it.",
                    )
                )
                break
    return issues


def _guidance_metric_misclassified(metric: str, blob: str) -> bool:
    metric_low = metric.lower()
    blob_low = blob.lower()
    if any(token in metric_low for token in ("revenue", "sales", "net sales")):
        return any(
            token in blob_low
            for token in (
                "net income",
                "earnings per share",
                " eps",
                "adjusted ebit",
                "ebitda",
                "free cash flow",
                "operating cash flow",
            )
        )
    if "free cash flow" in metric_low or "fcf" in metric_low:
        return "net sales" in blob_low or "net income" in blob_low
    if "net income" in metric_low:
        return "net sales" in blob_low or "revenue" in blob_low
    return False


def _iter_field_nodes(obj: Any, prefix: str = "") -> Iterable[Tuple[str, Mapping[str, Any]]]:
    if isinstance(obj, Mapping):
        if "value" in obj or ("status" in obj and ("core" in obj or "source_ref" in obj or "reason" in obj)):
            yield prefix or "$", obj
        for key, value in obj.items():
            child_prefix = f"{prefix}.{key}" if prefix else str(key)
            yield from _iter_field_nodes(value, child_prefix)
    elif isinstance(obj, list):
        for idx, value in enumerate(obj):
            child_prefix = f"{prefix}.{idx}" if prefix else str(idx)
            yield from _iter_field_nodes(value, child_prefix)


def _iter_text_values(obj: Any, prefix: str = "") -> Iterable[Tuple[str, str, str]]:
    if isinstance(obj, Mapping):
        if "value" in obj:
            value = obj.get("value")
            if isinstance(value, str) and value:
                yield prefix or "$", value, str(obj.get("source_ref") or "")
        for key, value in obj.items():
            if key == "value":
                continue
            child_prefix = f"{prefix}.{key}" if prefix else str(key)
            yield from _iter_text_values(value, child_prefix)
    elif isinstance(obj, list):
        for idx, value in enumerate(obj):
            child_prefix = f"{prefix}.{idx}" if prefix else str(idx)
            yield from _iter_text_values(value, child_prefix)
    elif isinstance(obj, str) and obj:
        yield prefix or "$", obj, ""


def _iter_audited_text_fields(obj: Mapping[str, Any]) -> Iterable[Tuple[str, str, str, bool]]:
    for collection_path, field_names, visible_ui in _VISIBLE_TEXT_FIELD_SPECS:
        collection = _path_get(obj, collection_path)
        if isinstance(collection, list):
            for idx, item in enumerate(collection):
                if not isinstance(item, Mapping):
                    continue
                for field_name in field_names:
                    value = item.get(field_name)
                    text = _field_text(value)
                    if not text:
                        continue
                    source_ref = _field_source_ref(value)
                    yield f"{collection_path}.{idx}.{field_name}", text, source_ref, visible_ui
            continue
        if isinstance(collection, Mapping):
            for field_name in field_names:
                value = collection.get(field_name)
                text = _field_text(value)
                if not text:
                    continue
                source_ref = _field_source_ref(value)
                yield f"{collection_path}.{field_name}", text, source_ref, visible_ui


def _path_get(obj: Any, dotted_path: str) -> Any:
    current = obj
    for part in dotted_path.split("."):
        if isinstance(current, Mapping):
            if part not in current:
                return None
            current = current[part]
            continue
        if isinstance(current, list):
            try:
                current = current[int(part)]
            except (ValueError, IndexError):
                return None
            continue
        return None
    return current


def _field_is_populated(value: Any) -> bool:
    if not isinstance(value, Mapping):
        return False
    return str(value.get("status") or "") == "populated" and not _is_empty(value.get("value"))


def _field_text(value: Any) -> str:
    if isinstance(value, Mapping):
        return str(value.get("value") or "")
    return str(value or "")


def _field_source_ref(value: Any) -> str:
    if isinstance(value, Mapping):
        return str(value.get("source_ref") or "")
    return ""


def _visible_text_limit(field: str) -> int:
    if "source_excerpt" in field or "notes_source" in field:
        return 260
    if "quarter_notes" in field:
        return 300
    if "segments" in field:
        return 220
    return 240


def _excerpt(text: str, limit: int = 180) -> str:
    clean = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(clean) <= limit:
        return clean
    return clean[: limit - 1].rstrip() + "…"


def _text_quality_action(classification: str, *, visible_ui: bool) -> str:
    if not visible_ui:
        return "Keep as audit/source evidence only; do not render as visible UI text."
    if classification == "clean_visible_ui":
        return "No action required."
    if classification == "boilerplate_or_legal":
        return "Demote to source coverage/manual review; replace only with source-backed quarter commentary."
    if classification == "compensation_or_governance_noise":
        return "Demote governance or compensation snippets to audit evidence; do not show in quarter notes."
    if classification == "accounting_policy_or_definition":
        return "Do not use definitions as operating reads; map a real source-backed operating driver instead."
    if classification == "release_header_or_source_title":
        return "Remove release headers/source titles from visible notes and keep only concise sourced facts."
    if classification == "fragmented_sentence":
        return "Review the parser extraction boundary and rebuild a complete sentence before rendering."
    if classification == "too_long_unstructured":
        return "Condense to a concise source-backed visible summary or demote to audit-only evidence."
    return "Require manual review before rendering this text visibly."


def _is_empty(value: Any) -> bool:
    if value in _EMPTY_VALUES:
        return True
    if isinstance(value, (list, tuple, set, dict)) and not value:
        return True
    return False


def _allowed_sector_terms(package: Mapping[str, Any]) -> set[str]:
    profile = package.get("company_profile") if isinstance(package, Mapping) else {}
    raw = profile.get("allowed_sector_terms", []) if isinstance(profile, Mapping) else []
    if isinstance(raw, Mapping):
        raw = raw.get("value", [])
    if isinstance(raw, str):
        raw = [raw]
    return {str(item).lower() for item in (raw or [])}


def _ticker(package: Mapping[str, Any]) -> str:
    meta = package.get("ticker_metadata") if isinstance(package, Mapping) else {}
    raw = meta.get("ticker") if isinstance(meta, Mapping) else ""
    if isinstance(raw, Mapping):
        raw = raw.get("value", "")
    return str(raw or "")


def _dedupe_issues(issues: Sequence[NormalizedDataIssue]) -> List[NormalizedDataIssue]:
    seen: set[Tuple[str, str, str]] = set()
    out: List[NormalizedDataIssue] = []
    for issue in issues:
        key = (issue.rule_id, issue.field, issue.message)
        if key in seen:
            continue
        seen.add(key)
        out.append(issue)
    return out
