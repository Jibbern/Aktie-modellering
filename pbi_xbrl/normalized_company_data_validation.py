"""Pre-render validation for normalized new-ticker data packages.

This module intentionally has no dependency on workbook writers.  It validates
the normalized data package before any Excel shell/fill step can run.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from pbi_xbrl.json_schema_validation import load_json_strict, validate_json_schema
from pbi_xbrl.new_ticker_guidance_scope import (
    ACTIVE_UPDATE_STAGES,
    CURRENT_GUIDANCE_ROLES,
    guidance_scope_key,
    latest_scope_publications,
    normalize_guidance_scope,
)


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
    r"\bdefinition of\b|"
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
_QUARTERLY_PERIOD_RE = re.compile(r"^\d{4}-Q[1-4]$")
_ANNUAL_PERIOD_RE = re.compile(r"^\d{4}-FY$")
_ALLOWED_UNITS = {
    "$",
    "$m",
    "$bn",
    "USD",
    "USDm",
    "USDbn",
    "%",
    "bps",
    "pp",
    "x",
    "$/share",
    "m shares",
    "shares",
    "count",
    "days",
    "quarters",
    "pts",
    "ratio",
    "stores",
    "visits",
    "m visits",
    "units",
}
_SUPPORTED_SEGMENT_DIMENSIONS = {
    "business_line",
    "reported_segment",
    "operating_segment",
    "geography",
    "brand",
    "product",
    "category",
    "total_company",
}
_NUMERIC_FINANCIAL_FIELDS = {
    "revenue",
    "gross_profit",
    "operating_income",
    "adjusted_ebitda",
    "net_income",
    "eps",
    "operating_cash_flow",
    "free_cash_flow",
    "diluted_shares",
    "capital_expenditures",
}
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_NORMALIZED_SCHEMA = ROOT / "docs" / "normalized_company_data.schema.json"


@dataclass(frozen=True)
class NormalizedDataIssue:
    severity: str
    rule_id: str
    field: str
    message: str
    source_ref: str = ""
    suggested_action: str = ""
    normalized_path: str = ""
    business_row_key: str = ""
    binding_id: str = ""
    evidence_key: str = ""
    root_cause: str = ""
    issue_type: str = ""
    canonical_issue_key: str = ""
    affected_period: str = ""
    visibility_disposition: str = ""
    promotion_blocking: Optional[bool] = None
    render_blocking: Optional[bool] = None

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "severity": self.severity,
            "rule_id": self.rule_id,
            "field": self.field,
            "message": self.message,
            "source_ref": self.source_ref,
            "suggested_action": self.suggested_action,
        }
        optional_values = {
            "normalized_path": self.normalized_path,
            "business_row_key": self.business_row_key,
            "binding_id": self.binding_id,
            "evidence_key": self.evidence_key,
            "root_cause": self.root_cause,
            "issue_type": self.issue_type,
            "canonical_issue_key": self.canonical_issue_key,
            "affected_period": self.affected_period,
            "visibility_disposition": self.visibility_disposition,
        }
        payload.update({key: value for key, value in optional_values.items() if value not in (None, "")})
        if self.promotion_blocking is not None:
            payload["promotion_blocking"] = self.promotion_blocking
        if self.render_blocking is not None:
            payload["render_blocking"] = self.render_blocking
        return payload


def validate_normalized_company_data_schema(
    package: Mapping[str, Any],
    *,
    schema_path: Path | str = DEFAULT_NORMALIZED_SCHEMA,
) -> List[NormalizedDataIssue]:
    """Validate package shape against the checked-in JSON Schema contract.

    The dependency-free evaluator implements every assertion keyword used by
    the checked-in Draft 2020-12 contracts and fails closed on unknown schema
    keywords. Duplicate JSON object keys are rejected while loading the schema.
    """

    path = Path(schema_path)
    try:
        schema = load_json_strict(path)
    except Exception as exc:
        return [
            NormalizedDataIssue(
                severity="P1",
                rule_id="normalized_schema_unavailable",
                field="$",
                message=f"Could not load normalized data schema: {exc}",
                suggested_action="Restore docs/normalized_company_data.schema.json before planning or rendering.",
            )
        ]

    failures = validate_json_schema(package, schema)
    return [
        NormalizedDataIssue(
            severity="P1",
            rule_id=f"normalized_schema_{keyword}",
            field=field,
            message=message,
            suggested_action="Correct the normalized package to satisfy docs/normalized_company_data.schema.json.",
        )
        for field, keyword, message in failures
    ]

def validate_normalized_company_data(
    package: Mapping[str, Any],
    *,
    binding_map: Optional[Sequence[Mapping[str, Any]]] = None,
    promotion_requested: bool = False,
    validate_schema: bool = True,
) -> List[NormalizedDataIssue]:
    """Return structured pre-render validation issues for a normalized package."""

    bindings = list(binding_map or ())
    issues: List[NormalizedDataIssue] = []
    # Shape must be established before semantic rules inspect individual fields.
    if validate_schema:
        issues.extend(validate_normalized_company_data_schema(package))
    issues.extend(_validate_field_statuses_and_core_fields(package))
    issues.extend(_validate_financial_row_domains(package))
    issues.extend(_validate_collection_business_keys(package))
    issues.extend(_validate_company_profile_semantics(package))
    issues.extend(_validate_debt_liquidity_semantics(package))
    issues.extend(_validate_source_backed_core_field_lineage(package, bindings))
    issues.extend(_validate_guidance(package))
    issues.extend(_validate_parser_noise(package))
    issues.extend(_validate_visible_text_quality(package))
    issues.extend(_validate_share_count_outliers(package))
    issues.extend(_validate_binding_map_gaps(package, bindings))
    if promotion_requested:
        issues.extend(_validate_investment_case_for_promotion(package))
    issues.extend(_validate_sector_leakage(package))
    return _dedupe_issues(issues)


def _validate_financial_row_domains(package: Mapping[str, Any]) -> List[NormalizedDataIssue]:
    issues: List[NormalizedDataIssue] = []
    for section in ("quarterly_financials", "annual_financials"):
        rows = _path_get(package, f"{section}.rows")
        if not isinstance(rows, list):
            continue
        for idx, row in enumerate(rows):
            if not isinstance(row, Mapping):
                continue
            period = str(row.get("period") or "")
            period_re = _QUARTERLY_PERIOD_RE if section == "quarterly_financials" else _ANNUAL_PERIOD_RE
            if not period or not period_re.fullmatch(period):
                issues.append(
                    NormalizedDataIssue(
                        severity="P1",
                        rule_id="invalid_period",
                        field=f"{section}.rows.{idx}.period",
                        message=f"{section} period must use {'YYYY-Qn' if section == 'quarterly_financials' else 'YYYY-FY'}.",
                        suggested_action="Normalize the reporting period before planning bindings.",
                    )
                )
            fiscal_year = row.get("fiscal_year")
            expected_year = int(period[:4]) if len(period) >= 4 and period[:4].isdigit() else None
            if not isinstance(fiscal_year, int) or expected_year is None or fiscal_year != expected_year:
                issues.append(
                    NormalizedDataIssue(
                        severity="P1",
                        rule_id="invalid_fiscal_year",
                        field=f"{section}.rows.{idx}.fiscal_year",
                        message="fiscal_year must be an integer matching the normalized period.",
                        suggested_action="Normalize fiscal period keys before planning bindings.",
                    )
                )
            if section == "quarterly_financials":
                fiscal_quarter = row.get("fiscal_quarter")
                expected_quarter = int(period[-1]) if _QUARTERLY_PERIOD_RE.fullmatch(period) else None
                if not isinstance(fiscal_quarter, int) or fiscal_quarter != expected_quarter:
                    issues.append(
                        NormalizedDataIssue(
                            severity="P1",
                            rule_id="invalid_fiscal_quarter",
                            field=f"{section}.rows.{idx}.fiscal_quarter",
                            message="fiscal_quarter must be 1-4 and match the normalized period.",
                            suggested_action="Normalize quarterly business keys before planning bindings.",
                        )
                    )
            for field_name in _NUMERIC_FINANCIAL_FIELDS:
                node = row.get(field_name)
                if not isinstance(node, Mapping) or str(node.get("status") or "") != "populated":
                    continue
                value = node.get("value")
                source_ref = str(node.get("source_ref") or "")
                field_path = f"{section}.rows.{idx}.{field_name}"
                if not isinstance(value, (int, float)) or isinstance(value, bool):
                    issues.append(
                        NormalizedDataIssue(
                            severity="P1",
                            rule_id="invalid_numeric_value_type",
                            field=field_path,
                            message="A populated financial metric must contain a numeric value.",
                            source_ref=source_ref,
                            suggested_action="Keep source text in evidence and normalize the numeric value separately.",
                        )
                    )
                    continue
                unit = str(node.get("unit") or "")
                if unit not in _ALLOWED_UNITS:
                    issues.append(
                        NormalizedDataIssue(
                            severity="P1",
                            rule_id="invalid_unit",
                            field=field_path,
                            message="A populated numeric financial metric requires a valid unit.",
                            source_ref=source_ref,
                            suggested_action="Use a normalized unit such as $m, %, x, $/share, or m shares.",
                        )
                    )
    segments = _path_get(package, "segments.items")
    if isinstance(segments, list):
        for idx, item in enumerate(segments):
            if not isinstance(item, Mapping):
                continue
            has_dimension = "dimension" in item
            has_member = "member" in item
            dimension = str(item.get("dimension") or "").strip()
            if has_dimension != has_member or (has_dimension and (not dimension or not str(item.get("member") or "").strip())) or (dimension and dimension not in _SUPPORTED_SEGMENT_DIMENSIONS):
                issues.append(
                    NormalizedDataIssue(
                        severity="P1",
                        rule_id="invalid_dimension",
                        field=f"segments.items.{idx}",
                        message="Segment rows require a supported dimension and a non-empty member.",
                        suggested_action="Normalize segment taxonomy before a dimension/member binding is planned.",
                    )
                )
    return issues


def _validate_collection_business_keys(package: Mapping[str, Any]) -> List[NormalizedDataIssue]:
    specs = (
        ("company_profile.revenue_streams", ("member",)),
        ("quarterly_financials.rows", ("period",)),
        ("annual_financials.rows", ("period",)),
        ("normalized_guidance.items", ("metric", "horizon", "source_date", "evidence_key")),
        ("segments.items", ("dimension", "member", "period", "metric")),
        ("operating_drivers.items", ("topic", "period", "driver_type", "driver", "evidence_key")),
        ("quarter_notes.items", ("quarter", "theme", "metric", "evidence_key")),
        ("valuation_outputs.items", ("metric", "as_of")),
    )
    issues: List[NormalizedDataIssue] = []
    for collection_path, key_fields in specs:
        rows = _path_get(package, collection_path)
        if not isinstance(rows, list):
            continue
        seen: set[tuple[str, ...]] = set()
        for idx, row in enumerate(rows):
            if not isinstance(row, Mapping):
                continue
            values = tuple(str(_normalized_value(_path_get(row, field)) or "").strip() for field in key_fields)
            if any(not value for value in values):
                issues.append(
                    NormalizedDataIssue(
                        severity="P1",
                        rule_id="invalid_business_row_key",
                        field=f"{collection_path}.{idx}",
                        message="Business row key is missing: " + ", ".join(key_fields[position] for position, value in enumerate(values) if not value) + ".",
                        suggested_action="Populate every business key before planner selection.",
                    )
                )
                continue
            if values in seen:
                issues.append(
                    NormalizedDataIssue(
                        severity="P1",
                        rule_id="duplicate_business_row_key",
                        field=f"{collection_path}.{idx}",
                        message=f"Duplicate business row key {'|'.join(values)!r}.",
                        suggested_action="Reconcile duplicate evidence before constructing the normalized package.",
                    )
                )
                continue
            seen.add(values)
    return issues


def _validate_company_profile_semantics(package: Mapping[str, Any]) -> List[NormalizedDataIssue]:
    issues: List[NormalizedDataIssue] = []
    description = str(_normalized_value(_path_get(package, "company_profile.business_description")) or "").strip()
    strategic_context = str(_normalized_value(_path_get(package, "company_profile.strategic_context")) or "").strip()
    if description and strategic_context and description.casefold() == strategic_context.casefold():
        issues.append(
            NormalizedDataIssue(
                severity="P1",
                rule_id="strategic_context_not_distinct",
                field="company_profile.strategic_context",
                message="Strategic context duplicates the company description instead of describing the current investor setup.",
                suggested_action="Normalize a distinct source-backed strategic context before planning SUMMARY.",
            )
        )

    streams = _path_get(package, "company_profile.revenue_streams")
    if not isinstance(streams, list):
        return issues
    percent_total = 0.0
    percent_rows = 0
    for idx, row in enumerate(streams):
        if not isinstance(row, Mapping):
            continue
        path = f"company_profile.revenue_streams.{idx}"
        mix = _normalized_value(row.get("mix"))
        unit = str(row.get("unit") or "")
        period = str(row.get("period") or "")
        source_ref = str(row.get("source_ref") or _field_source_ref(row.get("mix")) or "")
        if not isinstance(mix, (int, float)) or isinstance(mix, bool):
            issues.append(
                NormalizedDataIssue(
                    severity="P1",
                    rule_id="revenue_stream_mix_not_numeric",
                    field=f"{path}.mix",
                    message="Revenue-stream mix must be numeric; narrative revenue-model text cannot enter a mix cell.",
                    source_ref=source_ref,
                    suggested_action="Keep narrative business-model text separate and normalize member-level mix values.",
                )
            )
        elif unit == "%":
            percent_total += float(mix)
            percent_rows += 1
        if not re.fullmatch(r"\d{4}-(?:Q[1-4]|FY)|\d{4}-\d{2}-\d{2}", period):
            issues.append(
                NormalizedDataIssue(
                    severity="P1",
                    rule_id="revenue_stream_period_invalid",
                    field=f"{path}.period",
                    message="Revenue-stream period must be YYYY-Qn, YYYY-FY, or an ISO as-of date.",
                    source_ref=source_ref,
                    suggested_action="Attach the source period/as-of key to the normalized revenue-stream row.",
                )
            )
        if not source_ref:
            issues.append(
                NormalizedDataIssue(
                    severity="P1",
                    rule_id="missing_source_ref",
                    field=f"{path}.source_ref",
                    message="Revenue-stream row has no source_ref lineage.",
                    suggested_action="Attach the exact profile/source evidence for the member mix.",
                )
            )
    if percent_rows and not 99.0 <= percent_total <= 101.0:
        issues.append(
            NormalizedDataIssue(
                severity="P2",
                rule_id="revenue_stream_mix_not_reconciled",
                field="company_profile.revenue_streams",
                message=f"Percentage revenue-stream rows sum to {percent_total:.2f}% rather than approximately 100%.",
                suggested_action="Reconcile scope, period, and omitted members before promotion.",
            )
        )
    return issues


def _parse_liquidity_date(
    raw_value: Any,
    *,
    field: str,
    source_ref: str,
    required: bool,
    issues: List[NormalizedDataIssue],
) -> tuple[str, date | None]:
    value = str(raw_value or "").strip()
    if not value:
        if required:
            issues.append(
                NormalizedDataIssue(
                    severity="P1",
                    rule_id="liquidity_date_missing",
                    field=field,
                    message="Liquidity freshness requires an authoritative ISO date.",
                    source_ref=source_ref,
                    suggested_action="Populate the exact source-backed as-of date before using the liquidity value.",
                )
            )
        return value, None
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        parsed = None
    else:
        try:
            parsed = date.fromisoformat(value)
        except ValueError:
            parsed = None
    if parsed is None:
        issues.append(
            NormalizedDataIssue(
                severity="P1",
                rule_id="liquidity_date_invalid",
                field=field,
                message=f"Liquidity date {value!r} is not a valid ISO calendar date.",
                source_ref=source_ref,
                suggested_action="Normalize the source date as YYYY-MM-DD and preserve its evidence reference.",
            )
        )
    return value, parsed


def _validate_liquidity_freshness_contract(section: Mapping[str, Any]) -> List[NormalizedDataIssue]:
    issues: List[NormalizedDataIssue] = []
    freshness = section.get("liquidity_freshness") if isinstance(section.get("liquidity_freshness"), Mapping) else {}
    if not freshness:
        return issues

    total = section.get("total_liquidity") if isinstance(section.get("total_liquidity"), Mapping) else {}
    display = section.get("summary_liquidity_display") if isinstance(section.get("summary_liquidity_display"), Mapping) else {}
    as_of = section.get("as_of_date") if isinstance(section.get("as_of_date"), Mapping) else {}
    summary_as_of_field = section.get("summary_as_of_date") if isinstance(section.get("summary_as_of_date"), Mapping) else {}
    source_ref = str(freshness.get("source_ref") or total.get("source_ref") or "")
    disposition = str(freshness.get("disposition") or "")
    requires_liquidity_date = disposition in {"current", "stale_but_displayable_with_date"} or str(total.get("status") or "") == "populated"

    authoritative_summary_text, authoritative_summary_date = _parse_liquidity_date(
        _normalized_value(summary_as_of_field),
        field="debt_liquidity.summary_as_of_date",
        source_ref=source_ref,
        required=True,
        issues=issues,
    )
    freshness_summary_text, freshness_summary_date = _parse_liquidity_date(
        freshness.get("summary_as_of"),
        field="debt_liquidity.liquidity_freshness.summary_as_of",
        source_ref=source_ref,
        required=True,
        issues=issues,
    )
    authoritative_liquidity_text, authoritative_liquidity_date = _parse_liquidity_date(
        _normalized_value(as_of),
        field="debt_liquidity.as_of_date",
        source_ref=source_ref,
        required=requires_liquidity_date,
        issues=issues,
    )
    freshness_liquidity_text, freshness_liquidity_date = _parse_liquidity_date(
        freshness.get("liquidity_as_of"),
        field="debt_liquidity.liquidity_freshness.liquidity_as_of",
        source_ref=source_ref,
        required=requires_liquidity_date,
        issues=issues,
    )

    if (
        authoritative_summary_text
        and freshness_summary_text
        and authoritative_summary_text != freshness_summary_text
    ):
        issues.append(
            NormalizedDataIssue(
                severity="P1",
                rule_id="liquidity_summary_as_of_conflict",
                field="debt_liquidity.liquidity_freshness.summary_as_of",
                message="Liquidity freshness summary_as_of conflicts with debt_liquidity.summary_as_of_date.",
                source_ref=source_ref,
                suggested_action="Use one authoritative SUMMARY as-of date across the normalized section.",
            )
        )
    if (
        authoritative_liquidity_text
        and freshness_liquidity_text
        and authoritative_liquidity_text != freshness_liquidity_text
    ):
        issues.append(
            NormalizedDataIssue(
                severity="P1",
                rule_id="liquidity_as_of_conflict",
                field="debt_liquidity.liquidity_freshness.liquidity_as_of",
                message="Liquidity freshness liquidity_as_of conflicts with debt_liquidity.as_of_date.",
                source_ref=source_ref,
                suggested_action="Use one authoritative liquidity as-of date for the total, components, display and freshness contract.",
            )
        )

    total_period = str(total.get("period") or "").strip()
    if total_period and authoritative_liquidity_text and total_period != authoritative_liquidity_text:
        issues.append(
            NormalizedDataIssue(
                severity="P1",
                rule_id="liquidity_period_mismatch",
                field="debt_liquidity.total_liquidity.period",
                message="Total-liquidity period does not match the authoritative liquidity as-of date.",
                source_ref=source_ref,
                suggested_action="Bind the total and every component to the same source-backed date.",
            )
        )

    component_contract = freshness.get("component_as_of") if isinstance(freshness.get("component_as_of"), Mapping) else {}
    component_fields = {
        "cash": "liquidity_cash",
        "revolver": "revolver_availability",
        "other": "other_available_liquidity",
    }
    populated_component_dates: dict[str, str] = {}
    for component_key, section_field in component_fields.items():
        component = section.get(section_field) if isinstance(section.get(section_field), Mapping) else {}
        contract_value = str(component_contract.get(component_key) or "").strip()
        if contract_value:
            _parse_liquidity_date(
                contract_value,
                field=f"debt_liquidity.liquidity_freshness.component_as_of.{component_key}",
                source_ref=str(component.get("source_ref") or source_ref),
                required=False,
                issues=issues,
            )
        if str(component.get("status") or "") != "populated":
            continue
        component_period = str(component.get("period") or "").strip()
        _parse_liquidity_date(
            component_period,
            field=f"debt_liquidity.{section_field}.period",
            source_ref=str(component.get("source_ref") or source_ref),
            required=True,
            issues=issues,
        )
        populated_component_dates[component_key] = component_period
        if contract_value != component_period:
            issues.append(
                NormalizedDataIssue(
                    severity="P1",
                    rule_id="liquidity_component_as_of_conflict",
                    field=f"debt_liquidity.liquidity_freshness.component_as_of.{component_key}",
                    message=f"The {component_key} component date conflicts with its normalized field period.",
                    source_ref=str(component.get("source_ref") or source_ref),
                    suggested_action="Carry the component's exact source date into both the field and freshness contract.",
                )
            )
        if authoritative_liquidity_text and component_period != authoritative_liquidity_text:
            issues.append(
                NormalizedDataIssue(
                    severity="P1",
                    rule_id="liquidity_component_period_mismatch",
                    field=f"debt_liquidity.{section_field}.period",
                    message="Liquidity component period does not match the authoritative total-liquidity date.",
                    source_ref=str(component.get("source_ref") or source_ref),
                    suggested_action="Do not combine liquidity components from different dates.",
                )
            )

    computed_mixed = len({value for value in populated_component_dates.values() if value}) > 1
    declared_mixed = bool(freshness.get("mixed_date_components"))
    if computed_mixed != declared_mixed:
        issues.append(
            NormalizedDataIssue(
                severity="P1",
                rule_id="liquidity_mixed_date_flag_conflict",
                field="debt_liquidity.liquidity_freshness.mixed_date_components",
                message="The mixed-date flag does not agree with the populated component dates.",
                source_ref=source_ref,
                suggested_action="Derive mixed_date_components from the normalized component periods.",
            )
        )
    if declared_mixed or computed_mixed:
        issues.append(
            NormalizedDataIssue(
                severity="P1",
                rule_id="liquidity_mixed_date_components",
                field="debt_liquidity.liquidity_freshness",
                message="SUMMARY liquidity cannot combine components from different dates.",
                source_ref=source_ref,
                suggested_action="Reconcile components to one as-of date or leave the current SUMMARY value blank.",
            )
        )

    required_components_complete = all(
        str((section.get(field) or {}).get("status") or "") == "populated"
        for field in ("liquidity_cash", "revolver_availability")
        if isinstance(section.get(field), Mapping)
    ) and all(isinstance(section.get(field), Mapping) for field in ("liquidity_cash", "revolver_availability"))
    if disposition == "current":
        if authoritative_summary_date != authoritative_liquidity_date or freshness_summary_date != freshness_liquidity_date:
            issues.append(
                NormalizedDataIssue(
                    severity="P1",
                    rule_id="liquidity_current_freshness_mismatch",
                    field="debt_liquidity.liquidity_freshness.disposition",
                    message="Liquidity marked current does not share the authoritative SUMMARY as-of date.",
                    source_ref=source_ref,
                    suggested_action="Use stale_but_displayable_with_date or block the value from current SUMMARY.",
                )
            )
        if not required_components_complete or declared_mixed or computed_mixed:
            issues.append(
                NormalizedDataIssue(
                    severity="P1",
                    rule_id="liquidity_current_components_invalid",
                    field="debt_liquidity.liquidity_freshness.disposition",
                    message="Incomplete or mixed-date components cannot be classified as current liquidity.",
                    source_ref=source_ref,
                    suggested_action="Require same-date cash and revolver evidence before using the current disposition.",
                )
            )
    elif disposition == "stale_but_displayable_with_date":
        if authoritative_summary_date and authoritative_liquidity_date:
            if authoritative_liquidity_date > authoritative_summary_date:
                issues.append(
                    NormalizedDataIssue(
                        severity="P1",
                        rule_id="liquidity_future_date_invalid",
                        field="debt_liquidity.liquidity_freshness.disposition",
                        message="A future liquidity date cannot be classified as stale relative to SUMMARY.",
                        source_ref=source_ref,
                        suggested_action="Correct the source dates or block the conflicting liquidity record.",
                    )
                )
            elif authoritative_liquidity_date == authoritative_summary_date:
                issues.append(
                    NormalizedDataIssue(
                        severity="P1",
                        rule_id="liquidity_stale_disposition_invalid",
                        field="debt_liquidity.liquidity_freshness.disposition",
                        message="Liquidity marked stale has the same date as SUMMARY.",
                        source_ref=source_ref,
                        suggested_action="Use the current freshness disposition.",
                    )
                )

    if str(display.get("status") or "") == "populated":
        display_period = str(display.get("period") or "").strip()
        if authoritative_liquidity_text and display_period != authoritative_liquidity_text:
            issues.append(
                NormalizedDataIssue(
                    severity="P1",
                    rule_id="liquidity_display_period_mismatch",
                    field="debt_liquidity.summary_liquidity_display.period",
                    message="Visible liquidity period conflicts with the authoritative liquidity as-of date.",
                    source_ref=str(display.get("source_ref") or source_ref),
                    suggested_action="Date the visible display with the same authoritative liquidity as-of value.",
                )
            )
        visible_dates = re.findall(r"\b\d{4}-\d{2}-\d{2}\b", str(_normalized_value(display) or ""))
        if authoritative_liquidity_text and visible_dates != [authoritative_liquidity_text]:
            rule_id = "stale_liquidity_date_not_visible" if disposition == "stale_but_displayable_with_date" and not visible_dates else "liquidity_visible_date_mismatch"
            issues.append(
                NormalizedDataIssue(
                    severity="P1",
                    rule_id=rule_id,
                    field="debt_liquidity.summary_liquidity_display",
                    message="Visible SUMMARY liquidity must show exactly the authoritative liquidity as-of date.",
                    source_ref=str(display.get("source_ref") or source_ref),
                    suggested_action="Render the authoritative liquidity date once in the normalized display text.",
                )
            )
    elif disposition in {"current", "stale_but_displayable_with_date"}:
        rule_id = (
            "stale_liquidity_date_not_visible"
            if disposition == "stale_but_displayable_with_date"
            else "current_liquidity_date_not_visible"
        )
        issues.append(
            NormalizedDataIssue(
                severity="P1",
                rule_id=rule_id,
                field="debt_liquidity.summary_liquidity_display",
                message="Current or displayable liquidity must have a populated, visibly dated SUMMARY display value.",
                source_ref=source_ref,
                suggested_action="Include the authoritative liquidity as-of date in the visible display field.",
            )
        )
    return issues


def _validate_debt_liquidity_semantics(package: Mapping[str, Any]) -> List[NormalizedDataIssue]:
    section = _path_get(package, "debt_liquidity")
    if not isinstance(section, Mapping):
        return []
    issues: List[NormalizedDataIssue] = _validate_liquidity_freshness_contract(section)
    total = section.get("total_liquidity") if isinstance(section.get("total_liquidity"), Mapping) else {}
    if str(total.get("status") or "") != "populated":
        freshness = section.get("liquidity_freshness") if isinstance(section.get("liquidity_freshness"), Mapping) else {}
        display = section.get("summary_liquidity_display") if isinstance(section.get("summary_liquidity_display"), Mapping) else {}
        disposition = str(freshness.get("disposition") or "")
        if disposition in {"current", "stale_but_displayable_with_date"}:
            issues.append(
                NormalizedDataIssue(
                    severity="P1",
                    rule_id="liquidity_display_without_total",
                    field="debt_liquidity.liquidity_freshness.disposition",
                    message="Current or displayable liquidity requires a populated same-date total.",
                    source_ref=str(freshness.get("source_ref") or ""),
                    suggested_action="Use incomplete_components or blocked_from_current_summary until the total is source-backed.",
                )
            )
        if disposition in {"blocked_from_current_summary", "incomplete_components"} and str(display.get("status") or "") == "populated":
            issues.append(
                NormalizedDataIssue(
                    severity="P1",
                    rule_id="non_displayable_liquidity_has_visible_value",
                    field="debt_liquidity.summary_liquidity_display",
                    message="A blocked or incomplete liquidity record cannot have a populated SUMMARY display value.",
                    source_ref=str(freshness.get("source_ref") or ""),
                    suggested_action="Keep the display field missing and retain source detail in JSON/audit evidence.",
                )
            )
        return issues
    total_value = total.get("value")
    total_source_ref = str(total.get("source_ref") or "")
    total_period = str(total.get("period") or "")
    definition = section.get("liquidity_definition") if isinstance(section.get("liquidity_definition"), Mapping) else {}
    as_of = section.get("as_of_date") if isinstance(section.get("as_of_date"), Mapping) else {}
    definition_value = str(_normalized_value(definition) or "").strip()
    as_of_value = str(_normalized_value(as_of) or "").strip()
    if not isinstance(total_value, (int, float)) or isinstance(total_value, bool):
        issues.append(
            NormalizedDataIssue(
                severity="P1",
                rule_id="total_liquidity_not_numeric",
                field="debt_liquidity.total_liquidity",
                message="Populated total liquidity must contain a numeric value.",
                source_ref=total_source_ref,
                suggested_action="Normalize liquidity components and their sum before binding SUMMARY.",
            )
        )
    if not definition_value:
        issues.append(
            NormalizedDataIssue(
                severity="P1",
                rule_id="liquidity_definition_missing",
                field="debt_liquidity.liquidity_definition",
                message="Populated total liquidity has no definition/scope.",
                source_ref=total_source_ref,
                suggested_action="State which cash, revolver, and other components are included.",
            )
        )
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", as_of_value):
        issues.append(
            NormalizedDataIssue(
                severity="P1",
                rule_id="liquidity_as_of_invalid",
                field="debt_liquidity.as_of_date",
                message="Populated total liquidity requires an ISO as-of date.",
                source_ref=total_source_ref,
                suggested_action="Attach the common component as-of date.",
            )
        )
    component_names = ("liquidity_cash", "revolver_availability", "other_available_liquidity")
    component_values: list[float] = []
    for component_name in component_names:
        component = section.get(component_name) if isinstance(section.get(component_name), Mapping) else {}
        status = str(component.get("status") or "")
        if status == "populated":
            value = component.get("value")
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                issues.append(
                    NormalizedDataIssue(
                        severity="P1",
                        rule_id="liquidity_component_not_numeric",
                        field=f"debt_liquidity.{component_name}",
                        message="A populated liquidity component must be numeric.",
                        source_ref=str(component.get("source_ref") or total_source_ref),
                        suggested_action="Normalize the component value or mark its missing status honestly.",
                    )
                )
            else:
                component_values.append(float(value))
        elif component_name == "revolver_availability" and status in _MISSING_STATUSES:
            issues.append(
                NormalizedDataIssue(
                    severity="P1",
                    rule_id="cash_only_total_liquidity",
                    field="debt_liquidity.total_liquidity",
                    message="Total liquidity is populated while revolver availability is missing.",
                    source_ref=total_source_ref,
                    suggested_action="Leave total liquidity missing until revolver availability is source-backed, or mark the revolver not applicable with evidence.",
                )
            )
    if isinstance(total_value, (int, float)) and not isinstance(total_value, bool) and component_values:
        if abs(float(total_value) - sum(component_values)) > 0.01:
            issues.append(
                NormalizedDataIssue(
                    severity="P1",
                    rule_id="total_liquidity_not_reconciled",
                    field="debt_liquidity.total_liquidity",
                    message="Total liquidity does not reconcile to its populated components.",
                    source_ref=total_source_ref,
                    suggested_action="Reconcile cash, revolver, and other available-liquidity components.",
                )
            )
    freshness = section.get("liquidity_freshness") if isinstance(section.get("liquidity_freshness"), Mapping) else {}
    display = section.get("summary_liquidity_display") if isinstance(section.get("summary_liquidity_display"), Mapping) else {}
    disposition = str(freshness.get("disposition") or "")
    if disposition == "blocked_from_current_summary" and str(display.get("status") or "") == "populated":
        issues.append(
            NormalizedDataIssue(
                severity="P1",
                rule_id="blocked_liquidity_has_visible_value",
                field="debt_liquidity.summary_liquidity_display",
                message="Liquidity blocked from current SUMMARY still has a populated display value.",
                source_ref=str(freshness.get("source_ref") or total_source_ref),
                suggested_action="Clear the display field and retain the value in JSON/audit evidence only.",
            )
        )
    elif disposition == "incomplete_components" and (
        str(total.get("status") or "") == "populated" or str(display.get("status") or "") == "populated"
    ):
        issues.append(
            NormalizedDataIssue(
                severity="P1",
                rule_id="incomplete_liquidity_is_populated",
                field="debt_liquidity.total_liquidity",
                message="Incomplete liquidity components cannot produce a populated total or SUMMARY display.",
                source_ref=str(freshness.get("source_ref") or total_source_ref),
                suggested_action="Keep the value missing until same-date components are source-backed.",
            )
        )
    return issues


def _normalized_value(value: Any) -> Any:
    if isinstance(value, Mapping) and "status" in value:
        return value.get("value") if str(value.get("status") or "") == "populated" else None
    return value


def _validate_source_backed_core_field_lineage(
    package: Mapping[str, Any],
    bindings: Sequence[Mapping[str, Any]],
) -> List[NormalizedDataIssue]:
    issues: List[NormalizedDataIssue] = []
    for path, node in _iter_field_nodes(package):
        if not bool(node.get("core")) or str(node.get("status") or "") != "populated":
            continue
        if str(node.get("source_ref") or "").strip():
            continue
        issues.append(
            NormalizedDataIssue(
                severity="P1",
                rule_id="missing_source_ref",
                field=path,
                message="A populated source-backed core field is missing source_ref lineage.",
                suggested_action="Attach the selected evidence source before planning or rendering.",
            )
        )
    return issues


def _canonical_collection_path(path: str) -> str:
    return re.sub(r"\.\d+(?=\.|$)", ".0", path)


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
        # Collection cardinality, row selection, and overflow are owned by the
        # binding planner. Looking only at `.items.0` creates false-green or
        # false-red coverage reports for typed row contracts.
        if entry.get("row_selector") is not None or str(entry.get("planning_mode") or "") == "formula_owned":
            continue
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
    latest_publication = max(
        (
            str(item.get("publication_date") or "")
            for item in items
            if isinstance(item, Mapping) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(item.get("publication_date") or ""))
        ),
        default="",
    )
    guidance_rows = [item for item in items if isinstance(item, Mapping)] if isinstance(items, list) else []
    latest_rows = [item for item in guidance_rows if str(item.get("publication_date") or "") == latest_publication]
    latest_reporting_period = max((str(item.get("stated_in_period") or "") for item in latest_rows), default="")
    latest_horizon_year = max((_guidance_horizon_year(_field_text(item.get("horizon"))) for item in latest_rows), default=0)
    scope_publications = latest_scope_publications(guidance_rows)
    superseded_evidence_keys = {
        str(key)
        for item in guidance_rows
        for key in (item.get("supersedes_evidence_keys") or [])
        if isinstance(key, str) and key
    }
    for idx, item in enumerate(items if isinstance(items, list) else []):
        if not isinstance(item, Mapping):
            continue
        metric = _field_text(item.get("metric"))
        value = _field_text(item.get("value"))
        excerpt = str(item.get("source_excerpt") or item.get("line") or item.get("text") or "")
        blob = " ".join(part for part in (metric, value, excerpt) if part)
        source_ref = _field_source_ref(item.get("metric")) or _field_source_ref(item.get("value"))
        field_path = f"normalized_guidance.items.{idx}"
        publication_date = str(item.get("publication_date") or "")
        source_date = str(item.get("source_date") or "")
        stated_in_period = str(item.get("stated_in_period") or "")
        horizon = _field_text(item.get("horizon"))
        display_role = str(item.get("display_role") or "")
        update_stage = str(item.get("update_stage") or "")
        evidence_key = str(item.get("evidence_key") or "")
        normalized_scope = normalize_guidance_scope(item)
        scope_latest_publication = scope_publications.get(guidance_scope_key(item), "")
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", publication_date):
            issues.append(
                NormalizedDataIssue(
                    severity="P1",
                    rule_id="guidance_publication_date_invalid",
                    field=f"{field_path}.publication_date",
                    message="Guidance publication_date must be an ISO date independent of the reporting period.",
                    source_ref=source_ref,
                    suggested_action="Extract the actual document publication date.",
                )
            )
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", source_date):
            issues.append(
                NormalizedDataIssue(
                    severity="P1",
                    rule_id="guidance_source_date_invalid",
                    field=f"{field_path}.source_date",
                    message="Guidance source_date must be the source/reporting-period end date in ISO form.",
                    source_ref=source_ref,
                    suggested_action="Keep source period timing separate from publication_date.",
                )
            )
        if not _QUARTERLY_PERIOD_RE.fullmatch(stated_in_period):
            issues.append(
                NormalizedDataIssue(
                    severity="P1",
                    rule_id="guidance_reporting_period_invalid",
                    field=f"{field_path}.stated_in_period",
                    message="Guidance stated_in_period must use YYYY-Qn and represent the source reporting period.",
                    source_ref=source_ref,
                    suggested_action="Normalize the reporting-period key independently from publication_date and horizon.",
                )
            )
        if (
            latest_publication
            and publication_date == latest_publication
            and scope_latest_publication == publication_date
            and normalized_scope.fiscal_year
            and normalized_scope.fiscal_year >= int(publication_date[:4])
            and update_stage in ACTIVE_UPDATE_STAGES
            and display_role == "history"
        ):
            issues.append(
                NormalizedDataIssue(
                    severity="P1",
                    rule_id="current_guidance_visibility_misclassified",
                    field=f"{field_path}.display_role",
                    message="Latest-publication guidance for a current/future horizon is classified as history.",
                    source_ref=source_ref,
                    suggested_action="Classify it deterministically as current_primary or current_secondary.",
                )
            )
        if display_role in CURRENT_GUIDANCE_ROLES:
            horizon_year = _guidance_horizon_year(horizon)
            same_scope_newer = bool(scope_latest_publication and scope_latest_publication > publication_date)
            explicitly_superseded = bool(str(item.get("superseded_by_evidence_key") or "")) or evidence_key in superseded_evidence_keys
            stale_horizon = bool(
                latest_publication
                and publication_date < latest_publication
                and horizon_year
                and latest_horizon_year
                and horizon_year < latest_horizon_year
            )
            stale_reporting_context = bool(
                latest_publication
                and publication_date < latest_publication
                and stated_in_period
                and latest_reporting_period
                and stated_in_period < latest_reporting_period
                and horizon_year
                and latest_horizon_year
                and horizon_year < latest_horizon_year
            )
            withdrawn_current = update_stage not in ACTIVE_UPDATE_STAGES
            if same_scope_newer or explicitly_superseded or stale_horizon or stale_reporting_context or withdrawn_current:
                reasons = []
                if same_scope_newer:
                    reasons.append("a newer row exists for the same metric/horizon")
                if explicitly_superseded:
                    reasons.append("the evidence is explicitly superseded")
                if stale_horizon or stale_reporting_context:
                    reasons.append("the row belongs to an older reporting/horizon context")
                if withdrawn_current:
                    reasons.append("withdrawn guidance cannot remain current")
                issues.append(
                    NormalizedDataIssue(
                        severity="P1",
                        rule_id="stale_guidance_visibility_misclassified",
                        field=f"{field_path}.display_role",
                        message="Guidance is marked current even though " + "; ".join(dict.fromkeys(reasons)) + ".",
                        source_ref=source_ref,
                    suggested_action="Move superseded guidance to history/audit or document an explicit active carry-forward relationship.",
                )
            )
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


def _guidance_horizon_year(horizon: str) -> int:
    match = re.search(r"(?:FY)?(20\d{2})", horizon)
    return int(match.group(1)) if match else 0


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
        # A row contract cannot be evaluated by probing `.items.0` or a raw
        # period label. The JSON-only planner owns row selection, cardinality,
        # source lineage, and overflow for these bindings.
        if entry.get("row_selector") is not None or str(entry.get("planning_mode") or "") == "formula_owned":
            continue
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
