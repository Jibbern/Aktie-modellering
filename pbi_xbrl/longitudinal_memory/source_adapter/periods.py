"""Explicit source-evidence and calendar-hint period reconciliation."""
from __future__ import annotations

import calendar
import itertools
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Literal, Mapping

from pbi_xbrl.longitudinal_memory.calendar_rules import (
    CALENDAR_YEAR_RULE_ID,
    FISCAL_CALENDAR_RULES,
    SOURCE_LABELLED_52_53_WEEK_RULE_ID,
)

from .html import derive_fiscal_label_semantics
from .types import ExtractedEvidence, MappingError, SourceSet, text_sha256


INCLUSIVE_WEEKS_RULE = "rule:core:inclusive-weeks-ending@1"
REVIEWED_HORIZON_RULE = "rule:core:reviewed-calendar-horizon@1"
CONTIGUOUS_REVIEWED_HORIZON_RULE = (
    "rule:core:contiguous-reviewed-fiscal-horizon@1"
)
REVIEWED_MONTH_RULE = "rule:core:reviewed-relative-month@1"
SOURCE_LABELLED_CALENDAR_RULE = SOURCE_LABELLED_52_53_WEEK_RULE_ID
CALENDAR_YEAR_FISCAL_RULE = CALENDAR_YEAR_RULE_ID

_MONTH_DATE = (
    r"(?:January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+[0-9]{1,2},\s+[0-9]{4}"
)
_WEEK_END_CLAIM = re.compile(
    rf"(?P<weeks>[0-9]+|[A-Za-z]+(?:-[A-Za-z]+)?)\s+Weeks?\s+Ended\s+"
    rf"(?P<end>{_MONTH_DATE})",
    flags=re.IGNORECASE,
)
FiscalLabelPeriodType = Literal[
    "fiscal_quarter",
    "fiscal_year",
    "fiscal_ytd",
    "trailing_four_quarters",
    "unspecified_fiscal_context",
]
FiscalDurationClass = Literal[
    "fiscal_quarter_duration",
    "fiscal_year_duration",
    "fiscal_ytd_duration",
    "trailing_four_quarters_duration",
]


@dataclass(frozen=True, slots=True)
class NormalizedFiscalLabelClaim:
    source_occurrence_id: str
    claim_kind: str
    fiscal_year: int | None
    period_type: FiscalLabelPeriodType
    fiscal_quarter: int | None
    fiscal_ordinal: int | None
    claim_specificity: str
    source_text: str
    locator_identity: str
    extraction_method: str
    digest: str


@dataclass(frozen=True, slots=True)
class ReconciledFiscalPeriodTuple:
    fiscal_year: int
    period_type: FiscalLabelPeriodType
    fiscal_quarter: int | None
    fiscal_ordinal: int
    start_date: date
    end_date: date
    duration_class: FiscalDurationClass
    duration_days: int
    week_count: int | None
    fiscal_calendar_id: str
_ONES = {
    "zero": 0,
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
    "nineteen": 19,
}
_TENS = {"twenty": 20, "thirty": 30, "forty": 40, "fifty": 50}


def _derive_start(period: Mapping[str, Any]) -> date:
    end = date.fromisoformat(str(period["end_date"]))
    rule_id = str(period["start_rule_id"])
    declared_start = period.get("start_date")
    weeks = period.get("week_count")
    if rule_id == INCLUSIVE_WEEKS_RULE:
        if weeks is None:
            raise MappingError("Inclusive week derivation requires an explicit week count.")
        derived = end - timedelta(days=int(weeks) * 7 - 1)
        if declared_start is not None and date.fromisoformat(str(declared_start)) != derived:
            raise MappingError("Declared fiscal start conflicts with inclusive week derivation.")
        return derived
    if rule_id == REVIEWED_HORIZON_RULE:
        raise MappingError("Reviewed fiscal horizons must replay their exact authority tuple.")
    if rule_id == REVIEWED_MONTH_RULE:
        if declared_start is None:
            raise MappingError(f"{rule_id!r} requires an explicit reviewed start date.")
        return date.fromisoformat(str(declared_start))
    if rule_id == CALENDAR_YEAR_FISCAL_RULE:
        if declared_start is None:
            raise MappingError("Calendar-year fiscal periods require one declared source-replayed start.")
        return date.fromisoformat(str(declared_start))
    raise MappingError(f"Unknown period start rule {rule_id!r}.")


def _week_number(value: str) -> int:
    normalized = value.strip().casefold()
    if normalized.isdigit():
        return int(normalized)
    if normalized in _ONES:
        return _ONES[normalized]
    parts = normalized.replace("-", " ").split()
    if len(parts) == 2 and parts[0] in _TENS and parts[1] in _ONES:
        return _TENS[parts[0]] + _ONES[parts[1]]
    raise MappingError(f"Unsupported source week-count wording {value!r}.")


def _period_claims(excerpt: str) -> set[tuple[int, date]]:
    return {
        (
            _week_number(match.group("weeks")),
            datetime.strptime(match.group("end"), "%B %d, %Y").date(),
        )
        for match in _WEEK_END_CLAIM.finditer(excerpt)
    }


def _assertion_period_key(assertion: Mapping[str, Any]) -> str | None:
    for field in (
        "period_key",
        "horizon_period_key",
        "deadline_period_key",
        "statement_period_key",
        "effective_period_key",
    ):
        if field in assertion and assertion[field] is not None:
            return str(assertion[field])
    return None


_REPLAYED_FISCAL_LABEL_FIELDS = frozenset(
    {
        "claim_key",
        "claim_kind",
        "fiscal_year",
        "period_type",
        "fiscal_quarter",
        "claim_specificity",
        "source_text",
        "locator_identity",
        "match_ordinal",
        "extraction_method_id",
        "digest",
    }
)


def _atomic_error(raw: Mapping[str, Any], message: str) -> MappingError:
    return MappingError(
        f"Mandatory Needs Review: atomic fiscal-label reconciliation for "
        f"{raw['period_id']!r} {message}."
    )


def _ordinal_for(
    rule: Mapping[str, Any],
    *,
    fiscal_year: int,
    period_type: FiscalLabelPeriodType,
    fiscal_quarter: int | None,
) -> int:
    quarters_per_year = int(rule["quarters_per_year"])
    if period_type in {"fiscal_quarter", "fiscal_ytd"}:
        if fiscal_quarter is None:
            raise MappingError("Fiscal quarter/YTD ordinal derivation requires an explicit quarter.")
        ordinal_quarter = fiscal_quarter
    elif period_type in {"fiscal_year", "trailing_four_quarters"}:
        ordinal_quarter = quarters_per_year
    else:
        raise MappingError(f"Cannot derive a fiscal ordinal for {period_type!r}.")
    anchor_index = (
        int(rule["ordinal_anchor_fiscal_year"]) * quarters_per_year
        + int(rule["ordinal_anchor_fiscal_quarter"])
    )
    current_index = fiscal_year * quarters_per_year + ordinal_quarter
    return int(rule["ordinal_anchor"]) + current_index - anchor_index


def _normalize_fiscal_label_claims(
    raw: Mapping[str, Any],
    evidence_pairs: tuple[tuple[Mapping[str, Any], ExtractedEvidence], ...],
    rule: Mapping[str, Any],
) -> tuple[NormalizedFiscalLabelClaim, ...]:
    normalized: list[NormalizedFiscalLabelClaim] = []
    locator_occurrences: set[tuple[str, str]] = set()
    for occurrence, evidence in evidence_pairs:
        raw_claims = evidence.diagnostics.get("fiscal_label_claims")
        if not isinstance(raw_claims, (list, tuple)) or not raw_claims:
            raise _atomic_error(raw, "has an eligible occurrence without fiscal-label evidence")
        occurrence_id = str(occurrence["evidence_occurrence_id"])
        for claim in raw_claims:
            if not isinstance(claim, Mapping) or set(claim) != _REPLAYED_FISCAL_LABEL_FIELDS:
                raise _atomic_error(raw, "has malformed or open fiscal-label evidence")
            source_text = str(claim["source_text"])
            locator_identity = str(claim["locator_identity"])
            extraction_method = str(claim["extraction_method_id"])
            digest = str(claim["digest"])
            if not source_text or text_sha256(source_text) != digest:
                raise _atomic_error(raw, "has fiscal-label evidence with a wrong digest")
            locator_occurrence = (occurrence_id, locator_identity)
            if not locator_identity or locator_occurrence in locator_occurrences:
                raise _atomic_error(raw, "has duplicate fiscal-label locator identities")
            locator_occurrences.add(locator_occurrence)

            try:
                replayed = derive_fiscal_label_semantics(source_text)
            except ValueError as exc:
                raise _atomic_error(raw, f"has unparseable fiscal-label source text: {exc}") from exc
            for field in (
                "claim_kind",
                "fiscal_year",
                "period_type",
                "fiscal_quarter",
                "claim_specificity",
            ):
                if claim[field] != replayed[field]:
                    raise _atomic_error(
                        raw,
                        f"has producer fiscal-label metadata that disagrees with source text ({field})",
                    )

            fiscal_year = replayed["fiscal_year"]
            period_type = replayed["period_type"]
            fiscal_quarter = replayed["fiscal_quarter"]
            fiscal_ordinal = None
            if fiscal_year is not None and period_type != "unspecified_fiscal_context":
                fiscal_ordinal = _ordinal_for(
                    rule,
                    fiscal_year=fiscal_year,
                    period_type=period_type,
                    fiscal_quarter=fiscal_quarter,
                )
            normalized.append(
                NormalizedFiscalLabelClaim(
                    source_occurrence_id=occurrence_id,
                    claim_kind=str(replayed["claim_kind"]),
                    fiscal_year=fiscal_year,
                    period_type=period_type,
                    fiscal_quarter=fiscal_quarter,
                    fiscal_ordinal=fiscal_ordinal,
                    claim_specificity=str(replayed["claim_specificity"]),
                    source_text=str(replayed["source_text"]),
                    locator_identity=locator_identity,
                    extraction_method=extraction_method,
                    digest=digest,
                )
            )
    if not normalized:
        raise _atomic_error(raw, "has no reproducible source fiscal-label evidence")
    years_by_occurrence: dict[str, set[int]] = {}
    for claim in normalized:
        if claim.fiscal_year is not None:
            years_by_occurrence.setdefault(claim.source_occurrence_id, set()).add(
                claim.fiscal_year
            )
    for claim in normalized:
        if (
            claim.period_type != "unspecified_fiscal_context"
            and claim.fiscal_year is None
            and len(years_by_occurrence.get(claim.source_occurrence_id, set())) != 1
        ):
            raise _atomic_error(
                raw,
                "has a specific fiscal claim without one same-context fiscal year",
            )
    return tuple(
        sorted(
            normalized,
            key=lambda claim: (claim.locator_identity, claim.source_occurrence_id),
        )
    )


def reviewed_calendar_rule_id(source_set: SourceSet) -> str:
    """Return one reviewed rule identity from typed profile data, never inference."""

    rule = source_set.profile.get("reviewed_calendar_rule")
    if not isinstance(rule, Mapping):
        raise MappingError("Source profile has no reviewed fiscal-calendar rule.")
    rule_id = str(rule.get("rule_id", ""))
    if rule_id not in FISCAL_CALENDAR_RULES:
        raise MappingError(f"Unsupported reviewed fiscal-calendar rule {rule_id!r}.")
    return rule_id


def _reviewed_calendar_rule(source_set: SourceSet) -> Mapping[str, Any]:
    rule = source_set.profile.get("reviewed_calendar_rule")
    if reviewed_calendar_rule_id(source_set) != SOURCE_LABELLED_CALENDAR_RULE:
        raise MappingError("Actual fiscal periods require one supported reviewed calendar rule.")
    if rule.get("display_hint") != source_set.profile.get("calendar_hint"):
        raise MappingError("Profile calendar hint conflicts with its reviewed calendar rule.")
    if rule.get("fiscal_label_basis") != "direct-source":
        raise MappingError("Reviewed calendar rule cannot override direct source fiscal labels.")
    return rule


def _calendar_year_rule(source_set: SourceSet) -> Mapping[str, Any]:
    rule = source_set.profile.get("reviewed_calendar_rule")
    if not isinstance(rule, Mapping) or rule.get("rule_id") != CALENDAR_YEAR_FISCAL_RULE:
        raise MappingError("Calendar-year periods require the reviewed calendar-year fiscal rule.")
    if rule.get("display_hint") != source_set.profile.get("calendar_hint"):
        raise MappingError("Profile calendar hint conflicts with its reviewed calendar-year rule.")
    if rule.get("calendar_basis") != "calendar-year":
        raise MappingError("Calendar-year fiscal rule has the wrong calendar basis.")
    if rule.get("quarter_week_counts") or rule.get("annual_week_counts"):
        raise MappingError("Calendar-year fiscal rule cannot carry 52/53-week duration choices.")
    if set(rule.get("fiscal_year_end_months", ())) != {12}:
        raise MappingError("Calendar-year fiscal rule must end in December.")
    if rule.get("reviewed_horizons"):
        raise MappingError("Calendar-year fiscal periods use exact calendar boundaries, not reviewed 52/53-week horizons.")
    expected_boundaries = {
        "Q1:01-01:03-31",
        "Q2:04-01:06-30",
        "Q3:07-01:09-30",
        "Q4:10-01:12-31",
    }
    if set(rule.get("quarter_boundaries", ())) != expected_boundaries:
        raise MappingError("Calendar-year fiscal rule has incomplete or conflicting quarter boundaries.")
    return rule


def _calendar_expected_dates(
    *, fiscal_year: int, period_type: str, fiscal_quarter: int | None
) -> tuple[date, date, FiscalLabelPeriodType]:
    if period_type == "annual":
        if fiscal_quarter is not None:
            raise MappingError("Calendar-year annual period cannot carry a fiscal quarter.")
        return date(fiscal_year, 1, 1), date(fiscal_year, 12, 31), "fiscal_year"
    if period_type != "quarter" or fiscal_quarter not in {1, 2, 3, 4}:
        raise MappingError("Calendar-year actual periods must be a closed quarter or annual period.")
    boundaries = {
        1: ((1, 1), (3, 31)),
        2: ((4, 1), (6, 30)),
        3: ((7, 1), (9, 30)),
        4: ((10, 1), (12, 31)),
    }
    (start_month, start_day), (end_month, end_day) = boundaries[fiscal_quarter]
    return (
        date(fiscal_year, start_month, start_day),
        date(fiscal_year, end_month, end_day),
        "fiscal_quarter",
    )


def _reconcile_calendar_year_period(
    source_set: SourceSet,
    raw: Mapping[str, Any],
    evidence_by_assertion: Mapping[str, tuple[Mapping[str, Any], ExtractedEvidence]],
    assertions: Mapping[str, Mapping[str, Any]],
    *,
    calendar_id: str,
) -> tuple[ReconciledFiscalPeriodTuple, tuple[tuple[Mapping[str, Any], ExtractedEvidence], ...]]:
    rule = _calendar_year_rule(source_set)
    fiscal_year = int(raw["fiscal_year"])
    fiscal_quarter = raw.get("fiscal_quarter")
    period_type = str(raw["period_type"])
    expected_start, expected_end, normalized_type = _calendar_expected_dates(
        fiscal_year=fiscal_year,
        period_type=period_type,
        fiscal_quarter=fiscal_quarter,
    )
    declared_start = date.fromisoformat(str(raw["start_date"]))
    declared_end = date.fromisoformat(str(raw["end_date"]))
    if (declared_start, declared_end) != (expected_start, expected_end):
        raise _atomic_error(raw, "has non-calendar source boundaries")
    if raw.get("week_count") is not None or bool(raw.get("is_53_week_year")):
        raise _atomic_error(raw, "uses a 52/53-week representation under the calendar-year rule")
    expected_ordinal = _ordinal_for(
        rule,
        fiscal_year=fiscal_year,
        period_type=normalized_type,
        fiscal_quarter=fiscal_quarter,
    )
    if raw.get("fiscal_ordinal") != expected_ordinal:
        raise _atomic_error(raw, "has a fiscal ordinal that conflicts with the reviewed anchor")

    period_key = str(raw["period_key"])
    eligible: list[tuple[Mapping[str, Any], ExtractedEvidence]] = []
    eligible_keys: list[str] = []
    for assertion_key in sorted(assertions):
        assertion = assertions[assertion_key]
        if _assertion_period_key(assertion) != period_key:
            continue
        pair = evidence_by_assertion.get(assertion_key)
        if pair is None:
            raise _atomic_error(raw, f"is missing extracted evidence for {assertion_key!r}")
        occurrence, evidence = pair
        inline = evidence.diagnostics.get("inline_xbrl")
        claims = evidence.diagnostics.get("fiscal_label_claims")
        if inline is None and not claims:
            continue
        if occurrence.get("review_state") not in {"accepted", "reviewed"}:
            raise _atomic_error(raw, f"has blocker-level period evidence in {assertion_key!r}")
        if inline is not None:
            if inline.get("period_instant") is not None:
                raise _atomic_error(raw, "uses an instant Inline XBRL context for a duration period")
            context_dates = (
                date.fromisoformat(str(inline.get("period_start"))),
                date.fromisoformat(str(inline.get("period_end"))),
            )
            if context_dates != (expected_start, expected_end):
                raise _atomic_error(raw, "has an Inline XBRL context that conflicts with calendar boundaries")
            if str(inline.get("entity_identifier")) != str(source_set.profile.get("cik")):
                raise _atomic_error(raw, "has an Inline XBRL context for another entity")
        if claims:
            normalized_claims = _normalize_fiscal_label_claims(raw, (pair,), rule)
            for claim in normalized_claims:
                if claim.fiscal_year is not None and claim.fiscal_year != fiscal_year:
                    raise _atomic_error(raw, "has a direct source label for another fiscal year")
                if claim.period_type != "unspecified_fiscal_context" and claim.period_type != normalized_type:
                    raise _atomic_error(raw, "has a direct source label for another period type")
                if claim.fiscal_quarter is not None and claim.fiscal_quarter != fiscal_quarter:
                    raise _atomic_error(raw, "has a direct source label for another fiscal quarter")
        eligible.append(pair)
        eligible_keys.append(assertion_key)
    declared = raw.get("fiscal_claim_assertion_keys")
    if not isinstance(declared, (list, tuple)) or set(map(str, declared)) != set(eligible_keys):
        raise _atomic_error(raw, "declared calendar evidence membership is incomplete or contains ineligible assertions")
    if not eligible:
        raise _atomic_error(raw, "has no source context or direct fiscal-label evidence")
    duration_days = (expected_end - expected_start).days + 1
    duration_class: FiscalDurationClass = (
        "fiscal_quarter_duration" if normalized_type == "fiscal_quarter" else "fiscal_year_duration"
    )
    return (
        ReconciledFiscalPeriodTuple(
            fiscal_year=fiscal_year,
            period_type=normalized_type,
            fiscal_quarter=fiscal_quarter,
            fiscal_ordinal=expected_ordinal,
            start_date=expected_start,
            end_date=expected_end,
            duration_class=duration_class,
            duration_days=duration_days,
            week_count=None,
            fiscal_calendar_id=calendar_id,
        ),
        tuple(sorted(eligible, key=lambda pair: str(pair[0]["evidence_occurrence_id"]))),
    )


def _eligible_fiscal_evidence_closure(
    source_set: SourceSet,
    raw: Mapping[str, Any],
    assertions: Mapping[str, Mapping[str, Any]],
    evidence_by_assertion: Mapping[
        str, tuple[Mapping[str, Any], ExtractedEvidence]
    ],
    *,
    actual_report_date: str | None,
) -> tuple[tuple[Mapping[str, Any], ExtractedEvidence], ...]:
    period_key = str(raw["period_key"])
    documents = {row.document_key: row for row in source_set.documents}
    eligible: list[tuple[Mapping[str, Any], ExtractedEvidence]] = []
    eligible_assertion_keys: list[str] = []
    actual_assertion_kinds = {
        "period_evidence",
        "numerical_fact",
        "management_statement",
        "company_event",
    }
    for assertion_key in sorted(assertions):
        assertion = assertions[assertion_key]
        if _assertion_period_key(assertion) != period_key:
            continue
        evidence_pair = evidence_by_assertion.get(assertion_key)
        if evidence_pair is None:
            raise _atomic_error(raw, f"is missing extracted evidence for {assertion_key!r}")
        occurrence, evidence = evidence_pair
        claims = evidence.diagnostics.get("fiscal_label_claims")
        if not claims:
            continue
        document = documents.get(str(assertion["document_key"]))
        if document is None:
            raise _atomic_error(raw, f"has an unknown fiscal-label document for {assertion_key!r}")
        if (
            document.review_state not in {"accepted", "reviewed"}
            or assertion.get("review_state") not in {"accepted", "reviewed"}
            or evidence.review_state not in {"accepted", "reviewed"}
            or occurrence.get("review_state") not in {"accepted", "reviewed"}
        ):
            raise _atomic_error(raw, f"has blocker-level fiscal-label evidence in {assertion_key!r}")
        locator_kind = assertion.get("locator", {}).get("locator_kind")
        if locator_kind not in {"html-table", "html-text"}:
            raise _atomic_error(raw, f"has fiscal labels from an ineligible locator in {assertion_key!r}")
        if (
            actual_report_date is not None
            and assertion.get("assertion_kind") in actual_assertion_kinds
            and document.report_date != actual_report_date
        ):
            raise _atomic_error(raw, f"has fiscal-label evidence with a wrong report-period link")
        eligible.append(evidence_pair)
        eligible_assertion_keys.append(assertion_key)

    declared = raw.get("fiscal_claim_assertion_keys")
    if not isinstance(declared, (list, tuple)) or len(set(declared)) != len(declared):
        raise _atomic_error(raw, "has malformed declared fiscal-evidence membership")
    if set(str(value) for value in declared) != set(eligible_assertion_keys):
        raise _atomic_error(
            raw,
            "declared fiscal-evidence membership is incomplete or contains ineligible assertions",
        )
    if not eligible:
        raise _atomic_error(raw, "has no complete eligible fiscal-evidence closure")
    return tuple(
        sorted(eligible, key=lambda pair: str(pair[0]["evidence_occurrence_id"]))
    )


def _reconcile_atomic_fiscal_tuple(
    source_set: SourceSet,
    raw: Mapping[str, Any],
    evidence_pairs: tuple[tuple[Mapping[str, Any], ExtractedEvidence], ...],
    *,
    start: date,
    end: date,
    calendar_id: str,
) -> ReconciledFiscalPeriodTuple:
    rule = _reviewed_calendar_rule(source_set)
    claims = _normalize_fiscal_label_claims(raw, evidence_pairs, rule)
    years = {claim.fiscal_year for claim in claims if claim.fiscal_year is not None}
    specific_types = {
        claim.period_type
        for claim in claims
        if claim.period_type != "unspecified_fiscal_context"
    }
    quarters = {
        claim.fiscal_quarter for claim in claims if claim.fiscal_quarter is not None
    }
    if len(years) != 1:
        raise _atomic_error(raw, "does not resolve exactly one compatible fiscal year")
    if len(specific_types) != 1:
        raise _atomic_error(raw, "does not resolve exactly one compatible period type")

    fiscal_year = next(iter(years))
    period_type = next(iter(specific_types))
    if period_type == "fiscal_quarter":
        if len(quarters) != 1:
            raise _atomic_error(raw, "does not resolve exactly one compatible fiscal quarter")
        fiscal_quarter = next(iter(quarters))
        duration_class: FiscalDurationClass = "fiscal_quarter_duration"
        raw_period_type = "quarter"
        permitted_weeks = {int(value) for value in rule["quarter_week_counts"]}
    elif period_type == "fiscal_ytd":
        if quarters:
            raise _atomic_error(raw, "combines YTD and quarter label semantics")
        fiscal_quarter = raw.get("fiscal_quarter")
        if fiscal_quarter not in {1, 2, 3}:
            raise _atomic_error(raw, "does not declare one compatible YTD fiscal quarter")
        duration_class = "fiscal_ytd_duration"
        raw_period_type = "ytd"
        quarter_week_counts = tuple(int(value) for value in rule["quarter_week_counts"])
        permitted_weeks = {
            sum(values)
            for values in itertools.product(quarter_week_counts, repeat=int(fiscal_quarter))
        }
    elif period_type == "fiscal_year":
        if quarters:
            raise _atomic_error(raw, "combines annual and quarter label semantics")
        fiscal_quarter = None
        duration_class = "fiscal_year_duration"
        raw_period_type = "annual"
        permitted_weeks = {int(value) for value in rule["annual_week_counts"]}
    else:
        raise _atomic_error(raw, f"resolves unsupported period type {period_type!r}")

    if (
        str(raw["start_rule_id"]) == REVIEWED_HORIZON_RULE
        and period_type != "fiscal_year"
    ):
        raise _atomic_error(raw, "reviewed full-year horizon resolved to a non-annual label tuple")

    expected_ordinal = _ordinal_for(
        rule,
        fiscal_year=fiscal_year,
        period_type=period_type,
        fiscal_quarter=fiscal_quarter,
    )
    claim_ordinals = {
        claim.fiscal_ordinal for claim in claims if claim.fiscal_ordinal is not None
    }
    if claim_ordinals and claim_ordinals != {expected_ordinal}:
        raise _atomic_error(raw, "has label-derived ordinals that disagree")

    if int(raw["fiscal_year"]) != fiscal_year:
        raise _atomic_error(raw, "source fiscal year disagrees with the declared period")
    if str(raw["period_type"]) != raw_period_type:
        raise _atomic_error(raw, "source period type disagrees with the declared period")
    if raw.get("fiscal_quarter") != fiscal_quarter:
        raise _atomic_error(raw, "source fiscal quarter disagrees with the declared period")
    if raw.get("fiscal_ordinal") != expected_ordinal:
        raise _atomic_error(raw, "source-derived ordinal disagrees with the declared period")

    weeks = raw.get("week_count")
    if not isinstance(weeks, int) or weeks not in permitted_weeks:
        raise _atomic_error(raw, "duration conflicts with the reconciled period type")
    duration_days = (end - start).days + 1
    if duration_days != weeks * 7:
        raise _atomic_error(raw, "start/end disagree with the reconciled duration")
    if period_type == "fiscal_year":
        if end.month not in {int(value) for value in rule["fiscal_year_end_months"]}:
            raise _atomic_error(raw, "period end conflicts with the reconciled full-year tuple")
        if bool(raw["is_53_week_year"]) != (weeks == 53):
            raise _atomic_error(raw, "has an inconsistent 52/53-week label")

    return ReconciledFiscalPeriodTuple(
        fiscal_year=fiscal_year,
        period_type=period_type,
        fiscal_quarter=fiscal_quarter,
        fiscal_ordinal=expected_ordinal,
        start_date=start,
        end_date=end,
        duration_class=duration_class,
        duration_days=duration_days,
        week_count=weeks,
        fiscal_calendar_id=calendar_id,
    )


def _replay_reviewed_horizon(
    source_set: SourceSet,
    raw: Mapping[str, Any],
    reconciled_by_key: Mapping[str, Mapping[str, Any]],
    *,
    calendar_id: str,
) -> tuple[date, date, str]:
    rule = _reviewed_calendar_rule(source_set)
    horizons = rule.get("reviewed_horizons")
    if not isinstance(horizons, (list, tuple)):
        raise _atomic_error(raw, "has no closed reviewed-horizon authority set")
    matches = [row for row in horizons if row.get("period_key") == raw.get("period_key")]
    if len(matches) != 1:
        raise _atomic_error(raw, "does not resolve one reviewed-horizon authority")
    authority = matches[0]
    if (
        authority.get("derivation_rule_id") != CONTIGUOUS_REVIEWED_HORIZON_RULE
        or authority.get("review_state") != "reviewed"
    ):
        raise _atomic_error(raw, "uses an unsupported reviewed-horizon derivation authority")
    anchor_key = str(authority["anchor_period_key"])
    anchor = reconciled_by_key.get(anchor_key)
    if anchor is None:
        raise _atomic_error(raw, "references an unreconciled reviewed-horizon anchor period")
    if (
        anchor.get("period_type") != "annual"
        or anchor.get("reconciliation_state") != "reconciled"
        or anchor.get("calendar_id") != calendar_id
        or int(anchor["fiscal_year"]) + 1 != int(authority["fiscal_year"])
    ):
        raise _atomic_error(raw, "has an incompatible reviewed-horizon anchor period")

    duration_days = int(authority["duration_days"])
    week_count = int(authority["week_count"])
    if duration_days != week_count * 7:
        raise _atomic_error(raw, "has an internally inconsistent reviewed horizon duration")
    expected_start = date.fromisoformat(str(anchor["end_date"])) + timedelta(days=1)
    expected_end = expected_start + timedelta(days=duration_days - 1)
    if (
        date.fromisoformat(str(authority["start_date"])) != expected_start
        or date.fromisoformat(str(authority["end_date"])) != expected_end
    ):
        raise _atomic_error(raw, "reviewed horizon boundary disagrees with its contiguous anchor")

    authority_tuple = (
        int(authority["fiscal_year"]),
        str(authority["period_type"]),
        authority.get("fiscal_quarter"),
        int(authority["fiscal_ordinal"]),
        str(authority["fiscal_calendar_id"]),
        expected_start.isoformat(),
        expected_end.isoformat(),
        duration_days,
        week_count,
    )
    declared_tuple = (
        int(raw["fiscal_year"]),
        str(raw["period_type"]),
        raw.get("fiscal_quarter"),
        int(raw["fiscal_ordinal"]),
        calendar_id,
        str(raw["start_date"]),
        str(raw["end_date"]),
        (date.fromisoformat(str(raw["end_date"])) - date.fromisoformat(str(raw["start_date"]))).days
        + 1,
        int(raw["week_count"]),
    )
    if declared_tuple != authority_tuple:
        raise _atomic_error(raw, "declared reviewed horizon disagrees with its exact authority tuple")
    if bool(raw["is_53_week_year"]) != (week_count == 53):
        raise _atomic_error(raw, "has an inconsistent reviewed 52/53-week horizon")
    return expected_start, expected_end, str(anchor["end_date"])


def _replay_relative_month(
    source_set: SourceSet,
    raw: Mapping[str, Any],
    assertion: Mapping[str, Any],
    evidence: ExtractedEvidence,
) -> tuple[date, date]:
    if raw.get("period_type") != "month" or raw.get("week_count") is not None:
        raise MappingError("Reviewed relative-month evidence must resolve one month period.")
    if "this month" not in evidence.excerpt.casefold():
        raise MappingError("Relative-month evidence lacks the explicit 'this month' phrase.")
    link_key = assertion.get("required_reviewed_link_key")
    links = [
        row
        for row in source_set.reviewed_links
        if row.get("link_key") == link_key
        and row.get("relation_type") == "event-date-support"
        and row.get("from_document_key") == assertion.get("document_key")
        and row.get("review_state") in {"accepted", "reviewed"}
    ]
    if len(links) != 1:
        raise MappingError("Relative-month evidence lacks one eligible reviewed event-date link.")
    target_key = str(links[0]["to_document_key"])
    targets = [row for row in source_set.documents if row.document_key == target_key]
    if len(targets) != 1 or targets[0].review_state not in {"accepted", "reviewed"}:
        raise MappingError("Relative-month evidence link has no eligible target document.")
    reference = date.fromisoformat(targets[0].publication_date)
    start = date(reference.year, reference.month, 1)
    end = date(reference.year, reference.month, calendar.monthrange(reference.year, reference.month)[1])
    declared_start = date.fromisoformat(str(raw["start_date"]))
    declared_end = date.fromisoformat(str(raw["end_date"]))
    if (declared_start, declared_end) != (start, end):
        raise MappingError("Declared relative-month period disagrees with its reviewed source event date.")
    return start, end


def reconcile_periods(
    source_set: SourceSet,
    evidence_by_assertion: Mapping[
        str, tuple[Mapping[str, Any], ExtractedEvidence]
    ],
    *,
    calendar_id: str,
) -> tuple[dict[str, Any], ...]:
    result: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    assertions = {
        str(row["assertion_key"]): row for row in source_set.required_assertions
    }
    documents = {row.document_key: row for row in source_set.documents}
    reconciled_by_key: dict[str, Mapping[str, Any]] = {}
    ordered_periods = sorted(
        source_set.periods,
        key=lambda row: (
            str(row["start_rule_id"]) == REVIEWED_HORIZON_RULE,
            str(row["period_id"]),
        ),
    )
    for raw in ordered_periods:
        period_id = str(raw["period_id"])
        if period_id in seen_ids:
            raise MappingError(f"Duplicate period identity {period_id!r}.")
        seen_ids.add(period_id)
        evidence_key = str(raw["evidence_assertion_key"])
        evidence_pair = evidence_by_assertion.get(evidence_key)
        if evidence_pair is None:
            raise MappingError(f"Period {period_id!r} has no extracted evidence.")
        occurrence, evidence = evidence_pair
        assertion = assertions.get(evidence_key)
        if assertion is None or _assertion_period_key(assertion) != str(raw["period_key"]):
            raise MappingError(f"Period {period_id!r} evidence is linked to a different economic period.")
        rule_id = str(raw["start_rule_id"])
        if rule_id == REVIEWED_MONTH_RULE:
            start, end = _replay_relative_month(source_set, raw, assertion, evidence)
            if raw.get("fiscal_claim_assertion_keys"):
                raise MappingError("Effective-only month periods cannot claim fiscal-label membership.")
            anchor_report_date = None
        elif rule_id == CALENDAR_YEAR_FISCAL_RULE:
            start = _derive_start(raw)
            end = date.fromisoformat(str(raw["end_date"]))
            anchor_report_date = end.isoformat()
        elif rule_id == REVIEWED_HORIZON_RULE:
            start, end, anchor_report_date = _replay_reviewed_horizon(
                source_set,
                raw,
                reconciled_by_key,
                calendar_id=calendar_id,
            )
        else:
            start = _derive_start(raw)
            end = date.fromisoformat(str(raw["end_date"]))
            anchor_report_date = end.isoformat()
        day_count = (end - start).days + 1
        weeks = raw.get("week_count")
        if weeks is not None and day_count != int(weeks) * 7:
            raise MappingError(f"Period {period_id!r} has an unsafe duration.")
        fiscal_tuple: ReconciledFiscalPeriodTuple | None = None
        closure: tuple[tuple[Mapping[str, Any], ExtractedEvidence], ...] = ()
        if rule_id == CALENDAR_YEAR_FISCAL_RULE:
            fiscal_tuple, closure = _reconcile_calendar_year_period(
                source_set,
                raw,
                evidence_by_assertion,
                assertions,
                calendar_id=calendar_id,
            )
        elif rule_id == INCLUSIVE_WEEKS_RULE:
            claims = _period_claims(evidence.excerpt)
            if (int(weeks), end) not in claims:
                raise MappingError(
                    f"Period {period_id!r} disagrees with its source-backed week count or end date."
                )
            evidence_document = documents.get(str(assertion["document_key"]))
            if evidence_document is None or evidence_document.report_date != end.isoformat():
                raise MappingError(
                    f"Period {period_id!r} disagrees with its source document report-period linkage."
                )
            closure = _eligible_fiscal_evidence_closure(
                source_set,
                raw,
                assertions,
                evidence_by_assertion,
                actual_report_date=anchor_report_date,
            )
            fiscal_tuple = _reconcile_atomic_fiscal_tuple(
                source_set,
                raw,
                closure,
                start=start,
                end=end,
                calendar_id=calendar_id,
            )
        elif rule_id == REVIEWED_HORIZON_RULE:
            closure = _eligible_fiscal_evidence_closure(
                source_set,
                raw,
                assertions,
                evidence_by_assertion,
                actual_report_date=anchor_report_date,
            )
            fiscal_tuple = _reconcile_atomic_fiscal_tuple(
                source_set,
                raw,
                closure,
                start=start,
                end=end,
                calendar_id=calendar_id,
            )
        state = str(raw["reconciliation_state"])
        if evidence.review_state not in {"accepted", "reviewed"}:
            state = "needs_review"
        if fiscal_tuple is not None:
            output_fiscal_year = fiscal_tuple.fiscal_year
            output_fiscal_quarter = fiscal_tuple.fiscal_quarter
            output_period_type = {
                "fiscal_quarter": "quarter",
                "fiscal_ytd": "ytd",
                "fiscal_year": "annual",
            }[fiscal_tuple.period_type]
            output_start = fiscal_tuple.start_date
            output_end = fiscal_tuple.end_date
            output_day_count = fiscal_tuple.duration_days
            output_week_count: int | None = fiscal_tuple.week_count
            output_ordinal: int | None = fiscal_tuple.fiscal_ordinal
        else:
            output_fiscal_year = int(raw["fiscal_year"])
            output_fiscal_quarter = raw["fiscal_quarter"]
            output_period_type = str(raw["period_type"])
            output_start = start
            output_end = end
            output_day_count = day_count
            output_week_count = weeks
            output_ordinal = raw["fiscal_ordinal"]
        evidence_occurrence_ids = sorted(
            {
                str(occurrence["evidence_occurrence_id"]),
                *(
                    str(closure_occurrence["evidence_occurrence_id"])
                    for closure_occurrence, _ in closure
                ),
            }
        )
        period_row = {
            "period_id": period_id,
            "calendar_id": calendar_id,
            "company_id": source_set.company_id,
            "fiscal_year": output_fiscal_year,
            "fiscal_quarter": output_fiscal_quarter,
            "period_type": output_period_type,
            "start_date": output_start.isoformat(),
            "end_date": output_end.isoformat(),
            "day_count": output_day_count,
            "week_count": output_week_count,
            "fiscal_ordinal": output_ordinal,
            "is_53_week_year": bool(raw["is_53_week_year"]),
            "evidence_occurrence_ids": evidence_occurrence_ids,
            "reconciliation_state": state,
        }
        result.append(period_row)
        reconciled_by_key[str(raw["period_key"])] = period_row
    return tuple(sorted(result, key=lambda row: str(row["period_id"])))
