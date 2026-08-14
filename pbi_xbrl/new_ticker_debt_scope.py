"""Ticker-neutral debt identities and immutable source-row dispositions."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable, Mapping, Sequence


DEBT_SOURCE_SCALES = frozenset({"ones", "thousands", "millions", "not_applicable"})
DEBT_SOURCE_STATUSES = frozenset({"accepted", "manual_review_required", "rejected"})
DEBT_PERIOD_ROLES = frozenset({"current", "historical"})
DEBT_PROFILE_ECONOMIC_VALIDATION_CONTRACT = "contract:debt-profile-economic-validation@1"
DEBT_PROFILE_ECONOMIC_VALIDATION_ATTR = "debt_profile_economic_validation"

_FACILITY_TYPES = frozenset({"asset_based_revolver", "revolving_credit_facility"})
_INSTRUMENT_TYPES = frozenset(
    {
        "senior_note",
        "bond",
        "term_loan",
        "finance_lease",
        "operating_lease_liability",
        "other_funded_debt",
    }
)
_MATURITY_TYPES = frozenset({"scheduled_principal", "current_portion", "contractual_principal"})
_CREDIT_NOTE_TYPES = frozenset(
    {
        "facility_draw_status",
        "covenant_compliance",
        "facility_amendment",
        "refinancing",
        "debt_redemption",
        "borrowing_restriction",
        "credit_rating",
    }
)
_RATE_TYPES = frozenset({"fixed", "floating", "mixed", "not_reported", "not_applicable"})
_SECURED_STATUSES = frozenset({"secured", "unsecured", "mixed", "not_reported", "not_applicable"})
_SENIORITY_STATUSES = frozenset({"senior", "subordinated", "mixed", "not_reported", "not_applicable"})
_DEBT_PROFILE_SUBJECT_SOURCE_CONTRACTS = {
    "facility": frozenset({"legacy_revolver_history", "resolved_debt_facility_disposition"}),
    "funded_debt": frozenset(
        {"legacy_xbrl_debt_facts", "resolved_debt_instrument_dispositions"}
    ),
}


class DebtResolutionError(ValueError):
    """Fail-closed debt normalization or selection error."""

    def __init__(self, rule_id: str, message: str, **context: Any) -> None:
        self.rule_id = rule_id
        self.context = dict(context)
        detail = ", ".join(f"{key}={value!r}" for key, value in sorted(self.context.items()))
        super().__init__(f"{rule_id}: {message}" + (f" ({detail})" if detail else ""))


def _token(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().casefold()).strip("_")


def canonical_debt_id(value: Any, *, field: str) -> str:
    canonical = _token(value)
    if not canonical or not re.fullmatch(r"[a-z][a-z0-9_]*", canonical):
        raise DebtResolutionError(
            "invalid_debt_canonical_id",
            "Debt business IDs must canonicalize to a stable snake-case identifier.",
            field=field,
            raw_value=str(value or ""),
            canonical_value=canonical,
        )
    return canonical


def canonical_debt_currency(value: Any) -> str:
    token = _token(value)
    aliases = {"usd": "USD", "us_dollar": "USD", "us_dollars": "USD"}
    canonical = aliases.get(token)
    if canonical != "USD":
        raise DebtResolutionError(
            "unsupported_debt_currency",
            "The bounded debt contract currently accepts USD only.",
            raw_currency=str(value or ""),
            canonical_currency=canonical or token,
        )
    return canonical


def canonical_debt_unit(value: Any) -> str:
    token = re.sub(r"[\s_-]+", " ", str(value or "").strip().casefold())
    aliases = {
        "$m": "$m",
        "usdm": "$m",
        "usd m": "$m",
        "usd millions": "$m",
        "million usd": "$m",
        "million dollars": "$m",
    }
    canonical = aliases.get(token)
    if canonical != "$m":
        raise DebtResolutionError(
            "unsupported_debt_unit",
            "Debt currency amounts must normalize to $m.",
            raw_unit=str(value or ""),
            canonical_unit=canonical or token,
        )
    return canonical


def normalize_debt_currency_to_millions(
    value: Any,
    *,
    source_unit: Any,
    source_scale: Any,
) -> float:
    """Normalize one source amount from declared semantics, never magnitude."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DebtResolutionError(
            "debt_amount_not_numeric",
            "Debt source amount must be numeric.",
            raw_value=value,
        )
    unit = _token(source_unit)
    scale = _token(source_scale)
    if unit not in {"usd", "us_dollar", "us_dollars", "dollar", "dollars"}:
        raise DebtResolutionError(
            "unsupported_debt_source_unit",
            "Debt source currency must be explicitly USD.",
            source_unit=str(source_unit or ""),
        )
    if scale not in DEBT_SOURCE_SCALES - {"not_applicable"}:
        raise DebtResolutionError(
            "unsupported_debt_source_scale",
            "Debt source scale must be explicit.",
            source_scale=str(source_scale or ""),
        )
    multiplier = {"ones": 0.000001, "thousands": 0.001, "millions": 1.0}[scale]
    return round(float(value) * multiplier, 6)


def _iso_date(value: Any, *, field: str, allow_empty: bool = False) -> str:
    raw = str(value or "").strip()
    if allow_empty and not raw:
        return ""
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise DebtResolutionError(
            "invalid_debt_date",
            "Debt periods and publication dates must be exact ISO dates.",
            field=field,
            raw_value=raw,
        ) from exc
    return parsed.isoformat()


def _canonical_choice(value: Any, *, field: str, allowed: frozenset[str]) -> str:
    canonical = _token(value)
    if canonical not in allowed:
        raise DebtResolutionError(
            "unsupported_debt_vocabulary",
            "Debt row uses a value outside the bounded canonical vocabulary.",
            field=field,
            raw_value=str(value or ""),
            canonical_value=canonical,
            allowed=sorted(allowed),
        )
    return canonical


def _string_tuple(values: Any, *, field: str, required: bool = True) -> tuple[str, ...]:
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
        values = ()
    normalized = tuple(sorted({str(value).strip() for value in values if str(value).strip()}))
    if required and not normalized:
        raise DebtResolutionError(
            "debt_lineage_missing",
            "Accepted debt records require complete evidence and source lineage.",
            field=field,
        )
    return normalized


@dataclass(frozen=True)
class ResolvedDebtAmount:
    value: float | None
    status: str
    currency: str
    unit: str
    source_value: float | None
    source_unit: str
    source_scale: str
    as_of_date: str
    source_ref: str
    source_row_ref: str
    evidence_refs: tuple[str, ...]
    evidence_classification: str
    derivation: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "value": self.value,
            "status": self.status,
            "currency": self.currency,
            "unit": self.unit,
            "source_value": self.source_value,
            "source_unit": self.source_unit,
            "source_scale": self.source_scale,
            "as_of_date": self.as_of_date,
            "source_ref": self.source_ref,
            "source_row_ref": self.source_row_ref,
            "evidence_refs": list(self.evidence_refs),
            "evidence_classification": self.evidence_classification,
            "derivation": self.derivation,
            "reason": self.reason,
            "core": False,
        }


def _resolve_amount(
    raw: Any,
    *,
    field: str,
    parent_as_of_date: str,
    business_key: str,
) -> ResolvedDebtAmount:
    if not isinstance(raw, Mapping):
        raise DebtResolutionError(
            "debt_amount_contract_missing",
            "Debt amount fields require one typed source disposition.",
            field=field,
            business_key=business_key,
        )
    status = str(raw.get("status") or "").strip()
    if status not in {"populated", "missing_source", "manual_review_required", "parser_conflict", "not_applicable"}:
        raise DebtResolutionError(
            "invalid_debt_amount_status",
            "Debt amount uses an unsupported normalized status.",
            field=field,
            status=status,
            business_key=business_key,
        )
    as_of_date = _iso_date(raw.get("as_of_date"), field=f"{field}.as_of_date")
    if as_of_date != parent_as_of_date:
        raise DebtResolutionError(
            "debt_amount_period_mismatch",
            "Every amount in a debt row must share the row's exact as-of identity.",
            field=field,
            amount_as_of_date=as_of_date,
            row_as_of_date=parent_as_of_date,
            business_key=business_key,
        )
    currency = canonical_debt_currency(raw.get("currency"))
    unit = canonical_debt_unit(raw.get("unit"))
    source_scale = _token(raw.get("source_scale"))
    if source_scale not in DEBT_SOURCE_SCALES:
        raise DebtResolutionError(
            "unsupported_debt_source_scale",
            "Debt source scale must be declared explicitly.",
            field=field,
            source_scale=source_scale,
            business_key=business_key,
        )
    source_unit = str(raw.get("source_unit") or "").strip()
    source_ref = str(raw.get("source_ref") or "").strip()
    source_row_ref = str(raw.get("source_row_ref") or "").strip()
    evidence_refs = _string_tuple(raw.get("evidence_refs"), field=f"{field}.evidence_refs")
    evidence_classification = str(raw.get("evidence_classification") or "").strip()
    derivation = str(raw.get("derivation") or "").strip()
    reason = str(raw.get("reason") or "").strip()
    if not source_ref or not source_row_ref:
        raise DebtResolutionError(
            "debt_lineage_missing",
            "Debt amount requires exact source and source-row references.",
            field=field,
            source_ref=source_ref,
            source_row_ref=source_row_ref,
            business_key=business_key,
        )
    raw_value = raw.get("value")
    raw_source_value = raw.get("source_value")
    if status == "populated":
        if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
            raise DebtResolutionError(
                "debt_amount_not_numeric",
                "A populated debt amount must be numeric.",
                field=field,
                raw_value=raw_value,
                business_key=business_key,
            )
        value = round(float(raw_value), 6)
        if source_scale == "not_applicable":
            if evidence_classification == "source_backed_calculation":
                if not derivation:
                    raise DebtResolutionError(
                        "debt_calculation_lineage_missing",
                        "Derived debt amounts require an explicit calculation lineage.",
                        field=field,
                        business_key=business_key,
                    )
                source_value = None
            else:
                if isinstance(raw_source_value, bool) or not isinstance(raw_source_value, (int, float)):
                    raise DebtResolutionError(
                        "debt_source_value_missing",
                        "A source-backed textual zero requires its explicit numeric source value.",
                        field=field,
                        business_key=business_key,
                    )
                source_value = float(raw_source_value)
                if abs(source_value - value) > 0.000001:
                    raise DebtResolutionError(
                        "debt_amount_scale_mismatch",
                        "A not-applicable source scale cannot transform the source value.",
                        field=field,
                        source_value=source_value,
                        normalized_value=value,
                        business_key=business_key,
                    )
        else:
            if isinstance(raw_source_value, bool) or not isinstance(raw_source_value, (int, float)):
                raise DebtResolutionError(
                    "debt_source_value_missing",
                    "A populated debt amount requires its exact pre-normalization source value.",
                    field=field,
                    business_key=business_key,
                )
            source_value = float(raw_source_value)
            expected = normalize_debt_currency_to_millions(
                source_value,
                source_unit=source_unit,
                source_scale=source_scale,
            )
            if abs(expected - value) > 0.000001:
                raise DebtResolutionError(
                    "debt_amount_scale_mismatch",
                    "Normalized debt value does not match its declared source scale.",
                    field=field,
                    source_value=source_value,
                    source_scale=source_scale,
                    expected_value=expected,
                    normalized_value=value,
                    business_key=business_key,
                )
    else:
        if raw_value is not None or raw_source_value is not None:
            raise DebtResolutionError(
                "unavailable_debt_amount_has_value",
                "Unavailable debt facts cannot carry an investor-facing or source numeric value.",
                field=field,
                status=status,
                value=raw_value,
                source_value=raw_source_value,
                business_key=business_key,
            )
        if not reason:
            raise DebtResolutionError(
                "unavailable_debt_amount_reason_missing",
                "Unavailable debt facts require an explicit reason.",
                field=field,
                status=status,
                business_key=business_key,
            )
        value = None
        source_value = None
    return ResolvedDebtAmount(
        value=value,
        status=status,
        currency=currency,
        unit=unit,
        source_value=source_value,
        source_unit=source_unit,
        source_scale=source_scale,
        as_of_date=as_of_date,
        source_ref=source_ref,
        source_row_ref=source_row_ref,
        evidence_refs=evidence_refs,
        evidence_classification=evidence_classification,
        derivation=derivation,
        reason=reason,
    )


@dataclass(frozen=True)
class ResolvedDebtFacilityDisposition:
    facility_id: str
    facility_name: str
    facility_type: str
    borrower: str
    currency: str
    as_of_date: str
    publication_date: str
    period_role: str
    source_status: str
    source_table_scope: str
    aggregation_role: str
    commitment: ResolvedDebtAmount
    loan_cap: ResolvedDebtAmount
    drawn_balance: ResolvedDebtAmount
    drawn_status: str
    letters_of_credit: ResolvedDebtAmount
    gross_capacity: ResolvedDebtAmount
    minimum_excess_availability: ResolvedDebtAmount
    net_availability: ResolvedDebtAmount
    cash_and_equivalents: ResolvedDebtAmount
    restricted_cash: ResolvedDebtAmount
    same_date_liquidity: ResolvedDebtAmount
    facility_expiry_date: str
    evidence_key: str
    evidence_refs: tuple[str, ...]
    source_refs: tuple[str, ...]
    source_row_ref: str
    source_document_sha256: str
    business_key: str
    resolution_status: str
    reason: str

    @property
    def source_ref(self) -> str:
        return self.source_refs[0] if self.source_refs else ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "facility_id": self.facility_id,
            "facility_name": self.facility_name,
            "facility_type": self.facility_type,
            "borrower": self.borrower,
            "currency": self.currency,
            "as_of_date": self.as_of_date,
            "publication_date": self.publication_date,
            "period_role": self.period_role,
            "source_status": self.source_status,
            "source_table_scope": self.source_table_scope,
            "aggregation_role": self.aggregation_role,
            "commitment": self.commitment.to_dict(),
            "loan_cap": self.loan_cap.to_dict(),
            "drawn_balance": self.drawn_balance.to_dict(),
            "drawn_status": self.drawn_status,
            "letters_of_credit": self.letters_of_credit.to_dict(),
            "gross_capacity": self.gross_capacity.to_dict(),
            "minimum_excess_availability": self.minimum_excess_availability.to_dict(),
            "net_availability": self.net_availability.to_dict(),
            "cash_and_equivalents": self.cash_and_equivalents.to_dict(),
            "restricted_cash": self.restricted_cash.to_dict(),
            "same_date_liquidity": self.same_date_liquidity.to_dict(),
            "facility_expiry_date": self.facility_expiry_date,
            "evidence_key": self.evidence_key,
            "evidence_refs": list(self.evidence_refs),
            "source_refs": list(self.source_refs),
            "source_ref": self.source_ref,
            "source_row_ref": self.source_row_ref,
            "source_document_sha256": self.source_document_sha256,
            "business_key": self.business_key,
            "resolution_status": self.resolution_status,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class ResolvedDebtInstrumentDisposition:
    instrument_id: str
    instrument_name: str
    instrument_type: str
    issuer: str
    currency: str
    as_of_date: str
    publication_date: str
    period_role: str
    source_status: str
    source_table_scope: str
    aggregation_role: str
    balance: ResolvedDebtAmount
    current_balance: ResolvedDebtAmount
    noncurrent_balance: ResolvedDebtAmount
    rate_type: str
    reference_rate: str
    spread_bps: float | None
    effective_rate: float | None
    maturity_date: str
    secured_status: str
    seniority: str
    evidence_key: str
    evidence_refs: tuple[str, ...]
    source_refs: tuple[str, ...]
    source_row_ref: str
    source_document_sha256: str
    business_key: str
    resolution_status: str
    reason: str

    @property
    def source_ref(self) -> str:
        return self.source_refs[0] if self.source_refs else ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "instrument_id": self.instrument_id,
            "instrument_name": self.instrument_name,
            "instrument_type": self.instrument_type,
            "issuer": self.issuer,
            "currency": self.currency,
            "as_of_date": self.as_of_date,
            "publication_date": self.publication_date,
            "period_role": self.period_role,
            "source_status": self.source_status,
            "source_table_scope": self.source_table_scope,
            "aggregation_role": self.aggregation_role,
            "balance": self.balance.to_dict(),
            "current_balance": self.current_balance.to_dict(),
            "noncurrent_balance": self.noncurrent_balance.to_dict(),
            "rate_type": self.rate_type,
            "reference_rate": self.reference_rate,
            "spread_bps": self.spread_bps,
            "effective_rate": self.effective_rate,
            "maturity_date": self.maturity_date,
            "secured_status": self.secured_status,
            "seniority": self.seniority,
            "evidence_key": self.evidence_key,
            "evidence_refs": list(self.evidence_refs),
            "source_refs": list(self.source_refs),
            "source_ref": self.source_ref,
            "source_row_ref": self.source_row_ref,
            "source_document_sha256": self.source_document_sha256,
            "business_key": self.business_key,
            "resolution_status": self.resolution_status,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class ResolvedDebtMaturityDisposition:
    maturity_id: str
    instrument_id: str
    maturity_type: str
    due_date: str
    maturity_bucket: str
    currency: str
    as_of_date: str
    publication_date: str
    period_role: str
    source_status: str
    source_table_scope: str
    aggregation_role: str
    amount: ResolvedDebtAmount
    evidence_key: str
    evidence_refs: tuple[str, ...]
    source_refs: tuple[str, ...]
    source_row_ref: str
    source_document_sha256: str
    business_key: str
    resolution_status: str
    reason: str

    @property
    def source_ref(self) -> str:
        return self.source_refs[0] if self.source_refs else ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "maturity_id": self.maturity_id,
            "instrument_id": self.instrument_id,
            "maturity_type": self.maturity_type,
            "due_date": self.due_date,
            "maturity_bucket": self.maturity_bucket,
            "currency": self.currency,
            "as_of_date": self.as_of_date,
            "publication_date": self.publication_date,
            "period_role": self.period_role,
            "source_status": self.source_status,
            "source_table_scope": self.source_table_scope,
            "aggregation_role": self.aggregation_role,
            "amount": self.amount.to_dict(),
            "evidence_key": self.evidence_key,
            "evidence_refs": list(self.evidence_refs),
            "source_refs": list(self.source_refs),
            "source_ref": self.source_ref,
            "source_row_ref": self.source_row_ref,
            "source_document_sha256": self.source_document_sha256,
            "business_key": self.business_key,
            "resolution_status": self.resolution_status,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class ResolvedDebtCreditNoteDisposition:
    note_id: str
    subject_id: str
    note_type: str
    text: str
    as_of_date: str
    publication_date: str
    period_role: str
    source_status: str
    source_table_scope: str
    aggregation_role: str
    evidence_key: str
    evidence_refs: tuple[str, ...]
    source_refs: tuple[str, ...]
    source_row_ref: str
    source_document_sha256: str
    business_key: str
    resolution_status: str
    reason: str

    @property
    def source_ref(self) -> str:
        return self.source_refs[0] if self.source_refs else ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "note_id": self.note_id,
            "subject_id": self.subject_id,
            "note_type": self.note_type,
            "text": self.text,
            "as_of_date": self.as_of_date,
            "publication_date": self.publication_date,
            "period_role": self.period_role,
            "source_status": self.source_status,
            "source_table_scope": self.source_table_scope,
            "aggregation_role": self.aggregation_role,
            "evidence_key": self.evidence_key,
            "evidence_refs": list(self.evidence_refs),
            "source_refs": list(self.source_refs),
            "source_ref": self.source_ref,
            "source_row_ref": self.source_row_ref,
            "source_document_sha256": self.source_document_sha256,
            "business_key": self.business_key,
            "resolution_status": self.resolution_status,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class DebtProfileEconomicValidationResult:
    """Independent economic identity required before Debt_Profile presentation gates."""

    contract_id: str
    subject_kind: str
    subject_ids: tuple[str, ...]
    as_of_date: str
    evidence_keys: tuple[str, ...]
    evidence_refs: tuple[str, ...]
    source_refs: tuple[str, ...]
    source_row_refs: tuple[str, ...]
    source_contract: str
    business_key: str
    economic_validated: bool
    issue_ids: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return self.economic_validated and not self.issue_ids

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_id": self.contract_id,
            "subject_kind": self.subject_kind,
            "subject_ids": list(self.subject_ids),
            "as_of_date": self.as_of_date,
            "evidence_keys": list(self.evidence_keys),
            "evidence_refs": list(self.evidence_refs),
            "source_refs": list(self.source_refs),
            "source_row_refs": list(self.source_row_refs),
            "source_contract": self.source_contract,
            "business_key": self.business_key,
            "economic_validated": self.economic_validated,
            "issue_ids": list(self.issue_ids),
            "passed": self.passed,
        }


def validate_debt_profile_economic_subject(
    *,
    subject_kind: Any,
    subject_ids: Sequence[Any],
    as_of_date: Any,
    evidence_keys: Sequence[Any],
    evidence_refs: Sequence[Any],
    source_refs: Sequence[Any],
    source_row_refs: Sequence[Any],
    source_contract: Any,
    economic_validated: bool,
) -> DebtProfileEconomicValidationResult:
    """Validate stable facility/funded-debt identity without using presentation geometry."""

    issues: list[str] = []
    kind = _token(subject_kind)
    if kind not in _DEBT_PROFILE_SUBJECT_SOURCE_CONTRACTS:
        issues.append("debt_profile_subject_kind_invalid")

    canonical_subject_ids: list[str] = []
    for raw_id in subject_ids if isinstance(subject_ids, Sequence) and not isinstance(subject_ids, (str, bytes)) else ():
        try:
            canonical_subject_ids.append(canonical_debt_id(raw_id, field="debt_profile_subject_id"))
        except DebtResolutionError:
            issues.append("debt_profile_subject_identity_invalid")
    canonical_subjects = tuple(sorted(set(canonical_subject_ids)))
    if not canonical_subjects:
        issues.append("debt_profile_subject_identity_missing")
    if len(canonical_subjects) != len(canonical_subject_ids):
        issues.append("debt_profile_subject_identity_duplicate")
    if kind == "facility" and len(canonical_subjects) != 1:
        issues.append("debt_profile_facility_identity_ambiguous")

    canonical_evidence_keys: list[str] = []
    for raw_key in evidence_keys if isinstance(evidence_keys, Sequence) and not isinstance(evidence_keys, (str, bytes)) else ():
        try:
            canonical_evidence_keys.append(canonical_debt_id(raw_key, field="debt_profile_evidence_key"))
        except DebtResolutionError:
            issues.append("debt_profile_evidence_identity_invalid")
    canonical_evidence = tuple(sorted(set(canonical_evidence_keys)))
    if not canonical_evidence:
        issues.append("debt_profile_evidence_identity_missing")
    if len(canonical_evidence) != len(canonical_evidence_keys):
        issues.append("debt_profile_evidence_identity_duplicate")
    if kind in {"facility", "funded_debt"} and len(canonical_evidence) != len(canonical_subjects):
        issues.append("debt_profile_subject_evidence_cardinality_mismatch")

    def _refs(values: Sequence[Any]) -> tuple[str, ...]:
        if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
            return ()
        return tuple(sorted({str(value).strip() for value in values if str(value).strip()}))

    normalized_evidence_refs = _refs(evidence_refs)
    normalized_source_refs = _refs(source_refs)
    normalized_source_row_refs = _refs(source_row_refs)
    if not normalized_evidence_refs or not normalized_source_refs or not normalized_source_row_refs:
        issues.append("debt_profile_evidence_lineage_missing")

    try:
        canonical_as_of = _iso_date(as_of_date, field="debt_profile_as_of_date")
    except DebtResolutionError:
        canonical_as_of = ""
        issues.append("debt_profile_subject_as_of_invalid")

    canonical_source_contract = _token(source_contract)
    if canonical_source_contract not in _DEBT_PROFILE_SUBJECT_SOURCE_CONTRACTS.get(kind, frozenset()):
        issues.append("debt_profile_source_contract_invalid")
    if not economic_validated:
        issues.append("debt_profile_economic_validation_failed")

    issue_ids = tuple(sorted(set(issues)))
    business_key = (
        f"debt_profile_subject|{kind}|{'+'.join(canonical_subjects)}|{canonical_as_of}"
        if kind and canonical_subjects and canonical_as_of
        else ""
    )
    return DebtProfileEconomicValidationResult(
        contract_id=DEBT_PROFILE_ECONOMIC_VALIDATION_CONTRACT,
        subject_kind=kind,
        subject_ids=canonical_subjects,
        as_of_date=canonical_as_of,
        evidence_keys=canonical_evidence,
        evidence_refs=normalized_evidence_refs,
        source_refs=normalized_source_refs,
        source_row_refs=normalized_source_row_refs,
        source_contract=canonical_source_contract,
        business_key=business_key,
        economic_validated=bool(economic_validated),
        issue_ids=issue_ids,
    )


def validate_resolved_debt_facility_for_profile(
    facility: ResolvedDebtFacilityDisposition | None,
) -> DebtProfileEconomicValidationResult:
    """Project one canonical resolved facility into the profile-validation contract."""

    if facility is None:
        return validate_debt_profile_economic_subject(
            subject_kind="facility",
            subject_ids=(),
            as_of_date="",
            evidence_keys=(),
            evidence_refs=(),
            source_refs=(),
            source_row_refs=(),
            source_contract="resolved_debt_facility_disposition",
            economic_validated=False,
        )
    amounts = (
        facility.commitment,
        facility.loan_cap,
        facility.drawn_balance,
        facility.letters_of_credit,
        facility.gross_capacity,
        facility.minimum_excess_availability,
        facility.net_availability,
    )
    compatible_amounts = bool(
        all(
            amount.as_of_date == facility.as_of_date
            and amount.currency == facility.currency
            and amount.unit == "$m"
            for amount in amounts
        )
    )

    def _source_backed(amount: ResolvedDebtAmount) -> bool:
        return bool(
            amount.status == "populated"
            and amount.value is not None
            and amount.source_ref
            and amount.source_row_ref
            and amount.evidence_refs
            and amount.evidence_classification
            in {"source_backed_fact", "source_backed_calculation"}
        )

    source_backed_amounts = tuple(
        amount
        for amount in amounts
        if _source_backed(amount)
    )
    capacity_backed = _source_backed(facility.commitment) or _source_backed(facility.loan_cap)
    drawn_state_valid = bool(
        (
            facility.drawn_status == "not_reported"
            and facility.drawn_balance.status != "populated"
            and facility.drawn_balance.value is None
        )
        or (
            facility.drawn_status in {"reported_zero", "reported_value"}
            and _source_backed(facility.drawn_balance)
        )
    )
    net_availability_backed = _source_backed(facility.net_availability)
    loan_cap = facility.loan_cap.value
    loc = facility.letters_of_credit.value
    gross = facility.gross_capacity.value
    minimum = facility.minimum_excess_availability.value
    net = facility.net_availability.value
    gross_components_populated = any(value is not None for value in (loc, gross))
    gross_reconciles = bool(
        not gross_components_populated
        or (
            None not in (loan_cap, loc, gross)
            and _source_backed(facility.letters_of_credit)
            and _source_backed(facility.gross_capacity)
            and abs(float(gross) - (float(loan_cap) - float(loc))) <= 0.000001
        )
    )
    net_components_populated = any(value is not None for value in (gross, minimum))
    net_reconciles = bool(
        not net_components_populated
        or (
            None not in (gross, minimum, net)
            and _source_backed(facility.gross_capacity)
            and _source_backed(facility.minimum_excess_availability)
            and abs(float(net) - (float(gross) - float(minimum))) <= 0.000001
        )
    )
    facility_lineage_valid = bool(
        facility.evidence_key
        and facility.evidence_refs
        and facility.source_refs
        and facility.source_row_ref
        and re.fullmatch(r"[0-9a-f]{64}", facility.source_document_sha256)
    )
    economic_validated = bool(
        facility.source_status == "accepted"
        and facility.resolution_status == "populated"
        and facility.period_role == "current"
        and facility.aggregation_role == "liquidity_capacity"
        and facility_lineage_valid
        and compatible_amounts
        and capacity_backed
        and drawn_state_valid
        and net_availability_backed
        and gross_reconciles
        and net_reconciles
    )
    return validate_debt_profile_economic_subject(
        subject_kind="facility",
        subject_ids=(facility.facility_id,),
        as_of_date=facility.as_of_date,
        evidence_keys=(facility.evidence_key,),
        evidence_refs=(
            *facility.evidence_refs,
            *(ref for amount in source_backed_amounts for ref in amount.evidence_refs),
        ),
        source_refs=(*facility.source_refs, *(amount.source_ref for amount in source_backed_amounts)),
        source_row_refs=(facility.source_row_ref, *(amount.source_row_ref for amount in source_backed_amounts)),
        source_contract="resolved_debt_facility_disposition",
        economic_validated=economic_validated,
    )


def validate_resolved_funded_debt_for_profile(
    instruments: Sequence[ResolvedDebtInstrumentDisposition],
) -> DebtProfileEconomicValidationResult:
    """Validate a source-typed funded-debt subject independently of projected row count."""

    current_core = tuple(
        sorted(
            (
                row
                for row in instruments
                if row.period_role == "current"
                and row.source_status == "accepted"
                and row.resolution_status == "populated"
                and row.aggregation_role == "core_debt"
                and row.balance.status == "populated"
                and row.balance.value is not None
            ),
            key=lambda row: (row.instrument_id, row.business_key),
        )
    )
    dates = {row.as_of_date for row in current_core}
    currencies = {row.currency for row in current_core}
    source_backed = bool(
        current_core
        and len(dates) == 1
        and len(currencies) == 1
        and all(
            row.evidence_key
            and row.evidence_refs
            and row.source_refs
            and row.source_row_ref
            and row.balance.source_ref
            and row.balance.source_row_ref
            and row.balance.evidence_refs
            and row.balance.evidence_classification
            in {"source_backed_fact", "source_backed_calculation"}
            and row.balance.as_of_date == row.as_of_date
            and row.balance.currency == row.currency
            and row.balance.unit == "$m"
            and re.fullmatch(r"[0-9a-f]{64}", row.source_document_sha256)
            for row in current_core
        )
    )
    return validate_debt_profile_economic_subject(
        subject_kind="funded_debt",
        subject_ids=tuple(row.instrument_id for row in current_core),
        as_of_date=next(iter(dates)) if len(dates) == 1 else "",
        evidence_keys=tuple(row.evidence_key for row in current_core),
        evidence_refs=tuple(
            ref
            for row in current_core
            for ref in (*row.evidence_refs, *row.balance.evidence_refs)
        ),
        source_refs=tuple(ref for row in current_core for ref in (*row.source_refs, row.balance.source_ref)),
        source_row_refs=tuple(
            ref for row in current_core for ref in (row.source_row_ref, row.balance.source_row_ref)
        ),
        source_contract="resolved_debt_instrument_dispositions",
        economic_validated=source_backed,
    )


def _common_row(
    row: Mapping[str, Any],
    *,
    kind: str,
    id_field: str,
) -> dict[str, Any]:
    raw_id = str(row.get(id_field) or "")
    canonical_id = canonical_debt_id(raw_id, field=id_field)
    as_of_date = _iso_date(row.get("as_of_date"), field="as_of_date")
    publication_date = _iso_date(row.get("publication_date"), field="publication_date")
    period_role = _canonical_choice(row.get("period_role"), field="period_role", allowed=DEBT_PERIOD_ROLES)
    source_status = _canonical_choice(row.get("source_status"), field="source_status", allowed=DEBT_SOURCE_STATUSES)
    evidence_key = canonical_debt_id(row.get("evidence_key"), field="evidence_key")
    evidence_refs = _string_tuple(row.get("evidence_refs"), field="evidence_refs")
    source_refs = _string_tuple(row.get("source_refs"), field="source_refs")
    source_row_ref = str(row.get("source_row_ref") or "").strip()
    source_document_sha256 = str(row.get("source_document_sha256") or "").strip().lower()
    source_table_scope = canonical_debt_id(row.get("source_table_scope"), field="source_table_scope")
    if not source_row_ref or not re.fullmatch(r"[0-9a-f]{64}", source_document_sha256):
        raise DebtResolutionError(
            "debt_lineage_missing",
            "Debt row requires an exact source row and source-document SHA-256.",
            kind=kind,
            raw_id=raw_id,
            canonical_id=canonical_id,
            source_row_ref=source_row_ref,
            source_document_sha256=source_document_sha256,
        )
    business_key = "|".join((kind, canonical_id, as_of_date))
    return {
        "canonical_id": canonical_id,
        "raw_id": raw_id,
        "as_of_date": as_of_date,
        "publication_date": publication_date,
        "period_role": period_role,
        "source_status": source_status,
        "source_table_scope": source_table_scope,
        "evidence_key": evidence_key,
        "evidence_refs": evidence_refs,
        "source_refs": source_refs,
        "source_row_ref": source_row_ref,
        "source_document_sha256": source_document_sha256,
        "business_key": business_key,
        "resolution_status": "populated" if source_status == "accepted" else "manual_review_required",
        "reason": str(row.get("reason") or "").strip(),
    }


def _optional_number(value: Any, *, field: str, business_key: str) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DebtResolutionError(
            "invalid_debt_numeric_metadata",
            "Debt numeric metadata must be numeric or null.",
            field=field,
            value=value,
            business_key=business_key,
        )
    return float(value)


def _required_text(value: Any, *, field: str, business_key: str) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if not text:
        raise DebtResolutionError(
            "debt_display_identity_missing",
            "Accepted debt rows require a stable display identity.",
            field=field,
            business_key=business_key,
        )
    return text


def _resolved_facility(row: Mapping[str, Any]) -> ResolvedDebtFacilityDisposition:
    common = _common_row(row, kind="facility", id_field="facility_id")
    key = common["business_key"]
    facility_type = _canonical_choice(row.get("facility_type"), field="facility_type", allowed=_FACILITY_TYPES)
    aggregation_role = _token(row.get("aggregation_role"))
    if aggregation_role != "liquidity_capacity":
        raise DebtResolutionError(
            "invalid_debt_aggregation_role",
            "Revolving facilities are non-additive liquidity-capacity records.",
            raw_aggregation_role=str(row.get("aggregation_role") or ""),
            canonical_aggregation_role=aggregation_role,
            expected_aggregation_role="liquidity_capacity",
            business_key=key,
            source_row_ref=common["source_row_ref"],
        )
    amounts = {
        name: _resolve_amount(
            row.get(name),
            field=name,
            parent_as_of_date=common["as_of_date"],
            business_key=key,
        )
        for name in (
            "commitment",
            "loan_cap",
            "drawn_balance",
            "letters_of_credit",
            "gross_capacity",
            "minimum_excess_availability",
            "net_availability",
            "cash_and_equivalents",
            "restricted_cash",
            "same_date_liquidity",
        )
    }
    drawn_status = _canonical_choice(
        row.get("drawn_status"),
        field="drawn_status",
        allowed=frozenset({"reported_zero", "reported_value", "not_reported"}),
    )
    drawn = amounts["drawn_balance"]
    if drawn_status == "not_reported" and drawn.status == "populated":
        raise DebtResolutionError(
            "debt_drawn_state_conflict",
            "An unreported drawn balance cannot carry a populated value.",
            business_key=key,
            source_row_ref=common["source_row_ref"],
        )
    if drawn_status == "reported_zero" and (drawn.status != "populated" or drawn.value != 0):
        raise DebtResolutionError(
            "debt_drawn_state_conflict",
            "A reported-zero drawn state requires an exact source-backed zero.",
            business_key=key,
            source_row_ref=common["source_row_ref"],
        )
    if drawn_status == "reported_value" and (drawn.status != "populated" or drawn.value is None or drawn.value <= 0):
        raise DebtResolutionError(
            "debt_drawn_state_conflict",
            "A reported-value drawn state requires a positive source-backed amount.",
            business_key=key,
            source_row_ref=common["source_row_ref"],
        )
    commitment = amounts["commitment"].value
    loan_cap = amounts["loan_cap"].value
    loc = amounts["letters_of_credit"].value
    gross = amounts["gross_capacity"].value
    minimum = amounts["minimum_excess_availability"].value
    net = amounts["net_availability"].value
    if commitment is not None and loan_cap is not None and loan_cap - commitment > 0.000001:
        raise DebtResolutionError(
            "debt_facility_cap_exceeds_commitment",
            "Facility loan cap cannot exceed its source-backed commitment.",
            commitment=commitment,
            loan_cap=loan_cap,
            business_key=key,
        )
    if None not in (loan_cap, loc, gross) and abs(float(gross) - (float(loan_cap) - float(loc))) > 0.000001:
        raise DebtResolutionError(
            "debt_facility_gross_capacity_mismatch",
            "Gross capacity must reconcile to the exact source-table loan cap less letters of credit.",
            loan_cap=loan_cap,
            letters_of_credit=loc,
            gross_capacity=gross,
            business_key=key,
        )
    if None not in (gross, minimum, net) and abs(float(net) - (float(gross) - float(minimum))) > 0.000001:
        raise DebtResolutionError(
            "debt_facility_net_availability_mismatch",
            "Net availability must reconcile to gross capacity less minimum excess availability.",
            gross_capacity=gross,
            minimum_excess_availability=minimum,
            net_availability=net,
            business_key=key,
        )
    cash = amounts["cash_and_equivalents"].value
    liquidity = amounts["same_date_liquidity"].value
    if liquidity is not None:
        if cash is None or net is None or abs(float(liquidity) - (float(cash) + float(net))) > 0.000001:
            raise DebtResolutionError(
                "debt_same_date_liquidity_mismatch",
                "Same-date liquidity must equal cash plus net revolver availability only.",
                cash=cash,
                net_availability=net,
                restricted_cash=amounts["restricted_cash"].value,
                same_date_liquidity=liquidity,
                business_key=key,
            )
    expiry = _iso_date(row.get("facility_expiry_date"), field="facility_expiry_date", allow_empty=True)
    return ResolvedDebtFacilityDisposition(
        facility_id=common["canonical_id"],
        facility_name=_required_text(row.get("facility_name"), field="facility_name", business_key=key),
        facility_type=facility_type,
        borrower=_required_text(row.get("borrower"), field="borrower", business_key=key),
        currency=canonical_debt_currency(row.get("currency")),
        as_of_date=common["as_of_date"],
        publication_date=common["publication_date"],
        period_role=common["period_role"],
        source_status=common["source_status"],
        source_table_scope=common["source_table_scope"],
        aggregation_role=aggregation_role,
        commitment=amounts["commitment"],
        loan_cap=amounts["loan_cap"],
        drawn_balance=amounts["drawn_balance"],
        drawn_status=drawn_status,
        letters_of_credit=amounts["letters_of_credit"],
        gross_capacity=amounts["gross_capacity"],
        minimum_excess_availability=amounts["minimum_excess_availability"],
        net_availability=amounts["net_availability"],
        cash_and_equivalents=amounts["cash_and_equivalents"],
        restricted_cash=amounts["restricted_cash"],
        same_date_liquidity=amounts["same_date_liquidity"],
        facility_expiry_date=expiry,
        evidence_key=common["evidence_key"],
        evidence_refs=common["evidence_refs"],
        source_refs=common["source_refs"],
        source_row_ref=common["source_row_ref"],
        source_document_sha256=common["source_document_sha256"],
        business_key=key,
        resolution_status=common["resolution_status"],
        reason=common["reason"],
    )


def _resolved_instrument(row: Mapping[str, Any]) -> ResolvedDebtInstrumentDisposition:
    common = _common_row(row, kind="instrument", id_field="instrument_id")
    key = common["business_key"]
    instrument_type = _canonical_choice(row.get("instrument_type"), field="instrument_type", allowed=_INSTRUMENT_TYPES)
    aggregation_role = _token(row.get("aggregation_role"))
    expected_role = "excluded_from_core_debt" if instrument_type == "operating_lease_liability" else "core_debt"
    if aggregation_role != expected_role:
        raise DebtResolutionError(
            "invalid_debt_aggregation_role",
            "Debt instrument aggregation role conflicts with its canonical instrument type.",
            instrument_type=instrument_type,
            raw_aggregation_role=str(row.get("aggregation_role") or ""),
            canonical_aggregation_role=aggregation_role,
            expected_aggregation_role=expected_role,
            business_key=key,
            source_row_ref=common["source_row_ref"],
        )
    balance = _resolve_amount(row.get("balance"), field="balance", parent_as_of_date=common["as_of_date"], business_key=key)
    current = _resolve_amount(row.get("current_balance"), field="current_balance", parent_as_of_date=common["as_of_date"], business_key=key)
    noncurrent = _resolve_amount(row.get("noncurrent_balance"), field="noncurrent_balance", parent_as_of_date=common["as_of_date"], business_key=key)
    if balance.value is not None and current.value is not None and noncurrent.value is not None:
        if abs(balance.value - current.value - noncurrent.value) > 0.000001:
            raise DebtResolutionError(
                "debt_instrument_balance_reconciliation_failed",
                "Instrument balance must reconcile to current and noncurrent components.",
                balance=balance.value,
                current_balance=current.value,
                noncurrent_balance=noncurrent.value,
                business_key=key,
            )
    return ResolvedDebtInstrumentDisposition(
        instrument_id=common["canonical_id"],
        instrument_name=_required_text(row.get("instrument_name"), field="instrument_name", business_key=key),
        instrument_type=instrument_type,
        issuer=_required_text(row.get("issuer"), field="issuer", business_key=key),
        currency=canonical_debt_currency(row.get("currency")),
        as_of_date=common["as_of_date"],
        publication_date=common["publication_date"],
        period_role=common["period_role"],
        source_status=common["source_status"],
        source_table_scope=common["source_table_scope"],
        aggregation_role=aggregation_role,
        balance=balance,
        current_balance=current,
        noncurrent_balance=noncurrent,
        rate_type=_canonical_choice(row.get("rate_type"), field="rate_type", allowed=_RATE_TYPES),
        reference_rate=str(row.get("reference_rate") or "").strip(),
        spread_bps=_optional_number(row.get("spread_bps"), field="spread_bps", business_key=key),
        effective_rate=_optional_number(row.get("effective_rate"), field="effective_rate", business_key=key),
        maturity_date=_iso_date(row.get("maturity_date"), field="maturity_date", allow_empty=True),
        secured_status=_canonical_choice(row.get("secured_status"), field="secured_status", allowed=_SECURED_STATUSES),
        seniority=_canonical_choice(row.get("seniority"), field="seniority", allowed=_SENIORITY_STATUSES),
        evidence_key=common["evidence_key"],
        evidence_refs=common["evidence_refs"],
        source_refs=common["source_refs"],
        source_row_ref=common["source_row_ref"],
        source_document_sha256=common["source_document_sha256"],
        business_key=key,
        resolution_status=common["resolution_status"],
        reason=common["reason"],
    )


def _resolved_maturity(row: Mapping[str, Any]) -> ResolvedDebtMaturityDisposition:
    common = _common_row(row, kind="maturity", id_field="maturity_id")
    key = common["business_key"]
    maturity_type = _canonical_choice(row.get("maturity_type"), field="maturity_type", allowed=_MATURITY_TYPES)
    aggregation_role = _token(row.get("aggregation_role"))
    if aggregation_role != "core_debt_maturity":
        raise DebtResolutionError(
            "invalid_debt_aggregation_role",
            "Debt maturities must represent funded-debt or finance-lease principal, not facility expiry.",
            raw_aggregation_role=str(row.get("aggregation_role") or ""),
            canonical_aggregation_role=aggregation_role,
            expected_aggregation_role="core_debt_maturity",
            business_key=key,
        )
    return ResolvedDebtMaturityDisposition(
        maturity_id=common["canonical_id"],
        instrument_id=canonical_debt_id(row.get("instrument_id"), field="instrument_id"),
        maturity_type=maturity_type,
        due_date=_iso_date(row.get("due_date"), field="due_date"),
        maturity_bucket=canonical_debt_id(row.get("maturity_bucket"), field="maturity_bucket"),
        currency=canonical_debt_currency(row.get("currency")),
        as_of_date=common["as_of_date"],
        publication_date=common["publication_date"],
        period_role=common["period_role"],
        source_status=common["source_status"],
        source_table_scope=common["source_table_scope"],
        aggregation_role=aggregation_role,
        amount=_resolve_amount(row.get("amount"), field="amount", parent_as_of_date=common["as_of_date"], business_key=key),
        evidence_key=common["evidence_key"],
        evidence_refs=common["evidence_refs"],
        source_refs=common["source_refs"],
        source_row_ref=common["source_row_ref"],
        source_document_sha256=common["source_document_sha256"],
        business_key=key,
        resolution_status=common["resolution_status"],
        reason=common["reason"],
    )


def _resolved_credit_note(row: Mapping[str, Any]) -> ResolvedDebtCreditNoteDisposition:
    common = _common_row(row, kind="credit_note", id_field="note_id")
    key = common["business_key"]
    note_type = _canonical_choice(row.get("note_type"), field="note_type", allowed=_CREDIT_NOTE_TYPES)
    text = re.sub(r"\s+", " ", str(row.get("text") or "")).strip()
    if common["source_status"] == "accepted" and not text:
        raise DebtResolutionError(
            "debt_credit_note_text_missing",
            "Accepted credit notes require exact bounded source text.",
            business_key=key,
            source_row_ref=common["source_row_ref"],
        )
    aggregation_role = _token(row.get("aggregation_role"))
    if aggregation_role != "non_additive_context":
        raise DebtResolutionError(
            "invalid_debt_aggregation_role",
            "Credit notes are non-additive evidence records.",
            raw_aggregation_role=str(row.get("aggregation_role") or ""),
            canonical_aggregation_role=aggregation_role,
            expected_aggregation_role="non_additive_context",
            business_key=key,
        )
    return ResolvedDebtCreditNoteDisposition(
        note_id=common["canonical_id"],
        subject_id=canonical_debt_id(row.get("subject_id"), field="subject_id"),
        note_type=note_type,
        text=text,
        as_of_date=common["as_of_date"],
        publication_date=common["publication_date"],
        period_role=common["period_role"],
        source_status=common["source_status"],
        source_table_scope=common["source_table_scope"],
        aggregation_role=aggregation_role,
        evidence_key=common["evidence_key"],
        evidence_refs=common["evidence_refs"],
        source_refs=common["source_refs"],
        source_row_ref=common["source_row_ref"],
        source_document_sha256=common["source_document_sha256"],
        business_key=key,
        resolution_status=common["resolution_status"],
        reason=common["reason"],
    )


def _resolve_collection(
    rows: Sequence[Mapping[str, Any]],
    *,
    kind: str,
    resolver: Any,
) -> tuple[Any, ...]:
    resolved_with_source = [(resolver(source_row), source_row) for source_row in rows]
    seen: dict[str, tuple[Any, Mapping[str, Any]]] = {}
    for row, source_row in resolved_with_source:
        prior_entry = seen.get(row.business_key)
        prior = prior_entry[0] if prior_entry is not None else None
        if prior is not None:
            raise DebtResolutionError(
                "duplicate_debt_business_identity",
                "Two source rows canonicalize to the same debt business identity.",
                kind=kind,
                raw_id=str(source_row.get("note_id" if kind == "credit_note" else f"{kind}_id") or ""),
                canonical_id=getattr(row, "note_id" if kind == "credit_note" else f"{kind}_id", ""),
                business_key=row.business_key,
                first_source_row_ref=prior.source_row_ref,
                conflicting_source_row_ref=row.source_row_ref,
                first_evidence_key=prior.evidence_key,
                conflicting_evidence_key=row.evidence_key,
            )
        seen[row.business_key] = (row, source_row)
    resolved = [row for row, _source_row in resolved_with_source]
    ordered = tuple(sorted(resolved, key=lambda row: (row.as_of_date, row.business_key, row.evidence_key)))
    by_subject: dict[str, list[Any]] = {}
    for row in ordered:
        subject = (
            row.facility_id
            if isinstance(row, ResolvedDebtFacilityDisposition)
            else row.instrument_id
            if isinstance(row, ResolvedDebtInstrumentDisposition)
            else row.instrument_id
            if isinstance(row, ResolvedDebtMaturityDisposition)
            else f"{row.subject_id}|{row.note_type}"
        )
        by_subject.setdefault(subject, []).append(row)
    for subject, subject_rows in by_subject.items():
        accepted = [row for row in subject_rows if row.source_status == "accepted"]
        if not accepted:
            continue
        latest = max(row.as_of_date for row in accepted)
        for row in accepted:
            expected = "current" if row.as_of_date == latest else "historical"
            if row.period_role != expected:
                raise DebtResolutionError(
                    "debt_period_role_mismatch",
                    "Current and historical debt roles must follow exact accepted as-of ordering.",
                    subject_id=subject,
                    as_of_date=row.as_of_date,
                    declared_role=row.period_role,
                    expected_role=expected,
                    business_key=row.business_key,
                    source_row_ref=row.source_row_ref,
                )
    return ordered


def resolve_debt_facilities(rows: Sequence[Mapping[str, Any]]) -> tuple[ResolvedDebtFacilityDisposition, ...]:
    return _resolve_collection(rows, kind="facility", resolver=_resolved_facility)


def resolve_debt_instruments(rows: Sequence[Mapping[str, Any]]) -> tuple[ResolvedDebtInstrumentDisposition, ...]:
    return _resolve_collection(rows, kind="instrument", resolver=_resolved_instrument)


def resolve_debt_maturities(rows: Sequence[Mapping[str, Any]]) -> tuple[ResolvedDebtMaturityDisposition, ...]:
    return _resolve_collection(rows, kind="maturity", resolver=_resolved_maturity)


def resolve_debt_credit_notes(rows: Sequence[Mapping[str, Any]]) -> tuple[ResolvedDebtCreditNoteDisposition, ...]:
    return _resolve_collection(rows, kind="credit_note", resolver=_resolved_credit_note)


def resolve_debt_collections(section: Mapping[str, Any]) -> dict[str, tuple[Any, ...]]:
    facilities = resolve_debt_facilities(_mapping_rows(section.get("facilities"), field="facilities"))
    instruments = resolve_debt_instruments(_mapping_rows(section.get("instruments"), field="instruments"))
    maturities = resolve_debt_maturities(_mapping_rows(section.get("maturities"), field="maturities"))
    credit_notes = resolve_debt_credit_notes(_mapping_rows(section.get("credit_notes"), field="credit_notes"))
    instrument_roles = {row.instrument_id: row.aggregation_role for row in instruments}
    for maturity in maturities:
        role = instrument_roles.get(maturity.instrument_id)
        if role is None:
            raise DebtResolutionError(
                "debt_maturity_instrument_missing",
                "Maturity row references no resolved debt instrument.",
                instrument_id=maturity.instrument_id,
                business_key=maturity.business_key,
                source_row_ref=maturity.source_row_ref,
            )
        if role != "core_debt":
            raise DebtResolutionError(
                "debt_maturity_non_core_instrument",
                "Operating leases and other excluded instruments cannot enter core-debt maturities.",
                instrument_id=maturity.instrument_id,
                instrument_aggregation_role=role,
                business_key=maturity.business_key,
                source_row_ref=maturity.source_row_ref,
            )
    return {
        "facilities": facilities,
        "instruments": instruments,
        "maturities": maturities,
        "credit_notes": credit_notes,
    }


def _mapping_rows(value: Any, *, field: str) -> list[Mapping[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list) or any(not isinstance(row, Mapping) for row in value):
        raise DebtResolutionError(
            "invalid_debt_collection",
            "Debt collections must be arrays of typed row objects.",
            field=field,
        )
    return list(value)


def select_latest_debt_facilities(
    rows: Sequence[ResolvedDebtFacilityDisposition] | Sequence[Mapping[str, Any]],
    *,
    limit: int = 12,
) -> tuple[ResolvedDebtFacilityDisposition, ...]:
    if limit < 1:
        raise DebtResolutionError("invalid_debt_history_limit", "Debt history limit must be positive.", limit=limit)
    resolved = (
        tuple(rows)
        if all(isinstance(row, ResolvedDebtFacilityDisposition) for row in rows)
        else resolve_debt_facilities(rows)  # type: ignore[arg-type]
    )
    return tuple(sorted(resolved, key=lambda row: (row.as_of_date, row.business_key))[-limit:])


def dispositions_to_package_section(resolved: Mapping[str, Iterable[Any]]) -> dict[str, list[dict[str, Any]]]:
    return {
        name: [row.to_dict() for row in rows]
        for name, rows in resolved.items()
    }
