"""Ticker-neutral debt workbook rowsets and conditional sheet visibility."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from pbi_xbrl.debt_sheet_visibility import (
    DEBT_CREDIT_NOTES_SHEET,
    DEBT_MATURITY_SHEET,
    DEBT_PROFILE_SHEET,
    LEVERAGE_LIQUIDITY_SHEET,
    REVOLVER_HISTORY_SHEET,
    debt_sheet_default_states,
    debt_sheet_readiness_contracts,
)
from pbi_xbrl.new_ticker_debt_scope import (
    DebtResolutionError,
    ResolvedDebtAmount,
    ResolvedDebtCreditNoteDisposition,
    ResolvedDebtFacilityDisposition,
    ResolvedDebtInstrumentDisposition,
    ResolvedDebtMaturityDisposition,
    canonical_debt_id,
    resolve_debt_collections,
    select_latest_debt_facilities,
    validate_resolved_debt_facility_for_profile,
    validate_resolved_funded_debt_for_profile,
)


DEBT_PRODUCT_SHEETS = (
    "Debt_Profile",
    "Revolver_History",
    "Leverage_Liquidity",
    "Debt_Credit_Notes",
    "Debt_Maturity_Ladder",
    "Debt_Tranches_Latest",
    "Debt_Tranches_Q",
    "Debt_Buckets",
    "Debt_Recon",
)


class DebtProjectionError(ValueError):
    """Fail-closed workbook projection error with stable diagnostic identity."""

    def __init__(self, rule_id: str, message: str, **context: Any) -> None:
        self.rule_id = rule_id
        self.context = dict(context)
        detail = ", ".join(f"{key}={value!r}" for key, value in sorted(self.context.items()))
        super().__init__(f"{rule_id}: {message}" + (f" ({detail})" if detail else ""))


def _canonical_digest(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _joined(values: Sequence[str]) -> str:
    return " | ".join(dict.fromkeys(value for value in values if value))


def _amount_state(amount: ResolvedDebtAmount, *, excluded: bool = False) -> str:
    if excluded and amount.status == "populated":
        return "excluded_from_calculation"
    if amount.status == "populated" and amount.value == 0:
        return "reported_zero"
    return amount.status


@dataclass(frozen=True)
class DebtFacilityProjectionRole:
    """Explicit product role for one canonical facility identity."""

    facility_id: str
    profile_role: str
    liquidity_role: str
    aggregation_group_id: str
    evidence_refs: tuple[str, ...]


@dataclass(frozen=True)
class DebtFacilityProjectionPolicy:
    """Optional profile-owned proof for otherwise ambiguous facility projections."""

    roles: tuple[DebtFacilityProjectionRole, ...]


@dataclass(frozen=True)
class _ResolvedFacilityPeriodDisposition:
    primary_facility: ResolvedDebtFacilityDisposition
    components: tuple[ResolvedDebtFacilityDisposition, ...]
    component_facility_ids: tuple[str, ...]
    net_availability: float | None
    cash: float | None
    restricted_cash: float | None
    currency: str
    unit: str
    aggregation_mode: str
    aggregation_group_id: str
    evidence_key: str
    source_ref: str


@dataclass(frozen=True)
class ResolvedDebtProfileRow:
    row_key: str
    priority: int
    category: str
    item: str
    facility_or_instrument: str
    value: float | None
    unit: str
    as_of_date: str
    expiry_or_maturity: str
    state: str
    evidence_key: str
    definition_or_source: str
    source_ref: str

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


@dataclass(frozen=True)
class ResolvedRevolverHistoryRow:
    row_key: str
    priority: int
    as_of_date: str
    publication_date: str
    facility: str
    commitment: float | None
    loan_cap: float | None
    drawn: float | None
    letters_of_credit: float | None
    gross_capacity: float | None
    minimum_excess: float | None
    net_availability: float | None
    rate_basis: str
    expiry: str
    covenant_state: str
    source_state: str
    evidence_source: str
    source_ref: str

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


@dataclass(frozen=True)
class ResolvedLeverageLiquidityDisposition:
    row_key: str
    priority: int
    period: str
    as_of_date: str
    currency: str
    cash: float | None
    restricted_cash: float | None
    core_debt: float | None
    operating_leases: float | None
    revolver_availability: float | None
    disposition_state: str
    evidence_key: str
    component_period_explanation: str
    source_ref: str
    formula_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = dict(self.__dict__)
        payload["formula_ids"] = list(self.formula_ids)
        return payload


@dataclass(frozen=True)
class ResolvedDebtCreditNoteRow:
    row_key: str
    priority: int
    topic: str
    facility_or_instrument: str
    as_of_date: str
    publication_date: str
    exact_bounded_note: str
    state: str
    evidence_key: str
    source: str
    source_ref: str

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


@dataclass(frozen=True)
class ResolvedDebtMaturityRow:
    row_key: str
    priority: int
    instrument: str
    maturity_bucket: str
    amount: float
    unit: str
    due_date: str
    as_of_date: str
    state: str
    evidence_source: str
    source_ref: str

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


@dataclass(frozen=True)
class DebtWorkbookProjection:
    debt_profile_rows: tuple[ResolvedDebtProfileRow, ...]
    revolver_history_rows: tuple[ResolvedRevolverHistoryRow, ...]
    leverage_liquidity_rows: tuple[ResolvedLeverageLiquidityDisposition, ...]
    debt_credit_note_rows: tuple[ResolvedDebtCreditNoteRow, ...]
    debt_maturity_rows: tuple[ResolvedDebtMaturityRow, ...]
    sheet_states: tuple[tuple[str, str], ...]
    selection_audit: tuple[dict[str, Any], ...]
    blocking_issues: tuple[dict[str, Any], ...]
    projection_digest: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "debt_profile_rows": [row.to_dict() for row in self.debt_profile_rows],
            "revolver_history_rows": [row.to_dict() for row in self.revolver_history_rows],
            "leverage_liquidity_rows": [row.to_dict() for row in self.leverage_liquidity_rows],
            "debt_credit_note_rows": [row.to_dict() for row in self.debt_credit_note_rows],
            "debt_maturity_rows": [row.to_dict() for row in self.debt_maturity_rows],
            "sheet_states": dict(self.sheet_states),
            "selection_audit": [dict(row) for row in self.selection_audit],
            "blocking_issues": [dict(row) for row in self.blocking_issues],
            "projection_digest": self.projection_digest,
        }


def _period_end_map(package: Mapping[str, Any]) -> dict[str, str]:
    quarterly = package.get("quarterly_financials")
    rows = quarterly.get("rows") if isinstance(quarterly, Mapping) else None
    if not isinstance(rows, list):
        rows = []
    result: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        period = str(row.get("period") or "").strip()
        period_end = str(row.get("period_end") or "").strip()
        if not period or not period_end:
            continue
        prior = result.get(period_end)
        if prior is not None:
            raise DebtProjectionError(
                "duplicate_debt_period_end_identity",
                "Quarterly period ends must resolve to one exact fiscal period.",
                period_end=period_end,
                first_period=prior,
                conflicting_period=period,
            )
        result[period_end] = period
    return result


def _latest_by_date(
    rows: Sequence[Any],
    *,
    predicate: Any,
) -> Any | None:
    accepted = [row for row in rows if predicate(row)]
    return max(accepted, key=lambda row: (row.as_of_date, row.business_key, row.evidence_key)) if accepted else None


def _blocking_issue(rule_id: str, message: str, *, business_key: str, **context: Any) -> dict[str, Any]:
    return {
        "severity": "P1",
        "rule_id": rule_id,
        "business_key": business_key,
        "message": message,
        **context,
    }


def _facility_role_index(
    policy: DebtFacilityProjectionPolicy | None,
) -> dict[str, DebtFacilityProjectionRole]:
    if policy is None:
        return {}
    result: dict[str, DebtFacilityProjectionRole] = {}
    for raw_role in policy.roles:
        try:
            facility_id = canonical_debt_id(raw_role.facility_id, field="facility_id")
            aggregation_group_id = canonical_debt_id(
                raw_role.aggregation_group_id,
                field="aggregation_group_id",
            )
        except DebtResolutionError as exc:
            raise DebtProjectionError(exc.rule_id, str(exc), **exc.context) from exc
        profile_role = str(raw_role.profile_role or "").strip().lower()
        liquidity_role = str(raw_role.liquidity_role or "").strip().lower()
        if profile_role not in {"primary", "detail"}:
            raise DebtProjectionError(
                "invalid_debt_facility_profile_role",
                "Facility projection profile roles must be primary or detail.",
                facility_id=facility_id,
                profile_role=profile_role,
            )
        if liquidity_role not in {"primary", "additive", "overlapping_detail"}:
            raise DebtProjectionError(
                "invalid_debt_facility_liquidity_role",
                "Facility liquidity roles must explicitly define primary, additive, or overlapping detail behavior.",
                facility_id=facility_id,
                liquidity_role=liquidity_role,
            )
        evidence_refs = tuple(dict.fromkeys(str(value).strip() for value in raw_role.evidence_refs if str(value).strip()))
        if not evidence_refs:
            raise DebtProjectionError(
                "debt_facility_projection_role_lineage_missing",
                "Facility projection roles require explicit evidence for primary or aggregation treatment.",
                facility_id=facility_id,
            )
        if facility_id in result:
            raise DebtProjectionError(
                "duplicate_debt_facility_projection_role",
                "One canonical facility may have only one projection role.",
                facility_id=facility_id,
            )
        result[facility_id] = DebtFacilityProjectionRole(
            facility_id=facility_id,
            profile_role=profile_role,
            liquidity_role=liquidity_role,
            aggregation_group_id=aggregation_group_id,
            evidence_refs=evidence_refs,
        )
    return result


def _accepted_facilities(
    facilities: Sequence[ResolvedDebtFacilityDisposition],
) -> tuple[ResolvedDebtFacilityDisposition, ...]:
    return tuple(
        sorted(
            (
                row
                for row in facilities
                if row.source_status == "accepted"
                and row.resolution_status == "populated"
                and row.aggregation_role == "liquidity_capacity"
            ),
            key=lambda row: (row.as_of_date, row.facility_id, row.business_key),
        )
    )


def _select_current_profile_facility(
    facilities: Sequence[ResolvedDebtFacilityDisposition],
    role_by_facility_id: Mapping[str, DebtFacilityProjectionRole],
) -> tuple[ResolvedDebtFacilityDisposition | None, tuple[dict[str, Any], ...]]:
    accepted = _accepted_facilities(facilities)
    if not accepted:
        return None, ()
    latest_date = max(row.as_of_date for row in accepted)
    candidates = tuple(row for row in accepted if row.as_of_date == latest_date)
    if len(candidates) == 1:
        return candidates[0], ()

    candidate_ids = tuple(row.facility_id for row in candidates)
    candidate_roles = [role_by_facility_id.get(facility_id) for facility_id in candidate_ids]
    primary = [
        row
        for row, role in zip(candidates, candidate_roles, strict=True)
        if role is not None and role.profile_role == "primary"
    ]
    currencies = {row.currency for row in candidates}
    units = {row.net_availability.unit for row in candidates}
    if any(role is None for role in candidate_roles) or len(primary) != 1 or len(currencies) != 1 or len(units) != 1:
        issue = _blocking_issue(
            "debt_profile_ambiguous_current_facility",
            "The compact debt profile requires one explicitly declared current primary liquidity facility.",
            business_key=f"facility_period|{latest_date}",
            as_of_date=latest_date,
            facility_ids=list(candidate_ids),
            currencies=sorted(currencies),
            units=sorted(units),
            evidence_keys=[row.evidence_key for row in candidates],
            source_row_refs=[row.source_row_ref for row in candidates],
        )
        return None, (issue,)
    return primary[0], ()


def _amount_identity(amount: ResolvedDebtAmount) -> tuple[Any, ...]:
    return (amount.status, amount.value, amount.currency, amount.unit, amount.as_of_date)


def _resolve_facility_period(
    facilities: Sequence[ResolvedDebtFacilityDisposition],
    role_by_facility_id: Mapping[str, DebtFacilityProjectionRole],
) -> tuple[_ResolvedFacilityPeriodDisposition | None, tuple[dict[str, Any], ...]]:
    components = tuple(sorted(facilities, key=lambda row: (row.facility_id, row.business_key)))
    if not components:
        return None, ()
    if len(components) == 1:
        facility = components[0]
        return (
            _ResolvedFacilityPeriodDisposition(
                primary_facility=facility,
                components=components,
                component_facility_ids=(facility.facility_id,),
                net_availability=facility.net_availability.value,
                cash=facility.cash_and_equivalents.value,
                restricted_cash=facility.restricted_cash.value,
                currency=facility.currency,
                unit=facility.net_availability.unit,
                aggregation_mode="single_facility",
                aggregation_group_id=facility.facility_id,
                evidence_key=facility.evidence_key,
                source_ref=facility.source_ref,
            ),
            (),
        )

    as_of_date = components[0].as_of_date
    facility_ids = tuple(row.facility_id for row in components)
    roles = [role_by_facility_id.get(facility_id) for facility_id in facility_ids]
    currencies = {row.currency for row in components}
    units = {row.net_availability.unit for row in components}
    groups = {role.aggregation_group_id for role in roles if role is not None}
    primary_roles = [
        row
        for row, role in zip(components, roles, strict=True)
        if role is not None and role.profile_role == "primary"
    ]
    amount_compatible = (
        all(row.net_availability.status == "populated" and row.net_availability.value is not None for row in components)
        and len({_amount_identity(row.cash_and_equivalents) for row in components}) == 1
        and len({_amount_identity(row.restricted_cash) for row in components}) == 1
    )
    role_set = {role.liquidity_role for role in roles if role is not None}
    additive = len(role_set) == 1 and role_set == {"additive"}
    primary_overlap = (
        len(primary_roles) == 1
        and sum(role is not None and role.liquidity_role == "primary" for role in roles) == 1
        and all(
            role is not None and role.liquidity_role in {"primary", "overlapping_detail"}
            for role in roles
        )
        and role_by_facility_id[primary_roles[0].facility_id].liquidity_role == "primary"
    )
    compatible = (
        all(role is not None for role in roles)
        and len(primary_roles) == 1
        and len(currencies) == 1
        and len(units) == 1
        and len(groups) == 1
        and amount_compatible
        and (additive or primary_overlap)
    )
    if not compatible:
        issue = _blocking_issue(
            "debt_liquidity_ambiguous_facility_aggregation",
            "Multiple same-date facilities require explicit, compatible and evidence-backed aggregation roles.",
            business_key=f"facility_period|{as_of_date}",
            as_of_date=as_of_date,
            facility_ids=list(facility_ids),
            currencies=sorted(currencies),
            units=sorted(units),
            aggregation_group_ids=sorted(groups),
            liquidity_roles=[role.liquidity_role if role is not None else "missing" for role in roles],
            evidence_keys=[row.evidence_key for row in components],
            source_row_refs=[row.source_row_ref for row in components],
        )
        return None, (issue,)

    primary = primary_roles[0]
    policy_evidence = [value for role in roles if role is not None for value in role.evidence_refs]
    availability = (
        round(sum(float(row.net_availability.value) for row in components), 6)
        if additive
        else primary.net_availability.value
    )
    return (
        _ResolvedFacilityPeriodDisposition(
            primary_facility=primary,
            components=components,
            component_facility_ids=facility_ids,
            net_availability=availability,
            cash=primary.cash_and_equivalents.value,
            restricted_cash=primary.restricted_cash.value,
            currency=primary.currency,
            unit=primary.net_availability.unit,
            aggregation_mode="additive_non_overlapping" if additive else "primary_with_overlapping_detail",
            aggregation_group_id=next(iter(groups)),
            evidence_key=_joined([*(row.evidence_key for row in components), *policy_evidence]),
            source_ref=_joined([*(row.source_ref for row in components), *policy_evidence]),
        ),
        (),
    )


def _core_debt_at(
    instruments: Sequence[ResolvedDebtInstrumentDisposition],
    *,
    as_of_date: str,
    currency: str,
) -> tuple[float | None, tuple[ResolvedDebtInstrumentDisposition, ...]]:
    eligible = tuple(
        sorted(
            (
                row
                for row in instruments
                if row.as_of_date == as_of_date
                and row.currency == currency
                and row.source_status == "accepted"
                and row.aggregation_role == "core_debt"
                and row.balance.status == "populated"
                and row.balance.value is not None
            ),
            key=lambda row: row.business_key,
        )
    )
    if not eligible:
        return None, ()
    return round(sum(float(row.balance.value) for row in eligible), 6), eligible


def _current_core_debt_zero_evidence(
    facility: ResolvedDebtFacilityDisposition,
    instruments: Sequence[ResolvedDebtInstrumentDisposition],
    notes: Sequence[ResolvedDebtCreditNoteDisposition],
) -> tuple[ResolvedDebtCreditNoteDisposition, ...]:
    """Return the typed evidence pair proving current core funded debt is zero.

    The proof is intentionally narrow: the current facility must carry a
    source-backed reported-zero draw, an accepted same-date draw-status note,
    and an accepted prior redemption of all funded notes.  Any accepted
    same-date core-debt instrument defeats the zero proof.
    """

    if (
        facility.period_role != "current"
        or facility.drawn_status != "reported_zero"
        or facility.drawn_balance.status != "populated"
        or facility.drawn_balance.value != 0
    ):
        return ()
    if any(
        row.as_of_date == facility.as_of_date
        and row.currency == facility.currency
        and row.source_status == "accepted"
        and row.aggregation_role == "core_debt"
        and row.balance.status == "populated"
        and row.balance.value is not None
        for row in instruments
    ):
        return ()
    draw_status = tuple(
        row
        for row in notes
        if row.as_of_date == facility.as_of_date
        and row.source_status == "accepted"
        and row.resolution_status == "populated"
        and row.note_type == "facility_draw_status"
        and row.subject_id == facility.facility_id
    )
    redemptions = tuple(
        row
        for row in notes
        if row.as_of_date <= facility.as_of_date
        and row.source_status == "accepted"
        and row.resolution_status == "populated"
        and row.note_type == "debt_redemption"
    )
    if len(draw_status) != 1 or not redemptions:
        return ()
    latest_redemption = max(
        redemptions,
        key=lambda row: (row.as_of_date, row.publication_date, row.business_key),
    )
    if any(
        row.source_status == "accepted"
        and row.aggregation_role == "core_debt"
        and latest_redemption.as_of_date < row.as_of_date <= facility.as_of_date
        for row in instruments
    ):
        return ()
    return (draw_status[0], latest_redemption)


def _debt_profile_rows(
    facility: ResolvedDebtFacilityDisposition | None,
    instruments: Sequence[ResolvedDebtInstrumentDisposition],
    notes: Sequence[ResolvedDebtCreditNoteDisposition],
) -> tuple[ResolvedDebtProfileRow, ...]:
    if facility is None:
        current_core = tuple(
            sorted(
                (
                    row
                    for row in instruments
                    if row.period_role == "current"
                    and row.source_status == "accepted"
                    and row.aggregation_role == "core_debt"
                    and row.balance.status == "populated"
                    and row.balance.value is not None
                ),
                key=lambda row: row.business_key,
            )
        )
        if not current_core:
            return ()
        rows = [
            ResolvedDebtProfileRow(
                row_key=f"debt_profile|instrument|{row.business_key}",
                priority=priority,
                category="core_debt",
                item=row.instrument_name,
                facility_or_instrument=row.instrument_name,
                value=row.balance.value,
                unit=row.balance.unit,
                as_of_date=row.as_of_date,
                expiry_or_maturity=row.maturity_date,
                state=_amount_state(row.balance),
                evidence_key=row.evidence_key,
                definition_or_source=(
                    f"{row.instrument_type.replace('_', ' ')}; {row.secured_status}; {row.seniority}."
                ),
                source_ref=row.source_ref,
            )
            for priority, row in enumerate(current_core, start=1)
        ]
        currencies = {row.currency for row in current_core}
        dates = {row.as_of_date for row in current_core}
        if len(currencies) != 1 or len(dates) != 1:
            raise DebtProjectionError(
                "debt_profile_instrument_aggregation_incompatible",
                "The compact funded-debt total requires one currency and one exact as-of date.",
                currencies=sorted(currencies),
                as_of_dates=sorted(dates),
                business_keys=[row.business_key for row in current_core],
            )
        rows.append(
            ResolvedDebtProfileRow(
                row_key=f"debt_profile|total|{next(iter(dates))}",
                priority=len(rows) + 1,
                category="core_debt",
                item="Core funded debt",
                facility_or_instrument=_joined([row.instrument_name for row in current_core]),
                value=round(sum(float(row.balance.value) for row in current_core), 6),
                unit="$m",
                as_of_date=next(iter(dates)),
                expiry_or_maturity="",
                state="populated",
                evidence_key=_joined([row.evidence_key for row in current_core]),
                definition_or_source="Source-backed current core funded debt instruments.",
                source_ref=_joined([row.source_ref for row in current_core]),
            )
        )
        return tuple(rows)
    lease = _latest_by_date(
        instruments,
        predicate=lambda row: row.as_of_date == facility.as_of_date
        and row.source_status == "accepted"
        and row.aggregation_role == "excluded_from_core_debt"
        and row.instrument_type == "operating_lease_liability",
    )
    core_debt, core_rows = _core_debt_at(instruments, as_of_date=facility.as_of_date, currency=facility.currency)
    zero_evidence = (
        _current_core_debt_zero_evidence(facility, instruments, notes)
        if core_debt is None
        else ()
    )
    if zero_evidence:
        core_debt = 0.0
    amount_specs = (
        ("facility_capacity", "Revolver commitment", facility.facility_name, facility.facility_expiry_date, facility.commitment, False, "Committed revolving facility size."),
        ("facility_capacity", "Borrowing-base / loan cap", facility.facility_name, facility.facility_expiry_date, facility.loan_cap, False, "Lesser borrowing-base or contractual loan cap."),
        ("facility_balance", "Revolver drawn balance", facility.facility_name, facility.facility_expiry_date, facility.drawn_balance, False, "Reported revolver borrowings; unavailable is not zero."),
        ("facility_capacity", "Letters of credit", facility.facility_name, facility.facility_expiry_date, facility.letters_of_credit, False, "Letters of credit reduce gross revolver capacity."),
        ("facility_capacity", "Gross revolver capacity", facility.facility_name, facility.facility_expiry_date, facility.gross_capacity, False, "Loan cap less letters of credit."),
        ("facility_capacity", "Minimum excess availability", facility.facility_name, facility.facility_expiry_date, facility.minimum_excess_availability, False, "Required excess availability held back from usable liquidity."),
        ("liquidity", "Net revolver availability", facility.facility_name, facility.facility_expiry_date, facility.net_availability, False, "Gross capacity less minimum excess availability."),
        ("liquidity", "Cash and cash equivalents", "Total Company", "", facility.cash_and_equivalents, False, "Unrestricted cash and cash equivalents."),
        ("excluded", "Restricted cash - excluded from liquidity", "Total Company", "", facility.restricted_cash, True, "Restricted cash is displayed separately and excluded from available liquidity."),
    )
    result: list[ResolvedDebtProfileRow] = []
    for priority, (category, item, subject, expiry, amount, excluded, definition) in enumerate(amount_specs, start=1):
        result.append(
            ResolvedDebtProfileRow(
                row_key=f"debt_profile|{priority:02d}|{facility.as_of_date}",
                priority=priority,
                category=category,
                item=item,
                facility_or_instrument=subject,
                value=amount.value,
                unit=amount.unit if amount.value is not None else "",
                as_of_date=facility.as_of_date if amount.value is not None else "",
                expiry_or_maturity=expiry,
                state=_amount_state(amount, excluded=excluded),
                evidence_key=facility.evidence_key,
                definition_or_source=definition,
                source_ref=amount.source_ref,
            )
        )
    if lease is not None:
        result.append(
            ResolvedDebtProfileRow(
                row_key=f"debt_profile|10|{facility.as_of_date}",
                priority=10,
                category="excluded",
                item="Operating lease liabilities - excluded from core debt",
                facility_or_instrument=lease.instrument_name,
                value=lease.balance.value,
                unit=lease.balance.unit if lease.balance.value is not None else "",
                as_of_date=lease.as_of_date if lease.balance.value is not None else "",
                expiry_or_maturity="",
                state=_amount_state(lease.balance, excluded=True),
                evidence_key=lease.evidence_key,
                definition_or_source="Operating lease liabilities remain separate from core funded debt.",
                source_ref=lease.balance.source_ref,
            )
        )
    else:
        result.append(
            ResolvedDebtProfileRow(
                row_key=f"debt_profile|10|{facility.as_of_date}",
                priority=10,
                category="excluded",
                item="Operating lease liabilities - excluded from core debt",
                facility_or_instrument="",
                value=None,
                unit="",
                as_of_date="",
                expiry_or_maturity="",
                state="unavailable",
                evidence_key=facility.evidence_key,
                definition_or_source="No compatible operating-lease record resolved for the facility date.",
                source_ref=facility.source_ref,
            )
        )
    result.append(
        ResolvedDebtProfileRow(
            row_key=f"debt_profile|11|{facility.as_of_date}",
            priority=11,
            category="core_debt",
            item="Core funded debt state",
            facility_or_instrument=(
                _joined([row.instrument_name for row in core_rows])
                if core_rows
                else "No funded core debt"
                if zero_evidence
                else ""
            ),
            value=core_debt,
            unit="$m" if core_debt is not None else "",
            as_of_date=facility.as_of_date,
            expiry_or_maturity="",
            state=(
                "reported_zero"
                if zero_evidence
                else "populated"
                if core_debt is not None
                else "unavailable"
            ),
            evidence_key=_joined(
                [
                    *(row.evidence_key for row in core_rows),
                    *(row.evidence_key for row in zero_evidence),
                ]
            )
            or facility.evidence_key,
            definition_or_source=(
                "Source-backed core funded debt instruments."
                if core_rows
                else "Source-backed zero: all funded notes redeemed and no ABL borrowings outstanding."
                if zero_evidence
                else "no source-backed core borrowings identified"
            ),
            source_ref=_joined(
                [
                    *(row.source_ref for row in core_rows),
                    *(row.source_ref for row in zero_evidence),
                ]
            )
            or facility.source_ref,
        )
    )
    return tuple(result)


def _note_index(
    notes: Sequence[ResolvedDebtCreditNoteDisposition],
) -> dict[tuple[str, str], ResolvedDebtCreditNoteDisposition]:
    index: dict[tuple[str, str], ResolvedDebtCreditNoteDisposition] = {}
    for row in notes:
        if row.source_status != "accepted":
            continue
        key = (row.as_of_date, row.note_type)
        prior = index.get(key)
        if prior is not None:
            raise DebtProjectionError(
                "duplicate_debt_note_projection_identity",
                "A debt note topic and as-of date must resolve to one accepted source record.",
                as_of_date=row.as_of_date,
                note_type=row.note_type,
                first_business_key=prior.business_key,
                conflicting_business_key=row.business_key,
            )
        index[key] = row
    return index


def _revolver_history_rows(
    facilities: Sequence[ResolvedDebtFacilityDisposition],
    notes: Sequence[ResolvedDebtCreditNoteDisposition],
    *,
    primary_facility_id: str | None,
) -> tuple[ResolvedRevolverHistoryRow, ...]:
    accepted = _accepted_facilities(facilities)
    if primary_facility_id:
        primary = tuple(row for row in accepted if row.facility_id == primary_facility_id)
        selected_rows = list(primary[-12:])
        if len(selected_rows) < 12:
            selected_keys = {row.business_key for row in selected_rows}
            supplements = [row for row in accepted if row.business_key not in selected_keys]
            selected_rows.extend(supplements[-(12 - len(selected_rows)) :])
        selected = tuple(sorted(selected_rows, key=lambda row: (row.as_of_date, row.facility_id, row.business_key)))
    else:
        selected = select_latest_debt_facilities(accepted, limit=12) if accepted else ()
    note_by_date_type = _note_index(notes)
    result: list[ResolvedRevolverHistoryRow] = []
    for priority, facility in enumerate(selected, start=1):
        covenant = note_by_date_type.get((facility.as_of_date, "covenant_compliance"))
        missing_drawn = facility.drawn_balance.value is None
        evidence = [facility.evidence_key]
        refs = [facility.source_ref]
        if covenant is not None:
            evidence.append(covenant.evidence_key)
            refs.append(covenant.source_ref)
        result.append(
            ResolvedRevolverHistoryRow(
                row_key=f"revolver_history|{facility.business_key}",
                priority=priority,
                as_of_date=facility.as_of_date,
                publication_date=facility.publication_date,
                facility=facility.facility_name,
                commitment=facility.commitment.value,
                loan_cap=facility.loan_cap.value,
                drawn=facility.drawn_balance.value,
                letters_of_credit=facility.letters_of_credit.value,
                gross_capacity=facility.gross_capacity.value,
                minimum_excess=facility.minimum_excess_availability.value,
                net_availability=facility.net_availability.value,
                rate_basis="Not reported",
                expiry=facility.facility_expiry_date,
                covenant_state="Reported compliance note" if covenant is not None else "Not reported",
                source_state="accepted_with_missing_drawn" if missing_drawn else "accepted",
                evidence_source=_joined(evidence),
                source_ref=_joined(refs),
            )
        )
    return tuple(result)


def _leverage_liquidity_rows(
    package: Mapping[str, Any],
    facilities: Sequence[ResolvedDebtFacilityDisposition],
    instruments: Sequence[ResolvedDebtInstrumentDisposition],
    notes: Sequence[ResolvedDebtCreditNoteDisposition],
    role_by_facility_id: Mapping[str, DebtFacilityProjectionRole],
) -> tuple[tuple[ResolvedLeverageLiquidityDisposition, ...], tuple[dict[str, Any], ...]]:
    by_date: dict[str, list[ResolvedDebtFacilityDisposition]] = {}
    for facility in _accepted_facilities(facilities):
        by_date.setdefault(facility.as_of_date, []).append(facility)
    selected_dates = sorted(by_date)[-12:]
    period_by_end = _period_end_map(package)
    result: list[ResolvedLeverageLiquidityDisposition] = []
    issues: list[dict[str, Any]] = []
    for as_of_date in selected_dates:
        facility_period, period_issues = _resolve_facility_period(by_date[as_of_date], role_by_facility_id)
        if period_issues:
            issues.extend(period_issues)
            continue
        if facility_period is None:
            continue
        facility = facility_period.primary_facility
        period = period_by_end.get(as_of_date, "")
        if not period:
            raise DebtProjectionError(
                "debt_projection_period_identity_missing",
                "Every visible leverage/liquidity row requires one exact quarterly period identity.",
                facility_business_keys=[row.business_key for row in facility_period.components],
                as_of_date=as_of_date,
                source_row_refs=[row.source_row_ref for row in facility_period.components],
            )
        lease = _latest_by_date(
            instruments,
            predicate=lambda row: row.as_of_date == as_of_date
            and row.source_status == "accepted"
            and row.aggregation_role == "excluded_from_core_debt"
            and row.instrument_type == "operating_lease_liability",
        )
        core_debt, core_rows = _core_debt_at(
            instruments,
            as_of_date=as_of_date,
            currency=facility_period.currency,
        )
        zero_evidence = (
            _current_core_debt_zero_evidence(facility, instruments, notes)
            if core_debt is None and len(facility_period.components) == 1
            else ()
        )
        if zero_evidence:
            core_debt = 0.0
        evidence = [facility_period.evidence_key]
        refs = [facility_period.source_ref]
        if lease is not None:
            evidence.append(lease.evidence_key)
            refs.append(lease.source_ref)
        evidence.extend(row.evidence_key for row in core_rows)
        refs.extend(row.source_ref for row in core_rows)
        evidence.extend(row.evidence_key for row in zero_evidence)
        refs.extend(row.source_ref for row in zero_evidence)
        state = (
            "source_backed_reported_zero"
            if zero_evidence
            else "source_backed"
            if core_debt is not None
            else "debt_unavailable"
        )
        if facility_period.aggregation_mode == "single_facility":
            explanation = (
                f"Cash, restricted cash, revolver availability and leases use {as_of_date}; "
                + (
                    "core debt uses the same date and is source-backed reported zero."
                    if zero_evidence
                    else "core debt uses the same date."
                    if core_debt is not None
                    else "core debt is unavailable and is not replaced by leases."
                )
            )
            row_key = f"leverage_liquidity|{facility.business_key}"
        else:
            explanation = (
                f"Cash, restricted cash and leases use {as_of_date}; revolver availability uses "
                f"{facility_period.aggregation_mode} for {', '.join(facility_period.component_facility_ids)}; "
                + (
                    "core debt uses the same date and is source-backed reported zero."
                    if zero_evidence
                    else "core debt uses the same date."
                    if core_debt is not None
                    else "core debt is unavailable and is not replaced by leases."
                )
            )
            row_key = (
                f"leverage_liquidity|period|{as_of_date}|"
                f"{facility_period.aggregation_group_id}"
            )
        result.append(
            ResolvedLeverageLiquidityDisposition(
                row_key=row_key,
                priority=len(result) + 1,
                period=period,
                as_of_date=as_of_date,
                currency=facility_period.currency,
                cash=facility_period.cash,
                restricted_cash=facility_period.restricted_cash,
                core_debt=core_debt,
                operating_leases=lease.balance.value if lease is not None else None,
                revolver_availability=facility_period.net_availability,
                disposition_state=state,
                evidence_key=_joined(evidence),
                component_period_explanation=explanation,
                source_ref=_joined(refs),
                formula_ids=(
                    "debt_product_net_debt",
                    "debt_product_same_date_liquidity",
                    "debt_product_gross_leverage",
                    "debt_product_net_leverage",
                ),
            )
        )
    return tuple(result), tuple(issues)


_CREDIT_TOPIC_SPECS = (
    (1, "Facility and security terms", frozenset({"facility_amendment"})),
    (2, "Rate basis, spread and unused fee", frozenset()),
    (3, "Borrowing-base scope and sublimits", frozenset({"borrowing_restriction"})),
    (4, "Minimum availability requirement", frozenset()),
    (5, "Covenant compliance", frozenset({"covenant_compliance"})),
    (6, "Senior-notes redemption / no ABL borrowings", frozenset({"debt_redemption", "facility_draw_status"})),
)


def _credit_note_rows(
    notes: Sequence[ResolvedDebtCreditNoteDisposition],
) -> tuple[ResolvedDebtCreditNoteRow, ...]:
    result: list[ResolvedDebtCreditNoteRow] = []
    for priority, topic, note_types in _CREDIT_TOPIC_SPECS:
        candidates = [
            row
            for row in notes
            if row.source_status == "accepted" and row.resolution_status == "populated" and row.note_type in note_types
        ]
        if not candidates:
            continue
        latest_identity = max((row.as_of_date, row.publication_date) for row in candidates)
        winners = [row for row in candidates if (row.as_of_date, row.publication_date) == latest_identity]
        if len(winners) != 1:
            raise DebtProjectionError(
                "conflicting_debt_credit_note_projection",
                "A visible credit-note topic has multiple accepted latest records.",
                topic=topic,
                as_of_date=latest_identity[0],
                publication_date=latest_identity[1],
                business_keys=[row.business_key for row in winners],
                source_row_refs=[row.source_row_ref for row in winners],
            )
        row = winners[0]
        result.append(
            ResolvedDebtCreditNoteRow(
                row_key=f"debt_credit_note|{priority:02d}|{row.business_key}",
                priority=priority,
                topic=topic,
                facility_or_instrument=row.subject_id,
                as_of_date=row.as_of_date,
                publication_date=row.publication_date,
                exact_bounded_note=row.text,
                state="accepted",
                evidence_key=row.evidence_key,
                source=row.source_table_scope,
                source_ref=row.source_ref,
            )
        )
    return tuple(result)


def _maturity_rows_and_issues(
    instruments: Sequence[ResolvedDebtInstrumentDisposition],
    maturities: Sequence[ResolvedDebtMaturityDisposition],
) -> tuple[tuple[ResolvedDebtMaturityRow, ...], tuple[dict[str, Any], ...]]:
    core = tuple(
        row
        for row in instruments
        if row.period_role == "current"
        and row.source_status == "accepted"
        and row.aggregation_role == "core_debt"
        and row.balance.status == "populated"
        and row.balance.value is not None
    )
    if not core:
        return (), ()
    by_instrument: dict[str, list[ResolvedDebtMaturityDisposition]] = {}
    for row in maturities:
        if (
            row.source_status == "accepted"
            and row.resolution_status == "populated"
            and row.amount.status == "populated"
        ):
            by_instrument.setdefault(row.instrument_id, []).append(row)
    issues: list[dict[str, Any]] = []
    result: list[ResolvedDebtMaturityRow] = []
    for instrument in sorted(core, key=lambda row: row.business_key):
        rows = by_instrument.get(instrument.instrument_id, [])
        period_mismatches = [row for row in rows if row.as_of_date != instrument.as_of_date]
        if period_mismatches:
            issues.append(
                _blocking_issue(
                    "debt_maturity_instrument_period_mismatch",
                    "Every maturity used for an active instrument must share its exact source snapshot date.",
                    business_key=instrument.business_key,
                    instrument_id=instrument.instrument_id,
                    issuer=instrument.issuer,
                    instrument_as_of_date=instrument.as_of_date,
                    maturity_as_of_dates=sorted({row.as_of_date for row in period_mismatches}),
                    maturity_business_keys=[row.business_key for row in period_mismatches],
                    evidence_keys=[row.evidence_key for row in period_mismatches],
                    source_row_refs=[row.source_row_ref for row in period_mismatches],
                    source_refs=[row.source_ref for row in period_mismatches],
                )
            )
            continue
        identity_mismatches = [
            row
            for row in rows
            if row.currency != instrument.currency
            or row.amount.currency != instrument.balance.currency
            or row.amount.unit != instrument.balance.unit
            or row.aggregation_role != "core_debt_maturity"
        ]
        if identity_mismatches:
            issues.append(
                _blocking_issue(
                    "debt_maturity_instrument_identity_mismatch",
                    "Maturity currency, unit and aggregation identity must match the funded instrument.",
                    business_key=instrument.business_key,
                    instrument_id=instrument.instrument_id,
                    instrument_currency=instrument.currency,
                    instrument_unit=instrument.balance.unit,
                    maturity_business_keys=[row.business_key for row in identity_mismatches],
                    maturity_currencies=[row.currency for row in identity_mismatches],
                    maturity_units=[row.amount.unit for row in identity_mismatches],
                    evidence_keys=[row.evidence_key for row in identity_mismatches],
                    source_row_refs=[row.source_row_ref for row in identity_mismatches],
                )
            )
            continue
        amount = round(sum(float(row.amount.value or 0.0) for row in rows), 6)
        if not rows or abs(amount - float(instrument.balance.value)) > 0.000001:
            issues.append(
                {
                    "severity": "P1",
                    "rule_id": "debt_maturity_reconciliation_failed",
                    "business_key": instrument.business_key,
                    "instrument_id": instrument.instrument_id,
                    "instrument_balance": instrument.balance.value,
                    "maturity_total": amount if rows else None,
                    "source_row_ref": instrument.source_row_ref,
                    "message": "Accepted maturity rows do not reconcile to the eligible funded instrument balance.",
                }
            )
            continue
        for row in sorted(rows, key=lambda item: (item.due_date, item.business_key)):
            result.append(
                ResolvedDebtMaturityRow(
                    row_key=f"debt_maturity|{row.business_key}",
                    priority=len(result) + 1,
                    instrument=instrument.instrument_name,
                    maturity_bucket=row.maturity_bucket,
                    amount=float(row.amount.value),
                    unit=row.amount.unit,
                    due_date=row.due_date,
                    as_of_date=row.as_of_date,
                    state="accepted",
                    evidence_source=_joined((row.evidence_key, instrument.evidence_key)),
                    source_ref=_joined((row.source_ref, instrument.source_ref)),
                )
            )
    if issues:
        return (), tuple(issues)
    return tuple(result), ()


def build_debt_workbook_projection(
    package: Mapping[str, Any],
    *,
    debt_module_active: bool = True,
    facility_policy: DebtFacilityProjectionPolicy | None = None,
) -> DebtWorkbookProjection:
    """Resolve all debt product rowsets and visibility from fresh package facts."""

    if not debt_module_active:
        payload = {
            "debt_profile_rows": [],
            "revolver_history_rows": [],
            "leverage_liquidity_rows": [],
            "debt_credit_note_rows": [],
            "debt_maturity_rows": [],
            "sheet_states": {sheet: "hidden" for sheet in DEBT_PRODUCT_SHEETS},
            "selection_audit": [],
            "blocking_issues": [],
        }
        return DebtWorkbookProjection(
            (), (), (), (), (), tuple(payload["sheet_states"].items()), (), (), _canonical_digest(payload)
        )
    section = package.get("debt_liquidity")
    if not isinstance(section, Mapping):
        section = {}
    try:
        resolved = resolve_debt_collections(section)
    except DebtResolutionError as exc:
        raise DebtProjectionError(exc.rule_id, str(exc), **exc.context) from exc
    facilities = resolved["facilities"]
    instruments = resolved["instruments"]
    maturities = resolved["maturities"]
    notes = resolved["credit_notes"]
    role_by_facility_id = _facility_role_index(facility_policy)
    profile_facility, profile_issues = _select_current_profile_facility(facilities, role_by_facility_id)
    profile_rows = _debt_profile_rows(profile_facility, instruments, notes) if not profile_issues else ()
    profile_economic_validation = (
        validate_resolved_debt_facility_for_profile(profile_facility)
        if profile_facility is not None
        else validate_resolved_funded_debt_for_profile(instruments)
    )
    history_rows = _revolver_history_rows(
        facilities,
        notes,
        primary_facility_id=profile_facility.facility_id if profile_facility is not None else None,
    )
    leverage_rows, liquidity_issues = _leverage_liquidity_rows(
        package,
        facilities,
        instruments,
        notes,
        role_by_facility_id,
    )
    credit_rows = _credit_note_rows(notes)
    maturity_rows, maturity_issues = _maturity_rows_and_issues(instruments, maturities)
    blocking_issues = (*profile_issues, *liquidity_issues, *maturity_issues)

    useful_profile_rows = sum(row.value is not None or row.state == "unavailable" for row in profile_rows)
    conditional_sheets = (
        DEBT_PROFILE_SHEET,
        REVOLVER_HISTORY_SHEET,
        LEVERAGE_LIQUIDITY_SHEET,
        DEBT_CREDIT_NOTES_SHEET,
        DEBT_MATURITY_SHEET,
    )
    readiness_contracts = debt_sheet_readiness_contracts(conditional_sheets)
    visibility_minimums = {
        sheet_name: contract.minimum_count
        for sheet_name, contract in readiness_contracts.items()
        if contract.minimum_count is not None
    }
    states = debt_sheet_default_states()
    if set(states) != set(DEBT_PRODUCT_SHEETS):
        raise DebtProjectionError(
            "debt_projection_visibility_manifest_mismatch",
            "Debt product sheets must exactly match the manifest-owned debt sheet inventory.",
            missing_sheets=sorted(set(DEBT_PRODUCT_SHEETS) - set(states)),
            unexpected_sheets=sorted(set(states) - set(DEBT_PRODUCT_SHEETS)),
        )
    states[DEBT_PROFILE_SHEET] = (
        readiness_contracts[DEBT_PROFILE_SHEET].ready_state
        if profile_economic_validation.passed
        and useful_profile_rows >= visibility_minimums[DEBT_PROFILE_SHEET]
        and not blocking_issues
        else states[DEBT_PROFILE_SHEET]
    )
    states[REVOLVER_HISTORY_SHEET] = (
        readiness_contracts[REVOLVER_HISTORY_SHEET].ready_state
        if len(history_rows) >= visibility_minimums[REVOLVER_HISTORY_SHEET] and not blocking_issues
        else states[REVOLVER_HISTORY_SHEET]
    )
    states[LEVERAGE_LIQUIDITY_SHEET] = (
        readiness_contracts[LEVERAGE_LIQUIDITY_SHEET].ready_state
        if len(leverage_rows) >= visibility_minimums[LEVERAGE_LIQUIDITY_SHEET] and not blocking_issues
        else states[LEVERAGE_LIQUIDITY_SHEET]
    )
    states[DEBT_CREDIT_NOTES_SHEET] = (
        readiness_contracts[DEBT_CREDIT_NOTES_SHEET].ready_state
        if len(credit_rows) >= visibility_minimums[DEBT_CREDIT_NOTES_SHEET] and not blocking_issues
        else states[DEBT_CREDIT_NOTES_SHEET]
    )
    states[DEBT_MATURITY_SHEET] = (
        readiness_contracts[DEBT_MATURITY_SHEET].ready_state
        if maturity_rows and not blocking_issues
        else states[DEBT_MATURITY_SHEET]
    )

    selected_history_keys = {row.row_key.removeprefix("revolver_history|") for row in history_rows}
    audit = tuple(
        {
            "business_key": row.business_key,
            "as_of_date": row.as_of_date,
            "selected_for_history": row.business_key in selected_history_keys,
            "disposition": "selected" if row.business_key in selected_history_keys else "outside_latest_twelve",
        }
        for row in facilities
    )
    payload = {
        "debt_profile_rows": [row.to_dict() for row in profile_rows],
        "revolver_history_rows": [row.to_dict() for row in history_rows],
        "leverage_liquidity_rows": [row.to_dict() for row in leverage_rows],
        "debt_credit_note_rows": [row.to_dict() for row in credit_rows],
        "debt_maturity_rows": [row.to_dict() for row in maturity_rows],
        "sheet_states": states,
        "selection_audit": list(audit),
        "blocking_issues": list(blocking_issues),
    }
    return DebtWorkbookProjection(
        debt_profile_rows=profile_rows,
        revolver_history_rows=history_rows,
        leverage_liquidity_rows=leverage_rows,
        debt_credit_note_rows=credit_rows,
        debt_maturity_rows=maturity_rows,
        sheet_states=tuple((sheet, states[sheet]) for sheet in DEBT_PRODUCT_SHEETS),
        selection_audit=audit,
        blocking_issues=blocking_issues,
        projection_digest=_canonical_digest(payload),
    )
