"""Semantic ownership for source-backed debt-rate facts.

Rate values are selected only after instrument, reporting period, semantic role,
and basis/scope ownership are established.  Source occurrence is retained as
lineage and never used as an economic tie-breaker.
"""
from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Iterable, Optional, Sequence, Tuple


DEBT_RATE_OWNERSHIP_CONTRACT_ID = "contract:debt-rate-semantic-ownership@1"


class DebtRateOwnershipError(ValueError):
    """Raised when a canonical debt-rate owner cannot be established."""


class DebtRateConflictError(DebtRateOwnershipError):
    """Raised for incompatible same-authority facts with one semantic identity."""


class DebtRateRole(str, Enum):
    COUPON_STATED_RATE = "coupon_stated_rate"
    EFFECTIVE_INTEREST_RATE = "effective_interest_rate"
    FLOATING_BASE_RATE = "floating_base_rate"
    SPREAD_MARGIN = "spread_margin"
    ALL_IN_RATE = "all_in_rate"
    CONVERSION_RELATED_RATE = "conversion_related_rate"
    OTHER_PERCENTAGE = "other_percentage"
    NOT_A_RATE = "not_a_rate"


class DebtRateAuthority(str, Enum):
    DERIVED_DISPLAY = "derived_display"
    TABLE_DIRECT = "table_direct"
    STRUCTURED_DIRECT = "structured_direct"


_AUTHORITY_RANK = {
    DebtRateAuthority.DERIVED_DISPLAY: 1,
    DebtRateAuthority.TABLE_DIRECT: 2,
    DebtRateAuthority.STRUCTURED_DIRECT: 3,
}


_COUPON_CONCEPTS = frozenset({"debtinstrumentinterestratestatedpercentage"})
_EFFECTIVE_CONCEPTS = frozenset(
    {
        "debtinstrumentinterestrateeffectivepercentage",
        "debtinstrumentinterestrateeffectivepercentagecash",
        "debtinstrumentinterestrateeffectivepercentagepaidinkind",
        "debtinstrumentinternalrateofreturn",
    }
)
_FLOATING_BASE_CONCEPTS = frozenset(
    {
        "debtinstrumentfloatingrate",
        "debtinstrumentinterestratebasisforeffectiverate",
        "debtinstrumentvariableinterestrate",
    }
)
_SPREAD_CONCEPTS = frozenset(
    {
        "debtinstrumentbasisspreadonvariablerate1",
        "debtinstrumentinterestratepremium",
        "lineofcreditfacilityinterestratespreadatperiodendwhengreaterthan50ofloancap",
        "lineofcreditfacilityinterestratespreadatperiodendwhenlessthan50ofloancap",
    }
)
_ALL_IN_CONCEPTS = frozenset(
    {
        "debtinstrumentinterestrateduringperiod",
        "debtweightedaverageinterestrate",
        "lineofcreditfacilityinterestrateduringperiod",
    }
)
_CONVERSION_CONCEPTS = frozenset({"debtconversionconvertedinstrumentrate"})
_OTHER_DEBT_PERCENTAGE_CONCEPTS = frozenset(
    {
        "debtinstrumentinterestrateincreasedecrease",
        "debtinstrumentinterestrateincreasedecreasequarterly",
        "longtermdebtpercentagebearingfixedinterestrate",
        "longtermdebtpercentagebearingvariableinterestrate",
    }
)


def _local_concept_name(concept: object) -> str:
    return str(concept or "").strip().lower().rsplit(":", 1)[-1]


def classify_debt_rate_concept(concept: object) -> DebtRateRole:
    """Map source concepts to roles without treating every percent as a coupon."""

    local_name = _local_concept_name(concept)
    if local_name in _COUPON_CONCEPTS:
        return DebtRateRole.COUPON_STATED_RATE
    if local_name in _EFFECTIVE_CONCEPTS:
        return DebtRateRole.EFFECTIVE_INTEREST_RATE
    if local_name in _FLOATING_BASE_CONCEPTS:
        return DebtRateRole.FLOATING_BASE_RATE
    if local_name in _SPREAD_CONCEPTS:
        return DebtRateRole.SPREAD_MARGIN
    if local_name in _ALL_IN_CONCEPTS:
        return DebtRateRole.ALL_IN_RATE
    if local_name in _CONVERSION_CONCEPTS:
        return DebtRateRole.CONVERSION_RELATED_RATE
    if local_name in _OTHER_DEBT_PERCENTAGE_CONCEPTS:
        return DebtRateRole.OTHER_PERCENTAGE
    return DebtRateRole.NOT_A_RATE


@dataclass(frozen=True)
class DebtRateFactCandidate:
    instrument_identity: str
    reporting_date: dt.date
    role: DebtRateRole
    value: float
    rendered_text: str
    raw_scalar: str
    concept: str
    unit_ref: str
    canonical_unit: str
    context_id: str
    fact_id: str
    basis_scope: str
    source_locator: str
    authority: DebtRateAuthority
    direct: bool
    effective_start: Optional[dt.date] = None
    effective_end: Optional[dt.date] = None

    def __post_init__(self) -> None:
        if not str(self.instrument_identity or "").strip():
            raise DebtRateOwnershipError("Debt-rate fact is missing instrument identity.")
        if not isinstance(self.reporting_date, dt.date):
            raise DebtRateOwnershipError("Debt-rate fact is missing reporting-period identity.")
        if self.role is DebtRateRole.NOT_A_RATE:
            raise DebtRateOwnershipError("A NOT_A_RATE source token cannot own a debt-rate fact.")
        if not str(self.fact_id or "").strip():
            raise DebtRateOwnershipError("Debt-rate fact is missing stable source identity.")
        if not str(self.canonical_unit or "").strip():
            raise DebtRateOwnershipError("Debt-rate fact is missing canonical unit identity.")

    @property
    def semantic_key(self) -> Tuple[str, dt.date, DebtRateRole, str]:
        return (
            str(self.instrument_identity).strip(),
            self.reporting_date,
            self.role,
            str(self.basis_scope or "").strip(),
        )


@dataclass(frozen=True)
class ResolvedDebtRateFact:
    selected: DebtRateFactCandidate
    corroborating_fact_ids: Tuple[str, ...] = ()

    @property
    def evidence_fact_ids(self) -> Tuple[str, ...]:
        return tuple(
            sorted(
                {
                    str(self.selected.fact_id).strip(),
                    *(str(value).strip() for value in self.corroborating_fact_ids),
                }
            )
        )

    def as_record(self) -> dict[str, object]:
        fact = self.selected
        return {
            "rate_ownership_contract_id": DEBT_RATE_OWNERSHIP_CONTRACT_ID,
            "instrument_identity": fact.instrument_identity,
            "reporting_date": fact.reporting_date.isoformat(),
            "rate_role": fact.role.value,
            "basis_scope": fact.basis_scope,
            "rate_value": fact.value,
            "rate_display": fact.rendered_text,
            "raw_scalar": fact.raw_scalar,
            "rate_fact_name": fact.concept,
            "rate_unit_ref": fact.unit_ref,
            "rate_canonical_unit": fact.canonical_unit,
            "rate_context_id": fact.context_id,
            "rate_fact_id": fact.fact_id,
            "rate_fact_ids": self.evidence_fact_ids,
            "source_locator": fact.source_locator,
            "rate_authority": fact.authority.value,
            "rate_direct": fact.direct,
            "effective_start": fact.effective_start.isoformat() if fact.effective_start else None,
            "effective_end": fact.effective_end.isoformat() if fact.effective_end else None,
        }


def _economic_value_signature(candidate: DebtRateFactCandidate) -> Tuple[Decimal, str]:
    return Decimal(str(candidate.value)), str(candidate.canonical_unit).strip().lower()


def resolve_debt_rate_facts(
    candidates: Iterable[DebtRateFactCandidate],
    *,
    requested_reporting_date: dt.date,
) -> Tuple[ResolvedDebtRateFact, ...]:
    """Resolve same-role facts deterministically; never borrow another period."""

    if not isinstance(requested_reporting_date, dt.date):
        raise DebtRateOwnershipError("A requested reporting date is required for debt-rate selection.")
    groups: dict[Tuple[str, dt.date, DebtRateRole, str], list[DebtRateFactCandidate]] = {}
    for candidate in candidates:
        if candidate.reporting_date != requested_reporting_date:
            continue
        groups.setdefault(candidate.semantic_key, []).append(candidate)

    resolved: list[ResolvedDebtRateFact] = []
    for semantic_key in sorted(
        groups,
        key=lambda value: (value[0], value[1].isoformat(), value[2].value, value[3]),
    ):
        facts = groups[semantic_key]
        strongest_rank = max(_AUTHORITY_RANK[fact.authority] for fact in facts)
        strongest = [fact for fact in facts if _AUTHORITY_RANK[fact.authority] == strongest_rank]
        value_signatures = {_economic_value_signature(fact) for fact in strongest}
        if len(value_signatures) != 1:
            identities = sorted(str(fact.fact_id) for fact in strongest)
            raise DebtRateConflictError(
                "Conflicting same-authority debt-rate facts for "
                f"{semantic_key!r}: {identities!r}."
            )
        ordered = sorted(
            strongest,
            key=lambda fact: (
                str(fact.fact_id),
                str(fact.context_id),
                str(fact.source_locator),
                str(fact.concept),
            ),
        )
        selected = ordered[0]
        resolved.append(
            ResolvedDebtRateFact(
                selected=selected,
                corroborating_fact_ids=tuple(fact.fact_id for fact in ordered[1:]),
            )
        )
    return tuple(resolved)


def select_debt_detail_rate(
    resolved_facts: Sequence[ResolvedDebtRateFact],
    *,
    preferred_basis_scope: str = "",
) -> Optional[ResolvedDebtRateFact]:
    """Select the one role consumed by the legacy single-rate Debt Detail field.

    Source products retain every role.  The existing visible field consumes a
    coupon/stated rate first and a spread/margin only when no coupon is present.
    """

    preferred_scope = str(preferred_basis_scope or "").strip()
    for role in (DebtRateRole.COUPON_STATED_RATE, DebtRateRole.SPREAD_MARGIN):
        matches = [fact for fact in resolved_facts if fact.selected.role is role]
        if preferred_scope:
            scoped = [fact for fact in matches if fact.selected.basis_scope == preferred_scope]
            if scoped:
                matches = scoped
        if not matches:
            continue
        if len(matches) != 1:
            identities = sorted(fact.selected.fact_id for fact in matches)
            raise DebtRateConflictError(
                f"Debt Detail has multiple incompatible {role.value} owners: {identities!r}."
            )
        return matches[0]
    return None


_DISPLAY_COUPON_PATTERN = re.compile(
    r"(?<![0-9.])(?P<rate>[0-9]+(?:\.[0-9]+)?)\s*%\s*"
    r"(?:(?:senior|subordinated|secured|unsecured|convertible)\s+)*"
    r"(?:notes?|debentures?|bonds?)\b",
    flags=re.IGNORECASE,
)


def display_coupon_rate(label: object) -> Optional[Tuple[float, str]]:
    """Return an explicit instrument-description coupon as a canonical ratio.

    Arbitrary percent tokens, maturity years, amounts, and parenthesized
    footnote numbers are intentionally outside this grammar.
    """

    text = " ".join(str(label or "").split())
    matches = list(_DISPLAY_COUPON_PATTERN.finditer(text))
    if not matches:
        return None
    values = {Decimal(match.group("rate")) for match in matches}
    if len(values) != 1:
        raise DebtRateConflictError(
            f"Instrument label contains conflicting stated-rate evidence: {text!r}."
        )
    source_percent = next(iter(values))
    return float(source_percent / Decimal("100")), f"{source_percent}%"


__all__ = [
    "DEBT_RATE_OWNERSHIP_CONTRACT_ID",
    "DebtRateAuthority",
    "DebtRateConflictError",
    "DebtRateFactCandidate",
    "DebtRateOwnershipError",
    "DebtRateRole",
    "ResolvedDebtRateFact",
    "classify_debt_rate_concept",
    "display_coupon_rate",
    "resolve_debt_rate_facts",
    "select_debt_detail_rate",
]
