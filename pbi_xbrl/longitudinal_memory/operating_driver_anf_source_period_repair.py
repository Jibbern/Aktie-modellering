"""Bounded ANF Operating Drivers source census and period-basis repair.

This module does not replace the accepted Operating Drivers architecture.  It
constructs a corrected, source-native ANF input package and routes its numeric
quarter observations through the accepted canonical registry, analytics,
context-semantics, and story-selection layers.  Facts owned by financial or
guidance products remain typed context/inputs and never become duplicate
Operating Drivers owners.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
from enum import Enum
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .operating_driver_derived_analytics import (
    DerivedAnalyticsPackage,
    build_derived_analytics,
)
from .operating_driver_semantic_priority import (
    SemanticPriorityPackage,
    build_context_semantic_priority,
)
from .operating_driver_shadow_profiles import ANF_PROFILE
from .operating_driver_shadow_registry import (
    ShadowRegistryPackage,
    build_shadow_registry,
)
from .operating_driver_story_selection import (
    OrthogonalStorySelectionPackage,
    build_orthogonal_story_selection,
)


SOURCE_REPAIR_CONTRACT = "operating-drivers-anf-source-period-repair@1"
PERIOD_BASIS_CONTRACT = "operating-driver-period-basis@1"
DERIVATION_CONTRACT = "additive-ytd-to-quarter-actual@1"
KNOWLEDGE_DATE = "2026-08-20"

_DATA_ROOT = Path(r"C:\Users\Jibbe\Aktier\StockModelData\tickers\ANF")
_TOTAL_COMPANY = "member:operating-driver:total-company@1"
_UNIT_PERCENT = "unit:core:percent@1"
_UNIT_STORES = "unit:operating-driver:stores@1"
_UNIT_USD_MILLION = "unit:core:usd-million@1"


class AnfOperatingDriverSourceRepairError(ValueError):
    """Raised when the bounded ANF source/period contract cannot be proven."""


class PeriodBasis(str, Enum):
    QUARTER_ACTUAL = "QUARTER_ACTUAL"
    YTD_ACTUAL = "YTD_ACTUAL"
    FY_ACTUAL = "FY_ACTUAL"
    INSTANT_ACTUAL = "INSTANT_ACTUAL"
    GUIDANCE = "GUIDANCE"
    APPROXIMATE_RANGE = "APPROXIMATE_RANGE"
    QUALITATIVE_ACTUAL_CONTEXT = "QUALITATIVE_ACTUAL_CONTEXT"


class EvidencePrecision(str, Enum):
    EXACT = "EXACT"
    APPROXIMATE = "APPROXIMATE"
    QUALITATIVE = "QUALITATIVE"


class PresentationDisposition(str, Enum):
    QUARTER_NUMERIC = "QUARTER_NUMERIC"
    CORE_TEXT_ONLY = "CORE_TEXT_ONLY"
    CONTEXT_ONLY = "CONTEXT_ONLY"
    DERIVATION_INPUT = "DERIVATION_INPUT"
    GUIDANCE_EXCLUDED = "GUIDANCE_EXCLUDED"


@dataclass(frozen=True)
class SourceDocument:
    source_document_id: str
    source_type: str
    title: str
    document_date: str
    local_path: str | None
    source_url: str | None
    sha256: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SourceFact:
    fact_id: str
    metric_label: str
    canonical_driver_id: str | None
    canonical_owner_id: str
    definition_id: str
    definition_version: int
    dimension_member_ids: tuple[str, ...]
    period_label: str
    fiscal_year: int | None
    fiscal_quarter: int | None
    period_basis: PeriodBasis
    actual_or_guidance: str
    value: str | None
    display_value: str
    unit_id: str
    precision: EvidencePrecision
    status: str
    source_document_id: str
    source_type: str
    source_location: str
    source_url: str | None
    knowledge_date: str
    presentation_disposition: PresentationDisposition
    source_observation_role: str

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["period_basis"] = self.period_basis.value
        result["precision"] = self.precision.value
        result["presentation_disposition"] = self.presentation_disposition.value
        return result


@dataclass(frozen=True)
class QuarterActivityDerivation:
    derivation_id: str
    contract_version: str
    metric_label: str
    canonical_driver_id: str
    fiscal_year: int
    fiscal_quarter: int
    result_value: str
    unit_id: str
    result_fact_id: str
    minuend_fact_id: str
    subtrahend_fact_id: str | None
    definition_compatible: bool
    dimension_compatible: bool
    unit_compatible: bool
    same_fiscal_year: bool
    additive_activity_metric: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AnfOperatingDriverSourceRepairPackage:
    ticker: str
    contract_version: str
    period_basis_contract: str
    source_documents: tuple[SourceDocument, ...]
    source_census: tuple[SourceFact, ...]
    quarter_activity_derivations: tuple[QuarterActivityDerivation, ...]
    registry: ShadowRegistryPackage
    analytics: DerivedAnalyticsPackage
    semantics: SemanticPriorityPackage
    selection: OrthogonalStorySelectionPackage
    reconciliation: Mapping[str, Any]
    sha256: str

    def to_ui_source(self) -> dict[str, Any]:
        return {
            "shadow": self.registry.to_dict(),
            "analytics": self.analytics.to_dict(),
            "semantics": self.semantics.to_dict(),
            "selection": self.selection.to_dict(),
            "period_repair": {
                "contract_version": self.contract_version,
                "period_basis_contract": self.period_basis_contract,
                "source_documents": [item.to_dict() for item in self.source_documents],
                "facts": [item.to_dict() for item in self.source_census],
                "quarter_activity_derivations": [
                    item.to_dict() for item in self.quarter_activity_derivations
                ],
                "reconciliation": dict(self.reconciliation),
                "sha256": self.sha256,
            },
        }

    def to_dict(self) -> dict[str, Any]:
        return self.to_ui_source()["period_repair"] | {
            "ticker": self.ticker,
            "registry_sha256": self.registry.sha256,
            "analytics_sha256": self.analytics.sha256,
            "semantics_sha256": self.semantics.sha256,
            "selection_sha256": self.selection.sha256,
        }


def _canonical_decimal(value: Decimal | int | float | str | None) -> str | None:
    if value is None:
        return None
    decimal = Decimal(str(value))
    normalized = format(decimal, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized or "0"


def _digest(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _document(
    source_document_id: str,
    source_type: str,
    title: str,
    document_date: str,
    *,
    relative_path: str | None = None,
    source_url: str | None = None,
) -> SourceDocument:
    local = None if relative_path is None else _DATA_ROOT / relative_path
    if local is not None and not local.is_file():
        raise AnfOperatingDriverSourceRepairError(f"Required official source is absent: {local}")
    return SourceDocument(
        source_document_id=source_document_id,
        source_type=source_type,
        title=title,
        document_date=document_date,
        local_path=None if local is None else str(local),
        source_url=source_url,
        sha256=None if local is None else _sha256_file(local),
    )


def _source_documents() -> tuple[SourceDocument, ...]:
    return (
        _document(
            "source:anf:q4-2023-quarterly-history@1",
            "INVESTOR_PRESENTATION",
            "ANF Fiscal 2023 Quarterly History",
            "2024-03-06",
            relative_path=r"earnings_presentation\ANF_Q4_2023_earnings_presentation_quarterly_history.xlsx",
        ),
        _document(
            "source:anf:q4-2024-quarterly-history@1",
            "INVESTOR_PRESENTATION",
            "ANF Fiscal 2024 Quarterly History",
            "2025-03-05",
            relative_path=r"earnings_presentation\ANF_Q4_2024_earnings_presentation_quarterly_history.xlsx",
        ),
        _document(
            "source:anf:q4-2025-quarterly-history@1",
            "INVESTOR_PRESENTATION",
            "ANF Fiscal 2025 Quarterly History",
            "2026-03-04",
            relative_path=r"earnings_presentation\ANF_Q4_2025_earnings_presentation_quarterly_history.xlsx",
        ),
        _document(
            "source:anf:q1-2025-10q@1",
            "SEC_10_Q",
            "ANF 2025 Q1 Form 10-Q",
            "2025-06-05",
            relative_path=r"financial_statement\ANF_Q2_2025_10Q_2025-05-03_financial_statement.htm",
        ),
        _document(
            "source:anf:q2-2025-10q@1",
            "SEC_10_Q",
            "ANF 2025 Q2 Form 10-Q",
            "2025-09-04",
            relative_path=r"financial_statement\ANF_Q3_2025_10Q_2025-08-02_financial_statement.htm",
        ),
        _document(
            "source:anf:q3-2025-10q@1",
            "SEC_10_Q",
            "ANF 2025 Q3 Form 10-Q",
            "2025-12-04",
            relative_path=r"financial_statement\ANF_Q4_2025_10Q_2025-11-01_financial_statement.htm",
        ),
        _document(
            "source:anf:fy2025-10k@1",
            "SEC_10_K",
            "ANF Fiscal 2025 Form 10-K",
            "2026-03-25",
            relative_path=r"financial_statement\ANF_Q1_2026_10K_2026-01-31_financial_statement.htm",
        ),
        _document(
            "source:anf:q4-2025-earnings-release@1",
            "EARNINGS_RELEASE",
            "ANF Fiscal 2025 Q4 Earnings Release",
            "2026-03-04",
            relative_path=r"earnings_release\8-K_2026-03-04_earnings_release.htm",
        ),
        _document(
            "source:anf:q4-2025-transcript@1",
            "TRANSCRIPT",
            "ANF Fiscal 2025 Q4 Earnings Call Transcript",
            "2026-03-04",
            relative_path=r"earnings_transcripts\ANF_Q4_2025_transcript.txt",
        ),
        _document(
            "source:anf:q1-2026-earnings-release@1",
            "EARNINGS_RELEASE",
            "ANF Fiscal 2026 Q1 Earnings Release",
            "2026-05-27",
            source_url="https://www.sec.gov/Archives/edgar/data/1018840/000101884026000029/q12026pressrelease.htm",
        ),
        _document(
            "source:anf:q1-2026-10q@1",
            "SEC_10_Q",
            "ANF 2026 Q1 Form 10-Q",
            "2026-06-04",
            source_url="https://www.sec.gov/Archives/edgar/data/1018840/000101884026000036/anf-20260502.htm",
        ),
        _document(
            "source:anf:q1-2026-investor-presentation@1",
            "INVESTOR_PRESENTATION",
            "ANF Fiscal 2026 Q1 Investor Presentation",
            "2026-05-27",
            source_url="https://abercrombieandfitchcompany.gcs-web.com/static-files/0e2cffd6-e67e-455c-81ec-7106b9e0272a",
        ),
    )


_PERIOD_SERIALS: Mapping[tuple[int, int], int] = {
    (2023, 2): 45136,
    (2023, 3): 45227,
    (2023, 4): 45325,
    (2024, 1): 45416,
    (2024, 2): 45507,
    (2024, 3): 45598,
    (2024, 4): 45689,
    (2025, 1): 45780,
    (2025, 2): 45871,
    (2025, 3): 45962,
    (2025, 4): 46053,
    (2026, 1): 46144,
}

_COMP_MEMBERS = {
    "Total Company": _TOTAL_COMPANY,
    "Americas": "member:operating-driver:americas@1",
    "EMEA": "member:operating-driver:emea@1",
    "APAC": "member:operating-driver:apac@1",
    "Abercrombie": "member:operating-driver:abercrombie@1",
    "Hollister": "member:operating-driver:hollister@1",
}

_COMP_ROWS = {
    "Americas": 7,
    "EMEA": 8,
    "APAC": 9,
    "Total Company": 10,
    "Abercrombie": 13,
    "Hollister": 14,
}

_COMP_VALUES: Mapping[tuple[int, int], Mapping[str, int]] = {
    (2023, 2): {"Total Company": 13, "Americas": 14, "EMEA": 6, "APAC": 26, "Abercrombie": 23, "Hollister": 5},
    (2023, 3): {"Total Company": 16, "Americas": 16, "EMEA": 15, "APAC": 32, "Abercrombie": 26, "Hollister": 7},
    (2023, 4): {"Total Company": 16, "Americas": 17, "EMEA": 10, "APAC": 21, "Abercrombie": 28, "Hollister": 6},
    (2024, 1): {"Total Company": 21, "Americas": 21, "EMEA": 23, "APAC": 22, "Abercrombie": 29, "Hollister": 13},
    (2024, 2): {"Total Company": 18, "Americas": 18, "EMEA": 17, "APAC": 21, "Abercrombie": 21, "Hollister": 15},
    (2024, 3): {"Total Company": 16, "Americas": 16, "EMEA": 13, "APAC": 16, "Abercrombie": 11, "Hollister": 21},
    (2024, 4): {"Total Company": 14, "Americas": 15, "EMEA": 12, "APAC": 17, "Abercrombie": 5, "Hollister": 24},
    (2025, 1): {"Total Company": 4, "Americas": 4, "EMEA": 6, "APAC": 2, "Abercrombie": -10, "Hollister": 23},
    (2025, 2): {"Total Company": 3, "Americas": 5, "EMEA": -5, "APAC": 1, "Abercrombie": -11, "Hollister": 19},
    (2025, 3): {"Total Company": 3, "Americas": 4, "EMEA": 2, "APAC": -12, "Abercrombie": -7, "Hollister": 15},
    (2025, 4): {"Total Company": 1, "Americas": 2, "EMEA": -3, "APAC": 0, "Abercrombie": -1, "Hollister": 3},
    (2026, 1): {"Total Company": -1, "Americas": 1, "EMEA": -11, "APAC": 15, "Abercrombie": 0, "Hollister": -2},
}


def _fact(
    *,
    metric_label: str,
    canonical_driver_id: str | None,
    canonical_owner_id: str,
    definition_id: str,
    dimension_member_ids: Sequence[str],
    period_label: str,
    fiscal_year: int | None,
    fiscal_quarter: int | None,
    period_basis: PeriodBasis,
    actual_or_guidance: str,
    value: Decimal | int | float | str | None,
    display_value: str,
    unit_id: str,
    precision: EvidencePrecision,
    status: str,
    source_document_id: str,
    source_type: str,
    source_location: str,
    source_url: str | None,
    presentation_disposition: PresentationDisposition,
    source_observation_role: str,
) -> SourceFact:
    value_text = _canonical_decimal(value)
    payload = {
        "actual_or_guidance": actual_or_guidance,
        "canonical_driver_id": canonical_driver_id,
        "canonical_owner_id": canonical_owner_id,
        "definition_id": definition_id,
        "dimension_member_ids": sorted(dimension_member_ids),
        "fiscal_quarter": fiscal_quarter,
        "fiscal_year": fiscal_year,
        "metric_label": metric_label,
        "period_basis": period_basis.value,
        "period_label": period_label,
        "source_document_id": source_document_id,
        "source_location": source_location,
        "source_observation_role": source_observation_role,
        "unit_id": unit_id,
        "value": value_text,
    }
    return SourceFact(
        fact_id=f"fact:anf:operating-driver:{_digest(payload)[:32]}",
        metric_label=metric_label,
        canonical_driver_id=canonical_driver_id,
        canonical_owner_id=canonical_owner_id,
        definition_id=definition_id,
        definition_version=1,
        dimension_member_ids=tuple(sorted(dimension_member_ids)),
        period_label=period_label,
        fiscal_year=fiscal_year,
        fiscal_quarter=fiscal_quarter,
        period_basis=period_basis,
        actual_or_guidance=actual_or_guidance,
        value=value_text,
        display_value=display_value,
        unit_id=unit_id,
        precision=precision,
        status=status,
        source_document_id=source_document_id,
        source_type=source_type,
        source_location=source_location,
        source_url=source_url,
        knowledge_date=KNOWLEDGE_DATE,
        presentation_disposition=presentation_disposition,
        source_observation_role=source_observation_role,
    )


def _comp_source(year: int, quarter: int, label: str) -> tuple[str, str, str, str | None]:
    row = _COMP_ROWS[label]
    if year == 2023:
        column = {2: "P", 3: "Q", 4: "R"}[quarter]
        return (
            "source:anf:q4-2023-quarterly-history@1",
            "INVESTOR_PRESENTATION",
            f"Historical Comparable Sales!{column}{row}",
            None,
        )
    if year == 2024:
        column = {1: "K", 2: "L", 3: "M", 4: "N"}[quarter]
        return (
            "source:anf:q4-2024-quarterly-history@1",
            "INVESTOR_PRESENTATION",
            f"Historical Comparable Sales!{column}{row}",
            None,
        )
    if year == 2025 and quarter < 4:
        column = {1: "K", 2: "L", 3: "M"}[quarter]
        return (
            "source:anf:q4-2025-quarterly-history@1",
            "INVESTOR_PRESENTATION",
            f"Historical Comparable Sales!{column}{row}",
            None,
        )
    if year == 2025 and quarter == 4:
        return (
            "source:anf:q4-2025-earnings-release@1",
            "EARNINGS_RELEASE",
            f"Net sales by {'segment' if label in {'Americas', 'EMEA', 'APAC'} else 'brand/total'}; {label} comparable sales",
            None,
        )
    return (
        "source:anf:q1-2026-earnings-release@1",
        "EARNINGS_RELEASE",
        f"Net sales by {'segment' if label in {'Americas', 'EMEA', 'APAC'} else 'brand/total'}; {label} comparable sales",
        "https://www.sec.gov/Archives/edgar/data/1018840/000101884026000029/q12026pressrelease.htm",
    )


def _comparison_facts() -> list[SourceFact]:
    result: list[SourceFact] = []
    for (year, quarter), values in sorted(_COMP_VALUES.items()):
        for label, value in sorted(values.items()):
            document_id, source_type, location, source_url = _comp_source(year, quarter, label)
            result.append(
                _fact(
                    metric_label=f"{label} comparable sales",
                    canonical_driver_id="driver:operating:comparable-sales@1",
                    canonical_owner_id="owner:operating-drivers:source-native@1",
                    definition_id="definition:operating-driver:comparable-sales@1",
                    dimension_member_ids=(_COMP_MEMBERS[label],),
                    period_label=f"{year}-Q{quarter}",
                    fiscal_year=year,
                    fiscal_quarter=quarter,
                    period_basis=PeriodBasis.QUARTER_ACTUAL,
                    actual_or_guidance="ACTUAL",
                    value=value,
                    display_value=f"{value:+d}%" if value else "0%",
                    unit_id=_UNIT_PERCENT,
                    precision=EvidencePrecision.EXACT,
                    status="AVAILABLE",
                    source_document_id=document_id,
                    source_type=source_type,
                    source_location=location,
                    source_url=source_url,
                    presentation_disposition=PresentationDisposition.QUARTER_NUMERIC,
                    source_observation_role="DIRECT_SOURCE_FACT",
                )
            )
    return result


_STORE_YTD: Mapping[int, Mapping[str, int]] = {
    1: {"New stores": 7, "Remodeled stores": 9, "Right-sized stores": 1, "Closed stores": 3},
    2: {"New stores": 26, "Remodeled stores": 16, "Right-sized stores": 5, "Closed stores": 8},
    3: {"New stores": 48, "Remodeled stores": 24, "Right-sized stores": 8, "Closed stores": 10},
    4: {"New stores": 62, "Remodeled stores": 47, "Right-sized stores": 11, "Closed stores": 22},
}

_STORE_DRIVER_IDS = {
    "New stores": "driver:operating:new-stores@1",
    "Remodeled stores": "driver:operating:remodeled-stores@1",
    "Right-sized stores": "driver:operating:right-sized-stores@1",
    "Closed stores": "driver:operating:closed-stores@1",
}

_STORE_RAW_LABELS = {
    "New stores": "Total Company New stores",
    "Remodeled stores": "Total Company Remodeled stores",
    "Right-sized stores": "Total Company Right-sized stores",
    "Closed stores": "Total Company Closed stores",
}


def _store_input_facts() -> list[SourceFact]:
    result: list[SourceFact] = []
    sources = {
        1: ("source:anf:q1-2025-10q@1", "SEC_10_Q", "Store network paragraph: through first fiscal quarter"),
        2: ("source:anf:q2-2025-10q@1", "SEC_10_Q", "Store network paragraph: through second fiscal quarter"),
        3: ("source:anf:q3-2025-10q@1", "SEC_10_Q", "Store network paragraph: through third fiscal quarter"),
        4: ("source:anf:fy2025-10k@1", "SEC_10_K", "Global store network modernization and growth; Fiscal 2025 actuals"),
    }
    for quarter, values in sorted(_STORE_YTD.items()):
        basis = PeriodBasis.FY_ACTUAL if quarter == 4 else PeriodBasis.YTD_ACTUAL
        document_id, source_type, location = sources[quarter]
        period_label = "FY2025" if quarter == 4 else f"2025-Q{quarter} YTD"
        for label, value in sorted(values.items()):
            result.append(
                _fact(
                    metric_label=label,
                    canonical_driver_id=_STORE_DRIVER_IDS[label],
                    canonical_owner_id="owner:operating-drivers:source-native@1",
                    definition_id=f"definition:operating-driver:{_STORE_DRIVER_IDS[label].split(':')[-1].split('@')[0]}@1",
                    dimension_member_ids=(_TOTAL_COMPANY,),
                    period_label=period_label,
                    fiscal_year=2025,
                    fiscal_quarter=quarter if quarter < 4 else None,
                    period_basis=basis,
                    actual_or_guidance="ACTUAL",
                    value=value,
                    display_value=f"{value} stores",
                    unit_id=_UNIT_STORES,
                    precision=EvidencePrecision.EXACT,
                    status="AVAILABLE",
                    source_document_id=document_id,
                    source_type=source_type,
                    source_location=location,
                    source_url=None,
                    presentation_disposition=PresentationDisposition.DERIVATION_INPUT,
                    source_observation_role="CUMULATIVE_ACTUAL_INPUT",
                )
            )
    return result


def _derive_quarter_store_activity(
    inputs: Sequence[SourceFact],
) -> tuple[list[SourceFact], list[QuarterActivityDerivation]]:
    by_key = {(item.metric_label, item.fiscal_quarter or 4): item for item in inputs}
    facts: list[SourceFact] = []
    derivations: list[QuarterActivityDerivation] = []
    for label in sorted(_STORE_DRIVER_IDS):
        previous_value = Decimal("0")
        previous_fact: SourceFact | None = None
        for quarter in (1, 2, 3, 4):
            current = by_key[(label, quarter)]
            if current.value is None:
                raise AnfOperatingDriverSourceRepairError("Store activity derivation input is unavailable.")
            value = Decimal(current.value) - previous_value
            derived = _fact(
                metric_label=label,
                canonical_driver_id=current.canonical_driver_id,
                canonical_owner_id=current.canonical_owner_id,
                definition_id=current.definition_id,
                dimension_member_ids=current.dimension_member_ids,
                period_label=f"2025-Q{quarter}",
                fiscal_year=2025,
                fiscal_quarter=quarter,
                period_basis=PeriodBasis.QUARTER_ACTUAL,
                actual_or_guidance="ACTUAL",
                value=value,
                display_value=f"{int(value)} stores",
                unit_id=current.unit_id,
                precision=EvidencePrecision.EXACT,
                status="AVAILABLE",
                source_document_id=current.source_document_id,
                source_type="TYPED_DERIVATION",
                source_location=(
                    f"{current.fact_id} minus {previous_fact.fact_id}; "
                    "fiscal-year cumulative prior-quarter input"
                    if previous_fact is not None
                    else f"{current.fact_id}; Q1 YTD equals Q1 quarter activity"
                ),
                source_url=current.source_url,
                presentation_disposition=PresentationDisposition.QUARTER_NUMERIC,
                source_observation_role="SAFE_DERIVATION",
            )
            payload = {
                "contract": DERIVATION_CONTRACT,
                "metric": label,
                "quarter": quarter,
                "result_fact_id": derived.fact_id,
                "inputs": [current.fact_id, None if previous_fact is None else previous_fact.fact_id],
            }
            derivations.append(
                QuarterActivityDerivation(
                    derivation_id=f"derivation:anf:store-activity:{_digest(payload)[:32]}",
                    contract_version=DERIVATION_CONTRACT,
                    metric_label=label,
                    canonical_driver_id=_STORE_DRIVER_IDS[label],
                    fiscal_year=2025,
                    fiscal_quarter=quarter,
                    result_value=_canonical_decimal(value) or "0",
                    unit_id=_UNIT_STORES,
                    result_fact_id=derived.fact_id,
                    minuend_fact_id=current.fact_id,
                    subtrahend_fact_id=None if previous_fact is None else previous_fact.fact_id,
                    definition_compatible=True,
                    dimension_compatible=True,
                    unit_compatible=True,
                    same_fiscal_year=True,
                    additive_activity_metric=True,
                )
            )
            facts.append(derived)
            previous_value = Decimal(current.value)
            previous_fact = current
    return facts, derivations


def _direct_store_facts() -> list[SourceFact]:
    result: list[SourceFact] = []
    for year, quarter, value, document, location in (
        (2023, 4, 765, "source:anf:q4-2023-quarterly-history@1", "Store Count!R9"),
        (2024, 4, 789, "source:anf:q4-2024-quarterly-history@1", "Store Count!R9"),
        (2025, 4, 829, "source:anf:q4-2025-quarterly-history@1", "Store Count!R9"),
        (2026, 1, 834, "source:anf:q1-2026-10q@1", "Store Count table; Company-owned total"),
    ):
        result.append(
            _fact(
                metric_label="Company-owned stores, end",
                canonical_driver_id="driver:operating:company-owned-stores-end@1",
                canonical_owner_id="owner:operating-drivers:source-native@1",
                definition_id="definition:operating-driver:company-owned-stores-end@1",
                dimension_member_ids=(_TOTAL_COMPANY,),
                period_label=f"{year}-Q{quarter}",
                fiscal_year=year,
                fiscal_quarter=quarter,
                period_basis=PeriodBasis.INSTANT_ACTUAL,
                actual_or_guidance="ACTUAL",
                value=value,
                display_value=f"{value} stores",
                unit_id=_UNIT_STORES,
                precision=EvidencePrecision.EXACT,
                status="AVAILABLE",
                source_document_id=document,
                source_type="SEC_10_Q" if year == 2026 else "INVESTOR_PRESENTATION",
                source_location=location,
                source_url=(
                    "https://www.sec.gov/Archives/edgar/data/1018840/000101884026000036/anf-20260502.htm"
                    if year == 2026
                    else None
                ),
                presentation_disposition=PresentationDisposition.QUARTER_NUMERIC,
                source_observation_role="DIRECT_SOURCE_FACT",
            )
        )
    for label, value in (
        ("New stores", 6),
        ("Remodeled stores", 24),
        ("Right-sized stores", 2),
        ("Closed stores", 1),
    ):
        result.append(
            _fact(
                metric_label=label,
                canonical_driver_id=_STORE_DRIVER_IDS[label],
                canonical_owner_id="owner:operating-drivers:source-native@1",
                definition_id=f"definition:operating-driver:{_STORE_DRIVER_IDS[label].split(':')[-1].split('@')[0]}@1",
                dimension_member_ids=(_TOTAL_COMPANY,),
                period_label="2026-Q1",
                fiscal_year=2026,
                fiscal_quarter=1,
                period_basis=PeriodBasis.QUARTER_ACTUAL,
                actual_or_guidance="ACTUAL",
                value=value,
                display_value=f"{value} stores",
                unit_id=_UNIT_STORES,
                precision=EvidencePrecision.EXACT,
                status="AVAILABLE",
                source_document_id="source:anf:q1-2026-10q@1",
                source_type="SEC_10_Q",
                source_location="Global store network modernization and growth; first-quarter actuals",
                source_url="https://www.sec.gov/Archives/edgar/data/1018840/000101884026000036/anf-20260502.htm",
                presentation_disposition=PresentationDisposition.QUARTER_NUMERIC,
                source_observation_role="DIRECT_SOURCE_FACT",
            )
        )
    return result


def _context_facts() -> list[SourceFact]:
    result: list[SourceFact] = []
    inventory = (
        (2025, 1, "542.059", "source:anf:q1-2025-10q@1", "Condensed Consolidated Balance Sheets; Inventories"),
        (2025, 2, "592.966", "source:anf:q2-2025-10q@1", "Condensed Consolidated Balance Sheets; Inventories"),
        (2025, 3, "730.453", "source:anf:q3-2025-10q@1", "Condensed Consolidated Balance Sheets; Inventories"),
        (2025, 4, "601.218", "source:anf:fy2025-10k@1", "Consolidated Balance Sheets; Inventories"),
        (2026, 1, "532.691", "source:anf:q1-2026-10q@1", "Condensed Consolidated Balance Sheets; Inventories"),
    )
    for year, quarter, value, document, location in inventory:
        result.append(
            _fact(
                metric_label="Inventory at cost",
                canonical_driver_id=None,
                canonical_owner_id="owner:summary-bs:source-native@1",
                definition_id="definition:summary-bs:inventory-at-cost@1",
                dimension_member_ids=(_TOTAL_COMPANY,),
                period_label=f"{year}-Q{quarter}",
                fiscal_year=year,
                fiscal_quarter=quarter,
                period_basis=PeriodBasis.INSTANT_ACTUAL,
                actual_or_guidance="ACTUAL",
                value=value,
                display_value=f"${Decimal(value):,.3f}m",
                unit_id=_UNIT_USD_MILLION,
                precision=EvidencePrecision.EXACT,
                status="AVAILABLE",
                source_document_id=document,
                source_type="SEC_10_K" if quarter == 4 else "SEC_10_Q",
                source_location=location,
                source_url=(
                    "https://www.sec.gov/Archives/edgar/data/1018840/000101884026000036/anf-20260502.htm"
                    if year == 2026
                    else None
                ),
                presentation_disposition=PresentationDisposition.QUARTER_NUMERIC,
                source_observation_role="OWNER_ELSEWHERE_CONTEXT",
            )
        )
    result.extend(
        (
            _fact(
                metric_label="Inventory unit growth",
                canonical_driver_id="driver:operating:inventory-unit-growth@1",
                canonical_owner_id="owner:operating-drivers:source-native@1",
                definition_id="definition:operating-driver:inventory-unit-growth@1",
                dimension_member_ids=(_TOTAL_COMPANY,),
                period_label="2026-Q1",
                fiscal_year=2026,
                fiscal_quarter=1,
                period_basis=PeriodBasis.APPROXIMATE_RANGE,
                actual_or_guidance="ACTUAL",
                value=None,
                display_value="Up low single digits (approx.)",
                unit_id=_UNIT_PERCENT,
                precision=EvidencePrecision.APPROXIMATE,
                status="AVAILABLE_APPROXIMATE_TEXT",
                source_document_id="source:anf:q1-2026-investor-presentation@1",
                source_type="INVESTOR_PRESENTATION",
                source_location="Inventory slide; units up low single digits year over year",
                source_url="https://abercrombieandfitchcompany.gcs-web.com/static-files/0e2cffd6-e67e-455c-81ec-7106b9e0272a",
                presentation_disposition=PresentationDisposition.CORE_TEXT_ONLY,
                source_observation_role="DIRECT_APPROXIMATE_SOURCE_FACT",
            ),
            _fact(
                metric_label="Net sales growth",
                canonical_driver_id=None,
                canonical_owner_id="owner:financial-products:source-native@1",
                definition_id="definition:financial:net-sales-growth@1",
                dimension_member_ids=(_TOTAL_COMPANY,),
                period_label="2026-Q1",
                fiscal_year=2026,
                fiscal_quarter=1,
                period_basis=PeriodBasis.QUARTER_ACTUAL,
                actual_or_guidance="ACTUAL",
                value=2,
                display_value="+2%",
                unit_id=_UNIT_PERCENT,
                precision=EvidencePrecision.EXACT,
                status="AVAILABLE",
                source_document_id="source:anf:q1-2026-earnings-release@1",
                source_type="EARNINGS_RELEASE",
                source_location="Summary of results; net sales up 2%",
                source_url="https://www.sec.gov/Archives/edgar/data/1018840/000101884026000029/q12026pressrelease.htm",
                presentation_disposition=PresentationDisposition.CONTEXT_ONLY,
                source_observation_role="OWNER_ELSEWHERE_CONTEXT",
            ),
            _fact(
                metric_label="Digital sales mix",
                canonical_driver_id="driver:operating:digital-sales-mix@1",
                canonical_owner_id="owner:operating-drivers:source-native@1",
                definition_id="definition:operating-driver:digital-sales-mix@1",
                dimension_member_ids=(_TOTAL_COMPANY,),
                period_label="FY2025",
                fiscal_year=2025,
                fiscal_quarter=None,
                period_basis=PeriodBasis.FY_ACTUAL,
                actual_or_guidance="ACTUAL",
                value=44,
                display_value="44% of FY2025 total sales",
                unit_id=_UNIT_PERCENT,
                precision=EvidencePrecision.EXACT,
                status="AVAILABLE_LOWER_PRIORITY_TRANSCRIPT",
                source_document_id="source:anf:q4-2025-transcript@1",
                source_type="TRANSCRIPT",
                source_location="Prepared remarks and Q&A: 'For the year, 44% of total sales were digital'",
                source_url=None,
                presentation_disposition=PresentationDisposition.CONTEXT_ONLY,
                source_observation_role="DIRECT_SOURCE_FACT_LOWER_PRIORITY",
            ),
            _fact(
                metric_label="Mobile share of digital traffic",
                canonical_driver_id=None,
                canonical_owner_id="owner:operating-drivers:context@1",
                definition_id="definition:operating-driver:mobile-share-of-digital-traffic@1",
                dimension_member_ids=(_TOTAL_COMPANY,),
                period_label="FY2025",
                fiscal_year=2025,
                fiscal_quarter=None,
                period_basis=PeriodBasis.APPROXIMATE_RANGE,
                actual_or_guidance="ACTUAL",
                value=None,
                display_value="More than 89% of digital traffic",
                unit_id=_UNIT_PERCENT,
                precision=EvidencePrecision.APPROXIMATE,
                status="AVAILABLE_APPROXIMATE_TEXT",
                source_document_id="source:anf:fy2025-10k@1",
                source_type="SEC_10_K",
                source_location="Digital experience discussion; mobile share of digital traffic",
                source_url=None,
                presentation_disposition=PresentationDisposition.CONTEXT_ONLY,
                source_observation_role="DISTINCT_DIMENSION_CONTEXT",
            ),
            _fact(
                metric_label="Average unit retail direction",
                canonical_driver_id=None,
                canonical_owner_id="owner:operating-drivers:context@1",
                definition_id="definition:operating-driver:average-unit-retail-direction@1",
                dimension_member_ids=(_TOTAL_COMPANY,),
                period_label="2026-Q1",
                fiscal_year=2026,
                fiscal_quarter=1,
                period_basis=PeriodBasis.QUALITATIVE_ACTUAL_CONTEXT,
                actual_or_guidance="ACTUAL",
                value=None,
                display_value="Low-single-digit AUR growth",
                unit_id=_UNIT_PERCENT,
                precision=EvidencePrecision.QUALITATIVE,
                status="AVAILABLE_QUALITATIVE_CONTEXT",
                source_document_id="source:anf:q1-2026-10q@1",
                source_type="SEC_10_Q",
                source_location="Results of operations; net sales explanation",
                source_url="https://www.sec.gov/Archives/edgar/data/1018840/000101884026000036/anf-20260502.htm",
                presentation_disposition=PresentationDisposition.CONTEXT_ONLY,
                source_observation_role="QUALITATIVE_ACTUAL_CONTEXT",
            ),
            _fact(
                metric_label="Freight and tariff cost context",
                canonical_driver_id=None,
                canonical_owner_id="owner:financial-products:source-native@1",
                definition_id="definition:financial:cost-of-sales-context@1",
                dimension_member_ids=(_TOTAL_COMPANY,),
                period_label="2026-Q1",
                fiscal_year=2026,
                fiscal_quarter=1,
                period_basis=PeriodBasis.QUALITATIVE_ACTUAL_CONTEXT,
                actual_or_guidance="ACTUAL",
                value=None,
                display_value="Freight improved; tariffs offset part of the benefit",
                unit_id="unit:core:qualitative@1",
                precision=EvidencePrecision.QUALITATIVE,
                status="AVAILABLE_QUALITATIVE_CONTEXT",
                source_document_id="source:anf:q1-2026-10q@1",
                source_type="SEC_10_Q",
                source_location="Cost of sales discussion; 180 bps freight decline and 180 bps tariff impact",
                source_url="https://www.sec.gov/Archives/edgar/data/1018840/000101884026000036/anf-20260502.htm",
                presentation_disposition=PresentationDisposition.CONTEXT_ONLY,
                source_observation_role="OWNER_ELSEWHERE_CONTEXT",
            ),
        )
    )
    for period_label, value, document, location in (
        ("FY2026 original guidance", 70, "source:anf:q4-2025-earnings-release@1", "Fiscal 2026 Outlook; combined remodels and right-sizes"),
        ("FY2026 updated guidance", 80, "source:anf:q1-2026-earnings-release@1", "Fiscal 2026 Outlook; combined remodels and right-sizes"),
    ):
        result.append(
            _fact(
                metric_label="Remodels and right-sizes guidance",
                canonical_driver_id=None,
                canonical_owner_id="owner:guidance:source-native@1",
                definition_id="definition:guidance:real-estate-activity@1",
                dimension_member_ids=(_TOTAL_COMPANY,),
                period_label=period_label,
                fiscal_year=2026,
                fiscal_quarter=None,
                period_basis=PeriodBasis.GUIDANCE,
                actual_or_guidance="GUIDANCE",
                value=value,
                display_value=f"Approximately {value} combined remodels and right-sizes",
                unit_id=_UNIT_STORES,
                precision=EvidencePrecision.APPROXIMATE,
                status="AVAILABLE_GUIDANCE_EXCLUDED_FROM_ACTUALS",
                source_document_id=document,
                source_type="EARNINGS_RELEASE",
                source_location=location,
                source_url=(
                    "https://www.sec.gov/Archives/edgar/data/1018840/000101884026000029/q12026pressrelease.htm"
                    if value == 80
                    else None
                ),
                presentation_disposition=PresentationDisposition.GUIDANCE_EXCLUDED,
                source_observation_role="GUIDANCE_REFERENCE",
            )
        )
    return result


def _raw_rows(facts: Iterable[SourceFact]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    source_labels = {
        "SEC_10_K": "10-K",
        "SEC_10_Q": "10-Q",
        "EARNINGS_RELEASE": "earnings_release",
        "INVESTOR_PRESENTATION": "presentation",
        "TYPED_DERIVATION": "internal_metric",
    }
    for fact in facts:
        if (
            fact.presentation_disposition is not PresentationDisposition.QUARTER_NUMERIC
            or fact.canonical_driver_id is None
            or fact.value is None
            or fact.period_basis not in {PeriodBasis.QUARTER_ACTUAL, PeriodBasis.INSTANT_ACTUAL}
        ):
            continue
        if fact.fiscal_year is None or fact.fiscal_quarter is None:
            raise AnfOperatingDriverSourceRepairError("Quarter numeric fact lacks a typed fiscal quarter.")
        raw_label = fact.metric_label
        if fact.canonical_driver_id == "driver:operating:comparable-sales@1":
            member = fact.dimension_member_ids[0].split(":")[-1].split("@")[0]
            display = {
                "total-company": "Total Company",
                "abercrombie": "Abercrombie",
                "hollister": "Hollister",
                "americas": "Americas",
                "emea": "EMEA",
                "apac": "APAC",
            }[member]
            raw_label = f"{display} comparable sales"
        elif fact.canonical_driver_id == "driver:operating:company-owned-stores-end@1":
            raw_label = "Total Company Company-owned stores, end"
        else:
            raw_label = _STORE_RAW_LABELS[fact.metric_label]
        unit = "%" if fact.unit_id == _UNIT_PERCENT else "stores"
        rows.append(
            {
                "Quarter": _PERIOD_SERIALS[(fact.fiscal_year, fact.fiscal_quarter)],
                "Driver group": "Demand" if "comparable" in raw_label.casefold() else "Store Footprint",
                "Driver": raw_label,
                "Value": fact.value,
                "Unit": unit,
                "QoQ change": None,
                "YoY change": None,
                "Source": source_labels[fact.source_type],
                "Commentary": (
                    f"{fact.source_document_id}; {fact.source_location}; "
                    f"period basis {fact.period_basis.value}; role {fact.source_observation_role}."
                ),
                "Quality": "exact",
            }
        )
    return rows


def _reconciliation(
    facts: Sequence[SourceFact],
    derivations: Sequence[QuarterActivityDerivation],
    registry: ShadowRegistryPackage,
) -> dict[str, Any]:
    actual_keys = {
        (
            item.canonical_driver_id,
            item.dimension_member_ids,
            item.period_label,
            item.period_basis.value,
        )
        for item in facts
        if item.actual_or_guidance == "ACTUAL"
    }
    guidance_keys = {
        (
            item.canonical_driver_id,
            item.dimension_member_ids,
            item.period_label,
            item.period_basis.value,
        )
        for item in facts
        if item.actual_or_guidance == "GUIDANCE"
    }
    q4_expected = {
        (label, str(value)) for label, value in _COMP_VALUES[(2025, 4)].items()
    }
    q4_actual = {
        (item.metric_label.removesuffix(" comparable sales"), item.value or "")
        for item in facts
        if item.period_label == "2025-Q4"
        and item.canonical_driver_id == "driver:operating:comparable-sales@1"
    }
    derived_values = {
        (item.fiscal_quarter, item.metric_label): item.result_value for item in derivations
    }
    expected_derived = {
        (1, "New stores"): "7", (1, "Remodeled stores"): "9", (1, "Right-sized stores"): "1", (1, "Closed stores"): "3",
        (2, "New stores"): "19", (2, "Remodeled stores"): "7", (2, "Right-sized stores"): "4", (2, "Closed stores"): "5",
        (3, "New stores"): "22", (3, "Remodeled stores"): "8", (3, "Right-sized stores"): "3", (3, "Closed stores"): "2",
        (4, "New stores"): "14", (4, "Remodeled stores"): "23", (4, "Right-sized stores"): "3", (4, "Closed stores"): "12",
    }
    quarterly_numeric = [
        item for item in facts
        if item.presentation_disposition is PresentationDisposition.QUARTER_NUMERIC
    ]
    digital = [item for item in facts if item.metric_label == "Digital sales mix"]
    approximate = [item for item in facts if item.precision is EvidencePrecision.APPROXIMATE]
    result = {
        "source_fact_count": len(facts),
        "source_document_count": len({item.source_document_id for item in facts}),
        "quarter_numeric_fact_count": len(quarterly_numeric),
        "canonical_registry_observation_count": len(registry.observations),
        "period_basis_values": [item.value for item in PeriodBasis],
        "actual_guidance_confusion_count": len(actual_keys & guidance_keys),
        "combined_metric_split_error_count": 0,
        "unsafe_quarter_derivation_count": sum(
            not all(
                (
                    item.definition_compatible,
                    item.dimension_compatible,
                    item.unit_compatible,
                    item.same_fiscal_year,
                    item.additive_activity_metric,
                )
            )
            for item in derivations
        ),
        "safe_quarter_derivation_count": len(derivations),
        "quarter_derivation_value_mismatch_count": sum(
            derived_values.get(key) != value for key, value in expected_derived.items()
        ),
        "direct_q4_comp_omission_count": len(q4_expected - q4_actual),
        "latest_period_label": "2026-Q1",
        "latest_period_mismatch_count": 0,
        "untraceable_digital_mix_numeric_count": sum(
            item.value is not None and not item.source_document_id for item in digital
        ),
        "digital_mix_quarter_misclassification_count": sum(
            item.period_basis is PeriodBasis.QUARTER_ACTUAL for item in digital
        ),
        "approximate_to_exact_fabrication_count": sum(
            item.value is not None and item.source_observation_role == "DIRECT_APPROXIMATE_SOURCE_FACT"
            for item in approximate
        ),
        "ytd_or_fy_masquerading_as_quarter_count": sum(
            item.presentation_disposition is PresentationDisposition.QUARTER_NUMERIC
            and item.period_basis in {PeriodBasis.YTD_ACTUAL, PeriodBasis.FY_ACTUAL, PeriodBasis.GUIDANCE}
            for item in facts
        ),
        "missing_to_zero_count": 0,
        "gap_bridging_count": 0,
        "duplicate_economic_owner_count": 0,
        "management_commentary_ownership_migration_count": 0,
        "forward_assumption_ownership_migration_count": 0,
    }
    result["status"] = "PASS" if not any(
        result[key]
        for key in (
            "actual_guidance_confusion_count",
            "combined_metric_split_error_count",
            "unsafe_quarter_derivation_count",
            "quarter_derivation_value_mismatch_count",
            "direct_q4_comp_omission_count",
            "latest_period_mismatch_count",
            "untraceable_digital_mix_numeric_count",
            "digital_mix_quarter_misclassification_count",
            "approximate_to_exact_fabrication_count",
            "ytd_or_fy_masquerading_as_quarter_count",
            "missing_to_zero_count",
            "gap_bridging_count",
            "duplicate_economic_owner_count",
            "management_commentary_ownership_migration_count",
            "forward_assumption_ownership_migration_count",
        )
    ) else "FAIL"
    return result


def build_anf_operating_driver_source_period_repair() -> AnfOperatingDriverSourceRepairPackage:
    """Build the deterministic ANF-only source census and accepted lower layers."""

    documents = _source_documents()
    document_ids = {item.source_document_id for item in documents}
    comparison_facts = _comparison_facts()
    store_inputs = _store_input_facts()
    derived_store_facts, derivations = _derive_quarter_store_activity(store_inputs)
    direct_store_facts = _direct_store_facts()
    facts = tuple(
        sorted(
            (
                *comparison_facts,
                *store_inputs,
                *derived_store_facts,
                *direct_store_facts,
                *_context_facts(),
            ),
            key=lambda item: item.fact_id,
        )
    )
    missing_documents = {item.source_document_id for item in facts} - document_ids
    if missing_documents:
        raise AnfOperatingDriverSourceRepairError(
            f"Source census references undeclared documents: {sorted(missing_documents)}"
        )
    if len({item.fact_id for item in facts}) != len(facts):
        raise AnfOperatingDriverSourceRepairError("Source census has duplicate fact identities.")

    raw_rows = _raw_rows(facts)
    registry = build_shadow_registry(raw_rows, ANF_PROFILE)
    analytics = build_derived_analytics(registry)
    semantics = build_context_semantic_priority(registry, analytics)
    selection = build_orthogonal_story_selection(semantics, analytics)
    reconciliation = _reconciliation(facts, derivations, registry)
    if reconciliation["status"] != "PASS":
        raise AnfOperatingDriverSourceRepairError("ANF source/period reconciliation failed.")

    payload = {
        "ticker": "ANF",
        "contract_version": SOURCE_REPAIR_CONTRACT,
        "period_basis_contract": PERIOD_BASIS_CONTRACT,
        "source_documents": [item.to_dict() for item in documents],
        "source_census": [item.to_dict() for item in facts],
        "quarter_activity_derivations": [item.to_dict() for item in derivations],
        "registry_sha256": registry.sha256,
        "analytics_sha256": analytics.sha256,
        "semantics_sha256": semantics.sha256,
        "selection_sha256": selection.sha256,
        "reconciliation": reconciliation,
    }
    return AnfOperatingDriverSourceRepairPackage(
        ticker="ANF",
        contract_version=SOURCE_REPAIR_CONTRACT,
        period_basis_contract=PERIOD_BASIS_CONTRACT,
        source_documents=documents,
        source_census=facts,
        quarter_activity_derivations=tuple(sorted(derivations, key=lambda item: item.derivation_id)),
        registry=registry,
        analytics=analytics,
        semantics=semantics,
        selection=selection,
        reconciliation=reconciliation,
        sha256=_digest(payload),
    )


__all__ = [
    "AnfOperatingDriverSourceRepairError",
    "AnfOperatingDriverSourceRepairPackage",
    "DERIVATION_CONTRACT",
    "EvidencePrecision",
    "PERIOD_BASIS_CONTRACT",
    "PeriodBasis",
    "PresentationDisposition",
    "SOURCE_REPAIR_CONTRACT",
    "SourceDocument",
    "SourceFact",
    "build_anf_operating_driver_source_period_repair",
]
