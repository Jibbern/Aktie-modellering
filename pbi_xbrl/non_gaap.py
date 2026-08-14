"""Non-GAAP text parsing, HTML stripping, and adjusted-metric extraction helpers."""
from __future__ import annotations

import re
import json
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from .cache_semantics import (
    ADJUSTED_METRIC_UNIT_NORMALIZATION_VERSION,
    NON_GAAP_ADJUSTMENT_DOMAIN_VERSION,
)
from .adjusted_metric_history import (
    ADJUSTED_METRIC_HISTORY_CONTRACT,
    AdjustedMetricId,
    AdjustedMetricPeriodType,
    AdjustedMetricScope,
    AdjustedMetricSourceRole,
    reported_adjusted_metric_definition_id,
)
from .debt_parser import coerce_number, read_html_tables_any
from .longitudinal_memory.identity import build_identity
from .metrics import _ADJ_EBIT_SYNONYMS, _GAAP_EBIT_SYNONYMS
from .sec_xbrl import normalize_accession, parse_date, strip_html


ADJUSTED_METRIC_SOURCE_UNIT_CONTRACT = "contract:adjusted-metric-source-unit-lineage@1"
NON_GAAP_ADJUSTMENT_DOMAIN_CONTRACT = "contract:non-gaap-adjustment-measure-domain@1"


class MeasureDomain(str, Enum):
    MONETARY_AMOUNT = "monetary_amount"
    PER_SHARE_AMOUNT = "per_share_amount"


class AdjustmentTableRole(str, Enum):
    AMOUNT_RECONCILIATION = "amount_reconciliation"
    EPS_RECONCILIATION = "eps_reconciliation"
    MIXED_RECONCILIATION = "mixed_reconciliation"
    UNRESOLVED = "unresolved"


class SourceUnitContractError(ValueError):
    """Raised when source amount scale cannot be resolved unambiguously."""


class SourceAmountScale(str, Enum):
    ONES = "ones"
    THOUSANDS = "thousands"
    MILLIONS = "millions"

    @property
    def factor_to_usd(self) -> float:
        return {
            SourceAmountScale.ONES: 1.0,
            SourceAmountScale.THOUSANDS: 1_000.0,
            SourceAmountScale.MILLIONS: 1_000_000.0,
        }[self]


@dataclass(frozen=True)
class SourceAmountUnit:
    currency: str
    scale: SourceAmountScale
    declaration: str
    measure_domain: MeasureDomain = MeasureDomain.MONETARY_AMOUNT

    @property
    def factor_to_usd(self) -> float:
        return self.scale.factor_to_usd

    @property
    def canonical_unit(self) -> str:
        if self.measure_domain is MeasureDomain.PER_SHARE_AMOUNT:
            return "USD/share"
        return "USD"

    @property
    def canonical_unit_id(self) -> str:
        if self.measure_domain is MeasureDomain.PER_SHARE_AMOUNT:
            return "unit:core:currency-per-share@1"
        return "unit:core:currency@1"


@dataclass(frozen=True)
class CanonicalSourceAmount:
    raw_source_scalar: float
    source_unit: SourceAmountUnit
    canonical_currency: str
    canonical_value: float

    @property
    def canonical_usd_millions(self) -> float:
        if self.source_unit.measure_domain is not MeasureDomain.MONETARY_AMOUNT:
            raise SourceUnitContractError(
                "USD-millions conversion is not defined for per-share source amounts."
            )
        if self.canonical_currency != "USD":
            raise SourceUnitContractError(
                f"USD-millions conversion requires USD, received {self.canonical_currency!r}."
            )
        return self.canonical_value / 1_000_000.0

    @property
    def measure_domain(self) -> MeasureDomain:
        return self.source_unit.measure_domain

    @property
    def canonical_unit(self) -> str:
        return self.source_unit.canonical_unit

    @property
    def canonical_unit_id(self) -> str:
        return self.source_unit.canonical_unit_id


@dataclass(frozen=True)
class AdjustmentTableClassification:
    role: AdjustmentTableRole
    measure_domain: Optional[MeasureDomain]
    source_unit: Optional[SourceAmountUnit]
    per_share_source_unit: Optional[SourceAmountUnit]
    source_unit_row_index: Optional[int]
    evidence: str


@dataclass(frozen=True)
class CanonicalAdjustmentFact:
    period: pd.Timestamp
    metric_id: str
    source_label: str
    table_role: AdjustmentTableRole
    measure_domain: MeasureDomain
    basis: str
    scope: str
    definition_id: str
    amount: CanonicalSourceAmount
    source_table_index: int
    source_row_index: int
    source_column_index: int
    source_column_label: str
    source_unit_row_index: Optional[int]

    @property
    def source_locator(self) -> str:
        return (
            f"html-table:{self.source_table_index};row:{self.source_row_index};"
            f"column:{self.source_column_index}"
        )

    @property
    def semantic_key(self) -> Tuple[str, str, str, str, str, str]:
        return (
            pd.Timestamp(self.period).date().isoformat(),
            self.metric_id,
            self.measure_domain.value,
            self.basis,
            self.scope,
            self.definition_id,
        )

    def to_lineage_record(self) -> Dict[str, Any]:
        return {
            "contract": NON_GAAP_ADJUSTMENT_DOMAIN_CONTRACT,
            "period": pd.Timestamp(self.period).date().isoformat(),
            "metric_id": self.metric_id,
            "source_label": self.source_label,
            "table_role": self.table_role.value,
            "measure_domain": self.measure_domain.value,
            "basis": self.basis,
            "scope": self.scope,
            "definition_id": self.definition_id,
            "raw_source_scalar": self.amount.raw_source_scalar,
            "raw_source_unit_text": self.amount.source_unit.declaration,
            "normalized_source_scale": self.amount.source_unit.scale.value,
            "currency": self.amount.source_unit.currency,
            "canonical_unit": self.amount.canonical_unit,
            "canonical_unit_id": self.amount.canonical_unit_id,
            "canonical_value": self.amount.canonical_value,
            "source_table_index": self.source_table_index,
            "source_row_index": self.source_row_index,
            "source_column_index": self.source_column_index,
            "source_column_label": self.source_column_label,
            "source_unit_row_index": self.source_unit_row_index,
            "source_locator": self.source_locator,
        }


_SOURCE_UNIT_PATTERN = re.compile(
    r"(?:"
    r"(?:\$|USD|U\.S\.\s+dollars?|dollars?)\s*(?:amounts?\s+)?"
    r"(?:are\s+)?(?:stated\s+)?(?:in\s+)?"
    r"|(?:amounts?\s+)?(?:are\s+)?(?:stated\s+)?in\s+"
    r")(?P<scale>thousands?|millions?)\b",
    re.IGNORECASE,
)


def _source_unit_declarations(text: str) -> List[Tuple[int, SourceAmountUnit]]:
    declarations: List[Tuple[int, SourceAmountUnit]] = []
    for match in _SOURCE_UNIT_PATTERN.finditer(str(text or "")):
        token = str(match.group("scale") or "").lower()
        scale = SourceAmountScale.THOUSANDS if token.startswith("thousand") else SourceAmountScale.MILLIONS
        declarations.append(
            (
                match.start(),
                SourceAmountUnit(
                    currency="USD",
                    scale=scale,
                    declaration=re.sub(r"\s+", " ", match.group(0)).strip(),
                    measure_domain=MeasureDomain.MONETARY_AMOUNT,
                ),
            )
        )
    return declarations


def detect_source_amount_unit(text: str, *, default_to_ones: bool = False) -> Optional[SourceAmountUnit]:
    """Resolve an explicit USD amount scale without magnitude inference."""

    declarations = _source_unit_declarations(text)
    scales = {unit.scale for _, unit in declarations}
    if len(scales) > 1:
        raise SourceUnitContractError(
            "Source scope contains conflicting amount scales: "
            + ", ".join(sorted(scale.value for scale in scales))
        )
    if declarations:
        return declarations[0][1]
    if default_to_ones:
        return SourceAmountUnit(currency="USD", scale=SourceAmountScale.ONES, declaration="unit:USD")
    return None


def normalize_source_amount(
    value: Any,
    source_unit: SourceAmountUnit,
) -> CanonicalSourceAmount:
    """Convert a source scalar to canonical USD exactly once."""

    if isinstance(value, CanonicalSourceAmount):
        if value.source_unit != source_unit:
            raise SourceUnitContractError(
                "An already-normalized amount cannot be reinterpreted under a different source unit."
            )
        return value
    if not isinstance(source_unit, SourceAmountUnit):
        raise SourceUnitContractError(f"Expected SourceAmountUnit, received {source_unit!r}.")
    if source_unit.currency != "USD":
        raise SourceUnitContractError(f"Unsupported source currency {source_unit.currency!r}.")
    source_value = value
    if isinstance(value, str):
        parenthetical = re.fullmatch(r"\(\s*([^()]+?)\s*\)", value.strip())
        if parenthetical is not None:
            source_value = "-" + parenthetical.group(1)
    raw_scalar = coerce_number(source_value)
    if raw_scalar is None:
        raise SourceUnitContractError(f"Source amount is not numeric: {value!r}.")
    raw_float = float(raw_scalar)
    return CanonicalSourceAmount(
        raw_source_scalar=raw_float,
        source_unit=source_unit,
        canonical_currency="USD",
        canonical_value=raw_float * source_unit.factor_to_usd,
    )


def _source_unit_for_table(
    table: pd.DataFrame,
    document_default: Optional[SourceAmountUnit],
) -> Tuple[Optional[SourceAmountUnit], Optional[int]]:
    declarations: List[Tuple[int, int, SourceAmountUnit, str]] = []
    if table is not None and not table.empty:
        for row_index in range(len(table)):
            for column_index in range(int(table.shape[1])):
                raw = table.iat[row_index, column_index]
                try:
                    if pd.isna(raw):
                        continue
                except Exception:
                    pass
                cell_text = re.sub(r"\s+", " ", str(raw or "")).strip()
                if not cell_text:
                    continue
                for _, detected in _source_unit_declarations(cell_text):
                    declarations.append(
                        (
                            row_index,
                            column_index,
                            SourceAmountUnit(
                                currency=detected.currency,
                                scale=detected.scale,
                                declaration=cell_text,
                            ),
                            cell_text,
                        )
                    )
    scales = {unit.scale for _, _, unit, _ in declarations}
    if len(scales) > 1:
        raise SourceUnitContractError(
            "Adjusted-metric table contains conflicting amount scales: "
            + ", ".join(sorted(scale.value for scale in scales))
        )
    if declarations:
        row_index, _column_index, unit, _cell_text = declarations[0]
        return unit, row_index
    return document_default, None


def _amount_lineage_metadata(
    *,
    amount: CanonicalSourceAmount,
    table_index: int,
    row_index: int,
    column_index: int,
    unit_row_index: Optional[int],
    column_label: Optional[str],
) -> Dict[str, Any]:
    return {
        "raw_source_scalar": amount.raw_source_scalar,
        "source_currency": amount.source_unit.currency,
        "source_scale": amount.source_unit.scale.value,
        "source_scale_factor": amount.source_unit.factor_to_usd,
        "source_unit_declaration": amount.source_unit.declaration,
        "measure_domain": amount.measure_domain.value,
        "source_unit_row_index": unit_row_index,
        "canonical_currency": amount.canonical_currency,
        "canonical_unit": amount.canonical_unit,
        "canonical_unit_id": amount.canonical_unit_id,
        "canonical_value": amount.canonical_value,
        "canonical_usd_millions": amount.canonical_usd_millions,
        "source_table_index": int(table_index),
        "source_row_index": int(row_index),
        "source_column_index": int(column_index),
        "source_column_label": str(column_label or ""),
        "source_locator": f"html-table:{int(table_index)};row:{int(row_index)};column:{int(column_index)}",
    }


def _clean_table_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return re.sub(r"\s+", " ", str(value)).strip()


def _table_blob(table: pd.DataFrame) -> str:
    if table is None or table.empty:
        return ""
    return " ".join(
        text
        for text in (_clean_table_text(value) for value in table.to_numpy().ravel())
        if text
    )


def _canonical_adjustment_metric_id(label: str) -> str:
    token = re.sub(r"[^a-z0-9]+", "-", str(label or "").strip().lower()).strip("-")
    if not token:
        raise SourceUnitContractError("Adjustment metric identity requires a non-empty source label.")
    return f"metric:non-gaap-adjustment:{token}@1"


def _eps_source_unit(declaration: str) -> SourceAmountUnit:
    return SourceAmountUnit(
        currency="USD",
        scale=SourceAmountScale.ONES,
        declaration=declaration,
        measure_domain=MeasureDomain.PER_SHARE_AMOUNT,
    )


def _eps_table_unit_declaration(table: pd.DataFrame) -> str:
    cells = [
        _clean_table_text(value)
        for value in table.to_numpy().ravel()
        if _clean_table_text(value)
    ]
    endpoints: List[str] = []
    for cell in cells:
        low = cell.lower()
        if (
            re.search(r"\bgaap\s+(?:diluted\s+)?eps\b", low)
            or "reported diluted earnings" in low and "per share" in low
            or re.search(r"\badjusted\s+(?:diluted\s+)?(?:eps|earnings\s+per\s+share)\b", low)
        ):
            normalized = re.sub(r"\s+", " ", cell).strip()
            if normalized not in endpoints:
                endpoints.append(normalized)
    currency_marker = next((cell for cell in cells if cell.strip() == "$"), "USD")
    return " | ".join(endpoints[:2] + [currency_marker])


def classify_adjustment_table(
    table: pd.DataFrame,
    *,
    document_default: Optional[SourceAmountUnit] = None,
) -> AdjustmentTableClassification:
    """Classify adjustment evidence before keyword rows are materialized."""

    blob = _table_blob(table)
    low = blob.lower()
    if not low:
        return AdjustmentTableClassification(
            role=AdjustmentTableRole.UNRESOLVED,
            measure_domain=None,
            source_unit=None,
            per_share_source_unit=None,
            source_unit_row_index=None,
            evidence="empty_table",
        )
    local_unit, unit_row_index = _source_unit_for_table(table, None)
    has_gaap_eps = bool(
        re.search(r"\bgaap\s+(?:diluted\s+)?eps\b|\bgaap\s+(?:earnings|loss)\s+per\s+share\b", low)
    )
    has_adjusted_eps = bool(
        re.search(r"\badjusted\s+(?:diluted\s+)?(?:eps|earnings\s+per\s+share)\b", low)
    )
    has_eps_reconciliation_heading = bool(
        re.search(
            r"reconciliation\s+of\s+reported[^.]{0,160}per\s+share[^.]{0,160}"
            r"adjusted[^.]{0,80}per\s+share",
            low,
        )
    )
    has_eps_reconciliation = (has_gaap_eps and has_adjusted_eps) or has_eps_reconciliation_heading
    has_adjusted_amount_endpoint = bool(
        re.search(r"\badjusted\s+ebit(?:da)?\b|\badjusted\s+free\s+cash\s+flow\b", low)
    )
    has_fcf_reconciliation = (
        "free cash flow" in low
        and (
            "cash flow from operating activities" in low
            or "cash flows from operating activities" in low
            or "capital expenditures" in low
        )
    )
    has_amount_reconciliation = has_adjusted_amount_endpoint or has_fcf_reconciliation

    if has_eps_reconciliation and has_amount_reconciliation:
        if local_unit is None:
            return AdjustmentTableClassification(
                role=AdjustmentTableRole.UNRESOLVED,
                measure_domain=None,
                source_unit=None,
                per_share_source_unit=None,
                source_unit_row_index=None,
                evidence="mixed_reconciliation_missing_table_local_amount_unit",
            )
        return AdjustmentTableClassification(
            role=AdjustmentTableRole.MIXED_RECONCILIATION,
            measure_domain=None,
            source_unit=local_unit,
            per_share_source_unit=_eps_source_unit(_eps_table_unit_declaration(table)),
            source_unit_row_index=unit_row_index,
            evidence="gaap_to_adjusted_eps_and_adjusted_amount_endpoints",
        )
    if has_eps_reconciliation:
        if "$" not in blob and "usd" not in low and "dollar" not in low:
            return AdjustmentTableClassification(
                role=AdjustmentTableRole.UNRESOLVED,
                measure_domain=None,
                source_unit=None,
                per_share_source_unit=None,
                source_unit_row_index=None,
                evidence="eps_reconciliation_missing_currency_identity",
            )
        return AdjustmentTableClassification(
            role=AdjustmentTableRole.EPS_RECONCILIATION,
            measure_domain=MeasureDomain.PER_SHARE_AMOUNT,
            source_unit=_eps_source_unit(_eps_table_unit_declaration(table)),
            per_share_source_unit=_eps_source_unit(_eps_table_unit_declaration(table)),
            source_unit_row_index=None,
            evidence="gaap_eps_to_adjusted_eps",
        )
    if has_amount_reconciliation:
        if local_unit is None:
            # An explicit unscaled dollar column is table-local ONES evidence.
            if "$" in blob:
                local_unit = SourceAmountUnit(
                    currency="USD",
                    scale=SourceAmountScale.ONES,
                    declaration="table-cell:$",
                    measure_domain=MeasureDomain.MONETARY_AMOUNT,
                )
            else:
                return AdjustmentTableClassification(
                    role=AdjustmentTableRole.UNRESOLVED,
                    measure_domain=None,
                    source_unit=None,
                    per_share_source_unit=None,
                    source_unit_row_index=None,
                    evidence="amount_reconciliation_missing_source_unit",
                )
        return AdjustmentTableClassification(
            role=AdjustmentTableRole.AMOUNT_RECONCILIATION,
            measure_domain=MeasureDomain.MONETARY_AMOUNT,
            source_unit=local_unit,
            per_share_source_unit=None,
            source_unit_row_index=unit_row_index,
            evidence="adjusted_amount_or_free_cash_flow_reconciliation",
        )
    return AdjustmentTableClassification(
        role=AdjustmentTableRole.UNRESOLVED,
        measure_domain=None,
        source_unit=None,
        per_share_source_unit=None,
        source_unit_row_index=None,
        evidence="no_supported_adjustment_table_role",
    )


def _adjustment_row_domain(
    classification: AdjustmentTableClassification,
    *,
    row_label: str,
    section_domain: Optional[MeasureDomain] = None,
) -> MeasureDomain:
    if section_domain is not None:
        return section_domain
    if classification.role is AdjustmentTableRole.EPS_RECONCILIATION:
        return MeasureDomain.PER_SHARE_AMOUNT
    if classification.role is AdjustmentTableRole.AMOUNT_RECONCILIATION:
        return MeasureDomain.MONETARY_AMOUNT
    if classification.role is AdjustmentTableRole.MIXED_RECONCILIATION:
        if _is_eps_label(row_label) or "per share" in row_label.lower():
            return MeasureDomain.PER_SHARE_AMOUNT
        return MeasureDomain.MONETARY_AMOUNT
    raise SourceUnitContractError(
        f"Adjustment row {row_label!r} belongs to unresolved table role {classification.role.value!r}."
    )


def _adjustment_source_unit(
    classification: AdjustmentTableClassification,
    *,
    row_label: str,
    section_domain: Optional[MeasureDomain] = None,
) -> SourceAmountUnit:
    domain = _adjustment_row_domain(
        classification,
        row_label=row_label,
        section_domain=section_domain,
    )
    if domain is MeasureDomain.PER_SHARE_AMOUNT:
        if classification.per_share_source_unit is None:
            raise SourceUnitContractError(
                f"Per-share adjustment row {row_label!r} has no table-owned per-share unit."
            )
        return classification.per_share_source_unit
    if classification.source_unit is None:
        raise SourceUnitContractError(
            f"Monetary adjustment row {row_label!r} has no table-owned source unit."
        )
    return classification.source_unit


def _build_adjustment_fact(
    *,
    period: pd.Timestamp,
    source_label: str,
    raw_value: Any,
    classification: AdjustmentTableClassification,
    table_index: int,
    row_index: int,
    column_index: int,
    column_label: str,
    section_domain: Optional[MeasureDomain] = None,
) -> Optional[CanonicalAdjustmentFact]:
    source_unit = _adjustment_source_unit(
        classification,
        row_label=source_label,
        section_domain=section_domain,
    )
    parsed = coerce_number(raw_value)
    if parsed is None:
        return None
    amount = normalize_source_amount(parsed, source_unit)
    domain = source_unit.measure_domain
    definition_id = (
        "definition:issuer-adjusted-eps-reconciliation-component@1"
        if domain is MeasureDomain.PER_SHARE_AMOUNT
        else "definition:issuer-non-gaap-adjustment-amount@1"
    )
    return CanonicalAdjustmentFact(
        period=pd.Timestamp(period).normalize(),
        metric_id=_canonical_adjustment_metric_id(source_label),
        source_label=re.sub(r"\s+", " ", str(source_label)).strip(),
        table_role=classification.role,
        measure_domain=domain,
        basis="adjusted_non_gaap_reconciliation",
        scope="reported_consolidated_at_period",
        definition_id=definition_id,
        amount=amount,
        source_table_index=int(table_index),
        source_row_index=int(row_index),
        source_column_index=int(column_index),
        source_column_label=str(column_label or ""),
        source_unit_row_index=classification.source_unit_row_index,
    )


def reconcile_adjustment_facts(
    facts: Iterable[CanonicalAdjustmentFact],
) -> List[CanonicalAdjustmentFact]:
    """Reconcile exact duplicates without collapsing facts across domains."""

    grouped: Dict[Tuple[str, str, str, str, str, str], List[CanonicalAdjustmentFact]] = {}
    for fact in facts:
        grouped.setdefault(fact.semantic_key, []).append(fact)
    selected: List[CanonicalAdjustmentFact] = []
    for key in sorted(grouped):
        candidates = sorted(
            grouped[key],
            key=lambda fact: (
                fact.source_table_index,
                fact.source_row_index,
                fact.source_column_index,
                fact.source_locator,
            ),
        )
        values = {float(fact.amount.canonical_value) for fact in candidates}
        units = {fact.amount.canonical_unit_id for fact in candidates}
        if len(values) != 1 or len(units) != 1:
            raise SourceUnitContractError(
                "Conflicting same-domain adjustment facts for semantic key "
                f"{key!r}: "
                + ", ".join(
                    f"{fact.amount.canonical_value!r} {fact.amount.canonical_unit} at {fact.source_locator}"
                    for fact in candidates
                )
            )
        selected.append(candidates[0])
    return selected


def reconcile_adjustment_breakdown_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Reconcile bundle rows by semantic identity, never by physical row order."""

    if frame is None or frame.empty:
        return pd.DataFrame() if frame is None else frame.copy()
    required = {
        "period",
        "metric_id",
        "measure_domain",
        "basis",
        "scope",
        "definition_id",
        "canonical_value",
        "canonical_unit_id",
        "source_occurrence_id",
    }
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise SourceUnitContractError(
            "Adjustment breakdown rows are missing canonical semantic fields: " + ", ".join(missing)
        )
    keys = ["period", "metric_id", "measure_domain", "basis", "scope", "definition_id"]
    selected: List[Dict[str, Any]] = []
    grouped = frame.groupby(keys, dropna=False, sort=True)
    for key, group in grouped:
        values = {float(value) for value in group["canonical_value"].tolist()}
        units = {str(value) for value in group["canonical_unit_id"].tolist()}
        if len(values) != 1 or len(units) != 1:
            details = group[
                ["canonical_value", "canonical_unit_id", "source_document_id", "source_locator"]
            ].to_dict("records")
            raise SourceUnitContractError(
                f"Conflicting same-domain adjustment breakdown rows for {key!r}: {details!r}"
            )
        ordered = group.sort_values(
            ["source_document_id", "source_occurrence_id", "source_locator"],
            kind="stable",
        )
        row = ordered.iloc[0].to_dict()
        occurrences = sorted({str(value) for value in ordered["source_occurrence_id"].tolist()})
        row["corroboration_count"] = len(occurrences)
        row["corroborating_occurrence_ids_json"] = json.dumps(
            occurrences,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        selected.append(row)
    return pd.DataFrame(selected).sort_values(
        keys + ["source_document_id", "source_locator"],
        kind="stable",
    ).reset_index(drop=True)


def find_ex99_docs(index_json: Dict[str, Any]) -> List[str]:
    items = index_json.get("directory", {}).get("item", [])
    names = [it.get("name", "") for it in items]
    cand = []
    for n in names:
        ln = n.lower()
        if not ln.endswith((".htm", ".html", ".txt", ".pdf")):
            continue
        if re.search(
            r"(ex[-_]?99|99[-_.]?[12]|earnings(?:press)?releas|pressrelea|shareholderletter|stockholderletter|ceoletter|investorletter)",
            ln,
        ):
            cand.append(n)
    return sorted(set(cand))


def infer_quarter_end_from_text(txt: str) -> Optional[pd.Timestamp]:
    patterns = [
        r"(?:Thirteen|Twenty[-\s]?Six|Thirty[-\s]?Nine|Fifty[-\s]?Two|Fifty[-\s]?Three)\s+Weeks\s+Ended\s+([A-Za-z]+)\s+(\d{1,2}),?\s*(\d{4})",
        r"Three\s+Months\s+Ended\s+([A-Za-z]+)\s+(\d{1,2}),?\s*(\d{4})",
        r"Quarter\s+Ended\s+([A-Za-z]+)\s+(\d{1,2}),?\s*(\d{4})",
        r"Fourth\s+Quarter\s+and\s+Full\s+Year\s+(\d{4})",
        r"Third\s+Quarter\s+and\s+Full\s+Year\s+(\d{4})",
        r"Second\s+Quarter\s+and\s+Full\s+Year\s+(\d{4})",
        r"First\s+Quarter\s+and\s+Full\s+Year\s+(\d{4})",
        r"Fourth\s+Quarter\s+(\d{4})",
        r"Third\s+Quarter\s+(\d{4})",
        r"Second\s+Quarter\s+(\d{4})",
        r"First\s+Quarter\s+(\d{4})",
        r"Q([1-4])\s*(20\d{2})",
    ]
    for pat in patterns:
        m = re.search(pat, txt, re.IGNORECASE)
        if not m:
            continue
        if len(m.groups()) == 2:
            try:
                q = int(m.group(1))
                year = int(m.group(2))
                if 1 <= q <= 4:
                    return pd.Timestamp(year=year, month=3 * q, day=30 if q in (2, 3) else 31).date()
            except Exception:
                pass
        if len(m.groups()) == 1:
            year = int(m.group(1))
            if "Fourth" in pat:
                return pd.Timestamp(year=year, month=12, day=31).date()
            if "Third" in pat:
                return pd.Timestamp(year=year, month=9, day=30).date()
            if "Second" in pat:
                return pd.Timestamp(year=year, month=6, day=30).date()
            if "First" in pat:
                return pd.Timestamp(year=year, month=3, day=31).date()
        if len(m.groups()) >= 3:
            month, day, year = m.group(1), m.group(2), m.group(3)
            try:
                return pd.Timestamp(f"{month} {day} {year}").date()
            except Exception:
                continue
    return None


def normalize_number_spacing(s: str) -> str:
    s = re.sub(r"(\d)\s+,(\s+)?(\d)", r"\1,\3", s)
    s = re.sub(r"(\d)\s+(\d{2,3},\d{3})", r"\1\2", s)
    return s


def _slice_three_month_block(lines: List[str]) -> List[str]:
    """Return a slice of lines that appear to belong to the 'Three Months Ended' section."""
    start = None
    end = None
    for i, ln in enumerate(lines):
        if re.search(r"three\s+months\s+ended|quarter\s+ended|thirteen\s+weeks\s+ended", ln, re.I):
            start = i
            continue
        if start is not None and re.search(r"six\s+months|nine\s+months|twelve\s+months|twenty[-\s]?six\s+weeks|thirty[-\s]?nine\s+weeks|fifty[-\s]?two\s+weeks|fifty[-\s]?three\s+weeks|year\s+ended|fiscal\s+year", ln, re.I):
            # Allow a few header lines where 3M/6M appear together before the data rows.
            if i - start <= 3:
                continue
            end = i
            break
    if start is not None:
        return lines[start:end] if end is not None else lines[start:]
    return lines


def _detect_scale(html: str) -> float:
    unit = detect_source_amount_unit(html, default_to_ones=True)
    if unit is None:  # pragma: no cover - default_to_ones guarantees a unit
        return 1.0
    return unit.factor_to_usd


def _detect_local_scale(text: str, default_scale: float = 1.0) -> float:
    unit = detect_source_amount_unit(str(text or ""), default_to_ones=False)
    return unit.factor_to_usd if unit is not None else float(default_scale)


def _label_matches(label: str, needles: List[str]) -> bool:
    ln = label.lower()
    return any(n in ln for n in needles)


def _parse_date_from_text(text: str) -> Optional[pd.Timestamp]:
    m = re.search(
        r"(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|"
        r"Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\.?\s+\d{1,2},?\s+\d{4}",
        text,
        re.IGNORECASE,
    )
    if not m:
        return None
    try:
        return pd.to_datetime(m.group(0), errors="coerce").date()
    except Exception:
        return None


def _period_hint_from_text(text: str) -> Optional[str]:
    t = re.sub(r"[\s\-]+", " ", str(text or "").lower())
    hits: List[str] = []
    if re.search(r"three months|quarter ended|quarterly", t):
        hits.append("3M")
    if re.search(r"six months", t):
        hits.append("6M")
    if re.search(r"nine months", t):
        hits.append("9M")
    if re.search(r"twelve months|year ended|fiscal year|annual", t):
        hits.append("FY")
    hits = list(dict.fromkeys(hits))
    if len(hits) == 1:
        return hits[0]
    return None


def _flatten_cols(cols: Any) -> List[str]:
    if isinstance(cols, pd.MultiIndex):
        return [" ".join([str(c) for c in tup if str(c) != "nan"]).strip() for tup in cols]
    return [str(c) for c in cols]


def _find_header_dates(
    df: pd.DataFrame,
) -> Tuple[List[str], Optional[int], Dict[int, pd.Timestamp], Optional[str]]:
    cols = _flatten_cols(df.columns)
    col_dates: Dict[int, pd.Timestamp] = {}
    for i, c in enumerate(cols):
        d = _parse_date_from_text(c)
        if d:
            col_dates[i] = d
    if col_dates:
        return cols, None, col_dates, _period_hint_from_text(" ".join(cols))

    if not df.empty:
        row0 = [str(x) for x in df.iloc[0].tolist()]
        row0_hint = _period_hint_from_text(" ".join(row0))
        row_dates: Dict[int, pd.Timestamp] = {}
        for i, c in enumerate(row0):
            d = _parse_date_from_text(c)
            if d:
                row_dates[i] = d
        if row_dates:
            return row0, 0, row_dates, row0_hint

    for ridx in range(min(6, len(df))):
        row = [str(x) for x in df.iloc[ridx].tolist()]
        row_hint = _period_hint_from_text(" ".join(row))
        row_dates: Dict[int, pd.Timestamp] = {}
        for i, c in enumerate(row):
            d = _parse_date_from_text(c)
            if d:
                row_dates[i] = d
        row_text = " ".join(row).lower()
        if row_dates and ("month" in row_text or "ended" in row_text or "three months" in row_text or "quarter" in row_text):
            return row, ridx, row_dates, row_hint
        if row_dates:
            return row, ridx, row_dates, row_hint

        if "three months" in row_text or "quarter ended" in row_text:
            if ridx + 1 < len(df):
                row2 = [str(x) for x in df.iloc[ridx + 1].tolist()]
                row_dates2: Dict[int, pd.Timestamp] = {}
                for i, c in enumerate(row2):
                    d = _parse_date_from_text(c)
                    if d:
                        row_dates2[i] = d
                if row_dates2:
                    return row2, ridx + 1, row_dates2, row_hint

    return cols, None, {}, None


def _is_eps_label(label: str) -> bool:
    ln = str(label or "").lower()
    if "per share" in ln or "eps" in ln:
        if "shares used" in ln or "weighted-average shares" in ln:
            return False
        return True
    return False


def _is_adj_eps_label(label: str) -> bool:
    ln = str(label or "").lower()
    if "adjusted" in ln and _is_eps_label(ln):
        return True
    return False


def _parse_adjusted_from_text(
    txt: str,
    quarter_end: Optional[pd.Timestamp],
    mode: str,
) -> Tuple[Optional[float], Optional[float], Optional[float], Dict[str, float], str, Optional[str]]:
    if not txt:
        return None, None, None, {}, "ocr_no_text", None
    if quarter_end is None:
        return None, None, None, {}, "no_quarter_end", None

    t_low = txt.lower()
    # Guard: skip segment-only pages (Adjusted Segment EBIT/EBITDA tables)
    if ("adjusted segment" in t_low or "reportable segments" in t_low) and "reconciliation of reported" not in t_low:
        return None, None, None, {}, "segment_page", None

    txt = normalize_number_spacing(txt)
    try:
        scale = _detect_scale(txt)
    except SourceUnitContractError:
        return None, None, None, {}, "ambiguous_source_unit", None

    if mode == "strict":
        q_detect = infer_quarter_end_from_text(txt)
        if q_detect is None or q_detect != quarter_end:
            return None, None, None, {}, "ocr_no_quarter_end", None

    lines = [re.sub(r"\s+", " ", ln).strip() for ln in txt.splitlines() if ln.strip()]
    # Restrict parsing to the "Three Months Ended" block when present.
    lines_3m = _slice_three_month_block(lines)

    # Try to detect "Three Months Ended" header years to pick correct column
    years_3m: List[int] = []
    has_6m_block = any(re.search(r"six\s+months\s+ended|twenty[-\s]?six\s+weeks\s+ended", ln, re.I) for ln in lines[:80])
    for i, ln in enumerate(lines_3m[:40]):
        if re.search(r"three months|quarter ended|thirteen weeks", ln, re.I):
            yrs = [int(y) for y in re.findall(r"(20\d{2})", ln)]
            if not yrs:
                # check next couple of lines for year headers
                for j in range(1, 3):
                    if i + j < len(lines_3m):
                        yrs.extend([int(y) for y in re.findall(r"(20\d{2})", lines_3m[i + j])])
            if yrs:
                # keep order, unique
                seen = set()
                for y in yrs:
                    if y not in seen:
                        years_3m.append(y)
                        seen.add(y)
            if years_3m:
                break

    def _pick_number_by_year(nums: List[float]) -> Optional[float]:
        if not nums:
            return None
        # If a 6M block exists and we have 4+ numbers, assume first two are 3M (current/prior)
        if has_6m_block and len(nums) >= 4:
            nums = nums[:2]
        elif has_6m_block and len(nums) >= 3:
            nums = nums[:2]
        if quarter_end is None or not years_3m or len(nums) < 2:
            return nums[0]
        y = int(pd.Timestamp(quarter_end).year)
        if y not in years_3m and int(pd.Timestamp(quarter_end).month) in (1, 2) and (y - 1) in years_3m:
            y = y - 1
        if y == years_3m[0]:
            return nums[0]
        if len(years_3m) > 1 and y == years_3m[1]:
            return nums[1]
        return nums[0]

    def _extract_nums_from_line(line: str) -> List[float]:
        tokens = re.findall(r"\(?-?\d{1,3}(?:,\d{3})*(?:\.\d+)?\)?", line)
        nums: List[float] = []
        local_scale = _detect_local_scale(line, scale)
        for t in tokens:
            v = coerce_number(t)
            if v is None:
                continue
            # skip year-like tokens
            if isinstance(v, (int, float)) and 1900 <= float(v) <= 2100 and len(str(int(v))) == 4:
                continue
            nums.append(float(v) * local_scale)
        return nums

    def _find_value(keys: List[str], exclude_terms: Optional[List[str]] = None) -> Optional[float]:
        for i, ln in enumerate(lines_3m):
            if _label_matches(ln, keys):
                ln_low = ln.lower()
                if exclude_terms and any(term in ln_low for term in exclude_terms):
                    continue
                if "reconciliation of" in ln_low:
                    # Skip section headers that mention adjusted metrics but have no numbers.
                    continue
                # Avoid picking segment tables when we need consolidated adjusted metrics
                if "segment" in ln_low:
                    continue
                # Prefer numbers that appear after the label to avoid cross-line OCR bleed.
                match_key = None
                match_pos = None
                for k in keys:
                    pos = ln_low.find(k)
                    if pos >= 0 and (match_pos is None or pos < match_pos):
                        match_pos = pos
                        match_key = k
                if match_key is not None and match_pos is not None:
                    ln_use = ln[match_pos + len(match_key):]
                else:
                    ln_use = ln
                nums = _extract_nums_from_line(ln_use)
                if not nums:
                    # numbers may be on the next line(s)
                    for j in range(1, 3):
                        if i + j < len(lines_3m):
                            next_line = str(lines_3m[i + j] or "")
                            if re.search(r"[A-Za-z]{3,}", next_line):
                                continue
                            nums = _extract_nums_from_line(next_line)
                            if nums:
                                break
                if not nums:
                    continue
                return _pick_number_by_year(nums)
        return None

    # "Adjusted earnings before interest" is an EBIT label, not EBITDA.
    adj_ebitda = _find_value(
        [
            "adjusted ebitda",
            "adjusted earnings before interest taxes depreciation and amortization",
        ]
    )
    adj_ebit = _find_value(_ADJ_EBIT_SYNONYMS, exclude_terms=["ebitda", "depreciation and amortization"])
    adj_eps: Optional[float] = None

    if adj_ebit is None:
        in_operating_income_block = False
        for ln in lines_3m:
            ln_low = ln.lower()
            if "operating income" in ln_low and ("bps change" in ln_low or "net sales" in ln_low or "gaap" in ln_low):
                in_operating_income_block = True
                continue
            if in_operating_income_block and re.search(r"^(adjusted\s+non-gaap|non-gaap\s+constant)", ln_low):
                nums = _extract_nums_from_line(ln)
                if nums:
                    adj_ebit = _pick_number_by_year(nums)
                    break
            if in_operating_income_block and (
                "net income per diluted share" in ln_low
                or "reconciliation of ebitda" in ln_low
                or "balance sheets" in ln_low
            ):
                in_operating_income_block = False

    for i, ln in enumerate(lines_3m):
        if not _is_adj_eps_label(ln):
            continue
        ln_low = ln.lower()
        if "reconciliation of" in ln_low and "adjusted" in ln_low and "per share" in ln_low:
            # Header row; the next line is often GAAP EPS, not adjusted EPS.
            continue
        def _extract_eps_nums(s: str) -> List[float]:
            tokens = re.findall(r"\(?-?\d+(?:\.\d+)?\)?", s)
            eps_nums: List[float] = []
            for t in tokens:
                v = coerce_number(t)
                if v is None:
                    continue
                if isinstance(v, (int, float)) and 1900 <= float(v) <= 2100 and len(str(int(v))) == 4:
                    continue
                # EPS should be a small magnitude
                if abs(float(v)) > 100:
                    continue
                eps_nums.append(float(v))
            return eps_nums

        # Prefer tokens after the adjusted-EPS label to avoid picking GAAP line numbers.
        use_ln = ln
        pos_adj = ln_low.find("adjusted")
        if pos_adj >= 0:
            use_ln = ln[pos_adj:]
        eps_nums = _extract_eps_nums(use_ln)
        if not eps_nums:
            for j in range(1, 3):
                if i + j < len(lines_3m):
                    eps_nums = _extract_eps_nums(lines_3m[i + j])
                    if eps_nums:
                        break
        if not eps_nums:
            continue
        adj_eps = _pick_number_by_year(eps_nums)
        break

    if adj_eps is None:
        in_eps_block = False
        for ln in lines_3m:
            ln_low = ln.lower()
            if "net income per diluted share" in ln_low or "earnings per share" in ln_low:
                in_eps_block = True
                continue
            if in_eps_block and re.search(r"^(adjusted\s+non-gaap|non-gaap\s+constant)", ln_low):
                eps_nums = []
                for t in re.findall(r"\(?-?\d+(?:\.\d+)?\)?", ln):
                    v = coerce_number(t)
                    if v is None:
                        continue
                    if 1900 <= float(v) <= 2100 and len(str(int(v))) == 4:
                        continue
                    if abs(float(v)) <= 100:
                        eps_nums.append(float(v))
                if eps_nums:
                    adj_eps = _pick_number_by_year(eps_nums)
                    break
            if in_eps_block and ("reconciliation of ebitda" in ln_low or "balance sheets" in ln_low):
                in_eps_block = False

    if adj_ebitda is None and adj_ebit is None and adj_eps is None:
        return None, None, None, {}, "ocr_no_metrics", None

    status = "ok_ocr" if mode == "strict" else "ok_relaxed_ocr"
    return adj_ebit, adj_ebitda, adj_eps, {}, status, "ocr"


def parse_adjusted_from_plain_text(
    txt: str,
    quarter_end: Optional[pd.Timestamp],
    mode: str = "relaxed",
) -> Tuple[Optional[float], Optional[float], Optional[float], Dict[str, float], str, Optional[str]]:
    return _parse_adjusted_from_text(txt, quarter_end, mode)


def parse_adjusted_from_ex99(
    html_bytes: bytes,
    quarter_end: Optional[pd.Timestamp],
    mode: str = "strict",
    *,
    extraction_metadata: Optional[Dict[str, Dict[str, Any]]] = None,
    adjustment_facts: Optional[List[CanonicalAdjustmentFact]] = None,
) -> Tuple[Optional[float], Optional[float], Optional[float], Dict[str, float], str, Optional[str]]:
    html = html_bytes.decode("utf-8", errors="ignore")
    html = normalize_number_spacing(html)
    if extraction_metadata is not None:
        extraction_metadata.clear()
    if adjustment_facts is not None:
        adjustment_facts.clear()
    document_unit_ambiguous = False
    try:
        document_default_unit = detect_source_amount_unit(html, default_to_ones=False)
    except SourceUnitContractError:
        document_default_unit = None
        document_unit_ambiguous = True
    if document_default_unit is None and not document_unit_ambiguous:
        document_default_unit = SourceAmountUnit(
            currency="USD",
            scale=SourceAmountScale.ONES,
            declaration="unit:USD",
        )

    tables = read_html_tables_any(html.encode("utf-8"))
    adj_ebit = None
    adj_ebitda = None
    adj_eps: Optional[float] = None
    adjustments: Dict[str, float] = {}
    adj_fcf: Optional[float] = None

    def to_amount(x: Any, source_unit: SourceAmountUnit) -> Optional[CanonicalSourceAmount]:
        v = coerce_number(x)
        if v is None:
            return None
        return normalize_source_amount(v, source_unit)

    def to_num(x: Any, source_unit: SourceAmountUnit) -> Optional[float]:
        amount = to_amount(x, source_unit)
        return None if amount is None else amount.canonical_value

    def to_num_eps(x: Any) -> Optional[float]:
        v = coerce_number(x)
        if v is None:
            return None
        return float(v)

    adjustments_keywords = [
        "restruct",
        "pension",
        "impair",
        "litigation",
        "integration",
        "foreign exchange",
        "fx",
        "refinanc",
        "gain",
        "loss",
        "other",
        "non-cash",
        "stock-based",
        "amortization",
        "depreciation",
    ]

    if quarter_end is None:
        return None, None, None, {}, "no_quarter_end", None

    def _parse_consolidated_colspan_table(
        table: pd.DataFrame,
        *,
        table_index: int,
        source_unit: SourceAmountUnit,
        unit_row_index: Optional[int],
    ) -> Optional[
        Tuple[
            Optional[float],
            Optional[float],
            Optional[float],
            Dict[str, float],
            str,
            Dict[str, Dict[str, Any]],
            List[CanonicalAdjustmentFact],
        ]
    ]:
        if table is None or table.empty:
            return None
        blob = " ".join(str(v or "") for v in table.astype(str).values.ravel()).lower()
        if "adjusted ebit" not in blob or "adjusted ebitda" not in blob:
            return None
        is_consolidated_recon = (
            "reported consolidated results" in blob
            or (
                "reported net" in blob
                and "adjusted ebit" in blob
                and "adjusted ebitda" in blob
                and "segment adjusted" not in blob
            )
        )
        if not is_consolidated_recon:
            return None

        target_year = str(int(pd.Timestamp(quarter_end).year))
        header_rows = min(10, len(table))
        ncols = int(table.shape[1])

        def _txt(v: Any) -> str:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return ""
            s = str(v)
            if s.lower() == "nan":
                return ""
            return re.sub(r"\s+", " ", s).strip()

        def _context_for_col(col_idx: int) -> str:
            vals: List[str] = []
            for r_idx in range(header_rows):
                for c_idx in (col_idx - 1, col_idx, col_idx + 1):
                    if c_idx < 0 or c_idx >= ncols:
                        continue
                    s = _txt(table.iat[r_idx, c_idx])
                    if s:
                        vals.append(s)
            return " ".join(vals).lower()

        candidates: List[Tuple[int, int]] = []
        for c_idx in range(1, ncols):
            ctx = _context_for_col(c_idx)
            if target_year not in ctx:
                continue
            period_score = 0
            if "three months" in ctx or "three month" in ctx or "3 months" in ctx:
                period_score += 30
            if "six months" in ctx or "nine months" in ctx or "year ended" in ctx or "twelve months" in ctx:
                period_score -= 20
            candidates.append((c_idx, period_score))
        if not candidates:
            for c_idx in range(1, ncols):
                ctx = _context_for_col(c_idx)
                if "three months" in ctx or "three month" in ctx or "3 months" in ctx:
                    candidates.append((c_idx, 10))
        if not candidates:
            candidates = [(c_idx, 0) for c_idx in range(1, ncols)]
        candidates = sorted(candidates, key=lambda item: item[1], reverse=True)

        def _row_label(row: pd.Series) -> str:
            labels: List[str] = []
            for c_idx in range(min(2, ncols)):
                s = _txt(row.iloc[c_idx] if c_idx < len(row) else "")
                if s:
                    labels.append(s)
            return " ".join(labels).strip().lower()

        def _value_from_row(
            row: pd.Series,
            eps: bool = False,
        ) -> Tuple[Optional[float], Optional[CanonicalSourceAmount], Optional[int]]:
            seen_cols: set[int] = set()
            for c_idx, _score in candidates:
                for probe in (c_idx, c_idx + 1, c_idx - 1):
                    if probe < 1 or probe >= ncols or probe in seen_cols:
                        continue
                    seen_cols.add(probe)
                    raw = row.iloc[probe] if probe < len(row) else None
                    amount = None if eps else to_amount(raw, source_unit)
                    val = to_num_eps(raw) if eps else (amount.canonical_value if amount is not None else None)
                    if val is not None:
                        return val, amount, probe
            return None, None, None

        found_adj_ebit: Optional[float] = None
        found_adj_ebitda: Optional[float] = None
        found_adj_eps: Optional[float] = None
        found_adjustments: Dict[str, float] = {}
        found_adjustment_facts: List[CanonicalAdjustmentFact] = []
        found_metadata: Dict[str, Dict[str, Any]] = {}
        has_recon_row = False
        section_domain: Optional[MeasureDomain] = None
        classification = classify_adjustment_table(
            table,
            document_default=document_default_unit,
        )

        for row_index, row in table.iterrows():
            label = _row_label(row)
            if not label or label == "nan":
                continue
            if "reconciliation" in label and "per share" in label:
                section_domain = MeasureDomain.PER_SHARE_AMOUNT
                has_recon_row = True
                continue
            if "reconciliation" in label and ("adjusted ebit" in label or "adjusted ebitda" in label):
                section_domain = MeasureDomain.MONETARY_AMOUNT
                has_recon_row = True
                continue
            if "reconciliation" in label and "free cash flow" in label:
                section_domain = MeasureDomain.MONETARY_AMOUNT
                has_recon_row = True
                continue
            if "adjusted segment" in label:
                continue
            if "adjusted ebitda" in label:
                found_adj_ebitda, amount, source_column_index = _value_from_row(row)
                if amount is not None and source_column_index is not None:
                    found_metadata["adj_ebitda"] = _amount_lineage_metadata(
                        amount=amount,
                        table_index=table_index,
                        row_index=int(row_index),
                        column_index=source_column_index,
                        unit_row_index=unit_row_index,
                        column_label=f"{target_year} 3M consolidated table",
                    )
                continue
            if _label_matches(label, _ADJ_EBIT_SYNONYMS) and "ebitda" not in label:
                found_adj_ebit, amount, source_column_index = _value_from_row(row)
                if amount is not None and source_column_index is not None:
                    found_metadata["adj_ebit"] = _amount_lineage_metadata(
                        amount=amount,
                        table_index=table_index,
                        row_index=int(row_index),
                        column_index=source_column_index,
                        unit_row_index=unit_row_index,
                        column_label=f"{target_year} 3M consolidated table",
                    )
                continue
            if _is_adj_eps_label(label):
                eps_val, _amount, _source_column_index = _value_from_row(row, eps=True)
                if eps_val is not None and abs(eps_val) <= 100:
                    found_adj_eps = eps_val
                continue
            if _label_matches(label, _GAAP_EBIT_SYNONYMS):
                has_recon_row = True
            if _label_matches(label, adjustments_keywords):
                if any(
                    skip in label
                    for skip in (
                        "net (loss) income",
                        "net income",
                        "income before taxes",
                        "earnings per share",
                        "diluted (loss) earnings",
                    )
                ):
                    has_recon_row = True
                    continue
                adj_val, amount, source_column_index = _value_from_row(row)
                fact: Optional[CanonicalAdjustmentFact] = None
                if amount is not None and source_column_index is not None and classification.role is not AdjustmentTableRole.UNRESOLVED:
                    fact = _build_adjustment_fact(
                        period=pd.Timestamp(quarter_end),
                        source_label=label,
                        raw_value=amount.raw_source_scalar,
                        classification=classification,
                        table_index=table_index,
                        row_index=int(row_index),
                        column_index=source_column_index,
                        column_label=f"{target_year} 3M consolidated table",
                        section_domain=section_domain,
                    )
                    if fact is not None:
                        found_adjustment_facts.append(fact)
                if (
                    fact is not None
                    and fact.measure_domain is MeasureDomain.MONETARY_AMOUNT
                ):
                    has_recon_row = True

        reconciled_facts = reconcile_adjustment_facts(found_adjustment_facts)
        for fact in reconciled_facts:
            if fact.measure_domain is MeasureDomain.MONETARY_AMOUNT:
                found_adjustments[fact.source_label] = float(fact.amount.canonical_value)

        if (found_adj_ebit is not None or found_adj_ebitda is not None or found_adj_eps is not None) and (
            has_recon_row or found_adjustments
        ):
            return (
                found_adj_ebit,
                found_adj_ebitda,
                found_adj_eps,
                found_adjustments,
                f"{target_year} 3M consolidated table",
                found_metadata,
                reconciled_facts,
            )
        return None

    unit_errors: List[str] = []
    for table_index, t in enumerate(tables):
        try:
            table_unit, unit_row_index = _source_unit_for_table(t, document_default_unit)
        except SourceUnitContractError as exc:
            unit_errors.append(f"table {table_index}: {exc}")
            continue
        if table_unit is None:
            unit_errors.append(f"table {table_index}: no unambiguous table-local amount unit")
            continue
        parsed = _parse_consolidated_colspan_table(
            t,
            table_index=table_index,
            source_unit=table_unit,
            unit_row_index=unit_row_index,
        )
        if parsed is not None:
            aebit, aebitda, aeps, adj, col_label, metric_metadata, parsed_facts = parsed
            if extraction_metadata is not None:
                extraction_metadata.update(metric_metadata)
            if adjustment_facts is not None:
                adjustment_facts.extend(parsed_facts)
            return aebit, aebitda, aeps, adj, ("ok" if mode == "strict" else "ok_relaxed"), col_label

    for table_index, t in enumerate(tables):
        if t is None or t.empty:
            continue
        try:
            classification = classify_adjustment_table(
                t,
                document_default=document_default_unit,
            )
        except SourceUnitContractError as exc:
            unit_errors.append(f"table {table_index}: {exc}")
            continue
        try:
            table_unit, unit_row_index = _source_unit_for_table(t, document_default_unit)
        except SourceUnitContractError as exc:
            unit_errors.append(f"table {table_index}: {exc}")
            continue
        if table_unit is None and classification.role is AdjustmentTableRole.EPS_RECONCILIATION:
            table_unit = classification.source_unit
            unit_row_index = classification.source_unit_row_index
        if table_unit is None:
            unit_errors.append(f"table {table_index}: no unambiguous table-local amount unit")
            continue

        t2 = t.copy()
        cols, header_row_idx, col_dates, table_hint = _find_header_dates(t2)
        if header_row_idx is not None:
            t2.columns = cols
            t2 = t2.drop(t2.index[header_row_idx]).reset_index(drop=True)
        else:
            t2.columns = cols

        col_periods = {i: _period_hint_from_text(c) for i, c in enumerate(cols)}
        if table_hint:
            for i in range(len(cols)):
                if col_periods.get(i) is None:
                    col_periods[i] = table_hint

        col_quarters: Dict[int, Tuple[int, int]] = {}
        for i, c in enumerate(cols):
            m = re.search(r"Q([1-4])\s*(20\d{2})", c, re.IGNORECASE)
            if m:
                col_quarters[i] = (int(m.group(2)), int(m.group(1)))

        header_context: Dict[int, str] = {}
        header_labels: Dict[int, str] = {}
        for i in range(len(cols)):
            parts = [str(cols[i])]
            for ridx in range(min(6, len(t2))):
                if i >= len(t2.iloc[ridx]):
                    continue
                cell = str(t2.iloc[ridx, i] or "").strip()
                if not cell or cell.lower() == "nan":
                    continue
                parts.append(cell)
                if i not in header_labels and re.search(
                    r"\bQ[1-4]\s*20\d{2}\b|\bFull\s+Year\s+20\d{2}\b",
                    cell,
                    re.IGNORECASE,
                ):
                    header_labels[i] = cell
            header_context[i] = " ".join(parts)
            if i not in col_quarters:
                m = re.search(r"\bQ([1-4])\s*(20\d{2})\b", header_context[i], re.IGNORECASE)
                if m:
                    col_quarters[i] = (int(m.group(2)), int(m.group(1)))
            if col_periods.get(i) is None:
                if re.search(r"\bfull\s+year\b|\byear\s+ended\b|\bfiscal\s+year\b", header_context[i], re.IGNORECASE):
                    col_periods[i] = "FY"
                elif re.search(r"\bQ[1-4]\s*20\d{2}\b", header_context[i], re.IGNORECASE):
                    col_periods[i] = "3M"

        col_idx = None
        col_label = None
        if mode == "strict":
            match_cols = [i for i, d in col_dates.items() if d == quarter_end] if col_dates else []
            if not match_cols and quarter_end is not None:
                q = (int(quarter_end.month) - 1) // 3 + 1
                y = int(quarter_end.year)
                match_cols = [i for i, (yy, qq) in col_quarters.items() if yy == y and qq == q]
            if match_cols:
                # Require 3M only if explicitly labeled otherwise allow unknown period
                match_cols = [i for i in match_cols if col_periods.get(i) in (None, "3M")]
            if not match_cols:
                continue
            col_idx = match_cols[0]
            col_label = cols[col_idx]
        else:
            candidate_cols = [i for i in range(1, len(cols))]
            if col_dates:
                match_cols = [i for i, d in col_dates.items() if d == quarter_end]
                if not match_cols and quarter_end is not None:
                    q = (int(quarter_end.month) - 1) // 3 + 1
                    y = int(quarter_end.year)
                    match_cols = [i for i, (yy, qq) in col_quarters.items() if yy == y and qq == q]
                if match_cols:
                    candidate_cols = match_cols
            if quarter_end is not None:
                q = (int(quarter_end.month) - 1) // 3 + 1
                y = int(quarter_end.year)
                quarter_match = [
                    i
                    for i in candidate_cols
                    if col_quarters.get(i) == (y, q)
                    and col_periods.get(i) in (None, "3M")
                ]
                if quarter_match:
                    candidate_cols = quarter_match
                year_match = [i for i in candidate_cols if str(y) in header_context.get(i, str(cols[i]))]
                if year_match:
                    candidate_cols = year_match
                three_month_match = [i for i in candidate_cols if col_periods.get(i) in (None, "3M")]
                if three_month_match:
                    candidate_cols = three_month_match

            def _score_col(i: int) -> Tuple[int, float]:
                nums = []
                for _, row in t2.iterrows():
                    v = to_num(row.iloc[i], table_unit) if i < len(row) else None
                    if v is not None:
                        nums.append(abs(v))
                if not nums:
                    return (0, 0.0)
                nums.sort()
                return (len(nums), nums[len(nums) // 2])

            scored = [(i,) + _score_col(i) for i in candidate_cols]
            scored = [s for s in scored if s[1] > 0]
            if not scored:
                continue
            scored.sort(key=lambda x: (x[1], x[2]), reverse=True)
            col_idx = scored[0][0]
            col_label = header_labels.get(col_idx, cols[col_idx])

        first = t2.columns[0]
        t2[first] = t2[first].astype(str)

        has_adjusted = False
        has_recon = False
        has_eps = False
        has_adj_eps = False
        table_metric_metadata: Dict[str, Dict[str, Any]] = {}
        table_adjustment_facts: List[CanonicalAdjustmentFact] = []
        section_domain: Optional[MeasureDomain] = None

        for row_index, row in t2.iterrows():
            label = str(row.get(first, "")).strip().lower()
            if not label or label == "nan":
                continue
            if "reconciliation" in label and "per share" in label:
                section_domain = MeasureDomain.PER_SHARE_AMOUNT
            elif "reconciliation" in label and (
                "adjusted ebit" in label
                or "adjusted ebitda" in label
                or "free cash flow" in label
            ):
                section_domain = MeasureDomain.MONETARY_AMOUNT
            amount = to_amount(row.iloc[col_idx], table_unit) if col_idx < len(row) else None
            v = amount.canonical_value if amount is not None else None
            if v is None:
                # still allow EPS parse without scale
                v_eps = to_num_eps(row.iloc[col_idx]) if col_idx < len(row) else None
            else:
                v_eps = to_num_eps(row.iloc[col_idx]) if col_idx < len(row) else None

            is_margin_row = "margin" in label or label.rstrip().endswith("%")

            if "adjusted ebitda" in label and not is_margin_row and v is not None:
                adj_ebitda = v
                has_adjusted = True
                if amount is not None:
                    table_metric_metadata["adj_ebitda"] = _amount_lineage_metadata(
                        amount=amount,
                        table_index=table_index,
                        row_index=int(row_index),
                        column_index=col_idx,
                        unit_row_index=unit_row_index,
                        column_label=col_label,
                    )
            if (
                _label_matches(label, _ADJ_EBIT_SYNONYMS)
                and "ebitda" not in label
                and not is_margin_row
                and v is not None
            ):
                adj_ebit = v
                has_adjusted = True
                if amount is not None:
                    table_metric_metadata["adj_ebit"] = _amount_lineage_metadata(
                        amount=amount,
                        table_index=table_index,
                        row_index=int(row_index),
                        column_index=col_idx,
                        unit_row_index=unit_row_index,
                        column_label=col_label,
                    )
            if _is_eps_label(label):
                has_eps = True
            if _is_adj_eps_label(label):
                if v_eps is not None and abs(v_eps) <= 100:
                    adj_eps = v_eps
                    has_adj_eps = True

            if "free cash flow" in label and not is_margin_row and v is not None:
                adj_fcf = v
                if amount is not None:
                    table_metric_metadata["adj_fcf"] = _amount_lineage_metadata(
                        amount=amount,
                        table_index=table_index,
                        row_index=int(row_index),
                        column_index=col_idx,
                        unit_row_index=unit_row_index,
                        column_label=col_label,
                    )
            if _label_matches(label, adjustments_keywords) and v is not None:
                if classification.role is AdjustmentTableRole.UNRESOLVED:
                    unit_errors.append(
                        f"table {table_index}: adjustment row {label!r} has unresolved table role "
                        f"({classification.evidence})"
                    )
                else:
                    fact = _build_adjustment_fact(
                        period=pd.Timestamp(quarter_end),
                        source_label=label,
                        raw_value=row.iloc[col_idx],
                        classification=classification,
                        table_index=table_index,
                        row_index=int(row_index),
                        column_index=int(col_idx),
                        column_label=str(col_label or ""),
                        section_domain=section_domain,
                    )
                    if fact is not None:
                        table_adjustment_facts.append(fact)
                        has_recon = True

            if _label_matches(label, _GAAP_EBIT_SYNONYMS):
                has_recon = True

        if (has_adjusted and has_recon and (adj_ebit is not None or adj_ebitda is not None)) or (has_adj_eps and has_eps) or (adj_fcf is not None):
            reconciled_facts = reconcile_adjustment_facts(table_adjustment_facts)
            table_adjustments = {
                fact.source_label: float(fact.amount.canonical_value)
                for fact in reconciled_facts
                if fact.measure_domain is MeasureDomain.MONETARY_AMOUNT
            }
            if adj_fcf is not None:
                table_adjustments["__adj_fcf"] = adj_fcf
            if extraction_metadata is not None:
                extraction_metadata.update(table_metric_metadata)
            if adjustment_facts is not None:
                adjustment_facts.extend(reconciled_facts)
            return adj_ebit, adj_ebitda, adj_eps, table_adjustments, ("ok" if mode == "strict" else "ok_relaxed"), col_label

    status = "ambiguous_source_unit" if unit_errors and document_unit_ambiguous else "no_matching_column"
    return None, None, None, {}, status, None


def build_non_gaap_tier3(
    sec: Any,
    cik_int: int,
    submissions: Dict[str, Any],
    max_quarters: int,
    mode: str = "strict",
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    recent = submissions.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    accns = recent.get("accessionNumber", [])
    filing_dates = recent.get("filingDate", []) or []
    report_dates = recent.get("reportDate", []) or []

    rows_m: List[Dict[str, Any]] = []
    rows_b: List[Dict[str, Any]] = []
    rows_f: List[Dict[str, Any]] = []

    n = min(len(forms), len(accns))
    for i in range(n):
        form = forms[i]
        accn = accns[i]
        fdate = filing_dates[i] if i < len(filing_dates) else None
        rdate = report_dates[i] if i < len(report_dates) else None

        if form != "8-K":
            continue

        accn_nd = normalize_accession(accn)
        try:
            idx = sec.accession_index_json(cik_int, accn_nd)
        except Exception:
            continue

        exdocs = find_ex99_docs(idx)
        if not exdocs:
            rows_f.append({"accn": accn, "filed": fdate, "status": "no_ex99"})
            continue

        picked = None
        q_end = None
        picked_metadata: Dict[str, Dict[str, Any]] = {}
        picked_adjustment_facts: List[CanonicalAdjustmentFact] = []

        for fn in exdocs[:8]:
            try:
                b = sec.download_document(cik_int, accn_nd, fn)
            except Exception:
                continue
            try:
                sec.download_html_assets(cik_int, accn_nd, b)
            except Exception:
                pass
            try:
                sec.download_index_images(cik_int, accn_nd, idx)
            except Exception:
                pass

            txt = strip_html(b.decode("utf-8", errors="ignore"))
            q_end = infer_quarter_end_from_text(txt) or parse_date(rdate) or parse_date(fdate)

            parse_metadata: Dict[str, Dict[str, Any]] = {}
            parse_adjustment_facts: List[CanonicalAdjustmentFact] = []
            aebit, aebitda, aeps, adj, status, col_label = parse_adjusted_from_ex99(
                b,
                q_end,
                mode=mode,
                extraction_metadata=parse_metadata,
                adjustment_facts=parse_adjustment_facts,
            )
            if status not in ("ok", "ok_relaxed"):
                try:
                    ocr_txt = sec.ocr_html_assets(
                        accn_nd,
                        b,
                        context={"doc": fn, "quarter": q_end, "purpose": "non_gaap_ocr", "report_date": rdate, "filing_date": fdate, "save_text": True},
                    )
                except Exception:
                    ocr_txt = ""
                if ocr_txt:
                    aebit, aebitda, aeps, adj, status, col_label = _parse_adjusted_from_text(ocr_txt, q_end, mode=mode)
                    parse_metadata = {}
                    parse_adjustment_facts = []
                if status not in ("ok", "ok_relaxed", "ok_ocr", "ok_relaxed_ocr"):
                    rows_f.append({"accn": accn, "filed": fdate, "status": status, "doc": fn})
                    continue

            picked = (fn, aebit, aebitda, aeps, adj, col_label)
            picked_metadata = parse_metadata
            picked_adjustment_facts = list(parse_adjustment_facts)
            break

        if picked is None:
            rows_f.append({"accn": accn, "filed": fdate, "status": "ex99_no_metrics"})
            continue

        fn, aebit, aebitda, aeps, adj, col_label = picked
        adj_fcf = None
        if isinstance(adj, dict) and "__adj_fcf" in adj:
            adj_fcf = adj.pop("__adj_fcf", None)
        if q_end is None:
            q_end = parse_date(rdate) or parse_date(fdate)

        source_document_id = build_identity(
            "sec-document",
            (
                ("cik", f"{int(cik_int):010d}"),
                ("accn", str(accn)),
                ("doc", str(fn)),
            ),
        )
        metric_lineage_columns: Dict[str, Any] = {
            "source_lineage_contract": ADJUSTED_METRIC_SOURCE_UNIT_CONTRACT,
            "adjusted_metric_history_contract": ADJUSTED_METRIC_HISTORY_CONTRACT,
            "adjustment_domain_contract": NON_GAAP_ADJUSTMENT_DOMAIN_CONTRACT,
            "adjustment_domain_version": NON_GAAP_ADJUSTMENT_DOMAIN_VERSION,
            "source_document_id": source_document_id,
        }
        for metric_name, metadata in sorted(picked_metadata.items()):
            if metric_name not in {"adj_ebit", "adj_ebitda", "adj_fcf"}:
                continue
            occurrence_id = build_identity(
                "non-gaap-occurrence",
                (
                    ("doc", source_document_id),
                    ("metric", metric_name),
                    ("period", str(q_end)),
                    ("locator", str(metadata.get("source_locator") or "")),
                ),
            )
            for field_name, field_value in metadata.items():
                metric_lineage_columns[f"{metric_name}_{field_name}"] = field_value
            metric_id = AdjustedMetricId(metric_name)
            metric_lineage_columns.update(
                {
                    f"{metric_name}_metric_id": metric_id.value,
                    f"{metric_name}_period_type": AdjustedMetricPeriodType.QUARTER.value,
                    f"{metric_name}_basis": "adjusted_non_gaap",
                    f"{metric_name}_scope": AdjustedMetricScope.REPORTED_CONSOLIDATED.value,
                    f"{metric_name}_definition_id": reported_adjusted_metric_definition_id(metric_id),
                    f"{metric_name}_source_role": AdjustedMetricSourceRole.DIRECT.value,
                    f"{metric_name}_source_authority": "issuer_direct_period_release",
                    f"{metric_name}_authority_rank": 300 if mode == "strict" else 250,
                    f"{metric_name}_source_document_id": source_document_id,
                    f"{metric_name}_source_metric_label": {
                        AdjustedMetricId.ADJUSTED_EBIT: "Adjusted EBIT",
                        AdjustedMetricId.ADJUSTED_EBITDA: "Adjusted EBITDA",
                        AdjustedMetricId.ADJUSTED_FCF: "Adjusted FCF",
                    }[metric_id],
                }
            )
            metric_lineage_columns[f"{metric_name}_source_occurrence_id"] = occurrence_id

        adjustment_records: List[Dict[str, Any]] = []
        for fact in sorted(
            picked_adjustment_facts,
            key=lambda item: (item.semantic_key, item.source_locator),
        ):
            record = fact.to_lineage_record()
            occurrence_id = build_identity(
                "non-gaap-adjustment-occurrence",
                (
                    ("doc", source_document_id),
                    ("period", pd.Timestamp(fact.period).date().isoformat()),
                    ("metric", fact.metric_id),
                    ("domain", fact.measure_domain.value),
                    ("locator", fact.source_locator),
                ),
            )
            economic_id = build_identity(
                "non-gaap-adjustment-fact",
                (
                    ("period", pd.Timestamp(fact.period).date().isoformat()),
                    ("metric", fact.metric_id),
                    ("domain", fact.measure_domain.value),
                    ("basis", fact.basis),
                    ("scope", fact.scope),
                    ("definition", fact.definition_id),
                ),
            )
            record.update(
                {
                    "source_document_id": source_document_id,
                    "source_occurrence_id": occurrence_id,
                    "economic_fact_id": economic_id,
                }
            )
            adjustment_records.append(record)

        evidence_json = json.dumps(
            adjustment_records,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )

        rows_m.append({
            "quarter": q_end,
            "adj_ebit": aebit,
            "adj_ebitda": aebitda,
            "adj_eps": aeps,
            "adj_fcf": adj_fcf,
            "source": "ex99",
            "accn": accn,
            "filed": parse_date(fdate),
            "doc": fn,
            "confidence": "low" if mode == "relaxed" else "high",
            "col": col_label,
            "period_type": AdjustedMetricPeriodType.QUARTER.value,
            "adjustment_evidence_json": evidence_json,
            "monetary_adjustment_fact_count": sum(
                record["measure_domain"] == MeasureDomain.MONETARY_AMOUNT.value
                for record in adjustment_records
            ),
            "per_share_adjustment_fact_count": sum(
                record["measure_domain"] == MeasureDomain.PER_SHARE_AMOUNT.value
                for record in adjustment_records
            ),
            **metric_lineage_columns,
        })

        amount_projection = {
            str(lab): float(val)
            for lab, val in adj.items()
            if not str(lab).startswith("__")
        }
        for record in adjustment_records:
            if record["measure_domain"] != MeasureDomain.MONETARY_AMOUNT.value:
                continue
            lab = str(record["source_label"])
            if lab not in amount_projection:
                continue
            val = float(record["canonical_value"])
            if val != amount_projection[lab]:
                raise SourceUnitContractError(
                    f"Adjustment amount projection disagrees with canonical fact for {lab!r}: "
                    f"{amount_projection[lab]!r} vs {val!r}."
                )
            rows_b.append({
                "quarter": q_end,
                "label": lab,
                "value": val,
                "source": "ex99",
                "accn": accn,
                "doc": fn,
                "confidence": "low" if mode == "relaxed" else "high",
                "col": col_label,
                **record,
            })

        rows_f.append({
            "accn": accn,
            "filed": fdate,
            "status": "ok" if mode == "strict" else "ok_relaxed",
            "doc": fn,
            "quarter": str(q_end),
            "col": col_label,
        })

    m = pd.DataFrame(rows_m)
    b = reconcile_adjustment_breakdown_frame(pd.DataFrame(rows_b))
    f = pd.DataFrame(rows_f)

    if not m.empty:
        m = m.sort_values("quarter").drop_duplicates(subset=["quarter"], keep="last")
        qs = sorted(m["quarter"].unique())[-max_quarters:]
        m = m[m["quarter"].isin(qs)].copy()
        if not b.empty:
            b = b[b["quarter"].isin(qs)].copy()

    return m, b, f
