"""Ticker-neutral source-native Summary and balance-sheet/segment products.

The contracts in this module stop at product ownership.  Workbook coordinates are
deliberately absent: a later projection may bind fields to a presentation surface,
but cannot become the owner of the economics represented here.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, DivisionByZero, InvalidOperation
from types import MappingProxyType
from typing import Any, Iterable, Literal, Mapping, Sequence

from .identity import build_identity, canonical_company_id
from .types import canonical_decimal


SUMMARY_PRODUCT_TYPE = "SourceNativeSummaryProduct@1"
BS_SEGMENT_PRODUCT_TYPE = "SourceNativeBSSegmentProduct@1"
SUMMARY_SHADOW_TYPE = "SourceNativeSummaryShadow@1"
BS_SEGMENT_SHADOW_TYPE = "SourceNativeBSSegmentShadow@1"
PRODUCT_CONTRACT_VERSION = "1.0.0-candidate"
SUMMARY_TEMPORAL_CONTRACT_ID = "contract:summary-temporal-semantics@1"
BS_SEGMENT_TEMPORAL_CONTRACT_ID = "contract:bs-segment-temporal-recast@1"
ZERO_MISSING_CONTRACT_ID = "contract:economic-zero-missing@1"
DERIVATION_CONTRACT_ID = "contract:source-native-derivations@1"
BALANCE_IDENTITY_CONTRACT_ID = "contract:balance-sheet-identity@1"
SEGMENT_COMPARABILITY_CONTRACT_ID = "contract:segment-comparability@1"


ProductStatus = Literal["available", "unavailable", "needs_review", "not_applicable"]
ValueState = Literal["present", "explicit_zero", "derived_zero", "missing", "not_applicable"]


class ProductContractError(ValueError):
    """Raised when a source-native product would lose economic meaning."""


def _freeze_mapping(value: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
    if value is None:
        return None
    return MappingProxyType(dict(value))


def exact_value(value: str | int | Decimal) -> dict[str, str]:
    return {"kind": "exact", "value": canonical_decimal(value)}


def qualitative_value(text: str) -> dict[str, str]:
    if not str(text).strip():
        raise ProductContractError("Qualitative values must be non-empty.")
    return {"kind": "qualitative", "text": str(text)}


def date_value(value: str) -> dict[str, str]:
    text = str(value)
    if len(text) != 10 or text[4] != "-" or text[7] != "-":
        raise ProductContractError(f"Expected ISO date, received {value!r}.")
    return {"kind": "date", "value": text}


def numeric_from_value(value: Mapping[str, Any]) -> Decimal:
    if value.get("kind") != "exact":
        raise ProductContractError("Only exact numeric values may enter exact derivations.")
    try:
        result = Decimal(str(value["value"]))
    except (InvalidOperation, KeyError) as exc:
        raise ProductContractError(f"Invalid exact value {value!r}.") from exc
    if not result.is_finite():
        raise ProductContractError("Derivation inputs must be finite.")
    return result


@dataclass(frozen=True)
class ProductField:
    """One immutable economic field, independent of workbook placement."""

    field_id: str
    metric_key: str
    metric_id: str
    period_id: str
    temporal_role: str
    semantic_role: str
    unit_id: str
    currency: str | None
    definition_id: str
    basis_id: str
    scope_id: str
    dimension_set_id: str
    status: ProductStatus
    value_state: ValueState
    directness: str
    value: Mapping[str, Any] | None = None
    canonical_fact_id: str | None = None
    derivation_id: str | None = None
    reason: str | None = None
    candidate_fact_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", _freeze_mapping(self.value))
        object.__setattr__(self, "candidate_fact_ids", tuple(self.candidate_fact_ids))
        if self.status == "available":
            if self.value is None or not self.canonical_fact_id:
                raise ProductContractError(
                    f"Available field {self.field_id} requires a value and canonical fact."
                )
            if self.value_state in {"missing", "not_applicable"}:
                raise ProductContractError(f"Available field {self.field_id} cannot be missing.")
        else:
            if self.value is not None:
                raise ProductContractError(
                    f"Non-available field {self.field_id} cannot publish an economic value."
                )
            expected_state = "not_applicable" if self.status == "not_applicable" else "missing"
            if self.value_state != expected_state:
                raise ProductContractError(
                    f"Field {self.field_id} has status {self.status!r} but state {self.value_state!r}."
                )
            if not self.reason:
                raise ProductContractError(f"Non-available field {self.field_id} requires a reason.")
        if self.value_state in {"explicit_zero", "derived_zero"}:
            if self.value is None or numeric_from_value(self.value) != 0:
                raise ProductContractError(f"Zero state for {self.field_id} requires exact numeric zero.")
        if self.value_state == "explicit_zero" and self.directness != "direct":
            raise ProductContractError("Explicit zero must be direct source evidence.")
        if self.value_state == "derived_zero" and self.directness != "derived":
            raise ProductContractError("Derived zero must be produced by a derivation.")
        if self.directness == "derived" and self.status == "available" and not self.derivation_id:
            raise ProductContractError(f"Derived field {self.field_id} requires a derivation ID.")
        if "!" in self.field_id or "cell=" in self.field_id.casefold():
            raise ProductContractError("Workbook coordinates cannot be economic field identities.")

    def to_dict(self) -> dict[str, Any]:
        return {
            "basis_id": self.basis_id,
            "candidate_fact_ids": list(self.candidate_fact_ids),
            "canonical_fact_id": self.canonical_fact_id,
            "currency": self.currency,
            "definition_id": self.definition_id,
            "derivation_id": self.derivation_id,
            "dimension_set_id": self.dimension_set_id,
            "directness": self.directness,
            "field_id": self.field_id,
            "metric_id": self.metric_id,
            "metric_key": self.metric_key,
            "period_id": self.period_id,
            "reason": self.reason,
            "scope_id": self.scope_id,
            "semantic_role": self.semantic_role,
            "status": self.status,
            "temporal_role": self.temporal_role,
            "unit_id": self.unit_id,
            "value": dict(self.value) if self.value is not None else None,
            "value_state": self.value_state,
        }


@dataclass(frozen=True)
class SourceNativeProduct:
    """Immutable field collection with an explicit presentation temporal contract."""

    product_type: str
    product_version: str
    product_id: str
    company_id: str
    temporal_contract_id: str
    fields: tuple[ProductField, ...]
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "company_id", canonical_company_id(self.company_id))
        object.__setattr__(self, "fields", tuple(self.fields))
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))
        validate_product(self)

    def to_dict(self) -> dict[str, Any]:
        return {
            "company_id": self.company_id,
            "fields": [row.to_dict() for row in self.fields],
            "metadata": dict(self.metadata),
            "product_id": self.product_id,
            "product_type": self.product_type,
            "product_version": self.product_version,
            "temporal_contract_id": self.temporal_contract_id,
        }


def product_field_identity(
    *,
    company_id: str,
    product_surface: str,
    metric_id: str,
    period_id: str,
    dimension_set_id: str,
    semantic_role: str,
) -> str:
    return build_identity(
        "product-field",
        (
            ("co", canonical_company_id(company_id)),
            ("surface", product_surface),
            ("metric", metric_id),
            ("period", period_id),
            ("dims", dimension_set_id),
            ("role", semantic_role),
        ),
    )


def canonical_fact_identity(
    *,
    metric_id: str,
    definition_id: str,
    basis_id: str,
    period_id: str,
    dimension_set_id: str,
    unit_id: str,
    currency: str | None,
) -> str:
    return build_identity(
        "canonical-fact",
        (
            ("metric", metric_id),
            ("definition", definition_id),
            ("basis", basis_id),
            ("period", period_id),
            ("dims", dimension_set_id),
            ("unit", unit_id),
            ("ccy", currency or "na"),
        ),
    )


def derivation_identity(
    *, rule_id: str, output_fact_id: str, input_fact_ids: Iterable[str]
) -> str:
    inputs = ";".join(sorted(set(input_fact_ids)))
    if not inputs:
        raise ProductContractError("A derivation requires explicit input fact IDs.")
    return build_identity(
        "derived-fact",
        (("rule", rule_id), ("output", output_fact_id), ("inputs", inputs)),
    )


DERIVATION_ARITY: Mapping[str, tuple[int, int | None]] = MappingProxyType(
    {
        "derivation:financial:sum@1": (1, None),
        "derivation:financial:subtract@1": (2, 2),
        "derivation:financial:ratio@1": (2, 2),
        "derivation:financial:growth@1": (2, 2),
        "derivation:financial:percentage-point-difference@1": (2, 2),
        "derivation:financial:q4-fy-minus-ytd@1": (2, 2),
        "derivation:financial:ttm-four-quarter-sum@1": (4, 4),
        "derivation:financial:ttm-fy-minus-prior-q1-plus-current-q1@1": (3, 3),
        "derivation:financial:segment-sum@1": (2, None),
        "derivation:financial:balance-sheet-identity@1": (3, 3),
    }
)


def evaluate_derivation(rule_id: str, inputs: Sequence[str | int | Decimal]) -> str:
    """Evaluate one exact derivation without missing-to-zero coercion."""

    if rule_id not in DERIVATION_ARITY:
        raise ProductContractError(f"Unsupported derivation rule {rule_id!r}.")
    minimum, maximum = DERIVATION_ARITY[rule_id]
    if len(inputs) < minimum or (maximum is not None and len(inputs) > maximum):
        raise ProductContractError(
            f"Derivation {rule_id} received {len(inputs)} inputs; expected {minimum}"
            + (f"..{maximum}" if maximum != minimum else "")
            + "."
        )
    if any(value is None for value in inputs):
        raise ProductContractError("Missing derivation inputs must remain missing, never zero.")
    try:
        values = [Decimal(str(value)) for value in inputs]
    except (InvalidOperation, ValueError) as exc:
        raise ProductContractError(f"Derivation inputs must be exact decimals: {inputs!r}.") from exc
    if not all(value.is_finite() for value in values):
        raise ProductContractError("Derivation inputs must be finite.")

    if rule_id in {
        "derivation:financial:sum@1",
        "derivation:financial:ttm-four-quarter-sum@1",
        "derivation:financial:segment-sum@1",
    }:
        result = sum(values, Decimal(0))
    elif rule_id in {
        "derivation:financial:subtract@1",
        "derivation:financial:percentage-point-difference@1",
        "derivation:financial:q4-fy-minus-ytd@1",
    }:
        result = values[0] - values[1]
    elif rule_id == "derivation:financial:ratio@1":
        if values[1] == 0:
            raise ProductContractError("A ratio denominator cannot be zero.")
        result = values[0] / values[1]
    elif rule_id == "derivation:financial:growth@1":
        if values[1] == 0:
            raise ProductContractError("A growth denominator cannot be zero.")
        result = values[0] / values[1] - Decimal(1)
    elif rule_id == "derivation:financial:ttm-fy-minus-prior-q1-plus-current-q1@1":
        result = values[0] - values[1] + values[2]
    elif rule_id == "derivation:financial:balance-sheet-identity@1":
        result = values[0] - values[1] - values[2]
    else:  # pragma: no cover - exhaustive contract guard
        raise ProductContractError(f"No evaluator for {rule_id!r}.")
    return canonical_decimal(format(result, "f"))


def value_state_for(*, status: ProductStatus, directness: str, value: Mapping[str, Any] | None) -> ValueState:
    if status == "not_applicable":
        return "not_applicable"
    if status != "available":
        return "missing"
    if value is not None and value.get("kind") == "exact" and numeric_from_value(value) == 0:
        return "derived_zero" if directness == "derived" else "explicit_zero"
    return "present"


def validate_product(product: SourceNativeProduct) -> None:
    if product.product_type not in {SUMMARY_PRODUCT_TYPE, BS_SEGMENT_PRODUCT_TYPE}:
        raise ProductContractError(f"Unsupported product type {product.product_type!r}.")
    if product.product_version != PRODUCT_CONTRACT_VERSION:
        raise ProductContractError(f"Unsupported product version {product.product_version!r}.")
    ids = [row.field_id for row in product.fields]
    if len(ids) != len(set(ids)):
        raise ProductContractError("Product field identities must be unique.")
    canonical = sorted(
        product.fields,
        key=lambda row: (row.period_id, row.metric_id, row.dimension_set_id, row.semantic_role),
    )
    if list(product.fields) != canonical:
        raise ProductContractError("Product fields must use canonical semantic ordering.")
    if any("workbook" in key.casefold() and key != "workbook_binding_status" for key in product.metadata):
        raise ProductContractError("Product metadata cannot make a workbook the economic owner.")


def validate_balance_sheet_identity(
    *, assets: str, liabilities: str, equity_including_nci: str, parent_equity: str, nci: str
) -> dict[str, str | bool]:
    parent_plus_nci = evaluate_derivation(
        "derivation:financial:sum@1", (parent_equity, nci)
    )
    if Decimal(parent_plus_nci) != Decimal(equity_including_nci):
        raise ProductContractError("Parent equity plus NCI does not equal total equity.")
    liabilities_and_equity = evaluate_derivation(
        "derivation:financial:sum@1", (liabilities, equity_including_nci)
    )
    residual = evaluate_derivation(
        "derivation:financial:subtract@1", (assets, liabilities_and_equity)
    )
    return {
        "assets": canonical_decimal(assets),
        "equity_attributable_to_parent": canonical_decimal(parent_equity),
        "equity_including_nci": canonical_decimal(equity_including_nci),
        "liabilities": canonical_decimal(liabilities),
        "liabilities_and_equity": liabilities_and_equity,
        "nci": canonical_decimal(nci),
        "passed": abs(Decimal(residual)) <= Decimal("0.000000001"),
        "residual": residual,
    }


def validate_segment_sum(
    *, components: Sequence[str], total: str, tolerance: str
) -> dict[str, str | bool]:
    component_sum = evaluate_derivation("derivation:financial:segment-sum@1", components)
    residual = evaluate_derivation("derivation:financial:subtract@1", (component_sum, total))
    return {
        "component_sum": component_sum,
        "passed": abs(Decimal(residual)) <= Decimal(tolerance),
        "residual": residual,
        "source_rounding_tolerance": canonical_decimal(tolerance),
        "total": canonical_decimal(total),
    }


__all__ = [
    "BALANCE_IDENTITY_CONTRACT_ID",
    "BS_SEGMENT_PRODUCT_TYPE",
    "BS_SEGMENT_SHADOW_TYPE",
    "BS_SEGMENT_TEMPORAL_CONTRACT_ID",
    "DERIVATION_CONTRACT_ID",
    "PRODUCT_CONTRACT_VERSION",
    "ProductContractError",
    "ProductField",
    "SEGMENT_COMPARABILITY_CONTRACT_ID",
    "SUMMARY_PRODUCT_TYPE",
    "SUMMARY_SHADOW_TYPE",
    "SUMMARY_TEMPORAL_CONTRACT_ID",
    "SourceNativeProduct",
    "ZERO_MISSING_CONTRACT_ID",
    "canonical_fact_identity",
    "date_value",
    "derivation_identity",
    "evaluate_derivation",
    "exact_value",
    "product_field_identity",
    "qualitative_value",
    "validate_balance_sheet_identity",
    "validate_product",
    "validate_segment_sum",
    "value_state_for",
]
