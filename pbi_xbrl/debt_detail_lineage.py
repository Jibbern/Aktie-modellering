"""Typed lineage disposition for rows entering Valuation Debt Detail."""
from __future__ import annotations

from enum import Enum
from typing import Any, Mapping

import pandas as pd


DEBT_DETAIL_LINEAGE_DISPOSITION_COLUMN = "debt_lineage_disposition"
DEBT_DETAIL_SOURCE_LINEAGE_CONTRACT_ID = "contract:debt-detail-source-lineage@1"
FINANCIAL_STATEMENT_DEBT_ROW_CONTRACT_ID = "contract:financial-statement-debt-table-row@1"


class DebtDetailLineageDisposition(str, Enum):
    """Whether a projected row owns, fails, or does not claim typed source lineage."""

    VALID = "VALID"
    INVALID = "INVALID"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class DebtDetailLineageContractError(ValueError):
    """Raised when nullable or untyped lineage state reaches the render contract."""


def _present(value: Any) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass
    return bool(str(value).strip())


def _scalar_text(value: Any) -> str:
    return str(value).strip() if _present(value) else ""


def _missing_scalar(value: Any) -> bool:
    if value is None:
        return True
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        return False
    return isinstance(missing, bool) and missing


def row_claims_source_backed_debt_lineage(row: Mapping[str, Any]) -> bool:
    return (
        _scalar_text(row.get("lineage_contract_id"))
        == DEBT_DETAIL_SOURCE_LINEAGE_CONTRACT_ID
        or _scalar_text(row.get("source_kind")) == "Slides_Debt_Profile"
    )


def source_backed_debt_lineage_is_complete(row: Mapping[str, Any]) -> bool:
    required = (
        "economic_id",
        "source_document_id",
        "source_occurrence_id",
        "source_document_name",
        "source_document_sha256",
        "source_locator",
        "reporting_date",
        "metric_id",
        "unit_id",
        "currency",
        "value_status",
    )
    if not all(_present(row.get(name)) for name in required):
        return False
    if _scalar_text(row.get("value_status")) not in {"direct_source", "derived"}:
        return False
    if _scalar_text(row.get("value_status")) == "derived":
        if not _present(row.get("derivation_rule_id")):
            return False
        inputs = row.get("derivation_input_ids")
        if not isinstance(inputs, tuple) or not inputs or not all(_present(item) for item in inputs):
            return False
    if (
        _scalar_text(row.get("source_row_contract_id"))
        == FINANCIAL_STATEMENT_DEBT_ROW_CONTRACT_ID
    ):
        typed_required = (
            "source_page",
            "source_table_id",
            "source_context_id",
            "source_member",
            "source_fact_id",
            "issuer_instrument_label",
        )
        if not all(_present(row.get(name)) for name in typed_required):
            return False
        try:
            if int(row.get("source_page")) <= 0:
                return False
        except (TypeError, ValueError):
            return False
    return True


def normalize_debt_detail_lineage_dispositions(frame: Any) -> pd.DataFrame:
    """Normalize mixed source-backed and independently owned rows before rendering."""

    if frame is None or not isinstance(frame, pd.DataFrame):
        return pd.DataFrame()
    out = frame.copy(deep=True)
    if out.empty:
        out[DEBT_DETAIL_LINEAGE_DISPOSITION_COLUMN] = pd.Series(dtype=object)
        return out
    normalized: list[DebtDetailLineageDisposition] = []
    for index, row in out.iterrows():
        raw = row.get(DEBT_DETAIL_LINEAGE_DISPOSITION_COLUMN)
        if isinstance(raw, DebtDetailLineageDisposition):
            disposition = raw
        elif isinstance(raw, str) and raw in {
            item.value for item in DebtDetailLineageDisposition
        }:
            # Pandas may serialize a str-backed Enum during masked assignment;
            # normalize the exact versioned values back to their typed owner.
            disposition = DebtDetailLineageDisposition(raw)
        elif _missing_scalar(raw):
            if row_claims_source_backed_debt_lineage(row):
                disposition = (
                    DebtDetailLineageDisposition.VALID
                    if source_backed_debt_lineage_is_complete(row)
                    else DebtDetailLineageDisposition.INVALID
                )
            else:
                disposition = DebtDetailLineageDisposition.NOT_APPLICABLE
        else:
            identity = (
                _scalar_text(row.get("economic_id"))
                or _scalar_text(row.get("tranche_name"))
                or str(index)
            )
            raise DebtDetailLineageContractError(
                "Debt-detail projection supplied an untyped lineage disposition for "
                f"{identity!r}: {raw!r}; expected one of "
                f"{tuple(item.value for item in DebtDetailLineageDisposition)!r}."
            )
        if disposition is DebtDetailLineageDisposition.VALID and not source_backed_debt_lineage_is_complete(row):
            disposition = DebtDetailLineageDisposition.INVALID
        normalized.append(disposition)
    out[DEBT_DETAIL_LINEAGE_DISPOSITION_COLUMN] = pd.Series(
        normalized,
        index=out.index,
        dtype=object,
    )
    if "source_backed_lineage_valid" in out.columns:
        out = out.drop(columns=["source_backed_lineage_valid"])
    return out


def require_debt_detail_lineage_disposition(
    row: Mapping[str, Any],
    *,
    row_identity: Any = None,
) -> DebtDetailLineageDisposition:
    """Consume a normalized state without pandas/Python truthiness coercion."""

    raw = row.get(DEBT_DETAIL_LINEAGE_DISPOSITION_COLUMN)
    if isinstance(raw, DebtDetailLineageDisposition):
        return raw
    identity = (
        _scalar_text(row_identity)
        or _scalar_text(row.get("economic_id"))
        or _scalar_text(row.get("tranche_name"))
        or "unknown-debt-row"
    )
    raise DebtDetailLineageContractError(
        f"Debt-detail renderer received invalid lineage disposition for {identity!r}: "
        f"{raw!r}; expected one of "
        f"{tuple(item.value for item in DebtDetailLineageDisposition)!r}."
    )
