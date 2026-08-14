from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import pandas as pd

from .new_ticker_debt_scope import (
    DEBT_PROFILE_ECONOMIC_VALIDATION_ATTR,
    DEBT_PROFILE_ECONOMIC_VALIDATION_CONTRACT,
    DebtProfileEconomicValidationResult,
)
from .workbook_modules import load_workbook_module_manifest


DEBT_MODULE_ID = "debt_liquidity"
DEBT_PROFILE_SHEET = "Debt_Profile"
REVOLVER_HISTORY_SHEET = "Revolver_History"
LEVERAGE_LIQUIDITY_SHEET = "Leverage_Liquidity"
DEBT_CREDIT_NOTES_SHEET = "Debt_Credit_Notes"
DEBT_MATURITY_SHEET = "Debt_Maturity_Ladder"
DEBT_PROFILE_READINESS_ATTR = "debt_profile_readiness_status"
DEBT_PROFILE_READY = "ready"
DEBT_PROFILE_INSUFFICIENT = "insufficient"
DEBT_MATURITY_RECONCILIATION_ATTR = "debt_maturity_reconciliation_status"
DEBT_MATURITY_RECONCILED = "reconciled"
DEBT_MATURITY_NEEDS_REVIEW = "needs_review"

_ALLOWED_SHEET_STATES = {"visible", "hidden", "veryHidden"}
_REQUIRED_PROFILE_COLUMNS = ("quarter", "metric", "value", "source")
_REQUIRED_MATURITY_COLUMNS = (
    "quarter",
    "maturity_year",
    "maturity_label",
    "amount_total",
    "source_kind",
    "source_basis",
)
_DIRECT_REVOLVER_SOURCE_TYPES = {"table", "text", "xbrl"}
_NON_SOURCE_BACKED_PROFILE_SOURCES = {
    "",
    "derived",
    "missing",
    "nan",
    "needs_review",
    "none",
    "qa_guardrail",
    "unavailable",
}
_REVOLVER_VALUE_SOURCE_PAIRS = (
    ("revolver_commitment", "commitment_source_type"),
    ("revolver_facility_size", "facility_source_type"),
    ("revolver_drawn", "drawn_source_type"),
    ("revolver_letters_of_credit", "lc_source_type"),
    ("revolver_availability", "availability_source_type"),
)
_PROFILE_DEBT_ANCHOR_METRICS = {
    "debt_core",
    "debt_current",
    "debt_fair_value",
    "debt_long_term",
    "total_debt",
}


class DebtSheetVisibilityError(ValueError):
    """Raised when the manifest cannot own debt-sheet visibility safely."""


@dataclass(frozen=True)
class DebtSheetReadinessContract:
    """Validated manifest-owned readiness overlay for one conditional debt sheet."""

    mode: str
    visibility_mode: str
    ready_state: str
    minimum_count: int | None
    economic_validation_contract: str


def mark_debt_profile_readiness(
    frame: pd.DataFrame,
    *,
    economic_validation: DebtProfileEconomicValidationResult,
) -> pd.DataFrame:
    """Attach an independent economic validation result, never a row-count decision."""

    frame.attrs[DEBT_PROFILE_ECONOMIC_VALIDATION_ATTR] = economic_validation
    frame.attrs[DEBT_PROFILE_READINESS_ATTR] = (
        DEBT_PROFILE_READY if economic_validation.passed else DEBT_PROFILE_INSUFFICIENT
    )
    return frame


def mark_debt_maturity_reconciliation(frame: pd.DataFrame, *, reconciled: bool) -> pd.DataFrame:
    """Attach the upstream reconciliation decision without recomputing economics."""

    frame.attrs[DEBT_MATURITY_RECONCILIATION_ATTR] = (
        DEBT_MATURITY_RECONCILED if reconciled else DEBT_MATURITY_NEEDS_REVIEW
    )
    return frame


def debt_profile_source_backed_row_count(frame: Any) -> int:
    """Count distinct complete profile metrics backed by a non-derived source."""

    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return 0
    if any(column not in frame.columns for column in _REQUIRED_PROFILE_COLUMNS):
        return 0
    quarters = pd.to_datetime(frame["quarter"], errors="coerce")
    values = pd.to_numeric(frame["value"], errors="coerce")
    metrics = frame["metric"].fillna("").astype(str).str.strip().str.lower()
    sources = frame["source"].fillna("").astype(str).str.strip().str.lower()
    source_backed = ~sources.isin(_NON_SOURCE_BACKED_PROFILE_SOURCES)
    source_backed &= ~sources.str.contains("needs review", regex=False)
    complete = quarters.notna() & values.notna() & metrics.ne("") & source_backed
    complete &= ~metrics.str.contains("needs review", regex=False)
    return int(metrics[complete].nunique())


def debt_profile_has_source_backed_debt_anchor(frame: Any) -> bool:
    """Require at least one direct funded-debt metric in the presentation rowset."""

    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return False
    if any(column not in frame.columns for column in _REQUIRED_PROFILE_COLUMNS):
        return False
    quarters = pd.to_datetime(frame["quarter"], errors="coerce")
    values = pd.to_numeric(frame["value"], errors="coerce")
    metrics = frame["metric"].fillna("").astype(str).str.strip().str.lower()
    sources = frame["source"].fillna("").astype(str).str.strip().str.lower()
    direct_source = ~sources.isin(_NON_SOURCE_BACKED_PROFILE_SOURCES)
    direct_source &= ~sources.str.contains("needs review", regex=False)
    return bool(
        (
            quarters.notna()
            & values.notna()
            & metrics.isin(_PROFILE_DEBT_ANCHOR_METRICS)
            & direct_source
        ).any()
    )


def debt_profile_is_publishable(
    frame: Any,
    *,
    minimum_count: int,
    economic_validation: DebtProfileEconomicValidationResult | None = None,
) -> bool:
    """Require independent economics plus separate complete presentation geometry."""

    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return False
    validation = economic_validation or frame.attrs.get(DEBT_PROFILE_ECONOMIC_VALIDATION_ATTR)
    if not isinstance(validation, DebtProfileEconomicValidationResult) or not validation.passed:
        return False
    if validation.contract_id != DEBT_PROFILE_ECONOMIC_VALIDATION_CONTRACT:
        return False
    profile_dates = pd.to_datetime(frame.get("quarter"), errors="coerce")
    if profile_dates.isna().all():
        return False
    latest_profile_date = profile_dates.max().date().isoformat()
    if validation.as_of_date != latest_profile_date:
        return False
    return bool(
        debt_profile_source_backed_row_count(frame) >= minimum_count
        and debt_profile_has_source_backed_debt_anchor(frame)
    )


def debt_maturity_is_publishable(frame: Any) -> bool:
    """Return whether an explicitly reconciled maturity rowset is complete enough to expose."""

    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return False
    if frame.attrs.get(DEBT_MATURITY_RECONCILIATION_ATTR) != DEBT_MATURITY_RECONCILED:
        return False
    if any(column not in frame.columns for column in _REQUIRED_MATURITY_COLUMNS):
        return False

    quarters = pd.to_datetime(frame["quarter"], errors="coerce")
    maturity_years = pd.to_numeric(frame["maturity_year"], errors="coerce")
    amounts = pd.to_numeric(frame["amount_total"], errors="coerce")
    if quarters.isna().any() or maturity_years.isna().any() or amounts.isna().any():
        return False

    for column in ("maturity_label", "source_kind", "source_basis"):
        values = frame[column].fillna("").astype(str).str.strip()
        if values.eq("").any():
            return False

    source_kinds = frame["source_kind"].fillna("").astype(str).str.strip().str.lower()
    labels = frame["maturity_label"].fillna("").astype(str).str.strip().str.lower()
    if source_kinds.eq("qa_guardrail").any() or labels.str.contains("needs review", regex=False).any():
        return False
    if "qa_status" in frame.columns:
        statuses = frame["qa_status"].fillna("").astype(str).str.strip().str.lower()
        if statuses.isin({"fail", "failed", "blocked", "needs_review"}).any():
            return False
    return True


def revolver_history_is_publishable(frame: Any, *, minimum_count: int) -> bool:
    """Require four source-backed periods with at least one reported facility value."""

    if not isinstance(frame, pd.DataFrame) or frame.empty or "quarter" not in frame.columns:
        return False
    dates = pd.to_datetime(frame["quarter"], errors="coerce")
    has_source_backed_value = pd.Series(False, index=frame.index, dtype=bool)
    for value_column, source_column in _REVOLVER_VALUE_SOURCE_PAIRS:
        if value_column not in frame.columns or source_column not in frame.columns:
            continue
        values = pd.to_numeric(frame[value_column], errors="coerce")
        sources = frame[source_column].fillna("").astype(str).str.strip().str.lower()
        has_source_backed_value |= values.notna() & sources.isin(_DIRECT_REVOLVER_SOURCE_TYPES)
    accepted_dates = dates[dates.notna() & has_source_backed_value]
    return accepted_dates.dt.normalize().nunique() >= minimum_count


def leverage_liquidity_is_publishable(frame: Any, *, minimum_count: int) -> bool:
    """Require four complete exact-period leverage dispositions, not cash-only rows."""

    required = (
        "quarter",
        "corporate_net_debt",
        "ebitda_ttm",
        "corporate_net_leverage",
        "corporate_net_leverage_basis",
    )
    if (
        not isinstance(frame, pd.DataFrame)
        or frame.empty
        or any(column not in frame.columns for column in required)
    ):
        return False
    dates = pd.to_datetime(frame["quarter"], errors="coerce")
    complete = dates.notna()
    for column in ("corporate_net_debt", "ebitda_ttm", "corporate_net_leverage"):
        complete &= pd.to_numeric(frame[column], errors="coerce").notna()
    basis = frame["corporate_net_leverage_basis"].fillna("").astype(str).str.strip().str.lower()
    complete &= basis.eq("gaap_ebitda_ttm")
    accepted_dates = dates[complete]
    return accepted_dates.dt.normalize().nunique() >= minimum_count


def debt_credit_notes_is_publishable(frame: Any, *, minimum_count: int) -> bool:
    """Require two complete typed notes with source identity and no blocking QA status."""

    required = ("quarter", "category", "snippet", "source_class", "method")
    if (
        not isinstance(frame, pd.DataFrame)
        or frame.empty
        or any(column not in frame.columns for column in required)
    ):
        return False
    dates = pd.to_datetime(frame["quarter"], errors="coerce")
    complete = dates.notna()
    for column in required[1:]:
        complete &= frame[column].fillna("").astype(str).str.strip().ne("")
    if "qa_severity" in frame.columns:
        severity = frame["qa_severity"].fillna("").astype(str).str.strip().str.lower()
        complete &= ~severity.isin({"error", "fail", "failed", "blocked"})
    return int(complete.sum()) >= minimum_count


def _debt_module(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    matches = [
        module
        for module in payload.get("modules") or []
        if isinstance(module, Mapping) and str(module.get("module_id") or "") == DEBT_MODULE_ID
    ]
    if len(matches) != 1:
        raise DebtSheetVisibilityError(
            f"Expected exactly one {DEBT_MODULE_ID!r} module, found {len(matches)}."
        )
    return matches[0]


def _sheet_contracts(module: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    for contract in module.get("sheets") or []:
        if not isinstance(contract, Mapping):
            continue
        sheet_name = str(contract.get("sheet") or "").strip()
        if not sheet_name:
            raise DebtSheetVisibilityError("Debt sheet contract has no sheet name.")
        if sheet_name in result:
            raise DebtSheetVisibilityError(f"Duplicate debt sheet contract {sheet_name!r}.")
        result[sheet_name] = contract
    return result


def _default_state(contract: Mapping[str, Any], sheet_name: str) -> str:
    state = str(contract.get("default_state") or "hidden").strip()
    if state not in _ALLOWED_SHEET_STATES:
        raise DebtSheetVisibilityError(
            f"Debt sheet {sheet_name!r} has unsupported default state {state!r}."
        )
    return state


def _readiness(contract: Mapping[str, Any], sheet_name: str) -> DebtSheetReadinessContract:
    readiness = contract.get("readiness")
    if not isinstance(readiness, Mapping) or not str(readiness.get("mode") or "").strip():
        raise DebtSheetVisibilityError(f"Conditional debt sheet {sheet_name!r} has no readiness contract.")
    visibility_mode = str(readiness.get("visibility_mode") or "").strip()
    ready_state = str(readiness.get("ready_state") or "").strip()
    if visibility_mode != "readiness_overlay":
        raise DebtSheetVisibilityError(
            f"Conditional debt sheet {sheet_name!r} has unsupported visibility mode {visibility_mode!r}."
        )
    if ready_state != "visible":
        raise DebtSheetVisibilityError(
            f"Conditional debt sheet {sheet_name!r} has unsupported ready state {ready_state!r}."
        )
    if _default_state(contract, sheet_name) != "hidden":
        raise DebtSheetVisibilityError(
            f"Conditional debt sheet {sheet_name!r} must use hidden baseline state."
        )
    minimum = readiness.get("minimum_count")
    if minimum is not None and (not isinstance(minimum, int) or minimum < 1):
        raise DebtSheetVisibilityError(
            f"Conditional debt sheet {sheet_name!r} has invalid minimum_count {minimum!r}."
        )
    economic_validation_contract = str(readiness.get("economic_validation_contract") or "").strip()
    if sheet_name == DEBT_PROFILE_SHEET:
        if economic_validation_contract != DEBT_PROFILE_ECONOMIC_VALIDATION_CONTRACT:
            raise DebtSheetVisibilityError(
                f"Debt profile has unsupported economic validation contract "
                f"{economic_validation_contract!r}."
            )
    elif economic_validation_contract:
        raise DebtSheetVisibilityError(
            f"Debt sheet {sheet_name!r} cannot declare profile economic validation "
            f"{economic_validation_contract!r}."
        )
    return DebtSheetReadinessContract(
        mode=str(readiness.get("mode") or "").strip(),
        visibility_mode=visibility_mode,
        ready_state=ready_state,
        minimum_count=minimum,
        economic_validation_contract=economic_validation_contract,
    )


def debt_sheet_default_states(
    *,
    module_payload: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    """Return the manifest-owned baseline state for every debt sheet."""

    payload = load_workbook_module_manifest() if module_payload is None else module_payload
    contracts = _sheet_contracts(_debt_module(payload))
    return {sheet_name: _default_state(contract, sheet_name) for sheet_name, contract in contracts.items()}


def debt_sheet_readiness_contracts(
    sheet_names: tuple[str, ...],
    *,
    module_payload: Mapping[str, Any] | None = None,
) -> dict[str, DebtSheetReadinessContract]:
    """Read validated readiness overlays for named conditional debt sheets."""

    payload = load_workbook_module_manifest() if module_payload is None else module_payload
    contracts = _sheet_contracts(_debt_module(payload))
    result: dict[str, DebtSheetReadinessContract] = {}
    for sheet_name in sheet_names:
        contract = contracts.get(sheet_name)
        if contract is None:
            raise DebtSheetVisibilityError(f"Debt module has no sheet contract for {sheet_name!r}.")
        result[sheet_name] = _readiness(contract, sheet_name)
    return result


def debt_sheet_minimum_count(
    sheet_name: str,
    *,
    module_payload: Mapping[str, Any] | None = None,
) -> int:
    """Read a conditional sheet's minimum count from the machine-readable manifest owner."""

    return debt_sheet_minimum_counts((sheet_name,), module_payload=module_payload)[sheet_name]


def debt_sheet_minimum_counts(
    sheet_names: tuple[str, ...],
    *,
    module_payload: Mapping[str, Any] | None = None,
) -> dict[str, int]:
    """Read multiple conditional minimums with one strict manifest load."""

    payload = load_workbook_module_manifest() if module_payload is None else module_payload
    contracts = _sheet_contracts(_debt_module(payload))
    result: dict[str, int] = {}
    for sheet_name in sheet_names:
        contract = contracts.get(sheet_name)
        if contract is None:
            raise DebtSheetVisibilityError(f"Debt module has no sheet contract for {sheet_name!r}.")
        minimum = _readiness(contract, sheet_name).minimum_count
        if minimum is None:
            raise DebtSheetVisibilityError(
                f"Conditional debt sheet {sheet_name!r} has no positive minimum_count."
            )
        result[sheet_name] = minimum
    return result


def resolve_legacy_debt_sheet_visibility(
    sheet_frames: Mapping[str, Any],
    *,
    module_payload: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    """Resolve every legacy debt-sheet state from manifest readiness and current frames."""

    payload = load_workbook_module_manifest() if module_payload is None else module_payload
    module = _debt_module(payload)
    contracts = _sheet_contracts(module)
    states = {sheet_name: _default_state(contract, sheet_name) for sheet_name, contract in contracts.items()}
    conditional_sheets = {sheet_name for sheet_name, contract in contracts.items() if "readiness" in contract}
    visible_block_sheets = {
        str(block.get("sheet") or "").strip()
        for block in module.get("visible_blocks") or []
        if isinstance(block, Mapping) and str(block.get("sheet") or "").strip() in contracts
    }
    if visible_block_sheets != conditional_sheets:
        raise DebtSheetVisibilityError(
            "Debt readiness overlays do not exactly match visible blocks: "
            f"missing={sorted(visible_block_sheets - conditional_sheets)!r}, "
            f"unexpected={sorted(conditional_sheets - visible_block_sheets)!r}."
        )
    rules = {
        DEBT_PROFILE_SHEET: ("minimum_useful_rows", debt_profile_is_publishable),
        REVOLVER_HISTORY_SHEET: ("minimum_source_backed_periods", revolver_history_is_publishable),
        LEVERAGE_LIQUIDITY_SHEET: ("minimum_coherent_periods", leverage_liquidity_is_publishable),
        DEBT_CREDIT_NOTES_SHEET: ("minimum_typed_rows", debt_credit_notes_is_publishable),
        DEBT_MATURITY_SHEET: ("explicit_reconciled_complete_schedule", debt_maturity_is_publishable),
    }
    if conditional_sheets != set(rules):
        raise DebtSheetVisibilityError(
            "Debt conditional-sheet readiness rules do not exactly match the manifest: "
            f"missing={sorted(conditional_sheets - set(rules))!r}, "
            f"unexpected={sorted(set(rules) - conditional_sheets)!r}."
        )
    profile_validation_candidates = tuple(
        frame.attrs.get(DEBT_PROFILE_ECONOMIC_VALIDATION_ATTR)
        for frame in (
            sheet_frames.get(DEBT_PROFILE_SHEET),
            sheet_frames.get(REVOLVER_HISTORY_SHEET),
        )
        if isinstance(frame, pd.DataFrame)
        and frame.attrs.get(DEBT_PROFILE_ECONOMIC_VALIDATION_ATTR) is not None
    )
    if len(profile_validation_candidates) > 1 and len(set(profile_validation_candidates)) != 1:
        raise DebtSheetVisibilityError(
            "Debt profile and revolver inputs declare conflicting economic validation results."
        )
    profile_economic_validation = (
        profile_validation_candidates[0] if profile_validation_candidates else None
    )
    for sheet_name, (expected_mode, predicate) in rules.items():
        readiness = _readiness(contracts[sheet_name], sheet_name)
        mode = readiness.mode
        if mode != expected_mode:
            raise DebtSheetVisibilityError(
                f"Debt sheet {sheet_name!r} readiness mode {mode!r} does not match {expected_mode!r}."
            )
        frame = sheet_frames.get(sheet_name)
        if sheet_name == DEBT_PROFILE_SHEET:
            minimum = readiness.minimum_count
            if minimum is None:
                raise DebtSheetVisibilityError(
                    f"Conditional debt sheet {sheet_name!r} has no positive minimum_count."
                )
            ready = predicate(
                frame,
                minimum_count=minimum,
                economic_validation=profile_economic_validation,
            )
        elif mode == "explicit_reconciled_complete_schedule":
            ready = predicate(frame)
        else:
            minimum = readiness.minimum_count
            if minimum is None:
                raise DebtSheetVisibilityError(
                    f"Conditional debt sheet {sheet_name!r} has no positive minimum_count."
                )
            ready = predicate(frame, minimum_count=minimum)
        if ready:
            states[sheet_name] = readiness.ready_state
    return states


def apply_legacy_debt_sheet_visibility(
    workbook: Any,
    sheet_frames: Mapping[str, Any],
    *,
    module_payload: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    """Apply manifest-owned debt states while preserving every non-debt sheet."""

    states = resolve_legacy_debt_sheet_visibility(
        sheet_frames,
        module_payload=module_payload,
    )
    for sheet_name, state in states.items():
        if sheet_name in workbook.sheetnames:
            workbook[sheet_name].sheet_state = state
    return states
