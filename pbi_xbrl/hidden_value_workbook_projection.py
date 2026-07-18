"""Deterministic workbook projections for accepted Hidden Value evaluations.

This module reshapes an already evaluated :class:`HiddenValueEvaluationPlan` for
exact-cell planning.  It deliberately does not resolve metrics, score signals,
or reinterpret economic results.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from pbi_xbrl.hidden_value_signal_economics import HiddenValueEvaluationPlan
from pbi_xbrl.json_schema_validation import load_json_strict, validate_json_schema


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SIGNAL_CONTRACT = ROOT / "docs" / "hidden_value_signal_contract.json"
WORKBOOK_PROJECTION_SCHEMA = ROOT / "docs" / "hidden_value_workbook_projection.schema.json"
WORKBOOK_PROJECTION_VERSION = "1.0.0"


class HiddenValueWorkbookProjectionError(ValueError):
    """Raised when an accepted evaluation cannot be projected deterministically."""


@dataclass(frozen=True)
class HiddenValueWorkbookProjection:
    ticker: str
    profile_id: str
    as_of_period: str
    contract_digest: str
    evaluation_plan_digest: str
    base_rows: tuple[dict[str, Any], ...]
    audit_rows: tuple[dict[str, Any], ...]
    recompute_rows: tuple[dict[str, Any], ...]
    flags_rows: tuple[dict[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "projection_version": WORKBOOK_PROJECTION_VERSION,
            "status": "PASS",
            "ticker": self.ticker,
            "profile_id": self.profile_id,
            "as_of_period": self.as_of_period,
            "contract_digest": self.contract_digest,
            "evaluation_plan_digest": self.evaluation_plan_digest,
            "base_rows": [dict(row) for row in self.base_rows],
            "audit_rows": [dict(row) for row in self.audit_rows],
            "recompute_rows": [dict(row) for row in self.recompute_rows],
            "flags_rows": [dict(row) for row in self.flags_rows],
        }
        payload["projection_digest"] = _digest(payload)
        return payload


def build_hidden_value_workbook_projection(
    evaluation_plan: HiddenValueEvaluationPlan,
    *,
    contract_path: Path | str = DEFAULT_SIGNAL_CONTRACT,
) -> HiddenValueWorkbookProjection:
    """Project one accepted plan into typed, deterministic workbook support rows."""

    if evaluation_plan.status != "PASS":
        raise HiddenValueWorkbookProjectionError("Only a PASS Hidden Value evaluation can be projected.")
    contract = load_json_strict(Path(contract_path))
    if str(contract.get("plan_version") or "") != evaluation_plan.plan_version:
        raise HiddenValueWorkbookProjectionError("Hidden Value plan and contract versions differ.")
    if _digest(contract) != evaluation_plan.contract_digest:
        raise HiddenValueWorkbookProjectionError("Hidden Value contract digest differs from the accepted evaluation.")

    signal_by_id = {
        str(signal.get("signal_id") or ""): signal
        for signal in contract.get("signals") or []
        if isinstance(signal, Mapping)
    }
    base_rows = _base_rows(evaluation_plan)
    audit_rows: list[dict[str, Any]] = []
    recompute_rows: list[dict[str, Any]] = []
    record_order = 0

    for candidate in evaluation_plan.candidates:
        signal = signal_by_id.get(candidate.signal_id)
        if signal is None:
            raise HiddenValueWorkbookProjectionError(
                f"Signal {candidate.signal_id!r} is absent from the authoritative contract."
            )
        resolved_by_id = {row.metric_id: row for row in candidate.resolved_inputs}
        candidate_records: list[dict[str, Any]] = []

        for metric_id in map(str, signal.get("required_metric_ids") or []):
            metric = resolved_by_id.get(metric_id)
            if metric is None:
                raise HiddenValueWorkbookProjectionError(
                    f"Candidate {candidate.candidate_key!r} lacks required metric {metric_id!r}."
                )
            record_order += 1
            candidate_records.append(
                _recompute_record(
                    record_order=record_order,
                    candidate_key=candidate.candidate_key,
                    signal_id=candidate.signal_id,
                    record_type="required_metric",
                    stage="required",
                    item_id=metric_id,
                    metric_key=_metric_key(metric.to_dict()),
                    expected_value=metric.value,
                    expected_passed=metric.available,
                    expected_status=metric.status,
                )
            )

        predicate_contracts = {
            (stage, str(predicate.get("predicate_id") or "")): predicate
            for stage in ("eligibility", "trigger", "near_miss")
            for predicate in (signal.get(stage) or {}).get("predicates") or []
            if isinstance(predicate, Mapping)
        }
        for predicate in candidate.predicate_results:
            definition = predicate_contracts.get((predicate.stage, predicate.predicate_id))
            if definition is None:
                raise HiddenValueWorkbookProjectionError(
                    f"Predicate {candidate.signal_id}:{predicate.stage}:{predicate.predicate_id} is not contracted."
                )
            metric = resolved_by_id.get(predicate.metric_id)
            right = resolved_by_id.get(predicate.comparison_metric_id or "")
            record_order += 1
            candidate_records.append(
                _recompute_record(
                    record_order=record_order,
                    candidate_key=candidate.candidate_key,
                    signal_id=candidate.signal_id,
                    record_type="predicate",
                    stage=predicate.stage,
                    item_id=predicate.predicate_id,
                    metric_key=_metric_key(metric.to_dict()) if metric else "",
                    operator=predicate.operator,
                    right_metric_key=_metric_key(right.to_dict()) if right else "",
                    threshold=definition.get("threshold"),
                    expected_value=predicate.value,
                    expected_comparison=predicate.comparison_value,
                    expected_passed=predicate.passed,
                    expected_status=predicate.reason,
                )
            )

        component_contracts = {
            str(component.get("component_id") or ""): component
            for component in signal.get("score_components") or []
            if isinstance(component, Mapping)
        }
        for component in candidate.component_scores:
            definition = component_contracts.get(component.component_id)
            if definition is None:
                raise HiddenValueWorkbookProjectionError(
                    f"Score component {candidate.signal_id}:{component.component_id} is not contracted."
                )
            normalization = definition.get("normalization") or {}
            metric = resolved_by_id.get(component.metric_id)
            record_order += 1
            candidate_records.append(
                _recompute_record(
                    record_order=record_order,
                    candidate_key=candidate.candidate_key,
                    signal_id=candidate.signal_id,
                    record_type="score_component",
                    stage="score",
                    item_id=component.component_id,
                    metric_key=_metric_key(metric.to_dict()) if metric else "",
                    required_component=bool(definition.get("required")),
                    weight=component.weight,
                    normalization_direction=str(normalization.get("direction") or ""),
                    normalization_threshold=normalization.get("threshold"),
                    normalization_span=normalization.get("span"),
                    normalization_base=normalization.get("base"),
                    expected_value=component.value,
                    expected_included_weight=component.included_weight,
                    expected_normalized_score=component.normalized_score,
                    expected_weighted_score=component.weighted_score,
                    expected_status=component.status,
                )
            )

        recompute_rows.extend(candidate_records)
        audit_rows.append(
            {
                "candidate_key": candidate.candidate_key,
                "signal_id": candidate.signal_id,
                "display_name": candidate.display_name,
                "profile_id": candidate.profile_id,
                "as_of_period": candidate.as_of_period,
                "expected_state": candidate.state,
                "expected_triggered": candidate.triggered,
                "expected_score": candidate.score,
                "expected_score_denominator": candidate.score_denominator,
                "expected_severity": candidate.severity or "",
                "priority": candidate.priority,
                "reasons": _json_cell(candidate.reasons),
                "evidence_ids": _json_cell(candidate.evidence_ids),
                "source_refs": _json_cell(candidate.source_refs),
                "resolved_metric_keys": _json_cell(
                    sorted(_metric_key(row.to_dict()) for row in candidate.resolved_inputs)
                ),
                "recompute_record_keys": _json_cell([row["record_key"] for row in candidate_records]),
                "module_eligible": not any(
                    reason.startswith(("required_modules_disabled:", "required_profile_packs_disabled:"))
                    for reason in candidate.reasons
                ),
                "source_ref": candidate.source_refs[0] if candidate.source_refs else "",
            }
        )

    flags_rows = []
    candidate_by_key = {candidate.candidate_key: candidate for candidate in evaluation_plan.candidates}
    for rank, flag in enumerate(evaluation_plan.flags_projection, start=1):
        candidate = candidate_by_key.get(str(flag.get("candidate_key") or ""))
        if candidate is None or not candidate.triggered:
            raise HiddenValueWorkbookProjectionError("Flags projection contains an unknown or non-triggered candidate.")
        flags_rows.append(
            {
                "display_rank": rank,
                "candidate_key": candidate.candidate_key,
                "signal_id": candidate.signal_id,
                "display_name": candidate.display_name,
                "score": candidate.score,
                "triggered": True,
                "state": candidate.state,
                "severity": candidate.severity or "",
                "as_of_period": candidate.as_of_period,
                "reason": "; ".join(candidate.reasons),
                "evidence_ids": _json_cell(candidate.evidence_ids),
                "source_refs": _json_cell(candidate.source_refs),
                "audit_id": candidate.candidate_key,
                "source_ref": candidate.source_refs[0] if candidate.source_refs else "",
            }
        )

    projection = HiddenValueWorkbookProjection(
        ticker=evaluation_plan.ticker,
        profile_id=evaluation_plan.profile_id,
        as_of_period=evaluation_plan.as_of_period,
        contract_digest=evaluation_plan.contract_digest,
        evaluation_plan_digest=_digest(evaluation_plan.to_dict()),
        base_rows=tuple(base_rows),
        audit_rows=tuple(audit_rows),
        recompute_rows=tuple(recompute_rows),
        flags_rows=tuple(flags_rows),
    )
    failures = validate_json_schema(projection.to_dict(), load_json_strict(WORKBOOK_PROJECTION_SCHEMA))
    if failures:
        sample = "; ".join(f"{field} {keyword}: {message}" for field, keyword, message in failures[:12])
        raise HiddenValueWorkbookProjectionError(
            f"Hidden Value workbook projection does not satisfy its schema: {sample}"
        )
    return projection


def _base_rows(evaluation_plan: HiddenValueEvaluationPlan) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in evaluation_plan.base_projection:
        key = _metric_key(raw)
        if key in seen:
            raise HiddenValueWorkbookProjectionError(f"Duplicate Hidden Value base metric key {key!r}.")
        seen.add(key)
        source_refs = tuple(map(str, raw.get("source_refs") or []))
        rows.append(
            {
                "metric_key": key,
                "metric_id": str(raw.get("metric_id") or ""),
                "value": raw.get("value"),
                "unit": str(raw.get("unit") or ""),
                "period": str(raw.get("period") or ""),
                "period_role": str(raw.get("period_role") or ""),
                "dimension_id": str(raw.get("dimension_id") or ""),
                "member": str(raw.get("member") or ""),
                "status": str(raw.get("status") or ""),
                "reason": str(raw.get("reason") or ""),
                "evidence_ids": _json_cell(raw.get("evidence_ids") or []),
                "source_refs": _json_cell(source_refs),
                "formula_ids": _json_cell(raw.get("formula_ids") or []),
                "source_ref": source_refs[0] if source_refs else "",
            }
        )
    return sorted(rows, key=lambda row: str(row["metric_key"]))


def _recompute_record(
    *,
    record_order: int,
    candidate_key: str,
    signal_id: str,
    record_type: str,
    stage: str,
    item_id: str,
    metric_key: str,
    operator: str = "",
    right_metric_key: str = "",
    threshold: Any = None,
    required_component: bool = False,
    weight: Any = None,
    normalization_direction: str = "",
    normalization_threshold: Any = None,
    normalization_span: Any = None,
    normalization_base: Any = None,
    expected_value: Any = None,
    expected_comparison: Any = None,
    expected_passed: Any = None,
    expected_included_weight: Any = None,
    expected_normalized_score: Any = None,
    expected_weighted_score: Any = None,
    expected_status: str = "",
) -> dict[str, Any]:
    record_key = f"{candidate_key}|{record_type}|{stage}|{item_id}"
    return {
        "record_order": record_order,
        "record_key": record_key,
        "candidate_key": candidate_key,
        "signal_id": signal_id,
        "record_type": record_type,
        "stage": stage,
        "item_id": item_id,
        "metric_key": metric_key,
        "operator": operator,
        "right_metric_key": right_metric_key,
        "threshold": threshold,
        "required_component": required_component,
        "weight": weight,
        "normalization_direction": normalization_direction,
        "normalization_threshold": normalization_threshold,
        "normalization_span": normalization_span,
        "normalization_base": normalization_base,
        "expected_value": expected_value,
        "expected_comparison": expected_comparison,
        "expected_passed": expected_passed,
        "expected_included_weight": expected_included_weight,
        "expected_normalized_score": expected_normalized_score,
        "expected_weighted_score": expected_weighted_score,
        "expected_status": expected_status,
        "source_ref": "",
    }


def _metric_key(row: Mapping[str, Any]) -> str:
    values = (
        str(row.get("metric_id") or ""),
        str(row.get("period") or ""),
        str(row.get("dimension_id") or "total_company"),
        str(row.get("member") or "total_company"),
    )
    if not values[0] or not values[1]:
        raise HiddenValueWorkbookProjectionError(f"Metric key requires metric_id and period: {values!r}.")
    if any("|" in value for value in values):
        raise HiddenValueWorkbookProjectionError(f"Metric key fields cannot contain '|': {values!r}.")
    return "|".join(values)


def _json_cell(values: Any) -> str:
    return json.dumps(list(values), ensure_ascii=False, separators=(",", ":"))


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()
