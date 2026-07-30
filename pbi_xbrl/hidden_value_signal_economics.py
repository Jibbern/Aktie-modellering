"""Deterministic Hidden Value signal economics for the generic ticker engine.

The checked-in JSON contract owns signal-specific metrics, thresholds, weights,
states and visibility policy.  This module provides generic resolution, predicate,
scoring and projection operations.  It does not read Excel caches, write workbooks,
or infer behavior from ticker symbols.
"""
from __future__ import annotations

import hashlib
import math
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

from pbi_xbrl.json_schema_validation import load_json_strict, validate_json_schema
from pbi_xbrl.new_ticker_style_planner import (
    FORMULA_ECONOMIC_SPECS,
    EconomicPoint,
    FormulaEconomicLookup,
)
from pbi_xbrl.workbook_modules import (
    DEFAULT_MODULE_MANIFEST,
    canonical_json_sha256,
    load_workbook_module_manifest,
    resolve_module_profile,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_HIDDEN_VALUE_CONTRACT = ROOT / "docs" / "hidden_value_signal_contract.json"
HIDDEN_VALUE_CONTRACT_SCHEMA = ROOT / "docs" / "hidden_value_signal_contract.schema.json"
HIDDEN_VALUE_PLAN_SCHEMA = ROOT / "docs" / "hidden_value_evaluation_plan.schema.json"

_QUARTER_SUFFIXES = ("-Q1", "-Q2", "-Q3", "-Q4")
_CALCULATED_STATUSES = {"source_backed", "formula_calculated", "derived_calculated"}
_INVALID_REASONS = {
    "source_conflict",
    "formula_input_unit_or_denominator_invalid",
    "formula_ttm_unit_or_component_invalid",
    "formula_comparison_unit_mismatch",
    "formula_zero_denominator",
    "unit_mismatch",
    "period_mismatch",
    "zero_denominator",
    "nonpositive_denominator",
    "nonpositive_prior_denominator",
    "resolver_cycle",
}


class HiddenValueContractError(ValueError):
    """Raised when the authoritative signal contract is invalid."""


class HiddenValueEvaluationError(RuntimeError):
    """Raised when deterministic signal evaluation cannot be completed."""


@dataclass(frozen=True)
class ResolvedMetric:
    metric_id: str
    value: float | None
    unit: str
    period: str
    period_role: str
    status: str
    reason: str
    source_refs: tuple[str, ...] = ()
    formula_ids: tuple[str, ...] = ()
    dimension_id: str = "total_company"
    member: str = "total_company"

    @property
    def available(self) -> bool:
        return self.value is not None and self.status in _CALCULATED_STATUSES

    @property
    def evidence_ids(self) -> tuple[str, ...]:
        return tuple(_evidence_id(ref) for ref in self.source_refs)

    def to_dict(self) -> dict[str, Any]:
        return {
            "metric_id": self.metric_id,
            "value": self.value,
            "unit": self.unit,
            "period": self.period,
            "period_role": self.period_role,
            "dimension_id": self.dimension_id,
            "member": self.member,
            "status": self.status,
            "reason": self.reason,
            "evidence_ids": list(self.evidence_ids),
            "source_refs": list(self.source_refs),
            "formula_ids": list(self.formula_ids),
        }


@dataclass(frozen=True)
class PredicateResult:
    stage: str
    predicate_id: str
    metric_id: str
    operator: str
    value: float | None
    comparison_value: float | None
    comparison_metric_id: str | None
    passed: bool | None
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "predicate_id": self.predicate_id,
            "metric_id": self.metric_id,
            "operator": self.operator,
            "value": self.value,
            "comparison_value": self.comparison_value,
            "comparison_metric_id": self.comparison_metric_id,
            "passed": self.passed,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class ComponentScore:
    component_id: str
    metric_id: str
    value: float | None
    weight: float
    included_weight: float
    normalized_score: float | None
    weighted_score: float | None
    status: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "component_id": self.component_id,
            "metric_id": self.metric_id,
            "value": self.value,
            "weight": self.weight,
            "included_weight": self.included_weight,
            "normalized_score": self.normalized_score,
            "weighted_score": self.weighted_score,
            "status": self.status,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class HiddenValueCandidate:
    candidate_key: str
    signal_id: str
    display_name: str
    profile_id: str
    as_of_period: str
    state: str
    triggered: bool
    score: int | None
    score_denominator: float
    severity: str | None
    priority: int
    reasons: tuple[str, ...]
    evidence_ids: tuple[str, ...]
    source_refs: tuple[str, ...]
    resolved_inputs: tuple[ResolvedMetric, ...]
    predicate_results: tuple[PredicateResult, ...]
    component_scores: tuple[ComponentScore, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_key": self.candidate_key,
            "signal_id": self.signal_id,
            "display_name": self.display_name,
            "profile_id": self.profile_id,
            "as_of_period": self.as_of_period,
            "state": self.state,
            "triggered": self.triggered,
            "score": self.score,
            "score_denominator": self.score_denominator,
            "severity": self.severity,
            "priority": self.priority,
            "reasons": list(self.reasons),
            "evidence_ids": list(self.evidence_ids),
            "source_refs": list(self.source_refs),
            "resolved_inputs": [row.to_dict() for row in self.resolved_inputs],
            "predicate_results": [row.to_dict() for row in self.predicate_results],
            "component_scores": [row.to_dict() for row in self.component_scores],
        }


@dataclass
class HiddenValueEvaluationPlan:
    ticker: str
    profile_id: str
    as_of_period: str
    contract_digest: str
    plan_version: str
    candidates: list[HiddenValueCandidate] = field(default_factory=list)
    base_projection: list[dict[str, Any]] = field(default_factory=list)
    audit_projection: list[dict[str, Any]] = field(default_factory=list)
    recompute_projection: list[dict[str, Any]] = field(default_factory=list)
    flags_projection: list[dict[str, Any]] = field(default_factory=list)

    @property
    def status(self) -> str:
        return "PASS"

    @property
    def state_counts(self) -> dict[str, int]:
        counts = Counter(candidate.state for candidate in self.candidates)
        return dict(sorted(counts.items()))

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_version": self.plan_version,
            "status": self.status,
            "ticker": self.ticker,
            "profile_id": self.profile_id,
            "as_of_period": self.as_of_period,
            "contract_digest": self.contract_digest,
            "candidate_count": len(self.candidates),
            "state_counts": self.state_counts,
            "candidates": [candidate.to_dict() for candidate in self.candidates],
            "projections": {
                "base": list(self.base_projection),
                "audit": list(self.audit_projection),
                "recompute": list(self.recompute_projection),
                "flags": list(self.flags_projection),
            },
        }


def load_hidden_value_signal_contract(
    path: Path | str = DEFAULT_HIDDEN_VALUE_CONTRACT,
    *,
    module_manifest_path: Path | str = DEFAULT_MODULE_MANIFEST,
) -> dict[str, Any]:
    """Load and semantically validate the single authoritative signal contract."""

    payload = load_json_strict(path)
    if not isinstance(payload, dict):
        raise HiddenValueContractError("Hidden Value signal contract must be a JSON object.")
    failures = validate_json_schema(payload, load_json_strict(HIDDEN_VALUE_CONTRACT_SCHEMA))
    if failures:
        sample = "; ".join(f"{field} {keyword}: {message}" for field, keyword, message in failures[:12])
        raise HiddenValueContractError(f"Hidden Value signal contract does not satisfy its schema: {sample}")
    manifest = load_workbook_module_manifest(module_manifest_path)
    issues = validate_hidden_value_signal_contract(payload, manifest)
    if issues:
        raise HiddenValueContractError("Invalid Hidden Value signal contract: " + "; ".join(issues[:12]))
    return payload


def validate_hidden_value_signal_contract(
    payload: Mapping[str, Any],
    module_manifest: Mapping[str, Any],
) -> list[str]:
    """Return semantic issues without copying any signal-specific rule into Python."""

    issues: list[str] = []
    modules = {str(row.get("module_id") or "") for row in module_manifest.get("modules") or []}
    packs = {str(row.get("pack_id") or "") for row in module_manifest.get("profile_packs") or []}
    owner = str(payload.get("owner_module_id") or "")
    if owner not in modules:
        issues.append(f"Unknown owner module {owner!r}.")

    metric_rows = [row for row in payload.get("metric_resolvers") or [] if isinstance(row, Mapping)]
    metric_ids = [str(row.get("metric_id") or "") for row in metric_rows]
    issues.extend(_duplicate_issues(metric_ids, "metric resolver"))
    metric_set = set(metric_ids)
    resolver_dependencies: dict[str, set[str]] = {}
    for row in metric_rows:
        metric_id = str(row.get("metric_id") or "")
        resolver = row.get("resolver") if isinstance(row.get("resolver"), Mapping) else {}
        kind = str(resolver.get("kind") or "")
        required_fields = {
            "formula": ("formula_id",),
            "source_metric": ("source_metric_ids",),
            "package_point": ("path",),
            "lagged": ("input_metric_id", "lag"),
            "relative_change": ("current_metric_id", "prior_metric_id", "denominator_rule"),
            "scaled_change": ("current_metric_id", "prior_metric_id", "scale"),
            "ratio": ("numerator_metric_id", "denominator_metric_id", "denominator_rule"),
            "rolling_sum": ("input_metric_id", "window"),
            "positive_window_count": ("input_metric_id", "window"),
            "nondecreasing_streak": ("input_metric_id", "max_periods"),
            "max_constant_scaled": ("scaled_metric_id", "constant", "scale"),
            "market_cap": ("explicit_metric_id", "price_metric_id", "shares_metric_id"),
        }.get(kind, ())
        missing = [field_name for field_name in required_fields if resolver.get(field_name) in (None, "", [])]
        if missing:
            issues.append(f"Metric {metric_id!r} resolver {kind!r} is missing {missing!r}.")
        if kind == "formula" and str(resolver.get("formula_id") or "") not in FORMULA_ECONOMIC_SPECS:
            issues.append(f"Metric {metric_id!r} references unknown formula {resolver.get('formula_id')!r}.")
        dependencies = {
            str(resolver.get(field_name) or "")
            for field_name in (
                "input_metric_id",
                "current_metric_id",
                "prior_metric_id",
                "numerator_metric_id",
                "denominator_metric_id",
                "explicit_metric_id",
                "price_metric_id",
                "shares_metric_id",
                "scaled_metric_id",
            )
            if resolver.get(field_name)
        }
        resolver_dependencies[metric_id] = dependencies
        for dependency in sorted(dependencies - metric_set):
            issues.append(f"Metric {metric_id!r} references unknown metric {dependency!r}.")
    issues.extend(_cycle_issues(resolver_dependencies))

    signal_rows = [row for row in payload.get("signals") or [] if isinstance(row, Mapping)]
    signal_ids = [str(row.get("signal_id") or "") for row in signal_rows]
    issues.extend(_duplicate_issues(signal_ids, "signal"))
    for signal in signal_rows:
        signal_id = str(signal.get("signal_id") or "")
        if str(signal.get("owner_module_id") or "") != owner:
            issues.append(f"Signal {signal_id!r} is not owned by {owner!r}.")
        unknown_modules = sorted(set(map(str, signal.get("required_modules") or [])) - modules)
        if unknown_modules:
            issues.append(f"Signal {signal_id!r} requires unknown modules {unknown_modules!r}.")
        unknown_packs = sorted(set(map(str, signal.get("profile_pack_ids") or [])) - packs)
        if unknown_packs:
            issues.append(f"Signal {signal_id!r} requires unknown profile packs {unknown_packs!r}.")
        required = set(map(str, signal.get("required_metric_ids") or []))
        optional = set(map(str, signal.get("optional_metric_ids") or []))
        if required & optional:
            issues.append(f"Signal {signal_id!r} has metrics that are both required and optional.")
        deduplication_fields = set(map(str, signal.get("deduplication_fields") or []))
        allowed_identity_fields = {"signal_id", "profile_id", "as_of_period", "dimension_id", "member"}
        if not {"signal_id", "profile_id", "as_of_period"}.issubset(deduplication_fields):
            issues.append(
                f"Signal {signal_id!r} deduplication must include signal_id, profile_id, and as_of_period."
            )
        unknown_identity_fields = sorted(deduplication_fields - allowed_identity_fields)
        if unknown_identity_fields:
            issues.append(f"Signal {signal_id!r} has unknown deduplication fields {unknown_identity_fields!r}.")
        used_metrics = set(required | optional)
        for group_name in ("eligibility", "trigger", "near_miss"):
            group = signal.get(group_name) if isinstance(signal.get(group_name), Mapping) else {}
            predicate_ids = [str(row.get("predicate_id") or "") for row in group.get("predicates") or []]
            issues.extend(f"Signal {signal_id!r} {item}" for item in _duplicate_issues(predicate_ids, f"{group_name} predicate"))
            for predicate in group.get("predicates") or []:
                used_metrics.add(str(predicate.get("metric_id") or ""))
                if predicate.get("right_metric_id"):
                    used_metrics.add(str(predicate.get("right_metric_id")))
        component_ids = [str(row.get("component_id") or "") for row in signal.get("score_components") or []]
        issues.extend(f"Signal {signal_id!r} {item}" for item in _duplicate_issues(component_ids, "score component"))
        for component in signal.get("score_components") or []:
            used_metrics.add(str(component.get("metric_id") or ""))
        unknown_metrics = sorted(used_metrics - metric_set)
        if unknown_metrics:
            issues.append(f"Signal {signal_id!r} references unknown metrics {unknown_metrics!r}.")
        undeclared = sorted(used_metrics - required - optional)
        if undeclared:
            issues.append(f"Signal {signal_id!r} uses undeclared metrics {undeclared!r}.")
        required_component_weight = sum(
            float(row.get("weight") or 0)
            for row in signal.get("score_components") or []
            if bool(row.get("required"))
        )
        if not bool(signal.get("reweight_available_components")) and not math.isclose(required_component_weight, 100.0):
            issues.append(f"Signal {signal_id!r} required component weights must total 100 without reweighting.")
    return issues


class _MetricResolver:
    def __init__(self, package: Mapping[str, Any], contract: Mapping[str, Any]) -> None:
        self.package = package
        self.contract = contract
        self.metric_contracts = {
            str(row.get("metric_id") or ""): row
            for row in contract.get("metric_resolvers") or []
            if isinstance(row, Mapping)
        }
        self.lookup = FormulaEconomicLookup(package)
        self.latest_period = self.lookup.periods(period_type="quarter")[-1] if self.lookup.periods(period_type="quarter") else ""
        self.cache: dict[tuple[str, str], ResolvedMetric] = {}
        self._active: set[tuple[str, str]] = set()

    def resolve(self, metric_id: str, period: str | None = None) -> ResolvedMetric:
        target_period = period if period is not None else self.latest_period
        key = (metric_id, target_period)
        if key in self.cache:
            return self.cache[key]
        contract = self.metric_contracts.get(metric_id)
        if not isinstance(contract, Mapping):
            result = ResolvedMetric(metric_id, None, "", target_period, "derived", "invalid_input", "unknown_metric")
            self.cache[key] = result
            return result
        if key in self._active:
            result = self._missing(contract, target_period, "invalid_input", "resolver_cycle")
            self.cache[key] = result
            return result
        self._active.add(key)
        try:
            result = self._resolve_contract(contract, target_period)
        finally:
            self._active.discard(key)
        self.cache[key] = result
        return result

    def _resolve_contract(self, contract: Mapping[str, Any], period: str) -> ResolvedMetric:
        resolver = contract.get("resolver") if isinstance(contract.get("resolver"), Mapping) else {}
        kind = str(resolver.get("kind") or "")
        handlers = {
            "formula": self._formula,
            "source_metric": self._source_metric,
            "package_point": self._package_point,
            "lagged": self._lagged,
            "relative_change": self._relative_change,
            "scaled_change": self._scaled_change,
            "ratio": self._ratio,
            "rolling_sum": self._rolling_sum,
            "positive_window_count": self._positive_window_count,
            "nondecreasing_streak": self._nondecreasing_streak,
            "max_constant_scaled": self._max_constant_scaled,
            "market_cap": self._market_cap,
        }
        handler = handlers.get(kind)
        return handler(contract, resolver, period) if handler else self._missing(contract, period, "invalid_input", "unknown_resolver_kind")

    def _formula(self, contract: Mapping[str, Any], resolver: Mapping[str, Any], period: str) -> ResolvedMetric:
        formula_id = str(resolver.get("formula_id") or "")
        formula_lineage = self.lookup.formula_lineage(formula_id)
        point, reason = self.lookup.formula_point(formula_id, period)
        if point is None:
            status = "invalid_input" if reason in _INVALID_REASONS or "unit" in reason or "denominator" in reason else "insufficient_evidence"
            return self._missing(contract, period, status, reason, formula_ids=formula_lineage)
        if point.unit != str(contract.get("unit") or ""):
            return self._missing(contract, period, "invalid_input", "unit_mismatch", source_refs=point.source_refs, formula_ids=formula_lineage)
        return self._available(contract, period, point.value, "formula_calculated", "formula_calculated", point.source_refs, formula_lineage)

    def _source_metric(self, contract: Mapping[str, Any], resolver: Mapping[str, Any], period: str) -> ResolvedMetric:
        points: list[EconomicPoint] = []
        raw_statuses: list[str] = []
        dimension_mismatch = False
        for source_metric_id in map(str, resolver.get("source_metric_ids") or []):
            records = self._raw_source_records(source_metric_id, period)
            for row in records:
                raw_statuses.append(str(row.get("status") or "").strip().lower())
                if not _is_total_company_row(row):
                    dimension_mismatch = True
                    continue
                point = _economic_point_from_source_row(row, set(map(str, self.contract.get("trusted_statuses") or [])))
                if point is not None:
                    points.append(point)
        if points:
            units = {point.unit for point in points}
            values = {round(point.value, 12) for point in points}
            if len(units) != 1 or len(values) != 1:
                return self._missing(contract, period, "invalid_input", "source_conflict", source_refs=_merge_source_refs(points))
            point = points[0]
            if point.unit != str(contract.get("unit") or ""):
                return self._missing(contract, period, "invalid_input", "unit_mismatch", source_refs=_merge_source_refs(points))
            return self._available(contract, period, point.value, "source_backed", "source_backed", _merge_source_refs(points), ())
        if raw_statuses and all(status == "not_applicable" for status in raw_statuses):
            return self._missing(contract, period, "unavailable", "source_not_applicable")
        disposition_path = str(resolver.get("disposition_path") or "")
        disposition = _get_path(self.package, disposition_path) if disposition_path else None
        if isinstance(disposition, Mapping):
            status = str(disposition.get("status") or "").strip().lower()
            source_refs = _source_refs_from_mapping(disposition)
            unavailable_statuses = {
                "not_applicable",
                *map(str, resolver.get("unavailable_disposition_statuses") or []),
            }
            if status in unavailable_statuses:
                return self._missing(
                    contract,
                    period,
                    "unavailable",
                    f"source_{status}",
                    source_refs=source_refs,
                )
            if status and status not in set(map(str, self.contract.get("trusted_statuses") or [])):
                return self._missing(
                    contract,
                    period,
                    "insufficient_evidence",
                    f"source_disposition:{status}",
                    source_refs=source_refs,
                )
        reason = "dimension_member_mismatch" if dimension_mismatch else "source_missing_or_untrusted"
        return self._missing(contract, period, "insufficient_evidence", reason)

    def _package_point(self, contract: Mapping[str, Any], resolver: Mapping[str, Any], period: str) -> ResolvedMetric:
        raw = _get_path(self.package, str(resolver.get("path") or ""))
        if not isinstance(raw, Mapping):
            return self._missing(contract, period, "insufficient_evidence", "package_point_missing")
        status = str(raw.get("status") or "").strip().lower()
        if status == "not_applicable":
            return self._missing(contract, period, "unavailable", "package_point_not_applicable", source_refs=_source_refs_from_mapping(raw))
        if status not in set(map(str, self.contract.get("trusted_statuses") or [])):
            return self._missing(contract, period, "insufficient_evidence", f"package_point_status:{status or 'missing'}", source_refs=_source_refs_from_mapping(raw))
        value = raw.get("value")
        if not _numeric(value):
            return self._missing(contract, period, "insufficient_evidence", "package_point_value_missing", source_refs=_source_refs_from_mapping(raw))
        unit = str(raw.get("unit") or "")
        if unit != str(contract.get("unit") or ""):
            return self._missing(contract, period, "invalid_input", "unit_mismatch", source_refs=_source_refs_from_mapping(raw))
        period_path = str(resolver.get("period_path") or "")
        raw_period = str((_get_path(self.package, period_path) if period_path else None) or raw.get("period") or "")
        source_refs = _source_refs_from_mapping(raw)
        if not source_refs:
            return self._missing(contract, period, "insufficient_evidence", "package_point_lineage_missing")
        if not raw_period:
            return self._missing(contract, period, "insufficient_evidence", "package_point_period_missing", source_refs=source_refs)
        if raw_period != period:
            return self._missing(contract, period, "invalid_input", "period_mismatch", source_refs=_source_refs_from_mapping(raw))
        return self._available(contract, period, float(value), "source_backed", "source_backed", source_refs, ())

    def _lagged(self, contract: Mapping[str, Any], resolver: Mapping[str, Any], period: str) -> ResolvedMetric:
        shifted = _shift_quarter(period, int(resolver.get("lag") or 0))
        if not shifted:
            return self._missing(contract, period, "invalid_input", "period_mismatch")
        source = self.resolve(str(resolver.get("input_metric_id") or ""), shifted)
        return self._copy_result(contract, period, source, "lagged")

    def _relative_change(self, contract: Mapping[str, Any], resolver: Mapping[str, Any], period: str) -> ResolvedMetric:
        current = self.resolve(str(resolver.get("current_metric_id") or ""), period)
        prior = self.resolve(str(resolver.get("prior_metric_id") or ""), period)
        failure = self._input_failure(contract, period, (current, prior))
        if failure:
            return failure
        if current.unit != prior.unit:
            return self._missing_from_inputs(contract, period, "invalid_input", "unit_mismatch", (current, prior))
        denominator_rule = str(resolver.get("denominator_rule") or "nonzero")
        denominator = float(prior.value)
        if denominator_rule == "positive" and denominator <= 0:
            return self._missing_from_inputs(contract, period, "invalid_input", "nonpositive_denominator", (current, prior))
        if denominator_rule == "prior_positive" and denominator <= 0:
            return self._missing_from_inputs(contract, period, "invalid_input", "nonpositive_prior_denominator", (current, prior))
        if abs(denominator) <= 1e-12:
            return self._missing_from_inputs(contract, period, "invalid_input", "zero_denominator", (current, prior))
        scale = float(resolver.get("scale") if resolver.get("scale") is not None else 1.0)
        value = ((float(current.value) - denominator) / abs(denominator)) * scale
        return self._derived(contract, period, value, "relative_change", (current, prior))

    def _scaled_change(self, contract: Mapping[str, Any], resolver: Mapping[str, Any], period: str) -> ResolvedMetric:
        current = self.resolve(str(resolver.get("current_metric_id") or ""), period)
        prior = self.resolve(str(resolver.get("prior_metric_id") or ""), period)
        failure = self._input_failure(contract, period, (current, prior))
        if failure:
            return failure
        if current.unit != prior.unit:
            return self._missing_from_inputs(contract, period, "invalid_input", "unit_mismatch", (current, prior))
        value = (float(current.value) - float(prior.value)) * float(resolver.get("scale") or 1.0)
        return self._derived(contract, period, value, "scaled_change", (current, prior))

    def _ratio(self, contract: Mapping[str, Any], resolver: Mapping[str, Any], period: str) -> ResolvedMetric:
        numerator = self.resolve(str(resolver.get("numerator_metric_id") or ""), period)
        denominator = self.resolve(str(resolver.get("denominator_metric_id") or ""), period)
        failure = self._input_failure(contract, period, (numerator, denominator))
        if failure:
            return failure
        if not _ratio_units_compatible(numerator.unit, denominator.unit, str(contract.get("unit") or "")):
            return self._missing_from_inputs(contract, period, "invalid_input", "unit_mismatch", (numerator, denominator))
        denominator_value = float(denominator.value)
        if str(resolver.get("denominator_rule") or "nonzero") == "positive" and denominator_value <= 0:
            return self._missing_from_inputs(contract, period, "invalid_input", "nonpositive_denominator", (numerator, denominator))
        if abs(denominator_value) <= 1e-12:
            return self._missing_from_inputs(contract, period, "invalid_input", "zero_denominator", (numerator, denominator))
        return self._derived(contract, period, float(numerator.value) / denominator_value, "ratio", (numerator, denominator))

    def _rolling_sum(self, contract: Mapping[str, Any], resolver: Mapping[str, Any], period: str) -> ResolvedMetric:
        values = self._window(str(resolver.get("input_metric_id") or ""), period, int(resolver.get("window") or 0))
        failure = self._input_failure(contract, period, values)
        if failure:
            return failure
        if len({row.unit for row in values}) != 1:
            return self._missing_from_inputs(contract, period, "invalid_input", "unit_mismatch", values)
        return self._derived(contract, period, sum(float(row.value) for row in values), "rolling_sum", values)

    def _positive_window_count(self, contract: Mapping[str, Any], resolver: Mapping[str, Any], period: str) -> ResolvedMetric:
        values = self._window(str(resolver.get("input_metric_id") or ""), period, int(resolver.get("window") or 0))
        failure = self._input_failure(contract, period, values)
        if failure:
            return failure
        return self._derived(contract, period, float(sum(float(row.value) > 0 for row in values)), "positive_window_count", values)

    def _nondecreasing_streak(self, contract: Mapping[str, Any], resolver: Mapping[str, Any], period: str) -> ResolvedMetric:
        metric_id = str(resolver.get("input_metric_id") or "")
        current = self.resolve(metric_id, period)
        if not current.available:
            return self._missing_from_inputs(contract, period, current.status, current.reason, (current,))
        values = [current]
        for lag in range(1, int(resolver.get("max_periods") or 1)):
            shifted = _shift_quarter(period, lag)
            if not shifted:
                break
            previous = self.resolve(metric_id, shifted)
            if not previous.available or previous.unit != current.unit or float(values[-1].value) < float(previous.value):
                break
            values.append(previous)
        return self._derived(contract, period, float(len(values)), "nondecreasing_streak", values)

    def _max_constant_scaled(self, contract: Mapping[str, Any], resolver: Mapping[str, Any], period: str) -> ResolvedMetric:
        source = self.resolve(str(resolver.get("scaled_metric_id") or ""), period)
        if not source.available:
            return self._missing_from_inputs(contract, period, source.status, source.reason, (source,))
        value = max(float(resolver.get("constant") or 0), float(source.value) * float(resolver.get("scale") or 0))
        return self._derived(contract, period, value, "max_constant_scaled", (source,))

    def _market_cap(self, contract: Mapping[str, Any], resolver: Mapping[str, Any], period: str) -> ResolvedMetric:
        explicit = self.resolve(str(resolver.get("explicit_metric_id") or ""), period)
        if explicit.available:
            return self._copy_result(contract, period, explicit, "explicit_market_cap")
        if explicit.status == "invalid_input":
            return self._missing_from_inputs(contract, period, "invalid_input", explicit.reason, (explicit,))
        price = self.resolve(str(resolver.get("price_metric_id") or ""), period)
        shares = self.resolve(str(resolver.get("shares_metric_id") or ""), period)
        failure = self._input_failure(contract, period, (price, shares))
        if failure:
            return failure
        if price.unit != "$/share" or shares.unit != "m shares" or price.period != shares.period:
            return self._missing_from_inputs(contract, period, "invalid_input", "unit_mismatch", (price, shares))
        return self._derived(contract, period, float(price.value) * float(shares.value), "price_times_shares", (price, shares))

    def _window(self, metric_id: str, period: str, window: int) -> tuple[ResolvedMetric, ...]:
        rows: list[ResolvedMetric] = []
        for lag in range(window):
            shifted = _shift_quarter(period, lag)
            if not shifted:
                return tuple(rows)
            rows.append(self.resolve(metric_id, shifted))
        return tuple(rows)

    def _raw_source_records(self, metric_id: str, period: str) -> list[Mapping[str, Any]]:
        return [
            row
            for row in ((self.package.get("calculation_history") or {}).get("quarterly_items") or [])
            if isinstance(row, Mapping)
            and str(row.get("metric") or "") == metric_id
            and str(row.get("period") or "") == period
        ]

    def _input_failure(
        self,
        contract: Mapping[str, Any],
        period: str,
        inputs: Sequence[ResolvedMetric],
    ) -> ResolvedMetric | None:
        if inputs and all(row.available for row in inputs):
            return None
        if any(row.status == "invalid_input" for row in inputs):
            status = "invalid_input"
        elif any(row.status == "insufficient_evidence" for row in inputs):
            status = "insufficient_evidence"
        else:
            status = "unavailable"
        reasons = sorted({row.reason for row in inputs if not row.available})
        return self._missing_from_inputs(contract, period, status, "+".join(reasons) or "required_input_missing", inputs)

    def _copy_result(self, contract: Mapping[str, Any], period: str, source: ResolvedMetric, reason: str) -> ResolvedMetric:
        if not source.available:
            return self._missing_from_inputs(contract, period, source.status, source.reason, (source,))
        if source.unit != str(contract.get("unit") or ""):
            return self._missing_from_inputs(contract, period, "invalid_input", "unit_mismatch", (source,))
        return self._available(contract, period, float(source.value), source.status, reason, source.source_refs, source.formula_ids)

    def _derived(
        self,
        contract: Mapping[str, Any],
        period: str,
        value: float,
        reason: str,
        inputs: Sequence[ResolvedMetric],
    ) -> ResolvedMetric:
        return self._available(
            contract,
            period,
            value,
            "derived_calculated",
            reason,
            _metric_source_refs(inputs),
            _metric_formula_ids(inputs),
        )

    def _available(
        self,
        contract: Mapping[str, Any],
        period: str,
        value: float,
        status: str,
        reason: str,
        source_refs: Sequence[str],
        formula_ids: Sequence[str],
    ) -> ResolvedMetric:
        return ResolvedMetric(
            str(contract.get("metric_id") or ""),
            float(value),
            str(contract.get("unit") or ""),
            period,
            str(contract.get("period_role") or "derived"),
            status,
            reason,
            tuple(sorted(set(map(str, source_refs)))),
            tuple(sorted(set(map(str, formula_ids)))),
        )

    def _missing_from_inputs(
        self,
        contract: Mapping[str, Any],
        period: str,
        status: str,
        reason: str,
        inputs: Sequence[ResolvedMetric],
    ) -> ResolvedMetric:
        return self._missing(
            contract,
            period,
            status,
            reason,
            source_refs=_metric_source_refs(inputs),
            formula_ids=_metric_formula_ids(inputs),
        )

    def _missing(
        self,
        contract: Mapping[str, Any],
        period: str,
        status: str,
        reason: str,
        *,
        source_refs: Sequence[str] = (),
        formula_ids: Sequence[str] = (),
    ) -> ResolvedMetric:
        return ResolvedMetric(
            str(contract.get("metric_id") or ""),
            None,
            str(contract.get("unit") or ""),
            period,
            str(contract.get("period_role") or "derived"),
            status,
            reason,
            tuple(sorted(set(map(str, source_refs)))),
            tuple(sorted(set(map(str, formula_ids)))),
        )


def evaluate_hidden_value_signals(
    package: Mapping[str, Any],
    *,
    profile_id: str,
    ticker: str = "",
    contract_path: Path | str = DEFAULT_HIDDEN_VALUE_CONTRACT,
    module_manifest_path: Path | str = DEFAULT_MODULE_MANIFEST,
) -> HiddenValueEvaluationPlan:
    """Evaluate A-G and produce deterministic future-workbook JSON projections."""

    manifest = load_workbook_module_manifest(module_manifest_path)
    profile = resolve_module_profile(manifest, profile_id)
    contract = load_hidden_value_signal_contract(contract_path, module_manifest_path=module_manifest_path)
    resolver = _MetricResolver(package, contract)
    digest = canonical_json_sha256(contract)
    plan = HiddenValueEvaluationPlan(
        ticker=str(ticker or ((package.get("ticker_metadata") or {}).get("ticker") or "")),
        profile_id=profile.profile_id,
        as_of_period=resolver.latest_period,
        contract_digest=digest,
        plan_version=str(contract.get("plan_version") or ""),
    )
    if str(contract.get("owner_module_id") or "") not in set(profile.enabled_modules):
        _finalize_plan(plan, resolver)
        _validate_plan(plan)
        return plan
    if not resolver.latest_period:
        raise HiddenValueEvaluationError("Hidden Value evaluation requires at least one source-backed quarter.")

    seen_candidates: set[str] = set()
    for signal in sorted(contract.get("signals") or [], key=lambda row: (int(row.get("priority") or 0), str(row.get("signal_id") or ""))):
        candidate = _evaluate_signal(signal, resolver, profile.profile_id, set(profile.enabled_modules), set(profile.profile_pack_ids), contract)
        if candidate.candidate_key in seen_candidates:
            raise HiddenValueEvaluationError(f"Duplicate Hidden Value candidate {candidate.candidate_key!r}.")
        seen_candidates.add(candidate.candidate_key)
        plan.candidates.append(candidate)
    _finalize_plan(plan, resolver)
    _validate_plan(plan)
    return plan


def _evaluate_signal(
    signal: Mapping[str, Any],
    resolver: _MetricResolver,
    profile_id: str,
    enabled_modules: set[str],
    enabled_packs: set[str],
    contract: Mapping[str, Any],
) -> HiddenValueCandidate:
    signal_id = str(signal.get("signal_id") or "")
    period = resolver.latest_period
    identity_values = {
        "signal_id": signal_id,
        "profile_id": profile_id,
        "as_of_period": period,
        "dimension_id": "total_company",
        "member": "total_company",
    }
    deduplication_fields = tuple(map(str, signal.get("deduplication_fields") or []))
    unknown_deduplication_fields = sorted(set(deduplication_fields) - set(identity_values))
    if unknown_deduplication_fields:
        raise HiddenValueEvaluationError(
            f"Signal {signal_id!r} has unknown deduplication fields {unknown_deduplication_fields!r}."
        )
    candidate_key = "|".join(identity_values[field_name] for field_name in deduplication_fields)
    priority = int(signal.get("priority") or 0)
    missing_modules = sorted(set(map(str, signal.get("required_modules") or [])) - enabled_modules)
    required_packs = set(map(str, signal.get("profile_pack_ids") or []))
    missing_packs = sorted(required_packs - enabled_packs)
    metric_ids = list(dict.fromkeys(map(str, (signal.get("required_metric_ids") or []) + (signal.get("optional_metric_ids") or []))))
    resolved = tuple(resolver.resolve(metric_id, period) for metric_id in metric_ids)
    by_metric = {row.metric_id: row for row in resolved}
    if missing_modules or missing_packs:
        reasons = tuple(
            ([f"required_modules_disabled:{','.join(missing_modules)}"] if missing_modules else [])
            + ([f"required_profile_packs_disabled:{','.join(missing_packs)}"] if missing_packs else [])
        )
        predicates = _suppressed_predicates(signal, "module_or_profile_pack_disabled")
        components = _suppressed_components(signal, "module_or_profile_pack_disabled")
        return _candidate(
            signal,
            profile_id,
            period,
            candidate_key,
            "unavailable",
            False,
            None,
            0,
            reasons,
            resolved,
            predicates,
            components,
            contract,
        )

    eligibility_results, eligibility_passed = _predicate_results("eligibility", signal.get("eligibility") or {}, by_metric)
    trigger_results, trigger_passed = _predicate_results("trigger", signal.get("trigger") or {}, by_metric)
    near_results, near_passed = _predicate_results("near_miss", signal.get("near_miss") or {}, by_metric)
    all_predicates = eligibility_results + trigger_results + near_results
    component_scores, score, denominator = _score_components(signal, by_metric)

    required_rows = [by_metric[metric_id] for metric_id in map(str, signal.get("required_metric_ids") or [])]
    unavailable = [row for row in required_rows if not row.available]
    if unavailable:
        if any(row.status == "invalid_input" for row in unavailable):
            state = "invalid_input"
        elif any(row.status == "insufficient_evidence" for row in unavailable):
            state = "insufficient_evidence"
        else:
            state = "unavailable"
        reasons = tuple(sorted({f"{row.metric_id}:{row.reason}" for row in unavailable}))
        return _candidate(
            signal,
            profile_id,
            period,
            candidate_key,
            state,
            False,
            None,
            denominator,
            reasons,
            resolved,
            all_predicates,
            component_scores,
            contract,
        )

    if not eligibility_passed:
        reasons = tuple(f"eligibility_failed:{row.predicate_id}" for row in eligibility_results if row.passed is False)
        return _candidate(
            signal,
            profile_id,
            period,
            candidate_key,
            "not_triggered",
            False,
            None,
            denominator,
            reasons,
            resolved,
            all_predicates,
            component_scores,
            contract,
        )

    trigger_true_count = sum(row.passed is True for row in trigger_results)
    minimum_trigger_predicates = int((signal.get("near_miss") or {}).get("minimum_trigger_predicates") or 0)
    if near_passed and trigger_true_count < minimum_trigger_predicates:
        near_passed = False
    available_trigger_results = [row for row in trigger_results if row.passed is not None]
    if not available_trigger_results:
        return _candidate(
            signal,
            profile_id,
            period,
            candidate_key,
            "insufficient_evidence",
            False,
            None,
            0,
            ("no_economically_comparable_trigger_metric",),
            resolved,
            all_predicates,
            component_scores,
            contract,
        )

    if score is None and trigger_passed:
        state = "insufficient_evidence"
        triggered = False
        reasons = ("trigger_predicates_passed_but_score_not_calculable",)
    elif trigger_passed:
        state = "triggered"
        triggered = True
        reasons = ("all_trigger_requirements_satisfied",)
    elif near_passed:
        state = "near_miss"
        triggered = False
        reasons = ("near_miss_requirements_satisfied",)
    else:
        state = "not_triggered"
        triggered = False
        reasons = tuple(f"trigger_failed:{row.predicate_id}" for row in trigger_results if row.passed is False)
    return _candidate(
        signal,
        profile_id,
        period,
        candidate_key,
        state,
        triggered,
        score,
        denominator,
        reasons,
        resolved,
        all_predicates,
        component_scores,
        contract,
    )


def _candidate(
    signal: Mapping[str, Any],
    profile_id: str,
    period: str,
    candidate_key: str,
    state: str,
    triggered: bool,
    score: int | None,
    denominator: float,
    reasons: Sequence[str],
    resolved: Sequence[ResolvedMetric],
    predicates: Sequence[PredicateResult],
    components: Sequence[ComponentScore],
    contract: Mapping[str, Any],
) -> HiddenValueCandidate:
    source_refs = _metric_source_refs(resolved)
    return HiddenValueCandidate(
        candidate_key=candidate_key,
        signal_id=str(signal.get("signal_id") or ""),
        display_name=str(signal.get("display_name") or ""),
        profile_id=profile_id,
        as_of_period=period,
        state=state,
        triggered=triggered,
        score=score,
        score_denominator=denominator,
        severity=_severity(score, contract) if score is not None else None,
        priority=int(signal.get("priority") or 0),
        reasons=tuple(reasons),
        evidence_ids=tuple(_evidence_id(ref) for ref in source_refs),
        source_refs=source_refs,
        resolved_inputs=tuple(resolved),
        predicate_results=tuple(predicates),
        component_scores=tuple(components),
    )


def _predicate_results(
    stage: str,
    group: Mapping[str, Any],
    metrics: Mapping[str, ResolvedMetric],
) -> tuple[tuple[PredicateResult, ...], bool]:
    rows: list[PredicateResult] = []
    for predicate in group.get("predicates") or []:
        metric_id = str(predicate.get("metric_id") or "")
        metric = metrics.get(metric_id)
        right_metric_id = str(predicate.get("right_metric_id") or "") or None
        right = metrics.get(right_metric_id) if right_metric_id else None
        comparison = float(predicate.get("threshold")) if predicate.get("threshold") is not None else (float(right.value) if right and right.available else None)
        if metric is None or not metric.available or comparison is None:
            passed: bool | None = None
            reason = "predicate_input_unavailable"
            value = float(metric.value) if metric and metric.available else None
        else:
            value = float(metric.value)
            passed = _compare(value, comparison, str(predicate.get("operator") or ""))
            reason = "passed" if passed else "failed"
        rows.append(
            PredicateResult(
                stage,
                str(predicate.get("predicate_id") or ""),
                metric_id,
                str(predicate.get("operator") or ""),
                value,
                comparison,
                right_metric_id,
                passed,
                reason,
            )
        )
    mode = str(group.get("mode") or "all")
    passed = all(row.passed is True for row in rows) if mode == "all" else any(row.passed is True for row in rows)
    return tuple(rows), passed


def _suppressed_predicates(signal: Mapping[str, Any], reason: str) -> tuple[PredicateResult, ...]:
    rows: list[PredicateResult] = []
    for stage in ("eligibility", "trigger", "near_miss"):
        for predicate in (signal.get(stage) or {}).get("predicates") or []:
            rows.append(
                PredicateResult(
                    stage,
                    str(predicate.get("predicate_id") or ""),
                    str(predicate.get("metric_id") or ""),
                    str(predicate.get("operator") or ""),
                    None,
                    float(predicate.get("threshold")) if predicate.get("threshold") is not None else None,
                    str(predicate.get("right_metric_id") or "") or None,
                    None,
                    reason,
                )
            )
    return tuple(rows)


def _suppressed_components(signal: Mapping[str, Any], reason: str) -> tuple[ComponentScore, ...]:
    return tuple(
        ComponentScore(
            str(component.get("component_id") or ""),
            str(component.get("metric_id") or ""),
            None,
            float(component.get("weight") or 0),
            0,
            None,
            None,
            "unavailable",
            reason,
        )
        for component in signal.get("score_components") or []
    )


def _score_components(
    signal: Mapping[str, Any],
    metrics: Mapping[str, ResolvedMetric],
) -> tuple[tuple[ComponentScore, ...], int | None, float]:
    rows: list[ComponentScore] = []
    denominator = 0.0
    weighted = 0.0
    required_missing = False
    reweight = bool(signal.get("reweight_available_components"))
    for component in signal.get("score_components") or []:
        metric_id = str(component.get("metric_id") or "")
        metric = metrics.get(metric_id)
        weight = float(component.get("weight") or 0)
        required = bool(component.get("required"))
        if metric is None or not metric.available:
            required_missing = required_missing or required
            rows.append(ComponentScore(str(component.get("component_id") or ""), metric_id, None, weight, 0, None, None, "unavailable", metric.reason if metric else "metric_not_resolved"))
            continue
        normalized = _normalized_score(float(metric.value), component.get("normalization") or {})
        included_weight = weight
        denominator += included_weight
        weighted_score = normalized * included_weight
        weighted += weighted_score
        rows.append(ComponentScore(str(component.get("component_id") or ""), metric_id, float(metric.value), weight, included_weight, normalized, weighted_score, "calculated", "calculated"))
    if required_missing or denominator <= 0:
        return tuple(rows), None, denominator
    if not reweight and not math.isclose(denominator, 100.0, rel_tol=1e-12, abs_tol=1e-12):
        return tuple(rows), None, denominator
    score = int(round(max(0.0, min(100.0, weighted * 100.0 / denominator))))
    return tuple(rows), score, denominator


def _finalize_plan(plan: HiddenValueEvaluationPlan, resolver: _MetricResolver) -> None:
    plan.base_projection = [
        row.to_dict()
        for _, row in sorted(resolver.cache.items(), key=lambda item: (item[0][1], item[0][0]))
    ]
    plan.audit_projection = [
        {
            "candidate_key": candidate.candidate_key,
            "signal_id": candidate.signal_id,
            "state": candidate.state,
            "triggered": candidate.triggered,
            "score": candidate.score,
            "score_denominator": candidate.score_denominator,
            "severity": candidate.severity,
            "reasons": list(candidate.reasons),
            "evidence_ids": list(candidate.evidence_ids),
            "source_refs": list(candidate.source_refs),
        }
        for candidate in plan.candidates
    ]
    recompute: list[dict[str, Any]] = []
    for candidate in plan.candidates:
        recompute.extend({"candidate_key": candidate.candidate_key, "record_type": "predicate", **row.to_dict()} for row in candidate.predicate_results)
        recompute.extend({"candidate_key": candidate.candidate_key, "record_type": "score_component", **row.to_dict()} for row in candidate.component_scores)
    plan.recompute_projection = sorted(
        recompute,
        key=lambda row: (
            str(row.get("candidate_key") or ""),
            str(row.get("record_type") or ""),
            str(row.get("stage") or ""),
            str(row.get("predicate_id") or row.get("component_id") or ""),
        ),
    )
    triggered = sorted(
        (candidate for candidate in plan.candidates if candidate.triggered),
        key=lambda row: (-(row.score if row.score is not None else -1), row.priority, row.signal_id, row.candidate_key),
    )
    plan.flags_projection = [
        {
            "candidate_key": row.candidate_key,
            "signal_id": row.signal_id,
            "display_name": row.display_name,
            "score": row.score,
            "triggered": True,
            "state": row.state,
            "severity": row.severity,
            "as_of_period": row.as_of_period,
            "reason": "; ".join(row.reasons),
            "evidence_ids": list(row.evidence_ids),
        }
        for row in triggered
    ]


def _validate_plan(plan: HiddenValueEvaluationPlan) -> None:
    payload = plan.to_dict()
    failures = validate_json_schema(payload, load_json_strict(HIDDEN_VALUE_PLAN_SCHEMA))
    if failures:
        sample = "; ".join(f"{field} {keyword}: {message}" for field, keyword, message in failures[:12])
        raise HiddenValueEvaluationError(f"Hidden Value evaluation plan does not satisfy its schema: {sample}")
    candidate_keys = [row.candidate_key for row in plan.candidates]
    if len(candidate_keys) != len(set(candidate_keys)):
        raise HiddenValueEvaluationError("Hidden Value evaluation plan contains duplicate candidate keys.")
    if any(not row.triggered for row in plan.candidates if row.candidate_key in {item["candidate_key"] for item in plan.flags_projection}):
        raise HiddenValueEvaluationError("Flags projection contains a non-triggered candidate.")


def _severity(score: int, contract: Mapping[str, Any]) -> str:
    for band in sorted(contract.get("severity_bands") or [], key=lambda row: float(row.get("minimum_score") or 0), reverse=True):
        if score >= float(band.get("minimum_score") or 0):
            return str(band.get("severity") or "")
    return ""


def _normalized_score(value: float, normalization: Mapping[str, Any]) -> float:
    threshold = float(normalization.get("threshold") or 0)
    span = float(normalization.get("span") or 1)
    base = float(normalization.get("base") or 0)
    if str(normalization.get("direction") or "higher") == "lower":
        raw = (threshold - value) / span + base
    else:
        raw = (value - threshold) / span + base
    return max(0.0, min(1.0, raw))


def _compare(value: float, comparison: float, operator: str) -> bool:
    return {
        "gt": value > comparison,
        "gte": value >= comparison,
        "lt": value < comparison,
        "lte": value <= comparison,
    }.get(operator, False)


def _shift_quarter(period: str, lag: int) -> str | None:
    if lag < 0 or not (len(period) == 7 and period[-3:] in _QUARTER_SUFFIXES):
        return None
    try:
        year = int(period[:4])
        quarter = int(period[-1])
    except ValueError:
        return None
    ordinal = year * 4 + quarter - 1 - lag
    return f"{ordinal // 4}-Q{ordinal % 4 + 1}"


def _ratio_units_compatible(numerator: str, denominator: str, result: str) -> bool:
    if result in {"%", "x"}:
        return numerator == denominator
    if result == "$/share":
        return numerator == "$m" and denominator == "m shares"
    if result == "$m":
        return numerator == "$/share" and denominator == "m shares"
    return numerator == denominator


def _get_path(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current


def _source_refs_from_mapping(value: Mapping[str, Any]) -> tuple[str, ...]:
    source_ref = str(value.get("source_ref") or "").strip()
    return (source_ref,) if source_ref else ()


def _economic_point_from_source_row(
    row: Mapping[str, Any],
    trusted_statuses: set[str],
) -> EconomicPoint | None:
    status = str(row.get("status") or "").strip().lower()
    unit = str(row.get("unit") or "").strip()
    refs = _source_refs_from_mapping(row)
    value = row.get("value")
    if status not in trusted_statuses or not unit or not refs or not _numeric(value):
        return None
    return EconomicPoint(float(value), unit, refs)


def _is_total_company_row(row: Mapping[str, Any]) -> bool:
    aliases = {"", "total company", "total_company", "total-company", "consolidated"}
    dimension = str(row.get("dimension_id") or row.get("dimension") or "").strip().lower()
    member = str(row.get("member") or "").strip().lower()
    return dimension in aliases and member in aliases


def _metric_source_refs(inputs: Sequence[ResolvedMetric]) -> tuple[str, ...]:
    return tuple(sorted({ref for row in inputs for ref in row.source_refs}))


def _metric_formula_ids(inputs: Sequence[ResolvedMetric]) -> tuple[str, ...]:
    return tuple(sorted({formula_id for row in inputs for formula_id in row.formula_ids}))


def _merge_source_refs(points: Sequence[EconomicPoint]) -> tuple[str, ...]:
    return tuple(sorted({ref for point in points for ref in point.source_refs}))


def _evidence_id(source_ref: str) -> str:
    return "evidence:" + hashlib.sha256(source_ref.encode("utf-8")).hexdigest()[:20]


def _numeric(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))


def _duplicate_issues(values: Sequence[str], label: str) -> list[str]:
    duplicates = sorted(value for value, count in Counter(values).items() if value and count > 1)
    return [f"Duplicate {label} IDs {duplicates!r}."] if duplicates else []


def _cycle_issues(graph: Mapping[str, set[str]]) -> list[str]:
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node: str, path: tuple[str, ...]) -> list[str]:
        if node in visiting:
            return ["Metric resolver dependency cycle: " + " -> ".join(path + (node,)) + "."]
        if node in visited:
            return []
        visiting.add(node)
        for dependency in sorted(graph.get(node) or set()):
            issues = visit(dependency, path + (node,))
            if issues:
                return issues
        visiting.remove(node)
        visited.add(node)
        return []

    for metric_id in sorted(graph):
        issues = visit(metric_id, ())
        if issues:
            return issues
    return []
