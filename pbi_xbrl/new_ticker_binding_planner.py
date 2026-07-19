"""Pre-render binding planner for the generic new-ticker engine.

This module is intentionally JSON-only.  It never imports openpyxl, loads a
workbook, or writes cells.  It converts a normalized package and declared shell
contracts into an auditable plan that a future value-only filler may execute.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from pbi_xbrl.json_schema_validation import load_json_strict, validate_json_schema
from pbi_xbrl.normalized_company_data_validation import (
    FIELD_STATUSES,
    NormalizedDataIssue,
    canonicalize_normalized_scenario_tokens,
    validate_normalized_company_data,
    validate_normalized_company_data_schema,
)
from pbi_xbrl.new_ticker_issue_ledger import build_canonical_issue_ledger
from pbi_xbrl.new_ticker_guidance_scope import (
    CURRENT_GUIDANCE_ROLES,
    current_guidance_indexes,
    guidance_scope_label,
)
from pbi_xbrl.segment_normalization import (
    canonical_segment_dimension_member,
    canonical_segment_display_member,
)
from pbi_xbrl.standard_template_shell_identity import (
    VerifiedShellIdentity,
    compute_binding_contract_signature,
    compute_manifest_contract_signature,
    is_verified_shell_identity,
    validate_verified_shell_token,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BINDING_MAP = ROOT / "docs" / "workbook_binding_map.json"
DEFAULT_MANIFEST = ROOT / "docs" / "standard_template_shell_manifest.json"
DEFAULT_SHELL = ROOT / "templates" / "standard_stock_model_template.xlsx"
ISSUE_LEDGER_SCHEMA = ROOT / "docs" / "new_ticker_issue_ledger.schema.json"
BINDING_PLAN_SCHEMA = ROOT / "docs" / "new_ticker_binding_plan.schema.json"
BINDING_PLAN_VERSION = "1.0.0"
BINDING_PLAN_SNAPSHOT_VERSION = "1.0.0"
_RANGE_RE = re.compile(r"^([A-Z]+)([1-9]\d*)(?::([A-Z]+)([1-9]\d*))?$")
_QUARTER_PERIOD_RE = re.compile(r"^(?P<year>[0-9]{4})-Q(?P<quarter>[1-4])$")
_BLOCKING_SEVERITIES = {"P0", "P1"}
_TABLE_MODES = {"table_rows", "validation_rows"}
_PIVOT_MODES = {"pivot_rows"}
_ROW_MODES = _TABLE_MODES | _PIVOT_MODES | {"series"}
_ROW_CONTRACT_KEYS = {
    "row_selector",
    "row_key",
    "sort_order",
    "capacity",
    "overflow_behavior",
    "required_columns",
    "target_columns",
    "source_ref_required",
}
_QA_SHEETS = {"QA_Log", "Needs_Review", "QA_Checks"}
_SCALAR_DISPOSITION_FIELDS = {"value", "unit", "period", "period_display", "status", "reason", "evidence"}


class BindingPlanningError(RuntimeError):
    """Raised only for an invalid planner invocation, never for workbook IO."""


class BindingPlanReproductionError(BindingPlanningError):
    """Raised when authoritative plan reproduction or comparison fails."""

    def __init__(self, message: str, *, plan: "BindingPlan | None" = None) -> None:
        self.plan = plan
        super().__init__(message)


@dataclass(frozen=True)
class PlannedWrite:
    binding_id: str
    normalized_path: str
    row_key: str
    target_sheet: str
    target_cell: str
    target_type: str
    target_role: str
    value: Any
    value_type: str
    source_ref: str
    capacity_used: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "binding_id": self.binding_id,
            "normalized_path": self.normalized_path,
            "row_key": self.row_key,
            "target_sheet": self.target_sheet,
            "target_cell": self.target_cell,
            "target_type": self.target_type,
            "target_role": self.target_role,
            "value": self.value,
            "value_type": self.value_type,
            "source_ref": self.source_ref,
            "capacity_used": self.capacity_used,
        }


@dataclass(frozen=True)
class _ScalarDispositionContract:
    disposition_id: str
    record_path: str
    metric_id: str
    expected_unit: str
    period_required: bool
    source_ref_required: bool
    period_display_reference_path: str
    period_display_source_path: str


@dataclass(frozen=True)
class ResolvedScalarDisposition:
    disposition_id: str
    business_key: str
    metric_id: str
    record_path: str
    value: int | float | None
    unit: str | None
    period: str | None
    period_display: str | None
    status: str
    reason: str
    source_status: str
    evidence_ids: tuple[str, ...]
    source_refs: tuple[str, ...]
    period_display_source_refs: tuple[str, ...]
    formula_ids: tuple[str, ...]
    validity_code: str

    @property
    def source_ref(self) -> str:
        return self.source_refs[0] if self.source_refs else ""


@dataclass
class BindingPlan:
    ticker: str
    planned_writes: list[PlannedWrite] = field(default_factory=list)
    binding_reports: list[dict[str, Any]] = field(default_factory=list)
    schema_issues: list[NormalizedDataIssue] = field(default_factory=list)
    semantic_issues: list[NormalizedDataIssue] = field(default_factory=list)
    planner_issues: list[NormalizedDataIssue] = field(default_factory=list)
    mapping_gaps: list[dict[str, Any]] = field(default_factory=list)
    manual_review_flags: list[dict[str, Any]] = field(default_factory=list)
    issue_ledger: dict[str, Any] = field(default_factory=dict)
    period_axes: dict[str, dict[str, Any]] = field(default_factory=dict)
    derived_plan_reports: list[dict[str, Any]] = field(default_factory=list)
    shell_identity_report: dict[str, Any] = field(default_factory=dict)
    qa_snapshot_status: str = "not_planned"
    planning_completed: bool = False

    @property
    def issues(self) -> list[NormalizedDataIssue]:
        return [*self.schema_issues, *self.semantic_issues, *self.planner_issues]

    @property
    def status(self) -> str:
        return "FAIL" if self.has_blockers else "PASS"

    @property
    def has_blockers(self) -> bool:
        if any(issue.severity.upper() in _BLOCKING_SEVERITIES for issue in self.issues):
            return True
        return any(
            str(issue.get("severity") or "").upper() in _BLOCKING_SEVERITIES
            or bool(issue.get("promotion_blocking"))
            or bool(issue.get("render_blocking"))
            for issue in self.issue_ledger.get("issues") or []
            if isinstance(issue, Mapping)
        )

    def blocking_issues(self) -> list[NormalizedDataIssue]:
        result = [issue for issue in self.issues if issue.severity.upper() in _BLOCKING_SEVERITIES]
        represented = {(issue.rule_id, issue.field, issue.message) for issue in result}
        for issue in self.issue_ledger.get("issues") or []:
            if not isinstance(issue, Mapping):
                continue
            if not (
                str(issue.get("severity") or "").upper() in _BLOCKING_SEVERITIES
                or bool(issue.get("promotion_blocking"))
                or bool(issue.get("render_blocking"))
            ):
                continue
            normalized = NormalizedDataIssue(
                severity=str(issue.get("severity") or "P1").upper(),
                rule_id=str(issue.get("rule_id") or "canonical_render_blocker"),
                field=str(issue.get("normalized_path") or issue.get("binding_id") or "issue_ledger"),
                message=str(issue.get("message") or "Canonical issue blocks rendering."),
                source_ref=str((issue.get("source_refs") or [""])[0] if issue.get("source_refs") else ""),
                suggested_action=str(issue.get("suggested_action") or "Resolve the canonical issue before rendering."),
            )
            key = (normalized.rule_id, normalized.field, normalized.message)
            if key not in represented:
                represented.add(key)
                result.append(normalized)
        return result

    def to_dict(self) -> dict[str, Any]:
        structured_skip_count = sum(len(report.get("skipped_rows") or []) for report in self.binding_reports)
        overflow_count = sum(len(report.get("overflow_rows") or []) for report in self.binding_reports)
        return {
            "plan_version": BINDING_PLAN_VERSION,
            "ticker": self.ticker,
            "status": self.status,
            "planned_write_count": len(self.planned_writes),
            "structured_skip_count": structured_skip_count,
            "overflow_count": overflow_count,
            "planned_writes": [write.to_dict() for write in self.planned_writes],
            "bindings": self.binding_reports,
            "schema_issues": [issue.to_dict() for issue in self.schema_issues],
            "semantic_issues": [issue.to_dict() for issue in self.semantic_issues],
            "planner_issues": [issue.to_dict() for issue in self.planner_issues],
            "mapping_gaps": self.mapping_gaps,
            "manual_review_flags": self.manual_review_flags,
            "issue_ledger": self.issue_ledger,
            "period_axes": self.period_axes,
            "derived_plans": self.derived_plan_reports,
            "shell_identity": self.shell_identity_report,
            "qa_snapshot_status": self.qa_snapshot_status,
        }


@dataclass(frozen=True)
class BindingPlanSnapshot:
    """Immutable cache of a reproduced plan, never proof of authorization.

    Every trust boundary must independently reproduce the plan from the package,
    approved shell, manifest, and binding map.  The consistency payload is useful
    for diagnostics only; a caller can recompute it.
    """

    _plan_payload_json: str = field(repr=False)
    _consistency_json: str = field(repr=False)

    @property
    def plan_payload(self) -> dict[str, Any]:
        return json.loads(self._plan_payload_json)

    @property
    def consistency(self) -> dict[str, Any]:
        return json.loads(self._consistency_json)

def is_binding_plan_snapshot(value: Any) -> bool:
    """Return whether *value* is a typed snapshot, not whether it is trusted."""

    return isinstance(value, BindingPlanSnapshot)


def plan_standard_template_writes(
    package: Mapping[str, Any],
    *,
    binding_payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    manifest: Mapping[str, Any],
    ticker_override: str | None = None,
    promotion_requested: bool = False,
    shell_identity_report: VerifiedShellIdentity | None = None,
) -> BindingPlan:
    """Plan exact cell writes without opening a workbook.

    A failed plan is an expected result for incomplete source coverage or an
    incompatible shell contract.  It is never repaired by sequential dumping or
    merged-cell concatenation.
    """

    bindings = _bindings_from_payload(binding_payload)
    scalar_disposition_contracts, scalar_contract_issues = _scalar_disposition_contracts(bindings)
    binding_contract = binding_payload if isinstance(binding_payload, Mapping) else {}
    raw_scenario_profiles = binding_contract.get("scenario_profile_packs")
    scenario_driver_map = (
        {
            str(row.get("profile_pack_id") or ""): {
                str(value) for value in row.get("scenario_driver_ids") or []
            }
            for row in raw_scenario_profiles
            if isinstance(row, Mapping) and str(row.get("profile_pack_id") or "")
        }
        if isinstance(raw_scenario_profiles, list)
        else None
    )
    allowed_profile_packs = set(scenario_driver_map) if scenario_driver_map is not None else None
    allowed_scenario_drivers = (
        set().union(*scenario_driver_map.values()) if scenario_driver_map else (set() if scenario_driver_map == {} else None)
    )
    module_profile = manifest.get("module_profile") if isinstance(manifest.get("module_profile"), Mapping) else {}
    allowed_dimensions = {
        str(row.get("dimension_id") or "")
        for row in module_profile.get("dimensions") or []
        if isinstance(row, Mapping) and str(row.get("dimension_id") or "")
    }
    package, token_issues = canonicalize_normalized_scenario_tokens(
        package,
        allowed_profile_pack_ids=allowed_profile_packs,
        allowed_scenario_driver_ids=allowed_scenario_drivers,
        allowed_dimension_ids=allowed_dimensions or None,
    )
    ticker = _ticker(package, ticker_override)
    plan = BindingPlan(ticker=ticker)
    if is_verified_shell_identity(shell_identity_report):
        plan.shell_identity_report = shell_identity_report.to_dict()
    plan.mapping_gaps.extend(_normalize_mapping_gaps(_path_get(package, "mapping_gaps")))
    plan.manual_review_flags.extend(_normalize_manual_review_flags(_path_get(package, "manual_review_flags")))
    plan.schema_issues.extend(validate_normalized_company_data_schema(package))
    plan.semantic_issues.extend(token_issues)
    plan.semantic_issues.extend(
        validate_normalized_company_data(
            package,
            binding_map=bindings,
            allowed_profile_pack_ids=allowed_profile_packs,
            allowed_scenario_driver_ids=allowed_scenario_drivers,
            allowed_scenario_driver_map=scenario_driver_map,
            allowed_dimension_ids=allowed_dimensions or None,
            promotion_requested=promotion_requested,
            validate_schema=False,
            scenario_tokens_canonicalized=True,
        )
    )
    plan.planner_issues.extend(_validate_manifest_and_binding_contracts(manifest, bindings))
    plan.planner_issues.extend(scalar_contract_issues)
    plan.planner_issues.extend(
        _validate_shell_identity_contract(
            manifest,
            shell_identity_report,
            binding_payload=binding_payload,
        )
    )
    _refresh_issue_ledger(plan)

    if plan.has_blockers:
        _add_blocking_reports(plan, bindings)
        return plan

    planning_package: Mapping[str, Any] = package
    profile_id = str(module_profile.get("profile_id") or "")
    if profile_id:
        try:
            from pbi_xbrl.hidden_value_signal_economics import evaluate_hidden_value_signals
            from pbi_xbrl.hidden_value_workbook_projection import build_hidden_value_workbook_projection

            hidden_value_plan = evaluate_hidden_value_signals(
                package,
                profile_id=profile_id,
                ticker=ticker,
            )
            hidden_value_projection = build_hidden_value_workbook_projection(hidden_value_plan)
            projection_payload = hidden_value_projection.to_dict()
            planning_package = dict(package)
            planning_package["_derived_workbook"] = {"hidden_value": projection_payload}
            plan.derived_plan_reports.append(
                {
                    "plan_id": "hidden_value_evaluation",
                    "status": hidden_value_plan.status,
                    "profile_id": hidden_value_plan.profile_id,
                    "as_of_period": hidden_value_plan.as_of_period,
                    "contract_digest": hidden_value_plan.contract_digest,
                    "evaluation_plan_digest": hidden_value_projection.evaluation_plan_digest,
                    "workbook_projection_digest": projection_payload["projection_digest"],
                    "candidate_count": len(hidden_value_plan.candidates),
                    "base_row_count": len(hidden_value_projection.base_rows),
                    "audit_row_count": len(hidden_value_projection.audit_rows),
                    "recompute_row_count": len(hidden_value_projection.recompute_rows),
                    "flags_row_count": len(hidden_value_projection.flags_rows),
                    "state_counts": hidden_value_plan.state_counts,
                }
            )
        except Exception as exc:
            plan.planner_issues.append(
                _planner_issue(
                    "P1",
                    "hidden_value_workbook_projection_failed",
                    "hidden_value_signals",
                    str(exc),
                )
            )
            _refresh_issue_ledger(plan)
            _add_blocking_reports(plan, bindings)
            return plan

    business_bindings = [
        binding
        for binding in bindings
        if bool(binding.get("writable")) and str(binding.get("source_policy") or "") != "validation-output"
    ]
    ordered_bindings = [
        *[binding for binding in business_bindings if str(binding.get("period_axis_role") or "") == "header"],
        *[binding for binding in business_bindings if str(binding.get("period_axis_role") or "") != "header"],
    ]
    scalar_dispositions = {
        disposition_id: _resolve_scalar_disposition(planning_package, contract)
        for disposition_id, contract in scalar_disposition_contracts.items()
    }
    for binding in ordered_bindings:
        report, writes, gaps, issues = _plan_binding(
            planning_package,
            binding,
            ticker=ticker,
            period_axes=plan.period_axes,
            scalar_dispositions=scalar_dispositions,
        )
        plan.binding_reports.append(report)
        plan.planned_writes.extend(writes)
        plan.mapping_gaps.extend(gaps)
        plan.planner_issues.extend(issues)
        if str(binding.get("period_axis_role") or "") == "header" and not issues:
            axis_issues = _register_period_axis(plan, binding, writes)
            plan.planner_issues.extend(axis_issues)

    disposition_report, disposition_issues = _audit_financial_fact_dispositions(
        package,
        binding_payload=binding_payload,
        planned_writes=plan.planned_writes,
        binding_reports=plan.binding_reports,
    )
    plan.binding_reports.append(disposition_report)
    plan.planner_issues.extend(disposition_issues)

    plan.planning_completed = True
    _dedupe_plan(plan)
    _validate_planned_write_types(plan, bindings)
    _refresh_issue_ledger(plan)
    _finalize_validation_outputs(plan, bindings, ticker=ticker)
    return plan


def plan_standard_template_writes_from_paths(
    package_path: Path | str,
    *,
    binding_map_path: Path | str = DEFAULT_BINDING_MAP,
    manifest_path: Path | str = DEFAULT_MANIFEST,
    shell_path: Path | str = DEFAULT_SHELL,
    ticker_override: str | None = None,
    promotion_requested: bool = False,
) -> BindingPlan:
    """Load JSON contracts and build a plan; no workbook file is touched."""

    package = _load_json(Path(package_path))
    binding_payload = _load_json(Path(binding_map_path))
    manifest = _load_json(Path(manifest_path))
    from pbi_xbrl.standard_template_shell_identity import verify_shell_identity

    shell_identity_report = verify_shell_identity(
        Path(shell_path),
        manifest=manifest,
        binding_payload=binding_payload,
    )
    return plan_standard_template_writes(
        package,
        binding_payload=binding_payload,
        manifest=manifest,
        ticker_override=ticker_override,
        promotion_requested=promotion_requested,
        shell_identity_report=shell_identity_report,
    )


def reproduce_binding_plan(
    package: Mapping[str, Any],
    *,
    binding_payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    manifest: Mapping[str, Any],
    shell_path: Path | str = DEFAULT_SHELL,
    ticker_override: str | None = None,
    promotion_requested: bool = False,
    expected_plan: Mapping[str, Any] | BindingPlan | BindingPlanSnapshot | None = None,
) -> BindingPlan:
    """Independently reproduce an exact PASS plan from authoritative inputs.

    Serialized plans and typed snapshots are cache/audit outputs only.  When one
    is supplied it must match the independently reproduced plan exactly.
    """

    from pbi_xbrl.standard_template_shell_identity import verify_shell_identity

    shell_identity_report = verify_shell_identity(
        Path(shell_path),
        manifest=manifest,
        binding_payload=binding_payload,
    )

    plan = plan_standard_template_writes(
        package,
        binding_payload=binding_payload,
        manifest=manifest,
        ticker_override=ticker_override,
        promotion_requested=promotion_requested,
        shell_identity_report=shell_identity_report,
    )
    plan_payload = plan.to_dict()
    if expected_plan is not None:
        if isinstance(expected_plan, BindingPlan):
            expected_payload: Any = expected_plan.to_dict()
        elif isinstance(expected_plan, BindingPlanSnapshot):
            expected_payload = expected_plan.plan_payload
        else:
            expected_payload = expected_plan
        if not isinstance(expected_payload, Mapping):
            raise BindingPlanReproductionError("Expected binding plan must be a mapping or BindingPlan.", plan=plan)
        if _canonical_json(expected_payload) != _canonical_json(plan_payload):
            raise BindingPlanReproductionError(
                "Serialized binding plan differs from the independently reproduced authoritative plan.",
                plan=plan,
            )
    if plan.status != "PASS" or plan.has_blockers:
        raise BindingPlanReproductionError("Only a completed PASS plan without final-ledger blockers can be executed.", plan=plan)
    return plan


def reproduce_binding_plan_snapshot(
    package: Mapping[str, Any],
    *,
    binding_payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    manifest: Mapping[str, Any],
    shell_path: Path | str = DEFAULT_SHELL,
    shell_identity_report: VerifiedShellIdentity | None = None,
    ticker_override: str | None = None,
    promotion_requested: bool = False,
    expected_plan: Mapping[str, Any] | BindingPlan | BindingPlanSnapshot | None = None,
) -> tuple[BindingPlan, BindingPlanSnapshot]:
    """Reproduce a plan and return an optional diagnostic cache snapshot."""

    plan = reproduce_binding_plan(
        package,
        binding_payload=binding_payload,
        manifest=manifest,
        shell_path=shell_path,
        ticker_override=ticker_override,
        promotion_requested=promotion_requested,
        expected_plan=expected_plan,
    )
    from pbi_xbrl.standard_template_shell_identity import verify_shell_identity

    actual_shell_identity = verify_shell_identity(
        Path(shell_path),
        manifest=manifest,
        binding_payload=binding_payload,
    )
    if shell_identity_report is not None and shell_identity_report.to_dict() != actual_shell_identity.to_dict():
        raise BindingPlanReproductionError(
            "Caller-supplied shell identity differs from independent shell verification.",
            plan=plan,
        )
    snapshot = _build_binding_plan_snapshot(
        plan.to_dict(),
        normalized_package=package,
        manifest=manifest,
        binding_payload=binding_payload,
        shell_identity_report=actual_shell_identity,
    )
    return plan, snapshot


def compare_binding_plan_snapshot(
    snapshot: BindingPlanSnapshot | None,
    *,
    normalized_package: Mapping[str, Any],
    manifest: Mapping[str, Any],
    binding_payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    shell_path: Path | str = DEFAULT_SHELL,
    shell_identity_report: VerifiedShellIdentity | None = None,
) -> list[dict[str, str]]:
    """Compare a cached snapshot with an independently reproduced plan."""

    if not is_binding_plan_snapshot(snapshot):
        return [{"rule_id": "binding_plan_snapshot_required", "message": "A BindingPlanSnapshot is required for comparison."}]
    assert snapshot is not None
    try:
        reproduce_binding_plan(
            normalized_package,
            binding_payload=binding_payload,
            manifest=manifest,
            shell_path=shell_path,
            expected_plan=snapshot,
        )
    except Exception as exc:
        return [{"rule_id": "binding_plan_reproduction_mismatch", "message": str(exc)}]
    if shell_identity_report is not None:
        from pbi_xbrl.standard_template_shell_identity import verify_shell_identity

        actual = verify_shell_identity(Path(shell_path), manifest=manifest, binding_payload=binding_payload)
        if shell_identity_report.to_dict() != actual.to_dict():
            return [{"rule_id": "binding_plan_shell_snapshot_mismatch", "message": "Cached shell identity differs from independent verification."}]
    return []


def _build_binding_plan_snapshot(
    plan_payload: Mapping[str, Any],
    *,
    normalized_package: Mapping[str, Any],
    manifest: Mapping[str, Any],
    binding_payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    shell_identity_report: VerifiedShellIdentity,
) -> BindingPlanSnapshot:
    consistency = _binding_plan_consistency_payload(
        plan_payload,
        normalized_package=normalized_package,
        manifest=manifest,
        binding_payload=binding_payload,
        shell_identity_report=shell_identity_report,
    )
    consistency["consistency_digest"] = _payload_digest(consistency)
    return BindingPlanSnapshot(
        _plan_payload_json=_canonical_json(plan_payload),
        _consistency_json=_canonical_json(consistency),
    )


def _binding_plan_consistency_payload(
    plan_payload: Mapping[str, Any],
    *,
    normalized_package: Mapping[str, Any],
    manifest: Mapping[str, Any],
    binding_payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    shell_identity_report: VerifiedShellIdentity,
) -> dict[str, Any]:
    exclusions = [
        {
            "binding_id": str(report.get("binding_id") or ""),
            "skipped_rows": report.get("skipped_rows") or [],
            "overflow_rows": report.get("overflow_rows") or [],
        }
        for report in plan_payload.get("bindings") or []
        if isinstance(report, Mapping)
    ]
    binding_version = (
        str(binding_payload.get("binding_planner_contract_version") or "")
        if isinstance(binding_payload, Mapping)
        else "legacy-sequence-contract"
    )
    shell_contract = {
        "status": shell_identity_report.status,
        "expected": shell_identity_report.expected,
        "actual": shell_identity_report.actual,
    }
    return {
        "snapshot_version": BINDING_PLAN_SNAPSHOT_VERSION,
        "normalized_package_digest": _payload_digest(normalized_package),
        "manifest_contract_digest": compute_manifest_contract_signature(manifest),
        "shell_identity_digest": _payload_digest(shell_contract),
        "binding_contract_digest": compute_binding_contract_signature(binding_payload),
        "binding_planner_contract_version": binding_version,
        "plan_version": str(plan_payload.get("plan_version") or ""),
        "plan_status": str(plan_payload.get("status") or ""),
        "has_blockers": _plan_payload_has_blockers(plan_payload),
        "plan_digest": _payload_digest(plan_payload),
        "planned_writes_digest": _payload_digest(plan_payload.get("planned_writes") or []),
        "exclusions_digest": _payload_digest(exclusions),
        "final_ledger_digest": _payload_digest(plan_payload.get("issue_ledger") or {}),
    }


def _plan_payload_has_blockers(plan_payload: Mapping[str, Any]) -> bool:
    ledger = plan_payload.get("issue_ledger") if isinstance(plan_payload.get("issue_ledger"), Mapping) else {}
    return any(
        str(issue.get("severity") or "").upper() in _BLOCKING_SEVERITIES
        or bool(issue.get("promotion_blocking"))
        or bool(issue.get("render_blocking"))
        for issue in ledger.get("issues") or []
        if isinstance(issue, Mapping)
    )


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)


def _payload_digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _validate_shell_identity_contract(
    manifest: Mapping[str, Any],
    report: VerifiedShellIdentity | None,
    *,
    binding_payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
) -> list[NormalizedDataIssue]:
    if not isinstance(manifest.get("shell_identity"), Mapping):
        return [
            _planner_issue(
                "P1",
                "shell_identity_missing",
                "shell_identity",
                "Planning requires a manifest with an approved shell_identity contract.",
            )
        ]
    if not is_verified_shell_identity(report):
        return [
            _planner_issue(
                "P1",
                "shell_identity_not_verified",
                "shell_identity",
                "Planning requires verification against the exact frozen shell artifact.",
            )
        ]
    token_issues = validate_verified_shell_token(
        report,
        manifest=manifest,
        binding_payload=binding_payload,
    )
    issues = [
        _planner_issue(
            "P1",
            str(issue.get("rule_id") or "shell_identity_failure"),
            "shell_identity",
            str(issue.get("message") or "Shell identity verification failed."),
        )
        for issue in token_issues
    ]
    return issues


def write_binding_plan_report(plan: BindingPlan, output_path: Path | str) -> Path:
    """Persist only a JSON report; this helper never creates an Excel file."""

    payload = plan.to_dict()
    failures = validate_json_schema(payload, load_json_strict(BINDING_PLAN_SCHEMA))
    if failures:
        sample = "; ".join(f"{field} {keyword}: {message}" for field, keyword, message in failures[:8])
        raise BindingPlanningError(f"Binding plan does not satisfy its JSON Schema: {sample}")
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
    return path


def _register_period_axis(
    plan: BindingPlan,
    binding: Mapping[str, Any],
    writes: Sequence[PlannedWrite],
) -> list[NormalizedDataIssue]:
    axis_id = str(binding.get("period_axis_id") or "")
    if not axis_id:
        return [_planner_issue("P1", "binding_period_axis_id_missing", str(binding.get("binding_id") or ""), "Period-axis header binding has no period_axis_id.")]
    if axis_id in plan.period_axes:
        return [_planner_issue("P1", "binding_period_axis_duplicate", axis_id, "More than one header binding resolved the same period axis.")]
    if not writes:
        return [_planner_issue("P1", "binding_period_axis_empty", axis_id, "Period-axis header binding produced no visible periods.")]
    period_to_column: dict[str, str] = {}
    period_to_cell: dict[str, str] = {}
    for write in writes:
        period = str(write.value or "")
        match = _RANGE_RE.fullmatch(write.target_cell)
        if not period or match is None:
            return [_planner_issue("P1", "binding_period_axis_invalid_write", axis_id, "Period-axis header writes must contain a period and one exact target cell.")]
        column = match.group(1)
        if period in period_to_column:
            return [_planner_issue("P1", "binding_period_axis_duplicate_period", axis_id, f"Period {period!r} appears more than once in the visible axis.")]
        period_to_column[period] = column
        period_to_cell[period] = write.target_cell
    continuity = str(binding.get("period_axis_continuity") or "")
    periods = list(period_to_column)
    if continuity:
        ordinals: list[int] = []
        for period in periods:
            if continuity == "consecutive_quarters":
                match = re.fullmatch(r"(\d{4})-Q([1-4])", period)
                ordinal = int(match.group(1)) * 4 + int(match.group(2)) - 1 if match else None
            elif continuity == "consecutive_fiscal_years":
                match = re.fullmatch(r"(\d{4})-FY", period)
                ordinal = int(match.group(1)) if match else None
            else:
                return [_planner_issue("P1", "binding_period_axis_continuity_invalid", axis_id, f"Unsupported continuity contract {continuity!r}.")]
            if ordinal is None:
                return [_planner_issue("P1", "binding_period_axis_period_invalid", axis_id, f"Period {period!r} does not satisfy {continuity}.")]
            ordinals.append(ordinal)
        if any(ordinals[index + 1] - ordinals[index] != 1 for index in range(len(ordinals) - 1)):
            return [_planner_issue("P1", "binding_period_axis_not_consecutive", axis_id, f"Resolved periods are not {continuity}: {periods!r}.")]
    plan.period_axes[axis_id] = {
        "period_axis_id": axis_id,
        "header_binding_id": str(binding.get("binding_id") or ""),
        "sheet": str(binding.get("sheet") or ""),
        "periods": periods,
        "continuity": continuity,
        "period_to_column": period_to_column,
        "period_to_cell": period_to_cell,
    }
    return []


def _audit_financial_fact_dispositions(
    package: Mapping[str, Any],
    *,
    binding_payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    planned_writes: Sequence[PlannedWrite],
    binding_reports: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], list[NormalizedDataIssue]]:
    """Require an explicit terminal disposition for every populated fact."""

    raw_dispositions = binding_payload.get("financial_field_dispositions") if isinstance(binding_payload, Mapping) else []
    dispositions: dict[tuple[str, str], Mapping[str, Any]] = {}
    issues: list[NormalizedDataIssue] = []
    for index, raw in enumerate(raw_dispositions or []):
        if not isinstance(raw, Mapping):
            issues.append(_planner_issue("P1", "financial_disposition_invalid", f"financial_field_dispositions.{index}", "Financial disposition entries must be objects."))
            continue
        section = str(raw.get("section") or "")
        field_name = str(raw.get("field") or "")
        disposition = str(raw.get("disposition") or "")
        reason = str(raw.get("reason") or "")
        key = (section, field_name)
        if not section or not field_name or disposition not in {"formula_owned", "audit_only", "explicitly_excluded"} or not reason:
            issues.append(_planner_issue("P1", "financial_disposition_invalid", f"financial_field_dispositions.{index}", "Disposition requires section, field, an allowed disposition, and a business reason."))
            continue
        if key in dispositions:
            issues.append(_planner_issue("P1", "financial_disposition_duplicate", f"{section}.{field_name}", "Financial disposition is declared more than once."))
            continue
        dispositions[key] = raw

    planned_paths = {write.normalized_path for write in planned_writes}
    selector_exclusions: dict[str, Mapping[str, Any]] = {}
    for report in binding_reports:
        for skipped in report.get("skipped_rows") or []:
            if not isinstance(skipped, Mapping):
                continue
            normalized_path = str(skipped.get("normalized_path") or "")
            reason = str(skipped.get("reason") or "")
            if normalized_path and reason:
                selector_exclusions.setdefault(normalized_path, skipped)
    skipped_rows: list[dict[str, Any]] = []
    populated_count = 0
    planned_count = 0
    for section in ("quarterly_financials", "annual_financials"):
        rows = _path_get(package, f"{section}.rows")
        if not isinstance(rows, list):
            continue
        for row_index, row in enumerate(rows):
            if not isinstance(row, Mapping):
                continue
            period = str(row.get("period") or "")
            for field_name, node in row.items():
                if not isinstance(node, Mapping) or str(node.get("status") or "") != "populated" or node.get("value") in (None, ""):
                    continue
                populated_count += 1
                normalized_path = f"{section}.rows.{row_index}.{field_name}"
                normalized_row_path = f"{section}.rows.{row_index}"
                if normalized_path in planned_paths:
                    planned_count += 1
                    continue
                disposition = dispositions.get((section, str(field_name)))
                if disposition is not None:
                    skipped_rows.append(
                        {
                            "binding_id": "financial_fact_disposition_audit",
                            "section": section,
                            "field": str(field_name),
                            "normalized_path": normalized_path,
                            "row_key": period,
                            "source_ref": str(node.get("source_ref") or ""),
                            "reason": str(disposition.get("reason") or ""),
                            "disposition": str(disposition.get("disposition") or ""),
                            "severity": "P2",
                        }
                    )
                    continue
                selector_exclusion = selector_exclusions.get(normalized_path) or selector_exclusions.get(normalized_row_path)
                if selector_exclusion is not None:
                    selector_reason = str(selector_exclusion.get("reason") or "")
                    skipped_rows.append(
                        {
                            "binding_id": "financial_fact_disposition_audit",
                            "section": section,
                            "field": str(field_name),
                            "normalized_path": normalized_path,
                            "row_key": str(selector_exclusion.get("row_key") or period),
                            "source_ref": str(selector_exclusion.get("source_ref") or node.get("source_ref") or ""),
                            "reason": f"selector_exclusion_audit_only:{selector_reason}",
                            "disposition": "audit_only",
                            "severity": str(selector_exclusion.get("severity") or "P2"),
                            "selector_exclusion": True,
                        }
                    )
                    continue
                issues.append(
                    _planner_issue(
                        "P1",
                        "populated_financial_fact_without_disposition",
                        normalized_path,
                        "Populated financial fact is neither planned, formula-owned, audit-only, nor explicitly excluded.",
                        normalized_path=normalized_path,
                        business_row_key=period,
                        source_ref=str(node.get("source_ref") or ""),
                        root_cause="missing_financial_fact_disposition",
                    )
                )
    report = {
        "binding_id": "financial_fact_disposition_audit",
        "mode": "disposition_audit",
        "sheet": "",
        "target": "",
        "normalized_field": "quarterly_financials.rows + annual_financials.rows",
        "capacity": 0,
        "capacity_used": planned_count,
        "overflow_rows": [],
        "skipped_rows": skipped_rows,
        "planned_write_count": 0,
        "populated_fact_count": populated_count,
        "planned_fact_count": planned_count,
        "explicit_disposition_count": len(skipped_rows),
        "selector_exclusion_count": sum(bool(row.get("selector_exclusion")) for row in skipped_rows),
        "unresolved_fact_count": sum(1 for issue in issues if issue.rule_id == "populated_financial_fact_without_disposition"),
    }
    return report, issues


def _plan_binding(
    package: Mapping[str, Any],
    binding: Mapping[str, Any],
    *,
    ticker: str,
    period_axes: Mapping[str, Mapping[str, Any]],
    scalar_dispositions: Mapping[str, ResolvedScalarDisposition],
) -> tuple[dict[str, Any], list[PlannedWrite], list[dict[str, Any]], list[NormalizedDataIssue]]:
    binding_id = str(binding.get("binding_id") or "")
    mode = _planning_mode(binding)
    report: dict[str, Any] = {
        "binding_id": binding_id,
        "mode": mode,
        "sheet": _resolve_sheet(str(binding.get("sheet") or ""), ticker),
        "target": _planner_target(binding),
        "normalized_field": str(binding.get("normalized_field") or ""),
        "capacity": int(binding.get("capacity") or 0),
        "capacity_used": 0,
        "overflow_rows": [],
        "skipped_rows": [],
        "planned_write_count": 0,
        "period_axis_id": str(binding.get("period_axis_id") or ""),
    }
    planning_state = str(binding.get("planning_state") or "active")
    if planning_state in {"inactive_legacy_contract", "optional_sector_pack", "retired_contract"}:
        report["skipped_rows"].append({"reason": planning_state})
        return report, [], [], []
    if planning_state != "active":
        reason = str(binding.get("planning_block_reason") or "Binding is intentionally blocked pending a safe shell contract.")
        severity = "P1"
        report["skipped_rows"].append(
            _structured_skip(binding, normalized_path=str(binding.get("normalized_field") or ""), row_key="binding", reason=reason, severity=severity)
        )
        gap = _gap(binding, reason=reason, severity=severity, normalized_path=str(binding.get("normalized_field") or ""), row_key="binding")
        return report, [], [gap], [_planner_issue(severity, "binding_planning_blocked", binding_id, reason)]

    if mode == "formula_owned":
        report["skipped_rows"].append({"reason": "formula_owned"})
        return report, [], [], []
    if mode in _TABLE_MODES:
        return _plan_table_binding(package, binding, ticker=ticker, report=report)
    if mode in _PIVOT_MODES:
        return _plan_pivot_binding(
            package,
            binding,
            ticker=ticker,
            report=report,
            period_axes=period_axes,
        )
    if mode == "series":
        return _plan_series_binding(
            package,
            binding,
            ticker=ticker,
            report=report,
            period_axes=period_axes,
        )
    if mode in {"scalar", "text_block"}:
        return _plan_scalar_binding(
            package,
            binding,
            ticker=ticker,
            report=report,
            scalar_dispositions=scalar_dispositions,
        )
    issue = _planner_issue("P1", "unsupported_binding_planning_mode", binding_id, f"Unsupported planning mode {mode!r}.")
    return report, [], [_gap(binding, reason=issue.message)], [issue]


def _scalar_disposition_contracts(
    bindings: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, _ScalarDispositionContract], list[NormalizedDataIssue]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    issues: list[NormalizedDataIssue] = []
    for binding in bindings:
        disposition_id = str(binding.get("scalar_disposition_id") or "")
        disposition_field = str(binding.get("scalar_disposition_field") or "")
        record_path = str(binding.get("scalar_disposition_path") or "")
        if not disposition_id:
            if disposition_field or record_path:
                issues.append(
                    _planner_issue(
                        "P1",
                        "binding_scalar_disposition_id_missing",
                        str(binding.get("binding_id") or ""),
                        "Scalar disposition fields require a non-empty scalar_disposition_id.",
                    )
                )
            continue
        grouped.setdefault(disposition_id, []).append(binding)

    contracts: dict[str, _ScalarDispositionContract] = {}
    record_owners: dict[str, str] = {}
    for disposition_id, rows in sorted(grouped.items()):
        binding_ids = sorted(str(row.get("binding_id") or "") for row in rows)
        fields = {str(row.get("scalar_disposition_field") or "") for row in rows}
        invalid_fields = sorted(field for field in fields if field not in _SCALAR_DISPOSITION_FIELDS)
        if invalid_fields:
            issues.append(
                _planner_issue(
                    "P1",
                    "binding_scalar_disposition_field_invalid",
                    disposition_id,
                    f"Scalar disposition fields are invalid: {invalid_fields!r}; bindings={binding_ids!r}.",
                )
            )
            continue
        record_paths = {
            str(row.get("scalar_disposition_path") or "")
            for row in rows
            if str(row.get("scalar_disposition_path") or "")
        }
        if len(record_paths) != 1:
            issues.append(
                _planner_issue(
                    "P1",
                    "binding_scalar_disposition_path_invalid",
                    disposition_id,
                    f"Scalar disposition requires exactly one record path; paths={sorted(record_paths)!r}, bindings={binding_ids!r}.",
                )
            )
            continue
        record_path = next(iter(record_paths))
        prior_owner = record_owners.get(record_path)
        if prior_owner is not None and prior_owner != disposition_id:
            issues.append(
                _planner_issue(
                    "P1",
                    "binding_scalar_disposition_duplicate_identity",
                    disposition_id,
                    (
                        f"Scalar record path {record_path!r} is owned by both {prior_owner!r} and "
                        f"{disposition_id!r}; bindings={binding_ids!r}."
                    ),
                )
            )
            continue
        record_owners[record_path] = disposition_id
        expected_units = {
            str(row.get("expected_unit") or "")
            for row in rows
            if str(row.get("expected_unit") or "")
        }
        if len(expected_units) > 1:
            issues.append(
                _planner_issue(
                    "P1",
                    "binding_scalar_disposition_unit_conflict",
                    disposition_id,
                    f"Scalar disposition has conflicting expected units: {sorted(expected_units)!r}.",
                )
            )
            continue
        display_reference_paths = {
            str(row.get("scalar_period_display_reference_path") or "")
            for row in rows
            if str(row.get("scalar_period_display_reference_path") or "")
        }
        if len(display_reference_paths) > 1:
            issues.append(
                _planner_issue(
                    "P1",
                    "binding_scalar_disposition_period_reference_conflict",
                    disposition_id,
                    f"Scalar disposition has conflicting period-display references: {sorted(display_reference_paths)!r}.",
                )
            )
            continue
        display_source_paths = {
            str(row.get("scalar_period_display_source_path") or "")
            for row in rows
            if str(row.get("scalar_period_display_source_path") or "")
        }
        if len(display_source_paths) > 1:
            issues.append(
                _planner_issue(
                    "P1",
                    "binding_scalar_disposition_period_source_conflict",
                    disposition_id,
                    f"Scalar disposition has conflicting period-display source paths: {sorted(display_source_paths)!r}.",
                )
            )
            continue
        period_required = any(
            str(path) == f"{record_path}.period"
            for row in rows
            for path in row.get("required_companion_paths") or []
        ) or bool(fields & {"period", "period_display"})
        contracts[disposition_id] = _ScalarDispositionContract(
            disposition_id=disposition_id,
            record_path=record_path,
            metric_id=record_path.rsplit(".", 1)[-1],
            expected_unit=next(iter(expected_units), ""),
            period_required=period_required,
            source_ref_required=any(_requires_source_ref(row) for row in rows),
            period_display_reference_path=next(iter(display_reference_paths), ""),
            period_display_source_path=next(iter(display_source_paths), ""),
        )
    return contracts, issues


def _resolve_scalar_disposition(
    package: Mapping[str, Any],
    contract: _ScalarDispositionContract,
) -> ResolvedScalarDisposition:
    raw = _path_get(package, contract.record_path)
    if not isinstance(raw, Mapping):
        return ResolvedScalarDisposition(
            disposition_id=contract.disposition_id,
            business_key=contract.record_path,
            metric_id=contract.metric_id,
            record_path=contract.record_path,
            value=None,
            unit=None,
            period=None,
            period_display=None,
            status="missing_mapping",
            reason=f"Scalar record {contract.record_path!r} is absent or is not an object.",
            source_status="",
            evidence_ids=(),
            source_refs=(),
            period_display_source_refs=(),
            formula_ids=(),
            validity_code="scalar_record_missing",
        )

    source_status = str(raw.get("status") or "").strip()
    source_refs = _ordered_scalar_strings(raw.get("source_ref"), raw.get("source_refs"))
    evidence_ids = _ordered_scalar_strings(raw.get("evidence_id"), raw.get("evidence_ids"))
    formula_ids = _ordered_scalar_strings(raw.get("formula_id"), raw.get("formula_ids"))
    raw_value = raw.get("value")
    raw_unit = str(raw.get("unit") or "").strip()
    raw_period = str(raw.get("period") or "").strip()
    raw_reason = str(raw.get("reason") or raw.get("missing_reason") or "").strip()

    if source_status != "populated":
        if source_status not in FIELD_STATUSES:
            status = "parser_conflict"
            reason = f"Scalar source status {source_status!r} is outside the normalized status vocabulary."
            validity_code = "scalar_status_invalid"
        elif raw_value not in (None, ""):
            status = "manual_review_required"
            reason = f"Scalar status {source_status!r} conflicts with a populated value."
            validity_code = "scalar_status_value_conflict"
        else:
            status = source_status
            reason = raw_reason or f"Scalar source status is {source_status}."
            validity_code = source_status
        return ResolvedScalarDisposition(
            disposition_id=contract.disposition_id,
            business_key=contract.record_path,
            metric_id=contract.metric_id,
            record_path=contract.record_path,
            value=None,
            unit=None,
            period=None,
            period_display=None,
            status=status,
            reason=reason,
            source_status=source_status,
            evidence_ids=evidence_ids,
            source_refs=source_refs,
            period_display_source_refs=(),
            formula_ids=formula_ids,
            validity_code=validity_code,
        )

    validity_code = "populated"
    reason = raw_reason
    if not isinstance(raw_value, (int, float)) or isinstance(raw_value, bool):
        validity_code = "scalar_value_invalid"
        reason = "Populated scalar value must be numeric."
    elif contract.expected_unit and raw_unit != contract.expected_unit:
        validity_code = "scalar_unit_mismatch"
        reason = f"Scalar unit {raw_unit!r} is incompatible with expected unit {contract.expected_unit!r}."
    elif contract.period_required and not raw_period:
        validity_code = "scalar_companion_missing"
        reason = f"Scalar value requires populated companion path(s): {contract.record_path}.period."
    elif contract.period_required and not _is_iso_calendar_date(raw_period):
        validity_code = "scalar_period_invalid"
        reason = f"Scalar period {raw_period!r} is not an exact ISO calendar date."
    elif contract.source_ref_required and not source_refs:
        validity_code = "missing_source_ref"
        reason = "Source-backed scalar/text value has no source_ref."

    if validity_code != "populated":
        return ResolvedScalarDisposition(
            disposition_id=contract.disposition_id,
            business_key=contract.record_path,
            metric_id=contract.metric_id,
            record_path=contract.record_path,
            value=None,
            unit=None,
            period=None,
            period_display=None,
            status="manual_review_required",
            reason=reason,
            source_status=source_status,
            evidence_ids=evidence_ids,
            source_refs=source_refs,
            period_display_source_refs=(),
            formula_ids=formula_ids,
            validity_code=validity_code,
        )

    period_display = f"As of {raw_period}" if raw_period else None
    if period_display and contract.period_display_reference_path:
        reference_value, _reference_source_ref, reference_populated = _read_field(
            package,
            contract.period_display_reference_path,
        )
        reference_period = str(reference_value or "").strip() if reference_populated else ""
        if _is_iso_calendar_date(reference_period) and reference_period > raw_period:
            period_display = f"{period_display} (stale)"
    period_display_source_refs: tuple[str, ...] = ()
    if contract.period_display_source_path:
        display_source = _path_get(package, contract.period_display_source_path)
        if isinstance(display_source, Mapping):
            display_period = str(display_source.get("period") or "").strip()
            display_status = str(display_source.get("status") or "").strip()
            if display_status == "populated" and display_period == raw_period:
                period_display_source_refs = _ordered_scalar_strings(
                    display_source.get("source_ref"),
                    display_source.get("source_refs"),
                )
    return ResolvedScalarDisposition(
        disposition_id=contract.disposition_id,
        business_key=contract.record_path,
        metric_id=contract.metric_id,
        record_path=contract.record_path,
        value=raw_value,
        unit=raw_unit or None,
        period=raw_period or None,
        period_display=period_display,
        status="populated",
        reason=reason,
        source_status=source_status,
        evidence_ids=evidence_ids,
        source_refs=source_refs,
        period_display_source_refs=period_display_source_refs,
        formula_ids=formula_ids,
        validity_code=validity_code,
    )


def _ordered_scalar_strings(*values: Any) -> tuple[str, ...]:
    result: list[str] = []
    for value in values:
        candidates = value if isinstance(value, (list, tuple)) else [value]
        for candidate in candidates:
            text = str(candidate or "").strip()
            if text and text not in result:
                result.append(text)
    return tuple(result)


def _is_iso_calendar_date(value: str) -> bool:
    try:
        return date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def _scalar_disposition_field_value(disposition: ResolvedScalarDisposition, field_name: str) -> Any:
    if field_name == "value":
        return disposition.value
    if field_name == "unit":
        return disposition.unit
    if field_name == "period":
        return disposition.period
    if field_name == "period_display":
        return disposition.period_display
    if field_name == "status":
        return disposition.status
    if field_name == "reason":
        return disposition.reason
    if field_name == "evidence":
        return disposition.source_ref or None
    return None


def _scalar_disposition_field_source_ref(disposition: ResolvedScalarDisposition, field_name: str) -> str:
    if field_name == "period" and disposition.period is None:
        return ""
    if field_name == "period_display" and disposition.period_display is None:
        return ""
    if field_name == "period_display" and disposition.period_display_source_refs:
        return disposition.period_display_source_refs[0]
    return disposition.source_ref


def _scalar_disposition_binding_failure(
    disposition: ResolvedScalarDisposition | None,
    field_name: str,
    binding: Mapping[str, Any],
) -> tuple[str, str, str] | None:
    if disposition is None or field_name != "value":
        return None
    failures = {
        "scalar_companion_missing": (
            _missing_binding_severity(binding),
            "binding_scalar_companion_missing",
        ),
        "scalar_unit_mismatch": ("P1", "binding_scalar_unit_mismatch"),
        "scalar_value_invalid": ("P1", "binding_scalar_value_invalid"),
        "scalar_period_invalid": ("P1", "binding_scalar_period_invalid"),
        "missing_source_ref": ("P1", "missing_source_ref"),
        "scalar_status_invalid": ("P1", "binding_scalar_status_invalid"),
        "scalar_status_value_conflict": ("P1", "binding_scalar_status_value_conflict"),
    }
    failure = failures.get(disposition.validity_code)
    if failure is None:
        return None
    return failure[0], failure[1], disposition.reason


def _plan_scalar_binding(
    package: Mapping[str, Any],
    binding: Mapping[str, Any],
    *,
    ticker: str,
    report: dict[str, Any],
    scalar_dispositions: Mapping[str, ResolvedScalarDisposition],
) -> tuple[dict[str, Any], list[PlannedWrite], list[dict[str, Any]], list[NormalizedDataIssue]]:
    binding_id = str(binding["binding_id"])
    source_path = str(binding.get("source_path") or binding.get("normalized_field") or "")
    target = _first_cell(_planner_target(binding))
    raw_source = _path_get(package, source_path)
    disposition_id = str(binding.get("scalar_disposition_id") or "")
    disposition_field = str(binding.get("scalar_disposition_field") or "")
    disposition = scalar_dispositions.get(disposition_id) if disposition_id else None
    if disposition_id and disposition is None:
        issue = _planner_issue(
            "P1",
            "binding_scalar_disposition_unresolved",
            binding_id,
            f"Scalar disposition {disposition_id!r} is not resolved for binding {binding_id!r}.",
        )
        return report, [], [_gap(binding, reason=issue.message)], [issue]
    if disposition is not None:
        value = _scalar_disposition_field_value(disposition, disposition_field)
        source_ref = _scalar_disposition_field_source_ref(disposition, disposition_field)
        populated = value not in (None, "")
        raw_source = _path_get(package, disposition.record_path)
    else:
        value, source_ref, populated = _read_field(package, source_path)
    source_ref_path = str(binding.get("source_ref_path") or "")
    if populated and not source_ref and source_ref_path:
        source_ref_value, source_ref_lineage, source_ref_populated = _read_field(package, source_ref_path)
        if source_ref_lineage:
            source_ref = source_ref_lineage
        elif source_ref_populated:
            source_ref = str(source_ref_value or "")
    if not populated:
        disposition_failure = _scalar_disposition_binding_failure(disposition, disposition_field, binding)
        if disposition_failure is not None:
            severity, rule_id, reason = disposition_failure
            event_key = _planner_event_key(binding, normalized_path=source_path, row_key="scalar", event_type=rule_id)
            report["skipped_rows"].append(
                _structured_skip(
                    binding,
                    normalized_path=source_path,
                    row_key="scalar",
                    reason=reason,
                    severity=severity,
                    source_ref=source_ref,
                    expected_target=target,
                )
            )
            issue = _planner_issue(
                severity,
                rule_id,
                f"{binding_id}:{source_path}",
                reason,
                normalized_path=source_path,
                business_row_key=disposition.business_key if disposition is not None else "scalar",
                binding_id=binding_id,
                source_ref=source_ref,
                root_cause=disposition.validity_code if disposition is not None else rule_id,
                issue_type="planner_mapping_gap",
                canonical_issue_key=event_key,
            )
            return report, [], [
                _gap(
                    binding,
                    reason=reason,
                    severity=severity,
                    normalized_path=source_path,
                    row_key="scalar",
                    source_ref=source_ref,
                    expected_target=target,
                    canonical_issue_key=event_key,
                    root_cause=disposition.validity_code if disposition is not None else rule_id,
                )
            ], [issue]
        severity = _missing_binding_severity(binding)
        reason = "Scalar/text value is not populated."
        event_key = _planner_event_key(binding, normalized_path=source_path, row_key="scalar", event_type="missing_value")
        report["skipped_rows"].append(
            _structured_skip(
                binding,
                normalized_path=source_path,
                row_key="scalar",
                reason=reason,
                severity=severity,
                source_ref=source_ref,
                expected_target=target,
            )
        )
        issue = _planner_issue(
            severity,
            "required_binding_missing" if severity == "P1" else "binding_value_missing",
            f"{binding_id}:{source_path}",
            reason,
            normalized_path=source_path,
            business_row_key="scalar",
            binding_id=binding_id,
            source_ref=source_ref,
            root_cause="missing_value",
            issue_type="planner_mapping_gap",
            canonical_issue_key=event_key,
        )
        return report, [], [_gap(binding, reason=reason, severity=severity, normalized_path=source_path, row_key="scalar", source_ref=source_ref, expected_target=target, canonical_issue_key=event_key, root_cause="missing_value")], [issue]
    missing_companions = [
        str(path)
        for path in binding.get("required_companion_paths") or []
        if not _read_field(package, str(path))[2]
    ] if disposition is None else []
    if missing_companions:
        severity = _missing_binding_severity(binding)
        reason = f"Scalar value requires populated companion path(s): {', '.join(missing_companions)}."
        report["skipped_rows"].append(
            _structured_skip(
                binding,
                normalized_path=source_path,
                row_key="scalar",
                reason="scalar_companion_missing",
                severity=severity,
                source_ref=source_ref,
                expected_target=target,
            )
        )
        issue = _planner_issue(
            severity,
            "binding_scalar_companion_missing",
            f"{binding_id}:{source_path}",
            reason,
            normalized_path=source_path,
            business_row_key="scalar",
            binding_id=binding_id,
            source_ref=source_ref,
        )
        return report, [], [_gap(binding, reason=reason, severity=severity, normalized_path=source_path, row_key="scalar", source_ref=source_ref, expected_target=target)], [issue]
    expected_unit = str(binding.get("expected_unit") or "")
    source_unit = str(raw_source.get("unit") or "") if isinstance(raw_source, Mapping) else ""
    if disposition is None and expected_unit and source_unit != expected_unit:
        reason = f"Scalar unit {source_unit!r} is incompatible with expected unit {expected_unit!r}."
        report["skipped_rows"].append(
            _structured_skip(binding, normalized_path=source_path, row_key="scalar", reason="scalar_unit_mismatch", severity="P1", source_ref=source_ref, expected_target=target)
        )
        issue = _planner_issue(
            "P1",
            "binding_scalar_unit_mismatch",
            f"{binding_id}:{source_path}",
            reason,
            normalized_path=source_path,
            business_row_key="scalar",
            binding_id=binding_id,
            source_ref=source_ref,
        )
        return report, [], [_gap(binding, reason=reason, severity="P1", normalized_path=source_path, row_key="scalar", source_ref=source_ref, expected_target=target)], [issue]
    if disposition is None and _requires_source_ref(binding) and not source_ref:
        reason = "Source-backed scalar/text value has no source_ref."
        event_key = _planner_event_key(binding, normalized_path=source_path, row_key="scalar", event_type="missing_source_ref")
        issue = _planner_issue(
            "P1",
            "missing_source_ref",
            source_path,
            reason,
            normalized_path=source_path,
            business_row_key="scalar",
            binding_id=binding_id,
            root_cause="missing_source_ref",
            issue_type="planner_mapping_gap",
            canonical_issue_key=event_key,
        )
        report["skipped_rows"].append(
            _structured_skip(binding, normalized_path=source_path, row_key="scalar", reason=issue.message, severity="P1", expected_target=target)
        )
        return report, [], [_gap(binding, reason=issue.message, severity="P1", normalized_path=source_path, row_key="scalar", expected_target=target, canonical_issue_key=event_key, root_cause="missing_source_ref")], [issue]
    write = _planned_write(
        binding,
        ticker=ticker,
        target_cell=target,
        normalized_path=source_path,
        row_key="scalar",
        value=value,
        source_ref=source_ref,
        capacity_used=1,
        target_type=str(binding.get("target_type") or ""),
        target_role=_binding_target_role(binding),
    )
    report["capacity_used"] = 1
    report["planned_write_count"] = 1
    return report, [write], [], []


def _plan_series_binding(
    package: Mapping[str, Any],
    binding: Mapping[str, Any],
    *,
    ticker: str,
    report: dict[str, Any],
    period_axes: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, Any], list[PlannedWrite], list[dict[str, Any]], list[NormalizedDataIssue]]:
    rows, skipped, selection_issues = _selected_rows(package, binding)
    report["skipped_rows"].extend(skipped)
    if selection_issues:
        return report, [], [_gap(binding, reason=issue.message) for issue in selection_issues], selection_issues
    coordinates = _series_coordinates(_planner_target(binding), str(binding.get("target_axis") or "columns"))
    capacity = int(binding["capacity"])
    if capacity != len(coordinates):
        issue = _planner_issue("P1", "binding_capacity_mismatch", str(binding["binding_id"]), "Declared series capacity does not match the declared target cell count.")
        return report, [], [_gap(binding, reason=issue.message)], [issue]
    minimum_rows = int(binding.get("minimum_rows") or 0)
    if len(rows) < minimum_rows:
        severity = "P1" if bool(binding.get("required")) else "P2"
        issue = _planner_issue(
            severity,
            "binding_minimum_row_count_unmet",
            str(binding["binding_id"]),
            f"Selected {len(rows)} row(s), below the declared minimum_rows={minimum_rows}.",
        )
        return report, [], [_gap(binding, reason=issue.message, severity=severity, normalized_path=str(binding.get("normalized_field") or ""), row_key="collection")], [issue]
    writes: list[PlannedWrite] = []
    gaps: list[dict[str, Any]] = []
    issues: list[NormalizedDataIssue] = []
    source_field = str(binding.get("source_field") or "")
    period_axis_id = str(binding.get("period_axis_id") or "")
    period_axis_role = str(binding.get("period_axis_role") or "")
    dependent_axis = period_axes.get(period_axis_id) if period_axis_id and period_axis_role == "dependent" else None
    if period_axis_role == "dependent" and not isinstance(dependent_axis, Mapping):
        issue = _planner_issue(
            "P1",
            "binding_period_axis_unresolved",
            str(binding["binding_id"]),
            f"Dependent series requires resolved period axis {period_axis_id!r}.",
        )
        return report, [], [_gap(binding, reason=issue.message, severity="P1")], [issue]
    planned_index = 0
    for source_position, row in enumerate(rows):
        row_key = _row_key(row, binding)
        normalized_path = _row_normalized_path(binding, row, source_position, source_field)
        row_source_ref = _row_source_ref(row, binding)
        target_cell = coordinates[planned_index] if planned_index < capacity else ""
        if dependent_axis is not None:
            period_field = str(binding.get("period_field") or "period")
            period = str(_read_row_field(row, period_field)[0] or "")
            axis_columns = dependent_axis.get("period_to_column") if isinstance(dependent_axis, Mapping) else {}
            column = str(axis_columns.get(period) or "") if isinstance(axis_columns, Mapping) else ""
            if not column:
                reason = f"period {period!r} has no column in period axis {period_axis_id!r}"
                record = _structured_skip(
                    binding,
                    normalized_path=normalized_path,
                    row_key=row_key,
                    reason="period_axis_period_missing",
                    severity="P1",
                    source_ref=row_source_ref,
                )
                report["overflow_rows"].append(record)
                gaps.append(_gap(binding, reason=reason, severity="P1", normalized_path=normalized_path, row_key=row_key, source_ref=row_source_ref))
                issues.append(_planner_issue("P1", "binding_period_axis_period_missing", f"{binding['binding_id']}:{row_key}", reason))
                continue
            min_col, min_row, max_col, max_row = _parse_range(_planner_target(binding))
            if min_row != max_row or not (min_col <= _column_index(column) <= max_col):
                reason = f"Axis column {column!r} is outside dependent target {_planner_target(binding)!r}."
                issues.append(_planner_issue("P1", "binding_period_axis_target_mismatch", str(binding["binding_id"]), reason))
                gaps.append(_gap(binding, reason=reason, severity="P1", normalized_path=normalized_path, row_key=row_key, source_ref=row_source_ref))
                continue
            target_cell = f"{column}{min_row}"
        elif planned_index >= capacity:
            severity = _overflow_severity(binding)
            reason = "capacity_exceeded"
            overflow = _structured_skip(
                binding,
                normalized_path=normalized_path,
                row_key=row_key,
                reason=reason,
                severity=severity,
                source_ref=row_source_ref,
            )
            report["overflow_rows"].append(overflow)
            gaps.append(
                _gap(
                    binding,
                    reason=reason,
                    severity=severity,
                    normalized_path=normalized_path,
                    row_key=row_key,
                    source_ref=row_source_ref,
                )
            )
            continue
        value, source_ref, populated = _read_bound_row_field(row, binding, source_field)
        if not populated:
            severity = (
                "P1"
                if bool(binding.get("required"))
                and source_field in set(binding.get("required_columns") or [])
                else _missing_binding_severity(binding)
            )
            reason = f"{source_field} not populated"
            event_key = _planner_event_key(binding, normalized_path=normalized_path, row_key=row_key, event_type="missing_value")
            report["skipped_rows"].append(
                _structured_skip(
                    binding,
                    normalized_path=normalized_path,
                    row_key=row_key,
                    reason=reason,
                    severity=severity,
                    source_ref=row_source_ref,
                    expected_target=target_cell,
                )
            )
            gaps.append(
                _gap(
                    binding,
                    reason=reason,
                    severity=severity,
                    normalized_path=normalized_path,
                    row_key=row_key,
                    source_ref=row_source_ref,
                    expected_target=target_cell,
                    canonical_issue_key=event_key,
                    root_cause="missing_value",
                )
            )
            issues.append(
                _planner_issue(
                    severity,
                    "required_row_value_missing",
                    f"{binding['binding_id']}:{row_key}:{source_field}",
                    reason,
                    normalized_path=normalized_path,
                    business_row_key=row_key,
                    binding_id=str(binding["binding_id"]),
                    source_ref=row_source_ref,
                    root_cause="missing_value",
                    issue_type="planner_mapping_gap",
                    canonical_issue_key=event_key,
                )
            )
            planned_index += 1
            continue
        if _requires_source_ref(binding) and not source_ref:
            reason = "Source-backed series value has no source_ref."
            event_key = _planner_event_key(binding, normalized_path=normalized_path, row_key=row_key, event_type="missing_source_ref")
            report["skipped_rows"].append(
                _structured_skip(
                    binding,
                    normalized_path=normalized_path,
                    row_key=row_key,
                    reason=reason,
                    severity="P1",
                    expected_target=target_cell,
                )
            )
            gaps.append(_gap(binding, reason=reason, severity="P1", normalized_path=normalized_path, row_key=row_key, expected_target=target_cell, canonical_issue_key=event_key, root_cause="missing_source_ref"))
            issues.append(
                _planner_issue(
                    "P1",
                    "missing_source_ref",
                    f"{binding['binding_id']}:{row_key}",
                    reason,
                    normalized_path=normalized_path,
                    business_row_key=row_key,
                    binding_id=str(binding["binding_id"]),
                    root_cause="missing_source_ref",
                    issue_type="planner_mapping_gap",
                    canonical_issue_key=event_key,
                )
            )
            planned_index += 1
            continue
        writes.append(
            _planned_write(
                binding,
                ticker=ticker,
                target_cell=target_cell,
                normalized_path=normalized_path,
                row_key=row_key,
                value=value,
                source_ref=source_ref,
                capacity_used=planned_index + 1,
                target_type=str(binding.get("target_type") or ""),
                target_role=_binding_target_role(binding),
            )
        )
        planned_index += 1
    if report["overflow_rows"]:
        severity = _overflow_severity(binding)
        issues.append(_planner_issue(severity, "binding_overflow", str(binding["binding_id"]), f"{len(report['overflow_rows'])} row(s) exceed declared series capacity."))
    report["capacity_used"] = len(writes)
    report["planned_write_count"] = len(writes)
    if not writes and _missing_binding_severity(binding) == "P1" and not issues:
        reason = "No source-backed row satisfied this required series binding."
        issues.append(_planner_issue("P1", "required_binding_missing", str(binding["binding_id"]), reason))
        gaps.append(_gap(binding, reason=reason, severity="P1", normalized_path=str(binding.get("normalized_field") or ""), row_key="collection"))
    return report, writes, gaps, issues


def _plan_table_binding(
    package: Mapping[str, Any],
    binding: Mapping[str, Any],
    *,
    ticker: str,
    report: dict[str, Any],
) -> tuple[dict[str, Any], list[PlannedWrite], list[dict[str, Any]], list[NormalizedDataIssue]]:
    rows, skipped, selection_issues = _selected_rows(package, binding)
    report["skipped_rows"].extend(skipped)
    if selection_issues:
        return report, [], [_gap(binding, reason=issue.message) for issue in selection_issues], selection_issues
    return _plan_table_rows(rows, binding, ticker=ticker, report=report)


def _plan_pivot_binding(
    package: Mapping[str, Any],
    binding: Mapping[str, Any],
    *,
    ticker: str,
    report: dict[str, Any],
    period_axes: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, Any], list[PlannedWrite], list[dict[str, Any]], list[NormalizedDataIssue]]:
    """Plan a member-by-period matrix without interpreting workbook content."""

    rows, skipped, selection_issues = _selected_rows(package, binding)
    report["skipped_rows"].extend(skipped)
    if selection_issues:
        return report, [], [_gap(binding, reason=issue.message) for issue in selection_issues], selection_issues

    period_field = str(binding.get("period_field") or "period")
    dimension_field = str(binding.get("dimension_field") or "dimension")
    member_field = str(binding.get("member_field") or "member")
    value_field = str(binding.get("value_field") or "")
    label_column = str(binding.get("label_target_column") or "A").upper()
    row_blocks = binding.get("row_blocks") if isinstance(binding.get("row_blocks"), Mapping) else {}
    period_axis_id = str(binding.get("period_axis_id") or "")
    period_axis = period_axes.get(period_axis_id)
    if not value_field or not row_blocks or not period_axis_id:
        issue = _planner_issue("P1", "binding_pivot_contract_invalid", str(binding.get("binding_id") or ""), "Pivot binding requires value_field, row_blocks, and period_axis_id.")
        return report, [], [_gap(binding, reason=issue.message, severity="P1")], [issue]
    if not isinstance(period_axis, Mapping):
        issue = _planner_issue(
            "P1",
            "binding_period_axis_unresolved",
            str(binding.get("binding_id") or ""),
            f"Pivot binding requires resolved period axis {period_axis_id!r}.",
        )
        return report, [], [_gap(binding, reason=issue.message, severity="P1")], [issue]
    period_to_column = period_axis.get("period_to_column") if isinstance(period_axis.get("period_to_column"), Mapping) else {}
    report["resolved_period_to_column"] = dict(period_to_column)

    writes: list[PlannedWrite] = []
    gaps: list[dict[str, Any]] = []
    issues: list[NormalizedDataIssue] = []
    member_rows: dict[tuple[str, str], int] = {}
    occupied_rows: dict[int, tuple[str, str]] = {}
    written_cells: set[str] = set()
    block_members: dict[str, list[tuple[str, str]]] = {}
    member_labels: dict[tuple[str, str], str] = {}
    for row in rows:
        period = str(_read_row_field(row, period_field)[0] or "")
        if period not in period_to_column:
            continue
        raw_dimension = str(_read_row_field(row, dimension_field)[0] or "")
        raw_member = str(_read_row_field(row, member_field)[0] or "")
        canonical_pair = canonical_segment_dimension_member(raw_dimension, raw_member)
        members = block_members.setdefault(canonical_pair[0], [])
        if canonical_pair not in members:
            members.append(canonical_pair)
        member_labels.setdefault(
            canonical_pair,
            canonical_segment_display_member(raw_dimension, raw_member),
        )

    for dimension, members in block_members.items():
        declared_rows = [int(row) for row in row_blocks.get(dimension) or []]
        for member_index, canonical_pair in enumerate(members):
            if member_index >= len(declared_rows):
                continue
            target_row = declared_rows[member_index]
            owner = occupied_rows.get(target_row)
            if owner and owner != canonical_pair:
                issues.append(_planner_issue("P1", "binding_pivot_row_collision", str(binding.get("binding_id") or ""), f"Pivot row {target_row} is claimed by both {owner} and {canonical_pair}."))
                continue
            occupied_rows[target_row] = canonical_pair
            member_rows[canonical_pair] = target_row

    for source_position, row in enumerate(rows):
        row_key = _row_key(row, binding)
        source_ref = _row_source_ref(row, binding)
        normalized_path = _row_normalized_path(binding, row, source_position, value_field)
        raw_dimension = str(_read_row_field(row, dimension_field)[0] or "")
        raw_member = str(_read_row_field(row, member_field)[0] or "")
        canonical_pair = canonical_segment_dimension_member(raw_dimension, raw_member)
        period = str(_read_row_field(row, period_field)[0] or "")
        if period not in period_to_column:
            report["skipped_rows"].append(
                _structured_skip(
                    binding,
                    normalized_path=normalized_path,
                    row_key=row_key,
                    reason="period_axis_outside_visible_window",
                    severity="P2",
                    source_ref=source_ref,
                )
            )
            continue
        if canonical_pair not in member_rows:
            reason = "pivot_member_has_no_declared_block_capacity"
            record = _structured_skip(binding, normalized_path=normalized_path, row_key=row_key, reason=reason, severity="P1", source_ref=source_ref)
            report["overflow_rows"].append(record)
            gaps.append(_gap(binding, reason=reason, severity="P1", normalized_path=normalized_path, row_key=row_key, source_ref=source_ref))
            continue
        value, value_source_ref, populated = _read_bound_row_field(row, binding, value_field)
        if not populated:
            reason = f"{value_field} not populated"
            report["skipped_rows"].append(_structured_skip(binding, normalized_path=normalized_path, row_key=row_key, reason=reason, severity="P1", source_ref=source_ref))
            gaps.append(_gap(binding, reason=reason, severity="P1", normalized_path=normalized_path, row_key=row_key, source_ref=source_ref))
            issues.append(_planner_issue("P1", "required_row_value_missing", f"{binding['binding_id']}:{row_key}:{value_field}", reason))
            continue
        target_row = member_rows[canonical_pair]
        label_cell = f"{label_column}{target_row}"
        if label_cell not in written_cells:
            label_value = _pivot_member_label(
                binding,
                dimension=canonical_pair[0],
                member=member_labels[canonical_pair],
            )
            writes.append(_planned_write(binding, ticker=ticker, target_cell=label_cell, normalized_path=_row_normalized_path(binding, row, source_position, member_field), row_key="|".join(canonical_pair), value=label_value, source_ref=source_ref, capacity_used=len(member_rows), target_type="text", target_role=f"{binding['binding_id']}.member"))
            written_cells.add(label_cell)
        target_cell = f"{period_to_column[period]}{target_row}"
        if target_cell in written_cells:
            issues.append(_planner_issue("P1", "binding_pivot_cell_collision", f"{binding['binding_id']}:{row_key}", f"More than one selected business row resolves to {target_cell}."))
            continue
        writes.append(_planned_write(binding, ticker=ticker, target_cell=target_cell, normalized_path=normalized_path, row_key=row_key, value=value, source_ref=value_source_ref or source_ref, capacity_used=len(member_rows), target_type=str(binding.get("value_target_type") or "number"), target_role=f"{binding['binding_id']}.value"))
        written_cells.add(target_cell)

    if report["overflow_rows"]:
        severity = "P1"
        issues.append(_planner_issue(severity, "binding_overflow", str(binding["binding_id"]), f"{len(report['overflow_rows'])} pivot row(s) could not enter the declared visible matrix."))
    report["capacity_used"] = len(member_rows)
    report["planned_write_count"] = len(writes)
    return report, writes, gaps, issues


def _pivot_member_label(binding: Mapping[str, Any], *, dimension: str, member: str) -> str:
    """Render a dimension-aware label without embedding ticker-specific names."""

    templates = binding.get("dimension_label_templates")
    template = templates.get(dimension) if isinstance(templates, Mapping) else None
    if not isinstance(template, str) or not template:
        return member
    try:
        return template.format(dimension=dimension.replace("_", " ").title(), member=member)
    except (KeyError, ValueError):
        return member


def _plan_table_rows(
    rows: Sequence[Mapping[str, Any]],
    binding: Mapping[str, Any],
    *,
    ticker: str,
    report: dict[str, Any],
) -> tuple[dict[str, Any], list[PlannedWrite], list[dict[str, Any]], list[NormalizedDataIssue]]:
    capacity = int(binding["capacity"])
    writes: list[PlannedWrite] = []
    gaps: list[dict[str, Any]] = []
    issues: list[NormalizedDataIssue] = []
    target_columns = list(binding.get("target_columns") or [])
    start_row, end_row = _range_rows(_planner_target(binding))
    target_rows = _table_target_rows(binding, start_row, end_row)
    if capacity != len(target_rows):
        issue = _planner_issue("P1", "binding_capacity_mismatch", str(binding["binding_id"]), "Declared table capacity does not match target row capacity.")
        return report, [], [_gap(binding, reason=issue.message)], [issue]
    minimum_rows = int(binding.get("minimum_rows") or 0)
    if len(rows) < minimum_rows:
        severity = "P1" if bool(binding.get("required")) else "P2"
        issue = _planner_issue(
            severity,
            "binding_minimum_row_count_unmet",
            str(binding["binding_id"]),
            f"Selected {len(rows)} row(s), below the declared minimum_rows={minimum_rows}.",
        )
        return report, [], [_gap(binding, reason=issue.message, severity=severity, normalized_path=str(binding.get("normalized_field") or ""), row_key="collection")], [issue]
    planned_row_count = 0
    for source_position, row in enumerate(rows):
        row_key = _row_key(row, binding)
        row_source_ref = _row_source_ref(row, binding)
        base_path = _row_normalized_path(binding, row, source_position, "")
        if planned_row_count >= capacity:
            severity = _overflow_severity(binding)
            reason = "capacity_exceeded"
            report["overflow_rows"].append(
                _structured_skip(binding, normalized_path=base_path.rstrip("."), row_key=row_key, reason=reason, severity=severity, source_ref=row_source_ref)
            )
            gaps.append(
                _gap(binding, reason=reason, severity=severity, normalized_path=base_path.rstrip("."), row_key=row_key, source_ref=row_source_ref)
            )
            continue
        target_row = target_rows[planned_row_count]
        missing_required = _missing_required_columns(row, binding)
        if missing_required:
            reason = "required_columns_not_populated: " + ", ".join(missing_required)
            expected_target = f"row {target_row}"
            event_key = _planner_event_key(binding, normalized_path=base_path.rstrip("."), row_key=row_key, event_type="missing_required_columns")
            severity = "P1" if bool(binding.get("required")) else _missing_binding_severity(binding)
            report["skipped_rows"].append(
                _structured_skip(
                    binding,
                    normalized_path=base_path.rstrip("."),
                    row_key=row_key,
                    reason=reason,
                    severity=severity,
                    source_ref=row_source_ref,
                    expected_target=expected_target,
                )
            )
            gaps.append(
                _gap(
                    binding,
                    reason=reason,
                    severity=severity,
                    normalized_path=base_path.rstrip("."),
                    row_key=row_key,
                    source_ref=row_source_ref,
                    expected_target=expected_target,
                    canonical_issue_key=event_key,
                    root_cause="missing_required_columns",
                )
            )
            issues.append(
                _planner_issue(
                    severity,
                    "required_row_schema_column_missing",
                    f"{binding['binding_id']}:{row_key}",
                    reason,
                    normalized_path=base_path.rstrip("."),
                    business_row_key=row_key,
                    binding_id=str(binding["binding_id"]),
                    source_ref=row_source_ref,
                    root_cause="missing_required_columns",
                    issue_type="planner_mapping_gap",
                    canonical_issue_key=event_key,
                )
            )
            planned_row_count += 1
            continue
        for column in target_columns:
            source_field = str(column.get("source_field") or column.get("column_id") or "")
            value, source_ref, populated = _read_bound_row_field(row, binding, source_field)
            if not populated:
                continue
            if _requires_source_ref(binding) and not source_ref:
                reason = "Source-backed table value has no source_ref."
                normalized_path = _row_normalized_path(binding, row, source_position, source_field)
                expected_target = f"{str(column['target_column']).upper()}{target_row}"
                event_key = _planner_event_key(binding, normalized_path=normalized_path, row_key=row_key, event_type="missing_source_ref")
                report["skipped_rows"].append(
                    _structured_skip(
                        binding,
                        normalized_path=normalized_path,
                        row_key=row_key,
                        reason=reason,
                        severity="P1",
                        expected_target=expected_target,
                    )
                )
                gaps.append(_gap(binding, reason=reason, severity="P1", normalized_path=normalized_path, row_key=row_key, expected_target=expected_target, canonical_issue_key=event_key, root_cause="missing_source_ref"))
                issues.append(
                    _planner_issue(
                        "P1",
                        "missing_source_ref",
                        f"{binding['binding_id']}:{row_key}:{source_field}",
                        reason,
                        normalized_path=normalized_path,
                        business_row_key=row_key,
                        binding_id=str(binding["binding_id"]),
                        root_cause="missing_source_ref",
                        issue_type="planner_mapping_gap",
                        canonical_issue_key=event_key,
                    )
                )
                continue
            target_cell = f"{str(column['target_column']).upper()}{target_row}"
            writes.append(
                _planned_write(
                    binding,
                    ticker=ticker,
                    target_cell=target_cell,
                    normalized_path=_row_normalized_path(binding, row, source_position, source_field),
                    row_key=row_key,
                    value=value,
                    source_ref=source_ref,
                    capacity_used=planned_row_count + 1,
                    target_type=_column_target_type(binding, column),
                    target_role=_column_target_role(binding, column),
                )
            )
        planned_row_count += 1
    if report["overflow_rows"]:
        severity = _overflow_severity(binding)
        issues.append(_planner_issue(severity, "binding_overflow", str(binding["binding_id"]), f"{len(report['overflow_rows'])} row(s) exceed declared table capacity."))
    report["capacity_used"] = min(planned_row_count, capacity)
    report["planned_write_count"] = len(writes)
    if not writes and _missing_binding_severity(binding) == "P1" and not bool(binding.get("allow_empty")) and not issues:
        reason = "No source-backed row satisfied this required table binding."
        gaps.append(_gap(binding, reason=reason, severity="P1", normalized_path=str(binding.get("normalized_field") or ""), row_key="collection"))
        issues.append(_planner_issue("P1", "required_binding_missing", str(binding["binding_id"]), reason))
    return report, writes, gaps, issues


def _selected_rows(
    package: Mapping[str, Any],
    binding: Mapping[str, Any],
) -> tuple[list[Mapping[str, Any]], list[dict[str, Any]], list[NormalizedDataIssue]]:
    selector = binding.get("row_selector")
    if not isinstance(selector, Mapping):
        return [], [], [_planner_issue("P1", "binding_row_selector_missing", str(binding.get("binding_id") or ""), "Row binding has no row_selector.")]
    source_path = str(selector.get("source_path") or "")
    raw_rows = _path_get(package, source_path)
    if not isinstance(raw_rows, list):
        return [], [], [_planner_issue("P1", "binding_row_source_invalid", str(binding.get("binding_id") or ""), f"row_selector source_path {source_path!r} is not a list.")]
    skipped: list[dict[str, Any]] = []
    rows: list[Mapping[str, Any]] = []
    issues: list[NormalizedDataIssue] = []
    approved_current_guidance = (
        current_guidance_indexes([row if isinstance(row, Mapping) else {} for row in raw_rows])
        if source_path == "normalized_guidance.items"
        else None
    )
    for source_index, raw in enumerate(raw_rows):
        if not isinstance(raw, Mapping):
            normalized_path = f"{source_path}.{source_index}"
            skipped.append(
                _structured_skip(binding, normalized_path=normalized_path, row_key=f"source_index:{source_index}", reason="row_not_object", severity="P1")
            )
            issues.append(_planner_issue("P1", "binding_row_not_object", f"{binding.get('binding_id')}:{source_index}", "A selected collection entry is not an object."))
            continue
        if (
            approved_current_guidance is not None
            and str(raw.get("display_role") or "") in CURRENT_GUIDANCE_ROLES
            and source_index not in approved_current_guidance
        ):
            normalized_path = f"{source_path}.{source_index}"
            reason = f"stale_or_superseded_current_guidance:{guidance_scope_label(raw)}"
            skipped.append(
                _structured_skip(
                    binding,
                    normalized_path=normalized_path,
                    row_key=_row_key(raw, binding) or f"source_index:{source_index}",
                    reason=reason,
                    severity="P1",
                    source_ref=_row_source_ref(raw, binding),
                )
            )
            issues.append(
                _planner_issue(
                    "P1",
                    "stale_guidance_selected_for_current_block",
                    normalized_path,
                    "A guidance row marked current is not the latest valid row in its canonical metric/horizon scope.",
                )
            )
            continue
        exclusion_reason = _selector_exclusion_reason(raw, selector)
        if exclusion_reason:
            skipped.append(
                _structured_skip(
                    binding,
                    normalized_path=f"{source_path}.{source_index}",
                    row_key=_row_key(raw, binding) or f"source_index:{source_index}",
                    reason=f"row_selector_excluded:{exclusion_reason}",
                    severity="P2",
                    source_ref=_row_source_ref(raw, binding),
                )
            )
            continue
        selected = dict(raw)
        selected["__planner_source_index"] = source_index
        rows.append(selected)
    valid_sort_rows: list[Mapping[str, Any]] = []
    sort_fields = _sort_fields(binding.get("sort_order") or [])
    for row in rows:
        missing_sort_fields = [field for field in sort_fields if not _read_row_field(row, field)[2]]
        if not missing_sort_fields:
            valid_sort_rows.append(row)
            continue
        source_index = int(row.get("__planner_source_index") or 0)
        normalized_path = f"{source_path}.{source_index}"
        reason = "missing_sort_keys: " + ", ".join(missing_sort_fields)
        skipped.append(
            _structured_skip(binding, normalized_path=normalized_path, row_key=f"source_index:{source_index}", reason=reason, severity="P1", source_ref=_row_source_ref(row, binding))
        )
        issues.append(_planner_issue("P1", "binding_sort_key_missing", f"{binding.get('binding_id')}:{source_index}", reason))
    rows = valid_sort_rows
    period_identity = selector.get("period_identity")
    if isinstance(period_identity, Mapping):
        rows, period_skips, period_issues = _validate_quarterly_period_identities(
            rows,
            binding,
            selector,
            source_path=source_path,
        )
        skipped.extend(period_skips)
        issues.extend(period_issues)
    rows = _sort_rows(rows, binding.get("sort_order") or [])
    keyed_rows: dict[str, Mapping[str, Any]] = {}
    unique_rows: list[Mapping[str, Any]] = []
    for row in rows:
        key = _row_key(row, binding)
        source_index = int(row.get("__planner_source_index") or 0)
        normalized_path = f"{source_path}.{source_index}"
        if not key:
            skipped.append(
                _structured_skip(binding, normalized_path=normalized_path, row_key=f"source_index:{source_index}", reason="row_key_missing", severity="P1", source_ref=_row_source_ref(row, binding))
            )
            issues.append(_planner_issue("P1", "binding_row_key_missing", str(binding.get("binding_id") or ""), "Selected row does not provide every row_key field."))
            continue
        prior_row = keyed_rows.get(key)
        if prior_row is not None:
            skipped.append(
                _structured_skip(binding, normalized_path=normalized_path, row_key=key, reason="duplicate_row_key", severity="P1", source_ref=_row_source_ref(row, binding))
            )
            message = f"Duplicate row_key {key!r}."
            if _is_segment_binding(binding):
                message = _segment_duplicate_row_message(prior_row, row, binding, key)
            issues.append(_planner_issue("P1", "binding_row_key_duplicate", str(binding.get("binding_id") or ""), message))
            continue
        keyed_rows[key] = row
        unique_rows.append(row)
    rows = unique_rows
    window = str(selector.get("window") or "all")
    if window == "latest_capacity" and rows:
        capacity = int(binding.get("capacity") or 0)
        if capacity > 0 and len(rows) > capacity:
            excluded = rows[:-capacity]
            for row in excluded:
                source_index = int(row.get("__planner_source_index") or 0)
                skipped.append(
                    _structured_skip(
                        binding,
                        normalized_path=f"{source_path}.{source_index}",
                        row_key=_row_key(row, binding) or f"source_index:{source_index}",
                        reason="period_axis_outside_visible_window",
                        severity="P2",
                        source_ref=_row_source_ref(row, binding),
                    )
                )
            rows = rows[-capacity:]
    pick = str(selector.get("pick") or "all")
    if pick in {"latest", "first"} and rows:
        eligible_rows = list(rows)
        selected_row = eligible_rows[-1] if pick == "latest" else eligible_rows[0]
        selected_key = _row_key(selected_row, binding) or f"source_index:{selected_row.get('__planner_source_index', 0)}"
        disposition = str(selector.get("pick_exclusion_disposition") or "possible_ambiguity")
        for excluded_row in eligible_rows:
            if excluded_row is selected_row:
                continue
            source_index = int(excluded_row.get("__planner_source_index") or 0)
            excluded_key = _row_key(excluded_row, binding) or f"source_index:{source_index}"
            skipped.append(
                _structured_skip(
                    binding,
                    normalized_path=f"{source_path}.{source_index}",
                    row_key=excluded_key,
                    reason=f"row_selector_pick_excluded:{disposition}",
                    severity="P2",
                    source_ref=_row_source_ref(excluded_row, binding),
                    selector_rule=f"pick={pick}",
                    selected_row_key=selected_key,
                    excluded_row_key=excluded_key,
                    period=_first_populated_row_scalar(excluded_row, ("period", "quarter", "horizon")),
                    exclusion_disposition=disposition,
                )
            )
        rows = [selected_row]
    if isinstance(period_identity, Mapping) and rows:
        metric_skips, metric_issues = _validate_selected_quarterly_metric(
            rows[0],
            binding,
            period_identity,
            source_path=source_path,
        )
        skipped.extend(metric_skips)
        issues.extend(metric_issues)
        if metric_issues:
            rows = []
    return rows, skipped, issues


def _validate_quarterly_period_identities(
    rows: Sequence[Mapping[str, Any]],
    binding: Mapping[str, Any],
    selector: Mapping[str, Any],
    *,
    source_path: str,
) -> tuple[list[Mapping[str, Any]], list[dict[str, Any]], list[NormalizedDataIssue]]:
    contract = selector.get("period_identity")
    if not isinstance(contract, Mapping):
        return list(rows), [], []
    binding_id = str(binding.get("binding_id") or "")
    if (
        str(contract.get("period_type") or "") != "quarterly"
        or str(contract.get("entity_scope") or "") != "total_company"
        or source_path != "quarterly_financials.rows"
    ):
        message = (
            "The typed latest-period selector must resolve quarterly total-company rows "
            "from quarterly_financials.rows."
        )
        return [], [], [_planner_issue("P1", "binding_period_identity_contract_invalid", binding_id, message)]

    period_field = str(contract.get("period_field") or "period")
    fiscal_year_field = str(contract.get("fiscal_year_field") or "fiscal_year")
    fiscal_quarter_field = str(contract.get("fiscal_quarter_field") or "fiscal_quarter")
    valid: list[Mapping[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    issues: list[NormalizedDataIssue] = []
    for row in rows:
        source_index = int(row.get("__planner_source_index") or 0)
        normalized_path = f"{source_path}.{source_index}"
        raw_period, _period_ref, period_populated = _read_row_field(row, period_field)
        raw_year, _year_ref, year_populated = _read_row_field(row, fiscal_year_field)
        raw_quarter, _quarter_ref, quarter_populated = _read_row_field(row, fiscal_quarter_field)
        match = _QUARTER_PERIOD_RE.fullmatch(str(raw_period or "")) if period_populated else None
        valid_year = isinstance(raw_year, int) and not isinstance(raw_year, bool)
        valid_quarter = isinstance(raw_quarter, int) and not isinstance(raw_quarter, bool)
        if (
            match is None
            or not year_populated
            or not quarter_populated
            or not valid_year
            or not valid_quarter
            or int(match.group("year")) != int(raw_year)
            or int(match.group("quarter")) != int(raw_quarter)
        ):
            source_ref = _row_source_ref(row, binding)
            message = (
                f"Invalid quarterly identity for {binding_id!r}: period={raw_period!r}, "
                f"fiscal_year={raw_year!r}, fiscal_quarter={raw_quarter!r}, "
                f"source_row={normalized_path!r}, source_ref={source_ref!r}."
            )
            skipped.append(
                _structured_skip(
                    binding,
                    normalized_path=normalized_path,
                    row_key=str(raw_period or f"source_index:{source_index}"),
                    reason="invalid_quarterly_period_identity",
                    severity="P1",
                    source_ref=source_ref,
                )
            )
            issues.append(
                _planner_issue(
                    "P1",
                    "binding_quarterly_period_identity_invalid",
                    f"{binding_id}:{source_index}",
                    message,
                    normalized_path=normalized_path,
                    business_row_key=str(raw_period or ""),
                    binding_id=binding_id,
                    source_ref=source_ref,
                )
            )
            continue
        valid.append(row)
    return valid, skipped, issues


def _validate_selected_quarterly_metric(
    row: Mapping[str, Any],
    binding: Mapping[str, Any],
    contract: Mapping[str, Any],
    *,
    source_path: str,
) -> tuple[list[dict[str, Any]], list[NormalizedDataIssue]]:
    binding_id = str(binding.get("binding_id") or "")
    source_index = int(row.get("__planner_source_index") or 0)
    normalized_path = f"{source_path}.{source_index}"
    metric_field = str(contract.get("metric_field") or "")
    expected_unit = str(contract.get("unit") or "")
    period_field = str(contract.get("period_field") or "period")
    period = str(_read_row_field(row, period_field)[0] or "")
    metric = _path_get(row, metric_field)
    metric_value, metric_source_ref, populated = _unwrap_field(metric)
    metric_unit = str(metric.get("unit") or "") if isinstance(metric, Mapping) else ""
    metric_period = str(metric.get("period") or "") if isinstance(metric, Mapping) else ""
    if populated and metric_unit == expected_unit and metric_period == period:
        return [], []
    source_ref = metric_source_ref or _row_source_ref(row, binding)
    message = (
        f"Latest quarterly metric is incompatible for {binding_id!r}: metric={metric_field!r}, "
        f"value={metric_value!r}, unit={metric_unit!r}, expected_unit={expected_unit!r}, "
        f"metric_period={metric_period!r}, selected_period={period!r}, "
        f"source_row={normalized_path!r}, source_ref={source_ref!r}."
    )
    skip = _structured_skip(
        binding,
        normalized_path=f"{normalized_path}.{metric_field}",
        row_key=period or f"source_index:{source_index}",
        reason="latest_quarter_metric_incompatible",
        severity="P1",
        source_ref=source_ref,
    )
    issue = _planner_issue(
        "P1",
        "binding_latest_quarter_metric_incompatible",
        f"{binding_id}:{metric_field}",
        message,
        normalized_path=f"{normalized_path}.{metric_field}",
        business_row_key=period,
        binding_id=binding_id,
        source_ref=source_ref,
    )
    return [skip], [issue]


def inspect_binding_eligibility(
    package: Mapping[str, Any],
    binding: Mapping[str, Any],
) -> dict[str, Any]:
    """Expose the planner's actual selector result for read-only coverage audits."""

    if str(binding.get("planning_state") or "active") != "active":
        return {
            "selected_rows": [],
            "structured_exclusions": [],
            "issues": [],
            "planning_state": str(binding.get("planning_state") or "inactive"),
        }
    if not isinstance(binding.get("row_selector"), Mapping):
        return {
            "selected_rows": [],
            "structured_exclusions": [],
            "issues": [],
            "planning_state": "active_scalar_or_nonrow",
        }
    rows, exclusions, issues = _selected_rows(package, binding)
    return {
        "selected_rows": [dict(row) for row in rows],
        "structured_exclusions": exclusions,
        "issues": [issue.to_dict() for issue in issues],
        "planning_state": "active",
    }


def _validate_manifest_and_binding_contracts(
    manifest: Mapping[str, Any],
    bindings: Sequence[Mapping[str, Any]],
) -> list[NormalizedDataIssue]:
    issues: list[NormalizedDataIssue] = []
    sheets = {str(sheet.get("sheet") or ""): sheet for sheet in manifest.get("sheets", []) if isinstance(sheet, Mapping)}
    cell_contracts, merge_families, manifest_contract_issues = _prepare_manifest_planner_contracts(manifest)
    issues.extend(manifest_contract_issues)
    active_axis_headers: dict[str, list[str]] = {}
    for binding in bindings:
        if str(binding.get("planning_state") or "active") != "active" or not bool(binding.get("writable")):
            continue
        if str(binding.get("period_axis_role") or "") == "header":
            active_axis_headers.setdefault(str(binding.get("period_axis_id") or ""), []).append(str(binding.get("binding_id") or ""))
    for axis_id, binding_ids in active_axis_headers.items():
        if not axis_id:
            issues.append(_planner_issue("P1", "binding_period_axis_id_missing", ",".join(binding_ids), "Period-axis header requires a non-empty period_axis_id."))
        elif len(binding_ids) != 1:
            issues.append(_planner_issue("P1", "binding_period_axis_duplicate", axis_id, "A period axis must have exactly one active header binding."))
    for binding in bindings:
        binding_id = str(binding.get("binding_id") or "")
        if not binding_id:
            issues.append(_planner_issue("P1", "binding_id_missing", "$", "Binding has no binding_id."))
            continue
        mode = _planning_mode(binding)
        active = str(binding.get("planning_state") or "active") == "active"
        sheet = sheets.get(str(binding.get("sheet") or ""))
        if sheet is None:
            issues.append(_planner_issue("P1", "binding_sheet_missing", binding_id, "Binding sheet is absent from the shell manifest."))
            continue
        target = str(binding.get("target") or "")
        planner_target = _planner_target(binding)
        try:
            target_range = _parse_range(target)
        except BindingPlanningError as exc:
            issues.append(_planner_issue("P1", "binding_target_invalid", binding_id, str(exc)))
            continue
        try:
            planner_target_range = _parse_range(planner_target)
        except BindingPlanningError as exc:
            issues.append(_planner_issue("P1", "binding_planner_target_invalid", binding_id, str(exc)))
            continue
        if not _contains(target_range, planner_target_range):
            issues.append(_planner_issue("P1", "binding_planner_target_outside_target", binding_id, "planner_target is outside the declared binding target."))
        if bool(binding.get("writable")):
            zone_id = str(binding.get("shell_zone") or "")
            zone = next((item for item in sheet.get("writable_zones", []) if isinstance(item, Mapping) and str(item.get("zone_id") or "") == zone_id), None)
            if zone is None:
                issues.append(_planner_issue("P1", "binding_shell_zone_missing", binding_id, f"Writable shell zone {zone_id!r} is missing."))
            else:
                try:
                    if not _contains(_parse_range(str(zone.get("target") or "")), target_range):
                        issues.append(_planner_issue("P1", "binding_outside_shell_zone", binding_id, "Binding target is outside its declared writable shell zone."))
                except BindingPlanningError as exc:
                    issues.append(_planner_issue("P1", "binding_shell_zone_invalid", binding_id, str(exc)))
            for zone in sheet.get("non_writable_zones", []):
                if isinstance(zone, Mapping) and _overlaps(target_range, _parse_range(str(zone.get("target") or ""))):
                    issues.append(_planner_issue("P1", "binding_overlaps_non_writable_zone", binding_id, "Binding target overlaps a non-writable shell zone."))
        if active and bool(binding.get("writable")) and mode != "formula_owned":
            issues.extend(_validate_exact_target_contracts(binding, cell_contracts, merge_families))
        if mode in _ROW_MODES:
            missing = sorted(key for key in _ROW_CONTRACT_KEYS if key not in binding)
            if missing:
                issues.append(_planner_issue("P1", "binding_row_contract_missing", binding_id, f"Binding is missing planner fields: {', '.join(missing)}."))
                continue
            if not isinstance(binding.get("row_selector"), Mapping):
                issues.append(_planner_issue("P1", "binding_row_selector_invalid", binding_id, "row_selector must be an object."))
            if not isinstance(binding.get("row_key"), list) or not binding.get("row_key"):
                issues.append(_planner_issue("P1", "binding_row_key_invalid", binding_id, "row_key must be a non-empty list."))
            elif any(not isinstance(field, str) or not field.strip() for field in binding.get("row_key") or []) or len(binding.get("row_key") or []) != len(set(binding.get("row_key") or [])):
                issues.append(_planner_issue("P1", "binding_row_key_invalid", binding_id, "row_key entries must be unique non-empty field names."))
            if not isinstance(binding.get("sort_order"), list):
                issues.append(_planner_issue("P1", "binding_sort_order_invalid", binding_id, "sort_order must be a list."))
            else:
                issues.extend(_validate_sort_order_contract(binding))
            selector = binding.get("row_selector")
            if isinstance(selector, Mapping) and str(selector.get("pick") or "all") not in {"all", "first", "latest"}:
                issues.append(_planner_issue("P1", "binding_row_pick_invalid", binding_id, "row_selector.pick must be all, first, or latest."))
            if isinstance(selector, Mapping) and "period_identity" in selector:
                issues.extend(_validate_period_identity_contract(binding, selector))
            if isinstance(selector, Mapping) and str(selector.get("pick") or "all") in {"first", "latest"}:
                disposition = str(selector.get("pick_exclusion_disposition") or "")
                if disposition not in {"expected_supersession", "expected_priority_selection", "possible_ambiguity"}:
                    issues.append(
                        _planner_issue(
                            "P1",
                            "binding_pick_exclusion_disposition_missing",
                            binding_id,
                            "first/latest selectors require an explicit pick_exclusion_disposition.",
                        )
                    )
            if isinstance(selector, Mapping) and str(selector.get("window") or "all") not in {"all", "latest_capacity"}:
                issues.append(_planner_issue("P1", "binding_row_window_invalid", binding_id, "row_selector.window must be all or latest_capacity."))
            if not isinstance(binding.get("capacity"), int) or int(binding.get("capacity") or 0) < 1:
                issues.append(_planner_issue("P1", "binding_capacity_invalid", binding_id, "capacity must be a positive integer."))
            if "minimum_rows" in binding:
                if not isinstance(binding.get("minimum_rows"), int) or int(binding.get("minimum_rows") or 0) < 0:
                    issues.append(_planner_issue("P1", "binding_minimum_rows_invalid", binding_id, "minimum_rows must be a non-negative integer."))
                elif int(binding.get("minimum_rows") or 0) > int(binding.get("capacity") or 0):
                    issues.append(_planner_issue("P1", "binding_minimum_rows_exceeds_capacity", binding_id, "minimum_rows cannot exceed capacity."))
            if "target_rows" in binding:
                target_rows = binding.get("target_rows")
                if not isinstance(target_rows, list) or not target_rows or any(not isinstance(row, int) or row < planner_target_range[1] or row > planner_target_range[3] for row in target_rows):
                    issues.append(_planner_issue("P1", "binding_target_rows_invalid", binding_id, "target_rows must be unique worksheet row numbers inside planner_target."))
                elif len(target_rows) != len(set(target_rows)):
                    issues.append(_planner_issue("P1", "binding_target_rows_invalid", binding_id, "target_rows must not contain duplicates."))
            if str(binding.get("overflow_behavior") or "") not in {"fail", "mapping_gap", "manual_review"}:
                issues.append(_planner_issue("P1", "binding_overflow_behavior_invalid", binding_id, "overflow_behavior must be fail, mapping_gap, or manual_review."))
            if not isinstance(binding.get("required_columns"), list):
                issues.append(_planner_issue("P1", "binding_required_columns_invalid", binding_id, "required_columns must be a list."))
            target_columns = binding.get("target_columns")
            if mode in _TABLE_MODES and active:
                issues.extend(_validate_target_columns(binding, planner_target_range, target_columns))
            elif mode == "series":
                if not str(binding.get("target_type") or ""):
                    issues.append(_planner_issue("P1", "binding_target_type_missing", binding_id, "Series binding requires an explicit target_type."))
                try:
                    if int(binding.get("capacity") or 0) != len(_series_coordinates(planner_target, str(binding.get("target_axis") or "columns"))):
                        issues.append(_planner_issue("P1", "binding_capacity_mismatch", binding_id, "Series capacity does not equal target cell capacity."))
                except BindingPlanningError as exc:
                    issues.append(_planner_issue("P1", "binding_target_invalid", binding_id, str(exc)))
        elif active and bool(binding.get("writable")):
            if not str(binding.get("target_type") or ""):
                issues.append(_planner_issue("P1", "binding_target_type_missing", binding_id, "Scalar/text binding requires an explicit target_type."))
        period_axis_role = str(binding.get("period_axis_role") or "")
        period_axis_id = str(binding.get("period_axis_id") or "")
        if period_axis_role and period_axis_role not in {"header", "dependent"}:
            issues.append(_planner_issue("P1", "binding_period_axis_role_invalid", binding_id, "period_axis_role must be header or dependent."))
        if mode in _PIVOT_MODES and active:
            if period_axis_role != "dependent" or not period_axis_id:
                issues.append(_planner_issue("P1", "binding_period_axis_contract_missing", binding_id, "Every active pivot binding must depend on a declared period_axis_id."))
            if "period_alignment" in binding or "period_target_columns" in binding:
                issues.append(_planner_issue("P1", "binding_independent_period_alignment_forbidden", binding_id, "Pivot bindings may not independently align periods or declare their own period columns."))
            row_blocks = binding.get("row_blocks")
            if isinstance(row_blocks, Mapping):
                row_owners: dict[int, str] = {}
                for dimension, raw_rows in row_blocks.items():
                    if not isinstance(raw_rows, list) or not raw_rows:
                        issues.append(_planner_issue("P1", "binding_pivot_row_block_invalid", binding_id, f"Pivot dimension {dimension!r} requires at least one target row."))
                        continue
                    for raw_row in raw_rows:
                        if not isinstance(raw_row, int):
                            issues.append(_planner_issue("P1", "binding_pivot_row_block_invalid", binding_id, f"Pivot dimension {dimension!r} contains a non-integer target row."))
                            continue
                        prior = row_owners.get(raw_row)
                        if prior is not None and prior != str(dimension):
                            issues.append(
                                _planner_issue(
                                    "P1",
                                    "binding_pivot_row_block_overlap",
                                    binding_id,
                                    f"Pivot row {raw_row} is shared by dimensions {prior!r} and {dimension!r}; dimension blocks must be disjoint.",
                                )
                            )
                        row_owners[raw_row] = str(dimension)
        if period_axis_role == "header":
            if mode != "series" or str(binding.get("source_field") or "") != str(binding.get("period_field") or "period"):
                issues.append(_planner_issue("P1", "binding_period_axis_header_invalid", binding_id, "Period-axis header must be a series whose source_field is its period_field."))
            continuity = str(binding.get("period_axis_continuity") or "")
            if continuity not in {"consecutive_quarters", "consecutive_fiscal_years"}:
                issues.append(_planner_issue("P1", "binding_period_axis_continuity_missing", binding_id, "Period-axis headers require an explicit supported continuity contract."))
        if period_axis_role == "dependent" and period_axis_id not in active_axis_headers:
            issues.append(_planner_issue("P1", "binding_period_axis_header_missing", binding_id, f"No active header binding declares period axis {period_axis_id!r}."))
        if period_axis_role == "dependent" and not str(binding.get("period_field") or ""):
            issues.append(_planner_issue("P1", "binding_period_axis_period_field_missing", binding_id, "A period-axis dependent binding requires period_field."))
        normalized_field = str(binding.get("normalized_field") or "")
        if str(binding.get("sheet") or "") == "Valuation" and "output" in str(binding.get("section") or "").lower() and normalized_field.startswith("mapping_gaps"):
            issues.append(_planner_issue("P1", "valuation_output_mapping_gap_forbidden", binding_id, "Valuation output rows must consume explicit valuation_outputs or be formula-owned; they must never consume mapping_gaps."))
        if normalized_field.startswith(("mapping_gaps", "manual_review_flags")) and str(binding.get("sheet") or "") not in _QA_SHEETS:
            issues.append(_planner_issue("P1", "qa_output_outside_qa_sheet", binding_id, "Mapping gaps and manual-review flags may only bind to QA_Log, Needs_Review, or QA_Checks."))
    return issues


def _validate_period_identity_contract(
    binding: Mapping[str, Any],
    selector: Mapping[str, Any],
) -> list[NormalizedDataIssue]:
    binding_id = str(binding.get("binding_id") or "")
    contract = selector.get("period_identity")
    if not isinstance(contract, Mapping):
        return [_planner_issue("P1", "binding_period_identity_contract_invalid", binding_id, "period_identity must be an object.")]
    expected = {
        "period_type": "quarterly",
        "entity_scope": "total_company",
        "period_field": "period",
        "fiscal_year_field": "fiscal_year",
        "fiscal_quarter_field": "fiscal_quarter",
    }
    invalid = [key for key, value in expected.items() if str(contract.get(key) or "") != value]
    metric_field = str(contract.get("metric_field") or "")
    unit = str(contract.get("unit") or "")
    if invalid or not metric_field or not unit:
        return [
            _planner_issue(
                "P1",
                "binding_period_identity_contract_invalid",
                binding_id,
                f"Invalid typed quarterly selector fields: invalid={invalid!r}, metric_field={metric_field!r}, unit={unit!r}.",
            )
        ]
    issues: list[NormalizedDataIssue] = []
    if str(selector.get("source_path") or "") != "quarterly_financials.rows":
        issues.append(_planner_issue("P1", "binding_period_identity_source_invalid", binding_id, "Typed total-company quarterly selection requires quarterly_financials.rows."))
    if str(selector.get("pick") or "") != "latest":
        issues.append(_planner_issue("P1", "binding_period_identity_pick_invalid", binding_id, "Typed SUMMARY selection requires pick=latest."))
    if "period" not in {str(value) for value in binding.get("row_key") or []}:
        issues.append(_planner_issue("P1", "binding_period_identity_row_key_invalid", binding_id, "Typed SUMMARY selection requires period in row_key."))
    if "period" not in _sort_fields(binding.get("sort_order") or []):
        issues.append(_planner_issue("P1", "binding_period_identity_sort_invalid", binding_id, "Typed SUMMARY selection requires deterministic period sorting."))
    source_field = str(binding.get("source_field") or "")
    if source_field not in {"period", metric_field}:
        issues.append(
            _planner_issue(
                "P1",
                "binding_period_identity_metric_invalid",
                binding_id,
                f"source_field {source_field!r} must be period or the declared metric_field {metric_field!r}.",
            )
        )
    return issues


def _validate_target_columns(
    binding: Mapping[str, Any],
    target_range: tuple[int, int, int, int],
    target_columns: Any,
) -> list[NormalizedDataIssue]:
    binding_id = str(binding.get("binding_id") or "")
    if not isinstance(target_columns, list) or not target_columns:
        return [_planner_issue("P1", "binding_target_columns_missing", binding_id, "Table binding requires non-empty target_columns.")]
    seen: set[str] = set()
    mapped_source_fields: set[str] = set()
    issues: list[NormalizedDataIssue] = []
    left, _top, right, _bottom = target_range
    for column in target_columns:
        if not isinstance(column, Mapping):
            issues.append(_planner_issue("P1", "binding_target_column_invalid", binding_id, "target_columns entries must be objects."))
            continue
        target_column = str(column.get("target_column") or "").upper()
        source_field = str(column.get("source_field") or column.get("column_id") or "")
        if not target_column or not source_field:
            issues.append(_planner_issue("P1", "binding_target_column_invalid", binding_id, "Each target column needs target_column and source_field."))
            continue
        mapped_source_fields.add(source_field)
        if not _column_target_type(binding, column):
            issues.append(_planner_issue("P1", "binding_target_type_missing", binding_id, f"Target column {target_column or '?'} requires target_type."))
        try:
            index = _column_index(target_column)
        except BindingPlanningError as exc:
            issues.append(_planner_issue("P1", "binding_target_column_invalid", binding_id, str(exc)))
            continue
        if index < left or index > right:
            issues.append(_planner_issue("P1", "binding_target_column_outside_target", binding_id, f"Column {target_column} is outside {binding.get('target')}."))
        if target_column in seen:
            issues.append(_planner_issue("P1", "binding_target_cell_collision", binding_id, f"Multiple row-schema fields resolve to column {target_column}."))
        seen.add(target_column)
    row_key_only = {str(field) for field in binding.get("row_key_only_columns") or []}
    missing_required_targets = [
        str(field)
        for field in binding.get("required_columns") or []
        if str(field) not in mapped_source_fields and str(field) not in row_key_only
    ]
    if missing_required_targets:
        issues.append(
            _planner_issue(
                "P1",
                "binding_required_column_unmapped",
                binding_id,
                "Required row fields have no target column or declared row-key-only role: " + ", ".join(missing_required_targets) + ".",
            )
        )
    return issues


def _prepare_manifest_planner_contracts(
    manifest: Mapping[str, Any],
) -> tuple[dict[str, list[tuple[tuple[int, int, int, int], Mapping[str, Any]]]], dict[str, list[tuple[tuple[int, int, int, int], Mapping[str, Any]]]], list[NormalizedDataIssue]]:
    cell_contracts: dict[str, list[tuple[tuple[int, int, int, int], Mapping[str, Any]]]] = {}
    merge_families: dict[str, list[tuple[tuple[int, int, int, int], Mapping[str, Any]]]] = {}
    issues: list[NormalizedDataIssue] = []
    for key, destination in (("planner_cell_contracts", cell_contracts), ("planner_merge_families", merge_families)):
        raw_contracts = manifest.get(key)
        if not isinstance(raw_contracts, list) or not raw_contracts:
            issues.append(_planner_issue("P1", "manifest_exact_cell_contracts_missing", key, f"Manifest requires a non-empty {key} list."))
            continue
        for idx, contract in enumerate(raw_contracts):
            field = f"{key}.{idx}"
            if not isinstance(contract, Mapping):
                issues.append(_planner_issue("P1", "manifest_exact_cell_contract_invalid", field, "Planner manifest contract must be an object."))
                continue
            sheet = str(contract.get("sheet") or "")
            target = str(contract.get("target") or "")
            try:
                parsed = _parse_range(target)
            except BindingPlanningError as exc:
                issues.append(_planner_issue("P1", "manifest_exact_cell_contract_invalid", field, str(exc)))
                continue
            if not sheet:
                issues.append(_planner_issue("P1", "manifest_exact_cell_contract_invalid", field, "Planner manifest contract requires sheet."))
                continue
            destination.setdefault(sheet, []).append((parsed, contract))
    return cell_contracts, merge_families, issues


def _validate_exact_target_contracts(
    binding: Mapping[str, Any],
    cell_contracts: Mapping[str, Sequence[tuple[tuple[int, int, int, int], Mapping[str, Any]]]],
    merge_families: Mapping[str, Sequence[tuple[tuple[int, int, int, int], Mapping[str, Any]]]],
) -> list[NormalizedDataIssue]:
    binding_id = str(binding.get("binding_id") or "")
    sheet = str(binding.get("sheet") or "")
    issues: list[NormalizedDataIssue] = []
    for cell, target_type, target_role in _declared_target_specs(binding):
        cell_range = _parse_range(cell)
        for merge_range, merge in merge_families.get(sheet, ()):
            if not _contains(merge_range, cell_range):
                continue
            anchor_column = str(merge.get("anchor_column") or "").upper()
            cell_column = re.match(r"^[A-Z]+", cell).group(0)  # validated by _parse_range
            if cell_column != anchor_column:
                issues.append(_planner_issue("P1", "manifest_merge_non_anchor_target", f"{binding_id}:{cell}", f"{sheet}!{cell} is inside a merged family owned by column {anchor_column}."))
            merge_binding_ids = {str(item) for item in merge.get("allowed_binding_ids") or []}
            if merge_binding_ids and binding_id not in merge_binding_ids:
                issues.append(_planner_issue("P1", "manifest_merge_owner_mismatch", f"{binding_id}:{cell}", f"Binding {binding_id} does not own merged family {merge.get('target')}."))
            owner_roles = {str(item) for item in merge.get("owner_roles") or []}
            if owner_roles and target_role not in owner_roles:
                issues.append(_planner_issue("P1", "manifest_merge_owner_mismatch", f"{binding_id}:{cell}", f"Target role {target_role!r} does not own merged family {merge.get('target')}."))
        matches = [contract for contract_range, contract in cell_contracts.get(sheet, ()) if _contains(contract_range, cell_range)]
        if not matches:
            issues.append(_planner_issue("P1", "manifest_exact_writable_cell_missing", f"{binding_id}:{cell}", f"{sheet}!{cell} has no exact writable planner cell contract."))
            continue
        if len(matches) > 1:
            issues.append(_planner_issue("P1", "manifest_exact_writable_cell_ambiguous", f"{binding_id}:{cell}", f"{sheet}!{cell} matches multiple planner cell contracts."))
            continue
        contract = matches[0]
        if contract.get("writable") is not True:
            issues.append(_planner_issue("P1", "manifest_exact_cell_protected", f"{binding_id}:{cell}", f"{sheet}!{cell} is not declared writable."))
        allowed_binding_ids = {str(item) for item in contract.get("allowed_binding_ids") or []}
        if allowed_binding_ids and binding_id not in allowed_binding_ids:
            issues.append(_planner_issue("P1", "manifest_cell_owner_mismatch", f"{binding_id}:{cell}", f"{sheet}!{cell} is not owned by binding {binding_id}."))
        allowed_types = {str(item) for item in contract.get("allowed_target_types") or []}
        if not target_type or (allowed_types and target_type not in allowed_types):
            issues.append(_planner_issue("P1", "manifest_target_type_mismatch", f"{binding_id}:{cell}", f"Target type {target_type!r} is not allowed for {sheet}!{cell}."))
        allowed_roles = {str(item) for item in contract.get("allowed_target_roles") or []}
        declared_role = str(contract.get("target_role") or "")
        binding_owned_role = target_role == binding_id or target_role.startswith(f"{binding_id}.")
        if not declared_role:
            issues.append(_planner_issue("P1", "manifest_target_role_missing", f"{binding_id}:{cell}", f"{sheet}!{cell} has no declared manifest target_role."))
        if not target_role or not binding_owned_role or (allowed_roles and target_role not in allowed_roles):
            issues.append(_planner_issue("P1", "manifest_target_role_mismatch", f"{binding_id}:{cell}", f"Target role {target_role!r} is not allowed for {sheet}!{cell}."))
    return issues


def _declared_target_specs(binding: Mapping[str, Any]) -> list[tuple[str, str, str]]:
    mode = _planning_mode(binding)
    target = _planner_target(binding)
    if mode in _TABLE_MODES:
        start_row, end_row = _range_rows(target)
        target_rows = _table_target_rows(binding, start_row, end_row)
        return [
            (
                f"{str(column.get('target_column') or '').upper()}{row}",
                _column_target_type(binding, column),
                _column_target_role(binding, column),
            )
            for row in target_rows
            for column in binding.get("target_columns") or []
            if isinstance(column, Mapping)
        ]
    if mode in _PIVOT_MODES:
        rows = sorted({int(row) for block_rows in (binding.get("row_blocks") or {}).values() for row in block_rows})
        label_column = str(binding.get("label_target_column") or "A").upper()
        min_col, _min_row, max_col, _max_row = _parse_range(target)
        period_columns = [
            _column_label(column)
            for column in range(min_col, max_col + 1)
            if _column_label(column) != label_column
        ]
        return [
            *((f"{label_column}{row}", "text", f"{binding['binding_id']}.member") for row in rows),
            *((f"{column}{row}", str(binding.get("value_target_type") or "number"), f"{binding['binding_id']}.value") for row in rows for column in period_columns),
        ]
    if mode == "series":
        return [
            (cell, str(binding.get("target_type") or ""), _binding_target_role(binding))
            for cell in _series_coordinates(target, str(binding.get("target_axis") or "columns"))
        ]
    return [(_first_cell(target), str(binding.get("target_type") or ""), _binding_target_role(binding))]


def _validate_sort_order_contract(binding: Mapping[str, Any]) -> list[NormalizedDataIssue]:
    binding_id = str(binding.get("binding_id") or "")
    issues: list[NormalizedDataIssue] = []
    fields: list[str] = []
    for raw in binding.get("sort_order") or []:
        if isinstance(raw, str):
            field, separator, direction = raw.partition(":")
            direction = direction or "asc"
            if separator and direction.lower() not in {"asc", "desc"}:
                issues.append(_planner_issue("P1", "binding_sort_direction_invalid", binding_id, f"Invalid sort direction in {raw!r}."))
        elif isinstance(raw, Mapping):
            field = str(raw.get("field") or "")
            direction = str(raw.get("direction") or "asc")
            if direction.lower() not in {"asc", "desc"}:
                issues.append(_planner_issue("P1", "binding_sort_direction_invalid", binding_id, f"Invalid sort direction {direction!r}."))
        else:
            field = ""
        if not field:
            issues.append(_planner_issue("P1", "binding_sort_key_invalid", binding_id, "Every sort_order entry requires a field."))
        fields.append(field)
    if len(fields) != len(set(fields)):
        issues.append(_planner_issue("P1", "binding_sort_key_duplicate", binding_id, "sort_order contains duplicate fields."))
    return issues


def _add_blocking_reports(plan: BindingPlan, bindings: Sequence[Mapping[str, Any]]) -> None:
    for binding in bindings:
        if not bool(binding.get("writable")):
            continue
        plan.binding_reports.append(
            {
                "binding_id": str(binding.get("binding_id") or ""),
                "mode": _planning_mode(binding),
                "sheet": str(binding.get("sheet") or ""),
                "target": _planner_target(binding),
                "normalized_field": str(binding.get("normalized_field") or ""),
                "capacity": int(binding.get("capacity") or 0),
                "capacity_used": 0,
                "overflow_rows": [],
                "skipped_rows": [
                    _structured_skip(
                        binding,
                        normalized_path=str(binding.get("normalized_field") or ""),
                        row_key="binding",
                        reason="blocking_schema_or_contract_issue",
                        severity="P1",
                    )
                ],
                "planned_write_count": 0,
            }
        )


def _refresh_issue_ledger(plan: BindingPlan) -> None:
    trusted_keys = sorted(
        {
            issue.canonical_issue_key
            for issue in plan.planner_issues
            if issue.canonical_issue_key
        }
    )
    plan.issue_ledger = build_canonical_issue_ledger(
        manual_review_flags=plan.manual_review_flags,
        mapping_gaps=plan.mapping_gaps,
        validation_issues=plan.issues,
        check_results=_planner_check_results(plan),
        trusted_canonical_issue_keys=trusted_keys,
    )
    failures = validate_json_schema(plan.issue_ledger, load_json_strict(ISSUE_LEDGER_SCHEMA))
    existing = {(issue.rule_id, issue.field, issue.message) for issue in plan.planner_issues}
    added = False
    for field, keyword, message in failures:
        issue = _planner_issue(
            "P1",
            f"issue_ledger_schema_{keyword}",
            field,
            message,
            issue_type="validation_failure",
            root_cause="issue_ledger_schema_violation",
        )
        key = (issue.rule_id, issue.field, issue.message)
        if key not in existing:
            existing.add(key)
            plan.planner_issues.append(issue)
            added = True
    if added:
        plan.issue_ledger = build_canonical_issue_ledger(
            manual_review_flags=plan.manual_review_flags,
            mapping_gaps=plan.mapping_gaps,
            validation_issues=plan.issues,
            check_results=_planner_check_results(plan),
            trusted_canonical_issue_keys=trusted_keys,
        )


def _finalize_validation_outputs(plan: BindingPlan, bindings: Sequence[Mapping[str, Any]], *, ticker: str) -> None:
    """Plan QA from one final snapshot, or fail without retaining stale QA writes."""

    scratch = BindingPlan(ticker=ticker)
    scratch.issue_ledger = plan.issue_ledger
    scratch.planning_completed = True
    _plan_validation_outputs(scratch, bindings, ticker=ticker)
    _dedupe_plan(scratch)
    _validate_planned_write_types(scratch, bindings)
    if scratch.planner_issues or scratch.mapping_gaps:
        plan.binding_reports.extend(scratch.binding_reports)
        plan.planner_issues.extend(scratch.planner_issues)
        plan.mapping_gaps.extend(scratch.mapping_gaps)
        plan.planner_issues.append(
            _planner_issue(
                "P1",
                "qa_presentation_snapshot_unstable",
                "issue_ledger.qa_presentation",
                "QA presentation could not be planned losslessly from the final canonical ledger; no QA writes were retained.",
            )
        )
        _dedupe_plan(plan)
        _refresh_issue_ledger(plan)
        plan.qa_snapshot_status = "failed"
        return
    plan.binding_reports.extend(scratch.binding_reports)
    plan.planned_writes.extend(scratch.planned_writes)
    plan.qa_snapshot_status = "stable"


def _planner_check_results(plan: BindingPlan) -> list[dict[str, Any]]:
    shell_issues = [issue for issue in plan.planner_issues if issue.rule_id.startswith("shell_")]
    type_issues = [issue for issue in plan.planner_issues if issue.rule_id == "target_value_type_mismatch"]
    contract_issues = [issue for issue in plan.planner_issues if issue not in shell_issues and issue not in type_issues]
    return [
        _stage_check("normalized_json_schema_validation", plan.schema_issues, "binding_plan.schema_issues"),
        _stage_check("normalized_semantic_validation", plan.semantic_issues, "binding_plan.semantic_issues"),
        _stage_check("shell_identity_validation", shell_issues, "binding_plan.planner_issues[shell_identity]"),
        _stage_check(
            "manifest_binding_contract_validation",
            contract_issues,
            "binding_plan.planner_issues[binding_contract]",
        ),
        _stage_check(
            "binding_mapping_and_capacity_validation",
            plan.mapping_gaps,
            "binding_plan.mapping_gaps",
            executed=plan.planning_completed,
        ),
        _stage_check(
            "planned_write_type_validation",
            type_issues,
            "binding_plan.planner_issues[target_value_type]",
            executed=plan.planning_completed,
        ),
    ]


def _stage_check(
    rule_id: str,
    rows: Sequence[Any],
    detail_ref: str,
    *,
    executed: bool = True,
) -> dict[str, Any]:
    normalized_rows = [
        row.to_dict() if hasattr(row, "to_dict") else dict(row)
        for row in rows
        if hasattr(row, "to_dict") or isinstance(row, Mapping)
    ]
    severities = [str(row.get("severity") or "P2").upper() for row in normalized_rows]
    blocking_count = sum(1 for severity in severities if severity in _BLOCKING_SEVERITIES)
    actionable_count = sum(1 for severity in severities if severity not in {"P3"})
    if not executed:
        status = "INFO"
        interpretation = "Not executed because an earlier blocking validation stage failed."
    elif blocking_count:
        status = "FAIL"
        interpretation = f"{blocking_count} blocking issue(s) found across {len(normalized_rows)} result(s)."
    elif normalized_rows:
        status = "REVIEW"
        interpretation = f"{len(normalized_rows)} non-blocking result(s) require review."
    else:
        status = "PASS"
        interpretation = "Completed with no issues."
    sections = sorted(
        {
            str(row.get("section") or row.get("field") or row.get("normalized_path") or "").lstrip("$.").split(".", 1)[0]
            for row in normalized_rows
            if row.get("section") or row.get("field") or row.get("normalized_path")
        }
    )
    return {
        "rule_id": rule_id,
        "status": status,
        "unique_issue_count": len(normalized_rows),
        "occurrence_count": len(normalized_rows),
        "blocking_count": blocking_count,
        "actionable_count": actionable_count,
        "affected_sections": ", ".join(sections) or "none",
        "interpretation": interpretation,
        "detail_ref": detail_ref,
    }


def _plan_validation_outputs(plan: BindingPlan, bindings: Sequence[Mapping[str, Any]], *, ticker: str) -> None:
    """Append QA writes only after all business bindings have been planned.

    The planner owns derived mapping gaps.  Writing those rows while the normal
    bindings are still being evaluated would either lose late gaps or force the
    Excel layer to invent audit data.  QA rows stay in their declared QA sheets
    and use the same typed row contract as business tables.
    """

    presentation = plan.issue_ledger.get("qa_presentation") or {}
    rows_by_binding = {
        "qa_log_validation_rows": list(presentation.get("qa_log_rows") or []),
        "needs_review_validation_rows": list(presentation.get("needs_review_rows") or []),
        "qa_checks_mapping_gap_rows": list(presentation.get("qa_check_rows") or []),
    }
    for binding in bindings:
        if not bool(binding.get("writable")) or str(binding.get("source_policy") or "") != "validation-output":
            continue
        binding_id = str(binding.get("binding_id") or "")
        report = {
            "binding_id": binding_id,
            "mode": _planning_mode(binding),
            "sheet": _resolve_sheet(str(binding.get("sheet") or ""), ticker),
            "target": _planner_target(binding),
            "normalized_field": str(binding.get("normalized_field") or ""),
            "capacity": int(binding.get("capacity") or 0),
            "capacity_used": 0,
            "overflow_rows": [],
            "skipped_rows": [],
            "planned_write_count": 0,
        }
        if str(binding.get("planning_state") or "active") != "active":
            report["skipped_rows"].append({"reason": "validation_binding_blocked"})
            plan.binding_reports.append(report)
            continue
        rows = rows_by_binding.get(binding_id)
        if rows is None:
            issue = _planner_issue("P1", "unknown_validation_output_binding", binding_id, "Validation-output binding has no planner-owned row source.")
            plan.binding_reports.append(report)
            plan.planner_issues.append(issue)
            plan.mapping_gaps.append(_gap(binding, reason=issue.message, severity="P1"))
            continue
        prepared_rows, skipped, issues = _prepare_explicit_rows(rows, binding)
        report["skipped_rows"].extend(skipped)
        if issues:
            plan.binding_reports.append(report)
            plan.planner_issues.extend(issues)
            plan.mapping_gaps.extend(_gap(binding, reason=issue.message, severity=issue.severity) for issue in issues)
            continue
        planned_report, writes, gaps, row_issues = _plan_table_rows(prepared_rows, binding, ticker=ticker, report=report)
        plan.binding_reports.append(planned_report)
        plan.planned_writes.extend(writes)
        plan.mapping_gaps.extend(gaps)
        plan.planner_issues.extend(row_issues)


def _prepare_explicit_rows(
    rows: Sequence[Mapping[str, Any]],
    binding: Mapping[str, Any],
) -> tuple[list[Mapping[str, Any]], list[dict[str, Any]], list[NormalizedDataIssue]]:
    indexed_rows: list[Mapping[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    issues: list[NormalizedDataIssue] = []
    for source_index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            skipped.append(
                _structured_skip(binding, normalized_path=f"{binding.get('normalized_field')}.{source_index}", row_key=f"source_index:{source_index}", reason="row_not_object", severity="P1")
            )
            issues.append(_planner_issue("P1", "binding_row_not_object", f"{binding.get('binding_id')}:{source_index}", "A QA output row is not an object."))
            continue
        selected = dict(row)
        selected.setdefault("__planner_source_index", source_index)
        indexed_rows.append(selected)
    ordered = _sort_rows(indexed_rows, binding.get("sort_order") or [])
    unique_rows: list[Mapping[str, Any]] = []
    seen: set[str] = set()
    for row in ordered:
        row_key = _row_key(row, binding)
        source_index = int(row.get("__planner_source_index") or 0)
        normalized_path = f"{binding.get('normalized_field')}.{source_index}"
        if not row_key:
            skipped.append(
                _structured_skip(binding, normalized_path=normalized_path, row_key=f"source_index:{source_index}", reason="row_key_missing", severity="P1", source_ref=str(row.get("source_ref") or ""))
            )
            issues.append(_planner_issue("P1", "binding_row_key_missing", str(binding.get("binding_id") or ""), "QA row does not provide every row_key field."))
            continue
        if row_key in seen:
            skipped.append(
                _structured_skip(binding, normalized_path=normalized_path, row_key=row_key, reason="duplicate_row_key", severity="P1", source_ref=str(row.get("source_ref") or ""))
            )
            issues.append(_planner_issue("P1", "binding_row_key_duplicate", str(binding.get("binding_id") or ""), f"Duplicate row_key {row_key!r}."))
            continue
        seen.add(row_key)
        unique_rows.append(row)
    return unique_rows, skipped, issues


def _normalize_mapping_gaps(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    normalized: list[dict[str, Any]] = []
    for idx, raw in enumerate(value):
        if not isinstance(raw, Mapping):
            continue
        field = str(raw.get("field") or raw.get("normalized_field") or "")
        message = str(raw.get("message") or raw.get("reason") or "Mapped field is not populated.")
        row = dict(raw)
        row.update(
            {
                "severity": str(raw.get("severity") or "P2"),
                "rule_id": str(raw.get("rule_id") or "mapping_gap"),
                "field": field,
                "message": message,
                "source_ref": str(raw.get("source_ref") or ""),
                "suggested_action": str(raw.get("suggested_action") or raw.get("missing_source_behavior") or "Resolve the source or mapping before render."),
                "binding_id": str(raw.get("binding_id") or f"package_mapping_gap:{idx}"),
                "sheet": str(raw.get("sheet") or ""),
                "section": str(raw.get("section") or ""),
                "target": str(raw.get("target") or ""),
                "normalized_field": field,
                "normalized_path": str(raw.get("normalized_path") or field),
                "row_key": str(raw.get("row_key") or f"package_mapping_gap:{idx}"),
                "expected_target": str(raw.get("expected_target") or raw.get("target") or ""),
                "reason": message,
            }
        )
        normalized.append(row)
    return normalized


def _normalize_manual_review_flags(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    normalized: list[dict[str, Any]] = []
    for idx, raw in enumerate(value):
        if not isinstance(raw, Mapping):
            continue
        row = dict(raw)
        row.update(
            {
                "severity": str(raw.get("severity") or "P2"),
                "rule_id": str(raw.get("rule_id") or "manual_review_required"),
                "field": str(raw.get("field") or raw.get("normalized_field") or ""),
                "message": str(raw.get("message") or raw.get("reason") or "Manual review is required."),
                "source_ref": str(raw.get("source_ref") or ""),
                "suggested_action": str(raw.get("suggested_action") or "Resolve the evidence or mapping before promotion."),
                "binding_id": str(raw.get("binding_id") or f"package_manual_review:{idx}"),
                "normalized_path": str(raw.get("normalized_path") or raw.get("field") or raw.get("normalized_field") or ""),
                "row_key": str(raw.get("row_key") or f"package_manual_review:{idx}"),
            }
        )
        normalized.append(row)
    return normalized


def _dedupe_plan(plan: BindingPlan) -> None:
    by_target: dict[tuple[str, str], PlannedWrite] = {}
    collisions: list[NormalizedDataIssue] = []
    unique_writes: list[PlannedWrite] = []
    for write in plan.planned_writes:
        key = (write.target_sheet, write.target_cell)
        prior = by_target.get(key)
        if prior is not None:
            collisions.append(_planner_issue("P1", "binding_target_cell_collision", write.binding_id, f"{key[0]}!{key[1]} is planned by both {prior.binding_id} and {write.binding_id}."))
            continue
        by_target[key] = write
        unique_writes.append(write)
    plan.planned_writes = unique_writes
    plan.planner_issues.extend(collisions)


def _validate_planned_write_types(plan: BindingPlan, bindings: Sequence[Mapping[str, Any]]) -> None:
    binding_by_id = {str(binding.get("binding_id") or ""): binding for binding in bindings}
    existing = {(issue.rule_id, issue.field, issue.message) for issue in plan.planner_issues}
    for write in plan.planned_writes:
        if _target_accepts_value(write.target_type, write.value):
            continue
        message = f"Value type {write.value_type!r} is incompatible with target_type {write.target_type!r}."
        key = ("target_value_type_mismatch", f"{write.binding_id}:{write.row_key}:{write.target_cell}", message)
        if key in existing:
            continue
        existing.add(key)
        issue = _planner_issue("P1", key[0], key[1], message)
        plan.planner_issues.append(issue)
        binding = binding_by_id.get(write.binding_id, {"binding_id": write.binding_id, "normalized_field": write.normalized_path})
        plan.mapping_gaps.append(
            _gap(
                binding,
                reason=message,
                severity="P1",
                normalized_path=write.normalized_path,
                row_key=write.row_key,
                source_ref=write.source_ref,
                expected_target=f"{write.target_sheet}!{write.target_cell}",
            )
        )


def _target_accepts_value(target_type: str, value: Any) -> bool:
    if target_type in {"text", "string", "source_ref", "status", "horizon"}:
        return isinstance(value, str)
    if target_type == "period":
        return isinstance(value, str) and bool(re.fullmatch(r"\d{4}-(?:Q[1-4]|FY)|FY\d{4}", value))
    if target_type == "date":
        return isinstance(value, str) and bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", value))
    if target_type in {"number", "currency", "percentage", "ratio"}:
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if target_type == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if target_type == "boolean":
        return isinstance(value, bool)
    if target_type == "value":
        return isinstance(value, (str, int, float, bool)) and value not in (None, "")
    return False


def _row_has_required_columns(row: Mapping[str, Any], binding: Mapping[str, Any]) -> bool:
    return all(_read_row_field(row, str(column))[2] for column in binding.get("required_columns") or [])


def _missing_required_columns(row: Mapping[str, Any], binding: Mapping[str, Any]) -> list[str]:
    return [str(column) for column in binding.get("required_columns") or [] if not _read_row_field(row, str(column))[2]]


def _selector_exclusion_reason(row: Mapping[str, Any], selector: Mapping[str, Any]) -> str:
    for raw_filter in selector.get("filters") or []:
        if not isinstance(raw_filter, Mapping):
            continue
        field = str(raw_filter.get("field") or "")
        value, _source_ref, populated = _read_row_field(row, field)
        if raw_filter.get("present") is True and not populated:
            return f"{field}:not_populated"
        if "equals" in raw_filter and value != raw_filter["equals"]:
            return f"{field}:not_equal"
        if "in" in raw_filter and value not in set(raw_filter.get("in") or []):
            return f"{field}:not_in_allowed_set"
        if "not_in" in raw_filter and value in set(raw_filter.get("not_in") or []):
            return f"{field}:explicitly_excluded"
    return ""


def _selector_matches(row: Mapping[str, Any], selector: Mapping[str, Any]) -> bool:
    return not _selector_exclusion_reason(row, selector)


def _sort_rows(rows: Sequence[Mapping[str, Any]], sort_order: Sequence[Any]) -> list[Mapping[str, Any]]:
    ordered = list(rows)
    for raw in reversed(list(sort_order)):
        if isinstance(raw, str):
            field, _, direction = raw.partition(":")
            descending = direction.lower() == "desc"
        elif isinstance(raw, Mapping):
            field = str(raw.get("field") or "")
            descending = str(raw.get("direction") or "asc").lower() == "desc"
        else:
            continue
        def sort_key(row: Mapping[str, Any]) -> tuple[int, float | str]:
            value = _read_row_field(row, field)[0]
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                return (0, float(value))
            return (1, str(value or ""))

        ordered.sort(key=sort_key, reverse=descending)
    return ordered


def _sort_fields(sort_order: Sequence[Any]) -> list[str]:
    fields: list[str] = []
    for raw in sort_order:
        if isinstance(raw, str):
            field = raw.partition(":")[0]
        elif isinstance(raw, Mapping):
            field = str(raw.get("field") or "")
        else:
            field = ""
        if field:
            fields.append(field)
    return fields


def _row_key(row: Mapping[str, Any], binding: Mapping[str, Any]) -> str:
    canonical_pair: tuple[str, str] | None = None
    dimension_field = str(binding.get("dimension_field") or "dimension")
    member_field = str(binding.get("member_field") or "member")
    if _is_segment_binding(binding):
        raw_dimension, _source_ref, dimension_populated = _read_row_field(row, dimension_field)
        raw_member, _member_source_ref, member_populated = _read_row_field(row, member_field)
        if dimension_populated and member_populated:
            canonical_pair = canonical_segment_dimension_member(raw_dimension, raw_member)
    parts: list[str] = []
    for field in binding.get("row_key") or []:
        value, _source_ref, populated = _read_row_field(row, str(field))
        if not populated:
            return ""
        if canonical_pair is not None and str(field) == dimension_field:
            value = canonical_pair[0]
        elif canonical_pair is not None and str(field) == member_field:
            value = canonical_pair[1]
        parts.append(str(value))
    return "|".join(parts)


def _is_segment_binding(binding: Mapping[str, Any]) -> bool:
    selector = binding.get("row_selector")
    return isinstance(selector, Mapping) and str(selector.get("source_path") or "") == "segments.items"


def _segment_duplicate_row_message(
    first_row: Mapping[str, Any],
    duplicate_row: Mapping[str, Any],
    binding: Mapping[str, Any],
    business_key: str,
) -> str:
    dimension_field = str(binding.get("dimension_field") or "dimension")
    member_field = str(binding.get("member_field") or "member")

    def context(row: Mapping[str, Any]) -> tuple[tuple[str, str], tuple[str, str], str]:
        raw_pair = (
            str(_read_row_field(row, dimension_field)[0] or ""),
            str(_read_row_field(row, member_field)[0] or ""),
        )
        canonical_pair = canonical_segment_dimension_member(*raw_pair)
        source_row_ref = str(_read_row_field(row, "source_row_ref")[0] or "")
        return raw_pair, canonical_pair, source_row_ref

    first_raw, first_canonical, first_source_row_ref = context(first_row)
    duplicate_raw, duplicate_canonical, duplicate_source_row_ref = context(duplicate_row)
    return (
        f"Duplicate canonical segment row_key {business_key!r}; "
        f"first_raw_pair={first_raw!r}, duplicate_raw_pair={duplicate_raw!r}, "
        f"first_canonical_pair={first_canonical!r}, duplicate_canonical_pair={duplicate_canonical!r}, "
        f"first_source_row_ref={first_source_row_ref!r}, "
        f"duplicate_source_row_ref={duplicate_source_row_ref!r}, business_key={business_key!r}."
    )


def _read_field(package: Mapping[str, Any], path: str) -> tuple[Any, str, bool]:
    value = _path_get(package, path)
    return _unwrap_field(value)


def _row_normalized_path(
    binding: Mapping[str, Any],
    row: Mapping[str, Any],
    planned_index: int,
    source_field: str,
) -> str:
    selector = binding.get("row_selector")
    source_path = str(selector.get("source_path") or "$") if isinstance(selector, Mapping) else "$"
    source_index = row.get("__planner_source_index")
    if not isinstance(source_index, int):
        source_index = planned_index
    base = f"{source_path}.{source_index}"
    return f"{base}.{source_field}" if source_field else base


def _read_row_field(row: Mapping[str, Any], path: str) -> tuple[Any, str, bool]:
    value = _path_get(row, path)
    return _unwrap_field(value)


def _first_populated_row_scalar(row: Mapping[str, Any], paths: Sequence[str]) -> str:
    """Return the first normalized scalar without serializing its field wrapper."""

    for path in paths:
        value, _source_ref, populated = _read_row_field(row, path)
        if populated:
            return str(value)
    return ""


def _read_bound_row_field(
    row: Mapping[str, Any],
    binding: Mapping[str, Any],
    source_field: str,
) -> tuple[Any, str, bool]:
    """Resolve a row value and its lineage without treating raw text as orphaned.

    Periods and source excerpts are deliberately plain values in the normalized
    contract.  A binding can therefore nominate a field-backed source reference
    rather than forcing period labels to masquerade as normalized data fields.
    """

    value, source_ref, populated = _read_row_field(row, source_field)
    if populated and source_field in {"source", "source_ref"} and not source_ref:
        source_ref = str(value or "")
    fallback = str(binding.get("source_ref_field") or "")
    if populated and not source_ref and fallback:
        _fallback_value, fallback_source_ref, fallback_populated = _read_row_field(row, fallback)
        if fallback_source_ref:
            source_ref = fallback_source_ref
        elif fallback_populated and fallback in {"source", "source_ref"}:
            source_ref = str(_fallback_value or "")
    return value, source_ref, populated


def _row_source_ref(row: Mapping[str, Any], binding: Mapping[str, Any]) -> str:
    fallback = str(binding.get("source_ref_field") or "")
    if fallback:
        _value, source_ref, populated = _read_bound_row_field(row, binding, fallback)
        if source_ref:
            return source_ref
        if populated and fallback in {"source", "source_ref"}:
            return str(_value or "")
    for field in binding.get("required_columns") or []:
        _value, source_ref, _populated = _read_bound_row_field(row, binding, str(field))
        if source_ref:
            return source_ref
    return ""


def _unwrap_field(value: Any) -> tuple[Any, str, bool]:
    if isinstance(value, Mapping) and "status" in value:
        populated = str(value.get("status") or "") == "populated" and value.get("value") not in (None, "")
        return value.get("value"), str(value.get("source_ref") or ""), populated
    return value, "", value not in (None, "")


def _planned_write(
    binding: Mapping[str, Any],
    *,
    ticker: str,
    target_cell: str,
    normalized_path: str,
    row_key: str,
    value: Any,
    source_ref: str,
    capacity_used: int,
    target_type: str,
    target_role: str,
) -> PlannedWrite:
    return PlannedWrite(
        binding_id=str(binding["binding_id"]),
        normalized_path=normalized_path,
        row_key=row_key,
        target_sheet=_resolve_sheet(str(binding["sheet"]), ticker),
        target_cell=target_cell,
        target_type=target_type,
        target_role=target_role,
        value=value,
        value_type=_value_type(value),
        source_ref=source_ref,
        capacity_used=capacity_used,
    )


def _gap(
    binding: Mapping[str, Any],
    *,
    reason: str,
    severity: str = "P2",
    normalized_path: str = "",
    row_key: str = "",
    source_ref: str = "",
    expected_target: str = "",
    canonical_issue_key: str = "",
    root_cause: str = "",
) -> dict[str, Any]:
    normalized_field = str(binding.get("normalized_field") or "")
    return {
        "severity": severity,
        "rule_id": "binding_plan_mapping_gap",
        "binding_id": str(binding.get("binding_id") or ""),
        "sheet": str(binding.get("sheet") or ""),
        "section": str(binding.get("section") or ""),
        "target": _planner_target(binding),
        "normalized_field": normalized_field,
        "normalized_path": normalized_path or normalized_field,
        "row_key": row_key,
        "source_ref": source_ref,
        "expected_target": expected_target or _planner_target(binding),
        "field": normalized_field,
        "reason": reason,
        "message": reason,
        "suggested_action": str(binding.get("missing_source_behavior") or "Resolve the normalized source or planner contract."),
        "canonical_issue_key": canonical_issue_key,
        "root_cause": root_cause,
    }


def _structured_skip(
    binding: Mapping[str, Any],
    *,
    normalized_path: str,
    row_key: str,
    reason: str,
    severity: str,
    source_ref: str = "",
    expected_target: str = "",
    selector_rule: str = "",
    selected_row_key: str = "",
    excluded_row_key: str = "",
    period: str = "",
    exclusion_disposition: str = "",
) -> dict[str, Any]:
    return {
        "binding_id": str(binding.get("binding_id") or ""),
        "row_key": row_key,
        "normalized_path": normalized_path,
        "source_ref": source_ref,
        "expected_target": expected_target or _planner_target(binding),
        "reason": reason,
        "severity": severity,
        "selector_rule": selector_rule,
        "selected_row_key": selected_row_key,
        "excluded_row_key": excluded_row_key,
        "period": period,
        "exclusion_disposition": exclusion_disposition,
    }


def _missing_binding_severity(binding: Mapping[str, Any]) -> str:
    promotion = str(binding.get("promotion_requirement") or "")
    return "P1" if bool(binding.get("required")) or promotion in {"required", "blocked_if_missing"} else "P2"


def _overflow_severity(binding: Mapping[str, Any]) -> str:
    return "P1" if str(binding.get("overflow_behavior") or "") == "fail" or bool(binding.get("required")) else "P2"


def _binding_target_role(binding: Mapping[str, Any]) -> str:
    return str(binding.get("target_role") or binding.get("binding_id") or "")


def _column_target_role(binding: Mapping[str, Any], column: Mapping[str, Any]) -> str:
    source_field = str(column.get("source_field") or column.get("column_id") or "")
    return str(column.get("target_role") or f"{binding.get('binding_id')}.{source_field}")


def _column_target_type(binding: Mapping[str, Any], column: Mapping[str, Any]) -> str:
    explicit = str(column.get("target_type") or "")
    if explicit:
        return explicit
    if str(binding.get("source_policy") or "") != "validation-output":
        return ""
    source_field = str(column.get("source_field") or "")
    return "text" if source_field in {"severity", "rule_id", "field", "message", "source_ref", "suggested_action"} else ""


def _planner_issue(
    severity: str,
    rule_id: str,
    field: str,
    message: str,
    *,
    normalized_path: str = "",
    business_row_key: str = "",
    binding_id: str = "",
    source_ref: str = "",
    root_cause: str = "",
    issue_type: str = "",
    canonical_issue_key: str = "",
    affected_period: str = "",
) -> NormalizedDataIssue:
    return NormalizedDataIssue(
        severity=severity,
        rule_id=rule_id,
        field=field,
        message=message,
        source_ref=source_ref,
        suggested_action="Correct the normalized data or binding planner contract before any workbook render.",
        normalized_path=normalized_path,
        business_row_key=business_row_key,
        binding_id=binding_id,
        root_cause=root_cause,
        issue_type=issue_type,
        canonical_issue_key=canonical_issue_key,
        affected_period=affected_period,
    )


def _planner_event_key(
    binding: Mapping[str, Any],
    *,
    normalized_path: str,
    row_key: str,
    event_type: str,
) -> str:
    """Correlate multiple records emitted for one planner event."""

    return "|".join(
        (
            "planner_event",
            str(binding.get("binding_id") or ""),
            event_type,
            normalized_path,
            row_key,
        )
    )


def _requires_source_ref(binding: Mapping[str, Any]) -> bool:
    return bool(binding.get("source_ref_required")) or str(binding.get("source_policy") or "") == "source-backed"


def _planning_mode(binding: Mapping[str, Any]) -> str:
    explicit = str(binding.get("planning_mode") or "")
    if explicit:
        return explicit
    shape = str(binding.get("value_shape") or "")
    if shape in {"quarterly_series", "annual_series"}:
        return "series"
    if shape in _TABLE_MODES:
        return shape
    return shape


def _planner_target(binding: Mapping[str, Any]) -> str:
    return str(binding.get("planner_target") or binding.get("target") or "")


def _bindings_from_payload(payload: Mapping[str, Any] | Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    if isinstance(payload, Mapping):
        raw = payload.get("bindings") or []
    else:
        raw = payload
    if not isinstance(raw, Sequence):
        raise BindingPlanningError("Binding payload must contain a bindings list.")
    return [item for item in raw if isinstance(item, Mapping)]


def _load_json(path: Path) -> dict[str, Any]:
    payload = load_json_strict(path)
    if not isinstance(payload, dict):
        raise BindingPlanningError(f"JSON contract must be an object: {path}")
    return payload


def _ticker(package: Mapping[str, Any], override: str | None) -> str:
    raw: Any = override
    if raw is None:
        raw = _path_get(package, "ticker_metadata.ticker.value")
    ticker = str(raw or "").strip().upper()
    if not ticker:
        raise BindingPlanningError("Ticker is required in ticker_metadata.ticker.value or ticker_override.")
    return ticker


def _path_get(obj: Any, dotted_path: str) -> Any:
    current = obj
    for part in dotted_path.split("."):
        if isinstance(current, Mapping):
            if part not in current:
                return None
            current = current[part]
        elif isinstance(current, list):
            try:
                current = current[int(part)]
            except (ValueError, IndexError):
                return None
        else:
            return None
    return current


def _parse_range(range_ref: str) -> tuple[int, int, int, int]:
    match = _RANGE_RE.fullmatch(range_ref.strip().upper())
    if match is None:
        raise BindingPlanningError(f"Invalid A1 range {range_ref!r}.")
    left = _column_index(match.group(1))
    top = int(match.group(2))
    right = _column_index(match.group(3) or match.group(1))
    bottom = int(match.group(4) or match.group(2))
    if right < left or bottom < top:
        raise BindingPlanningError(f"Invalid reversed A1 range {range_ref!r}.")
    return left, top, right, bottom


def _column_index(column: str) -> int:
    if not re.fullmatch(r"[A-Z]+", column):
        raise BindingPlanningError(f"Invalid column {column!r}.")
    out = 0
    for char in column:
        out = out * 26 + (ord(char) - ord("A") + 1)
    return out


def _column_label(index: int) -> str:
    out = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        out = chr(ord("A") + remainder) + out
    return out


def _first_cell(range_ref: str) -> str:
    left, top, _right, _bottom = _parse_range(range_ref)
    return f"{_column_label(left)}{top}"


def _range_rows(range_ref: str) -> tuple[int, int]:
    _left, top, _right, bottom = _parse_range(range_ref)
    return top, bottom


def _table_target_rows(binding: Mapping[str, Any], start_row: int, end_row: int) -> list[int]:
    declared = binding.get("target_rows")
    if isinstance(declared, list):
        return [int(row) for row in declared]
    return list(range(start_row, end_row + 1))


def _series_coordinates(range_ref: str, axis: str) -> list[str]:
    left, top, right, bottom = _parse_range(range_ref)
    if axis == "columns":
        if top != bottom:
            raise BindingPlanningError("Column-oriented series target must occupy one row.")
        return [f"{_column_label(column)}{top}" for column in range(left, right + 1)]
    if axis == "rows":
        if left != right:
            raise BindingPlanningError("Row-oriented series target must occupy one column.")
        return [f"{_column_label(left)}{row}" for row in range(top, bottom + 1)]
    raise BindingPlanningError("target_axis must be columns or rows.")


def _contains(outer: tuple[int, int, int, int], inner: tuple[int, int, int, int]) -> bool:
    left, top, right, bottom = outer
    inner_left, inner_top, inner_right, inner_bottom = inner
    return left <= inner_left and inner_right <= right and top <= inner_top and inner_bottom <= bottom


def _overlaps(first: tuple[int, int, int, int], second: tuple[int, int, int, int]) -> bool:
    left, top, right, bottom = first
    other_left, other_top, other_right, other_bottom = second
    return not (right < other_left or other_right < left or bottom < other_top or other_bottom < top)


def _resolve_sheet(sheet: str, ticker: str) -> str:
    return sheet.replace("{ticker}", ticker)


def _value_type(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if value is None:
        return "null"
    return type(value).__name__
