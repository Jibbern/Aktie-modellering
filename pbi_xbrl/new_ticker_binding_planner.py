"""Pre-render binding planner for the generic new-ticker engine.

This module is intentionally JSON-only.  It never imports openpyxl, loads a
workbook, or writes cells.  It converts a normalized package and declared shell
contracts into an auditable plan that a future value-only filler may execute.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from pbi_xbrl.normalized_company_data_validation import (
    NormalizedDataIssue,
    validate_normalized_company_data,
    validate_normalized_company_data_schema,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BINDING_MAP = ROOT / "docs" / "workbook_binding_map.json"
DEFAULT_MANIFEST = ROOT / "docs" / "standard_template_shell_manifest.json"
_RANGE_RE = re.compile(r"^([A-Z]+)([1-9]\d*)(?::([A-Z]+)([1-9]\d*))?$")
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


class BindingPlanningError(RuntimeError):
    """Raised only for an invalid planner invocation, never for workbook IO."""


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

    @property
    def issues(self) -> list[NormalizedDataIssue]:
        return [*self.schema_issues, *self.semantic_issues, *self.planner_issues]

    @property
    def status(self) -> str:
        return "FAIL" if any(issue.severity.upper() in _BLOCKING_SEVERITIES for issue in self.issues) else "PASS"

    def to_dict(self) -> dict[str, Any]:
        structured_skip_count = sum(len(report.get("skipped_rows") or []) for report in self.binding_reports)
        overflow_count = sum(len(report.get("overflow_rows") or []) for report in self.binding_reports)
        return {
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
        }


def plan_standard_template_writes(
    package: Mapping[str, Any],
    *,
    binding_payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    manifest: Mapping[str, Any],
    ticker_override: str | None = None,
    promotion_requested: bool = False,
) -> BindingPlan:
    """Plan exact cell writes without opening a workbook.

    A failed plan is an expected result for incomplete source coverage or an
    incompatible shell contract.  It is never repaired by sequential dumping or
    merged-cell concatenation.
    """

    bindings = _bindings_from_payload(binding_payload)
    ticker = _ticker(package, ticker_override)
    plan = BindingPlan(ticker=ticker)
    plan.schema_issues.extend(validate_normalized_company_data_schema(package))
    plan.semantic_issues.extend(
        validate_normalized_company_data(
            package,
            binding_map=bindings,
            promotion_requested=promotion_requested,
            validate_schema=False,
        )
    )
    plan.planner_issues.extend(_validate_manifest_and_binding_contracts(manifest, bindings))

    if any(issue.severity.upper() in _BLOCKING_SEVERITIES for issue in plan.issues):
        _add_blocking_reports(plan, bindings)
        _append_manual_review_flags(plan)
        return plan

    plan.mapping_gaps.extend(_normalize_mapping_gaps(_path_get(package, "mapping_gaps")))
    plan.manual_review_flags.extend(_normalize_manual_review_flags(_path_get(package, "manual_review_flags")))
    for binding in bindings:
        if not bool(binding.get("writable")):
            continue
        if str(binding.get("source_policy") or "") == "validation-output":
            continue
        report, writes, gaps, issues = _plan_binding(package, binding, ticker=ticker)
        plan.binding_reports.append(report)
        plan.planned_writes.extend(writes)
        plan.mapping_gaps.extend(gaps)
        plan.planner_issues.extend(issues)

    _dedupe_plan(plan)
    _append_manual_review_flags(plan)
    _plan_validation_outputs(plan, bindings, ticker=ticker)
    _dedupe_plan(plan)
    _validate_planned_write_types(plan, bindings)
    return plan


def plan_standard_template_writes_from_paths(
    package_path: Path | str,
    *,
    binding_map_path: Path | str = DEFAULT_BINDING_MAP,
    manifest_path: Path | str = DEFAULT_MANIFEST,
    ticker_override: str | None = None,
    promotion_requested: bool = False,
) -> BindingPlan:
    """Load JSON contracts and build a plan; no workbook file is touched."""

    package = _load_json(Path(package_path))
    binding_payload = _load_json(Path(binding_map_path))
    manifest = _load_json(Path(manifest_path))
    return plan_standard_template_writes(
        package,
        binding_payload=binding_payload,
        manifest=manifest,
        ticker_override=ticker_override,
        promotion_requested=promotion_requested,
    )


def write_binding_plan_report(plan: BindingPlan, output_path: Path | str) -> Path:
    """Persist only a JSON report; this helper never creates an Excel file."""

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(plan.to_dict(), indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
    return path


def _plan_binding(
    package: Mapping[str, Any],
    binding: Mapping[str, Any],
    *,
    ticker: str,
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
        return _plan_pivot_binding(package, binding, ticker=ticker, report=report)
    if mode == "series":
        return _plan_series_binding(package, binding, ticker=ticker, report=report)
    if mode in {"scalar", "text_block"}:
        return _plan_scalar_binding(package, binding, ticker=ticker, report=report)
    issue = _planner_issue("P1", "unsupported_binding_planning_mode", binding_id, f"Unsupported planning mode {mode!r}.")
    return report, [], [_gap(binding, reason=issue.message)], [issue]


def _plan_scalar_binding(
    package: Mapping[str, Any],
    binding: Mapping[str, Any],
    *,
    ticker: str,
    report: dict[str, Any],
) -> tuple[dict[str, Any], list[PlannedWrite], list[dict[str, Any]], list[NormalizedDataIssue]]:
    binding_id = str(binding["binding_id"])
    source_path = str(binding.get("source_path") or binding.get("normalized_field") or "")
    target = _first_cell(_planner_target(binding))
    value, source_ref, populated = _read_field(package, source_path)
    if not populated:
        severity = _missing_binding_severity(binding)
        reason = "Scalar/text value is not populated."
        report["skipped_rows"].append(
            _structured_skip(binding, normalized_path=source_path, row_key="scalar", reason=reason, severity=severity, expected_target=target)
        )
        issue = _planner_issue(severity, "required_binding_missing" if severity == "P1" else "binding_value_missing", f"{binding_id}:{source_path}", reason)
        return report, [], [_gap(binding, reason=reason, severity=severity, normalized_path=source_path, row_key="scalar", expected_target=target)], [issue]
    if _requires_source_ref(binding) and not source_ref:
        issue = _planner_issue("P1", "missing_source_ref", source_path, "Source-backed scalar/text value has no source_ref.")
        report["skipped_rows"].append(
            _structured_skip(binding, normalized_path=source_path, row_key="scalar", reason=issue.message, severity="P1", expected_target=target)
        )
        return report, [], [_gap(binding, reason=issue.message, severity="P1", normalized_path=source_path, row_key="scalar", expected_target=target)], [issue]
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
    planned_index = 0
    for source_position, row in enumerate(rows):
        row_key = _row_key(row, binding)
        normalized_path = _row_normalized_path(binding, row, source_position, source_field)
        row_source_ref = _row_source_ref(row, binding)
        if planned_index >= capacity:
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
            severity = "P1" if source_field in set(binding.get("required_columns") or []) else _missing_binding_severity(binding)
            reason = f"{source_field} not populated"
            report["skipped_rows"].append(
                _structured_skip(
                    binding,
                    normalized_path=normalized_path,
                    row_key=row_key,
                    reason=reason,
                    severity=severity,
                    source_ref=row_source_ref,
                    expected_target=coordinates[planned_index],
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
                    expected_target=coordinates[planned_index],
                )
            )
            issues.append(_planner_issue(severity, "required_row_value_missing", f"{binding['binding_id']}:{row_key}:{source_field}", reason))
            planned_index += 1
            continue
        if _requires_source_ref(binding) and not source_ref:
            reason = "Source-backed series value has no source_ref."
            report["skipped_rows"].append(
                _structured_skip(
                    binding,
                    normalized_path=normalized_path,
                    row_key=row_key,
                    reason=reason,
                    severity="P1",
                    expected_target=coordinates[planned_index],
                )
            )
            gaps.append(_gap(binding, reason=reason, severity="P1", normalized_path=normalized_path, row_key=row_key, expected_target=coordinates[planned_index]))
            issues.append(_planner_issue("P1", "missing_source_ref", f"{binding['binding_id']}:{row_key}", reason))
            planned_index += 1
            continue
        writes.append(
            _planned_write(
                binding,
                ticker=ticker,
                target_cell=coordinates[planned_index],
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
    period_columns = [str(column).upper() for column in binding.get("period_target_columns") or []]
    row_blocks = binding.get("row_blocks") if isinstance(binding.get("row_blocks"), Mapping) else {}
    alignment = str(binding.get("period_alignment") or "left")
    if not value_field or not period_columns or not row_blocks:
        issue = _planner_issue("P1", "binding_pivot_contract_invalid", str(binding.get("binding_id") or ""), "Pivot binding requires value_field, period_target_columns, and row_blocks.")
        return report, [], [_gap(binding, reason=issue.message, severity="P1")], [issue]

    periods = sorted({str(_read_row_field(row, period_field)[0]) for row in rows if _read_row_field(row, period_field)[2]})
    visible_periods = periods[-len(period_columns) :]
    excluded_periods = set(periods) - set(visible_periods)
    if alignment == "right":
        visible_columns = period_columns[len(period_columns) - len(visible_periods) :]
    else:
        visible_columns = period_columns[: len(visible_periods)]
    period_to_column = {period: visible_columns[index] for index, period in enumerate(visible_periods)}

    writes: list[PlannedWrite] = []
    gaps: list[dict[str, Any]] = []
    issues: list[NormalizedDataIssue] = []
    member_rows: dict[tuple[str, str], int] = {}
    occupied_rows: dict[int, tuple[str, str]] = {}
    written_cells: set[str] = set()
    block_members: dict[str, list[str]] = {}
    for row in rows:
        dimension = str(_read_row_field(row, dimension_field)[0] or "")
        member = str(_read_row_field(row, member_field)[0] or "")
        if dimension and member and member not in block_members.setdefault(dimension, []):
            block_members[dimension].append(member)

    for dimension, members in block_members.items():
        declared_rows = [int(row) for row in row_blocks.get(dimension) or []]
        for member_index, member in enumerate(members):
            if member_index >= len(declared_rows):
                continue
            target_row = declared_rows[member_index]
            owner = occupied_rows.get(target_row)
            if owner and owner != (dimension, member):
                issues.append(_planner_issue("P1", "binding_pivot_row_collision", str(binding.get("binding_id") or ""), f"Pivot row {target_row} is claimed by both {owner} and {(dimension, member)}."))
                continue
            occupied_rows[target_row] = (dimension, member)
            member_rows[(dimension, member)] = target_row

    for source_position, row in enumerate(rows):
        row_key = _row_key(row, binding)
        source_ref = _row_source_ref(row, binding)
        normalized_path = _row_normalized_path(binding, row, source_position, value_field)
        dimension = str(_read_row_field(row, dimension_field)[0] or "")
        member = str(_read_row_field(row, member_field)[0] or "")
        period = str(_read_row_field(row, period_field)[0] or "")
        reason = ""
        if period in excluded_periods:
            reason = "pivot_period_outside_visible_window"
        elif (dimension, member) not in member_rows:
            reason = "pivot_member_has_no_declared_block_capacity"
        if reason:
            severity = _overflow_severity(binding)
            record = _structured_skip(binding, normalized_path=normalized_path, row_key=row_key, reason=reason, severity=severity, source_ref=source_ref)
            report["overflow_rows"].append(record)
            gaps.append(_gap(binding, reason=reason, severity=severity, normalized_path=normalized_path, row_key=row_key, source_ref=source_ref))
            continue
        value, value_source_ref, populated = _read_bound_row_field(row, binding, value_field)
        if not populated:
            reason = f"{value_field} not populated"
            report["skipped_rows"].append(_structured_skip(binding, normalized_path=normalized_path, row_key=row_key, reason=reason, severity="P1", source_ref=source_ref))
            gaps.append(_gap(binding, reason=reason, severity="P1", normalized_path=normalized_path, row_key=row_key, source_ref=source_ref))
            issues.append(_planner_issue("P1", "required_row_value_missing", f"{binding['binding_id']}:{row_key}:{value_field}", reason))
            continue
        target_row = member_rows[(dimension, member)]
        label_cell = f"{label_column}{target_row}"
        if label_cell not in written_cells:
            writes.append(_planned_write(binding, ticker=ticker, target_cell=label_cell, normalized_path=_row_normalized_path(binding, row, source_position, member_field), row_key=f"{dimension}|{member}", value=member, source_ref=source_ref, capacity_used=len(member_rows), target_type="text", target_role=f"{binding['binding_id']}.member"))
            written_cells.add(label_cell)
        target_cell = f"{period_to_column[period]}{target_row}"
        if target_cell in written_cells:
            issues.append(_planner_issue("P1", "binding_pivot_cell_collision", f"{binding['binding_id']}:{row_key}", f"More than one selected business row resolves to {target_cell}."))
            continue
        writes.append(_planned_write(binding, ticker=ticker, target_cell=target_cell, normalized_path=normalized_path, row_key=row_key, value=value, source_ref=value_source_ref or source_ref, capacity_used=len(member_rows), target_type=str(binding.get("value_target_type") or "number"), target_role=f"{binding['binding_id']}.value"))
        written_cells.add(target_cell)

    if report["overflow_rows"]:
        severity = _overflow_severity(binding)
        issues.append(_planner_issue(severity, "binding_overflow", str(binding["binding_id"]), f"{len(report['overflow_rows'])} pivot row(s) could not enter the declared visible matrix."))
    report["capacity_used"] = len(member_rows)
    report["planned_write_count"] = len(writes)
    return report, writes, gaps, issues


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
            report["skipped_rows"].append(
                _structured_skip(
                    binding,
                    normalized_path=base_path.rstrip("."),
                    row_key=row_key,
                    reason=reason,
                    severity="P1",
                    source_ref=row_source_ref,
                    expected_target=expected_target,
                )
            )
            gaps.append(
                _gap(
                    binding,
                    reason=reason,
                    severity="P1",
                    normalized_path=base_path.rstrip("."),
                    row_key=row_key,
                    source_ref=row_source_ref,
                    expected_target=expected_target,
                )
            )
            issues.append(_planner_issue("P1", "required_row_schema_column_missing", f"{binding['binding_id']}:{row_key}", reason))
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
                gaps.append(_gap(binding, reason=reason, severity="P1", normalized_path=normalized_path, row_key=row_key, expected_target=expected_target))
                issues.append(_planner_issue("P1", "missing_source_ref", f"{binding['binding_id']}:{row_key}:{source_field}", reason))
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
    for source_index, raw in enumerate(raw_rows):
        if not isinstance(raw, Mapping):
            normalized_path = f"{source_path}.{source_index}"
            skipped.append(
                _structured_skip(binding, normalized_path=normalized_path, row_key=f"source_index:{source_index}", reason="row_not_object", severity="P1")
            )
            issues.append(_planner_issue("P1", "binding_row_not_object", f"{binding.get('binding_id')}:{source_index}", "A selected collection entry is not an object."))
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
    rows = _sort_rows(rows, binding.get("sort_order") or [])
    pick = str(selector.get("pick") or "all")
    if pick == "latest" and rows:
        rows = [rows[-1]]
    elif pick == "first" and rows:
        rows = [rows[0]]
    keys: set[str] = set()
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
        if key in keys:
            skipped.append(
                _structured_skip(binding, normalized_path=normalized_path, row_key=key, reason="duplicate_row_key", severity="P1", source_ref=_row_source_ref(row, binding))
            )
            issues.append(_planner_issue("P1", "binding_row_key_duplicate", str(binding.get("binding_id") or ""), f"Duplicate row_key {key!r}."))
            continue
        keys.add(key)
        unique_rows.append(row)
    return unique_rows, skipped, issues


def _validate_manifest_and_binding_contracts(
    manifest: Mapping[str, Any],
    bindings: Sequence[Mapping[str, Any]],
) -> list[NormalizedDataIssue]:
    issues: list[NormalizedDataIssue] = []
    sheets = {str(sheet.get("sheet") or ""): sheet for sheet in manifest.get("sheets", []) if isinstance(sheet, Mapping)}
    cell_contracts, merge_families, manifest_contract_issues = _prepare_manifest_planner_contracts(manifest)
    issues.extend(manifest_contract_issues)
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
        normalized_field = str(binding.get("normalized_field") or "")
        if str(binding.get("sheet") or "") == "Valuation" and "output" in str(binding.get("section") or "").lower() and normalized_field.startswith("mapping_gaps"):
            issues.append(_planner_issue("P1", "valuation_output_mapping_gap_forbidden", binding_id, "Valuation output rows must consume explicit valuation_outputs or be formula-owned; they must never consume mapping_gaps."))
        if normalized_field.startswith(("mapping_gaps", "manual_review_flags")) and str(binding.get("sheet") or "") not in _QA_SHEETS:
            issues.append(_planner_issue("P1", "qa_output_outside_qa_sheet", binding_id, "Mapping gaps and manual-review flags may only bind to QA_Log, Needs_Review, or QA_Checks."))
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
        period_columns = [str(column).upper() for column in binding.get("period_target_columns") or []]
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


def _append_manual_review_flags(plan: BindingPlan) -> None:
    seen: set[tuple[str, str, str, str, str, str]] = {
        (
            str(flag.get("binding_id") or ""),
            str(flag.get("rule_id") or ""),
            str(flag.get("field") or ""),
            str(flag.get("message") or ""),
            str(flag.get("normalized_path") or ""),
            str(flag.get("row_key") or ""),
        )
        for flag in plan.manual_review_flags
    }
    for gap in plan.mapping_gaps:
        key = (
            str(gap.get("binding_id") or ""),
            "binding_plan_manual_review",
            str(gap.get("normalized_field") or ""),
            str(gap.get("reason") or ""),
            str(gap.get("normalized_path") or ""),
            str(gap.get("row_key") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        plan.manual_review_flags.append(
            {
                "severity": str(gap.get("severity") or "P2"),
                "rule_id": "binding_plan_manual_review",
                "field": str(gap.get("normalized_field") or ""),
                "message": str(gap.get("reason") or "Binding plan requires review."),
                "source_ref": str(gap.get("source_ref") or ""),
                "suggested_action": str(gap.get("suggested_action") or "Resolve the source, row contract, or shell slot before rendering."),
                "binding_id": str(gap.get("binding_id") or ""),
                "row_key": str(gap.get("row_key") or f"gap:{gap.get('binding_id') or 'unknown'}:{gap.get('normalized_path') or gap.get('normalized_field') or 'field'}"),
                "normalized_path": str(gap.get("normalized_path") or gap.get("normalized_field") or ""),
                "expected_target": str(gap.get("expected_target") or gap.get("target") or ""),
            }
        )


def _plan_validation_outputs(plan: BindingPlan, bindings: Sequence[Mapping[str, Any]], *, ticker: str) -> None:
    """Append QA writes only after all business bindings have been planned.

    The planner owns derived mapping gaps.  Writing those rows while the normal
    bindings are still being evaluated would either lose late gaps or force the
    Excel layer to invent audit data.  QA rows stay in their declared QA sheets
    and use the same typed row contract as business tables.
    """

    rows_by_binding = {
        "qa_log_validation_rows": plan.manual_review_flags,
        "needs_review_validation_rows": plan.manual_review_flags,
        "qa_checks_mapping_gap_rows": plan.mapping_gaps,
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
        normalized.append(
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
    return normalized


def _normalize_manual_review_flags(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    normalized: list[dict[str, Any]] = []
    for idx, raw in enumerate(value):
        if not isinstance(raw, Mapping):
            continue
        normalized.append(
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
    seen_gaps: set[tuple[str, str, str, str, str]] = set()
    deduped_gaps: list[dict[str, Any]] = []
    for gap in plan.mapping_gaps:
        key = (
            str(gap.get("binding_id") or ""),
            str(gap.get("normalized_path") or gap.get("normalized_field") or ""),
            str(gap.get("row_key") or ""),
            str(gap.get("source_ref") or ""),
            str(gap.get("reason") or ""),
        )
        if key not in seen_gaps:
            seen_gaps.add(key)
            deduped_gaps.append(gap)
    plan.mapping_gaps = deduped_gaps


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
        ordered.sort(key=lambda row: str(_read_row_field(row, field)[0] or ""), reverse=descending)
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
    parts: list[str] = []
    for field in binding.get("row_key") or []:
        value, _source_ref, populated = _read_row_field(row, str(field))
        if not populated:
            return ""
        parts.append(str(value))
    return "|".join(parts)


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
) -> dict[str, Any]:
    return {
        "binding_id": str(binding.get("binding_id") or ""),
        "row_key": row_key,
        "normalized_path": normalized_path,
        "source_ref": source_ref,
        "expected_target": expected_target or _planner_target(binding),
        "reason": reason,
        "severity": severity,
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


def _planner_issue(severity: str, rule_id: str, field: str, message: str) -> NormalizedDataIssue:
    return NormalizedDataIssue(
        severity=severity,
        rule_id=rule_id,
        field=field,
        message=message,
        suggested_action="Correct the normalized data or binding planner contract before any workbook render.",
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
    return json.loads(path.read_text(encoding="utf-8"))


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
