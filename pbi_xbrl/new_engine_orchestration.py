"""Supported orchestration for deterministic new-engine shadow workbooks."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import uuid
from typing import Any, Mapping

from pbi_xbrl.excel_formula_serialization import (
    inventory_xlsx_formula_xml,
    validate_xlsx_formula_compatibility,
)
from pbi_xbrl.json_schema_validation import load_json_strict
from pbi_xbrl.new_engine_excel import run_excel_native_roundtrip
from pbi_xbrl.new_engine_transaction import (
    NewEngineTransactionError,
    candidate_path_for,
    normalize_candidate_acl,
    publish_no_overwrite,
)
from pbi_xbrl.new_ticker_binding_planner import BindingPlanReproductionError
from pbi_xbrl.new_ticker_style_planner import (
    DEFAULT_MODULE_MANIFEST,
    DEFAULT_STYLE_POLICY,
    StylePlanningError,
    reproduce_style_plan,
)
from pbi_xbrl.new_ticker_value_filler import (
    DEFAULT_BINDING_MAP,
    DEFAULT_MANIFEST,
    DEFAULT_TEMPLATE,
    NewTickerValueFillerError,
    fill_standard_template_from_package,
)
from pbi_xbrl.standard_template_formula_contract import FORMULA_CONTRACT_VERSION
from pbi_xbrl.standard_template_shell_identity import verify_post_fill_structural_identity
from pbi_xbrl.workbook_modules import canonical_json_sha256
from pbi_xbrl.workbook_validation_runner import validate_workbook


ROOT = Path(__file__).resolve().parents[1]
FORMULA_CONTRACT_PATH = ROOT / "pbi_xbrl" / "standard_template_formula_contract.py"
RECEIPT_VERSION = "new-engine-run/v1"
_VERSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class NewEngineOrchestrationError(RuntimeError):
    """Raised when supported orchestration cannot complete fail closed."""


@dataclass(frozen=True)
class _PlanContext:
    package_path: Path
    ticker: str
    profile_id: str
    template_path: Path
    manifest_path: Path
    binding_map_path: Path
    module_manifest_path: Path
    style_policy_path: Path
    package: Mapping[str, Any]
    manifest: Mapping[str, Any]
    binding_payload: Mapping[str, Any]
    module_payload: Mapping[str, Any]
    style_contract: Mapping[str, Any]
    binding_plan: Any
    style_plan: Any
    binding_payload_json: Mapping[str, Any]
    style_payload_json: Mapping[str, Any]
    binding_digest: str
    style_digest: str
    contract_digest: str
    formula_inventory: Mapping[str, Any]
    formula_inventory_digest: str
    input_files: Mapping[str, Mapping[str, Any]]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _file_identity(path: Path) -> dict[str, Any]:
    resolved = path.resolve()
    if not resolved.is_file():
        raise NewEngineOrchestrationError(f"Required input does not exist: {resolved}")
    return {"path": str(resolved), "size": resolved.stat().st_size, "sha256": _sha256(resolved)}


def _load_object(path: Path, label: str) -> Mapping[str, Any]:
    try:
        payload = load_json_strict(path)
    except Exception as exc:
        raise NewEngineOrchestrationError(f"Could not load {label} JSON {path}: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise NewEngineOrchestrationError(f"{label} must be a JSON object: {path}")
    return payload


def _package_ticker(package: Mapping[str, Any]) -> str:
    metadata = package.get("ticker_metadata")
    ticker_field = metadata.get("ticker") if isinstance(metadata, Mapping) else None
    raw = ticker_field.get("value") if isinstance(ticker_field, Mapping) else ticker_field
    return str(raw or "").strip().upper()


def _git_head() -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        shell=False,
    )
    return completed.stdout.strip() if completed.returncode == 0 else "unknown"


def _compact_formula_inventory(inventory: Mapping[str, Any], digest: str) -> dict[str, Any]:
    return {
        "contract_version": inventory.get("contract_version"),
        "cell_formula_count": int(inventory.get("cell_formula_count") or 0),
        "function_counts": dict(inventory.get("function_counts") or {}),
        "unprefixed_future_functions": dict(inventory.get("unprefixed_future_functions") or {}),
        "unsupported_functions": dict(inventory.get("unsupported_functions") or {}),
        "let_local_occurrences": int(inventory.get("let_local_occurrences") or 0),
        "future_function_cell_count": int(inventory.get("future_function_cell_count") or 0),
        "inventory_digest": digest,
    }


def _build_context(
    *,
    package_path: Path | str,
    ticker: str,
    profile_id: str,
    template_path: Path | str = DEFAULT_TEMPLATE,
    manifest_path: Path | str = DEFAULT_MANIFEST,
    binding_map_path: Path | str = DEFAULT_BINDING_MAP,
    module_manifest_path: Path | str = DEFAULT_MODULE_MANIFEST,
    style_policy_path: Path | str = DEFAULT_STYLE_POLICY,
    expected_contract_digest: str | None = None,
    expected_binding_plan_digest: str | None = None,
    expected_style_plan_digest: str | None = None,
) -> _PlanContext:
    paths = {
        "package": Path(package_path).resolve(),
        "template": Path(template_path).resolve(),
        "manifest": Path(manifest_path).resolve(),
        "binding_map": Path(binding_map_path).resolve(),
        "module_manifest": Path(module_manifest_path).resolve(),
        "style_policy": Path(style_policy_path).resolve(),
        "formula_contract": FORMULA_CONTRACT_PATH.resolve(),
    }
    input_files = {name: _file_identity(path) for name, path in paths.items()}
    package = _load_object(paths["package"], "normalized package")
    manifest = _load_object(paths["manifest"], "shell manifest")
    binding_payload = _load_object(paths["binding_map"], "binding map")
    module_payload = _load_object(paths["module_manifest"], "module manifest")
    style_contract = _load_object(paths["style_policy"], "style policy")

    requested_ticker = str(ticker or "").strip().upper()
    package_ticker = _package_ticker(package)
    if not requested_ticker or requested_ticker != package_ticker:
        raise NewEngineOrchestrationError(
            f"ticker mismatch: requested={requested_ticker!r}, normalized_package={package_ticker!r}."
        )
    requested_profile = str(profile_id or "").strip()
    manifest_profile = str((manifest.get("module_profile") or {}).get("profile_id") or "")
    binding_profile = str(binding_payload.get("module_profile_id") or "")
    known_profiles = {str(row.get("profile_id") or "") for row in module_payload.get("profiles") or []}
    if (
        not requested_profile
        or requested_profile != manifest_profile
        or requested_profile != binding_profile
        or requested_profile not in known_profiles
    ):
        raise NewEngineOrchestrationError(
            "profile mismatch: "
            f"requested={requested_profile!r}, manifest={manifest_profile!r}, binding_map={binding_profile!r}."
        )
    manifest_formula_version = str(manifest.get("formula_contract_version") or "")
    if manifest_formula_version != FORMULA_CONTRACT_VERSION:
        raise NewEngineOrchestrationError(
            "formula contract version mismatch: "
            f"manifest={manifest_formula_version!r}, runtime={FORMULA_CONTRACT_VERSION!r}."
        )

    compatibility_issues = validate_xlsx_formula_compatibility(paths["template"])
    if compatibility_issues:
        raise NewEngineOrchestrationError(f"Frozen shell formula compatibility failed: {compatibility_issues[:20]!r}")
    formula_inventory = inventory_xlsx_formula_xml(paths["template"])
    formula_inventory_digest = canonical_json_sha256(formula_inventory)
    contract_payload = {
        "formula_contract_version": FORMULA_CONTRACT_VERSION,
        "inputs": {name: row["sha256"] for name, row in input_files.items() if name != "package"},
        "profile_id": requested_profile,
        "formula_inventory_digest": formula_inventory_digest,
    }
    contract_digest = canonical_json_sha256(contract_payload)
    if expected_contract_digest is not None and expected_contract_digest != contract_digest:
        raise NewEngineOrchestrationError(
            f"contract digest mismatch: expected={expected_contract_digest}, reproduced={contract_digest}."
        )

    try:
        binding_plan, style_plan = reproduce_style_plan(
            package,
            binding_payload=binding_payload,
            manifest=manifest,
            shell_path=paths["template"],
            module_payload=module_payload,
            style_contract=style_contract,
            ticker_override=requested_ticker,
        )
    except BindingPlanReproductionError as exc:
        blocker_count = len(exc.plan.blocking_issues()) if exc.plan is not None else 0
        raise NewEngineOrchestrationError(
            f"binding/value planning blocker ({blocker_count} blocking issues): {exc}"
        ) from exc
    except StylePlanningError as exc:
        raise NewEngineOrchestrationError(f"style planning blocker: {exc}") from exc

    binding_payload_json = binding_plan.to_dict()
    style_payload_json = style_plan.to_dict()
    binding_digest = canonical_json_sha256(binding_payload_json)
    style_digest = canonical_json_sha256(style_payload_json)
    if expected_binding_plan_digest is not None and expected_binding_plan_digest != binding_digest:
        raise NewEngineOrchestrationError(
            f"binding/value plan digest mismatch: expected={expected_binding_plan_digest}, reproduced={binding_digest}."
        )
    if expected_style_plan_digest is not None and expected_style_plan_digest != style_digest:
        raise NewEngineOrchestrationError(
            f"style plan digest mismatch: expected={expected_style_plan_digest}, reproduced={style_digest}."
        )
    return _PlanContext(
        package_path=paths["package"],
        ticker=requested_ticker,
        profile_id=requested_profile,
        template_path=paths["template"],
        manifest_path=paths["manifest"],
        binding_map_path=paths["binding_map"],
        module_manifest_path=paths["module_manifest"],
        style_policy_path=paths["style_policy"],
        package=package,
        manifest=manifest,
        binding_payload=binding_payload,
        module_payload=module_payload,
        style_contract=style_contract,
        binding_plan=binding_plan,
        style_plan=style_plan,
        binding_payload_json=binding_payload_json,
        style_payload_json=style_payload_json,
        binding_digest=binding_digest,
        style_digest=style_digest,
        contract_digest=contract_digest,
        formula_inventory=formula_inventory,
        formula_inventory_digest=formula_inventory_digest,
        input_files=input_files,
    )


def _plan_counts(context: _PlanContext) -> dict[str, Any]:
    binding = context.binding_payload_json
    style = context.style_payload_json
    ledger = binding.get("issue_ledger") or {}
    ledger_summary = ledger.get("summary") or {}
    return {
        "binding": {
            "digest": context.binding_digest,
            "status": binding.get("status"),
            "planned_write_count": int(binding.get("planned_write_count") or 0),
            "structured_skip_count": int(binding.get("structured_skip_count") or 0),
            "overflow_count": int(binding.get("overflow_count") or 0),
            "issue_count": int(
                ledger_summary.get("canonical_unique_issue_count") or len(ledger.get("issues") or [])
            ),
            "occurrence_count": int(
                ledger_summary.get("detailed_occurrence_count") or len(ledger.get("occurrences") or [])
            ),
            "blocking_issue_count": int(ledger_summary.get("blocking_issue_count") or 0),
        },
        "style": {
            "digest": context.style_digest,
            "status": style.get("status"),
            "action_count": int(style.get("action_count") or 0),
            "decision_count": int(style.get("decision_count") or 0),
        },
    }


def _base_receipt(context: _PlanContext, command: str) -> dict[str, Any]:
    return {
        "receipt_version": RECEIPT_VERSION,
        "command": command,
        "status": "PASS",
        "repo_head": _git_head(),
        "inputs": dict(context.input_files),
        "contract_profile": {
            "contract_digest": context.contract_digest,
            "profile_id": context.profile_id,
            "ticker": context.ticker,
            "formula_contract_version": FORMULA_CONTRACT_VERSION,
        },
        "plans": _plan_counts(context),
        "formula_inventory": _compact_formula_inventory(
            context.formula_inventory, context.formula_inventory_digest
        ),
        "validations": {},
        "output": None,
    }


def _write_json_no_overwrite(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, indent=2)
            handle.write("\n")
    except FileExistsError as exc:
        raise NewEngineOrchestrationError(f"Artifact already exists: {path}") from exc


def _write_plan_artifacts(run_dir: Path, context: _PlanContext, *, include_receipt: bool) -> dict[str, Path]:
    binding_path = run_dir / "binding_plan.json"
    style_path = run_dir / "style_plan.json"
    receipt_path = run_dir / "run_receipt.json"
    for path in (binding_path, style_path, *( (receipt_path,) if include_receipt else () )):
        if path.exists():
            raise NewEngineOrchestrationError(f"Artifact already exists: {path}")
    created: list[Path] = []
    try:
        _write_json_no_overwrite(binding_path, context.binding_payload_json)
        created.append(binding_path)
        _write_json_no_overwrite(style_path, context.style_payload_json)
        created.append(style_path)
        if include_receipt:
            _write_json_no_overwrite(receipt_path, _base_receipt(context, "plan"))
            created.append(receipt_path)
    except Exception:
        for path in created:
            path.unlink(missing_ok=True)
        if run_dir.exists() and not any(run_dir.iterdir()):
            run_dir.rmdir()
        raise
    return {"binding_plan_path": binding_path, "style_plan_path": style_path, "receipt_path": receipt_path}


def run_plan(
    *,
    run_dir: Path | str,
    package_path: Path | str,
    ticker: str,
    profile_id: str,
    template_path: Path | str = DEFAULT_TEMPLATE,
    manifest_path: Path | str = DEFAULT_MANIFEST,
    binding_map_path: Path | str = DEFAULT_BINDING_MAP,
    module_manifest_path: Path | str = DEFAULT_MODULE_MANIFEST,
    style_policy_path: Path | str = DEFAULT_STYLE_POLICY,
    expected_contract_digest: str | None = None,
    expected_binding_plan_digest: str | None = None,
    expected_style_plan_digest: str | None = None,
) -> dict[str, Any]:
    """Reproduce and serialize non-authoritative value/style plan evidence."""

    context = _build_context(
        package_path=package_path,
        ticker=ticker,
        profile_id=profile_id,
        template_path=template_path,
        manifest_path=manifest_path,
        binding_map_path=binding_map_path,
        module_manifest_path=module_manifest_path,
        style_policy_path=style_policy_path,
        expected_contract_digest=expected_contract_digest,
        expected_binding_plan_digest=expected_binding_plan_digest,
        expected_style_plan_digest=expected_style_plan_digest,
    )
    paths = _write_plan_artifacts(Path(run_dir).resolve(), context, include_receipt=True)
    return {"status": "PASS", **paths, "receipt": _base_receipt(context, "plan")}


def _verify_plan_receipt(path: Path, context: _PlanContext) -> Mapping[str, Any]:
    receipt = _load_object(path, "plan receipt")
    expected = _base_receipt(context, "plan")
    checks = {
        "receipt_version": (receipt.get("receipt_version"), RECEIPT_VERSION),
        "command": (receipt.get("command"), "plan"),
        "status": (receipt.get("status"), "PASS"),
        "inputs": (receipt.get("inputs"), expected["inputs"]),
        "contract_profile": (receipt.get("contract_profile"), expected["contract_profile"]),
        "plans": (receipt.get("plans"), expected["plans"]),
        "formula_inventory": (receipt.get("formula_inventory"), expected["formula_inventory"]),
    }
    mismatches = [name for name, (actual, wanted) in checks.items() if actual != wanted]
    if mismatches:
        raise NewEngineOrchestrationError(
            "Plan receipt is stale or tampered and cannot be used as comparison evidence: "
            + ", ".join(mismatches)
        )
    return receipt


def _compact_validation(report: Mapping[str, Any]) -> dict[str, Any]:
    issues = list(report.get("issues") or [])
    return {
        "status": str(report.get("status") or report.get("overall") or "FAIL"),
        "issue_count": len(issues),
        "digest": canonical_json_sha256(report),
    }


def _strict_pre_fill_validation(context: _PlanContext) -> dict[str, Any]:
    from scripts.validate_standard_template_shell import validate_shell

    report = validate_shell(
        template_path=context.template_path,
        manifest_path=context.manifest_path,
        binding_map_path=context.binding_map_path,
        module_manifest_path=context.module_manifest_path,
        style_policy_path=context.style_policy_path,
    )
    if report.get("status") != "PASS":
        raise NewEngineOrchestrationError(f"Strict frozen-shell pre-fill validation failed: {report.get('issues')!r}")
    return report


def _strict_post_fill_validation(
    workbook_path: Path,
    context: _PlanContext,
    plan_path: Path,
    *,
    excel_native_roundtrip: bool = False,
) -> dict[str, Any]:
    from scripts.validate_standard_template_shell import validate_shell

    report = validate_shell(
        template_path=workbook_path,
        manifest_path=context.manifest_path,
        binding_map_path=context.binding_map_path,
        module_manifest_path=context.module_manifest_path,
        style_policy_path=context.style_policy_path,
        allow_filled_values=True,
        approved_shell_path=context.template_path,
        approved_plan_path=plan_path,
        normalized_package_path=context.package_path,
    )
    report_issues = list(report.get("issues") or [])
    accepted_native_layout_issues: list[Mapping[str, Any]] = []
    if report.get("status") != "PASS":
        if excel_native_roundtrip and report_issues and all(
            isinstance(issue, Mapping)
            and str(issue.get("rule_id") or "") == "post_fill_layout_drift"
            for issue in report_issues
        ):
            accepted_native_layout_issues = [
                issue for issue in report_issues if isinstance(issue, Mapping)
            ]
        else:
            raise NewEngineOrchestrationError(f"Strict post-fill validation failed: {report_issues!r}")
    if excel_native_roundtrip:
        native_identity = verify_post_fill_structural_identity(
            workbook_path,
            approved_shell_path=context.template_path,
            manifest=context.manifest,
            binding_payload=context.binding_payload,
            approved_plan=context.binding_payload_json,
            normalized_package=context.package,
            module_payload=context.module_payload,
            style_contract=context.style_contract,
            approved_style_plan=context.style_payload_json,
            excel_native_roundtrip=True,
        )
        if native_identity.get("status") != "PASS":
            raise NewEngineOrchestrationError(
                f"Excel-native post-fill identity validation failed: {native_identity.get('issues')!r}"
            )
        report = dict(report)
        report["status"] = "PASS"
        report["issues"] = [
            issue for issue in report_issues if issue not in accepted_native_layout_issues
        ]
        report["accepted_excel_native_layout_issues"] = accepted_native_layout_issues
        report["excel_native_identity"] = native_identity
    return report


def _saved_workbook_validation(path: Path, ticker: str) -> dict[str, Any]:
    result = validate_workbook(path, ticker)
    payload = result.to_dict()
    # The legacy runner scans every visible prose cell for legacy quarter-label
    # spellings. In the frozen new-engine shell, actual period axes are already
    # enforced independently by shell identity and the exact binding plan, so
    # these broad prose hits remain visible as advisory findings here.
    blocking_counts = {
        "formula_errors": result.formula_error_count,
        "needs_review_p1": result.needs_review_p1_count,
        "qa_blank_nan": result.qa_blank_nan_status_count,
        "cross_company_leakage": result.cross_company_leakage_count,
        "bad_markers": result.bad_marker_count,
        "ooxml_table_issues": result.ooxml_table_issue_count,
        "quality_guardrail_p0_p1": result.quality_guardrail_p0_p1_count,
        "missing_required_sheets": len(result.missing_required_sheets),
        "missing_named_ranges": len(result.missing_named_ranges),
        "calc_settings": 0 if result.calc_settings_ok else 1,
    }
    advisory_issues = [issue.to_dict() for issue in result.issues if issue.category == "quarter_label"]
    payload["runner_overall"] = result.overall
    payload["blocking_counts"] = blocking_counts
    payload["advisory_issues"] = advisory_issues
    payload["status"] = "PASS" if not any(blocking_counts.values()) else "FAIL"
    if payload["status"] != "PASS":
        raise NewEngineOrchestrationError(f"Saved-workbook validation failed: {payload.get('issues')!r}")
    return payload


def _formula_validation(path: Path) -> tuple[dict[str, Any], str]:
    issues = validate_xlsx_formula_compatibility(path)
    if issues:
        raise NewEngineOrchestrationError(f"Workbook formula compatibility failed: {issues[:20]!r}")
    inventory = inventory_xlsx_formula_xml(path)
    return inventory, canonical_json_sha256(inventory)


def _verify_formula_inventory_semantics(
    expected: Mapping[str, Any],
    actual: Mapping[str, Any],
) -> dict[str, Any]:
    """Compare fields that survive Excel shared-formula XML normalization."""

    expected_functions = dict(expected.get("function_counts") or {})
    actual_functions = dict(actual.get("function_counts") or {})
    comparisons = {
        "cell_formula_count": (
            int(actual.get("cell_formula_count") or 0),
            int(expected.get("cell_formula_count") or 0),
        ),
        "MAXIFS": (int(actual_functions.get("MAXIFS") or 0), int(expected_functions.get("MAXIFS") or 0)),
        "MINIFS": (int(actual_functions.get("MINIFS") or 0), int(expected_functions.get("MINIFS") or 0)),
        "LET": (int(actual_functions.get("LET") or 0), int(expected_functions.get("LET") or 0)),
        "let_local_occurrences": (
            int(actual.get("let_local_occurrences") or 0),
            int(expected.get("let_local_occurrences") or 0),
        ),
        "future_function_cell_count": (
            int(actual.get("future_function_cell_count") or 0),
            int(expected.get("future_function_cell_count") or 0),
        ),
    }
    mismatches = {
        name: {"actual": values[0], "expected": values[1]}
        for name, values in comparisons.items()
        if values[0] != values[1]
    }
    if mismatches:
        raise NewEngineOrchestrationError(
            f"Workbook formula inventory differs semantically from the frozen shell: {mismatches!r}"
        )
    return {"status": "PASS", "comparisons": comparisons}


def _common_context_kwargs(kwargs: Mapping[str, Any]) -> dict[str, Any]:
    names = {
        "package_path",
        "ticker",
        "profile_id",
        "template_path",
        "manifest_path",
        "binding_map_path",
        "module_manifest_path",
        "style_policy_path",
        "expected_contract_digest",
        "expected_binding_plan_digest",
        "expected_style_plan_digest",
    }
    return {name: value for name, value in kwargs.items() if name in names}


def render_shadow(
    *,
    run_dir: Path | str,
    output_root: Path | str,
    version: str,
    plan_receipt_path: Path | str,
    excel_native: str = "off",
    required_locale_id: int | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Build and validate one versioned candidate before no-overwrite publication."""

    if not _VERSION_RE.fullmatch(str(version or "")):
        raise NewEngineOrchestrationError("version must be a non-empty filesystem-safe identifier.")
    if excel_native not in {"off", "required"}:
        raise NewEngineOrchestrationError("excel_native must be 'off' or 'required'.")
    context = _build_context(**_common_context_kwargs(kwargs))
    _verify_plan_receipt(Path(plan_receipt_path).resolve(), context)
    output_dir = Path(output_root).resolve()
    final_path = output_dir / f"{context.ticker}_shadow_model_{version}.xlsx"
    final_receipt_path = output_dir / f"{context.ticker}_shadow_model_{version}.run.json"
    if final_path.exists() or final_receipt_path.exists():
        raise NewEngineOrchestrationError(
            f"Versioned shadow workbook or receipt already exists: {final_path}, {final_receipt_path}"
        )
    prefill = _strict_pre_fill_validation(context)

    run_path = Path(run_dir).resolve()
    plan_paths = _write_plan_artifacts(run_path, context, include_receipt=False)
    output_dir.mkdir(parents=True, exist_ok=True)
    candidate = candidate_path_for(final_path)
    receipt_candidate = final_receipt_path.with_name(
        f".{final_receipt_path.name}.{uuid.uuid4().hex}.candidate.json"
    )
    published = False
    receipt_published = False
    try:
        try:
            fill_result = fill_standard_template_from_package(
                context.package_path,
                output_path=candidate,
                ticker_override=context.ticker,
                template_path=context.template_path,
                manifest_path=context.manifest_path,
                binding_map_path=context.binding_map_path,
                module_manifest_path=context.module_manifest_path,
                style_policy_path=context.style_policy_path,
                expected_plan=context.binding_payload_json,
                expected_style_plan=context.style_payload_json,
            )
        except NewTickerValueFillerError as exc:
            raise NewEngineOrchestrationError(f"Exact-cell fill failed: {exc}") from exc
        postfill = _strict_post_fill_validation(candidate, context, plan_paths["binding_plan_path"])
        saved = _saved_workbook_validation(candidate, context.ticker)
        pre_excel_formula_inventory, pre_excel_formula_digest = _formula_validation(candidate)
        if pre_excel_formula_digest != context.formula_inventory_digest:
            raise NewEngineOrchestrationError(
                "Pre-Excel filled workbook formula inventory differs from the authoritative frozen shell."
            )
        excel_report: dict[str, Any] = {"status": "NOT_REQUESTED"}
        if excel_native == "required":
            normalize_candidate_acl(candidate)
            excel_report = run_excel_native_roundtrip(
                candidate,
                ticker=context.ticker,
                required_locale_id=required_locale_id,
            )
            postfill = _strict_post_fill_validation(
                candidate,
                context,
                plan_paths["binding_plan_path"],
                excel_native_roundtrip=True,
            )
            saved = _saved_workbook_validation(candidate, context.ticker)
        acl_report = normalize_candidate_acl(candidate)
        formula_inventory, formula_digest = _formula_validation(candidate)
        formula_semantics = _verify_formula_inventory_semantics(
            context.formula_inventory,
            formula_inventory,
        )
        output_hash = _sha256(candidate)
        receipt = _base_receipt(context, "render-shadow")
        receipt["validations"] = {
            "pre_fill": _compact_validation(prefill),
            "post_fill": _compact_validation(postfill),
            "saved_workbook": _compact_validation(saved),
            "excel_native": excel_report,
            "acl": acl_report,
            "pre_excel_formula_inventory": _compact_formula_inventory(
                pre_excel_formula_inventory,
                pre_excel_formula_digest,
            ),
            "formula_semantics": formula_semantics,
        }
        receipt["formula_inventory"] = _compact_formula_inventory(formula_inventory, formula_digest)
        receipt["output"] = {
            "path": str(final_path),
            "size": candidate.stat().st_size,
            "sha256": output_hash,
            "written_cell_count": fill_result.written_cell_count,
            "styled_cell_count": fill_result.styled_cell_count,
        }
        _write_json_no_overwrite(receipt_candidate, receipt)
        publish_no_overwrite(candidate, final_path)
        published = True
        try:
            publish_no_overwrite(receipt_candidate, final_receipt_path)
            receipt_published = True
        except Exception:
            final_path.unlink(missing_ok=True)
            published = False
            raise
        return {
            "status": "PASS",
            "output_path": final_path,
            "receipt_path": final_receipt_path,
            "run_dir": run_path,
            "receipt": receipt,
        }
    except NewEngineTransactionError as exc:
        raise NewEngineOrchestrationError(str(exc)) from exc
    finally:
        candidate.unlink(missing_ok=True)
        receipt_candidate.unlink(missing_ok=True)
        if published and not receipt_published:
            final_path.unlink(missing_ok=True)


def validate_workbook_immutable(
    *,
    workbook_path: Path | str,
    run_dir: Path | str,
    plan_receipt_path: Path | str,
    excel_native: str = "off",
    required_locale_id: int | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Validate a supplied workbook without ever opening it writable in Excel."""

    if excel_native not in {"off", "required"}:
        raise NewEngineOrchestrationError("excel_native must be 'off' or 'required'.")
    context = _build_context(**_common_context_kwargs(kwargs))
    _verify_plan_receipt(Path(plan_receipt_path).resolve(), context)
    workbook = Path(workbook_path).resolve()
    if not workbook.is_file():
        raise NewEngineOrchestrationError(f"Workbook does not exist: {workbook}")
    original_hash = _sha256(workbook)
    run_path = Path(run_dir).resolve()
    plan_paths = _write_plan_artifacts(run_path, context, include_receipt=False)
    try:
        postfill = _strict_post_fill_validation(
            workbook,
            context,
            plan_paths["binding_plan_path"],
            excel_native_roundtrip=True,
        )
        saved = _saved_workbook_validation(workbook, context.ticker)
        formula_inventory, formula_digest = _formula_validation(workbook)
        formula_semantics = _verify_formula_inventory_semantics(
            context.formula_inventory,
            formula_inventory,
        )
        excel_report: dict[str, Any] = {"status": "NOT_REQUESTED"}
        excel_postfill: Mapping[str, Any] | None = None
        excel_saved: Mapping[str, Any] | None = None
        if excel_native == "required":
            with tempfile.TemporaryDirectory(prefix="new-engine-excel-validate-") as temp_dir:
                isolated = Path(temp_dir) / workbook.name
                shutil.copyfile(workbook, isolated)
                excel_report = run_excel_native_roundtrip(
                    isolated,
                    ticker=context.ticker,
                    required_locale_id=required_locale_id,
                )
                excel_postfill = _strict_post_fill_validation(
                    isolated,
                    context,
                    plan_paths["binding_plan_path"],
                    excel_native_roundtrip=True,
                )
                excel_saved = _saved_workbook_validation(isolated, context.ticker)
        if _sha256(workbook) != original_hash:
            raise NewEngineOrchestrationError("Immutable validation changed the supplied workbook bytes.")
        receipt = _base_receipt(context, "validate")
        receipt["validations"] = {
            "post_fill": _compact_validation(postfill),
            "saved_workbook": _compact_validation(saved),
            "excel_native": excel_report,
            "excel_post_fill": _compact_validation(excel_postfill) if excel_postfill is not None else None,
            "excel_saved_workbook": _compact_validation(excel_saved) if excel_saved is not None else None,
            "formula_semantics": formula_semantics,
        }
        receipt["formula_inventory"] = _compact_formula_inventory(formula_inventory, formula_digest)
        receipt["output"] = {
            "path": str(workbook),
            "size": workbook.stat().st_size,
            "sha256": original_hash,
            "immutable_input": True,
        }
        receipt_path = run_path / "run_receipt.json"
        _write_json_no_overwrite(receipt_path, receipt)
        return {"status": "PASS", "receipt_path": receipt_path, "receipt": receipt}
    except Exception:
        input_changed = _sha256(workbook) != original_hash
        if run_path.exists():
            for key in ("binding_plan_path", "style_plan_path"):
                plan_paths[key].unlink(missing_ok=True)
            if not any(run_path.iterdir()):
                run_path.rmdir()
        if input_changed:
            raise NewEngineOrchestrationError(
                "Immutable validation changed the supplied workbook bytes while handling a failure."
            )
        raise
