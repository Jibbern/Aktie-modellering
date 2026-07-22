"""Compose supported fast, checkpoint, and release checks for the new engine."""
from __future__ import annotations

import argparse
from dataclasses import dataclass, field
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
from typing import Any, Callable, Mapping, Sequence


sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pbi_xbrl.json_schema_validation import load_json_strict  # noqa: E402
from pbi_xbrl.new_ticker_style_planner import (  # noqa: E402
    DEFAULT_MODULE_MANIFEST,
    DEFAULT_STYLE_POLICY,
)
from pbi_xbrl.new_ticker_value_filler import (  # noqa: E402
    DEFAULT_BINDING_MAP,
    DEFAULT_MANIFEST,
    DEFAULT_TEMPLATE,
)
from pbi_xbrl.render_validation_runner import run_render_validation  # noqa: E402


RECEIPT_VERSION = "new-engine-check-tier/v1"
BLOCKING = "blocking"
ADVISORY = "advisory"
_VERSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class CheckTierError(RuntimeError):
    """Raised when a check tier cannot be configured or executed safely."""


@dataclass(frozen=True)
class CheckSpec:
    """One existing check primitive selected by a tier."""

    name: str
    action: str
    classification: str = BLOCKING
    argv: tuple[str, ...] = ()
    paths: tuple[Path, ...] = ()
    details: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CheckResult:
    """Compact result emitted by the composition layer."""

    name: str
    action: str
    classification: str
    status: str
    elapsed_seconds: float
    argv: tuple[str, ...] = ()
    details: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "action": self.action,
            "classification": self.classification,
            "status": self.status,
            "elapsed_seconds": round(self.elapsed_seconds, 3),
            "argv": list(self.argv),
            "details": dict(self.details),
        }


CommandRunner = Callable[..., subprocess.CompletedProcess[str]]
VisualRunner = Callable[..., Any]


def _tail(value: str, limit: int = 4000) -> str:
    text = str(value or "")
    return text if len(text) <= limit else text[-limit:]


def _inside(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def _resolve_input_path(
    value: Path | str,
    repo_root: Path,
    *,
    require_repo_path: bool,
) -> Path:
    candidate = Path(value)
    resolved = (repo_root / candidate).resolve() if not candidate.is_absolute() else candidate.resolve()
    if require_repo_path and not _inside(resolved, repo_root):
        raise CheckTierError(f"Check input must stay inside the repository: {resolved}")
    return resolved


def _run_capture(
    argv: Sequence[str],
    *,
    repo_root: Path,
    env: Mapping[str, str],
    command_runner: CommandRunner,
) -> subprocess.CompletedProcess[str]:
    return command_runner(
        list(argv),
        cwd=str(repo_root),
        env=dict(env),
        capture_output=True,
        text=True,
        shell=False,
        check=False,
    )


def _changed_paths(
    repo_root: Path,
    changed_from: str,
    *,
    env: Mapping[str, str],
    command_runner: CommandRunner,
) -> tuple[Path, ...]:
    tracked = _run_capture(
        ["git", "diff", "--name-only", "--diff-filter=ACMR", changed_from, "--"],
        repo_root=repo_root,
        env=env,
        command_runner=command_runner,
    )
    if tracked.returncode != 0:
        raise CheckTierError(f"Could not enumerate changed files from {changed_from!r}: {_tail(tracked.stderr)}")
    untracked = _run_capture(
        ["git", "ls-files", "--others", "--exclude-standard"],
        repo_root=repo_root,
        env=env,
        command_runner=command_runner,
    )
    if untracked.returncode != 0:
        raise CheckTierError(f"Could not enumerate untracked files: {_tail(untracked.stderr)}")
    relative_names = {
        line.strip()
        for line in (*tracked.stdout.splitlines(), *untracked.stdout.splitlines())
        if line.strip()
    }
    paths = []
    for name in sorted(relative_names, key=str.casefold):
        path = _resolve_input_path(name, repo_root, require_repo_path=True)
        if path.is_file():
            paths.append(path)
    return tuple(paths)


def _deduplicate_paths(paths: Sequence[Path]) -> tuple[Path, ...]:
    result: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        key = os.path.normcase(str(path.resolve()))
        if key not in seen:
            seen.add(key)
            result.append(path.resolve())
    return tuple(sorted(result, key=lambda item: str(item).casefold()))


def _selected_source_paths(
    args: argparse.Namespace,
    *,
    repo_root: Path,
    env: Mapping[str, str],
    command_runner: CommandRunner,
    changed_paths: Sequence[Path] | None = None,
) -> tuple[tuple[Path, ...], tuple[Path, ...]]:
    changed = tuple(changed_paths) if changed_paths is not None else _changed_paths(
        repo_root,
        args.changed_from,
        env=env,
        command_runner=command_runner,
    )
    explicit_python = tuple(
        _resolve_input_path(path, repo_root, require_repo_path=True) for path in args.python_path
    )
    explicit_json = tuple(
        _resolve_input_path(path, repo_root, require_repo_path=False) for path in args.json_path
    )
    python_paths = _deduplicate_paths(
        [path for path in changed if path.suffix.lower() == ".py"] + list(explicit_python)
    )
    json_paths = _deduplicate_paths(
        [path for path in changed if path.suffix.lower() == ".json"] + list(explicit_json)
    )
    missing = [path for path in (*python_paths, *json_paths) if not path.is_file()]
    if missing:
        raise CheckTierError(f"Selected check inputs do not exist: {[str(path) for path in missing]!r}")
    return python_paths, json_paths


def _pytest_argv(
    args: argparse.Namespace,
    temp_root: Path,
    *,
    targets: Sequence[str] | None = None,
    temp_name: str = "pytest",
) -> tuple[str, ...]:
    argv = [
        sys.executable,
        "-m",
        "pytest",
        "-p",
        "no:cacheprovider",
        "--basetemp",
        str(temp_root / temp_name),
    ]
    if args.pytest_marker:
        argv.extend(["-m", args.pytest_marker])
    if args.pytest_k:
        argv.extend(["-k", args.pytest_k])
    argv.extend(targets if targets is not None else args.pytest_target)
    return tuple(argv)


def _required_test_targets(
    args: argparse.Namespace,
    attribute: str,
    option: str,
) -> tuple[str, ...]:
    targets = tuple(
        target.strip()
        for target in getattr(args, attribute, ())
        if isinstance(target, str) and target.strip()
    )
    if not targets:
        raise CheckTierError(f"{option} must declare at least one relevant pytest selection.")
    return targets


def _optional_plan_command(args: argparse.Namespace, temp_root: Path) -> tuple[str, ...] | None:
    package = getattr(args, "package", None)
    ticker = str(getattr(args, "ticker", "") or "").strip()
    profile_id = str(getattr(args, "profile_id", "") or "").strip()
    context = (package, ticker, profile_id)
    digests = (
        getattr(args, "expected_contract_digest", None),
        getattr(args, "expected_value_plan_digest", None),
        getattr(args, "expected_style_plan_digest", None),
    )
    if not any(context):
        if any(digests):
            raise CheckTierError(
                "Expected plan digests require --package, --ticker and --profile-id."
            )
        return None
    if not all(context):
        raise CheckTierError(
            "--package, --ticker and --profile-id must be supplied together."
        )

    argv = [
        sys.executable,
        "-m",
        "pbi_xbrl.new_engine",
        "plan",
        "--package",
        str(Path(package).resolve()),
        "--ticker",
        ticker,
        "--profile-id",
        profile_id,
        "--run-dir",
        str(temp_root / "semantic-plan"),
    ]
    for option, value in (
        ("--expected-contract-digest", digests[0]),
        ("--expected-value-plan-digest", digests[1]),
        ("--expected-style-plan-digest", digests[2]),
    ):
        if value:
            argv.extend([option, str(value)])
    return tuple(argv)


def build_fast_specs(
    args: argparse.Namespace,
    temp_root: Path,
    *,
    repo_root: Path,
    env: Mapping[str, str],
    command_runner: CommandRunner = subprocess.run,
    changed_paths: Sequence[Path] | None = None,
    include_semantic_plan: bool = True,
) -> list[CheckSpec]:
    """Select the fast-tier primitives without executing them."""

    python_paths, json_paths = _selected_source_paths(
        args,
        repo_root=repo_root,
        env=env,
        command_runner=command_runner,
        changed_paths=changed_paths,
    )
    specs = [
        CheckSpec("changed_python_compilation", "compile", paths=python_paths),
        CheckSpec("strict_changed_json", "strict_json", paths=json_paths),
    ]
    if include_semantic_plan:
        plan_argv = _optional_plan_command(args, temp_root)
        if plan_argv is not None:
            specs.append(CheckSpec("semantic_plan_reproduction", "subprocess", argv=plan_argv))
    specs.extend(
        [
            CheckSpec("git_diff_check", "subprocess", argv=("git", "diff", "--check")),
            CheckSpec(
                "git_cached_diff_check",
                "subprocess",
                argv=("git", "diff", "--cached", "--check"),
            ),
            CheckSpec("focused_pytest", "subprocess", argv=_pytest_argv(args, temp_root)),
        ]
    )
    return specs


def _audit_spec(generator: str, *, repo_root: Path = ROOT) -> CheckSpec:
    script = repo_root / "scripts" / "run_standard_template_audit_generator.py"
    if generator == "all":
        argv = (sys.executable, str(script), "--all", "--replay-only")
        name = "audit_replay_all"
    else:
        argv = (sys.executable, str(script), "--generator", generator, "--replay-only")
        name = f"audit_replay:{generator}"
    return CheckSpec(name, "subprocess", argv=argv)


def build_checkpoint_specs(
    args: argparse.Namespace,
    temp_root: Path,
    *,
    repo_root: Path,
    env: Mapping[str, str],
    command_runner: CommandRunner = subprocess.run,
    changed_paths: Sequence[Path] | None = None,
) -> list[CheckSpec]:
    """Select fast checks plus checkpoint-only validators and affected replays."""

    cross_profile_targets = _required_test_targets(
        args,
        "cross_profile_pytest_target",
        "--cross-profile-pytest-target",
    )

    specs = build_fast_specs(
        args,
        temp_root,
        repo_root=repo_root,
        env=env,
        command_runner=command_runner,
        changed_paths=changed_paths,
    )
    specs.append(
        CheckSpec(
            "cross_profile_pytest",
            "subprocess",
            argv=_pytest_argv(
                args,
                temp_root,
                targets=cross_profile_targets,
                temp_name="pytest-cross-profile",
            ),
        )
    )
    specs.append(
        CheckSpec(
            "standard_template_shell_validation",
            "subprocess",
            argv=(
                sys.executable,
                str(repo_root / "scripts" / "validate_standard_template_shell.py"),
            ),
        )
    )
    specs.extend(_audit_spec(generator, repo_root=repo_root) for generator in args.audit_generator)
    if bool(args.saved_workbook_dir) != bool(args.saved_ticker):
        raise CheckTierError("--saved-workbook-dir and at least one --saved-ticker must be supplied together.")
    if args.saved_workbook_dir:
        argv = [
            sys.executable,
            "-m",
            "pbi_xbrl.workbook_validation_runner",
            "--workbook-dir",
            str(Path(args.saved_workbook_dir).resolve()),
            "--output-dir",
            str(temp_root / "saved-workbook-validation"),
            "--tickers",
            *args.saved_ticker,
        ]
        specs.append(CheckSpec("saved_workbook_validation", "subprocess", argv=tuple(argv)))
    return specs


def _execute_spec(
    spec: CheckSpec,
    *,
    repo_root: Path,
    env: Mapping[str, str],
    command_runner: CommandRunner,
    visual_runner: VisualRunner,
) -> CheckResult:
    started = time.perf_counter()
    details: dict[str, Any]
    status = "PASS"
    classification = spec.classification
    try:
        if spec.action == "compile":
            for path in spec.paths:
                compile(path.read_bytes(), str(path), "exec")
            details = {"compiled_count": len(spec.paths), "paths": [str(path) for path in spec.paths]}
        elif spec.action == "strict_json":
            for path in spec.paths:
                load_json_strict(path)
            details = {"parsed_count": len(spec.paths), "paths": [str(path) for path in spec.paths]}
        elif spec.action == "subprocess":
            completed = _run_capture(
                spec.argv,
                repo_root=repo_root,
                env=env,
                command_runner=command_runner,
            )
            details = {
                "returncode": completed.returncode,
                "stdout_tail": _tail(completed.stdout),
                "stderr_tail": _tail(completed.stderr),
            }
            if completed.returncode != 0:
                status = "FAIL" if classification == BLOCKING else "ADVISORY"
        elif spec.action == "visual_audit":
            report = visual_runner(
                spec.details["workbooks"],
                output_root=spec.details["output_root"],
                timestamp=spec.details["timestamp"],
                enable_com=True,
                module_manifest_path=spec.details.get("module_manifest_path", DEFAULT_MODULE_MANIFEST),
            )
            details = report.to_dict()
            if report.overall == "FAIL":
                status = "FAIL"
                classification = BLOCKING
            elif report.overall == "SKIP_RENDER":
                status = "ADVISORY"
                classification = ADVISORY
        else:
            raise CheckTierError(f"Unsupported check action: {spec.action}")
    except Exception as exc:
        status = "FAIL" if classification == BLOCKING else "ADVISORY"
        details = {"error_type": type(exc).__name__, "message": str(exc)}
    return CheckResult(
        name=spec.name,
        action=spec.action,
        classification=classification,
        status=status,
        elapsed_seconds=time.perf_counter() - started,
        argv=spec.argv,
        details=details,
    )


def execute_specs(
    specs: Sequence[CheckSpec],
    *,
    repo_root: Path,
    env: Mapping[str, str],
    command_runner: CommandRunner = subprocess.run,
    visual_runner: VisualRunner = run_render_validation,
) -> list[CheckResult]:
    """Run selected checks, stopping at the first blocking failure."""

    results: list[CheckResult] = []
    for spec in specs:
        result = _execute_spec(
            spec,
            repo_root=repo_root,
            env=env,
            command_runner=command_runner,
            visual_runner=visual_runner,
        )
        results.append(result)
        print(f"[{result.status}] {result.name}", file=sys.stderr)
        if result.status == "FAIL" and result.classification == BLOCKING:
            break
    return results


def _overall(results: Sequence[CheckResult]) -> str:
    if any(result.status == "FAIL" and result.classification == BLOCKING for result in results):
        return "FAIL"
    if any(result.status == "ADVISORY" for result in results):
        return "PASS_WITH_ADVISORIES"
    return "PASS"


def _base_environment(temp_root: Path) -> dict[str, str]:
    env = dict(os.environ)
    env.update(
        {
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONUTF8": "1",
            "TEMP": str(temp_root),
            "TMP": str(temp_root),
            "TMPDIR": str(temp_root),
        }
    )
    return env


def _run_ephemeral_tier(
    args: argparse.Namespace,
    *,
    tier: str,
    command_runner: CommandRunner = subprocess.run,
    visual_runner: VisualRunner = run_render_validation,
) -> dict[str, Any]:
    repo_root = Path(args.repo_root).resolve()
    temp_path: Path
    with tempfile.TemporaryDirectory(prefix=f"new-engine-{tier}-") as temp_dir:
        temp_path = Path(temp_dir)
        env = _base_environment(temp_path)
        if tier == "fast":
            specs = build_fast_specs(
                args,
                temp_path,
                repo_root=repo_root,
                env=env,
                command_runner=command_runner,
            )
        else:
            specs = build_checkpoint_specs(
                args,
                temp_path,
                repo_root=repo_root,
                env=env,
                command_runner=command_runner,
            )
        results = execute_specs(
            specs,
            repo_root=repo_root,
            env=env,
            command_runner=command_runner,
            visual_runner=visual_runner,
        )
    return {
        "receipt_version": RECEIPT_VERSION,
        "tier": tier,
        "status": _overall(results),
        "checks": [result.to_dict() for result in results],
        "cleanup": {"temporary_root": str(temp_path), "removed": not temp_path.exists()},
        "persistent_artifacts": [],
    }


def _common_new_engine_argv(args: argparse.Namespace) -> list[str]:
    argv = [
        "--package",
        str(Path(args.package).resolve()),
        "--ticker",
        args.ticker,
        "--profile-id",
        args.profile_id,
        "--template",
        str(Path(args.template).resolve()),
        "--manifest",
        str(Path(args.manifest).resolve()),
        "--binding-map",
        str(Path(args.binding_map).resolve()),
        "--module-manifest",
        str(Path(args.module_manifest).resolve()),
        "--style-policy",
        str(Path(args.style_policy).resolve()),
        "--log-level",
        args.log_level,
    ]
    for option, value in (
        ("--expected-contract-digest", args.expected_contract_digest),
        ("--expected-value-plan-digest", args.expected_value_plan_digest),
        ("--expected-style-plan-digest", args.expected_style_plan_digest),
    ):
        if value:
            argv.extend([option, value])
    return argv


def release_plan_command(args: argparse.Namespace, reports_dir: Path) -> tuple[str, ...]:
    return tuple(
        [sys.executable, "-m", "pbi_xbrl.new_engine", "plan"]
        + _common_new_engine_argv(args)
        + ["--run-dir", str(reports_dir / "plan")]
    )


def release_render_command(args: argparse.Namespace, reports_dir: Path) -> tuple[str, ...]:
    return tuple(
        [sys.executable, "-m", "pbi_xbrl.new_engine", "render-shadow"]
        + _common_new_engine_argv(args)
        + [
            "--run-dir",
            str(reports_dir / "render"),
            "--plan-receipt",
            str(reports_dir / "plan" / "run_receipt.json"),
            "--output-root",
            str(Path(args.output_root).resolve()),
            "--version",
            args.version,
            "--excel-native",
            "required",
            "--excel-locale-id",
            str(args.excel_locale_id),
        ]
    )


def release_validate_command(
    args: argparse.Namespace,
    reports_dir: Path,
    shadow_workbook: Path,
) -> tuple[str, ...]:
    return tuple(
        [sys.executable, "-m", "pbi_xbrl.new_engine", "validate"]
        + _common_new_engine_argv(args)
        + [
            "--run-dir",
            str(reports_dir / "validate"),
            "--plan-receipt",
            str(reports_dir / "plan" / "run_receipt.json"),
            "--workbook",
            str(shadow_workbook),
            "--excel-native",
            "required",
            "--excel-locale-id",
            str(args.excel_locale_id),
        ]
    )


def release_promote_dry_run_command(
    args: argparse.Namespace,
    reports_dir: Path,
    shadow_workbook: Path,
    shadow_receipt: Path,
    shadow_sha256: str,
) -> tuple[str, ...]:
    argv = (
        [sys.executable, "-m", "pbi_xbrl.new_engine", "promote"]
        + _common_new_engine_argv(args)
        + [
            "--run-dir",
            str(reports_dir / "promotion-dry-run"),
            "--plan-receipt",
            str(reports_dir / "plan" / "run_receipt.json"),
            "--shadow-workbook",
            str(shadow_workbook),
            "--shadow-receipt",
            str(shadow_receipt),
            "--canonical-workbook",
            str(Path(args.canonical_workbook).resolve()),
            "--rollback-dir",
            str(Path(args.rollback_dir).resolve()),
            "--expected-shadow-sha256",
            shadow_sha256,
            "--product-approval-reference",
            args.product_approval_reference,
            "--expected-head",
            args.expected_head,
            "--excel-locale-id",
            str(args.excel_locale_id),
        ]
    )
    if "--execute" in argv:
        raise CheckTierError("Release tier must never execute canonical promotion.")
    return tuple(argv)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verify_release_gate(
    args: argparse.Namespace,
    *,
    repo_root: Path,
    env: Mapping[str, str],
    command_runner: CommandRunner,
) -> tuple[Path, Path, Path, Path]:
    head = _run_capture(
        ["git", "rev-parse", "HEAD"],
        repo_root=repo_root,
        env=env,
        command_runner=command_runner,
    )
    status = _run_capture(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"],
        repo_root=repo_root,
        env=env,
        command_runner=command_runner,
    )
    if head.returncode != 0 or head.stdout.strip().lower() != args.expected_head.lower():
        raise CheckTierError(
            f"Release HEAD mismatch: expected={args.expected_head!r}, actual={head.stdout.strip()!r}."
        )
    if status.returncode != 0 or status.stdout.strip():
        raise CheckTierError(f"Release requires a clean repository: {_tail(status.stdout or status.stderr)}")
    if not _VERSION_RE.fullmatch(args.version):
        raise CheckTierError("--version must be one non-empty filesystem-safe identifier.")
    reports_dir = Path(args.reports_dir).resolve()
    output_root = Path(args.output_root).resolve()
    if _inside(reports_dir, repo_root) or _inside(output_root, repo_root):
        raise CheckTierError("Release reports and shadow output must be outside the repository.")
    if reports_dir.exists():
        raise CheckTierError(f"Release reports directory already exists: {reports_dir}")
    ticker = args.ticker.strip().upper()
    shadow = output_root / f"{ticker}_shadow_model_{args.version}.xlsx"
    shadow_receipt = output_root / f"{ticker}_shadow_model_{args.version}.run.json"
    if shadow.exists() or shadow_receipt.exists():
        raise CheckTierError(f"Versioned shadow or receipt already exists: {shadow}, {shadow_receipt}")
    return reports_dir, output_root, shadow, shadow_receipt


def _write_release_receipt(path: Path, payload: Mapping[str, Any]) -> None:
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False, sort_keys=True)
        handle.write("\n")


def _cleanup_failed_release_outputs(
    *,
    shadow: Path,
    shadow_receipt: Path,
    expected_ticker: str,
    expected_head: str,
) -> dict[str, Any]:
    """Remove only a freshly verified render-shadow pair after a later release failure."""

    if not shadow.is_file() or not shadow_receipt.is_file():
        raise CheckTierError(
            "Cannot clean a failed release because the invocation-owned shadow pair is incomplete: "
            f"{shadow}, {shadow_receipt}."
        )
    expected_receipt = shadow.with_suffix(".run.json")
    if shadow_receipt.resolve() != expected_receipt.resolve():
        raise CheckTierError(
            f"Shadow receipt is not adjacent to its workbook: {shadow_receipt} != {expected_receipt}."
        )
    receipt_hash_before = _sha256(shadow_receipt)
    receipt = load_json_strict(shadow_receipt)
    receipt_hash_after = _sha256(shadow_receipt)
    if receipt_hash_before != receipt_hash_after:
        raise CheckTierError("Shadow receipt changed while release-failure cleanup was verifying it.")
    contract_profile = receipt.get("contract_profile") or {}
    output = receipt.get("output") or {}
    output_path = Path(str(output.get("path") or "")).resolve()
    shadow_hash = _sha256(shadow)
    required_identity = {
        "receipt_version": receipt.get("receipt_version") == "new-engine-run/v1",
        "command": receipt.get("command") == "render-shadow",
        "status": receipt.get("status") == "PASS",
        "ticker": str(contract_profile.get("ticker") or "").upper() == expected_ticker.upper(),
        "repo_head": str(receipt.get("repo_head") or "").lower() == expected_head.lower(),
        "output_path": output_path == shadow.resolve(),
        "output_hash": str(output.get("sha256") or "").lower() == shadow_hash.lower(),
        "output_size": int(output.get("size") or -1) == shadow.stat().st_size,
    }
    failed = [name for name, passed in required_identity.items() if not passed]
    if failed:
        raise CheckTierError(
            "Invocation-owned release output identity could not be proven before cleanup: "
            + ", ".join(failed)
        )
    shadow.unlink()
    shadow_receipt.unlink()
    if shadow.exists() or shadow_receipt.exists():
        raise CheckTierError("Failed release outputs still exist after exact-path cleanup.")
    return {
        "status": "PASS",
        "shadow_path": str(shadow),
        "shadow_sha256": shadow_hash,
        "shadow_receipt_path": str(shadow_receipt),
        "shadow_receipt_sha256": receipt_hash_before,
        "identity_checks": required_identity,
        "removed": True,
    }


def _run_release(
    args: argparse.Namespace,
    *,
    command_runner: CommandRunner = subprocess.run,
    visual_runner: VisualRunner = run_render_validation,
) -> dict[str, Any]:
    repo_root = Path(args.repo_root).resolve()
    cross_profile_targets = _required_test_targets(
        args,
        "cross_profile_pytest_target",
        "--cross-profile-pytest-target",
    )
    full_targets = _required_test_targets(
        args,
        "full_pytest_target",
        "--full-pytest-target",
    )
    results: list[CheckResult] = []
    reports_dir: Path | None = None
    shadow: Path | None = None
    shadow_receipt: Path | None = None
    temp_path: Path
    with tempfile.TemporaryDirectory(prefix="new-engine-release-") as temp_dir:
        temp_path = Path(temp_dir)
        env = _base_environment(temp_path)
        reports_dir, _output_root, shadow, shadow_receipt = _verify_release_gate(
            args,
            repo_root=repo_root,
            env=env,
            command_runner=command_runner,
        )
        strict_release_json = [
            Path(args.package),
            Path(args.manifest),
            Path(args.binding_map),
            Path(args.module_manifest),
            Path(args.style_policy),
        ]
        args.json_path = list(args.json_path) + strict_release_json
        pre_specs = build_fast_specs(
            args,
            temp_path,
            repo_root=repo_root,
            env=env,
            command_runner=command_runner,
            include_semantic_plan=False,
        )
        pre_specs.extend(
            [
                CheckSpec(
                    "cross_profile_pytest",
                    "subprocess",
                    argv=_pytest_argv(
                        args,
                        temp_path,
                        targets=cross_profile_targets,
                        temp_name="pytest-cross-profile",
                    ),
                ),
                CheckSpec(
                    "full_relevant_pytest",
                    "subprocess",
                    argv=_pytest_argv(
                        args,
                        temp_path,
                        targets=full_targets,
                        temp_name="pytest-full",
                    ),
                ),
                CheckSpec(
                    "standard_template_shell_validation",
                    "subprocess",
                    argv=(
                        sys.executable,
                        str(repo_root / "scripts" / "validate_standard_template_shell.py"),
                    ),
                ),
                _audit_spec("all", repo_root=repo_root),
            ]
        )
        results.extend(
            execute_specs(
                pre_specs,
                repo_root=repo_root,
                env=env,
                command_runner=command_runner,
                visual_runner=visual_runner,
            )
        )
        if _overall(results) == "FAIL":
            payload = {
                "receipt_version": RECEIPT_VERSION,
                "tier": "release",
                "status": "FAIL",
                "checks": [result.to_dict() for result in results],
                "cleanup": {"temporary_root": str(temp_path), "removed": None},
                "persistent_artifacts": [],
            }
            # The TemporaryDirectory context still removes the path before return.
            payload["cleanup"]["removed"] = True
            return payload

        reports_dir.mkdir(parents=True, exist_ok=False)
        phases = [
            CheckSpec("deterministic_plan", "subprocess", argv=release_plan_command(args, reports_dir)),
            CheckSpec("transactional_shadow_render", "subprocess", argv=release_render_command(args, reports_dir)),
        ]
        results.extend(
            execute_specs(
                phases,
                repo_root=repo_root,
                env=env,
                command_runner=command_runner,
                visual_runner=visual_runner,
            )
        )
        if _overall(results) != "FAIL" and shadow.is_file() and shadow_receipt.is_file():
            results.extend(
                execute_specs(
                    [
                        CheckSpec(
                            "immutable_excel_native_validation",
                            "subprocess",
                            argv=release_validate_command(args, reports_dir, shadow),
                        ),
                        CheckSpec(
                            "visual_product_audit",
                            "visual_audit",
                            details={
                                "workbooks": {args.ticker.upper(): shadow},
                                "output_root": reports_dir / "visual",
                                "timestamp": args.version,
                                "module_manifest_path": Path(args.module_manifest).resolve(),
                            },
                        ),
                    ],
                    repo_root=repo_root,
                    env=env,
                    command_runner=command_runner,
                    visual_runner=visual_runner,
                )
            )
        elif _overall(results) != "FAIL":
            results.append(
                CheckResult(
                    name="published_shadow_identity",
                    action="artifact_check",
                    classification=BLOCKING,
                    status="FAIL",
                    elapsed_seconds=0.0,
                    details={"shadow": str(shadow), "receipt": str(shadow_receipt)},
                )
            )
        if _overall(results) != "FAIL":
            shadow_hash = _sha256(shadow)
            results.extend(
                execute_specs(
                    [
                        CheckSpec(
                            "canonical_promotion_dry_run",
                            "subprocess",
                            argv=release_promote_dry_run_command(
                                args,
                                reports_dir,
                                shadow,
                                shadow_receipt,
                                shadow_hash,
                            ),
                        )
                    ],
                    repo_root=repo_root,
                    env=env,
                    command_runner=command_runner,
                    visual_runner=visual_runner,
                )
            )

        rendered_successfully = any(
            result.name == "transactional_shadow_render" and result.status == "PASS"
            for result in results
        )
        if _overall(results) == "FAIL" and rendered_successfully and shadow.exists() and shadow_receipt.exists():
            started = time.perf_counter()
            try:
                cleanup_details = _cleanup_failed_release_outputs(
                    shadow=shadow,
                    shadow_receipt=shadow_receipt,
                    expected_ticker=args.ticker,
                    expected_head=args.expected_head,
                )
                cleanup_status = "PASS"
            except Exception as exc:
                cleanup_details = {"error_type": type(exc).__name__, "message": str(exc)}
                cleanup_status = "FAIL"
            results.append(
                CheckResult(
                    name="failed_release_output_cleanup",
                    action="artifact_cleanup",
                    classification=BLOCKING,
                    status=cleanup_status,
                    elapsed_seconds=time.perf_counter() - started,
                    details=cleanup_details,
                )
            )

    assert reports_dir is not None
    payload = {
        "receipt_version": RECEIPT_VERSION,
        "tier": "release",
        "status": _overall(results),
        "checks": [result.to_dict() for result in results],
        "cleanup": {"temporary_root": str(temp_path), "removed": not temp_path.exists()},
        "persistent_artifacts": {
            "reports_dir": str(reports_dir),
            "shadow_workbook": str(shadow) if shadow and shadow.exists() else None,
            "shadow_receipt": str(shadow_receipt) if shadow_receipt and shadow_receipt.exists() else None,
        },
        "promotion": {"mode": "dry-run", "executed": False},
    }
    _write_release_receipt(reports_dir / "check_tier_receipt.json", payload)
    return payload


def _add_common_tier_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--changed-from", default="HEAD")
    parser.add_argument("--python-path", action="append", type=Path, default=[])
    parser.add_argument("--json-path", action="append", type=Path, default=[])
    parser.add_argument("--pytest-target", action="append", required=True)
    parser.add_argument("--pytest-k")
    parser.add_argument("--pytest-marker")


def _add_optional_plan_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--package", type=Path)
    parser.add_argument("--ticker")
    parser.add_argument("--profile-id")
    parser.add_argument("--expected-contract-digest")
    parser.add_argument("--expected-value-plan-digest")
    parser.add_argument("--expected-style-plan-digest")


def _add_checkpoint_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--cross-profile-pytest-target", action="append", required=True)
    parser.add_argument("--audit-generator", action="append", default=[])
    parser.add_argument("--saved-workbook-dir", type=Path)
    parser.add_argument("--saved-ticker", action="append", default=[])


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="tier", required=True)

    fast = subparsers.add_parser("fast", help="Run focused, cache-free development checks.")
    _add_common_tier_arguments(fast)
    _add_optional_plan_arguments(fast)

    checkpoint = subparsers.add_parser("checkpoint", help="Run focused checkpoint safeguards.")
    _add_common_tier_arguments(checkpoint)
    _add_optional_plan_arguments(checkpoint)
    _add_checkpoint_arguments(checkpoint)

    release = subparsers.add_parser("release", help="Run the full versioned shadow release tier.")
    _add_common_tier_arguments(release)
    release.add_argument("--package", required=True, type=Path)
    release.add_argument("--ticker", required=True)
    release.add_argument("--profile-id", required=True)
    release.add_argument("--template", type=Path, default=DEFAULT_TEMPLATE)
    release.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    release.add_argument("--binding-map", type=Path, default=DEFAULT_BINDING_MAP)
    release.add_argument("--module-manifest", type=Path, default=DEFAULT_MODULE_MANIFEST)
    release.add_argument("--style-policy", type=Path, default=DEFAULT_STYLE_POLICY)
    release.add_argument("--expected-contract-digest")
    release.add_argument("--expected-value-plan-digest")
    release.add_argument("--expected-style-plan-digest")
    release.add_argument("--cross-profile-pytest-target", action="append", required=True)
    release.add_argument("--full-pytest-target", action="append", required=True)
    release.add_argument("--output-root", required=True, type=Path)
    release.add_argument("--version", required=True)
    release.add_argument("--reports-dir", required=True, type=Path)
    release.add_argument("--canonical-workbook", required=True, type=Path)
    release.add_argument("--rollback-dir", required=True, type=Path)
    release.add_argument("--product-approval-reference", required=True)
    release.add_argument("--expected-head", required=True)
    release.add_argument("--excel-locale-id", required=True, type=int)
    release.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"), default="INFO")
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    command_runner: CommandRunner = subprocess.run,
    visual_runner: VisualRunner = run_render_validation,
) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        if args.tier in {"fast", "checkpoint"}:
            payload = _run_ephemeral_tier(
                args,
                tier=args.tier,
                command_runner=command_runner,
                visual_runner=visual_runner,
            )
        else:
            payload = _run_release(
                args,
                command_runner=command_runner,
                visual_runner=visual_runner,
            )
    except (CheckTierError, OSError) as exc:
        payload = {
            "receipt_version": RECEIPT_VERSION,
            "tier": getattr(args, "tier", None),
            "status": "FAIL",
            "reason": type(exc).__name__,
            "message": str(exc),
        }
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if payload["status"] in {"PASS", "PASS_WITH_ADVISORIES"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
