from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess

import pytest

import scripts.run_new_engine_checks as checks


ROOT = Path(__file__).resolve().parents[1]


def _tier_args(**overrides: object) -> argparse.Namespace:
    values: dict[str, object] = {
        "repo_root": ROOT,
        "changed_from": "HEAD",
        "python_path": [],
        "json_path": [],
        "pytest_target": ["tests/test_new_engine_cli.py"],
        "pytest_k": None,
        "pytest_marker": None,
        "package": None,
        "ticker": None,
        "profile_id": None,
        "expected_contract_digest": None,
        "expected_value_plan_digest": None,
        "expected_style_plan_digest": None,
        "cross_profile_pytest_target": ["tests/test_workbook_module_manifest.py"],
        "audit_generator": [],
        "saved_workbook_dir": None,
        "saved_ticker": [],
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def _release_args(tmp_path: Path) -> argparse.Namespace:
    values = vars(_tier_args())
    values.update(
        package=tmp_path / "ANF_package.json",
        ticker="ANF",
        profile_id="full_union",
        template=ROOT / "templates" / "standard_stock_model_template.xlsx",
        manifest=ROOT / "docs" / "standard_template_shell_manifest.json",
        binding_map=ROOT / "docs" / "workbook_binding_map.json",
        module_manifest=ROOT / "docs" / "workbook_module_manifest.json",
        style_policy=ROOT / "docs" / "standard_template_style_policy.json",
        expected_contract_digest=None,
        expected_value_plan_digest=None,
        expected_style_plan_digest=None,
        cross_profile_pytest_target=["tests/test_workbook_module_manifest.py"],
        full_pytest_target=["tests/test_new_engine_cli.py", "tests/test_new_engine_orchestration.py"],
        output_root=tmp_path / "outputs",
        version="v8",
        reports_dir=tmp_path / "reports",
        canonical_workbook=tmp_path / "canonical" / "ANF_model.xlsx",
        rollback_dir=tmp_path / "rollback",
        product_approval_reference="approval:ANF-v8",
        expected_head="c" * 40,
        excel_locale_id=1053,
        log_level="INFO",
    )
    return argparse.Namespace(**values)


def _completed(argv: object, returncode: int = 0, stdout: str = "", stderr: str = ""):
    return subprocess.CompletedProcess(argv, returncode, stdout=stdout, stderr=stderr)


def test_fast_selects_only_focused_cache_free_primitives(tmp_path: Path) -> None:
    args = _tier_args()
    specs = checks.build_fast_specs(
        args,
        tmp_path,
        repo_root=ROOT,
        env={},
        changed_paths=[ROOT / "scripts" / "run_new_engine_checks.py"],
    )

    assert [spec.name for spec in specs] == [
        "changed_python_compilation",
        "strict_changed_json",
        "git_diff_check",
        "git_cached_diff_check",
        "focused_pytest",
    ]
    pytest_argv = specs[-1].argv
    assert pytest_argv[:3] == (checks.sys.executable, "-m", "pytest")
    assert ("-p", "no:cacheprovider") == pytest_argv[3:5]
    assert "--basetemp" in pytest_argv
    assert specs[0].paths == (ROOT / "scripts" / "run_new_engine_checks.py",)


def test_checkpoint_adds_shell_affected_replay_and_optional_saved_validation(tmp_path: Path) -> None:
    args = _tier_args(
        audit_generator=["scripts/build_standard_template_binding_audit.py"],
        saved_workbook_dir=tmp_path / "ANF_model.xlsx",
        saved_ticker=["ANF"],
    )
    specs = checks.build_checkpoint_specs(
        args,
        tmp_path,
        repo_root=ROOT,
        env={},
        changed_paths=[],
    )

    names = [spec.name for spec in specs]
    assert names[-4:] == [
        "cross_profile_pytest",
        "standard_template_shell_validation",
        "audit_replay:scripts/build_standard_template_binding_audit.py",
        "saved_workbook_validation",
    ]
    assert all(spec.classification == checks.BLOCKING for spec in specs)
    saved = specs[-1].argv
    assert "pbi_xbrl.workbook_validation_runner" in saved
    assert str(tmp_path / "saved-workbook-validation") in saved


def test_fast_delegates_optional_package_digests_to_new_engine_plan(tmp_path: Path) -> None:
    package = tmp_path / "ANF_package.json"
    package.write_text("{}\n", encoding="utf-8")
    args = _tier_args(
        package=package,
        ticker="ANF",
        profile_id="full_union",
        expected_contract_digest="a" * 64,
        expected_value_plan_digest="b" * 64,
        expected_style_plan_digest="c" * 64,
    )

    specs = checks.build_fast_specs(
        args,
        tmp_path,
        repo_root=ROOT,
        env={},
        changed_paths=[],
    )

    semantic = next(spec for spec in specs if spec.name == "semantic_plan_reproduction")
    assert semantic.argv[:4] == (
        checks.sys.executable,
        "-m",
        "pbi_xbrl.new_engine",
        "plan",
    )
    assert semantic.argv[semantic.argv.index("--run-dir") + 1] == str(tmp_path / "semantic-plan")
    assert semantic.argv[semantic.argv.index("--expected-contract-digest") + 1] == "a" * 64
    assert semantic.argv[semantic.argv.index("--expected-value-plan-digest") + 1] == "b" * 64
    assert semantic.argv[semantic.argv.index("--expected-style-plan-digest") + 1] == "c" * 64


@pytest.mark.parametrize(
    "overrides",
    [
        {"package": Path("ANF.json")},
        {"expected_contract_digest": "a" * 64},
    ],
)
def test_optional_plan_context_fails_closed_when_partial(
    tmp_path: Path,
    overrides: dict[str, object],
) -> None:
    with pytest.raises(checks.CheckTierError):
        checks.build_fast_specs(
            _tier_args(**overrides),
            tmp_path,
            repo_root=ROOT,
            env={},
            changed_paths=[],
        )


def test_checkpoint_and_release_require_declared_broad_test_selections(tmp_path: Path) -> None:
    with pytest.raises(checks.CheckTierError, match="cross-profile-pytest-target"):
        checks.build_checkpoint_specs(
            _tier_args(cross_profile_pytest_target=[]),
            tmp_path,
            repo_root=ROOT,
            env={},
            changed_paths=[],
        )

    args = _release_args(tmp_path)
    args.full_pytest_target = []
    with pytest.raises(checks.CheckTierError, match="full-pytest-target"):
        checks._run_release(args, command_runner=lambda argv, **_kwargs: _completed(argv))


def test_release_commands_require_excel_and_promotion_is_dry_run(tmp_path: Path) -> None:
    args = _release_args(tmp_path)
    reports = Path(args.reports_dir)
    shadow = Path(args.output_root) / "ANF_shadow_model_v8.xlsx"
    receipt = Path(args.output_root) / "ANF_shadow_model_v8.run.json"

    plan = checks.release_plan_command(args, reports)
    render = checks.release_render_command(args, reports)
    validate = checks.release_validate_command(args, reports, shadow)
    promote = checks.release_promote_dry_run_command(args, reports, shadow, receipt, "a" * 64)

    assert plan[2] == "pbi_xbrl.new_engine"
    assert "plan" in plan
    assert render[render.index("--excel-native") + 1] == "required"
    assert validate[validate.index("--excel-native") + 1] == "required"
    assert "--expected-shadow-sha256" in promote
    assert "--product-approval-reference" in promote
    assert "--execute" not in promote


def test_blocking_failure_stops_later_checks(tmp_path: Path) -> None:
    calls: list[tuple[str, ...]] = []

    def runner(argv, **_kwargs):
        calls.append(tuple(argv))
        return _completed(argv, returncode=1, stderr="blocked")

    specs = [
        checks.CheckSpec("first", "subprocess", argv=("first",)),
        checks.CheckSpec("second", "subprocess", argv=("second",)),
    ]
    results = checks.execute_specs(
        specs,
        repo_root=ROOT,
        env={},
        command_runner=runner,
    )

    assert calls == [("first",)]
    assert results[0].status == "FAIL"
    assert checks._overall(results) == "FAIL"


class _VisualReport:
    def __init__(self, overall: str) -> None:
        self.overall = overall

    def to_dict(self) -> dict[str, str]:
        return {"overall": self.overall}


def test_skipped_visual_audit_is_advisory_and_does_not_stop_dry_run(tmp_path: Path) -> None:
    calls: list[tuple[str, ...]] = []

    def runner(argv, **_kwargs):
        calls.append(tuple(argv))
        return _completed(argv)

    specs = [
        checks.CheckSpec(
            "visual",
            "visual_audit",
            details={"workbooks": {}, "output_root": tmp_path, "timestamp": "v8"},
        ),
        checks.CheckSpec("promotion_dry_run", "subprocess", argv=("promote",)),
    ]
    results = checks.execute_specs(
        specs,
        repo_root=ROOT,
        env={},
        command_runner=runner,
        visual_runner=lambda *_args, **_kwargs: _VisualReport("SKIP_RENDER"),
    )

    assert results[0].classification == checks.ADVISORY
    assert results[0].status == "ADVISORY"
    assert calls == [("promote",)]
    assert checks._overall(results) == "PASS_WITH_ADVISORIES"


def test_visual_failure_is_blocking(tmp_path: Path) -> None:
    specs = [
        checks.CheckSpec(
            "visual",
            "visual_audit",
            details={"workbooks": {}, "output_root": tmp_path, "timestamp": "v8"},
        ),
        checks.CheckSpec("promotion_dry_run", "subprocess", argv=("promote",)),
    ]
    results = checks.execute_specs(
        specs,
        repo_root=ROOT,
        env={},
        command_runner=lambda argv, **_kwargs: _completed(argv),
        visual_runner=lambda *_args, **_kwargs: _VisualReport("FAIL"),
    )

    assert len(results) == 1
    assert results[0].classification == checks.BLOCKING
    assert results[0].status == "FAIL"


def test_fast_tier_removes_its_owned_temporary_root(tmp_path: Path) -> None:
    observed_temp_roots: list[Path] = []

    def runner(argv, **kwargs):
        env = kwargs["env"]
        temp_root = Path(env["TEMP"])
        observed_temp_roots.append(temp_root)
        (temp_root / "owned-test-artifact.txt").write_text("temporary", encoding="utf-8")
        if tuple(argv[:3]) == ("git", "diff", "--name-only"):
            return _completed(argv, stdout="")
        if tuple(argv[:3]) == ("git", "ls-files", "--others"):
            return _completed(argv, stdout="")
        return _completed(argv)

    payload = checks._run_ephemeral_tier(
        _tier_args(repo_root=ROOT),
        tier="fast",
        command_runner=runner,
    )

    assert payload["status"] == "PASS"
    assert payload["cleanup"]["removed"] is True
    assert observed_temp_roots
    assert all(not path.exists() for path in observed_temp_roots)
    assert payload["persistent_artifacts"] == []


def test_checkpoint_rejects_partial_saved_validation_configuration(tmp_path: Path) -> None:
    args = _tier_args(saved_workbook_dir=tmp_path / "ANF_model.xlsx")

    try:
        checks.build_checkpoint_specs(
            args,
            tmp_path,
            repo_root=ROOT,
            env={},
            changed_paths=[],
        )
    except checks.CheckTierError as exc:
        assert "--saved-workbook-dir" in str(exc)
    else:
        raise AssertionError("Partial saved-workbook configuration must fail closed.")


def test_release_end_to_end_orchestration_order_advisory_cleanup_and_dry_run(
    tmp_path: Path,
) -> None:
    args = _release_args(tmp_path)
    args.cross_profile_pytest_target = ["tests/cross_profile.py"]
    args.full_pytest_target = ["tests/full_release.py"]
    Path(args.package).write_text("{}\n", encoding="utf-8")
    events: list[str] = []
    command_calls: list[tuple[str, ...]] = []

    def runner(argv, **_kwargs):
        command = tuple(str(item) for item in argv)
        command_calls.append(command)
        if command[:3] == ("git", "rev-parse", "HEAD"):
            return _completed(argv, stdout=args.expected_head + "\n")
        if command[:3] == ("git", "status", "--porcelain=v1"):
            return _completed(argv, stdout="")
        if command[:3] == ("git", "diff", "--name-only"):
            return _completed(argv, stdout="")
        if command[:3] == ("git", "ls-files", "--others"):
            return _completed(argv, stdout="")
        if len(command) >= 4 and command[1:3] == ("-m", "pbi_xbrl.new_engine"):
            subcommand = command[3]
            events.append(subcommand)
            if subcommand == "render-shadow":
                shadow = Path(args.output_root) / "ANF_shadow_model_v8.xlsx"
                receipt = Path(args.output_root) / "ANF_shadow_model_v8.run.json"
                shadow.parent.mkdir(parents=True, exist_ok=True)
                shadow.write_bytes(b"isolated-shadow-fixture")
                receipt.write_text("{}\n", encoding="utf-8")
        return _completed(argv)

    def visual_runner(*_args, **_kwargs):
        events.append("visual-audit")
        return _VisualReport("SKIP_RENDER")

    payload = checks._run_release(
        args,
        command_runner=runner,
        visual_runner=visual_runner,
    )

    assert payload["status"] == "PASS_WITH_ADVISORIES"
    assert [item["name"] for item in payload["checks"]] == [
        "changed_python_compilation",
        "strict_changed_json",
        "git_diff_check",
        "git_cached_diff_check",
        "focused_pytest",
        "cross_profile_pytest",
        "full_relevant_pytest",
        "standard_template_shell_validation",
        "audit_replay_all",
        "deterministic_plan",
        "transactional_shadow_render",
        "immutable_excel_native_validation",
        "visual_product_audit",
        "canonical_promotion_dry_run",
    ]
    assert events == ["plan", "render-shadow", "validate", "visual-audit", "promote"]
    promote = next(command for command in command_calls if "promote" in command)
    assert "--execute" not in promote
    assert payload["promotion"] == {"mode": "dry-run", "executed": False}
    assert payload["cleanup"]["removed"] is True
    assert not Path(payload["cleanup"]["temporary_root"]).exists()


def test_release_blocking_full_selection_stops_before_outputs(tmp_path: Path) -> None:
    args = _release_args(tmp_path)
    args.full_pytest_target = ["tests/full_release.py"]
    Path(args.package).write_text("{}\n", encoding="utf-8")
    commands: list[tuple[str, ...]] = []

    def runner(argv, **_kwargs):
        command = tuple(str(item) for item in argv)
        commands.append(command)
        if command[:3] == ("git", "rev-parse", "HEAD"):
            return _completed(argv, stdout=args.expected_head + "\n")
        if command[:3] == ("git", "status", "--porcelain=v1"):
            return _completed(argv, stdout="")
        if command[:3] in {
            ("git", "diff", "--name-only"),
            ("git", "ls-files", "--others"),
        }:
            return _completed(argv, stdout="")
        if "tests/full_release.py" in command:
            return _completed(argv, returncode=1, stderr="full release selection failed")
        return _completed(argv)

    payload = checks._run_release(args, command_runner=runner)

    assert payload["status"] == "FAIL"
    assert payload["checks"][-1]["name"] == "full_relevant_pytest"
    assert not Path(args.reports_dir).exists()
    assert not Path(args.output_root).exists()
    assert not any("render-shadow" in command or "promote" in command for command in commands)
    assert payload["cleanup"]["removed"] is True


def test_release_rejects_existing_destination_before_checks_or_artifacts(tmp_path: Path) -> None:
    args = _release_args(tmp_path)
    Path(args.package).write_text("{}\n", encoding="utf-8")
    shadow = Path(args.output_root) / "ANF_shadow_model_v8.xlsx"
    shadow.parent.mkdir(parents=True)
    shadow.write_bytes(b"pre-existing")
    commands: list[tuple[str, ...]] = []

    def runner(argv, **_kwargs):
        command = tuple(str(item) for item in argv)
        commands.append(command)
        if command[:3] == ("git", "rev-parse", "HEAD"):
            return _completed(argv, stdout=args.expected_head + "\n")
        if command[:3] == ("git", "status", "--porcelain=v1"):
            return _completed(argv, stdout="")
        raise AssertionError(f"No validation command may run: {command!r}")

    with pytest.raises(checks.CheckTierError, match="already exists"):
        checks._run_release(args, command_runner=runner)

    assert len(commands) == 2
    assert shadow.read_bytes() == b"pre-existing"
    assert not Path(args.reports_dir).exists()
