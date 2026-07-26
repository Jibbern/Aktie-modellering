from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import shutil
import sys
from typing import Any
from zipfile import ZipFile

import pytest

from pbi_xbrl import new_engine_orchestration as orchestration
from pbi_xbrl import new_engine_promotion as promotion
from pbi_xbrl.new_engine_orchestration import run_plan, validate_workbook_immutable
from pbi_xbrl.new_ticker_value_filler import fill_standard_template_from_package


ROOT = Path(__file__).resolve().parents[1]
RELEASE_GATE_HEAD = "1" * 40


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _anf_package() -> Path:
    for parent in (ROOT, *ROOT.parents):
        output = parent / "StockModelData" / "outputs" / "stress_tests" / "ANF_new_ticker_engine"
        package = output / "ANF_normalized_data_package.json"
        if package.is_file():
            return package
    pytest.fail("ANF normalized package is required for promotion integration testing.")


def _shadow_receipt(path: Path, *, head: str) -> dict[str, Any]:
    return {
        "receipt_version": "new-engine-run/v1",
        "command": "render-shadow",
        "status": "PASS",
        "repo_head": head,
        "contract_profile": {"ticker": "ANF", "profile_id": "full_union"},
        "validations": {
            "pre_fill": {"status": "PASS"},
            "post_fill": {"status": "PASS"},
            "saved_workbook": {"status": "PASS"},
            "excel_native": {"status": "PASS"},
            "acl": {"status": "PASS"},
            "formula_semantics": {"status": "PASS"},
        },
        "output": {
            "path": str(path.resolve()),
            "size": path.stat().st_size,
            "sha256": _sha256(path),
        },
    }


def _integration_context(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    package = _anf_package()
    captured_plan_contexts: list[Any] = []
    build_context = orchestration._build_context

    def capture_context(**kwargs: Any) -> Any:
        context = build_context(**kwargs)
        captured_plan_contexts.append(context)
        return context

    monkeypatch.setattr(orchestration, "_build_context", capture_context)
    plan = run_plan(
        run_dir=tmp_path / "plan",
        package_path=package,
        ticker="ANF",
        profile_id="full_union",
    )
    binding_counts = plan["receipt"]["plans"]["binding"]
    style_counts = plan["receipt"]["plans"]["style"]
    formula_counts = plan["receipt"]["formula_inventory"]
    assert binding_counts["status"] == "PASS"
    assert binding_counts["planned_write_count"] == 22_760
    assert binding_counts["structured_skip_count"] == 2_017
    assert binding_counts["issue_count"] == 761
    assert binding_counts["occurrence_count"] == 2_323
    assert binding_counts["overflow_count"] == 0
    assert binding_counts["blocking_issue_count"] == 0
    assert style_counts["status"] == "PASS"
    assert style_counts["action_count"] == 770
    assert style_counts["decision_count"] == 1_298
    assert formula_counts["cell_formula_count"] == 2_213
    assert len(captured_plan_contexts) == 1
    plan_context = captured_plan_contexts[0]

    def reuse_verified_context(**kwargs: Any) -> Any:
        assert Path(kwargs["package_path"]).resolve() == package.resolve()
        assert str(kwargs["ticker"]).upper() == "ANF"
        assert kwargs["profile_id"] == "full_union"
        return plan_context

    monkeypatch.setattr(orchestration, "_build_context", reuse_verified_context)
    head = RELEASE_GATE_HEAD
    shadow = tmp_path / "ANF_shadow_model_integration.xlsx"
    canonical = tmp_path / "canonical" / "ANF_model.xlsx"
    canonical.parent.mkdir()
    fill_standard_template_from_package(package, output_path=shadow)
    shutil.copyfile(shadow, canonical)
    with ZipFile(canonical, "a") as archive:
        archive.comment = b"previous canonical integration fixture"
    assert _sha256(canonical) != _sha256(shadow)
    shadow_receipt = tmp_path / "shadow.run.json"
    shadow_receipt.write_text(
        json.dumps(_shadow_receipt(shadow, head=head), sort_keys=True),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        promotion,
        "_repository_state",
        lambda: {"head": head, "clean": True, "changes": []},
    )
    common = {
        "package_path": package,
        "ticker": "ANF",
        "profile_id": "full_union",
        "plan_receipt_path": plan["receipt_path"],
        "required_locale_id": 1053,
        "expected_head": head,
    }
    return {
        "common": common,
        "shadow": shadow,
        "shadow_receipt": shadow_receipt,
        "canonical": canonical,
        "rollback_dir": tmp_path / "rollbacks",
    }


def _promote(context: dict[str, Any], *, run_dir: Path) -> dict[str, Any]:
    common = dict(context["common"])
    return promotion.promote_workbook(
        run_dir=run_dir,
        shadow_workbook=context["shadow"],
        shadow_receipt_path=context["shadow_receipt"],
        canonical_workbook=context["canonical"],
        rollback_dir=context["rollback_dir"],
        product_approval_reference="integration:ANF-promotion",
        expected_shadow_sha256=_sha256(context["shadow"]),
        execute=True,
        **common,
    )


def _rollback(
    context: dict[str, Any],
    promoted: dict[str, Any],
    *,
    run_dir: Path,
) -> dict[str, Any]:
    common = dict(context["common"])
    return promotion.rollback_workbook(
        run_dir=run_dir,
        canonical_workbook=context["canonical"],
        rollback_record_path=promoted["rollback_record"],
        expected_rollback_record_sha256=promoted["rollback_record_sha256"],
        product_approval_reference="integration:ANF-rollback",
        execute=True,
        **common,
    )


@pytest.mark.skipif(sys.platform != "win32", reason="Windows ACL and desktop Excel integration is Windows-only")
def test_valid_xlsx_execute_promotion_and_rollback_use_real_strict_saved_validation_and_acl(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _integration_context(tmp_path, monkeypatch)
    canonical = Path(context["canonical"])
    shadow = Path(context["shadow"])
    old_bytes = canonical.read_bytes()
    replacement_observations: list[dict[str, Any]] = []
    isolated_excel_copies: list[Path] = []
    strict_invocations: list[str] = []
    strict_actual: dict[str, dict[str, Any]] = {}
    saved_invocations: list[str] = []
    saved_actual: dict[str, dict[str, Any]] = {}
    base_replace = promotion.replace_existing_atomic
    base_strict = orchestration._strict_post_fill_validation
    base_saved = orchestration._saved_workbook_validation

    def strict_once_per_byte_identity(workbook: Path, *args: Any, **kwargs: Any) -> dict[str, Any]:
        digest = _sha256(Path(workbook))
        strict_invocations.append(digest)
        if digest not in strict_actual:
            strict_actual[digest] = base_strict(workbook, *args, **kwargs)
        return deepcopy(strict_actual[digest])

    def saved_once_per_byte_identity(workbook: Path, ticker: str) -> dict[str, Any]:
        digest = _sha256(Path(workbook))
        saved_invocations.append(digest)
        if digest not in saved_actual:
            saved_actual[digest] = base_saved(workbook, ticker)
        return deepcopy(saved_actual[digest])

    def excel_pass(path: Path, **kwargs: Any) -> dict[str, Any]:
        isolated = Path(path)
        assert isolated.is_file()
        assert isolated not in {canonical, shadow}
        assert kwargs["required_locale_id"] == 1053
        isolated_excel_copies.append(isolated)
        return {
            "status": "PASS",
            "locale_id": 1053,
            "formula_error_count": 0,
            "owned_process_cleanup": "PASS",
            "owned_process_forced_termination": False,
        }

    def observed_replace(candidate: Path, destination: Path) -> None:
        rollback_workbooks = list(Path(context["rollback_dir"]).glob("*.rollback.xlsx"))
        rollback_records = list(Path(context["rollback_dir"]).glob("*.rollback.json"))
        assert len(rollback_workbooks) == 1
        assert len(rollback_records) == 1
        replacement_observations.append(
            {
                "record_sha256": _sha256(rollback_records[0]),
                "rollback_sha256": _sha256(rollback_workbooks[0]),
                "candidate_sha256": _sha256(Path(candidate)),
            }
        )
        base_replace(candidate, destination)

    monkeypatch.setattr(promotion, "replace_existing_atomic", observed_replace)
    monkeypatch.setattr("pbi_xbrl.new_engine_orchestration.run_excel_native_roundtrip", excel_pass)
    monkeypatch.setattr(orchestration, "_strict_post_fill_validation", strict_once_per_byte_identity)
    monkeypatch.setattr(orchestration, "_saved_workbook_validation", saved_once_per_byte_identity)

    promoted = _promote(context, run_dir=tmp_path / "promote")
    assert canonical.read_bytes() == shadow.read_bytes()
    assert promoted["receipt"]["validations"]["canonical"]["status"] == "PASS"
    assert promoted["receipt"]["validations"]["acl"]["status"] == "PASS"
    assert promoted["receipt"]["validations"]["acl"]["sha256_before"] == _sha256(shadow)
    assert promoted["receipt"]["validations"]["acl"]["sha256_after"] == _sha256(shadow)

    rolled_back = _rollback(context, promoted, run_dir=tmp_path / "rollback")
    assert canonical.read_bytes() == old_bytes
    assert rolled_back["receipt"]["validations"]["rollback_source"]["status"] == "PASS"
    assert rolled_back["receipt"]["validations"]["staged_candidate"]["status"] == "PASS"
    assert rolled_back["receipt"]["validations"]["canonical"]["status"] == "PASS"
    assert rolled_back["receipt"]["validations"]["acl"]["sha256_before"] == _sha256(canonical)
    assert rolled_back["receipt"]["validations"]["acl"]["sha256_after"] == _sha256(canonical)
    assert len(replacement_observations) == 2
    assert replacement_observations[0]["rollback_sha256"] == _sha256(canonical)
    assert len(isolated_excel_copies) == 6
    assert all(not path.exists() for path in isolated_excel_copies)
    assert len(strict_invocations) == 12
    assert len(strict_actual) == 2
    assert len(saved_invocations) == 12
    assert len(saved_actual) == 2
    assert not list(canonical.parent.glob(".*.candidate.xlsx"))


@pytest.mark.skipif(sys.platform != "win32", reason="Desktop Excel release validation is Windows-only")
def test_real_swedish_excel_validates_isolated_rollback_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _integration_context(tmp_path, monkeypatch)
    rollback_source = Path(context["canonical"])
    before = _sha256(rollback_source)

    result = validate_workbook_immutable(
        workbook_path=rollback_source,
        run_dir=tmp_path / "excel-validation",
        plan_receipt_path=context["common"]["plan_receipt_path"],
        excel_native="required",
        required_locale_id=1053,
        package_path=context["common"]["package_path"],
        ticker="ANF",
        profile_id="full_union",
    )

    validations = result["receipt"]["validations"]
    assert result["status"] == "PASS"
    assert validations["post_fill"]["status"] == "PASS"
    assert validations["saved_workbook"]["status"] == "PASS"
    assert validations["excel_native"]["status"] == "PASS"
    assert validations["excel_native"]["locale_id"] == 1053
    assert validations["excel_native"]["formula_error_count"] == 0
    assert validations["excel_native"]["owned_process_cleanup"] == "PASS"
    assert validations["excel_post_fill"]["status"] == "PASS"
    assert validations["excel_saved_workbook"]["status"] == "PASS"
    assert _sha256(rollback_source) == before


@pytest.mark.skipif(sys.platform != "win32", reason="Windows ACL integration is Windows-only")
def test_valid_xlsx_rollback_validation_failure_reapplies_promoted_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _integration_context(tmp_path, monkeypatch)
    canonical = Path(context["canonical"])
    validation_calls: list[Path] = []

    def pass_validation(workbook: Path, **_kwargs: Any) -> dict[str, Any]:
        validation_calls.append(Path(workbook))
        return {"status": "PASS", "workbook_sha256": _sha256(Path(workbook))}

    monkeypatch.setattr(promotion, "_validate_release_workbook", pass_validation)
    promoted = _promote(context, run_dir=tmp_path / "promote")
    promoted_bytes = canonical.read_bytes()
    validation_calls.clear()

    def fail_after_rollback_replacement(workbook: Path, **_kwargs: Any) -> dict[str, Any]:
        validation_calls.append(Path(workbook))
        if len(validation_calls) == 3:
            raise promotion.NewEnginePromotionError("injected restored-canonical validation failure")
        return {"status": "PASS", "workbook_sha256": _sha256(Path(workbook))}

    monkeypatch.setattr(promotion, "_validate_release_workbook", fail_after_rollback_replacement)

    with pytest.raises(promotion.NewEnginePromotionError, match="pre-rollback canonical workbook was restored"):
        _rollback(context, promoted, run_dir=tmp_path / "rollback")

    assert canonical.read_bytes() == promoted_bytes
    assert not (tmp_path / "rollback" / "rollback_receipt.json").exists()
    assert not list(canonical.parent.glob(".*.candidate.xlsx"))
