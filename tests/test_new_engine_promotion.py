from __future__ import annotations

import hashlib
import importlib
import json
from pathlib import Path

import pytest


HEAD = "e76f93979c59ab821e30124cd4f28121e275aaae"


def _module():
    return importlib.import_module("pbi_xbrl.new_engine_promotion")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _shadow_receipt(path: Path, *, excel_status: str = "PASS") -> dict[str, object]:
    return {
        "receipt_version": "new-engine-run/v1",
        "command": "render-shadow",
        "status": "PASS",
        "repo_head": HEAD,
        "contract_profile": {"ticker": "ANF", "profile_id": "full_union"},
        "validations": {
            "pre_fill": {"status": "PASS"},
            "post_fill": {"status": "PASS"},
            "saved_workbook": {"status": "PASS"},
            "excel_native": {"status": excel_status},
            "acl": {"status": "PASS"},
            "formula_semantics": {"status": "PASS"},
        },
        "output": {
            "path": str(path.resolve()),
            "size": path.stat().st_size,
            "sha256": _sha256(path),
        },
    }


def _write_receipt(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _common(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    promotion = _module()
    shadow = tmp_path / "ANF_shadow_model_v8.xlsx"
    shadow.write_bytes(b"accepted shadow bytes")
    canonical = tmp_path / "canonical" / "ANF_model.xlsx"
    canonical.parent.mkdir()
    canonical.write_bytes(b"old canonical bytes")
    receipt = _write_receipt(tmp_path / "shadow.run.json", _shadow_receipt(shadow))
    monkeypatch.setattr(
        promotion,
        "_repository_state",
        lambda: {"head": HEAD, "clean": True, "changes": []},
    )

    validation_calls: list[dict[str, object]] = []

    def validate(**kwargs: object) -> dict[str, object]:
        validation_calls.append(kwargs)
        workbook = Path(kwargs["workbook_path"])
        return {
            "status": "PASS",
            "receipt": {
                "validations": {
                    "post_fill": {"status": "PASS"},
                    "saved_workbook": {"status": "PASS"},
                    "excel_native": {"status": "PASS"},
                    "excel_post_fill": {"status": "PASS"},
                    "excel_saved_workbook": {"status": "PASS"},
                },
                "output": {"sha256": _sha256(workbook), "immutable_input": True},
            },
        }

    monkeypatch.setattr(promotion, "validate_workbook_immutable", validate)
    monkeypatch.setattr(
        promotion,
        "normalize_candidate_acl",
        lambda candidate: {
            "status": "PASS",
            "sha256_before": _sha256(Path(candidate)),
            "sha256_after": _sha256(Path(candidate)),
        },
    )
    return {
        "run_dir": tmp_path / "run",
        "shadow_workbook": shadow,
        "shadow_receipt_path": receipt,
        "canonical_workbook": canonical,
        "rollback_dir": tmp_path / "rollbacks",
        "product_approval_reference": "approval:ANF-v8",
        "expected_head": HEAD,
        "expected_shadow_sha256": _sha256(shadow),
        "plan_receipt_path": tmp_path / "plan.run.json",
        "required_locale_id": 1053,
        "package_path": tmp_path / "package.json",
        "ticker": "ANF",
        "profile_id": "full_union",
        "validation_calls": validation_calls,
    }


def _promote_kwargs(values: dict[str, object]) -> dict[str, object]:
    return {key: value for key, value in values.items() if key != "validation_calls"}


def test_promote_defaults_to_dry_run_and_mutates_no_canonical_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    promotion = _module()
    values = _common(tmp_path, monkeypatch)
    canonical = Path(values["canonical_workbook"])
    before = canonical.read_bytes()

    result = promotion.promote_workbook(**_promote_kwargs(values))

    assert result["status"] == "PASS"
    assert result["mode"] == "dry-run"
    assert canonical.read_bytes() == before
    assert not Path(values["rollback_dir"]).exists()
    assert not Path(values["run_dir"]).exists()
    assert len(values["validation_calls"]) == 1
    assert values["validation_calls"][0]["excel_native"] == "required"


@pytest.mark.parametrize("execute", [False, True])
def test_promote_rejects_cross_ticker_canonical_target_before_validation_or_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    execute: bool,
) -> None:
    promotion = _module()
    values = _common(tmp_path, monkeypatch)
    anf_canonical = Path(values["canonical_workbook"])
    wrong_canonical = anf_canonical.with_name("GPRE_model.xlsx")
    anf_canonical.replace(wrong_canonical)
    values["canonical_workbook"] = wrong_canonical
    before = wrong_canonical.read_bytes()

    with pytest.raises(promotion.NewEnginePromotionError, match="does not match ticker 'ANF'"):
        promotion.promote_workbook(execute=execute, **_promote_kwargs(values))

    assert wrong_canonical.read_bytes() == before
    assert values["validation_calls"] == []
    assert not Path(values["rollback_dir"]).exists()
    assert not Path(values["run_dir"]).exists()
    assert not list(wrong_canonical.parent.glob("*.candidate.xlsx"))


def test_promote_execute_creates_byte_exact_rollback_before_replacing_canonical(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    promotion = _module()
    values = _common(tmp_path, monkeypatch)
    canonical = Path(values["canonical_workbook"])
    old_hash = _sha256(canonical)

    result = promotion.promote_workbook(execute=True, **_promote_kwargs(values))

    assert result["status"] == "PASS"
    assert result["mode"] == "execute"
    assert canonical.read_bytes() == Path(values["shadow_workbook"]).read_bytes()
    rollback_copy = Path(result["rollback_workbook"])
    rollback_record = Path(result["rollback_record"])
    assert _sha256(rollback_copy) == old_hash
    record = json.loads(rollback_record.read_text(encoding="utf-8"))
    assert record["canonical"]["sha256"] == old_hash
    assert record["rollback"]["sha256"] == old_hash
    assert record["promoted"]["sha256"] == values["expected_shadow_sha256"]
    assert record["product_approval_reference"] == "approval:ANF-v8"
    assert Path(result["receipt_path"]).is_file()
    assert len(values["validation_calls"]) == 3
    assert all(call["excel_native"] == "required" for call in values["validation_calls"])


def test_post_replace_validation_failure_restores_old_canonical_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    promotion = _module()
    values = _common(tmp_path, monkeypatch)
    canonical = Path(values["canonical_workbook"])
    before = canonical.read_bytes()
    calls = values["validation_calls"]
    base_validate = promotion.validate_workbook_immutable

    def fail_final(**kwargs: object) -> dict[str, object]:
        result = base_validate(**kwargs)
        if len(calls) == 3:
            raise promotion.NewEnginePromotionError("post-promotion validation failed")
        return result

    monkeypatch.setattr(promotion, "validate_workbook_immutable", fail_final)

    with pytest.raises(promotion.NewEnginePromotionError, match="restored"):
        promotion.promote_workbook(execute=True, **_promote_kwargs(values))

    assert canonical.read_bytes() == before
    assert list(Path(values["rollback_dir"]).glob("*.rollback.xlsx"))
    assert not (Path(values["run_dir"]) / "promotion_receipt.json").exists()
    assert not list(canonical.parent.glob("*.candidate.xlsx"))


def test_shadow_receipt_excel_hash_repo_and_approval_gates_fail_before_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    promotion = _module()
    values = _common(tmp_path, monkeypatch)
    canonical = Path(values["canonical_workbook"])
    before = canonical.read_bytes()

    receipt_path = Path(values["shadow_receipt_path"])
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["validations"]["excel_native"]["status"] = "NOT_REQUESTED"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    with pytest.raises(promotion.NewEnginePromotionError, match="excel_native"):
        promotion.promote_workbook(execute=True, **_promote_kwargs(values))

    receipt_path.write_text(json.dumps(_shadow_receipt(Path(values["shadow_workbook"]))), encoding="utf-8")
    malformed = _shadow_receipt(Path(values["shadow_workbook"]))
    malformed["output"]["size"] = "not-a-number"
    receipt_path.write_text(json.dumps(malformed), encoding="utf-8")
    with pytest.raises(promotion.NewEnginePromotionError, match="output.size"):
        promotion.promote_workbook(execute=True, **_promote_kwargs(values))

    receipt_path.write_text(json.dumps(_shadow_receipt(Path(values["shadow_workbook"]))), encoding="utf-8")
    bad_hash = dict(_promote_kwargs(values), expected_shadow_sha256="0" * 64)
    with pytest.raises(promotion.NewEnginePromotionError, match="shadow SHA-256"):
        promotion.promote_workbook(execute=True, **bad_hash)

    dirty = dict(_promote_kwargs(values), product_approval_reference="")
    with pytest.raises(promotion.NewEnginePromotionError, match="approval"):
        promotion.promote_workbook(execute=True, **dirty)

    monkeypatch.setattr(
        promotion,
        "_repository_state",
        lambda: {"head": HEAD, "clean": False, "changes": [" M file.py"]},
    )
    with pytest.raises(promotion.NewEnginePromotionError, match="not clean"):
        promotion.promote_workbook(execute=True, **_promote_kwargs(values))
    assert canonical.read_bytes() == before
    assert not Path(values["rollback_dir"]).exists()


def test_rollback_is_dry_run_by_default_and_execute_restores_recorded_workbook(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    promotion = _module()
    values = _common(tmp_path, monkeypatch)
    promoted = promotion.promote_workbook(execute=True, **_promote_kwargs(values))
    canonical = Path(values["canonical_workbook"])
    promoted_bytes = canonical.read_bytes()

    dry = promotion.rollback_workbook(
        run_dir=tmp_path / "rollback-dry-run",
        canonical_workbook=canonical,
        rollback_record_path=promoted["rollback_record"],
        expected_rollback_record_sha256=promoted["rollback_record_sha256"],
        product_approval_reference="approval:rollback-ANF-v8",
        expected_head=HEAD,
    )
    assert dry["mode"] == "dry-run"
    assert canonical.read_bytes() == promoted_bytes
    assert not (tmp_path / "rollback-dry-run").exists()

    result = promotion.rollback_workbook(
        run_dir=tmp_path / "rollback-run",
        canonical_workbook=canonical,
        rollback_record_path=promoted["rollback_record"],
        expected_rollback_record_sha256=promoted["rollback_record_sha256"],
        product_approval_reference="approval:rollback-ANF-v8",
        expected_head=HEAD,
        execute=True,
    )
    assert result["status"] == "PASS"
    assert canonical.read_bytes() == b"old canonical bytes"
    assert Path(result["receipt_path"]).is_file()


def test_rollback_rejects_unexpected_current_canonical_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    promotion = _module()
    values = _common(tmp_path, monkeypatch)
    promoted = promotion.promote_workbook(execute=True, **_promote_kwargs(values))
    canonical = Path(values["canonical_workbook"])
    canonical.write_bytes(b"unexpected later canonical")

    with pytest.raises(promotion.NewEnginePromotionError, match="current canonical SHA-256"):
        promotion.rollback_workbook(
            run_dir=tmp_path / "rollback-run",
            canonical_workbook=canonical,
            rollback_record_path=promoted["rollback_record"],
            expected_rollback_record_sha256=promoted["rollback_record_sha256"],
            product_approval_reference="approval:rollback-ANF-v8",
            expected_head=HEAD,
            execute=True,
        )
    assert canonical.read_bytes() == b"unexpected later canonical"


def test_promotion_detects_canonical_race_immediately_before_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    promotion = _module()
    values = _common(tmp_path, monkeypatch)
    canonical = Path(values["canonical_workbook"])
    base_validate = promotion.validate_workbook_immutable
    calls = values["validation_calls"]

    def race_after_staged_validation(**kwargs: object) -> dict[str, object]:
        result = base_validate(**kwargs)
        if len(calls) == 2:
            canonical.write_bytes(b"concurrent canonical update")
        return result

    monkeypatch.setattr(promotion, "validate_workbook_immutable", race_after_staged_validation)

    with pytest.raises(promotion.NewEnginePromotionError, match="changed before promotion"):
        promotion.promote_workbook(execute=True, **_promote_kwargs(values))

    assert canonical.read_bytes() == b"concurrent canonical update"
    assert not list(canonical.parent.glob("*.candidate.xlsx"))


def test_rollback_record_requires_independently_supplied_exact_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    promotion = _module()
    values = _common(tmp_path, monkeypatch)
    promoted = promotion.promote_workbook(execute=True, **_promote_kwargs(values))
    canonical = Path(values["canonical_workbook"])
    before = canonical.read_bytes()

    with pytest.raises(promotion.NewEnginePromotionError, match="rollback record SHA-256"):
        promotion.rollback_workbook(
            run_dir=tmp_path / "rollback-run",
            canonical_workbook=canonical,
            rollback_record_path=promoted["rollback_record"],
            expected_rollback_record_sha256="0" * 64,
            product_approval_reference="approval:rollback-ANF-v8",
            expected_head=HEAD,
            execute=True,
        )
    assert canonical.read_bytes() == before
