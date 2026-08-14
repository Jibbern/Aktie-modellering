from __future__ import annotations

import os
from types import SimpleNamespace

import pandas as pd
import pytest
from openpyxl import Workbook

import stock_models
from pbi_xbrl.excel_writer import (
    _atomic_publish_workbook,
    _validate_serialized_workbook_structure,
)
from pbi_xbrl.excel_writer_core import WorkbookFinalizationError, finalize_workbook


def _context() -> SimpleNamespace:
    wb = Workbook()
    wb.active.title = "SUMMARY"
    return SimpleNamespace(
        state={"signals_base_df": pd.DataFrame()},
        wb=wb,
        derived=SimpleNamespace(signals_base_df=pd.DataFrame()),
        data=SimpleNamespace(extra_values={"runtime_sheet_states": {}}),
        desired_sheet_order=("SUMMARY",),
        raw_sheet_cluster=(),
        callbacks=SimpleNamespace(extra_callbacks={}),
    )


def test_runtime_sheet_state_failure_blocks_candidate_save(tmp_path) -> None:
    ctx = _context()
    ctx.data.extra_values["runtime_sheet_states"] = {"SUMMARY": "sometimes-visible"}
    save_calls: list[object] = []

    with pytest.raises(WorkbookFinalizationError, match="runtime_sheet_visibility"):
        finalize_workbook(ctx)

    assert save_calls == []
    assert not (tmp_path / "candidate.xlsx").exists()


def test_required_ordering_failure_is_material() -> None:
    class BrokenOrder:
        def __iter__(self):
            raise RuntimeError("order contract exploded")

    ctx = _context()
    ctx.desired_sheet_order = BrokenOrder()

    with pytest.raises(WorkbookFinalizationError) as caught:
        finalize_workbook(ctx)

    assert caught.value.stage == "declared_sheet_order"
    assert isinstance(caught.value.__cause__, RuntimeError)
    assert "order contract exploded" in str(caught.value.__cause__)


def test_required_cleanup_failure_is_material() -> None:
    ctx = _context()

    def fail_cleanup() -> None:
        raise ValueError("cleanup root cause")

    ctx.callbacks.extra_callbacks["_final_promise_progress_cleanup"] = fail_cleanup

    with pytest.raises(WorkbookFinalizationError) as caught:
        finalize_workbook(ctx)

    assert caught.value.stage == "promise_progress_cleanup_pre_polish"
    assert isinstance(caught.value.__cause__, ValueError)
    assert str(caught.value.__cause__) == "cleanup root cause"


def test_calculation_finalization_failure_is_material() -> None:
    class BrokenCalculation:
        def __setattr__(self, name: str, value: object) -> None:
            raise RuntimeError("calculation contract exploded")

    ctx = _context()
    ctx.wb.calculation = BrokenCalculation()

    with pytest.raises(WorkbookFinalizationError) as caught:
        finalize_workbook(ctx)

    assert caught.value.stage == "calculation_settings"
    assert "calculation contract exploded" in str(caught.value.__cause__)


def test_in_memory_structural_validation_is_material() -> None:
    ctx = _context()
    ctx.data.extra_values["runtime_sheet_states"] = {"SUMMARY": "hidden"}

    with pytest.raises(WorkbookFinalizationError) as caught:
        finalize_workbook(ctx)

    assert caught.value.stage == "in_memory_structural_validation"
    assert "no visible worksheet" in str(caught.value.__cause__)


def test_explicit_optional_enrichment_records_warning_and_continues() -> None:
    class BrokenColumnDimensions:
        def __getitem__(self, key: str) -> object:
            raise RuntimeError(f"width enrichment failed for {key}")

    ctx = _context()
    ctx.wb["SUMMARY"].column_dimensions = BrokenColumnDimensions()

    finalize_workbook(ctx)

    assert ctx.state["workbook_finalization_warnings"] == [
        {
            "stage": "summary_column_width_enrichment",
            "material": False,
            "error_type": "RuntimeError",
            "message": "width enrichment failed for A",
        }
    ]


def test_successful_finalization_saves_one_candidate_then_publishes(tmp_path) -> None:
    ctx = _context()
    finalize_workbook(ctx)
    final_path = tmp_path / "model.xlsx"
    save_calls: list[object] = []

    def save_once(candidate_path) -> None:
        save_calls.append(candidate_path)
        ctx.wb.save(candidate_path)

    _atomic_publish_workbook(
        final_path,
        write_candidate=save_once,
        validate_candidate=lambda path: _validate_serialized_workbook_structure(path, ctx),
    )

    assert len(save_calls) == 1
    assert final_path.is_file()
    assert not list(tmp_path.glob(".model.*.xlsx"))


def test_failed_saved_structure_validation_preserves_prior_final(tmp_path) -> None:
    final_path = tmp_path / "model.xlsx"
    final_path.write_bytes(b"prior-accepted-output")

    with pytest.raises(WorkbookFinalizationError) as caught:
        _atomic_publish_workbook(
            final_path,
            write_candidate=lambda path: path.write_bytes(b"candidate"),
            validate_candidate=lambda path: (_ for _ in ()).throw(ValueError("structure mismatch")),
        )

    assert caught.value.stage == "saved_workbook_structural_validation"
    assert isinstance(caught.value.__cause__, ValueError)
    assert final_path.read_bytes() == b"prior-accepted-output"
    assert not list(tmp_path.glob(".model.*.xlsx"))


def test_cli_promotion_validates_before_atomic_replace(tmp_path, monkeypatch) -> None:
    candidate = tmp_path / "candidate.xlsx"
    final_path = tmp_path / "final.xlsx"
    candidate.write_bytes(b"new-candidate")
    final_path.write_bytes(b"prior-final")

    def reject(*args, **kwargs):
        raise RuntimeError("readback mismatch")

    monkeypatch.setattr(stock_models, "_verify_workbook_candidate", reject)

    with pytest.raises(RuntimeError, match="readback mismatch"):
        stock_models._atomic_promote_verified_workbook(
            candidate,
            final_path,
            object(),
            quarter_notes_audit=False,
        )

    assert final_path.read_bytes() == b"prior-final"
    assert candidate.read_bytes() == b"new-candidate"


def test_cli_promotion_uses_atomic_replace_after_validation(tmp_path, monkeypatch) -> None:
    candidate = tmp_path / "candidate.xlsx"
    final_path = tmp_path / "final.xlsx"
    candidate.write_bytes(b"verified-candidate")
    final_path.write_bytes(b"prior-final")
    observed: list[tuple[object, ...]] = []

    monkeypatch.setattr(
        stock_models,
        "_verify_workbook_candidate",
        lambda *args, **kwargs: observed.append(args) or {"workbook_path": str(candidate)},
    )

    provenance = stock_models._atomic_promote_verified_workbook(
        candidate,
        final_path,
        object(),
        quarter_notes_audit=False,
    )

    assert observed
    assert not candidate.exists()
    assert final_path.read_bytes() == b"verified-candidate"
    assert provenance["workbook_path"] == str(final_path.resolve())


def test_atomic_publication_error_retains_root_cause(tmp_path, monkeypatch) -> None:
    final_path = tmp_path / "model.xlsx"
    final_path.write_bytes(b"prior")

    def reject_replace(source, destination) -> None:
        raise PermissionError("destination locked")

    monkeypatch.setattr(os, "replace", reject_replace)

    with pytest.raises(WorkbookFinalizationError) as caught:
        _atomic_publish_workbook(
            final_path,
            write_candidate=lambda path: path.write_bytes(b"candidate"),
            validate_candidate=lambda path: None,
        )

    assert caught.value.stage == "atomic_workbook_publication"
    assert isinstance(caught.value.__cause__, PermissionError)
    assert "destination locked" in str(caught.value.__cause__)
    assert final_path.read_bytes() == b"prior"
