from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pytest

import stock_models
from pbi_xbrl.data_portability import (
    TICKERS,
    cleanup_old_layout,
    main as portability_main,
    migrate_legacy_layout,
    restore_snapshot,
    snapshot_data_root,
    validate_data_root,
)


def _write(path: Path, text: str = "x") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def test_data_portability_default_tickers_include_gtx() -> None:
    assert TICKERS == ("PBI", "GPRE", "ANF", "GTX")


def test_migrate_maps_legacy_folders_into_portable_layout_and_skips_temp_files(tmp_path: Path) -> None:
    repo_root = tmp_path / "Aktier"
    data_root = tmp_path / "StockModelData"
    _write(repo_root / "sec_cache" / "PBI" / "submissions.json", "{}")
    _write(repo_root / "sec_cache" / "ANF" / "companyfacts_0001018840.json", "{}")
    _write(repo_root / "sec_cache" / "market_data" / "parsed" / "exports" / "GPRE.parquet", "market")
    _write(repo_root / "PBI" / "earnings_transcripts" / "pbi.txt", "pbi")
    _write(repo_root / "ANF" / "earnings_release" / "8-K_2019-03-07_earnings_release.htm", "anf")
    _write(repo_root / "PBI" / "__pycache__" / "skip.pyc", "skip")
    _write(repo_root / "ANF" / "press_release" / "~$temp.xlsx", "skip")
    _write(repo_root / "GPRE" / "basis_proxy" / "gpre_basis_proxy_summary.md", "basis")
    _write(repo_root / "writer_cache" / "writer.pkl", "cache")
    _write(repo_root / "Excel stock models" / "PBI_model.xlsx", "excel")

    report = migrate_legacy_layout(repo_root=repo_root, data_root=data_root)

    assert (data_root / "sec_cache" / "PBI" / "submissions.json").exists()
    assert (data_root / "market_cache" / "parsed" / "exports" / "GPRE.parquet").exists()
    assert (data_root / "tickers" / "PBI" / "earnings_transcripts" / "pbi.txt").exists()
    assert (data_root / "tickers" / "ANF" / "earnings_release" / "8-K_2019-03-07_earnings_release.htm").exists()
    assert (data_root / "sec_cache" / "ANF" / "companyfacts_0001018840.json").exists()
    assert (data_root / "basis_proxy" / "gpre_basis_proxy_summary.md").exists()
    assert (data_root / "writer_cache" / "writer.pkl").exists()
    assert (data_root / "outputs" / "Excel stock models" / "PBI_model.xlsx").exists()
    assert not (data_root / "tickers" / "PBI" / "__pycache__" / "skip.pyc").exists()
    assert not (data_root / "tickers" / "ANF" / "press_release" / "~$temp.xlsx").exists()
    assert report["copied_files"] >= 6
    assert report["total_bytes_copied"] > 0
    assert report["elapsed_seconds"] >= 0
    assert (data_root / "logs" / "data_migration_report.json").exists()


def test_migrate_reports_conflict_when_same_size_file_differs(tmp_path: Path) -> None:
    repo_root = tmp_path / "Aktier"
    data_root = tmp_path / "StockModelData"
    _write(repo_root / "PBI" / "press_release" / "release.txt", "abc")
    _write(data_root / "tickers" / "PBI" / "press_release" / "release.txt", "xyz")

    report = migrate_legacy_layout(repo_root=repo_root, data_root=data_root)

    assert report["conflict_count"] == 1
    assert (data_root / "tickers" / "PBI" / "press_release" / "release.txt").read_text(encoding="utf-8") == "xyz"


def test_snapshot_creates_zip_with_manifest_and_excludes_render_checks_by_default(tmp_path: Path) -> None:
    data_root = tmp_path / "StockModelData"
    _write(data_root / "sec_cache" / "PBI" / "submissions.json", "{}")
    _write(data_root / "tickers" / "PBI" / "press_release" / "release.txt", "release")
    _write(data_root / "render_checks" / "old" / "image.png", "render")
    _write(data_root / "logs" / "run.log", "log")
    _write(data_root / "validation_reports" / "workbook_validation" / "workbook_validation_report.json", "[]")
    out_zip = tmp_path / "OneDrive" / "StockModelData_snapshot.zip"

    report = snapshot_data_root(data_root=data_root, out_path=out_zip)

    assert out_zip.exists()
    with zipfile.ZipFile(out_zip) as zf:
        names = set(zf.namelist())
        assert "StockModelData_snapshot_manifest.json" in names
        assert "sec_cache/PBI/submissions.json" in names
        assert "tickers/PBI/press_release/release.txt" in names
        assert "render_checks/old/image.png" not in names
        assert "logs/run.log" not in names
        assert "validation_reports/workbook_validation/workbook_validation_report.json" in names
        manifest = json.loads(zf.read("StockModelData_snapshot_manifest.json").decode("utf-8"))
    assert manifest["file_count"] == report["file_count"]
    assert "render_checks" in manifest["excluded_roots"]


def test_restore_refuses_existing_data_root_without_overwrite_and_validates_layout(tmp_path: Path) -> None:
    data_root = tmp_path / "StockModelData"
    _write(data_root / "sec_cache" / "PBI" / "submissions.json", "{}")
    snapshot_path = tmp_path / "snapshot.zip"
    snapshot_data_root(data_root=data_root, out_path=snapshot_path)

    restore_root = tmp_path / "restore"
    _write(restore_root / "existing.txt", "do not overwrite")
    with pytest.raises(RuntimeError, match="already exists"):
        restore_snapshot(snapshot_path=snapshot_path, data_root=restore_root)

    report = restore_snapshot(snapshot_path=snapshot_path, data_root=restore_root, overwrite=True)
    assert (restore_root / "sec_cache" / "PBI" / "submissions.json").exists()
    assert report["required_folders_ok"] is True


def test_restore_dry_run_validates_manifest_without_extracting(tmp_path: Path) -> None:
    data_root = tmp_path / "StockModelData"
    _write(data_root / "sec_cache" / "PBI" / "submissions.json", "{}")
    snapshot_path = tmp_path / "snapshot.zip"
    snapshot_data_root(data_root=data_root, out_path=snapshot_path)

    restore_root = tmp_path / "restore_dry"
    report = restore_snapshot(snapshot_path=snapshot_path, data_root=restore_root, dry_run=True)

    assert report["dry_run"] is True
    assert not restore_root.exists()


def test_validate_data_root_writes_gate_report(tmp_path: Path) -> None:
    data_root = tmp_path / "StockModelData"
    for ticker in ("PBI", "GPRE", "ANF", "GTX"):
        _write(data_root / "sec_cache" / ticker / "submissions.json", "{}")
        _write(data_root / "tickers" / ticker / "press_release" / "release.txt", "release")
        _write(data_root / "outputs" / "Excel stock models" / f"{ticker}_model.xlsx", "excel")
    for folder in ("market_cache", "writer_cache", "basis_proxy", "logs"):
        (data_root / folder).mkdir(parents=True, exist_ok=True)

    report = validate_data_root(data_root=data_root, run_workbook_validation=False)

    assert report["overall"] == "PASS"
    assert (data_root / "logs" / "data_root_validation_report.json").exists()


def test_validate_data_root_accepts_gtx_xlsx_without_requiring_xlsm(tmp_path: Path) -> None:
    data_root = tmp_path / "StockModelData"
    for ticker in ("PBI", "GPRE", "ANF"):
        _write(data_root / "sec_cache" / ticker / "submissions.json", "{}")
        _write(data_root / "tickers" / ticker / "press_release" / "release.txt", "release")
        _write(data_root / "outputs" / "Excel stock models" / f"{ticker}_model.xlsm", "excel")
    _write(data_root / "sec_cache" / "GTX" / "submissions.json", "{}")
    _write(data_root / "tickers" / "GTX" / "press_release" / "release.txt", "release")
    _write(data_root / "outputs" / "Excel stock models" / "GTX_model.xlsx", "excel")
    for folder in ("market_cache", "writer_cache", "basis_proxy", "logs"):
        (data_root / folder).mkdir(parents=True, exist_ok=True)

    report = validate_data_root(data_root=data_root, run_workbook_validation=False)

    assert report["overall"] == "PASS"
    assert report["missing_workbooks"] == []


def test_cleanup_old_dry_run_lists_expected_legacy_paths_and_refuses_confirm_without_guards(tmp_path: Path) -> None:
    repo_root = tmp_path / "Aktier"
    data_root = tmp_path / "StockModelData"
    _write(repo_root / "sec_cache" / "PBI" / "submissions.json", "{}")
    _write(repo_root / "PBI" / "press_release" / "release.txt", "release")
    _write(repo_root / "Excel stock models" / "PBI_model.xlsx", "excel")
    data_root.mkdir(parents=True)

    dry = cleanup_old_layout(repo_root=repo_root, data_root=data_root, dry_run=True)

    listed = set(dry["candidates"])
    assert str((repo_root / "sec_cache").resolve()) in listed
    assert str((repo_root / "PBI").resolve()) in listed
    assert str((repo_root / "Excel stock models").resolve()) in listed
    assert str(data_root.resolve()) not in listed
    assert (data_root / "logs" / "data_cleanup_dry_run_report.json").exists()

    with pytest.raises(RuntimeError, match="migration report"):
        cleanup_old_layout(repo_root=repo_root, data_root=data_root, dry_run=False, confirm=True)


def test_cleanup_old_confirm_requires_snapshot_workbooks_and_validation(tmp_path: Path) -> None:
    repo_root = tmp_path / "Aktier"
    data_root = tmp_path / "StockModelData"
    _write(data_root / "logs" / "data_migration_report.json", "{}")
    _write(repo_root / "PBI" / "press_release" / "release.txt", "release")

    cleanup_old_layout(repo_root=repo_root, data_root=data_root, dry_run=True)
    with pytest.raises(RuntimeError, match="snapshot backup"):
        cleanup_old_layout(repo_root=repo_root, data_root=data_root, dry_run=False, confirm=True)

    _write(data_root / "logs" / "data_snapshot_report.json", json.dumps({"out_path": str(tmp_path / "snapshot.zip")}))
    _write(tmp_path / "snapshot.zip", "zip-placeholder")
    with pytest.raises(RuntimeError, match="workbook outputs missing"):
        cleanup_old_layout(repo_root=repo_root, data_root=data_root, dry_run=False, confirm=True)

    for ticker in ("PBI", "GPRE", "ANF", "GTX"):
        _write(data_root / "outputs" / "Excel stock models" / f"{ticker}_model.xlsx", "excel")
    with pytest.raises(RuntimeError, match="validation PASS"):
        cleanup_old_layout(repo_root=repo_root, data_root=data_root, dry_run=False, confirm=True)


def test_cleanup_old_archive_requires_prior_dry_run_and_archives_after_gates(tmp_path: Path) -> None:
    repo_root = tmp_path / "Aktier"
    data_root = tmp_path / "StockModelData"
    _write(repo_root / "PBI" / "press_release" / "release.txt", "release")
    _write(data_root / "logs" / "data_migration_report.json", "{}")
    _write(data_root / "logs" / "data_snapshot_report.json", json.dumps({"out_path": str(tmp_path / "snapshot.zip")}))
    _write(tmp_path / "snapshot.zip", "zip-placeholder")
    for ticker in ("PBI", "GPRE", "ANF", "GTX"):
        _write(data_root / "outputs" / "Excel stock models" / f"{ticker}_model.xlsx", "excel")
    validation_rows = [{"ticker": ticker, "overall": "PASS"} for ticker in ("PBI", "GPRE", "ANF", "GTX")]
    _write(data_root / "validation_reports" / "workbook_validation" / "workbook_validation_report.json", json.dumps(validation_rows))

    with pytest.raises(RuntimeError, match="dry-run"):
        cleanup_old_layout(repo_root=repo_root, data_root=data_root, dry_run=False, confirm=True, archive=True)

    cleanup_old_layout(repo_root=repo_root, data_root=data_root, dry_run=True)
    report = cleanup_old_layout(repo_root=repo_root, data_root=data_root, dry_run=False, confirm=True, archive=True)

    assert report["action"] == "archived"
    assert not (repo_root / "PBI").exists()
    assert Path(report["archive_root"]).exists()


def test_stock_models_data_subcommand_dispatches_to_portable_tools(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    data_root = tmp_path / "StockModelData"
    out_zip = tmp_path / "snapshot.zip"
    called: dict[str, object] = {}

    def _fake_main(argv: list[str], *, repo_root: Path) -> int:
        called["argv"] = list(argv)
        called["repo_root"] = repo_root
        return 0

    monkeypatch.setattr("pbi_xbrl.data_portability.main", _fake_main)
    monkeypatch.setattr("sys.argv", ["stock_models.py", "data", "snapshot", "--data-root", str(data_root), "--out", str(out_zip)])

    with pytest.raises(SystemExit) as exc_info:
        stock_models.main()

    assert exc_info.value.code == 0
    assert called["argv"] == ["snapshot", "--data-root", str(data_root), "--out", str(out_zip)]
    assert Path(called["repo_root"]).name == "Aktier"


def test_data_config_show_set_and_clear_root_cli(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    repo_root = tmp_path / "Aktier"
    data_root = tmp_path / "StockModelData"
    _write(data_root / "sec_cache" / "ANF" / "submissions.json", "{}")

    assert portability_main(["config", "set-root", str(data_root)], repo_root=repo_root) == 0
    set_payload = json.loads(capsys.readouterr().out)
    assert set_payload["data_root"] == str(data_root.resolve())
    assert set_payload["data_root_source"] == "config"

    assert portability_main(["config", "show"], repo_root=repo_root) == 0
    show_payload = json.loads(capsys.readouterr().out)
    assert show_payload["paths"]["excel_output_dir"] == str(data_root.resolve() / "outputs" / "Excel stock models")

    assert portability_main(["config", "clear-root"], repo_root=repo_root) == 0
    clear_payload = json.loads(capsys.readouterr().out)
    assert clear_payload["data_root_source"] == "legacy"


def test_data_config_set_root_refuses_onedrive_without_explicit_allow(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo_root = tmp_path / "Aktier"
    data_root = tmp_path / "OneDrive" / "StockModelData"
    _write(data_root / "sec_cache" / "ANF" / "submissions.json", "{}")

    with pytest.raises(RuntimeError, match="OneDrive"):
        portability_main(["config", "set-root", str(data_root)], repo_root=repo_root)

    assert portability_main(["config", "set-root", str(data_root), "--allow-onedrive-data-root"], repo_root=repo_root) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["data_root"] == str(data_root.resolve())
