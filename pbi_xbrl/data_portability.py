"""Portable StockModelData migration, snapshot, restore, and cleanup helpers."""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from . import __version__
from .path_config import (
    StockModelPathConfig,
    clear_config_data_root,
    light_data_root_check,
    resolve_effective_data_root,
    resolve_stock_model_paths,
    write_config_data_root,
)

SNAPSHOT_MANIFEST_NAME = "StockModelData_snapshot_manifest.json"
MIGRATION_REPORT_NAME = "data_migration_report.json"
SNAPSHOT_REPORT_NAME = "data_snapshot_report.json"
CLEANUP_REPORT_NAME = "data_cleanup_report.json"
CLEANUP_DRY_RUN_REPORT_NAME = "data_cleanup_dry_run_report.json"
DATA_ROOT_VALIDATION_REPORT_NAME = "data_root_validation_report.json"
TICKERS: Tuple[str, ...] = ("PBI", "GPRE", "ANF")
EXCEL_OUTPUT_FOLDER = "Excel stock models"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _safe_resolve(path: Path | str) -> Path:
    return Path(path).expanduser().resolve()


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except Exception:
        return False


def _git_commit(repo_root: Path) -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(repo_root / "Code" if (repo_root / "Code").exists() else repo_root),
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return ""


def _skip_path(path: Path) -> Optional[str]:
    parts = {str(part).strip().lower() for part in path.parts}
    name = path.name.strip()
    low = name.lower()
    if "__pycache__" in parts:
        return "__pycache__"
    if ".pytest_cache" in parts:
        return ".pytest_cache"
    if ".mypy_cache" in parts:
        return ".mypy_cache"
    if name.startswith("~$"):
        return "excel_temp"
    if "conflicted copy" in low or "onedrive" in low and "conflict" in low:
        return "sync_conflict_temp"
    if low == ".tmp" or low.endswith(".tmp"):
        return "tmp"
    return None


def _same_file_enough(src: Path, dst: Path) -> bool:
    try:
        if src.stat().st_size != dst.stat().st_size:
            return False
        return _sha256_file(src) == _sha256_file(dst)
    except Exception:
        return False


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _copy_tree(
    src: Path,
    dst: Path,
    *,
    exclude_top_level: Iterable[str] = (),
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "source": str(src),
        "destination": str(dst),
        "copied_files": 0,
        "skipped_files": 0,
        "conflicts": [],
        "excluded_files": [],
        "bytes_copied": 0,
    }
    exclude_names = {str(x or "").strip().lower() for x in exclude_top_level if str(x or "").strip()}
    if not src.exists():
        result["missing"] = True
        return result
    for item in src.rglob("*"):
        if not item.is_file():
            continue
        rel = item.relative_to(src)
        if rel.parts and str(rel.parts[0]).strip().lower() in exclude_names:
            result["excluded_files"].append({"path": str(item), "reason": f"excluded root {rel.parts[0]}"})
            continue
        skip_reason = _skip_path(item)
        if skip_reason:
            result["excluded_files"].append({"path": str(item), "reason": skip_reason})
            continue
        target = dst / rel
        if target.exists():
            if _same_file_enough(item, target):
                result["skipped_files"] += 1
                continue
            result["conflicts"].append({"source": str(item), "destination": str(target), "reason": "destination exists with different size"})
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(item, target)
        result["copied_files"] += 1
        try:
            result["bytes_copied"] += item.stat().st_size
        except Exception:
            pass
    return result


def _legacy_migration_mappings(repo_root: Path, paths: StockModelPathConfig) -> List[Dict[str, Any]]:
    mappings: List[Dict[str, Any]] = [
        {
            "label": "sec_cache",
            "source": repo_root / "sec_cache",
            "destination": paths.sec_cache_dir,
            "exclude_top_level": ("market_data",),
        },
        {
            "label": "market_cache",
            "source": repo_root / "sec_cache" / "market_data",
            "destination": paths.market_cache_dir,
        },
        {
            "label": "writer_cache",
            "source": repo_root / "writer_cache",
            "destination": paths.writer_cache_dir,
        },
        {
            "label": "basis_proxy",
            "source": repo_root / "GPRE" / "basis_proxy",
            "destination": paths.basis_proxy_dir,
        },
        {
            "label": "excel_outputs",
            "source": repo_root / EXCEL_OUTPUT_FOLDER,
            "destination": paths.excel_output_dir,
        },
        {
            "label": "render_checks",
            "source": repo_root / "render_checks",
            "destination": paths.render_checks_dir,
        },
        {
            "label": "validation_reports",
            "source": repo_root / "Code" / "validation_reports",
            "destination": paths.validation_reports_dir,
        },
        {
            "label": "logs",
            "source": repo_root / "logs",
            "destination": paths.logs_dir,
        },
    ]
    for ticker in TICKERS:
        exclude = ("basis_proxy",) if ticker == "GPRE" else ()
        mappings.append(
            {
                "label": f"ticker_{ticker}",
                "source": repo_root / ticker,
                "destination": paths.ticker_dir(ticker),
                "exclude_top_level": exclude,
            }
        )
    return mappings


def migrate_legacy_layout(
    *,
    repo_root: Path | str,
    data_root: Path | str,
) -> Dict[str, Any]:
    started = time.perf_counter()
    repo = _safe_resolve(repo_root)
    paths = resolve_stock_model_paths(repo, data_root)
    paths.ensure_runtime_dirs()
    report: Dict[str, Any] = {
        "operation": "migrate",
        "created_at": _utc_now(),
        "repo_root": str(repo),
        "data_root": str(paths.data_root),
        "copied_files": 0,
        "skipped_files": 0,
        "conflict_count": 0,
        "total_bytes_copied": 0,
        "elapsed_seconds": 0.0,
        "conflicts": [],
        "missing_source_folders": [],
        "destination_folders": [],
        "mappings": [],
    }
    for mapping in _legacy_migration_mappings(repo, paths):
        src = Path(mapping["source"])
        dst = Path(mapping["destination"])
        report["destination_folders"].append(str(dst))
        if not src.exists():
            report["missing_source_folders"].append(str(src))
            continue
        child = _copy_tree(src, dst, exclude_top_level=mapping.get("exclude_top_level", ()))
        child["label"] = mapping["label"]
        report["mappings"].append(child)
        report["copied_files"] += int(child.get("copied_files") or 0)
        report["skipped_files"] += int(child.get("skipped_files") or 0)
        report["total_bytes_copied"] += int(child.get("bytes_copied") or 0)
        report["conflicts"].extend(child.get("conflicts") or [])
    report["conflict_count"] = len(report["conflicts"])
    report["elapsed_seconds"] = round(time.perf_counter() - started, 3)
    report_path = paths.logs_dir / MIGRATION_REPORT_NAME
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    report["report_path"] = str(report_path)
    return report


def _snapshot_roots(
    data_root: Path,
    *,
    include_renders: bool = False,
    include_logs: bool = False,
    include_validation_reports: bool = False,
) -> List[Path]:
    names = ["sec_cache", "tickers", "market_cache", "writer_cache", "basis_proxy", "outputs"]
    if include_renders:
        names.append("render_checks")
    if include_validation_reports:
        names.append("validation_reports")
    if include_logs:
        names.append("logs")
    return [data_root / name for name in names]


def _iter_snapshot_files(roots: Iterable[Path]) -> Tuple[List[Path], List[Dict[str, str]]]:
    files: List[Path] = []
    excluded: List[Dict[str, str]] = []
    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            reason = _skip_path(path)
            if reason:
                excluded.append({"path": str(path), "reason": reason})
                continue
            files.append(path)
    return files, excluded


def _latest_validation_report_files(data_root: Path) -> List[Path]:
    root = data_root / "validation_reports" / "workbook_validation"
    return [
        path
        for path in (
            root / "workbook_validation_report.json",
            root / "workbook_validation_summary.csv",
        )
        if path.exists() and path.is_file()
    ]


def _preferred_workbook_path(excel_dir: Path, ticker: str) -> Path:
    ticker_u = str(ticker).upper()
    candidates = [
        excel_dir / f"{ticker_u}_model.xlsx",
        excel_dir / f"{ticker_u}_model.xlsm",
    ]
    existing = [path for path in candidates if path.exists() and path.is_file()]
    if not existing:
        return candidates[0]
    return max(existing, key=lambda path: path.stat().st_mtime)


def _preferred_workbook_paths(excel_dir: Path) -> Dict[str, Path]:
    return {ticker: _preferred_workbook_path(excel_dir, ticker) for ticker in TICKERS}


def _read_validation_status(data_root: Path) -> Dict[str, Any]:
    path = data_root / "validation_reports" / "workbook_validation" / "workbook_validation_report.json"
    if not path.exists():
        return {"available": False}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"available": False, "error": f"{type(exc).__name__}: {exc}"}
    if not isinstance(payload, list):
        return {"available": False, "error": "unexpected validation report shape"}
    rows = {
        str(row.get("ticker") or "").upper(): str(row.get("overall") or "")
        for row in payload
        if isinstance(row, dict)
    }
    return {"available": True, "overall_by_ticker": rows, "passes": all(rows.get(ticker) == "PASS" for ticker in TICKERS)}


def snapshot_data_root(
    *,
    data_root: Path | str,
    out_path: Path | str,
    include_renders: bool = False,
    include_logs: bool = False,
    include_validation_reports: bool = False,
    dry_run: bool = False,
    overwrite: bool = False,
    repo_root: Path | str | None = None,
) -> Dict[str, Any]:
    root = _safe_resolve(data_root)
    out = _safe_resolve(out_path)
    repo = _safe_resolve(repo_root) if repo_root is not None else Path(__file__).resolve().parents[2]
    if not root.exists():
        raise RuntimeError(f"data_root does not exist: {root}")
    roots = _snapshot_roots(
        root,
        include_renders=include_renders,
        include_logs=include_logs,
        include_validation_reports=include_validation_reports,
    )
    files, excluded_files = _iter_snapshot_files(roots)
    if not include_validation_reports:
        for path in _latest_validation_report_files(root):
            if path not in files and not _skip_path(path):
                files.append(path)
    total_size = 0
    for file in files:
        try:
            total_size += file.stat().st_size
        except Exception:
            pass
    excluded_roots = [
        name
        for name, included in (
            ("render_checks", include_renders),
            ("validation_reports", include_validation_reports),
            ("logs", include_logs),
        )
        if not included
    ]
    workbook_outputs = sorted(
        path.name
        for pattern in ("*_model.xlsx", "*_model.xlsm")
        for path in (root / "outputs" / EXCEL_OUTPUT_FOLDER).glob(pattern)
        if path.is_file()
    ) if (root / "outputs" / EXCEL_OUTPUT_FOLDER).exists() else []
    validation_status = _read_validation_status(root)
    manifest: Dict[str, Any] = {
        "snapshot_version": 1,
        "created_at": _utc_now(),
        "code_version": __version__,
        "git_commit": _git_commit(repo),
        "data_root_name": root.name,
        "included_folders": [str(path.relative_to(root)) for path in roots if path.exists()],
        "excluded_roots": excluded_roots,
        "excluded_files": excluded_files,
        "file_count": len(files),
        "total_size_bytes": total_size,
        "workbook_outputs": workbook_outputs,
        "validation_status": validation_status,
    }
    report = {
        "operation": "snapshot",
        "dry_run": bool(dry_run),
        "out_path": str(out),
        "file_count": len(files),
        "total_size_bytes": total_size,
        "manifest": manifest,
    }
    if dry_run:
        return report
    if out.exists() and not overwrite:
        raise RuntimeError(f"snapshot already exists: {out}")
    out.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        zf.writestr(SNAPSHOT_MANIFEST_NAME, json.dumps(manifest, indent=2, ensure_ascii=False))
        for file in files:
            zf.write(file, file.relative_to(root).as_posix())
    logs_dir = root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    snapshot_report = dict(report)
    snapshot_report["manifest"] = manifest
    (logs_dir / SNAPSHOT_REPORT_NAME).write_text(json.dumps(snapshot_report, indent=2, ensure_ascii=False), encoding="utf-8")
    return report


def _zip_members_are_safe(zf: zipfile.ZipFile) -> None:
    for info in zf.infolist():
        member = Path(info.filename)
        if member.is_absolute() or ".." in member.parts:
            raise RuntimeError(f"unsafe snapshot member path: {info.filename}")


def restore_snapshot(
    *,
    snapshot_path: Path | str,
    data_root: Path | str,
    overwrite: bool = False,
    dry_run: bool = False,
    validate: bool = True,
) -> Dict[str, Any]:
    snapshot = _safe_resolve(snapshot_path)
    root = _safe_resolve(data_root)
    if not snapshot.exists():
        raise RuntimeError(f"snapshot not found: {snapshot}")
    if root.exists() and any(root.iterdir()) and not overwrite:
        raise RuntimeError(f"restore data_root already exists and is not empty: {root}")
    with zipfile.ZipFile(snapshot) as zf:
        _zip_members_are_safe(zf)
        if SNAPSHOT_MANIFEST_NAME not in zf.namelist():
            raise RuntimeError("snapshot manifest missing")
        manifest = json.loads(zf.read(SNAPSHOT_MANIFEST_NAME).decode("utf-8"))
        if dry_run:
            return {
                "operation": "restore",
                "dry_run": True,
                "snapshot_path": str(snapshot),
                "data_root": str(root),
                "manifest": manifest,
            }
        root.mkdir(parents=True, exist_ok=True)
        zf.extractall(root)
    paths = resolve_stock_model_paths(root.parent, root)
    paths.ensure_runtime_dirs()
    for ticker in TICKERS:
        paths.ensure_runtime_dirs(ticker)
    required = [
        paths.sec_cache_dir,
        paths.ticker_dir("PBI").parent,
        paths.market_cache_dir,
        paths.writer_cache_dir,
        paths.basis_proxy_dir,
        paths.excel_output_dir,
    ]
    required_ok = all(path.exists() for path in required) if validate else True
    report = {
        "operation": "restore",
        "dry_run": False,
        "snapshot_path": str(snapshot),
        "data_root": str(root),
        "manifest": manifest,
        "required_folders_ok": required_ok,
        "required_folders": [str(path) for path in required],
    }
    (paths.logs_dir / "data_restore_report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    return report


def validate_data_root(
    *,
    data_root: Path | str,
    repo_root: Path | str | None = None,
    run_workbook_validation: bool = True,
) -> Dict[str, Any]:
    root = _safe_resolve(data_root)
    repo = _safe_resolve(repo_root) if repo_root is not None else Path(__file__).resolve().parents[2]
    paths = resolve_stock_model_paths(repo, root)
    required_folders = {
        "sec_cache": paths.sec_cache_dir,
        "tickers": paths.ticker_dir(None),
        "market_cache": paths.market_cache_dir,
        "writer_cache": paths.writer_cache_dir,
        "basis_proxy": paths.basis_proxy_dir,
        "excel_output": paths.excel_output_dir,
        "logs": paths.logs_dir,
    }
    missing_folders = [name for name, path in required_folders.items() if not path.exists()]
    workbook_paths = _preferred_workbook_paths(paths.excel_output_dir)
    missing_workbooks = [str(path) for path in workbook_paths.values() if not path.exists()]
    resolver_inside = all(
        _is_relative_to(path, root)
        for path in (
            paths.sec_cache_dir,
            paths.market_cache_dir,
            paths.writer_cache_dir,
            paths.basis_proxy_dir,
            paths.excel_output_dir,
            paths.render_checks_dir,
            paths.validation_reports_dir,
            paths.logs_dir,
        )
    )
    validation_summary: Dict[str, Any] = {"run": False}
    if run_workbook_validation and not missing_workbooks:
        from .workbook_validation_runner import validate_workbooks, write_validation_reports

        results = validate_workbooks(workbook_paths)
        write_validation_reports(results, paths.validation_reports_dir / "workbook_validation")
        validation_summary = {
            "run": True,
            "overall_by_ticker": {result.ticker: result.overall for result in results},
            "passes": all(result.overall == "PASS" for result in results),
        }
    elif not run_workbook_validation:
        validation_summary = {"run": False, "skipped_reason": "disabled by caller"}
    else:
        validation_summary = {"run": False, "skipped_reason": "workbooks missing"}
    overall = (
        "PASS"
        if not missing_folders
        and not missing_workbooks
        and resolver_inside
        and (not validation_summary.get("run") or validation_summary.get("passes") is True)
        else "FAIL"
    )
    report = {
        "operation": "validate-root",
        "created_at": _utc_now(),
        "data_root": str(root),
        "missing_folders": missing_folders,
        "missing_workbooks": missing_workbooks,
        "resolver_inside_data_root": resolver_inside,
        "workbook_validation": validation_summary,
        "overall": overall,
    }
    paths.logs_dir.mkdir(parents=True, exist_ok=True)
    (paths.logs_dir / DATA_ROOT_VALIDATION_REPORT_NAME).write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    return report


def _validation_report_passes(data_root: Path) -> bool:
    path = data_root / "validation_reports" / "workbook_validation" / "workbook_validation_report.json"
    if not path.exists():
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if not isinstance(payload, list):
        return False
    by_ticker = {str(row.get("ticker") or "").upper(): str(row.get("overall") or "").upper() for row in payload if isinstance(row, dict)}
    return all(by_ticker.get(ticker) == "PASS" for ticker in TICKERS)


def _snapshot_exists_from_report(data_root: Path, snapshot_path: Path | str | None = None) -> bool:
    if snapshot_path is not None and str(snapshot_path).strip():
        return _safe_resolve(snapshot_path).exists()
    report_path = data_root / "logs" / SNAPSHOT_REPORT_NAME
    if not report_path.exists():
        return False
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    out = str(report.get("out_path") or "").strip()
    return bool(out) and Path(out).expanduser().exists()


def _old_layout_candidates(repo_root: Path, data_root: Path) -> List[Path]:
    raw = [
        repo_root / "sec_cache",
        repo_root / "PBI",
        repo_root / "GPRE",
        repo_root / "ANF",
        repo_root / EXCEL_OUTPUT_FOLDER,
        repo_root / "writer_cache",
        repo_root / "render_checks",
        repo_root / "Code" / "validation_reports",
        repo_root / "logs",
    ]
    out: List[Path] = []
    seen: set[str] = set()
    for cand in raw:
        try:
            resolved = cand.resolve()
        except Exception:
            resolved = cand
        if not resolved.exists():
            continue
        if resolved == data_root.resolve() or _is_relative_to(resolved, data_root):
            continue
        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)
        out.append(resolved)
    return out


def _require_cleanup_guards(
    *,
    repo_root: Path,
    data_root: Path,
    snapshot_path: Path | str | None,
    allow_no_snapshot: bool,
) -> None:
    if not data_root.exists():
        raise RuntimeError(f"data_root does not exist: {data_root}")
    if not (data_root / "logs" / MIGRATION_REPORT_NAME).exists():
        raise RuntimeError("cleanup refused: migration report missing")
    if not (data_root / "logs" / CLEANUP_DRY_RUN_REPORT_NAME).exists():
        raise RuntimeError("cleanup refused: dry-run list has not been generated")
    if not allow_no_snapshot and not _snapshot_exists_from_report(data_root, snapshot_path=snapshot_path):
        raise RuntimeError("cleanup refused: snapshot backup missing")
    paths = resolve_stock_model_paths(repo_root, data_root)
    workbook_paths = _preferred_workbook_paths(paths.excel_output_dir)
    missing_workbooks = [str(path) for path in workbook_paths.values() if not path.exists()]
    if missing_workbooks:
        raise RuntimeError("cleanup refused: data-root workbook outputs missing: " + ", ".join(missing_workbooks))
    if not _validation_report_passes(data_root):
        raise RuntimeError("cleanup refused: workbook validation PASS report missing or incomplete")


def cleanup_old_layout(
    *,
    repo_root: Path | str,
    data_root: Path | str,
    dry_run: bool = True,
    confirm: bool = False,
    archive: bool = False,
    snapshot_path: Path | str | None = None,
    allow_no_snapshot: bool = False,
    confirm_delete_permanent: bool = False,
) -> Dict[str, Any]:
    repo = _safe_resolve(repo_root)
    root = _safe_resolve(data_root)
    candidates = _old_layout_candidates(repo, root)
    report: Dict[str, Any] = {
        "operation": "cleanup-old",
        "created_at": _utc_now(),
        "dry_run": bool(dry_run),
        "repo_root": str(repo),
        "data_root": str(root),
        "candidates": [str(path) for path in candidates],
        "action": "dry_run",
    }
    if dry_run:
        logs_dir = root / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        (logs_dir / CLEANUP_DRY_RUN_REPORT_NAME).write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        return report
    if not confirm:
        raise RuntimeError("cleanup refused: --confirm is required")
    _require_cleanup_guards(
        repo_root=repo,
        data_root=root,
        snapshot_path=snapshot_path,
        allow_no_snapshot=allow_no_snapshot,
    )
    if confirm_delete_permanent:
        for path in candidates:
            if path.exists():
                if path.is_dir():
                    shutil.rmtree(path)
                else:
                    path.unlink()
        report["action"] = "deleted_permanently"
    else:
        if not archive:
            raise RuntimeError("cleanup refused: --archive is required for non-permanent cleanup")
        archive_root = repo / f"OldDataArchive_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        archive_root.mkdir(parents=True, exist_ok=False)
        moved: List[Dict[str, str]] = []
        for path in candidates:
            dest = archive_root / path.name
            if dest.exists():
                dest = archive_root / f"{path.name}_{len(moved) + 1}"
            shutil.move(str(path), str(dest))
            moved.append({"source": str(path), "archive": str(dest)})
        report["action"] = "archived"
        report["archive_root"] = str(archive_root)
        report["moved"] = moved
    logs_dir = root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    (logs_dir / CLEANUP_REPORT_NAME).write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    return report


def _print_report(report: Dict[str, Any]) -> None:
    print(json.dumps(report, indent=2, ensure_ascii=False))


def _effective_root_or_error(
    repo: Path,
    *,
    cli_data_root: str = "",
    allow_onedrive_data_root: bool = False,
    require_data_root: bool = True,
) -> tuple[Optional[Path], Dict[str, Any]]:
    resolved = resolve_effective_data_root(
        repo,
        cli_data_root=cli_data_root,
        allow_onedrive_data_root=allow_onedrive_data_root,
    )
    if resolved.errors:
        raise RuntimeError(" | ".join(resolved.errors))
    if require_data_root and resolved.data_root is None:
        raise RuntimeError("no data_root configured; pass --data-root or run `stock_models.py data config set-root <path>`")
    report = {
        "data_root": "" if resolved.data_root is None else str(resolved.data_root),
        "data_root_source": resolved.source,
        "config_path": "" if resolved.config_path is None else str(resolved.config_path),
        "warnings": list(resolved.warnings),
    }
    return resolved.data_root, report


def show_config(
    *,
    repo_root: Path | str,
    data_root: str = "",
    allow_onedrive_data_root: bool = False,
) -> Dict[str, Any]:
    repo = _safe_resolve(repo_root)
    effective, base = _effective_root_or_error(
        repo,
        cli_data_root=data_root,
        allow_onedrive_data_root=allow_onedrive_data_root,
        require_data_root=False,
    )
    paths = resolve_stock_model_paths(repo, effective)
    return {
        "operation": "config show",
        "repo_root": str(repo),
        **base,
        "paths": {
            "sec_cache_dir": str(paths.sec_cache_dir),
            "tickers_dir": str(paths.ticker_dir(None)),
            "market_cache_dir": str(paths.market_cache_dir),
            "writer_cache_dir": str(paths.writer_cache_dir),
            "basis_proxy_dir": str(paths.basis_proxy_dir),
            "excel_output_dir": str(paths.excel_output_dir),
            "render_checks_dir": str(paths.render_checks_dir),
            "validation_reports_dir": str(paths.validation_reports_dir),
            "logs_dir": str(paths.logs_dir),
        },
    }


def set_config_root(
    *,
    repo_root: Path | str,
    data_root: Path | str,
    allow_onedrive_data_root: bool = False,
) -> Dict[str, Any]:
    root = _safe_resolve(data_root)
    ok, problems = light_data_root_check(
        root,
        allow_onedrive_data_root=allow_onedrive_data_root,
        require_existing=True,
    )
    if not ok:
        raise RuntimeError("cannot set data_root: " + " | ".join(problems))
    config_path = write_config_data_root(
        repo_root,
        root,
        allow_onedrive_data_root=allow_onedrive_data_root,
    )
    report = show_config(repo_root=repo_root)
    report["operation"] = "config set-root"
    report["config_path"] = str(config_path)
    return report


def clear_config_root(*, repo_root: Path | str) -> Dict[str, Any]:
    config_path = clear_config_data_root(repo_root)
    report = show_config(repo_root=repo_root)
    report["operation"] = "config clear-root"
    report["config_path"] = str(config_path)
    return report


def main(argv: Optional[Sequence[str]] = None, *, repo_root: Path | str | None = None) -> int:
    repo = _safe_resolve(repo_root) if repo_root is not None else Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(prog="stock_models.py data", description="Portable StockModelData utilities.")
    sub = parser.add_subparsers(dest="command", required=True)

    migrate_p = sub.add_parser("migrate", help="Copy legacy data folders into StockModelData.")
    migrate_p.add_argument("--data-root", default="")
    migrate_p.add_argument("--allow-onedrive-data-root", action="store_true")

    snapshot_p = sub.add_parser("snapshot", help="Create a portable StockModelData zip snapshot.")
    snapshot_p.add_argument("--data-root", default="")
    snapshot_p.add_argument("--out", required=True)
    snapshot_p.add_argument("--allow-onedrive-data-root", action="store_true")
    snapshot_p.add_argument("--include-renders", action="store_true")
    snapshot_p.add_argument("--include-logs", action="store_true")
    snapshot_p.add_argument("--include-validation-reports", action="store_true")
    snapshot_p.add_argument("--dry-run", action="store_true")
    snapshot_p.add_argument("--overwrite", action="store_true")

    restore_p = sub.add_parser("restore", help="Restore a portable StockModelData snapshot zip.")
    restore_p.add_argument("--snapshot", required=True)
    restore_p.add_argument("--data-root", required=True)
    restore_p.add_argument("--allow-onedrive-data-root", action="store_true")
    restore_p.add_argument("--overwrite", action="store_true")
    restore_p.add_argument("--validate", default=True, action=argparse.BooleanOptionalAction)
    restore_p.add_argument("--dry-run", action="store_true")

    validate_p = sub.add_parser("validate-root", help="Validate that a StockModelData root is usable.")
    validate_p.add_argument("--data-root", default="")
    validate_p.add_argument("--allow-onedrive-data-root", action="store_true")
    validate_p.add_argument("--skip-workbook-validation", action="store_true")

    cleanup_p = sub.add_parser("cleanup-old", help="Safely archive or delete old legacy data folders.")
    cleanup_p.add_argument("--data-root", default="")
    cleanup_p.add_argument("--allow-onedrive-data-root", action="store_true")
    cleanup_p.add_argument("--dry-run", action="store_true")
    cleanup_p.add_argument("--confirm", action="store_true")
    cleanup_p.add_argument("--archive", action="store_true")
    cleanup_p.add_argument("--snapshot", default="")
    cleanup_p.add_argument("--allow-no-snapshot", action="store_true")
    cleanup_p.add_argument("--confirm-delete-permanent", action="store_true")

    config_p = sub.add_parser("config", help="Show or edit local data-root config.")
    config_sub = config_p.add_subparsers(dest="config_command", required=True)
    config_show = config_sub.add_parser("show", help="Show the effective data root and resolved paths.")
    config_show.add_argument("--data-root", default="")
    config_show.add_argument("--allow-onedrive-data-root", action="store_true")
    config_set = config_sub.add_parser("set-root", help="Write repo-local StockModelData root config.")
    config_set.add_argument("root")
    config_set.add_argument("--allow-onedrive-data-root", action="store_true")
    config_sub.add_parser("clear-root", help="Clear the repo-local data_root config entry.")

    args = parser.parse_args(argv)
    if args.command == "migrate":
        effective, _base = _effective_root_or_error(
            repo,
            cli_data_root=args.data_root,
            allow_onedrive_data_root=bool(args.allow_onedrive_data_root),
        )
        _print_report(migrate_legacy_layout(repo_root=repo, data_root=effective))
        return 0
    if args.command == "snapshot":
        effective, _base = _effective_root_or_error(
            repo,
            cli_data_root=args.data_root,
            allow_onedrive_data_root=bool(args.allow_onedrive_data_root),
        )
        _print_report(
            snapshot_data_root(
                data_root=effective,
                out_path=args.out,
                include_renders=bool(args.include_renders),
                include_logs=bool(args.include_logs),
                include_validation_reports=bool(args.include_validation_reports),
                dry_run=bool(args.dry_run),
                overwrite=bool(args.overwrite),
                repo_root=repo,
            )
        )
        return 0
    if args.command == "restore":
        ok, problems = light_data_root_check(
            args.data_root,
            allow_onedrive_data_root=bool(args.allow_onedrive_data_root),
            require_existing=False,
        )
        if not ok:
            raise RuntimeError("cannot restore to data_root: " + " | ".join(problems))
        _print_report(
            restore_snapshot(
                snapshot_path=args.snapshot,
                data_root=args.data_root,
                overwrite=bool(args.overwrite),
                dry_run=bool(args.dry_run),
                validate=bool(args.validate),
            )
        )
        return 0
    if args.command == "validate-root":
        effective, _base = _effective_root_or_error(
            repo,
            cli_data_root=args.data_root,
            allow_onedrive_data_root=bool(args.allow_onedrive_data_root),
        )
        _print_report(
            validate_data_root(
                data_root=effective,
                repo_root=repo,
                run_workbook_validation=not bool(args.skip_workbook_validation),
            )
        )
        return 0
    if args.command == "cleanup-old":
        effective, _base = _effective_root_or_error(
            repo,
            cli_data_root=args.data_root,
            allow_onedrive_data_root=bool(args.allow_onedrive_data_root),
        )
        confirmed_action = bool(args.confirm or args.archive or args.confirm_delete_permanent)
        dry_run = bool(args.dry_run or not confirmed_action)
        _print_report(
            cleanup_old_layout(
                repo_root=repo,
                data_root=effective,
                dry_run=dry_run,
                confirm=confirmed_action,
                archive=bool(args.archive),
                snapshot_path=args.snapshot or None,
                allow_no_snapshot=bool(args.allow_no_snapshot),
                confirm_delete_permanent=bool(args.confirm_delete_permanent),
            )
        )
        return 0
    if args.command == "config":
        if args.config_command == "show":
            _print_report(
                show_config(
                    repo_root=repo,
                    data_root=args.data_root,
                    allow_onedrive_data_root=bool(args.allow_onedrive_data_root),
                )
            )
            return 0
        if args.config_command == "set-root":
            _print_report(
                set_config_root(
                    repo_root=repo,
                    data_root=args.root,
                    allow_onedrive_data_root=bool(args.allow_onedrive_data_root),
                )
            )
            return 0
        if args.config_command == "clear-root":
            _print_report(clear_config_root(repo_root=repo))
            return 0
    raise RuntimeError(f"unknown data command: {args.command}")
