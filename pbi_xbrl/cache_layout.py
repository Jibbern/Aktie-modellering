"""Helpers for resolving canonical ticker and shared cache roots."""
from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional


_CANONICAL_SKIP_TOP_LEVEL = {"market_data"}


def canonical_shared_cache_root(repo_root: Path) -> Path:
    return Path(repo_root).expanduser().resolve() / "sec_cache"


def canonical_ticker_cache_root(repo_root: Path, ticker: str | None) -> Path:
    t = str(ticker or "").strip().upper()
    return canonical_shared_cache_root(repo_root) / t if t else canonical_shared_cache_root(repo_root)


def legacy_ticker_cache_root(repo_root: Path, ticker: str | None) -> Path:
    t = str(ticker or "").strip().upper()
    return (Path(repo_root).expanduser().resolve() / t / "sec_cache") if t else canonical_shared_cache_root(repo_root)


def ticker_cache_roots_from_base_dir(base_dir: Path) -> List[Path]:
    base = Path(base_dir).expanduser().resolve()
    if base.name.lower() == "sec_cache":
        return [base]
    repo_root = base.parent
    ticker = base.name
    roots: List[Path] = []
    seen: set[str] = set()
    for cand in (
        canonical_ticker_cache_root(repo_root, ticker),
        legacy_ticker_cache_root(repo_root, ticker),
    ):
        try:
            key = str(cand.resolve())
        except Exception:
            key = str(cand)
        if key in seen:
            continue
        seen.add(key)
        roots.append(cand)
    return roots


def preferred_ticker_cache_root_from_base_dir(base_dir: Path) -> Path:
    roots = ticker_cache_roots_from_base_dir(base_dir)
    for cand in roots:
        if cand.exists():
            return cand
    return roots[0]


def ticker_cache_candidates(repo_root: Path, ticker: str | None, explicit_cache_dir: Optional[Path] = None) -> List[Path]:
    roots: List[Path] = []
    seen: set[str] = set()

    def _add(path_in: Optional[Path]) -> None:
        if path_in is None:
            return
        p = Path(path_in).expanduser()
        try:
            key = str(p.resolve())
        except Exception:
            key = str(p)
        if key in seen:
            return
        seen.add(key)
        roots.append(p)

    t = str(ticker or "").strip()
    t_u = t.upper()
    shared = canonical_shared_cache_root(repo_root)

    if explicit_cache_dir is not None:
        croot = Path(explicit_cache_dir).expanduser()
        _add(croot)
        if t_u:
            _add(croot / t_u)
            _add(croot / t)
            _add(croot / t.lower())

    if t_u:
        _add(canonical_ticker_cache_root(repo_root, t_u))
        if t and t != t_u:
            _add(shared / t)
            _add(shared / t.lower())
        _add(legacy_ticker_cache_root(repo_root, t_u))
        if t and t != t_u:
            _add(Path(repo_root).expanduser().resolve() / t / "sec_cache")
            _add(Path(repo_root).expanduser().resolve() / t.lower() / "sec_cache")

    _add(shared)
    return roots


def _copy_missing_tree(src: Path, dst: Path) -> tuple[int, int]:
    copied_files = 0
    created_dirs = 0
    for path_in in sorted(src.rglob("*")):
        rel = path_in.relative_to(src)
        dest = dst / rel
        if path_in.is_dir():
            if not dest.exists():
                dest.mkdir(parents=True, exist_ok=True)
                created_dirs += 1
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists():
            continue
        shutil.copy2(path_in, dest)
        copied_files += 1
    return copied_files, created_dirs


def bootstrap_canonical_ticker_cache(repo_root: Path, ticker: str | None) -> Dict[str, Any]:
    repo_root = Path(repo_root).expanduser().resolve()
    canonical = canonical_ticker_cache_root(repo_root, ticker)
    legacy = legacy_ticker_cache_root(repo_root, ticker)
    canonical.mkdir(parents=True, exist_ok=True)
    result: Dict[str, Any] = {
        "canonical": canonical,
        "legacy": legacy,
        "copied_files": 0,
        "created_dirs": 0,
        "skipped": [],
        "status": "noop",
    }
    if not ticker or not legacy.exists() or not legacy.is_dir():
        result["status"] = "no_legacy"
        return result

    for child in sorted(legacy.iterdir(), key=lambda p: p.name.lower()):
        if child.name.lower() in _CANONICAL_SKIP_TOP_LEVEL:
            result["skipped"].append(child.name)
            continue
        dest = canonical / child.name
        if child.is_dir():
            copied_files, created_dirs = _copy_missing_tree(child, dest)
            result["copied_files"] += copied_files
            result["created_dirs"] += created_dirs
            if not dest.exists():
                dest.mkdir(parents=True, exist_ok=True)
                result["created_dirs"] += 1
            continue
        if dest.exists():
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(child, dest)
        result["copied_files"] += 1

    result["status"] = "copied" if (result["copied_files"] or result["created_dirs"]) else "already_seeded"
    return result
