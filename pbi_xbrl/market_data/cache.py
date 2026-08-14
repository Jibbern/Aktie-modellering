"""Filesystem helpers for the `sec_cache/market_data` tree.

The service layer uses these helpers to keep the raw/index/parsed/export layout
stable regardless of whether callers pass the overall cache root or the nested
`market_data` directory directly.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

from ..cache_semantics import build_cache_identity, file_content_sha256


def resolve_market_cache_root(cache_dir: Path) -> Path:
    croot = Path(cache_dir).expanduser().resolve()
    if croot.name.lower() in {"market_data", "market_cache"}:
        root = croot
    elif croot.parent.name.lower() == "sec_cache" and (croot.parent.parent / "tickers").exists():
        root = croot.parent.parent / "market_cache"
    elif croot.name.lower() == "sec_cache":
        root = croot / "market_data"
    else:
        root = croot.parent / "market_data"
    root.mkdir(parents=True, exist_ok=True)
    return root


def ensure_market_cache_dirs(cache_root: Path) -> None:
    cache_root.mkdir(parents=True, exist_ok=True)
    (cache_root / "raw").mkdir(parents=True, exist_ok=True)
    (cache_root / "parsed").mkdir(parents=True, exist_ok=True)
    (cache_root / "parsed" / "exports").mkdir(parents=True, exist_ok=True)
    (cache_root / "index").mkdir(parents=True, exist_ok=True)


def raw_source_dir(cache_root: Path, source: str, year: int) -> Path:
    out = cache_root / "raw" / str(source) / str(year)
    out.mkdir(parents=True, exist_ok=True)
    return out


def parsed_obs_path(cache_root: Path, source: str) -> Path:
    out = cache_root / "parsed" / str(source)
    out.mkdir(parents=True, exist_ok=True)
    return out / "observations.parquet"


def parsed_quarter_path(cache_root: Path, source: str) -> Path:
    out = cache_root / "parsed" / str(source)
    out.mkdir(parents=True, exist_ok=True)
    return out / "quarterly.parquet"


def export_rows_path(cache_root: Path, ticker: str) -> Path:
    return cache_root / "parsed" / "exports" / f"{str(ticker or 'DEFAULT').upper()}.parquet"


def raw_manifest_path(cache_root: Path) -> Path:
    return cache_root / "index" / "raw_manifest.json"


def parsed_manifest_path(cache_root: Path) -> Path:
    return cache_root / "index" / "parsed_manifest.json"


def export_inputs_manifest_path(cache_root: Path, ticker: str) -> Path:
    out_dir = cache_root / "index" / "export_inputs"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{str(ticker or 'DEFAULT').upper()}.json"


def remote_debug_path(cache_root: Path, source: str) -> Path:
    root = resolve_market_cache_root(cache_root)
    out_dir = root / "index" / "remote_debug"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{str(source or 'unknown').strip().lower()}.json"


def load_manifest(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_manifest(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")


def file_fingerprint(path: Path) -> str:
    try:
        return file_content_sha256(Path(path))
    except (OSError, ValueError):
        return ""


def batch_fingerprint(tokens: Iterable[str]) -> str:
    vals = sorted(str(token) for token in tokens if str(token or ""))
    return build_cache_identity(
        "market-data-content-batch",
        {"tokens": vals},
    ).digest


def normalize_manifest_list(raw_manifest: Dict[str, Any], source: str) -> List[Dict[str, Any]]:
    rows = raw_manifest.get(str(source), [])
    if isinstance(rows, list):
        return [r for r in rows if isinstance(r, dict)]
    return []
