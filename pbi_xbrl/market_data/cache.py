"""Filesystem helpers for the `sec_cache/market_data` tree.

The service layer uses these helpers to keep the raw/index/parsed/export layout
stable regardless of whether callers pass the overall cache root or the nested
`market_data` directory directly.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List


def resolve_market_cache_root(cache_dir: Path) -> Path:
    croot = Path(cache_dir).expanduser().resolve()
    if croot.name.lower() == "market_data":
        root = croot
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
        st = path.stat()
    except Exception:
        return ""
    token = f"{path.name}|{int(st.st_size)}|{int(getattr(st, 'st_mtime_ns', int(st.st_mtime * 1000000000)))}"
    return hashlib.sha1(token.encode("utf-8", errors="ignore")).hexdigest()


def batch_fingerprint(tokens: Iterable[str]) -> str:
    vals = [str(x or "") for x in tokens if str(x or "")]
    if not vals:
        return "none"
    return hashlib.sha1("||".join(sorted(vals)).encode("utf-8", errors="ignore")).hexdigest()


def normalize_manifest_list(raw_manifest: Dict[str, Any], source: str) -> List[Dict[str, Any]]:
    rows = raw_manifest.get(str(source), [])
    if isinstance(rows, list):
        return [r for r in rows if isinstance(r, dict)]
    return []
