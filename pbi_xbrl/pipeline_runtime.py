"""Runtime signatures, cache helpers, and root-resolution utilities for the pipeline."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pandas as pd


def resolve_path_safe(path_in: Path) -> Path:
    try:
        return path_in.resolve()
    except Exception:
        return path_in


def path_belongs_to_ticker(
    path_obj: Optional[Path],
    ticker: Optional[str],
    ticker_roots: Optional[List[Path]] = None,
) -> bool:
    if path_obj is None:
        return False
    tkr = str(ticker or "").strip().upper()
    if not tkr:
        return True
    p_res = resolve_path_safe(Path(path_obj))
    roots = [Path(r) for r in (ticker_roots or []) if r is not None]
    for root in roots:
        r_res = resolve_path_safe(root)
        try:
            p_res.relative_to(r_res)
            return True
        except Exception:
            continue
    token_re = re.compile(rf"(?<![A-Z0-9]){re.escape(tkr)}(?![A-Z0-9])", re.I)
    return bool(token_re.search(str(p_res)))


def paths_signature(paths: List[Path], max_files: int = 1500) -> str:
    # File signatures are intentionally cheap freshness heuristics. They only need
    # to notice material input changes strongly enough to invalidate stale caches;
    # they are not meant to be cryptographic proofs of byte-identical trees.
    rows: List[str] = []
    for path_obj in sorted([Path(x) for x in paths if x is not None]):
        try:
            st = path_obj.stat()
        except Exception:
            continue
        rows.append(f"{path_obj.name}|{int(st.st_size)}|{int(st.st_mtime)}")
        if len(rows) >= max_files:
            break
    if not rows:
        return "none"
    return hashlib.sha1("||".join(rows).encode("utf-8", errors="ignore")).hexdigest()


def material_dirs_signature(base_dir: Path, ticker: Optional[str]) -> str:
    tkr = str(ticker or "").strip().upper()
    dirs = [
        base_dir / "earnings_release",
        base_dir / "Earnings Release",
        base_dir / "Earnings Releases",
        base_dir / "press_release",
        base_dir / "Press Release",
        base_dir / "slides",
        base_dir / "earnings_presentation",
        base_dir / "Earnings Presentation",
        base_dir / "Earnings Transcripts",
        base_dir / "transcripts",
        base_dir / "earnings_transcripts",
        base_dir / "annual_reports",
        base_dir / "financial_statement",
    ]
    if tkr:
        dirs.extend(
            [
                base_dir / f"{tkr}-10K",
                base_dir / f"{tkr}_10K",
                base_dir / f"{tkr} 10K",
            ]
        )
    files: List[Path] = []
    for path_obj in dirs:
        if not path_obj.exists() or not path_obj.is_dir():
            continue
        try:
            files.extend([item for item in path_obj.rglob("*") if item.is_file()])
        except Exception:
            continue
    return paths_signature(files, max_files=1500)


def resolve_pipeline_roots(
    *,
    repo_root: Path,
    default_base_dir: Path,
    ticker: Optional[str],
    material_root: Optional[Path] = None,
) -> Tuple[str, str, Path]:
    tkr_raw = str(ticker or "").strip()
    tkr_u = tkr_raw.upper()
    if material_root is not None:
        return tkr_raw, tkr_u, Path(material_root).expanduser().resolve()
    base_dir_candidates: List[Path] = []
    if tkr_raw:
        base_dir_candidates.extend(
            [
                repo_root / tkr_u,
                repo_root / tkr_raw,
                repo_root / tkr_raw.lower(),
            ]
        )
    if not tkr_raw:
        base_dir_candidates.append(default_base_dir)
    base_dir = next(
        (item for item in base_dir_candidates if item.exists() and item.is_dir()),
        (base_dir_candidates[0] if base_dir_candidates else default_base_dir),
    )
    return tkr_raw, tkr_u, base_dir


@contextmanager
def timed_stage(stage_timings: Dict[str, float], name: str, enabled: bool = True) -> Iterator[None]:
    start = time.perf_counter()
    try:
        yield
    finally:
        if enabled:
            stage_timings[name] = stage_timings.get(name, 0.0) + (time.perf_counter() - start)


def submissions_recent_signature(
    submissions: Dict[str, Any],
    forms_prefix: Optional[Tuple[str, ...]] = None,
    max_rows: int = 400,
) -> str:
    # This signature acts as an invalidation guard for SEC-driven stages. It tracks
    # recent filing identity and dates closely enough to notice relevant filing
    # changes without serializing the full submissions payload into every cache key.
    recent = ((submissions or {}).get("filings") or {}).get("recent") or {}
    accessions = list(recent.get("accessionNumber") or [])
    forms = list(recent.get("form") or [])
    reports = list(recent.get("reportDate") or [])
    filed = list(recent.get("filingDate") or [])
    primary_docs = list(recent.get("primaryDocument") or [])
    count = min(len(accessions), len(forms), len(reports), len(filed), len(primary_docs))
    rows: List[str] = []
    for idx in range(count):
        form = str(forms[idx] or "").upper().strip()
        if forms_prefix and not any(form.startswith(prefix) for prefix in forms_prefix):
            continue
        rows.append(
            "|".join(
                [
                    form,
                    str(accessions[idx] or ""),
                    str(reports[idx] or ""),
                    str(filed[idx] or ""),
                    str(primary_docs[idx] or ""),
                ]
            )
        )
    if len(rows) > max_rows:
        rows = rows[:max_rows]
    return hashlib.sha1("||".join(rows).encode("utf-8", errors="ignore")).hexdigest()


def dataframe_quick_signature(df: pd.DataFrame, cols: List[str]) -> str:
    # Dataframe signatures are cache-key inputs for expensive stages. They favor
    # stable column subsets over full-frame hashing so invalidation stays fast even
    # when large intermediate frames are involved.
    if df is None or df.empty:
        return "empty"
    use_cols = [col for col in cols if col in df.columns]
    if not use_cols:
        return f"shape={df.shape}"
    work = df[use_cols].copy()
    for col in use_cols:
        if str(work[col].dtype).startswith("datetime"):
            work[col] = pd.to_datetime(work[col], errors="coerce").astype(str)
    try:
        payload = pd.util.hash_pandas_object(work.fillna(""), index=False).values.tobytes()
        return hashlib.sha1(payload).hexdigest()
    except Exception:
        return hashlib.sha1(str(work.head(500)).encode("utf-8", errors="ignore")).hexdigest()


class PipelineStageCache:
    """Versioned stage cache for expensive intermediate pipeline artifacts.

    The stage cache is finer-grained than the bundle cache in `stock_models.py`.
    Each stage chooses its own invalidation signature so downstream workbook-only
    reruns can reuse expensive intermediate products without hiding true input or
    behavior changes behind stale state.
    """

    def __init__(self, cache_root: Path, cik10: str, version: int) -> None:
        self.cache_root = Path(cache_root)
        self.cik10 = str(cik10)
        self.version = int(version)
        try:
            self.cache_root.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

    def _paths(self, stage_name: str) -> Tuple[Path, Path]:
        meta_path = self.cache_root / f"{stage_name}_{self.cik10}.meta.json"
        data_path = self.cache_root / f"{stage_name}_{self.cik10}.pkl"
        return meta_path, data_path

    def load(self, stage_name: str, cache_key: str) -> Optional[Any]:
        meta_path, data_path = self._paths(stage_name)
        if not (meta_path.exists() and data_path.exists()):
            return None
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            # Both the cache format version and the caller-supplied key must match.
            # The version guards structural changes; the key guards input freshness.
            if meta.get("version") != self.version or str(meta.get("key")) != str(cache_key):
                return None
            obj = pd.read_pickle(data_path)
            print(f"[stage_cache] hit stage={stage_name}", flush=True)
            return obj
        except Exception:
            return None

    def save(self, stage_name: str, cache_key: str, obj: Any) -> None:
        meta_path, data_path = self._paths(stage_name)
        try:
            # Stage payloads are written as pickle + tiny JSON metadata so cache
            # inspection and invalidation debugging stay human-readable.
            pd.to_pickle(obj, data_path)
            meta_path.write_text(
                json.dumps(
                    {
                        "version": self.version,
                        "key": str(cache_key),
                        "saved_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
                    },
                    ensure_ascii=True,
                ),
                encoding="utf-8",
            )
            print(f"[stage_cache] saved stage={stage_name}", flush=True)
        except Exception:
            pass
