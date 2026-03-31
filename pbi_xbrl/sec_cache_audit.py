"""Audit-only helpers for identifying ambiguous or redundant SEC-cache content."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


@dataclass(frozen=True)
class SecCacheAuditCandidate:
    ticker: str
    category: str
    status: str
    key: str
    path_a: str
    path_b: str
    detail: str


def write_sec_cache_audit_report(
    repo_root: Path,
    *,
    tickers: Sequence[str] = ("PBI", "GPRE"),
    output_path: Optional[Path] = None,
) -> Path:
    rows: List[SecCacheAuditCandidate] = []
    for ticker in tickers:
        rows.extend(audit_sec_cache_ticker(repo_root, ticker))
    out_path = output_path or (repo_root / "sec_cache" / "_reports" / "sec_cache_audit_latest.md")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(render_sec_cache_audit_markdown(rows), encoding="utf-8")
    return out_path


def audit_sec_cache_ticker(repo_root: Path, ticker: str) -> List[SecCacheAuditCandidate]:
    ticker_u = str(ticker or "").strip().upper()
    cache_root = repo_root / "sec_cache" / ticker_u
    if not cache_root.exists():
        return []
    rows: List[SecCacheAuditCandidate] = []
    rows.extend(_legacy_flat_vs_nested_duplicates(cache_root, ticker_u))
    rows.extend(_manifest_orphaned_files(repo_root, ticker_u))
    rows.extend(_likely_redundant_curated_artifacts(repo_root, ticker_u))
    return rows


def render_sec_cache_audit_markdown(rows: Sequence[SecCacheAuditCandidate]) -> str:
    lines: List[str] = [
        "# SEC Cache Audit",
        "",
        "Audit only. No files were deleted in this pass.",
        "",
    ]
    by_ticker: Dict[str, List[SecCacheAuditCandidate]] = {}
    for row in rows:
        by_ticker.setdefault(row.ticker, []).append(row)
    for ticker in sorted(by_ticker):
        lines.append(f"## {ticker}")
        lines.append("")
        lines.append("| Category | Status | Key | Path A | Path B | Detail |")
        lines.append("| --- | --- | --- | --- | --- | --- |")
        ticker_rows = sorted(by_ticker[ticker], key=lambda r: (r.category, r.status, r.key, r.path_a, r.path_b))
        if not ticker_rows:
            lines.append("| - | - | - | - | - | - |")
        for row in ticker_rows:
            lines.append(
                "| "
                + " | ".join(
                    [
                        _md_cell(row.category),
                        _md_cell(row.status),
                        _md_cell(row.key),
                        _md_cell(row.path_a),
                        _md_cell(row.path_b),
                        _md_cell(row.detail),
                    ]
                )
                + " |"
            )
        lines.append("")
    return "\n".join(lines)


def _legacy_flat_vs_nested_duplicates(cache_root: Path, ticker: str) -> List[SecCacheAuditCandidate]:
    rows: List[SecCacheAuditCandidate] = []
    for flat_path in sorted(cache_root.glob("doc_*")):
        if not flat_path.is_file():
            continue
        match = re.match(r"doc_(\d{18})_(.+)$", flat_path.name)
        if not match:
            continue
        accn_key = match.group(1)
        inner_name = match.group(2)
        nested_matches = list(cache_root.glob(f"*/{accn_key}/docs/{inner_name}"))
        if not nested_matches:
            nested_matches = list(cache_root.glob(f"{accn_key}/docs/{inner_name}"))
        for nested_path in sorted(nested_matches):
            same_hash = _safe_sha256(flat_path) == _safe_sha256(nested_path)
            rows.append(
                SecCacheAuditCandidate(
                    ticker=ticker,
                    category="legacy_flat_vs_nested",
                    status="exact_duplicate" if same_hash else "name_duplicate_different_content",
                    key=f"{accn_key}|{inner_name}",
                    path_a=str(flat_path),
                    path_b=str(nested_path),
                    detail="Legacy flat SEC doc mirrors nested package doc." if same_hash else "Flat and nested docs share accession/name but differ in content.",
                )
            )
    return rows


def _manifest_orphaned_files(repo_root: Path, ticker: str) -> List[SecCacheAuditCandidate]:
    manifest_path = repo_root / "sec_cache" / ticker / "materials" / "source_material_manifest.json"
    if not manifest_path.exists():
        return []
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8", errors="ignore"))
    except Exception:
        return [
            SecCacheAuditCandidate(
                ticker=ticker,
                category="manifest_orphan",
                status="manifest_unreadable",
                key=str(manifest_path),
                path_a=str(manifest_path),
                path_b="",
                detail="Could not parse source material manifest JSON.",
            )
        ]
    rows: List[SecCacheAuditCandidate] = []
    entries = list(payload if isinstance(payload, list) else payload.get("entries") or [])
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        dest_path = Path(str(entry.get("destination_path") or "").strip()) if str(entry.get("destination_path") or "").strip() else None
        if dest_path is None:
            continue
        if dest_path.exists():
            continue
        rows.append(
            SecCacheAuditCandidate(
                ticker=ticker,
                category="manifest_orphan",
                status="missing_destination",
                key=str(entry.get("key") or entry.get("source_url") or dest_path.name),
                path_a=str(dest_path),
                path_b=str(manifest_path),
                detail="Manifest entry points to a missing curated destination file.",
            )
        )
    return rows


def _likely_redundant_curated_artifacts(repo_root: Path, ticker: str) -> List[SecCacheAuditCandidate]:
    manifest_path = repo_root / "sec_cache" / ticker / "materials" / "source_material_manifest.json"
    if not manifest_path.exists():
        return []
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8", errors="ignore"))
    except Exception:
        return []
    rows: List[SecCacheAuditCandidate] = []
    entries = list(payload if isinstance(payload, list) else payload.get("entries") or [])
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        src_txt = str(entry.get("source_doc") or "").strip()
        dest_txt = str(entry.get("destination_path") or "").strip()
        if not src_txt or not dest_txt:
            continue
        src_path = Path(src_txt)
        dest_path = Path(dest_txt)
        if not src_path.exists() or not dest_path.exists():
            continue
        rows.append(
            SecCacheAuditCandidate(
                ticker=ticker,
                category="curated_copy",
                status="active_copy_from_source",
                key=str(entry.get("key") or dest_path.name),
                path_a=str(src_path),
                path_b=str(dest_path),
                detail="Curated material is an active copy of a preserved source document; treat as in-use unless writer references change.",
            )
        )
    return rows


def _safe_sha256(path: Path) -> str:
    h = hashlib.sha256()
    try:
        with path.open("rb") as fh:
            while True:
                chunk = fh.read(1024 * 1024)
                if not chunk:
                    break
                h.update(chunk)
    except Exception:
        return ""
    return h.hexdigest()


def _md_cell(value: Any) -> str:
    text = str(value or "").replace("\n", " ").replace("|", "\\|").strip()
    return text or "-"
