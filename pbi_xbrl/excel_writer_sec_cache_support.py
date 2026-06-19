from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
import re
from typing import Any, List, MutableMapping, Optional


@dataclass(frozen=True)
class SecCacheSupportDeps:
    runtime: MutableMapping[str, Any]


class SecCacheSupport:
    def __init__(self, deps: SecCacheSupportDeps) -> None:
        self.deps = deps

    def _rt(self, name: str, default: Any = None) -> Any:
        return self.deps.runtime.get(name, default)

    def sec_cache_roots_local(self) -> List[Path]:
        Path_rt = self._rt("Path", Path)
        ticker_cache_candidates = self._rt("ticker_cache_candidates")
        ticker = self._rt("ticker")
        cache_dir = self._rt("cache_dir")

        roots: List[Path] = []
        seen: set[str] = set()

        def _add_root(p: Path) -> None:
            try:
                rp = str(p.resolve())
            except Exception:
                rp = str(p)
            if rp in seen or not p.exists():
                return
            seen.add(rp)
            roots.append(p)

        repo_root = Path_rt(__file__).resolve().parents[2]
        for cand in ticker_cache_candidates(repo_root, str(ticker or "").strip(), Path_rt(cache_dir) if cache_dir is not None else None):
            _add_root(cand)
        return roots

    def sec_cache_doc_paths_local(self, root: Path) -> List[Path]:
        document_cache = self._rt("document_cache")

        cache_key = str(root.resolve()) if root.exists() else str(root)
        cached = document_cache.sec_cache_doc_paths_by_root.get(cache_key)
        if cached is not None:
            return list(cached)
        doc_paths: List[Path] = []
        if root.exists():
            try:
                for path_in in root.rglob("doc_*"):
                    if not path_in.is_file():
                        continue
                    if path_in.suffix.lower() not in {".htm", ".html", ".txt"}:
                        continue
                    doc_paths.append(path_in)
            except Exception:
                doc_paths = []
        doc_paths = sorted(doc_paths, key=lambda z: z.stat().st_mtime if z.exists() else 0, reverse=True)
        document_cache.sec_cache_doc_paths_by_root[cache_key] = list(doc_paths)
        return list(doc_paths)

    def sec_cache_html_paths_local(self, root: Path) -> List[Path]:
        document_cache = self._rt("document_cache")

        cache_key = str(root.resolve()) if root.exists() else str(root)
        cached = document_cache.sec_cache_html_paths_by_root.get(cache_key)
        if cached is not None:
            return list(cached)
        html_paths: List[Path] = []
        if root.exists():
            try:
                html_paths = sorted(
                    (
                        path_in
                        for path_in in root.glob("*.htm")
                        if path_in.is_file()
                    ),
                    key=lambda z: z.stat().st_mtime if z.exists() else 0,
                    reverse=True,
                )
            except Exception:
                html_paths = []
        document_cache.sec_cache_html_paths_by_root[cache_key] = list(html_paths)
        return list(html_paths)

    def sec_cache_docs_for_token_local(self, root: Path, token: str) -> List[Path]:
        re_rt = self._rt("re", re)
        document_cache = self._rt("document_cache")

        token_txt = str(token or "").strip()
        if not token_txt:
            return []
        cache_key = str(root.resolve()) if root.exists() else str(root)
        token_index = document_cache.sec_cache_doc_paths_by_token_by_root.get(cache_key)
        if token_index is None:
            token_index = {}
            for path_in in self.sec_cache_doc_paths_local(root):
                for token_hit in set(re_rt.findall(r"20\d{6}", path_in.name)):
                    token_index.setdefault(token_hit, []).append(path_in)
            document_cache.sec_cache_doc_paths_by_token_by_root[cache_key] = token_index
        return list(token_index.get(token_txt) or [])

    def sec_cache_html_paths_for_token_local(self, root: Path, token: str) -> List[Path]:
        re_rt = self._rt("re", re)
        document_cache = self._rt("document_cache")

        token_txt = str(token or "").strip()
        if not token_txt:
            return []
        cache_key = str(root.resolve()) if root.exists() else str(root)
        token_index = document_cache.sec_cache_html_paths_by_token_by_root.get(cache_key)
        if token_index is None:
            token_index = {}
            for path_in in self.sec_cache_html_paths_local(root):
                for token_hit in set(re_rt.findall(r"20\d{6}", path_in.name)):
                    token_index.setdefault(token_hit, []).append(path_in)
            document_cache.sec_cache_html_paths_by_token_by_root[cache_key] = token_index
        return list(token_index.get(token_txt) or [])

    def infer_doc_quarter_local(self, path_in: Any, raw_text: Any = "") -> Optional[date]:
        Path_rt = self._rt("Path", Path)
        re_rt = self._rt("re", re)
        date_rt = self._rt("date", date)
        document_cache = self._rt("document_cache")
        _parse_quarter_from_filename = self._rt("_parse_quarter_from_filename")
        _parse_quarter_from_follow_text = self._rt("_parse_quarter_from_follow_text")
        _read_cached_doc_text = self._rt("_read_cached_doc_text")
        infer_quarter_end_from_text = self._rt("infer_quarter_end_from_text")

        try:
            p = Path_rt(path_in)
        except Exception:
            return None
        try:
            path_key = str(p.resolve())
        except Exception:
            path_key = str(p)
        if path_key in document_cache.inferred_quarter_by_path:
            return document_cache.inferred_quarter_by_path.get(path_key)
        text_in = str(raw_text or "")
        qd = _parse_quarter_from_filename(p.name)
        if not isinstance(qd, date_rt):
            if not text_in:
                try:
                    text_in = _read_cached_doc_text(p)
                except Exception:
                    text_in = ""
            qd = _parse_quarter_from_follow_text(text_in) or infer_quarter_end_from_text(text_in)
        if not isinstance(qd, date_rt):
            annual_letter_match = re_rt.search(
                r"(?:^|[_-])(20\d{2})(?:[^0-9]{0,24})?(?:annualletter|shareholderletter|shareholder.?letter)\b",
                p.name,
                re_rt.I,
            )
            if annual_letter_match:
                try:
                    qd = date_rt(int(annual_letter_match.group(1)), 12, 31)
                except Exception:
                    qd = None
        qd_out = qd if isinstance(qd, date_rt) else None
        document_cache.inferred_quarter_by_path[path_key] = qd_out
        return qd_out
