"""Source material and cache root discovery support for workbook rendering."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, MutableMapping


@dataclass(frozen=True)
class SourceRootSupportDeps:
    runtime: MutableMapping[str, Any]


class SourceRootSupport:
    def __init__(self, deps: SourceRootSupportDeps) -> None:
        self.runtime = deps.runtime

    def _rt(self, name: str, default: Any = None) -> Any:
        return self.runtime.get(name, default)

    def path_within_scope(self, path_in: Any, root_in: Any) -> bool:
        Path_rt = self._rt("Path", Path)
        try:
            Path_rt(path_in).expanduser().resolve().relative_to(Path_rt(root_in).expanduser().resolve())
            return True
        except Exception:
            return False

    def company_material_roots(self, ticker_roots: list[Any]) -> list[Any]:
        Path_rt = self._rt("Path", Path)
        ticker = self._rt("ticker")
        out_path = self._rt("out_path")
        cache_dir = self._rt("cache_dir")
        repo_root = self._rt("repo_root")
        canonical_shared_cache_root = self._rt("canonical_shared_cache_root")
        _path_belongs_to_ticker = self._rt("_path_belongs_to_ticker")
        roots: List[Any] = []
        seen: set[str] = set()

        def _add_root(p: Any) -> None:
            if not _path_belongs_to_ticker(p, ticker, ticker_roots):
                return
            try:
                rp = str(p.resolve())
            except Exception:
                rp = str(p)
            if rp in seen:
                return
            seen.add(rp)
            roots.append(p)

        if out_path.parent.name.lower().endswith("model excel") and out_path.parent.parent.exists():
            _add_root(out_path.parent.parent)
        tkr = str(ticker or "").strip()
        explicit_material_scope = False
        allow_repo_material_fallback = True
        if cache_dir is not None:
            try:
                cache_base = Path_rt(cache_dir).expanduser().resolve()
            except Exception:
                cache_base = Path_rt(cache_dir)
            if tkr:
                repo_ticker_root = repo_root / tkr.upper()
                repo_shared_cache = canonical_shared_cache_root(repo_root)
                allow_repo_material_fallback = (
                    self.path_within_scope(cache_base, repo_ticker_root)
                    or self.path_within_scope(cache_base, repo_shared_cache)
                    or self.path_within_scope(repo_root, cache_base)
                )
            legacy_company_root = cache_base.parent if cache_base.name.lower() == "sec_cache" else None
            if (
                legacy_company_root is not None
                and legacy_company_root.exists()
                and str(legacy_company_root.name or "").strip().upper() == str(tkr or "").strip().upper()
            ):
                ticker_roots.append(legacy_company_root)
                _add_root(legacy_company_root)
                explicit_material_scope = True
            if tkr:
                nearby_ancestors: List[Any] = []
                for ancestor in [cache_base.parent, *list(cache_base.parents)[:4]]:
                    try:
                        ancestor_key = str(Path_rt(ancestor).expanduser().resolve())
                    except Exception:
                        ancestor_key = str(ancestor)
                    if any(str(x) == ancestor_key for x in nearby_ancestors):
                        continue
                    nearby_ancestors.append(Path_rt(ancestor))
                for ancestor in nearby_ancestors:
                    for cand in [
                        ancestor / "tickers" / tkr.upper(),
                        ancestor / "tickers" / tkr,
                        ancestor / "tickers" / tkr.lower(),
                        ancestor / tkr.upper(),
                        ancestor / tkr,
                        ancestor / tkr.lower(),
                    ]:
                        if not cand.exists() or not cand.is_dir():
                            continue
                        ticker_roots.append(cand)
                        _add_root(cand)
                        explicit_material_scope = True
        if tkr:
            for cand in [repo_root / tkr.upper(), repo_root / tkr, repo_root / tkr.lower()]:
                if not allow_repo_material_fallback:
                    break
                if not cand.exists():
                    continue
                if explicit_material_scope:
                    break
                ticker_roots.append(cand)
                _add_root(cand)
        return roots

    def is_repo_profile_cache_path(self, path_in: Any) -> bool:
        ticker = self._rt("ticker")
        profile_ticker = self._rt("profile_ticker")
        repo_root = self._rt("repo_root")
        canonical_shared_cache_root = self._rt("canonical_shared_cache_root")
        profile_key = str(profile_ticker or ticker or "").strip().upper()
        if not profile_key:
            return False
        repo_shared = canonical_shared_cache_root(repo_root)
        repo_profile_roots = [
            repo_shared,
            repo_shared / profile_key,
            repo_root / profile_key / "sec_cache",
        ]
        return any(self.path_within_scope(path_in, root) for root in repo_profile_roots)

    def allow_repo_profile_cache_fallback(self) -> bool:
        Path_rt = self._rt("Path", Path)
        cache_dir = self._rt("cache_dir")
        repo_root = self._rt("repo_root")
        ticker = self._rt("ticker")
        profile_ticker = self._rt("profile_ticker")
        canonical_shared_cache_root = self._rt("canonical_shared_cache_root")
        if cache_dir is None:
            return True
        try:
            cache_base = Path_rt(cache_dir).expanduser().resolve()
        except Exception:
            cache_base = Path_rt(cache_dir)
        if self.path_within_scope(cache_base, canonical_shared_cache_root(repo_root)):
            return True
        profile_key = str(profile_ticker or ticker or "").strip().upper()
        if profile_key and self.path_within_scope(cache_base, repo_root / profile_key / "sec_cache"):
            return True
        return self.path_within_scope(repo_root, cache_base)

    def cache_roots(self, material_roots: list[Any]) -> list[Any]:
        Path_rt = self._rt("Path", Path)
        re = self._rt("re")
        manifest_df = self._rt("manifest_df")
        ticker = self._rt("ticker")
        cache_dir = self._rt("cache_dir")
        repo_root = self._rt("repo_root")
        ticker_cache_candidates = self._rt("ticker_cache_candidates")
        ticker_cache_roots_from_base_dir = self._rt("ticker_cache_roots_from_base_dir")
        canonical_shared_cache_root = self._rt("canonical_shared_cache_root")
        roots: List[Any] = []
        seen: set[str] = set()

        def _add_root(p: Any) -> None:
            try:
                rp = str(p.resolve())
            except Exception:
                rp = str(p)
            if rp in seen:
                return
            seen.add(rp)
            roots.append(p)

        # Manifest-derived cache roots (highest confidence).
        if manifest_df is not None and not manifest_df.empty:
            pcol = None
            col_map = {str(c).strip().lower(): c for c in manifest_df.columns}
            for key in ("path", "cache_path", "file_path", "local_path"):
                if key in col_map:
                    pcol = col_map[key]
                    break
            if pcol is not None:
                for raw in manifest_df[pcol].dropna().astype(str).head(1000):
                    s = str(raw).strip()
                    if s.lower().startswith("file:///"):
                        s = s[8:]
                        if re.match(r"^[A-Za-z]:", s):
                            s = s.replace("/", "\\")
                    elif s.lower().startswith("file://"):
                        s = s[7:]
                    xp = Path_rt(s)
                    if xp.exists():
                        _add_root(xp.parent)
                    else:
                        # Keep unresolved path parent for later probing.
                        _add_root(xp.parent)

        tkr = str(ticker or "").strip()
        ticker_specific_candidates = ticker_cache_candidates(repo_root, tkr, Path_rt(cache_dir) if cache_dir is not None else None)
        if not self.allow_repo_profile_cache_fallback():
            ticker_specific_candidates = [
                cand for cand in ticker_specific_candidates if not self.is_repo_profile_cache_path(cand)
            ]
        for cand in ticker_specific_candidates:
            if cand.exists():
                _add_root(cand)
        if cache_dir is not None and tkr:
            try:
                cache_base = Path_rt(cache_dir).expanduser()
            except Exception:
                cache_base = Path_rt(cache_dir)
            nearby_ancestors = [cache_base.parent, *list(cache_base.parents)[:4]]
            for ancestor in nearby_ancestors:
                for cand in [
                    ancestor / "sec_cache" / tkr.upper(),
                    ancestor / "sec_cache" / tkr,
                    ancestor / "sec_cache" / tkr.lower(),
                    ancestor / tkr.upper() / "sec_cache",
                    ancestor / tkr / "sec_cache",
                    ancestor / tkr.lower() / "sec_cache",
                ]:
                    if cand.exists():
                        _add_root(cand)
        for root in material_roots:
            for cand in ticker_cache_roots_from_base_dir(root):
                if cand.exists():
                    _add_root(cand)

        # Only fall back to global cache roots when no ticker-specific cache root exists.
        if self.allow_repo_profile_cache_fallback() and not any(
            c.exists() and c != canonical_shared_cache_root(repo_root) for c in ticker_specific_candidates
        ):
            _add_root(canonical_shared_cache_root(repo_root))
            _add_root(Path_rt("sec_cache"))
        return roots
