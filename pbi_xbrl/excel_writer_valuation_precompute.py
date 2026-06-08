"""Valuation source/precompute support for workbook rendering.

This module owns the document-backed capital-return precompute layer used by the
Valuation sheet and latest-quarter QA. It intentionally does not render workbook
cells; context passes the run-scoped helpers and caches through a runtime mapping
so the extracted code keeps the same source/cache behavior.
"""
from __future__ import annotations

import datetime as dt
import html
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence, Tuple

import pandas as pd


@dataclass(frozen=True)
class ValuationPrecomputeDeps:
    runtime: MutableMapping[str, Any]


class ValuationPrecomputeSupport:
    def __init__(self, deps: ValuationPrecomputeDeps) -> None:
        self.deps = deps
        self.runtime = deps.runtime
        self.valuation_precompute_bundle_cache = self.runtime.get("valuation_precompute_bundle_cache")
        self.valuation_filing_docs_by_quarter_cache = self.runtime.get("valuation_filing_docs_by_quarter_cache")
        self.valuation_buyback_auth_source_bundle_cache = self.runtime.get(
            "valuation_buyback_auth_source_bundle_cache"
        )

    def cache_snapshot(self) -> Dict[str, Any]:
        return {
            "valuation_precompute_bundle_cache": self.valuation_precompute_bundle_cache,
            "valuation_filing_docs_by_quarter_cache": self.valuation_filing_docs_by_quarter_cache,
            "valuation_buyback_auth_source_bundle_cache": self.valuation_buyback_auth_source_bundle_cache,
        }

    def manifest_local_path(self, p: Any) -> Optional[Path]:
        runtime = self.runtime
        cache_roots = runtime['cache_roots']

        if p is None:
            return None
        s = str(p).strip()
        if not s:
            return None
        sl = s.lower()
        if sl.startswith("file:///"):
            s = s[8:]
            if re.match(r"^[A-Za-z]:", s):
                s = s.replace("/", "\\")
        elif sl.startswith("file://"):
            s = s[7:]
        x = Path(s)
        if x.exists():
            return x
        try_local: List[Path] = [Path.cwd() / s]
        for cr in cache_roots:
            try_local.append(cr / x.name)
            try_local.append(cr / s)
        for cand in try_local:
            if cand.exists():
                return cand
        return None

    def load_buyback_auth_source_bundle(self, manifest_src: pd.DataFrame) -> Dict[str, Any]:
        runtime = self.runtime
        _resolve_col = runtime['_resolve_col']
        _submission_cache_files = runtime['_submission_cache_files']
        _submission_recent_rows = runtime['_submission_recent_rows']

        if self.valuation_buyback_auth_source_bundle_cache is not None:
            return self.valuation_buyback_auth_source_bundle_cache

        ck_col = None
        path_col = None
        mdf = pd.DataFrame()
        if manifest_src is not None and not manifest_src.empty:
            ck_col = _resolve_col(manifest_src, ["cache_key", "cachekey", "key"])
            path_col = _resolve_col(manifest_src, ["path", "cache_path", "file_path", "local_path"])
            status_col = _resolve_col(manifest_src, ["status"])
            if ck_col is not None and path_col is not None:
                mdf = manifest_src.copy()
                if status_col is not None:
                    ok_mask = mdf[status_col].astype(str).str.lower().isin(["ok", "cached", "hit", "success"])
                    if ok_mask.any():
                        mdf = mdf[ok_mask]

        doc_path_by_key: Dict[str, Path] = {}
        manifest_docs_by_accn: Dict[str, List[Tuple[str, Path]]] = {}
        if not mdf.empty and ck_col is not None and path_col is not None:
            for _, rr in mdf.iterrows():
                k = str(rr.get(ck_col) or "").strip()
                if not k or not k.startswith("doc_"):
                    continue
                pth = self.manifest_local_path(rr.get(path_col))
                if pth is not None and pth.exists():
                    doc_path_by_key[k] = pth
                    parts = k.split("_", 2)
                    if len(parts) >= 3 and parts[1]:
                        manifest_docs_by_accn.setdefault(parts[1], []).append((k, pth))

        self.valuation_buyback_auth_source_bundle_cache = {
            "sub_candidates": _submission_cache_files(),
            "recent_rows": _submission_recent_rows(max_files=8),
            "doc_path_by_key": doc_path_by_key,
            "manifest_docs_by_accn": manifest_docs_by_accn,
            "docs_by_accn": {},
        }
        return self.valuation_buyback_auth_source_bundle_cache

    def buyback_auth_docs_for_accession(self,
        accn_nd: str,
        primary_doc_key: str,
        source_bundle: Dict[str, Any],
    ) -> List[Tuple[str, Path]]:
        runtime = self.runtime
        cache_roots = runtime['cache_roots']

        docs_cache = source_bundle.setdefault("docs_by_accn", {})
        if accn_nd in docs_cache:
            return list(docs_cache.get(accn_nd) or [])

        doc_path_by_key = dict(source_bundle.get("doc_path_by_key") or {})
        manifest_docs_by_accn = dict(source_bundle.get("manifest_docs_by_accn") or {})
        filing_docs: List[Tuple[str, Path]] = []
        seen_doc_paths: set[str] = set()

        def _add_doc(doc_key: str, doc_path: Optional[Path]) -> None:
            if doc_path is None or not doc_path.exists():
                return
            try:
                rk = str(doc_path.resolve())
            except Exception:
                rk = str(doc_path)
            if rk in seen_doc_paths:
                return
            seen_doc_paths.add(rk)
            filing_docs.append((doc_key, doc_path))

        _add_doc(primary_doc_key, doc_path_by_key.get(primary_doc_key))
        if primary_doc_key not in doc_path_by_key:
            p_alt = None
            for cr in cache_roots:
                p_alt = self.manifest_local_path(cr / primary_doc_key)
                if p_alt is not None:
                    break
            if p_alt is None:
                hits: List[Path] = []
                for cr in cache_roots:
                    hits.extend(sorted(cr.glob(f"{primary_doc_key}*")))
                p_alt = hits[0] if hits else None
            _add_doc(primary_doc_key, p_alt)

        for cached_doc in self.docs_for_valuation_accn(accn_nd):
            _add_doc(cached_doc.name, cached_doc)

        for dk, dp in manifest_docs_by_accn.get(accn_nd, []):
            _add_doc(dk, dp)

        doc_pref = f"doc_{accn_nd}_"
        for cr in cache_roots:
            for gp in sorted(cr.glob(f"{doc_pref}*")):
                _add_doc(gp.name, gp)
        for cr in cache_roots:
            ocr_f = cr / f"ocr_{accn_nd}_index_images.txt"
            if ocr_f.exists():
                _add_doc(f"ocr_{accn_nd}_index_images", ocr_f)
                break

        docs_cache[accn_nd] = list(filing_docs)
        return list(filing_docs)

    def extract_latest_buyback_remaining_from_sec(self, manifest_src: pd.DataFrame) -> Dict[str, Any]:
        runtime = self.runtime
        _fmt_short_money_value_local = runtime['_fmt_short_money_value_local']
        _infer_doc_quarter_local = runtime['_infer_doc_quarter_local']
        _path_belongs_to_ticker = runtime['_path_belongs_to_ticker']
        _read_cached_doc_text = runtime['_read_cached_doc_text']
        cache_dir = runtime['cache_dir']
        cache_roots = runtime['cache_roots']
        data_root_from_sec_cache_path = runtime['data_root_from_sec_cache_path']
        glx_normalize_text = runtime['glx_normalize_text']
        parse_date = runtime['parse_date']
        profile_ticker = runtime['profile_ticker']
        ticker = runtime['ticker']
        ticker_roots = runtime['ticker_roots']

        out: Dict[str, Any] = {
            "remaining_dollars": None,
            "asof_date": None,
            "accn": None,
            "form": None,
            "doc_key": None,
            "doc_path": None,
            "snippet": None,
        }

        def _extract_buyback_table_remaining_capacity_local(text_in: Any) -> Optional[float]:
            text = glx_normalize_text(html.unescape(str(text_in or "")).replace("\xa0", " "))
            if not text or not re.search(r"\bmay yet be purchased\b|\bremaining capacity\b", text, re.I):
                return None
            month_row_re = re.compile(
                r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+"
                r"(20\d{2})\s+([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+)\s+\$\s*([0-9]+(?:\.\d+)?)\s+"
                r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+)(?:\s+\$?([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+))?",
                re.I,
            )
            capacities: List[float] = []
            for match in month_row_re.finditer(text):
                raw_capacity = str(match.group(6) or "").replace(",", "").strip()
                if not raw_capacity:
                    continue
                try:
                    value = float(raw_capacity) * 1_000.0
                except Exception:
                    continue
                if value > 0:
                    capacities.append(value)
            return float(capacities[-1]) if capacities else None

        source_bundle = self.load_buyback_auth_source_bundle(manifest_src)
        recent_rows = list(source_bundle.get("recent_rows") or [])
        if not recent_rows:
            print("[buyback_auth] no submissions cache file found", flush=True)

        def _buyback_auth_cache_path_local() -> Optional[Path]:
            if cache_dir is not None:
                try:
                    cache_base = Path(cache_dir).expanduser().resolve()
                except Exception:
                    cache_base = Path(cache_dir)
                portable_root = data_root_from_sec_cache_path(cache_base)
                if portable_root is not None:
                    return portable_root / "writer_cache" / "buyback_auth_remaining_cache.pkl"
                ticker_token = str(profile_ticker or ticker or "").strip().upper()
                try:
                    cache_parts = {str(part).strip().upper() for part in cache_base.parts}
                except Exception:
                    cache_parts = set()
                if ticker_token and ticker_token in cache_parts:
                    return cache_base / "writer_cache" / "buyback_auth_remaining_cache.pkl"
            for root_in in ticker_roots:
                try:
                    root_path = Path(root_in)
                except Exception:
                    continue
                if root_path.exists() and root_path.is_dir() and _path_belongs_to_ticker(root_path, ticker, ticker_roots):
                    return root_path / "writer_cache" / "buyback_auth_remaining_cache.pkl"
            return None

        def _buyback_auth_file_token(path_in: Path) -> Tuple[str, int, int]:
            try:
                resolved = str(path_in.expanduser().resolve())
            except Exception:
                resolved = str(path_in)
            try:
                st = path_in.stat()
                return (resolved, int(getattr(st, "st_size", 0) or 0), int(getattr(st, "st_mtime_ns", 0) or 0))
            except Exception:
                return (resolved, -1, -1)

        def _buyback_auth_direct_doc_payload_local() -> List[Tuple[str, int, int]]:
            doc_payload_direct: List[Tuple[str, int, int]] = []
            for cr in cache_roots:
                if not cr.exists() or not cr.is_dir():
                    continue
                for doc_path in sorted(cr.glob("doc_*")):
                    sfx = str(doc_path.suffix or "").lower()
                    if sfx not in {".htm", ".html", ".txt", ".xml"}:
                        continue
                    if not _path_belongs_to_ticker(doc_path, ticker, ticker_roots):
                        continue
                    doc_payload_direct.append(_buyback_auth_file_token(doc_path))
            return list(sorted(set(doc_payload_direct)))

        def _buyback_auth_direct_cache_key_local() -> Optional[Dict[str, Any]]:
            doc_payload_direct = _buyback_auth_direct_doc_payload_local()
            if not doc_payload_direct:
                return None
            return {
                "version": "buyback_auth_remaining_cache_v1_direct_docs",
                "ticker": str(profile_ticker or ticker or "").upper(),
                "rows": tuple(),
                "docs": tuple(doc_payload_direct),
            }

        def _buyback_auth_cache_key_local() -> Optional[Dict[str, Any]]:
            if not recent_rows:
                return _buyback_auth_direct_cache_key_local()
            row_payload: List[Tuple[str, str, str, str, str]] = []
            doc_payload: List[Tuple[str, int, int]] = []
            for row in recent_rows[:8]:
                form = str(row.get("form") or "").upper().strip()
                if not (form.startswith("10-Q") or form.startswith("10-K") or form.startswith("8-K")):
                    continue
                accn = str(row.get("accn") or "").strip()
                primary = str(row.get("doc") or "").strip()
                if not accn or not primary:
                    continue
                row_payload.append(
                    (
                        accn,
                        form,
                        str(row.get("filed") or ""),
                        str(row.get("report") or ""),
                        primary,
                    )
                )
                accn_nd = accn.replace("-", "")
                dkey_primary = f"doc_{accn_nd}_{primary}"
                for doc_key, doc_path in self.buyback_auth_docs_for_accession(accn_nd, dkey_primary, source_bundle):
                    sfx = str(doc_path.suffix or "").lower()
                    if sfx not in {".htm", ".html", ".txt", ".xml"} and "ocr_" not in str(doc_key).lower():
                        continue
                    doc_payload.append(_buyback_auth_file_token(doc_path))
            if not row_payload:
                return _buyback_auth_direct_cache_key_local()
            return {
                "version": "buyback_auth_remaining_cache_v1",
                "ticker": str(profile_ticker or ticker or "").upper(),
                "rows": tuple(row_payload),
                "docs": tuple(sorted(set(doc_payload))),
            }

        buyback_auth_cache_key = _buyback_auth_cache_key_local()
        buyback_auth_cache_path = _buyback_auth_cache_path_local()
        if buyback_auth_cache_key is not None and buyback_auth_cache_path is not None and buyback_auth_cache_path.exists():
            try:
                payload_cached = pd.read_pickle(buyback_auth_cache_path)
                if isinstance(payload_cached, dict) and payload_cached.get("cache_key") == buyback_auth_cache_key:
                    cached_out = payload_cached.get("result")
                    if isinstance(cached_out, dict):
                        return dict(cached_out)
            except Exception:
                pass

        all_candidates: List[Dict[str, Any]] = []

        def _build_auth_candidate(
            *,
            amount: Optional[float],
            snippet: Any,
            kind: str,
            base_score: int,
            asof_date: Optional[date],
            accn: str,
            form: str,
            doc_key: str,
            doc_path: Path,
            filing_ref_date: Optional[date],
        ) -> Optional[Dict[str, Any]]:
            if amount is None:
                return None
            snip = str(snippet or "").strip()
            snip_low = snip.lower()
            if "repurchase" not in snip_low and "buyback" not in snip_low:
                return None
            asof_d = asof_date or filing_ref_date
            if kind.startswith("authorization") and asof_d is not None and filing_ref_date is not None:
                if (filing_ref_date - asof_d).days > 120 and ("remaining" not in snip_low and "remained" not in snip_low):
                    return None
            score = int(base_score)
            if "remaining" in snip_low or "remained" in snip_low:
                score += 4
            if "authorized" in snip_low or "authorization" in snip_low:
                score += 2
            if "available" in snip_low:
                score += 1
            if "new" in snip_low:
                score += 1
            if "board" in snip_low:
                score += 1
            if "as of" in snip_low or "on " in snip_low:
                score += 1
            return {
                "remaining_dollars": float(amount),
                "authorization_dollars": (float(amount) if kind.startswith("authorization") else None),
                "asof_date": asof_d,
                "accn": accn,
                "form": form,
                "doc_key": doc_key,
                "doc_path": str(doc_path),
                "snippet": snip[:360],
                "kind": kind,
                "_score": score,
            }

        if not recent_rows:
            direct_docs: List[Path] = []
            seen_direct_docs: set[str] = set()
            for cr in cache_roots:
                if not cr.exists() or not cr.is_dir():
                    continue
                for doc_path in sorted(cr.glob("doc_*")):
                    if doc_path.suffix.lower() not in {".htm", ".html", ".txt", ".xml"}:
                        continue
                    if not _path_belongs_to_ticker(doc_path, ticker, ticker_roots):
                        continue
                    try:
                        doc_key = str(doc_path.resolve()).lower()
                    except Exception:
                        doc_key = str(doc_path).lower()
                    if doc_key in seen_direct_docs:
                        continue
                    seen_direct_docs.add(doc_key)
                    direct_docs.append(doc_path)
            for doc_path in direct_docs:
                try:
                    doc_text = _read_cached_doc_text(doc_path)
                except Exception:
                    doc_text = ""
                if not doc_text:
                    try:
                        doc_text = doc_path.read_text(encoding="utf-8", errors="ignore")
                    except Exception:
                        doc_text = ""
                filing_ref_date = _infer_doc_quarter_local(doc_path, doc_text)
                accn_match = re.match(r"doc_([0-9]{18})_", doc_path.name, re.I)
                accn_direct = str(accn_match.group(1) if accn_match else "").strip()
                table_remaining = _extract_buyback_table_remaining_capacity_local(doc_text)
                if table_remaining is not None:
                    cand = _build_auth_candidate(
                        amount=table_remaining,
                        snippet=(
                            "Share repurchase table remaining capacity "
                            f"{_fmt_short_money_value_local(float(table_remaining))}."
                        ),
                        kind="remaining_table",
                        base_score=12,
                        asof_date=filing_ref_date,
                        accn=accn_direct,
                        form="",
                        doc_key=doc_path.name,
                        doc_path=doc_path,
                        filing_ref_date=filing_ref_date,
                    )
                    if cand is not None:
                        cand["_filing_date"] = filing_ref_date
                        cand["_kind_pri"] = 4
                        all_candidates.append(cand)
                analysis = self.analyze_cap_alloc_doc(doc_path, include_core=False, include_auth_details=True)
                if not analysis.get("mentions_cap_alloc"):
                    continue
                for cand_in in list(analysis.get("remaining_candidates") or []):
                    cand = _build_auth_candidate(
                        amount=cand_in.get("amount"),
                        snippet=cand_in.get("snippet"),
                        kind="remaining",
                        base_score=9,
                        asof_date=cand_in.get("asof_date"),
                        accn=accn_direct,
                        form="",
                        doc_key=doc_path.name,
                        doc_path=doc_path,
                        filing_ref_date=filing_ref_date,
                    )
                    if cand is not None:
                        cand["_filing_date"] = filing_ref_date
                        cand["_kind_pri"] = 3
                        all_candidates.append(cand)
                for cand_in in list(analysis.get("authorization_candidates") or []):
                    kind = str(cand_in.get("kind") or "authorization")
                    cand = _build_auth_candidate(
                        amount=cand_in.get("amount"),
                        snippet=cand_in.get("snippet"),
                        kind=kind,
                        base_score=3,
                        asof_date=cand_in.get("asof_date"),
                        accn=accn_direct,
                        form="",
                        doc_key=doc_path.name,
                        doc_path=doc_path,
                        filing_ref_date=filing_ref_date,
                    )
                    if cand is not None:
                        cand["_filing_date"] = filing_ref_date
                        cand["_kind_pri"] = 2 if "increase_to" in kind else 1
                        all_candidates.append(cand)

        for row in recent_rows:
            form = str(row.get("form") or "").upper().strip()
            if not (form.startswith("10-Q") or form.startswith("10-K") or form.startswith("8-K")):
                continue
            accn = str(row.get("accn") or "").strip()
            primary = str(row.get("doc") or "").strip()
            if not accn or not primary:
                continue
            accn_nd = accn.replace("-", "")
            dkey_primary = f"doc_{accn_nd}_{primary}"
            filing_docs = self.buyback_auth_docs_for_accession(accn_nd, dkey_primary, source_bundle)
            if not filing_docs:
                continue

            filing_ref_date = parse_date(row.get("report")) or parse_date(row.get("filed"))
            best_remaining: Optional[Dict[str, Any]] = None
            best_auth: Optional[Dict[str, Any]] = None
            best_spent_since_start: Optional[Dict[str, Any]] = None

            for doc_key, doc_path in filing_docs:
                sfx = str(doc_path.suffix or "").lower()
                if sfx not in {".htm", ".html", ".txt", ".xml"} and "ocr_" not in doc_key.lower():
                    continue
                try:
                    doc_text_for_table = _read_cached_doc_text(doc_path)
                except Exception:
                    doc_text_for_table = ""
                if not doc_text_for_table:
                    try:
                        doc_text_for_table = doc_path.read_text(encoding="utf-8", errors="ignore")
                    except Exception:
                        doc_text_for_table = ""
                table_remaining = _extract_buyback_table_remaining_capacity_local(doc_text_for_table)
                if table_remaining is not None:
                    cand = _build_auth_candidate(
                        amount=table_remaining,
                        snippet=(
                            "Share repurchase table remaining capacity "
                            f"{_fmt_short_money_value_local(float(table_remaining))}."
                        ),
                        kind="remaining_table",
                        base_score=12,
                        asof_date=filing_ref_date,
                        accn=accn,
                        form=form,
                        doc_key=doc_key,
                        doc_path=doc_path,
                        filing_ref_date=filing_ref_date,
                    )
                    if cand is not None:
                        cand["_filing_date"] = filing_ref_date
                        cand["_kind_pri"] = 4
                        if best_remaining is None or int(cand["_score"]) > int(best_remaining["_score"]):
                            best_remaining = cand
                analysis = self.analyze_cap_alloc_doc(doc_path, include_core=False, include_auth_details=True)
                if not analysis.get("mentions_cap_alloc"):
                    continue

                for cand_in in list(analysis.get("remaining_candidates") or []):
                    cand = _build_auth_candidate(
                        amount=cand_in.get("amount"),
                        snippet=cand_in.get("snippet"),
                        kind="remaining",
                        base_score=10,
                        asof_date=cand_in.get("asof_date"),
                        accn=accn,
                        form=form,
                        doc_key=doc_key,
                        doc_path=doc_path,
                        filing_ref_date=filing_ref_date,
                    )
                    if cand is None:
                        continue
                    if best_remaining is None or int(cand["_score"]) > int(best_remaining["_score"]):
                        best_remaining = cand

                for cand_in in list(analysis.get("authorization_candidates") or []):
                    kind = str(cand_in.get("kind") or "authorization")
                    cand = _build_auth_candidate(
                        amount=cand_in.get("amount"),
                        snippet=cand_in.get("snippet"),
                        kind=kind,
                        base_score=4,
                        asof_date=cand_in.get("asof_date"),
                        accn=accn,
                        form=form,
                        doc_key=doc_key,
                        doc_path=doc_path,
                        filing_ref_date=filing_ref_date,
                    )
                    if cand is None:
                        continue
                    if kind == "authorization_increase_by":
                        cand["authorization_increase_dollars"] = cand.get("remaining_dollars")
                        cand["authorization_dollars"] = None
                        cand["remaining_dollars"] = None
                    if best_auth is None or int(cand["_score"]) > int(best_auth["_score"]):
                        best_auth = cand

                for cand_in in list(analysis.get("spent_since_start_candidates") or []):
                    spent_amt = cand_in.get("spent_dollars")
                    if spent_amt is None:
                        continue
                    cand = {
                        "spent_dollars": float(spent_amt),
                        "snippet": str(cand_in.get("snippet") or "")[:360],
                        "doc_key": doc_key,
                        "doc_path": str(doc_path),
                        "_score": int(cand_in.get("_score") or 0),
                    }
                    if (
                        best_spent_since_start is None
                        or int(cand["_score"]) > int(best_spent_since_start["_score"])
                    ):
                        best_spent_since_start = cand

            if best_remaining is not None:
                if best_auth is not None:
                    if best_auth.get("authorization_dollars") is not None:
                        best_remaining["authorization_dollars"] = best_auth.get("authorization_dollars")
                    if best_auth.get("authorization_increase_dollars") is not None:
                        best_remaining["authorization_increase_dollars"] = best_auth.get("authorization_increase_dollars")
                best_remaining["_filing_date"] = filing_ref_date
                best_remaining["_kind_pri"] = 3
                all_candidates.append(best_remaining)
                continue

            if best_auth is not None:
                if best_spent_since_start is not None and "increase_by" not in str(best_auth.get("kind") or "").lower():
                    auth_amt = best_auth.get("remaining_dollars")
                    spent_amt = best_spent_since_start.get("spent_dollars")
                    if auth_amt is not None and spent_amt is not None:
                        best_auth["authorization_dollars"] = float(auth_amt)
                        best_auth["spent_since_start_dollars"] = float(spent_amt)
                        best_auth["remaining_dollars"] = max(float(auth_amt) - float(spent_amt), 0.0)
                        best_auth["kind"] = "authorization_less_since_start"
                        best_auth["snippet"] = (
                            f"{str(best_auth.get('snippet') or '').strip()} | {str(best_spent_since_start.get('snippet') or '').strip()}"
                        )[:360]
                kkind = str(best_auth.get("kind") or "")
                best_auth["_filing_date"] = filing_ref_date
                best_auth["_kind_pri"] = 2 if "less_since_start" in kkind or "increase_to" in kkind else 1
                all_candidates.append(best_auth)

        if all_candidates:
            def _cand_sort_key(c: Dict[str, Any]) -> Tuple[pd.Timestamp, int, int]:
                fd = c.get("_filing_date")
                fdt = pd.Timestamp(fd) if fd is not None else pd.Timestamp("1900-01-01")
                return (
                    fdt,
                    int(c.get("_kind_pri") or 0),
                    int(c.get("_score") or 0),
                )

            best = sorted(all_candidates, key=_cand_sort_key, reverse=True)[0]
            out.update({k: v for k, v in best.items() if not str(k).startswith("_")})
            rem_m = float(out["remaining_dollars"]) / 1e6 if out.get("remaining_dollars") is not None else float("nan")
            print(
                f"[buyback_auth] match form={out.get('form')} accn={out.get('accn')} doc={Path(str(out.get('doc_path') or '')).name} "
                f"kind={out.get('kind')} remaining={rem_m:.1f}m asof={out.get('asof_date')}",
                flush=True,
            )
            if buyback_auth_cache_key is not None and buyback_auth_cache_path is not None:
                try:
                    buyback_auth_cache_path.parent.mkdir(parents=True, exist_ok=True)
                    pd.to_pickle(
                        {
                            "cache_key": buyback_auth_cache_key,
                            "saved_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                            "result": dict(out),
                        },
                        buyback_auth_cache_path,
                    )
                except Exception:
                    pass
            return out

        print("[buyback_auth] no remaining/authorization buyback match in recent SEC filings", flush=True)
        if buyback_auth_cache_key is not None and buyback_auth_cache_path is not None:
            try:
                buyback_auth_cache_path.parent.mkdir(parents=True, exist_ok=True)
                pd.to_pickle(
                    {
                        "cache_key": buyback_auth_cache_key,
                        "saved_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                        "result": dict(out),
                    },
                    buyback_auth_cache_path,
                )
            except Exception:
                pass
        return out

    def read_valuation_doc_raw(self, path_in: Path) -> str:
        runtime = self.runtime
        _read_cached_doc_raw = runtime['_read_cached_doc_raw']

        return _read_cached_doc_raw(path_in)

    def extract_valuation_filing_doc_text(self, path_in: Path) -> str:
        runtime = self.runtime
        _path_cache_key = runtime['_path_cache_key']
        _read_cached_doc_text = runtime['_read_cached_doc_text']
        runtime_extract_valuation_filing_doc_text = runtime['runtime_extract_valuation_filing_doc_text']
        valuation_precompute_runtime = runtime['valuation_precompute_runtime']

        cache_key = _path_cache_key(path_in)
        cached = valuation_precompute_runtime.filing_doc_text_cache.get(cache_key)
        if cached is not None:
            return cached
        text = _read_cached_doc_text(path_in)
        xbrl_blob = bool(
            text
            and len(str(text)) >= 400
            and (
                len(re.findall(r"\b(?:us-gaap|xbrli|iso4217|dei|srt|xlink|link):", str(text), re.I)) >= 5
                or (
                    len(re.findall(r"\b(?:us-gaap|xbrli|iso4217|dei|srt|xlink|link):", str(text), re.I)) >= 2
                    and len(re.findall(r"\b20\d{2}(?:-\d{2}-\d{2})?\b", str(text), re.I)) >= 20
                )
            )
        )
        if not xbrl_blob:
            valuation_precompute_runtime.filing_doc_text_cache[cache_key] = text
            return text
        raw_plain = runtime_extract_valuation_filing_doc_text(
            valuation_precompute_runtime,
            path_in,
            path_cache_key=_path_cache_key,
            read_cached_doc_raw=self.read_valuation_doc_raw,
        )
        if raw_plain:
            return raw_plain
        valuation_precompute_runtime.filing_doc_text_cache[cache_key] = text
        return text

    def docs_for_valuation_accn(self, accn_in: str) -> List[Path]:
        runtime = self.runtime
        _sec_docs_for_accession = runtime['_sec_docs_for_accession']
        source_docs_for_valuation_accn = runtime['source_docs_for_valuation_accn']

        return source_docs_for_valuation_accn(accn_in, accession_doc_lookup=_sec_docs_for_accession)

    def load_filing_docs_by_quarter(self,
        qs_local: Tuple[pd.Timestamp, ...],
        audit_df: pd.DataFrame,
    ) -> Dict[pd.Timestamp, List[Dict[str, Any]]]:
        runtime = self.runtime
        _resolve_col = runtime['_resolve_col']
        _submission_recent_rows = runtime['_submission_recent_rows']
        cache_root = runtime['cache_root']
        ctx_ref = runtime['ctx_ref']
        source_build_valuation_filing_docs_by_quarter = runtime['source_build_valuation_filing_docs_by_quarter']

        quarter_key = tuple(pd.Timestamp(q).normalize() for q in qs_local if pd.notna(q))
        if (
            self.valuation_filing_docs_by_quarter_cache is not None
            and tuple(self.valuation_filing_docs_by_quarter_cache.get("quarter_key") or ()) == quarter_key
        ):
            docs_by_quarter = dict(self.valuation_filing_docs_by_quarter_cache.get("docs_by_quarter") or {})
            if ctx_ref is not None:
                ctx_ref.derived.valuation_filing_docs_by_quarter = docs_by_quarter
            return docs_by_quarter

        docs_by_quarter = source_build_valuation_filing_docs_by_quarter(
            quarter_key,
            audit_df,
            cache_root=cache_root,
            resolve_col=_resolve_col,
            submission_recent_rows_fn=_submission_recent_rows,
            docs_for_valuation_accn_fn=self.docs_for_valuation_accn,
            extract_doc_text_fn=self.extract_valuation_filing_doc_text,
        )
        filtered_docs_by_quarter: Dict[pd.Timestamp, List[Dict[str, Any]]] = {}
        for qts, rows in dict(docs_by_quarter or {}).items():
            kept_rows: List[Dict[str, Any]] = []
            for rec in list(rows or []):
                rec_text = str(rec.get("text") or "")
                xbrl_blob = bool(
                    rec_text
                    and len(rec_text) >= 400
                    and (
                        len(re.findall(r"\b(?:us-gaap|xbrli|iso4217|dei|srt|xlink|link):", rec_text, re.I)) >= 5
                        or (
                            len(re.findall(r"\b(?:us-gaap|xbrli|iso4217|dei|srt|xlink|link):", rec_text, re.I)) >= 2
                            and len(re.findall(r"\b20\d{2}(?:-\d{2}-\d{2})?\b", rec_text, re.I)) >= 20
                        )
                    )
                )
                valuation_signal = bool(
                    re.search(
                        r"(?:issuer purchases of equity securities|common stock purchases during the three months ended|"
                        r"average price paid per share|repurchas\w*|buyback|dividends?\s+paid|"
                        r"net leverage|interest coverage)",
                        rec_text,
                        re.I,
                    )
                )
                if xbrl_blob and not valuation_signal:
                    continue
                kept_rows.append(rec)
            filtered_docs_by_quarter[pd.Timestamp(qts)] = kept_rows
        docs_by_quarter = filtered_docs_by_quarter
        self.valuation_filing_docs_by_quarter_cache = {
            "quarter_key": quarter_key,
            "docs_by_quarter": docs_by_quarter,
        }
        if ctx_ref is not None:
            ctx_ref.derived.valuation_filing_docs_by_quarter = docs_by_quarter
        return docs_by_quarter

    def extract_cap_alloc_text_maps_by_quarter(self,
        src_df: pd.DataFrame,
        text_cols_aliases: List[str],
    ) -> Dict[str, Dict[pd.Timestamp, Any]]:
        runtime = self.runtime
        _resolve_col = runtime['_resolve_col']

        out_buyback: Dict[pd.Timestamp, Any] = {}
        out_buyback_shares: Dict[pd.Timestamp, Any] = {}
        out_dividend: Dict[pd.Timestamp, Any] = {}
        if src_df is None or src_df.empty:
            return {
                "buyback_map": out_buyback,
                "buyback_shares_map": out_buyback_shares,
                "dividend_map": out_dividend,
            }
        qcol = _resolve_col(src_df, ["quarter", "quarter_end", "as_of_quarter", "period_end"])
        if qcol is None:
            return {
                "buyback_map": out_buyback,
                "buyback_shares_map": out_buyback_shares,
                "dividend_map": out_dividend,
            }
        resolved_text_cols: List[str] = []
        for alias in text_cols_aliases:
            cc = _resolve_col(src_df, [alias])
            if cc is not None and cc not in resolved_text_cols:
                resolved_text_cols.append(cc)
        if not resolved_text_cols:
            return {
                "buyback_map": out_buyback,
                "buyback_shares_map": out_buyback_shares,
                "dividend_map": out_dividend,
            }

        cols = [qcol] + resolved_text_cols
        frame = src_df.loc[:, cols]
        idx_map = {col: idx for idx, col in enumerate(frame.columns)}
        buyback_kw_re = re.compile(r"buyback|repurchase|repurchased|share\s+repurchase|treasury\s+stock", re.I)
        dividend_kw_re = re.compile(r"dividend|dividends", re.I)
        amount_re = re.compile(
            r"(?:\$\s*)?([0-9]{1,3}(?:,[0-9]{3})+(?:\.\d+)?|[0-9]+(?:\.\d+)?)(?:\s*(million|billion|m|bn))?",
            re.I,
        )
        shares_re = re.compile(
            r"([0-9]{1,3}(?:,[0-9]{3})+(?:\.\d+)?|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?\s+shares",
            re.I,
        )

        def _parse_amounts(blob: str) -> List[float]:
            vals: List[float] = []
            for m in amount_re.finditer(blob):
                raw_n = str(m.group(1) or "").replace(",", "")
                try:
                    n = float(raw_n)
                except Exception:
                    continue
                unit = str(m.group(2) or "").lower()
                if unit in {"billion", "bn"}:
                    n *= 1e9
                elif unit in {"million", "m"}:
                    n *= 1e6
                elif n < 100000:
                    continue
                if 0 <= abs(n) <= 10e9:
                    vals.append(float(n))
            return vals

        def _parse_share_amounts(blob: str) -> List[float]:
            vals: List[float] = []
            for m in shares_re.finditer(blob):
                raw_n = str(m.group(1) or "").replace(",", "")
                try:
                    n = float(raw_n)
                except Exception:
                    continue
                unit = str(m.group(2) or "").lower()
                if unit in {"billion", "bn"}:
                    n *= 1e9
                elif unit in {"million", "m"}:
                    n *= 1e6
                elif n < 1000:
                    continue
                vals.append(float(n))
            return vals

        for row in frame.itertuples(index=False, name=None):
            q_val = pd.to_datetime(row[idx_map[qcol]], errors="coerce")
            if pd.isna(q_val):
                continue
            text_parts = []
            for tc in resolved_text_cols:
                v = row[idx_map[tc]]
                if isinstance(v, str) and v.strip():
                    text_parts.append(v.strip())
            if not text_parts:
                continue
            blob = " ".join(text_parts)
            qts = pd.Timestamp(q_val)
            if buyback_kw_re.search(blob):
                try:
                    exec_summary = _compose_buyback_execution_summary_local(blob, qts.date())
                except NameError:
                    exec_summary = ""
                try:
                    exec_parts = _extract_buyback_execution_components_local(blob, qts.date()) if exec_summary else {}
                except NameError:
                    exec_parts = {}
                exec_amount = pd.to_numeric(exec_parts.get("amount"), errors="coerce")
                exec_shares = pd.to_numeric(exec_parts.get("shares"), errors="coerce")
                if pd.notna(exec_amount):
                    out_buyback[qts] = float(exec_amount)
                if pd.notna(exec_shares):
                    out_buyback_shares[qts] = float(exec_shares)
            if dividend_kw_re.search(blob) and self.classify_distribution_signal_local(blob, "") == "common_dividend":
                dividend_vals = _parse_amounts(blob)
                if dividend_vals:
                    out_dividend[qts] = max(dividend_vals, key=lambda x: abs(x))

        return {
            "buyback_map": out_buyback,
            "buyback_shares_map": out_buyback_shares,
            "dividend_map": out_dividend,
        }

    def cap_alloc_unit_mult(self, txt_low: str) -> float:
        runtime = self.runtime
        runtime_cap_alloc_unit_mult = runtime['runtime_cap_alloc_unit_mult']

        return runtime_cap_alloc_unit_mult(txt_low)

    def extract_cap_alloc_row_cash(self, txt: str, row_pat: str, mult: float) -> Optional[float]:
        runtime = self.runtime
        runtime_extract_cap_alloc_row_cash = runtime['runtime_extract_cap_alloc_row_cash']

        return runtime_extract_cap_alloc_row_cash(txt, row_pat, mult)

    def is_debt_repurchase_noise_local(self, text_in: Any) -> bool:
        runtime = self.runtime
        runtime_is_debt_repurchase_noise = runtime['runtime_is_debt_repurchase_noise']

        return runtime_is_debt_repurchase_noise(text_in)

    def extract_cap_alloc_quarter_cash_sentence(self,
        txt: str,
        kw_pat: str,
        must_have_pat: Optional[str] = None,
        deny_pat: Optional[str] = None,
    ) -> Tuple[Optional[float], Optional[str]]:
        runtime = self.runtime
        runtime_extract_cap_alloc_quarter_cash_sentence = runtime['runtime_extract_cap_alloc_quarter_cash_sentence']

        return runtime_extract_cap_alloc_quarter_cash_sentence(
            txt,
            kw_pat,
            must_have_pat=must_have_pat,
            deny_pat=deny_pat,
        )

    def parse_cap_alloc_amount(self, raw_num: str, unit: str) -> Optional[float]:
        runtime = self.runtime
        runtime_parse_cap_alloc_amount = runtime['runtime_parse_cap_alloc_amount']

        return runtime_parse_cap_alloc_amount(raw_num, unit)

    def classify_distribution_signal_local(self, note_text: str, source_hint: str = "") -> str:
        runtime = self.runtime
        runtime_classify_distribution_signal = runtime['runtime_classify_distribution_signal']

        return runtime_classify_distribution_signal(note_text, source_hint)

    def is_cumulative_buyback_context_local(self, text_in: Any) -> bool:
        runtime = self.runtime
        runtime_is_cumulative_buyback_context = runtime['runtime_is_cumulative_buyback_context']

        return runtime_is_cumulative_buyback_context(text_in)

    def buyback_execution_scope_text_local(self,
        text_in: Any,
        qd_ref: Optional[date] = None,
    ) -> str:
        runtime = self.runtime
        runtime_buyback_execution_scope_text = runtime['runtime_buyback_execution_scope_text']

        return runtime_buyback_execution_scope_text(text_in, qd_ref)

    def has_buyback_execution_table_context_local(self, text_in: Any) -> bool:
        runtime = self.runtime
        runtime_has_buyback_execution_table_context = runtime['runtime_has_buyback_execution_table_context']

        return runtime_has_buyback_execution_table_context(text_in)

    def has_quarter_execution_scope_local(self, text_in: Any, qd_ref: Optional[date]) -> bool:
        runtime = self.runtime
        glx_normalize_text = runtime['glx_normalize_text']

        text = glx_normalize_text(str(text_in or ""))
        if not text:
            return False
        if re.search(r"\bduring\s+the\s+(?:three\s+months|quarter)\b", text, re.I):
            return True
        if re.search(r"\bfor\s+the\s+three\s+months\s+ended\b", text, re.I):
            return True
        if not isinstance(qd_ref, date):
            return False
        q_num = ((qd_ref.month - 1) // 3) + 1
        quarter_word = {1: "first", 2: "second", 3: "third", 4: "fourth"}.get(q_num, "")
        if re.search(rf"\bin\s+Q{q_num}\b", text, re.I):
            return True
        if quarter_word and re.search(rf"\b(?:in\s+the\s+)?{quarter_word}\s+quarter\b", text, re.I):
            return True
        return False

    def analyze_cap_alloc_doc(self,
        path_in: Path,
        *,
        text: Optional[str] = None,
        include_core: bool = True,
        include_auth_details: bool = False,
    ) -> Dict[str, Any]:
        runtime = self.runtime
        _path_cache_key = runtime['_path_cache_key']
        valuation_precompute_runtime = runtime['valuation_precompute_runtime']

        cache_key = _path_cache_key(path_in)
        analysis = valuation_precompute_runtime.cap_alloc_doc_analysis_cache.get(cache_key)
        if analysis is None:
            txt = str(text or self.extract_valuation_filing_doc_text(path_in) or "")
            txt = re.sub(r"\s+", " ", txt).strip()
            txt_low = txt.lower()
            analysis = {
                "text": txt,
                "text_low": txt_low,
                "name_low": path_in.name.lower(),
                "mentions_cap_alloc": bool(
                    "repurchase" in txt_low
                    or "buyback" in txt_low
                    or "dividend" in txt_low
                    or "authorization" in txt_low
                    or "bought back" in txt_low
                ),
                "buyback_quarter_sentence_amount": None,
                "buyback_quarter_sentence_text": None,
                "buyback_row_cash": None,
                "dividend_row_cash": None,
                "dividend_ps_candidates": [],
                "buyback_note_text": None,
                "buyback_execution_candidates": [],
                "dividend_note_text": None,
                "remaining_candidates": [],
                "authorization_candidates": [],
                "spent_since_start_candidates": [],
                "_core_ready": False,
                "_auth_details_ready": False,
            }
            valuation_precompute_runtime.cap_alloc_doc_analysis_cache[cache_key] = analysis

        if (
            (not include_core or bool(analysis.get("_core_ready")))
            and (not include_auth_details or bool(analysis.get("_auth_details_ready")))
        ):
            return analysis

        txt = str(analysis.get("text") or "")
        txt_low = str(analysis.get("text_low") or txt.lower())
        if not txt or not analysis.get("mentions_cap_alloc"):
            if include_core:
                analysis["_core_ready"] = True
            if include_auth_details:
                analysis["_auth_details_ready"] = True
            valuation_precompute_runtime.cap_alloc_doc_analysis_cache[cache_key] = analysis
            return analysis

        if include_core and not bool(analysis.get("_core_ready")):
            mult = self.cap_alloc_unit_mult(txt_low)
            bb, bb_sent = self.extract_cap_alloc_quarter_cash_sentence(
                txt,
                r"(repurchas|buyback|common\s+stock\s+repurchases?)",
                must_have_pat=r"\b(repurchased|repurchasing|bought\s+back|at\s+total\s+cost|cash\s+flow\s+into\s+repurchasing|spent)\b",
                deny_pat=r"\b(authoriz|capacity|remaining|increased?\s+.*repurchase\s+authorization)\b",
            )
            if bb is not None and self.is_cumulative_buyback_context_local(bb_sent or ""):
                bb = None
                bb_sent = None
            analysis["buyback_quarter_sentence_amount"] = bb
            analysis["buyback_quarter_sentence_text"] = bb_sent
            if bb is None:
                analysis["buyback_row_cash"] = self.extract_cap_alloc_row_cash(
                    txt,
                    r"(repurchase(?:s)?\s+of\s+common\s+stock|common\s+stock\s+repurchases?)",
                    mult,
                )
            try:
                buyback_exec_summary = _compose_buyback_execution_summary_local(txt)
            except NameError:
                buyback_exec_summary = ""
            if buyback_exec_summary:
                try:
                    exec_parts = _extract_buyback_execution_components_local(txt)
                except NameError:
                    exec_parts = {}
                analysis["buyback_execution_candidates"] = [
                    {
                        "summary": buyback_exec_summary,
                        "shares": exec_parts.get("shares"),
                        "amount": exec_parts.get("amount"),
                        "avg_price": exec_parts.get("avg_price"),
                        "quarter_scoped": bool(exec_parts.get("quarter_scoped")),
                        "from_table": bool(exec_parts.get("from_table")),
                        "has_avg_price": exec_parts.get("avg_price") is not None,
                        "has_share_count": exec_parts.get("shares") is not None,
                        "has_amount": exec_parts.get("amount") is not None,
                        "explicit_count": int(exec_parts.get("explicit_count") or 0),
                    }
                ]
            dividend_row_cash = self.extract_cap_alloc_row_cash(
                txt,
                r"dividends?\s+paid(?:\s*\([^)]*\))?",
                mult,
            )
            if self.classify_distribution_signal_local(txt, path_in) == "common_dividend":
                analysis["dividend_row_cash"] = dividend_row_cash

            dps_patterns: List[Tuple[re.Pattern[str], int]] = [
                (
                    re.compile(
                        r"(?:board[^.]{0,120}(?:approved|declared)|declared|approved)[^.]{0,160}?"
                        r"(?:regular\s+)?quarterly\s+(?:cash\s+)?dividend[^.]{0,120}?\$?\s*([0-9]+(?:\.\d+)?)\s*"
                        r"(?:per\s+(?:common\s+)?share|a\s+share)",
                        re.I,
                    ),
                    20,
                ),
                (
                    re.compile(
                        r"(?:regular\s+)?quarterly\s+(?:cash\s+)?dividend[^.]{0,120}?\$?\s*([0-9]+(?:\.\d+)?)\s*"
                        r"(?:per\s+(?:common\s+)?share|a\s+share)",
                        re.I,
                    ),
                    16,
                ),
                (
                    re.compile(
                        r"dividends?\s+paid\s*\(\$?\s*([0-9]+(?:\.\d+)?)\s*(?:per\s+(?:common\s+)?share|a\s+share)\)",
                        re.I,
                    ),
                    8,
                ),
                (
                    re.compile(
                        r"(?:quarterly\s+dividend|dividend(?:s)?)\s+(?:of|to)\s+\$?\s*([0-9]+(?:\.\d+)?)\s*"
                        r"(?:per\s+(?:common\s+)?share|a\s+share)",
                        re.I,
                    ),
                    8,
                ),
            ]
            dps_candidates: List[Dict[str, Any]] = []
            for pat, base_score in dps_patterns:
                for m in pat.finditer(txt):
                    try:
                        dps_val = float(str(m.group(1) or "").replace(",", ""))
                    except Exception:
                        continue
                    if dps_val <= 0 or dps_val > 5:
                        continue
                    s0 = max(0, m.start() - 220)
                    s1 = min(len(txt), m.end() + 220)
                    window = txt[s0:s1]
                    dps_candidates.append(
                        {
                            "value": float(dps_val),
                            "base_score": int(base_score),
                            "window": window,
                            "window_low": window.lower(),
                        }
                    )
            analysis["dividend_ps_candidates"] = dps_candidates

            bcont = re.search(
                r"(?:expect(?:s|ed)?|intend(?:s|ed)?|plan(?:s|ned)?|will)[^.]{0,260}(?:repurchase|buyback)[^.]{0,260}\.",
                txt,
                re.I,
            )
            bauth = re.search(
                r"[^.]{0,260}(?:repurchase\s+authorization|authorization[^.]{0,120}repurchase|available[^.]{0,120}repurchase|capacity\s+remaining)[^.]{0,260}\.",
                txt,
                re.I,
            )
            if bcont or bauth:
                note_bits: List[str] = []
                if bcont:
                    note_bits.append(bcont.group(0).strip())
                if bauth:
                    note_bits.append(bauth.group(0).strip())
                candidate_buyback_note = " ".join(note_bits).strip()
                debt_repurchase_noise = self.is_debt_repurchase_noise_local(candidate_buyback_note)
                if candidate_buyback_note and not debt_repurchase_noise:
                    analysis["buyback_note_text"] = candidate_buyback_note
            if buyback_exec_summary:
                current_buyback_note = str(analysis.get("buyback_note_text") or "").strip()
                if not current_buyback_note or len(buyback_exec_summary) >= len(current_buyback_note):
                    analysis["buyback_note_text"] = buyback_exec_summary

            dm = re.search(r"we\s+paid\s+dividends?\s+of[^.]{0,260}\.", txt, re.I)
            dexp = re.search(
                r"we\s+(?:currently\s+)?expect[^.]{0,260}continue[^.]{0,260}dividend[^.]{0,260}\.",
                txt,
                re.I,
            )
            if dm or dexp:
                note_bits = []
                if dm:
                    note_bits.append(dm.group(0).strip())
                if dexp:
                    note_bits.append(dexp.group(0).strip())
                analysis["dividend_note_text"] = " ".join(note_bits)
            analysis["_core_ready"] = True

        if include_auth_details and not bool(analysis.get("_auth_details_ready")):
            month_date_re = re.compile(r"(?:as\s+of|at|on|by)\s+([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})", re.I)
            rem_patterns = [
                re.compile(
                    r"(?:approximately|about)?\s*\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?\s*"
                    r"(?:remained|remaining)\s+(?:available|authorized)?[^.]{0,160}?(?:share\s+repurchase|repurchase\s+program|repurchase\s+authorization)",
                    re.I,
                ),
                re.compile(
                    r"(?:share\s+repurchase|repurchase\s+program|repurchase\s+authorization)[^.]{0,180}?"
                    r"(?:approximately|about)?\s*\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?\s*"
                    r"(?:remained|remaining)",
                    re.I,
                ),
                re.compile(
                    r"(?:remaining|remained)[^.]{0,120}?(?:authorization|capacity|available)[^.]{0,120}?\$?\s*"
                    r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?[^.]{0,120}?(?:repurchase|buyback)",
                    re.I,
                ),
                re.compile(
                    r"(?:remaining|remained)[^.]{0,120}?(?:authorization|capacity|available)[^.]{0,120}?"
                    r"(?:repurchase|buyback|share\s+repurchase)[^.]{0,80}?\$?\s*"
                    r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?",
                    re.I,
                ),
                re.compile(
                    r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?"
                    r"\s*(?:in\s+)?(?:capacity|authorization)\s+(?:remaining|available)"
                    r"[^.]{0,180}?(?:authorization|repurchase|buyback)",
                    re.I,
                ),
                re.compile(
                    r"(?:as\s+of[^.]{0,80}?)?\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?"
                    r"\s*(?:in\s+)?(?:capacity|authorization)\s+(?:remaining|available)",
                    re.I,
                ),
                re.compile(
                    r"(?:as\s+of[^.]{0,80}?)?\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?"
                    r"\s*(?:in|of)?\s*(?:share\s+repurchase|repurchase)\s+authorization\s+remained",
                    re.I,
                ),
            ]
            auth_patterns = [
                re.compile(
                    r"(?:authorized|authorization(?:\s+from\s+our\s+board(?:\s+of\s+directors)?)?)"
                    r"[^.]{0,220}?\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?"
                    r"[^.]{0,220}?(?:share\s+repurchase|repurchase\s+program|repurchase\s+authorization)",
                    re.I,
                ),
                re.compile(
                    r"(?:share\s+repurchase|repurchase\s+program|repurchase\s+authorization)"
                    r"[^.]{0,220}?\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?"
                    r"[^.]{0,220}?(?:authorized|authorization)",
                    re.I,
                ),
                re.compile(
                    r"(?:board[^.]{0,120})?(?:authorized|approved)[^.]{0,220}?\$?\s*"
                    r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?"
                    r"[^.]{0,220}?(?:share\s+repurchase|buyback)",
                    re.I,
                ),
                re.compile(
                    r"(?:increas(?:e|ing)|update(?:d)?|raise(?:d)?)?[^.]{0,120}?"
                    r"(?:share\s+repurchase|buyback)[^.]{0,120}?authorization[^.]{0,80}?\bto\b\s+\$?\s*"
                    r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?",
                    re.I,
                ),
            ]
            spent_since_start_patterns = [
                re.compile(
                    r"(?:repurchased|bought\s+back)[^.]{0,180}?(?:at\s+)?(?:a\s+)?total\s+cost\s+of\s+\$?\s*"
                    r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?"
                    r"[^.]{0,160}?since\s+starting(?:\s+the\s+program)?(?:[^.]{0,60}?earlier\s+this\s+year)?",
                    re.I,
                ),
                re.compile(
                    r"total\s+cost\s+of\s+\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?"
                    r"[^.]{0,160}?since\s+starting(?:\s+the\s+program)?",
                    re.I,
                ),
            ]

            remaining_candidates: List[Dict[str, Any]] = []
            for pat in rem_patterns:
                for match in pat.finditer(txt):
                    snippet = txt[max(0, match.start() - 220): min(len(txt), match.end() + 220)].strip()
                    snippet_low = snippet.lower()
                    if "repurchase" not in snippet_low and "buyback" not in snippet_low:
                        continue
                    amount = self.parse_cap_alloc_amount(match.group(1), match.group(2))
                    if amount is None:
                        continue
                    asof_date = None
                    md = month_date_re.search(snippet)
                    if md:
                        asof_try = pd.to_datetime(f"{md.group(1)} {md.group(2)}, {md.group(3)}", errors="coerce")
                        if pd.notna(asof_try):
                            asof_date = pd.Timestamp(asof_try).date()
                    remaining_candidates.append(
                        {
                            "amount": float(amount),
                            "snippet": snippet[:360],
                            "snippet_low": snippet_low,
                            "asof_date": asof_date,
                        }
                    )
            analysis["remaining_candidates"] = remaining_candidates

            authorization_candidates: List[Dict[str, Any]] = []
            for pat in auth_patterns:
                for match in pat.finditer(txt):
                    snippet = txt[max(0, match.start() - 220): min(len(txt), match.end() + 220)].strip()
                    snippet_low = snippet.lower()
                    if "repurchase" not in snippet_low and "buyback" not in snippet_low:
                        continue
                    amount = self.parse_cap_alloc_amount(match.group(1), match.group(2))
                    if amount is None:
                        continue
                    kind = "authorization"
                    if re.search(r"authorization[^.]{0,40}?\bby\b", snippet_low):
                        kind = "authorization_increase_by"
                    elif "increas" in snippet_low and "to" in snippet_low:
                        kind = "authorization_increase_to"
                    asof_date = None
                    md = month_date_re.search(snippet)
                    if md:
                        asof_try = pd.to_datetime(f"{md.group(1)} {md.group(2)}, {md.group(3)}", errors="coerce")
                        if pd.notna(asof_try):
                            asof_date = pd.Timestamp(asof_try).date()
                    authorization_candidates.append(
                        {
                            "amount": float(amount),
                            "snippet": snippet[:360],
                            "snippet_low": snippet_low,
                            "kind": kind,
                            "asof_date": asof_date,
                        }
                    )
            analysis["authorization_candidates"] = authorization_candidates

            spent_since_start_candidates: List[Dict[str, Any]] = []
            for pat in spent_since_start_patterns:
                for match in pat.finditer(txt):
                    amount = self.parse_cap_alloc_amount(match.group(1), match.group(2))
                    if amount is None:
                        continue
                    snippet = txt[max(0, match.start() - 180): min(len(txt), match.end() + 180)].strip()
                    snippet_low = snippet.lower()
                    score = 0
                    if "since starting" in snippet_low:
                        score += 4
                    if "total cost" in snippet_low:
                        score += 3
                    if "earlier this year" in snippet_low:
                        score += 2
                    if "repurchased" in snippet_low or "bought back" in snippet_low:
                        score += 1
                    spent_since_start_candidates.append(
                        {
                            "spent_dollars": float(amount),
                            "snippet": snippet[:360],
                            "_score": score,
                        }
                    )
            analysis["spent_since_start_candidates"] = spent_since_start_candidates
            analysis["_auth_details_ready"] = True

        valuation_precompute_runtime.cap_alloc_doc_analysis_cache[cache_key] = analysis
        return analysis

    def extract_buyback_dividend_from_doc_index(self,
        qs_local: Tuple[pd.Timestamp, ...],
        audit_df: pd.DataFrame,
    ) -> Tuple[
        Dict[pd.Timestamp, Any],
        Dict[pd.Timestamp, Any],
        Dict[pd.Timestamp, str],
        Dict[pd.Timestamp, str],
        Dict[pd.Timestamp, Any],
        Dict[pd.Timestamp, Any],
        Dict[pd.Timestamp, Any],
    ]:
        runtime = self.runtime
        _ensure_terminal_period = runtime['_ensure_terminal_period']
        _path_cache_key = runtime['_path_cache_key']
        glx_normalize_text = runtime['glx_normalize_text']
        valuation_precompute_runtime = runtime['valuation_precompute_runtime']

        buyback_cash_out: Dict[pd.Timestamp, Any] = {}
        dividend_cash_out: Dict[pd.Timestamp, Any] = {}
        buyback_note_out: Dict[pd.Timestamp, str] = {}
        dividend_note_out: Dict[pd.Timestamp, str] = {}
        dividend_ps_out: Dict[pd.Timestamp, Any] = {}
        buyback_shares_out: Dict[pd.Timestamp, Any] = {}
        buyback_avg_price_out: Dict[pd.Timestamp, Any] = {}
        docs_by_quarter = self.load_filing_docs_by_quarter(qs_local, audit_df)
        if not docs_by_quarter:
            return (
                buyback_cash_out,
                dividend_cash_out,
                buyback_note_out,
                dividend_note_out,
                dividend_ps_out,
                buyback_shares_out,
                buyback_avg_price_out,
            )

        def _has_negative_buyback_statement_for_ref_doc_local(
            text_in: Any,
            qd_ref: Optional[date] = None,
        ) -> bool:
            text = glx_normalize_text(str(text_in or ""))
            if not text:
                return False
            if not re.search(
                r"\b(?:did not repurchas\w*|no repurchas\w* was made|no other repurchas\w* was made)\b",
                text,
                re.I,
            ):
                return False
            if not isinstance(qd_ref, date):
                return True
            q_num = ((qd_ref.month - 1) // 3) + 1
            quarter_tokens = {
                1: [r"\bq1\b", r"\bfirst quarter\b", rf"\bmarch 31,\s*{qd_ref.year}\b"],
                2: [r"\bq2\b", r"\bsecond quarter\b", rf"\bjune 30,\s*{qd_ref.year}\b"],
                3: [r"\bq3\b", r"\bthird quarter\b", rf"\bseptember 30,\s*{qd_ref.year}\b"],
                4: [r"\bq4\b", r"\bfourth quarter\b", rf"\bdecember 31,\s*{qd_ref.year}\b"],
            }.get(q_num, [])
            if any(re.search(token, text, re.I) for token in quarter_tokens):
                return True
            three_months_match = re.search(
                r"\bthree months ended\s+([A-Za-z]+)\s+\d{1,2},\s*(20\d{2})\b",
                text,
                re.I,
            )
            if three_months_match:
                try:
                    ts = pd.to_datetime(
                        f"{three_months_match.group(1)} 1 {three_months_match.group(2)}",
                        errors="raise",
                    )
                except Exception:
                    ts = pd.NaT
                if pd.notna(ts):
                    return ((int(ts.month) - 1) // 3) + 1 == q_num and int(ts.year) == int(qd_ref.year)
            return False

        def _is_non_equity_repurchase_noise_doc_local(text_in: Any) -> bool:
            text = glx_normalize_text(str(text_in or ""))
            if not text:
                return False
            low = text.lower()
            if not re.search(r"\brepurchas\w*\b|buyback", low, re.I):
                return False
            equity_context = bool(
                re.search(
                    r"\b(common stock|shares? of (?:its )?common stock|share repurchase|repurchase program|buyback|treasury stock)\b",
                    low,
                    re.I,
                )
            )
            debt_context = bool(
                re.search(
                    r"\b(fundamental change|indenture|convertible|senior notes?|2027 notes?|2030 notes?|holders?\b|subscription transactions?)\b",
                    low,
                    re.I,
                )
            )
            financing_or_commodity_context = bool(
                re.search(
                    r"\b(product financing|corn oil|obligation to repurchase|agreements? to repurchase|"
                    r"sold under agreements to repurchase|financial institution)\b",
                    low,
                    re.I,
                )
            )
            noteholder_put = bool(
                re.search(
                    r"\b(require the company to repurchase|repurchase their\b[^.]{0,120}\bnotes?\b|"
                    r"holders?\b[^.]{0,120}\bnotes?\b[^.]{0,120}\brepurchase)\b",
                    low,
                    re.I,
                )
            )
            return bool(noteholder_put or ((debt_context or financing_or_commodity_context) and not equity_context))

        def _rescue_buyback_execution_from_doc(
            text_in: Any,
            quarter_in: pd.Timestamp,
        ) -> Dict[str, Any]:
            def _fmt_share_count_local(value_in: float) -> str:
                value = float(value_in)
                if abs(value) >= 1_000_000.0:
                    return f"{value / 1_000_000.0:,.1f}m shares"
                return f"{value:,.0f} shares"

            def _fmt_short_money_local(value_in: float) -> str:
                value = float(value_in)
                if abs(value) >= 1_000_000_000.0:
                    return f"${value / 1_000_000_000.0:,.1f}bn"
                return f"${value / 1_000_000.0:,.1f}m"

            def _event_quarter_from_text_local(text_local: str, default_q: date) -> date:
                event_match = re.search(
                    r"\bon\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s*(20\d{2})\b",
                    text_local,
                    re.I,
                )
                if not event_match:
                    return default_q
                try:
                    event_ts = pd.to_datetime(
                        f"{event_match.group(1)} {event_match.group(2)}, {event_match.group(3)}",
                        errors="raise",
                    )
                    event_month = int(pd.Timestamp(event_ts).month)
                    event_year = int(pd.Timestamp(event_ts).year)
                    event_q_month = int(((event_month - 1) // 3 + 1) * 3)
                    event_q_day = 31 if event_q_month in {3, 12} else 30
                    return date(event_year, event_q_month, event_q_day)
                except Exception:
                    return default_q

            text = glx_normalize_text(html.unescape(str(text_in or "")).replace("\xa0", " "))
            if not text or not re.search(r"\brepurchas\w*\b", text, re.I):
                return {}
            table_ctx = self.has_buyback_execution_table_context_local(text)
            scoped_text = self.buyback_execution_scope_text_local(text, quarter_in.date()) or text
            if _is_non_equity_repurchase_noise_doc_local(scoped_text):
                return {}
            has_dated_event = bool(
                re.search(
                    r"\bon\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s*(20\d{2})\b",
                    scoped_text,
                    re.I,
                )
            )
            has_negative_for_q = _has_negative_buyback_statement_for_ref_doc_local(scoped_text, quarter_in.date())
            has_cumulative_context = self.is_cumulative_buyback_context_local(scoped_text)
            if has_negative_for_q and not has_dated_event:
                return {}
            if has_cumulative_context and not (has_dated_event or table_ctx):
                return {}
            has_quarter_scope = bool(
                table_ctx
                or has_dated_event
                or self.has_quarter_execution_scope_local(scoped_text, quarter_in.date())
            )
            if not has_quarter_scope:
                return {}
            try:
                summary = _compose_buyback_execution_summary_local(scoped_text, quarter_in.date())
            except NameError:
                summary = ""
            try:
                parts = _extract_buyback_execution_components_local(scoped_text, quarter_in.date())
            except NameError:
                parts = {}
            if not parts:
                share_match = re.search(
                    r"\brepurchas\w*\b(?:\s+(?:approximately|approx\.?|about|an\s+additional|additional|aggregate|an\s+aggregate))*\s+"
                    r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|m)?\s+shares\b",
                    scoped_text,
                    re.I,
                )
                amount_match = re.search(
                    r"\brepurchas\w*\b[^.]{0,240}?\bfor(?:\s+(?:a\s+)?total\s+of)?(?:\s+(?:approximately|approx\.?|about))?\s+\$?\s*"
                    r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?\b",
                    scoped_text,
                    re.I,
                )
                if not amount_match:
                    amount_match = re.search(
                        r"\bused(?:\s+(?:approximately|approx\.?|about))?\s+\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*"
                        r"(million|billion|m|bn)\s+of\s+(?:the\s+)?(?:net\s+)?proceeds\b[^.]{0,220}?\bto\s+repurchas\w*\b",
                        scoped_text,
                        re.I,
                    )
                avg_match = re.search(
                    r"\baverage price(?: paid)?(?: per share| of)\s+\$?\s*([0-9]+(?:\.\d+)?)\b",
                    scoped_text,
                    re.I,
                )
                direct_shares = None
                direct_amount = None
                direct_avg_price = None
                if share_match:
                    try:
                        direct_shares = float(str(share_match.group(1) or "").replace(",", ""))
                        if str(share_match.group(2) or "").strip().lower() in {"million", "m"}:
                            direct_shares *= 1_000_000.0
                    except Exception:
                        direct_shares = None
                if amount_match:
                    direct_amount = self.parse_cap_alloc_amount(amount_match.group(1), amount_match.group(2))
                if avg_match:
                    try:
                        direct_avg_price = float(str(avg_match.group(1) or "").replace(",", ""))
                    except Exception:
                        direct_avg_price = None
                if direct_shares is not None and direct_amount is not None:
                    event_q = _event_quarter_from_text_local(scoped_text, quarter_in.date())
                    parts = {
                        "shares": float(direct_shares),
                        "amount": float(direct_amount),
                        "avg_price": (float(direct_avg_price) if direct_avg_price is not None else None),
                        "anchor": f" in Q{((event_q.month - 1) // 3) + 1}",
                        "quarter_scoped": True,
                        "from_table": False,
                    }
            shares_val = pd.to_numeric(parts.get("shares"), errors="coerce")
            amount_val = pd.to_numeric(parts.get("amount"), errors="coerce")
            avg_price_val = pd.to_numeric(parts.get("avg_price"), errors="coerce")
            if pd.isna(shares_val) or pd.isna(amount_val):
                return {}
            if not summary:
                anchor = str(parts.get("anchor") or "").strip()
                if not anchor:
                    event_q = _event_quarter_from_text_local(scoped_text, quarter_in.date())
                    anchor = f" in Q{((event_q.month - 1) // 3) + 1}"
                if pd.notna(avg_price_val) and float(avg_price_val) > 0:
                    summary = _ensure_terminal_period(
                        f"Repurchased {_fmt_share_count_local(float(shares_val))} for "
                        f"{_fmt_short_money_local(float(amount_val))} with an average price of "
                        f"${float(avg_price_val):.2f}/share{anchor}"
                    )
                else:
                    summary = _ensure_terminal_period(
                        f"Repurchased {_fmt_share_count_local(float(shares_val))} for "
                        f"{_fmt_short_money_local(float(amount_val))}{anchor}"
                    )
            return {
                "summary": summary,
                "shares": float(shares_val),
                "amount": float(amount_val),
                "avg_price": (float(avg_price_val) if pd.notna(avg_price_val) else None),
            }

        prev_dps: Optional[float] = None
        for q in sorted(pd.Timestamp(x) for x in qs_local):
            doc_rows = list(docs_by_quarter.get(pd.Timestamp(q).normalize()) or [])
            if not doc_rows:
                continue

            best_bb: Optional[Tuple[float, float]] = None
            best_dv: Optional[Tuple[float, float]] = None
            best_dps: Optional[Tuple[float, float]] = None
            best_b_note: Optional[Tuple[float, str]] = None
            best_b_exec_note: Optional[Tuple[float, str]] = None
            best_b_context_note: Optional[Tuple[float, str]] = None
            best_d_note: Optional[Tuple[float, str]] = None
            best_bb_shares: Optional[Tuple[float, float]] = None
            best_bb_avg_price: Optional[Tuple[float, float]] = None

            for rec in doc_rows:
                doc_path = rec.get("path")
                if not isinstance(doc_path, Path):
                    continue
                rec_text = str(rec.get("text") or "")
                analysis = self.analyze_cap_alloc_doc(
                    doc_path,
                    text=rec_text,
                    include_auth_details=True,
                )
                if not analysis.get("mentions_cap_alloc"):
                    continue
                name_low = str(analysis.get("name_low") or rec.get("name") or "")
                src_bias = float(max(0, 10 - int(rec.get("accn_rank") or 0)))
                rec_text_norm = glx_normalize_text(str(rec_text or ""))
                q_num = ((pd.Timestamp(q).month - 1) // 3) + 1
                execution_scope_text = self.buyback_execution_scope_text_local(rec_text_norm, pd.Timestamp(q).date()) or rec_text_norm
                explicit_event_q = pd.NaT
                try:
                    evt_match = re.search(
                        r"\bon\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s*(20\d{2})\b",
                        execution_scope_text,
                        re.I,
                    )
                    if evt_match:
                        evt_ts = pd.to_datetime(
                            f"{evt_match.group(1)} {evt_match.group(2)}, {evt_match.group(3)}",
                            errors="coerce",
                        )
                        if pd.notna(evt_ts):
                            evt_month = int(pd.Timestamp(evt_ts).month)
                            evt_year = int(pd.Timestamp(evt_ts).year)
                            evt_q_month = int(((evt_month - 1) // 3 + 1) * 3)
                            evt_q_day = 31 if evt_q_month in {3, 12} else 30
                            explicit_event_q = pd.Timestamp(year=evt_year, month=evt_q_month, day=evt_q_day)
                except Exception:
                    explicit_event_q = pd.NaT
                has_explicit_dated_event = bool(pd.notna(explicit_event_q) and pd.Timestamp(explicit_event_q).normalize() == pd.Timestamp(q).normalize())
                has_table_or_quarter_scope = bool(
                    self.has_buyback_execution_table_context_local(rec_text_norm)
                    or self.has_quarter_execution_scope_local(execution_scope_text, pd.Timestamp(q).date())
                )
                has_cumulative_context = self.is_cumulative_buyback_context_local(execution_scope_text)
                has_negative_for_q = _has_negative_buyback_statement_for_ref_doc_local(execution_scope_text, pd.Timestamp(q).date())
                has_non_equity_repurchase_noise = _is_non_equity_repurchase_noise_doc_local(execution_scope_text)
                allow_quarter_execution = bool(
                    (has_table_or_quarter_scope or has_explicit_dated_event)
                    and not has_non_equity_repurchase_noise
                    and not (has_negative_for_q and not has_explicit_dated_event)
                    and not (has_cumulative_context and not has_explicit_dated_event)
                )

                bb = analysis.get("buyback_quarter_sentence_amount")
                bb_sent = analysis.get("buyback_quarter_sentence_text")
                bb_is_sentence = bb is not None
                if not allow_quarter_execution:
                    bb = None
                    bb_sent = None
                    bb_is_sentence = False
                if bb is not None:
                    bb_score = src_bias
                    if bb_is_sentence:
                        bb_score += 8.0
                    if "_pbi-" in name_low or "10k" in name_low or "10q" in name_low:
                        bb_score += 6.0
                    if "press" in name_low or "ex99" in name_low or "earnings" in name_low:
                        bb_score += 2.0 if bb_is_sentence else -2.0
                    if best_bb is None or bb_score > best_bb[0]:
                        best_bb = (bb_score, float(bb))
                    if bb_sent:
                        bsn = glx_normalize_text(bb_sent)
                        if bsn:
                            bnote_score = bb_score + 1.0
                            if best_b_exec_note is None or bnote_score > best_b_exec_note[0]:
                                best_b_exec_note = (bnote_score, bsn)
                            valuation_precompute_runtime.buyback_execution_doc_cache[
                                (_path_cache_key(doc_path), pd.Timestamp(q).date().isoformat())
                            ] = {
                                "amount": float(bb),
                                "summary": bsn,
                                "path": str(doc_path),
                                "quarter": pd.Timestamp(q).date().isoformat(),
                            }
                for exec_cand in (list(analysis.get("buyback_execution_candidates") or []) if allow_quarter_execution else []):
                    exec_summary = glx_normalize_text(str(exec_cand.get("summary") or ""))
                    if not exec_summary:
                        continue
                    exec_score = src_bias
                    if bool(exec_cand.get("quarter_scoped")):
                        exec_score += 8.0
                    if bool(exec_cand.get("from_table")):
                        exec_score += 6.0
                    if bool(exec_cand.get("has_share_count")):
                        exec_score += 3.0
                    if bool(exec_cand.get("has_amount")):
                        exec_score += 2.0
                    if bool(exec_cand.get("has_avg_price")):
                        exec_score += 3.0
                    if "_pbi-" in name_low or "10k" in name_low or "10q" in name_low:
                        exec_score += 4.0
                    if "press" in name_low or "ex99" in name_low or "earnings" in name_low:
                        exec_score += 2.0
                    exec_amount = pd.to_numeric(exec_cand.get("amount"), errors="coerce")
                    exec_shares = pd.to_numeric(exec_cand.get("shares"), errors="coerce")
                    exec_avg_price = pd.to_numeric(exec_cand.get("avg_price"), errors="coerce")
                    if pd.notna(exec_amount):
                        amt_score = exec_score + 2.0 + (2.0 if bool(exec_cand.get("from_table")) else 0.0)
                        if best_bb is None or amt_score > best_bb[0]:
                            best_bb = (amt_score, float(exec_amount))
                    if pd.notna(exec_shares):
                        sh_score = exec_score + 2.0
                        if best_bb_shares is None or sh_score > best_bb_shares[0]:
                            best_bb_shares = (sh_score, float(exec_shares))
                    if pd.notna(exec_avg_price):
                        px_score = exec_score + 1.0
                        if best_bb_avg_price is None or px_score > best_bb_avg_price[0]:
                            best_bb_avg_price = (px_score, float(exec_avg_price))
                    if best_b_exec_note is None or exec_score > best_b_exec_note[0]:
                        best_b_exec_note = (exec_score, exec_summary)
                direct_exec_parts = _rescue_buyback_execution_from_doc(rec_text, pd.Timestamp(q))
                direct_exec_summary = str(direct_exec_parts.get("summary") or "")
                if allow_quarter_execution and direct_exec_summary and direct_exec_parts:
                    direct_score = src_bias + 8.0
                    if bool(direct_exec_parts.get("from_table")):
                        direct_score += 6.0
                    if direct_exec_parts.get("shares") is not None:
                        direct_score += 3.0
                    if direct_exec_parts.get("amount") is not None:
                        direct_score += 3.0
                    if direct_exec_parts.get("avg_price") is not None:
                        direct_score += 2.0
                    if "_pbi-" in name_low or "10k" in name_low or "10q" in name_low:
                        direct_score += 4.0
                    if "press" in name_low or "ex99" in name_low or "earnings" in name_low:
                        direct_score += 2.0
                    direct_amount = pd.to_numeric(direct_exec_parts.get("amount"), errors="coerce")
                    direct_shares = pd.to_numeric(direct_exec_parts.get("shares"), errors="coerce")
                    direct_avg_price = pd.to_numeric(direct_exec_parts.get("avg_price"), errors="coerce")
                    if pd.notna(direct_amount):
                        amt_score = direct_score + 2.0
                        if best_bb is None or amt_score > best_bb[0]:
                            best_bb = (amt_score, float(direct_amount))
                    if pd.notna(direct_shares):
                        sh_score = direct_score + 2.0
                        if best_bb_shares is None or sh_score > best_bb_shares[0]:
                            best_bb_shares = (sh_score, float(direct_shares))
                    if pd.notna(direct_avg_price):
                        px_score = direct_score + 1.0
                        if best_bb_avg_price is None or px_score > best_bb_avg_price[0]:
                            best_bb_avg_price = (px_score, float(direct_avg_price))
                    if best_b_exec_note is None or direct_score > best_b_exec_note[0]:
                        best_b_exec_note = (direct_score, direct_exec_summary)
                for spent_cand in list(analysis.get("spent_since_start_candidates") or []):
                    snippet_txt = glx_normalize_text(str(spent_cand.get("snippet") or ""))
                    if not snippet_txt:
                        continue
                    spent_score = src_bias + float(spent_cand.get("_score") or 0.0)
                    if "press" in name_low or "ex99" in name_low or "earnings" in name_low:
                        spent_score += 6.0
                    if "ceo" in name_low or "letter" in name_low or "shareholder" in name_low:
                        spent_score += 4.0
                    if re.search(r"\bnotes?\b", name_low, re.I) and not re.search(r"(press|earnings|letter)", name_low, re.I):
                        spent_score -= 6.0
                    if best_b_context_note is None or spent_score > best_b_context_note[0]:
                        best_b_context_note = (spent_score, snippet_txt)

                dv = analysis.get("dividend_row_cash")
                if dv is not None:
                    dv_score = src_bias
                    if "_pbi-" in name_low or "10k" in name_low or "10q" in name_low:
                        dv_score += 5.0
                    if "press" in name_low or "ex99" in name_low or "earnings" in name_low:
                        dv_score -= 2.0
                    if best_dv is None or dv_score > best_dv[0]:
                        best_dv = (dv_score, float(dv))

                for cand in list(analysis.get("dividend_ps_candidates") or []):
                    try:
                        dps_val = float(cand.get("value"))
                    except Exception:
                        continue
                    if dps_val <= 0 or dps_val > 5:
                        continue
                    win_low = str(cand.get("window_low") or "").lower()
                    sc = float(cand.get("base_score") or 0.0) + src_bias
                    if "quarterly" in win_low:
                        sc += 4.0
                    if "board" in win_low or "declared" in win_low or "approved" in win_low:
                        sc += 3.0
                    if "payable" in win_low or "record" in win_low:
                        sc += 2.0
                    if "press" in name_low or "ex99" in name_low or "earnings" in name_low:
                        sc += 6.0
                    if "annualletter" in name_low:
                        sc -= 4.0
                    if re.search(r"\b(first\s+nine\s+months|full[\s-]?year|year\s+ended|during\s+20\d{2}|totaling)\b", win_low):
                        sc -= 12.0
                    if re.search(r"\b(balance\s+at|net\s+income|other\s+comprehensive)\b", win_low):
                        sc -= 6.0
                    if prev_dps is not None and dps_val > prev_dps * 2.5 and re.search(r"\b(full[\s-]?year|totaling|during\s+20\d{2})\b", win_low):
                        sc -= 8.0
                    if best_dps is None or sc > best_dps[0]:
                        best_dps = (sc, float(dps_val))

                btxt = str(analysis.get("buyback_note_text") or "").strip()
                if btxt and not self.is_debt_repurchase_noise_local(btxt):
                    bscore = src_bias + (6.0 if ("press" in name_low or "ex99" in name_low) else 0.0)
                    if best_b_note is None or bscore > best_b_note[0]:
                        best_b_note = (bscore, btxt)

                dtxt = str(analysis.get("dividend_note_text") or "").strip()
                if dtxt:
                    dscore = src_bias + (6.0 if ("press" in name_low or "ex99" in name_low) else 0.0)
                    if best_d_note is None or dscore > best_d_note[0]:
                        best_d_note = (dscore, dtxt)

            if best_bb is not None:
                buyback_cash_out[q] = best_bb[1]
            if best_dv is not None:
                dividend_cash_out[q] = best_dv[1]
            if best_dps is not None:
                dividend_ps_out[q] = best_dps[1]
                prev_dps = best_dps[1]
            if best_bb_shares is not None:
                buyback_shares_out[q] = best_bb_shares[1]
            if best_bb_avg_price is not None:
                buyback_avg_price_out[q] = best_bb_avg_price[1]
            if best_b_exec_note is not None:
                buyback_note_out[q] = best_b_exec_note[1]
            elif best_b_context_note is not None:
                buyback_note_out[q] = best_b_context_note[1]
            elif best_b_note is not None:
                buyback_note_out[q] = best_b_note[1]
            elif best_bb is not None:
                buyback_note_out[q] = "repurchase of common stock disclosed"
            if best_d_note is not None:
                dividend_note_out[q] = best_d_note[1]
            elif best_dv is not None:
                dividend_note_out[q] = "dividends paid disclosure"

        return (
            buyback_cash_out,
            dividend_cash_out,
            buyback_note_out,
            dividend_note_out,
            dividend_ps_out,
            buyback_shares_out,
            buyback_avg_price_out,
        )

    def ensure_precompute_bundle(self,
        qs_local: Tuple[pd.Timestamp, ...],
        render_bundle: Dict[str, Any],
    ) -> Dict[str, Any]:
        runtime = self.runtime
        _anf_buyback_execution_is_year_or_ttm = runtime['_anf_buyback_execution_is_year_or_ttm']
        _anf_fiscal_year_from_quarter_end = runtime['_anf_fiscal_year_from_quarter_end']
        _anf_format_year_ttm_buyback_summary = runtime['_anf_format_year_ttm_buyback_summary']
        _anf_prior_year_quarter = runtime['_anf_prior_year_quarter']
        _ensure_terminal_period = runtime['_ensure_terminal_period']
        _resolve_col = runtime['_resolve_col']
        _sec_cache_docs_for_token_local = runtime['_sec_cache_docs_for_token_local']
        _timed_writer_substage = runtime['_timed_writer_substage']
        audit = runtime['audit']
        cache_root = runtime['cache_root']
        capital_return_build_buyback_note = runtime['capital_return_build_buyback_note']
        ctx_ref = runtime['ctx_ref']
        debt_credit_notes = runtime['debt_credit_notes']
        glx_normalize_text = runtime['glx_normalize_text']
        is_anf_profile = runtime['is_anf_profile']
        is_pbi_profile = runtime['is_pbi_profile']
        profile_ticker = runtime['profile_ticker']
        quarter_notes = runtime['quarter_notes']
        slides_debt = runtime['slides_debt']
        slides_guidance = runtime['slides_guidance']
        ticker = runtime['ticker']

        # This precompute bundle is the heavier valuation memoization layer. It starts
        # from the cheaper render bundle and enriches it with document-derived
        # capital-return evidence that feeds visible Valuation rows and the latest-
        # quarter QA block.
        quarter_key = tuple(pd.Timestamp(q).normalize() for q in qs_local if pd.notna(q))
        if (
            self.valuation_precompute_bundle_cache is not None
            and (
                tuple(self.valuation_precompute_bundle_cache.get("quarter_key") or ()) == quarter_key
                or set(quarter_key).issubset(set(self.valuation_precompute_bundle_cache.get("quarter_key") or ()))
            )
        ):
            if ctx_ref is not None:
                ctx_ref.derived.valuation_precompute_bundle = self.valuation_precompute_bundle_cache
            return self.valuation_precompute_bundle_cache

        buyback_map = dict(render_bundle.get("buyback_map") or {})
        dividend_map = dict(render_bundle.get("dividend_map") or {})
        buyback_cash_facts_map = dict(render_bundle.get("buyback_cash_facts_map") or {})
        dividend_cash_facts_map = dict(render_bundle.get("dividend_cash_facts_map") or {})
        buyback_shares_q_map = dict(render_bundle.get("buyback_shares_q_map") or {})
        last4_quarters_map = dict(render_bundle.get("last4_quarters_map") or {})
        shares_map_for_dividend = dict(render_bundle.get("shares_map") or {})
        shares_out_map_for_dividend = dict(render_bundle.get("shares_out_map") or {})
        buyback_text_map: Dict[pd.Timestamp, Any] = {}
        buyback_shares_text_map: Dict[pd.Timestamp, Any] = {}
        dividend_text_map: Dict[pd.Timestamp, Any] = {}

        with _timed_writer_substage("write_excel.valuation.precompute.doc_index"):
            # Quarter-indexed filing docs are the preferred starting point because
            # they preserve accession/quarter alignment better than broad text scans.
            docs_by_quarter = self.load_filing_docs_by_quarter(quarter_key, audit)

        def _has_negative_buyback_statement_for_ref_precompute_local(
            text_in: Any,
            qd_ref: Optional[date] = None,
        ) -> bool:
            text = glx_normalize_text(str(text_in or ""))
            if not text:
                return False
            if not re.search(
                r"\b(?:did not repurchas\w*|no repurchas\w* was made|no other repurchas\w* was made)\b",
                text,
                re.I,
            ):
                return False
            if not isinstance(qd_ref, date):
                return True
            q_num = ((qd_ref.month - 1) // 3) + 1
            quarter_tokens = {
                1: [r"\bq1\b", r"\bfirst quarter\b", rf"\bmarch 31,\s*{qd_ref.year}\b"],
                2: [r"\bq2\b", r"\bsecond quarter\b", rf"\bjune 30,\s*{qd_ref.year}\b"],
                3: [r"\bq3\b", r"\bthird quarter\b", rf"\bseptember 30,\s*{qd_ref.year}\b"],
                4: [r"\bq4\b", r"\bfourth quarter\b", rf"\bdecember 31,\s*{qd_ref.year}\b"],
            }.get(q_num, [])
            if any(re.search(token, text, re.I) for token in quarter_tokens):
                return True
            three_months_match = re.search(
                r"\bthree months ended\s+([A-Za-z]+)\s+\d{1,2},\s*(20\d{2})\b",
                text,
                re.I,
            )
            if three_months_match:
                try:
                    ts = pd.to_datetime(
                        f"{three_months_match.group(1)} 1 {three_months_match.group(2)}",
                        errors="raise",
                    )
                except Exception:
                    ts = pd.NaT
                if pd.notna(ts):
                    return ((int(ts.month) - 1) // 3) + 1 == q_num and int(ts.year) == int(qd_ref.year)
            return False

        def _is_non_equity_repurchase_noise_precompute_local(text_in: Any) -> bool:
            text = glx_normalize_text(str(text_in or ""))
            if not text:
                return False
            low = text.lower()
            if not re.search(r"\brepurchas\w*\b|buyback", low, re.I):
                return False
            equity_context = bool(
                re.search(
                    r"\b(common stock|shares? of (?:its )?common stock|share repurchase|repurchase program|buyback|treasury stock)\b",
                    low,
                    re.I,
                )
            )
            debt_context = bool(
                re.search(
                    r"\b(fundamental change|indenture|convertible|senior notes?|2027 notes?|2030 notes?|holders?\b|subscription transactions?)\b",
                    low,
                    re.I,
                )
            )
            financing_or_commodity_context = bool(
                re.search(
                    r"\b(product financing|corn oil|obligation to repurchase|agreements? to repurchase|"
                    r"sold under agreements to repurchase|financial institution)\b",
                    low,
                    re.I,
                )
            )
            noteholder_put = bool(
                re.search(
                    r"\b(require the company to repurchase|repurchase their\b[^.]{0,120}\bnotes?\b|"
                    r"holders?\b[^.]{0,120}\bnotes?\b[^.]{0,120}\brepurchase)\b",
                    low,
                    re.I,
                )
            )
            return bool(noteholder_put or ((debt_context or financing_or_commodity_context) and not equity_context))

        def _best_cache_root_buyback_execution_precompute_local(
            q_ref: pd.Timestamp,
        ) -> Optional[Tuple[float, float, float, Optional[float], str]]:
            if not cache_root.exists():
                return None
            qts = pd.Timestamp(q_ref).normalize()
            ymd_txt = qts.strftime("%Y%m%d")
            best_exec: Optional[Tuple[float, float, float, Optional[float], str]] = None
            for dp in _sec_cache_docs_for_token_local(cache_root, ymd_txt):
                if not dp.is_file():
                    continue
                doc_txt = glx_normalize_text(html.unescape(self.extract_valuation_filing_doc_text(dp)).replace("\xa0", " "))
                table_ctx = bool(doc_txt) and self.has_buyback_execution_table_context_local(doc_txt)
                if not doc_txt or (not re.search(r"\brepurchas\w*\b", doc_txt, re.I) and not table_ctx):
                    continue
                scoped_text = self.buyback_execution_scope_text_local(doc_txt, qts.date()) or doc_txt
                if _is_non_equity_repurchase_noise_precompute_local(scoped_text):
                    continue
                has_dated_event = bool(
                    re.search(
                        r"\bon\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s*(20\d{2})\b",
                        scoped_text,
                        re.I,
                    )
                )
                has_quarter_scope = bool(
                    table_ctx
                    or has_dated_event
                    or self.has_quarter_execution_scope_local(scoped_text, qts.date())
                )
                has_negative_for_q = _has_negative_buyback_statement_for_ref_precompute_local(scoped_text, qts.date())
                has_cumulative_context = self.is_cumulative_buyback_context_local(scoped_text)
                if has_negative_for_q and not has_dated_event:
                    continue
                if has_cumulative_context and not (has_dated_event or table_ctx):
                    continue
                if not has_quarter_scope:
                    continue
                shares_match = re.search(
                    r"\brepurchased(?:\s+(?:approximately|approx\.?|about))?\s+([0-9]+(?:\.\d+)?)\s*(million|m)?\s+shares\b",
                    scoped_text,
                    re.I,
                )
                amount_match = None
                for amount_pattern in [
                    (
                        r"\brepurchas\w*\b.{0,260}?\bfor(?:\s+(?:a\s+)?total\s+of)?(?:\s+(?:approximately|approx\.?|about))?\s+\$?\s*"
                        r"([0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\b"
                    ),
                    (
                        r"\brepurchas\w*\b.{0,260}?\bat\s+(?:a\s+)?(?:total\s+)?cost\s+of(?:\s+(?:approximately|approx\.?|about))?\s+\$?\s*"
                        r"([0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\b"
                    ),
                    (
                        r"\b(?:used|deployed)\b.{0,180}?\$?\s*([0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\b"
                        r"[^.]{0,160}?\bto\s+repurchas\w*\b"
                    ),
                ]:
                    amount_match = re.search(amount_pattern, scoped_text, re.I)
                    if amount_match:
                        break
                table_total_match = None
                table_total_matches = (
                    list(
                        re.finditer(
                                r"\b([0-9]{1,3}(?:,[0-9]{3})+)\s*\$\s*([0-9]+(?:\.\d+)?)\s+\1\b",
                                scoped_text,
                                re.I,
                            )
                        )
                    if table_ctx
                    else []
                )
                if table_total_matches:
                    table_total_match = table_total_matches[-1]
                    shares_match = table_total_match
                    amount_match = None
                elif not shares_match or not amount_match:
                    continue
                try:
                    share_val = float(str(shares_match.group(1) or "").replace(",", ""))
                    if str(shares_match.group(2) or "").strip().lower() in {"million", "m"}:
                        share_val *= 1_000_000.0
                except Exception:
                    continue
                avg_val = None
                avg_match = re.search(
                    r"\baverage price(?: paid)?(?: per share| of)\s+\$?\s*([0-9]+(?:\.\d+)?)\b",
                    scoped_text,
                    re.I,
                )
                if avg_match:
                    try:
                        avg_val = float(str(avg_match.group(1) or "").replace(",", ""))
                    except Exception:
                        avg_val = None
                if amount_match is not None:
                    try:
                        amount_val = float(str(amount_match.group(1) or "").replace(",", ""))
                    except Exception:
                        amount_val = None
                    amount_unit = str(amount_match.group(2) or "").strip().lower()
                    if amount_val is not None:
                        if amount_unit in {"billion", "bn"}:
                            amount_val *= 1_000_000_000.0
                        elif amount_unit in {"million", "m"} or amount_val < 2_000.0:
                            amount_val *= 1_000_000.0
                else:
                    try:
                        avg_val = float(str(table_total_match.group(2) or "").replace(",", ""))
                    except Exception:
                        avg_val = avg_val
                    amount_val = (
                        float(share_val) * float(avg_val)
                        if avg_val is not None and float(avg_val) > 0
                        else None
                    )
                if amount_val is None:
                    continue
                score = 10.0
                name_low = dp.name.lower()
                if "10k" in name_low or "10q" in name_low or "-2025" in name_low or "_pbi-" in name_low or "_gpre-" in name_low:
                    score += 5.0
                if table_ctx:
                    score += 3.0
                if avg_val is not None:
                    score += 2.0
                summary_txt = _ensure_terminal_period(
                    f"Repurchased {float(share_val) / 1_000_000.0:,.1f}m shares for ${float(amount_val) / 1_000_000.0:,.1f}m"
                    + (
                        f" with an average price of ${float(avg_val):.2f}/share in Q{((qts.month - 1) // 3) + 1}"
                        if avg_val is not None and float(avg_val) > 0
                        else f" in Q{((qts.month - 1) // 3) + 1}"
                    )
                )
                candidate_exec = (score, float(share_val), float(amount_val), avg_val, summary_txt)
                if best_exec is None or candidate_exec[0] > best_exec[0]:
                    best_exec = candidate_exec
            return best_exec
        # Fallback order is deliberate:
        # 1) quarter-indexed filing docs
        # 2) direct scans of those docs
        # 3) broader cache-root rescue scans
        # 4) quarter-note support
        # 5) keyword-only context maps
        # Execution metrics stay blank unless the evidence is quarter-safe.
        with _timed_writer_substage("write_excel.valuation.precompute.buyback_dividend_maps"):
            (
                buyback_cash_doc_map,
                dividend_doc_map,
                buyback_doc_note_map,
                dividend_doc_note_map,
                dividend_ps_doc_map,
                buyback_shares_doc_map,
                buyback_avg_price_doc_map,
            ) = self.extract_buyback_dividend_from_doc_index(quarter_key, audit)
        with _timed_writer_substage("write_excel.valuation.precompute.buyback_doc_direct_fallback"):
            for q in quarter_key:
                qts = pd.Timestamp(q).normalize()
                best_direct_exec: Optional[Tuple[float, float, float, Optional[float], str]] = None
                for rec in list((docs_by_quarter or {}).get(qts) or []):
                    rec_text = glx_normalize_text(html.unescape(str(rec.get("text") or "")).replace("\xa0", " "))
                    table_ctx = bool(rec_text) and self.has_buyback_execution_table_context_local(rec_text)
                    if not rec_text or (not re.search(r"\brepurchas\w*\b", rec_text, re.I) and not table_ctx):
                        continue
                    execution_scope_text = self.buyback_execution_scope_text_local(rec_text, qts.date()) or rec_text
                    if _is_non_equity_repurchase_noise_precompute_local(execution_scope_text):
                        continue
                    explicit_event_q = None
                    event_match = re.search(
                        r"\bon\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s*(20\d{2})\b",
                        execution_scope_text,
                        re.I,
                    )
                    if event_match:
                        try:
                            event_ts = pd.to_datetime(
                                f"{event_match.group(1)} {event_match.group(2)}, {event_match.group(3)}",
                                errors="raise",
                            )
                            event_month = int(pd.Timestamp(event_ts).month)
                            event_year = int(pd.Timestamp(event_ts).year)
                            event_q_month = int(((event_month - 1) // 3 + 1) * 3)
                            event_q_day = 31 if event_q_month in {3, 12} else 30
                            explicit_event_q = date(event_year, event_q_month, event_q_day)
                        except Exception:
                            explicit_event_q = None
                    q_num = ((qts.month - 1) // 3) + 1
                    has_negative_for_q = bool(
                        re.search(
                            rf"\b(?:did not repurchas\w*|no repurchas\w* was made|no other repurchas\w* was made)\b[^.]*\b(?:q{q_num}|{qts.strftime('%B').lower()} 30|{qts.year})\b",
                            execution_scope_text.lower(),
                            re.I,
                        )
                    )
                    has_cumulative_context = bool(
                        re.search(
                            r"\b(?:since inception|to date|since starting(?:\s+the\s+program)?|since the beginning|authorized up to|authorization remained|remaining authorization|remaining capacity|may repurchase|under the program we may repurchase|did not repurchase any shares|no shares were repurchased)\b",
                            execution_scope_text,
                            re.I,
                        )
                    )
                    has_explicit_dated_event = bool(
                        isinstance(explicit_event_q, date)
                        and explicit_event_q == qts.date()
                        and re.search(r"\bon\s+[A-Za-z]+\s+\d{1,2},\s+20\d{2}\b", execution_scope_text, re.I)
                    )
                    has_explicit_execution_scope = bool(
                        table_ctx
                        or has_explicit_dated_event
                        or self.has_quarter_execution_scope_local(execution_scope_text, qts.date())
                    )
                    if has_negative_for_q and not has_explicit_dated_event:
                        continue
                    if has_cumulative_context and not has_explicit_dated_event:
                        continue
                    if not has_explicit_execution_scope:
                        continue
                    table_total_match = None
                    table_total_matches = (
                        list(
                            re.finditer(
                                r"\b([0-9]{1,3}(?:,[0-9]{3})+)\s*\$\s*([0-9]+(?:\.\d+)?)\s+\1\b",
                                execution_scope_text,
                                re.I,
                            )
                        )
                        if table_ctx
                        else []
                    )
                    if table_total_matches:
                        table_total_match = table_total_matches[-1]
                        share_match = table_total_match
                        amount_match = None
                    else:
                        share_match = re.search(
                            r"\brepurchas\w*\b(?:\s+(?:approximately|approx\.?|about|an\s+additional|additional|aggregate|an\s+aggregate))*\s+"
                            r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|m)?\s+shares\b",
                            execution_scope_text,
                            re.I,
                        )
                        amount_match = re.search(
                            r"\brepurchas\w*\b.{0,240}?\bfor(?:\s+(?:a\s+)?total\s+of)?(?:\s+(?:approximately|approx\.?|about))?\s+\$?\s*"
                            r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?\b",
                            execution_scope_text,
                            re.I,
                        )
                        if not amount_match:
                            amount_match = re.search(
                                r"\bused(?:\s+(?:approximately|approx\.?|about))?\s+\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*"
                                r"(million|billion|m|bn)\s+of\s+(?:the\s+)?(?:net\s+)?proceeds\b[^.]{0,220}?\bto\s+repurchas\w*\b",
                                execution_scope_text,
                                re.I,
                            )
                        if not share_match or not amount_match:
                            continue
                    try:
                        share_val = float(str(share_match.group(1) or "").replace(",", ""))
                        if str(getattr(share_match, "group", lambda *_: "")(2) or "").strip().lower() in {"million", "m"}:
                            share_val *= 1_000_000.0
                    except Exception:
                        continue
                    avg_match = re.search(
                        r"\baverage price(?: paid)?(?: per share| of)\s+\$?\s*([0-9]+(?:\.\d+)?)\b",
                        execution_scope_text,
                        re.I,
                    )
                    avg_price_val = None
                    if avg_match:
                        try:
                            avg_price_val = float(str(avg_match.group(1) or "").replace(",", ""))
                        except Exception:
                            avg_price_val = None
                    if amount_match is not None:
                        try:
                            amount_val = float(str(amount_match.group(1) or "").replace(",", ""))
                        except Exception:
                            amount_val = None
                        unit_low = str(amount_match.group(2) or "").strip().lower()
                        if amount_val is not None:
                            if unit_low in {"billion", "bn"}:
                                amount_val *= 1_000_000_000.0
                            elif unit_low in {"million", "m"} or amount_val < 2_000.0:
                                amount_val *= 1_000_000.0
                    else:
                        try:
                            avg_price_val = float(str(table_total_match.group(2) or "").replace(",", ""))
                        except Exception:
                            avg_price_val = avg_price_val
                        amount_val = (
                            float(share_val) * float(avg_price_val)
                            if avg_price_val is not None and float(avg_price_val) > 0
                            else None
                        )
                    if amount_val is None:
                        continue
                    score = float(max(0, 10 - int(rec.get("accn_rank") or 0)))
                    name_low = str(rec.get("name") or "").lower()
                    form_low = str(rec.get("form") or "").lower()
                    if form_low in {"10-k", "10-q"} or "10k" in name_low or "10q" in name_low or "_pbi-" in name_low or "_gpre-" in name_low:
                        score += 7.0
                    elif "press" in name_low or "ex99" in name_low or "earnings" in name_low:
                        score += 1.0
                    if table_ctx:
                        score += 6.0
                    if table_total_match is not None:
                        score += 2.0
                    if avg_price_val is not None:
                        score += 1.5
                    anchor_q = explicit_event_q if isinstance(explicit_event_q, date) else qts.date()
                    anchor = f" in Q{((anchor_q.month - 1) // 3) + 1}"
                    share_note = f"{float(share_val) / 1_000_000.0:,.1f}m shares"
                    amount_note = f"${float(amount_val) / 1_000_000.0:,.1f}m"
                    summary_txt = _ensure_terminal_period(
                        f"Repurchased {share_note} for {amount_note}"
                        + (
                            f" with an average price of ${float(avg_price_val):.2f}/share{anchor}"
                            if avg_price_val is not None and float(avg_price_val) > 0
                            else f"{anchor}"
                        )
                    )
                    candidate_direct_exec = (score, float(share_val), float(amount_val), avg_price_val, summary_txt)
                    if best_direct_exec is None or candidate_direct_exec[0] > best_direct_exec[0]:
                        best_direct_exec = candidate_direct_exec
                if best_direct_exec is None:
                    continue
                _, share_val, amount_val, avg_price_val, summary_txt = best_direct_exec
                buyback_shares_doc_map[qts] = share_val
                buyback_cash_doc_map[qts] = amount_val
                if avg_price_val is not None:
                    buyback_avg_price_doc_map[qts] = float(avg_price_val)
                buyback_doc_note_map[qts] = summary_txt
        with _timed_writer_substage("write_excel.valuation.precompute.buyback_cache_root_fallback"):
            for q in quarter_key:
                qts = pd.Timestamp(q).normalize()
                best_cache_exec = _best_cache_root_buyback_execution_precompute_local(qts)
                if best_cache_exec is None:
                    continue
                _, share_val, amount_val, avg_price_val, summary_txt = best_cache_exec
                current_cash_val = pd.to_numeric(buyback_cash_doc_map.get(qts), errors="coerce")
                current_cash_val = float(current_cash_val) if pd.notna(current_cash_val) else None
                current_shares_val = pd.to_numeric(buyback_shares_doc_map.get(qts), errors="coerce")
                current_shares_val = float(current_shares_val) if pd.notna(current_shares_val) else None
                current_avg_val = pd.to_numeric(buyback_avg_price_doc_map.get(qts), errors="coerce")
                current_avg_val = float(current_avg_val) if pd.notna(current_avg_val) else None
                if current_shares_val is None:
                    buyback_shares_doc_map[qts] = float(share_val)
                if current_cash_val is None:
                    buyback_cash_doc_map[qts] = float(amount_val)
                if avg_price_val is not None and current_avg_val is None:
                    buyback_avg_price_doc_map[qts] = float(avg_price_val)
                if not str((buyback_doc_note_map or {}).get(qts) or "").strip():
                    buyback_doc_note_map[qts] = summary_txt
        with _timed_writer_substage("write_excel.valuation.precompute.buyback_quarter_notes_fallback"):
            if quarter_notes is not None and not quarter_notes.empty:
                qn = quarter_notes.copy()
                qcol_qn = _resolve_col(qn, ["quarter", "quarter_end", "period_end"])
                txtcol_qn = _resolve_col(qn, ["note", "text_full", "claim"])
                doc_type_qn = _resolve_col(qn, ["doc_type", "source_type"])
                if qcol_qn and txtcol_qn:
                    qn["_valuation_buyback_quarter"] = (
                        pd.to_datetime(qn[qcol_qn], errors="coerce").dt.to_period("Q").dt.end_time.dt.normalize()
                    )
                    for q in quarter_key:
                        qts = pd.Timestamp(q).normalize()
                        if (
                            buyback_cash_doc_map.get(qts) is not None
                            and buyback_shares_doc_map.get(qts) is not None
                        ):
                            continue
                        win = qn[qn["_valuation_buyback_quarter"] == qts]
                        if win.empty:
                            continue
                        best_qn_exec: Optional[Tuple[float, float, float, Optional[float], str]] = None
                        q_num = ((qts.month - 1) // 3) + 1
                        for _, rec in win.iterrows():
                            note_txt = glx_normalize_text(str(rec.get(txtcol_qn) or ""))
                            if not note_txt or not re.search(r"\brepurchased\b", note_txt, re.I):
                                continue
                            if re.search(
                                r"\b(?:since inception|to date|authorized up to|authorization remained|remaining authorization|remaining capacity|under the program|may repurchase)\b",
                                note_txt,
                                re.I,
                            ):
                                continue
                            shares_match = re.search(
                                r"\brepurchased(?:\s+(?:approximately|approx\.?|about))?\s+([0-9]+(?:\.\d+)?)\s*(million|m)?\s+shares\b",
                                note_txt,
                                re.I,
                            )
                            amount_match = re.search(
                                r"\bfor(?:\s+(?:a\s+)?total\s+of)?\s+\$?\s*([0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\b",
                                note_txt,
                                re.I,
                            )
                            if not shares_match or not amount_match:
                                continue
                            try:
                                share_val = float(str(shares_match.group(1) or "").replace(",", ""))
                                if str(shares_match.group(2) or "").strip().lower() in {"million", "m"}:
                                    share_val *= 1_000_000.0
                                amount_val = self.parse_cap_alloc_amount(amount_match.group(1), amount_match.group(2))
                            except Exception:
                                continue
                            avg_match = re.search(
                                r"\baverage price(?: of| paid per share)?\s+\$?\s*([0-9]+(?:\.\d+)?)\s*/?\s*share\b",
                                note_txt,
                                re.I,
                            )
                            avg_val = None
                            if avg_match:
                                try:
                                    avg_val = float(str(avg_match.group(1) or "").replace(",", ""))
                                except Exception:
                                    avg_val = None
                            score = 10.0
                            if avg_val is not None:
                                score += 3.0
                            if re.search(rf"\bin q{q_num}\b", note_txt, re.I):
                                score += 2.0
                            doc_type_txt = str(rec.get(doc_type_qn) or "").lower() if doc_type_qn else ""
                            if "10-k" in doc_type_txt or "10-q" in doc_type_txt:
                                score += 2.0
                            elif "earnings" in doc_type_txt or "8-k" in doc_type_txt:
                                score += 1.0
                            candidate_qn_exec = (score, float(share_val), float(amount_val), avg_val, note_txt)
                            if best_qn_exec is None or candidate_qn_exec[0] > best_qn_exec[0]:
                                best_qn_exec = candidate_qn_exec
                        if best_qn_exec is None:
                            continue
                        _, share_val, amount_val, avg_val, note_txt = best_qn_exec
                        if buyback_shares_doc_map.get(qts) is None:
                            buyback_shares_doc_map[qts] = share_val
                        if buyback_cash_doc_map.get(qts) is None:
                            buyback_cash_doc_map[qts] = amount_val
                        if buyback_avg_price_doc_map.get(qts) is None and avg_val is not None:
                            buyback_avg_price_doc_map[qts] = float(avg_val)
                        if not str((buyback_doc_note_map or {}).get(qts) or "").strip():
                            buyback_doc_note_map[qts] = note_txt
        with _timed_writer_substage("write_excel.valuation.precompute.keyword_maps"):
            # Keyword maps are context/support only. They can enrich notes and QA,
            # but they should not override quarter-safe execution evidence above.
            for txt_src in [debt_credit_notes, slides_debt, slides_guidance, quarter_notes]:
                if txt_src is None or txt_src.empty:
                    continue
                parsed = self.extract_cap_alloc_text_maps_by_quarter(
                    txt_src,
                    ["note", "snippet", "line", "text", "row_text"],
                )
                for k, v in (parsed.get("buyback_map") or {}).items():
                    if k not in buyback_text_map:
                        buyback_text_map[k] = v
                for k, v in (parsed.get("buyback_shares_map") or {}).items():
                    if k not in buyback_shares_text_map:
                        buyback_shares_text_map[k] = v
                for k, v in (parsed.get("dividend_map") or {}).items():
                    if k not in dividend_text_map:
                        dividend_text_map[k] = v

        def _has_no_buyback_execution_disclosure_for_q_local(q_ref: pd.Timestamp) -> bool:
            qts = pd.Timestamp(q_ref).normalize()
            for rec in list((docs_by_quarter or {}).get(qts) or []):
                rec_text = glx_normalize_text(html.unescape(str(rec.get("text") or "")).replace("\xa0", " "))
                if rec_text and _has_negative_buyback_statement_for_ref_precompute_local(rec_text, qts.date()):
                    return True
            return False

        def _buyback_cashflow_cumulative_local(q_ref: pd.Timestamp) -> Optional[float]:
            qts = pd.Timestamp(q_ref).normalize()
            best_val: Optional[float] = None
            best_score = float("-inf")
            for rec in list((docs_by_quarter or {}).get(qts) or []):
                rec_text = glx_normalize_text(html.unescape(str(rec.get("text") or "")).replace("\xa0", " "))
                if not rec_text or not re.search(r"\bcommon stock repurchases\b", rec_text, re.I):
                    continue
                form_low = str(rec.get("form") or "").lower()
                name_low = str(rec.get("name") or "").lower()
                filing_like = bool(
                    form_low in {"10-q", "10-k"}
                    or "10q" in name_low
                    or "10k" in name_low
                    or "_pbi-" in name_low
                    or "_gpre-" in name_low
                )
                if not filing_like:
                    continue
                q_token_compact = qts.strftime("%Y%m%d")
                q_token_dash = qts.strftime("%Y-%m-%d")
                if q_token_compact not in name_low and q_token_dash not in name_low:
                    continue
                amount_match = re.search(
                    r"\bcommon stock repurchases\b[^0-9()]{0,80}\(\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+)\s*\)",
                    rec_text,
                    re.I,
                )
                if not amount_match:
                    continue
                try:
                    amount_val = float(str(amount_match.group(1) or "").replace(",", "")) * 1_000.0
                except Exception:
                    continue
                score = 0.0
                score += 10.0
                if re.search(r"\b(?:six|nine|twelve|year|three) months ended\b|\byear ended\b|\bstatements of cash flows\b", rec_text, re.I):
                    score += 2.0
                if score > best_score:
                    best_score = score
                    best_val = float(amount_val)
            return best_val

        buyback_cashflow_cumulative_map: Dict[pd.Timestamp, float] = {}
        for q in quarter_key:
            qts = pd.Timestamp(q).normalize()
            cum_v = _buyback_cashflow_cumulative_local(qts)
            if cum_v is not None:
                buyback_cashflow_cumulative_map[qts] = float(cum_v)

        buyback_exec_cash_map: Dict[pd.Timestamp, Any] = {}
        for q in quarter_key:
            qts = pd.Timestamp(q).normalize()
            cash_v = pd.to_numeric(buyback_cash_doc_map.get(qts), errors="coerce")
            buyback_exec_cash_map[qts] = float(cash_v) if pd.notna(cash_v) else None
        prev_cum_by_year: Dict[int, float] = {}
        prev_q_by_year: Dict[int, pd.Timestamp] = {}
        for q in sorted(buyback_cashflow_cumulative_map):
            qts = pd.Timestamp(q).normalize()
            cur_cum = float(buyback_cashflow_cumulative_map[qts])
            q_num = ((qts.month - 1) // 3) + 1
            year_key = int(qts.year)
            derived_q_cash: Optional[float]
            if q_num == 1:
                derived_q_cash = None if _has_no_buyback_execution_disclosure_for_q_local(qts) else cur_cum
            else:
                prev_q = prev_q_by_year.get(year_key)
                prev_cum = prev_cum_by_year.get(year_key)
                prev_q_num = (((int(prev_q.month) - 1) // 3) + 1) if isinstance(prev_q, pd.Timestamp) else None
                if prev_q_num == (q_num - 1) and prev_cum is not None and cur_cum >= prev_cum:
                    derived_q_cash = cur_cum - prev_cum
                else:
                    derived_q_cash = None
            existing_direct_cash = pd.to_numeric(buyback_exec_cash_map.get(qts), errors="coerce")
            if derived_q_cash is not None and pd.isna(existing_direct_cash):
                buyback_exec_cash_map[qts] = float(derived_q_cash)
            prev_cum_by_year[year_key] = cur_cum
            prev_q_by_year[year_key] = qts
        if dividend_doc_map:
            for k, v in dividend_doc_map.items():
                if v is None:
                    continue
                existing_dividend_cash = pd.to_numeric(dividend_map.get(k), errors="coerce")
                # `History_Q.dividends_cash` is already the quarter-safe cash dividend
                # fact. SEC document scans can pick up YTD/TTM dividend rows, so use
                # them only as a fallback when the quarterly fact is absent.
                if pd.isna(existing_dividend_cash):
                    dividend_map[k] = v
        if buyback_shares_doc_map:
            for k, v in buyback_shares_doc_map.items():
                if v is None:
                    continue
                buyback_shares_text_map[k] = v

        with _timed_writer_substage("write_excel.valuation.precompute.buyback_share_maps"):
            # Share/count maps are memoized separately because later TTM display and
            # QA logic need both cash and shares views without re-reading document text.
            buyback_shares_map: Dict[pd.Timestamp, Any] = {}
            for q in quarter_key:
                qts = pd.Timestamp(q).normalize()
                v = pd.to_numeric(buyback_shares_doc_map.get(qts), errors="coerce")
                v = float(v) if pd.notna(v) else None
                if v in (None, 0):
                    note_txt = str((buyback_doc_note_map or {}).get(qts) or "")
                    note_low = note_txt.lower()
                    note_quarter_safe = bool(
                        note_txt
                        and not re.search(r"\b(to date|since inception|under the program|authorization remained|authorized up to|may repurchase)\b", note_low, re.I)
                        and re.search(
                            r"\bon\s+[A-Z][a-z]+\s+\d{1,2},\s+\d{4}\b|\bduring the quarter\b|\bin q[1-4]\b|three months ended|common stock purchases during the three months ended",
                            note_txt,
                            re.I,
                        )
                    )
                    if note_quarter_safe:
                        m_sh = re.search(
                            r"repurchased(?:\s+(?:approximately|approx\.?|about))?\s+([0-9]+(?:\.\d+)?)\s*(million|m)?\s+shares",
                            note_txt,
                            re.I,
                        )
                        if m_sh:
                            try:
                                v = float(m_sh.group(1))
                                if str(m_sh.group(2) or "").strip().lower() in {"million", "m"}:
                                    v *= 1_000_000.0
                            except Exception:
                                v = v
                buyback_shares_map[qts] = v

        buyback_map = buyback_exec_cash_map

        buyback_quarter_coverage_map: Dict[pd.Timestamp, bool] = {}
        for q in quarter_key:
            qts = pd.Timestamp(q).normalize()
            if not _has_no_buyback_execution_disclosure_for_q_local(qts):
                continue
            buyback_map[qts] = 0.0
            buyback_shares_map[qts] = 0.0
            buyback_avg_price_doc_map.pop(qts, None)
            buyback_doc_note_map.pop(qts, None)
            buyback_shares_text_map.pop(qts, None)
        for q in quarter_key:
            qts = pd.Timestamp(q).normalize()
            covered = bool(buyback_map.get(qts) is not None or buyback_shares_map.get(qts) is not None)
            if not covered:
                q_num = ((qts.month - 1) // 3) + 1
                q_year = int(qts.year)
                q_labels = {
                    1: r"(?:q1|first quarter|march 31,\s*%d)" % q_year,
                    2: r"(?:q2|second quarter|june 30,\s*%d)" % q_year,
                    3: r"(?:q3|third quarter|september 30,\s*%d)" % q_year,
                    4: r"(?:q4|fourth quarter|december 31,\s*%d)" % q_year,
                }.get(q_num, rf"q{q_num}")
                for rec in list((docs_by_quarter or {}).get(qts) or []):
                    rec_text = glx_normalize_text(str(rec.get("text") or ""))
                    if not rec_text:
                        continue
                    if re.search(
                        rf"\b(?:did not repurchas\w*|no repurchas\w* was made)\b[^.]*\b{q_labels}\b",
                        rec_text,
                        re.I,
                    ):
                        covered = True
                        break
            buyback_quarter_coverage_map[qts] = bool(covered)

        def _ttm_zero_fill_local(
            src: Dict[pd.Timestamp, Any],
            qq: pd.Timestamp,
            coverage_map: Optional[Dict[pd.Timestamp, bool]] = None,
        ) -> Optional[float]:
            if not src:
                return None
            last4 = last4_quarters_map.get(pd.Timestamp(qq))
            if not last4:
                return None
            if coverage_map is not None:
                if not all(bool(coverage_map.get(pd.Timestamp(qv))) for qv in last4):
                    return None
            vals = [src.get(pd.Timestamp(qv)) for qv in last4]
            if all(v is None for v in vals):
                return None
            return float(sum(float(v) if v is not None else 0.0 for v in vals))

        def _money_m_local(v: Optional[float]) -> str:
            if v is None or pd.isna(v):
                return "n/a"
            return f"${float(v) / 1e6:,.1f}m"

        def _delta_m_local(v: Optional[float]) -> str:
            if v is None or pd.isna(v):
                return "n/a"
            sgn = "+" if float(v) >= 0 else "-"
            return f"{sgn}${abs(float(v)) / 1e6:,.1f}m"

        def _shares_m_local(v: Optional[float]) -> str:
            if v is None or pd.isna(v):
                return "n/a"
            sgn = "+" if float(v) >= 0 else "-"
            return f"{sgn}{abs(float(v)) / 1e6:,.3f}m"

        def _ps_local(v: Optional[float]) -> str:
            if v is None or pd.isna(v):
                return "n/a"
            return f"${float(v):,.3f}"

        def _build_buyback_auth_only_summary_local(
            *,
            remaining_dollars: Optional[float],
            auth_snapshot: Dict[str, Any],
            auth_context_text: str,
            note_source_text: str,
            maturity_text: Optional[str],
        ) -> str:
            bits_local: List[str] = []
            if remaining_dollars is not None and not pd.isna(remaining_dollars):
                bits_local.append(f"Remaining capacity {_money_m_local(float(remaining_dollars))}")
            latest_auth_date = auth_snapshot.get("asof_date")
            auth_increase = pd.to_numeric(auth_snapshot.get("authorization_increase_dollars"), errors="coerce")
            auth_total = pd.to_numeric(auth_snapshot.get("authorization_dollars"), errors="coerce")
            auth_blob = glx_normalize_text(
                " | ".join(
                    [
                        str(auth_snapshot.get("snippet") or ""),
                        str(auth_context_text or ""),
                        str(note_source_text or ""),
                    ]
                )
            )
            parsed_increase_val = None
            parsed_increase_kind = ""
            if pd.isna(auth_increase):
                inc_fallback = re.search(
                    r"\b(?:increase(?:d)?|raised?|expanded?)\b[^|]{0,140}?\b(by|to)\b\s+\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?",
                    auth_blob,
                    re.I,
                )
                if inc_fallback:
                    try:
                        parsed_increase_val = float(str(inc_fallback.group(2)).replace(",", ""))
                        unit_txt = str(inc_fallback.group(3) or "").lower()
                        if unit_txt in {"billion", "bn"}:
                            parsed_increase_val *= 1e9
                        elif unit_txt in {"million", "m"} or parsed_increase_val < 2000:
                            parsed_increase_val *= 1e6
                        parsed_increase_kind = str(inc_fallback.group(1) or "").lower()
                    except Exception:
                        parsed_increase_val = None
            if pd.notna(auth_increase) and float(auth_increase) >= 1_000_000.0:
                bits_local.append(
                    f"Latest increase by {_money_m_local(float(auth_increase))}"
                    + (f" on {latest_auth_date}" if latest_auth_date else "")
                )
            elif parsed_increase_val is not None and float(parsed_increase_val) >= 1_000_000.0:
                bits_local.append(
                    f"Latest increase {parsed_increase_kind or 'by'} {_money_m_local(float(parsed_increase_val))}"
                    + (f" on {latest_auth_date}" if latest_auth_date else "")
                )
            elif pd.notna(auth_total) and float(auth_total) >= 1_000_000.0:
                auth_kind = "increase to" if "increase" in str(auth_snapshot.get("kind") or "").lower() else "authorization"
                bits_local.append(
                    f"Latest {auth_kind} {_money_m_local(float(auth_total))}"
                    + (f" on {latest_auth_date}" if latest_auth_date else "")
                )
            if maturity_text:
                bits_local.append(f"Maturity date {maturity_text}")
            continuation_blob = glx_normalize_text(
                " | ".join(
                    [
                        str(note_source_text or ""),
                        str(auth_context_text or ""),
                        str(auth_snapshot.get("snippet") or ""),
                    ]
                )
            )
            if continuation_blob and re.search(r"(expect|intend|plan|execute|continue)[^.]{0,120}(repurch|buyback)", continuation_blob, re.I):
                bits_local.append("Continuation mentioned.")
            return " | ".join([bit for bit in bits_local if bit]) or "No current authorization / remaining-capacity disclosure."

        def _pbi_ytd_buyback_split_for_q_local(q_ref: pd.Timestamp, note_source_text: str = "") -> Dict[str, Any]:
            if not is_pbi_profile:
                return {}
            qts = pd.Timestamp(q_ref).normalize()
            q_num = ((int(qts.month) - 1) // 3) + 1
            quarter_tokens = {
                1: r"(?:q1|first quarter)",
                2: r"(?:q2|second quarter)",
                3: r"(?:q3|third quarter)",
                4: r"(?:q4|fourth quarter)",
            }.get(q_num, r"(?:q[1-4]|first quarter|second quarter|third quarter|fourth quarter)")

            def _parse_shares(raw_num: Any, unit_in: Any = "") -> Optional[float]:
                try:
                    val = float(str(raw_num or "").replace(",", ""))
                except Exception:
                    return None
                unit_low = str(unit_in or "").strip().lower()
                if unit_low in {"million", "m"} or val < 100_000.0:
                    val *= 1_000_000.0
                return float(val) if val > 0 else None

            def _parse_money(raw_num: Any, unit_in: Any = "") -> Optional[float]:
                try:
                    val = float(str(raw_num or "").replace(",", ""))
                except Exception:
                    return None
                unit_low = str(unit_in or "").strip().lower()
                if unit_low in {"billion", "bn"}:
                    val *= 1_000_000_000.0
                elif unit_low in {"million", "m"} or val < 2_000.0:
                    val *= 1_000_000.0
                return float(val) if val > 0 else None

            def _money_note(val: float) -> str:
                return f"${float(val) / 1_000_000.0:,.1f}m"

            def _shares_note(val: float) -> str:
                return f"{float(val) / 1_000_000.0:,.1f}m shares"

            def _add_candidate_text(out: List[str], text_in: Any) -> None:
                txt = glx_normalize_text(html.unescape(str(text_in or "")).replace("\xa0", " "))
                if txt and txt.lower() not in {z.lower() for z in out}:
                    out.append(txt)

            candidates: List[str] = []
            _add_candidate_text(candidates, note_source_text)
            for maybe_map in [buyback_doc_note_map, buyback_text_map, buyback_shares_text_map]:
                try:
                    _add_candidate_text(candidates, maybe_map.get(qts))
                except Exception:
                    pass
            try:
                for dp in _sec_cache_docs_for_token_local(cache_root, qts.strftime("%Y%m%d")):
                    if not dp.is_file():
                        continue
                    _add_candidate_text(candidates, self.extract_valuation_filing_doc_text(dp))
            except Exception:
                pass
            local_roots: List[Path] = []
            try:
                local_roots.append(cache_root.parent)
            except Exception:
                pass
            try:
                if cache_root.parent.name.lower() == "sec_cache":
                    local_roots.append(cache_root.parent.parent / str(profile_ticker or ticker or "").strip().upper())
            except Exception:
                pass
            seen_local_paths: set[str] = set()
            for root_in in local_roots:
                try:
                    if not root_in or not root_in.exists():
                        continue
                    for family in ["earnings_release", "CEO_letters", "press_release"]:
                        fam_dir = root_in / family
                        if not fam_dir.exists():
                            continue
                        for dp in sorted(fam_dir.glob("*")):
                            if not dp.is_file():
                                continue
                            dp_key = str(dp.resolve())
                            if dp_key in seen_local_paths:
                                continue
                            seen_local_paths.add(dp_key)
                            name_low = dp.name.lower()
                            if "2026" not in name_low and "q1" not in name_low:
                                continue
                            if dp.suffix.lower() not in {".htm", ".html", ".txt", ".xml"}:
                                continue
                            _add_candidate_text(candidates, self.extract_valuation_filing_doc_text(dp))
                except Exception:
                    continue

            total_pat = re.compile(
                r"\brepurchas\w*\b[^.]{0,260}?"
                r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|m)?\s+shares\b"
                r"[^.]{0,180}?\bfor\s+\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*"
                r"(million|billion|m|bn)?\b",
                re.I,
            )
            include_pat = re.compile(
                rf"\bincluding\s+([0-9]{{1,3}}(?:,[0-9]{{3}})+|[0-9]+(?:\.\d+)?)\s*(million|m)?\s+shares\b"
                rf"[^.]{{0,180}}?\bfor\s+\$?\s*([0-9]{{1,3}}(?:,[0-9]{{3}})+|[0-9]+(?:\.\d+)?)\s*"
                rf"(million|billion|m|bn)?\b[^.]{{0,180}}?\bin\s+(?:the\s+)?{quarter_tokens}\b",
                re.I,
            )
            for txt in candidates:
                if not re.search(r"\byear[- ]to[- ]date|ytd|through\s+[A-Z][a-z]+\s+\d{1,2}", txt, re.I):
                    continue
                total_match = total_pat.search(txt)
                include_match = include_pat.search(txt)
                if not total_match or not include_match:
                    continue
                total_shares = _parse_shares(total_match.group(1), total_match.group(2))
                total_amount = _parse_money(total_match.group(3), total_match.group(4))
                quarter_shares = _parse_shares(include_match.group(1), include_match.group(2))
                quarter_amount = _parse_money(include_match.group(3), include_match.group(4))
                if (
                    total_shares is None
                    or total_amount is None
                    or quarter_shares is None
                    or quarter_amount is None
                    or total_shares + 1.0 < quarter_shares
                    or total_amount + 1.0 < quarter_amount
                ):
                    continue
                cutoff_match = re.search(
                    r"\bthrough\s+((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2})(?:,\s*20\d{2})?\b",
                    txt,
                    re.I,
                )
                cutoff_txt = re.sub(r"\s+", " ", str(cutoff_match.group(1) or "")).strip() if cutoff_match else ""
                post_shares = max(0.0, float(total_shares) - float(quarter_shares))
                post_amount = max(0.0, float(total_amount) - float(quarter_amount))
                avg_price = float(quarter_amount) / float(quarter_shares) if quarter_shares else None
                quarter_note = (
                    f"Latest quarter +{float(quarter_shares) / 1_000_000.0:,.3f}m"
                    + (f" at ${float(avg_price):.2f}/share" if avg_price is not None else "")
                    + f" for {_money_note(float(quarter_amount))}"
                )
                post_note = ""
                if post_shares > 0 or post_amount > 0:
                    post_note = capital_return_build_buyback_note(
                        shares=float(post_shares),
                        cash=float(post_amount),
                        post_quarter=True,
                        through_date=cutoff_txt,
                    )
                return {
                    "quarter_shares": float(quarter_shares),
                    "quarter_amount": float(quarter_amount),
                    "quarter_avg_price": float(avg_price) if avg_price is not None else None,
                    "post_shares": float(post_shares),
                    "post_amount": float(post_amount),
                    "cutoff": cutoff_txt,
                    "quarter_note": quarter_note,
                    "post_note": post_note,
                }
            return {}

        def _pbi_cash_dividend_ps_for_q_local(
            q_ref: pd.Timestamp,
            *,
            doc_ps: Optional[float],
            prev_doc_ps: Optional[float],
        ) -> Optional[float]:
            if not is_pbi_profile:
                return doc_ps
            qts = pd.Timestamp(q_ref).normalize()
            candidates: List[str] = []
            for txt in [dividend_doc_note_map.get(qts) if dividend_doc_note_map else ""]:
                norm = glx_normalize_text(str(txt or ""))
                if norm:
                    candidates.append(norm)
            try:
                for dp in _sec_cache_docs_for_token_local(cache_root, qts.strftime("%Y%m%d")):
                    if dp.is_file():
                        norm = glx_normalize_text(self.extract_valuation_filing_doc_text(dp))
                        if norm:
                            candidates.append(norm)
            except Exception:
                pass
            blob = " | ".join(candidates)
            m_from_to = re.search(
                r"\b(?:increase(?:d|s)?|raising|raised)\b[^.]{0,120}?\bdividend\b[^.]{0,120}?\bfrom\s+\$?\s*"
                r"([0-9]+(?:\.\d+)?)\s+to\s+\$?\s*([0-9]+(?:\.\d+)?)\s+per\s+share",
                blob,
                re.I,
            )
            if m_from_to:
                try:
                    return float(m_from_to.group(1))
                except Exception:
                    pass
            if qts.month == 3 and prev_doc_ps is not None and doc_ps is not None:
                try:
                    if float(doc_ps) > float(prev_doc_ps) + 0.004:
                        return float(prev_doc_ps)
                except Exception:
                    pass
            return doc_ps

        def _precompute_qmap_get_local(mp: Dict[pd.Timestamp, Any], q_ref: pd.Timestamp) -> Any:
            if not mp:
                return None
            qts = pd.Timestamp(q_ref).normalize()
            if qts in mp:
                return mp.get(qts)
            for raw_q, val in mp.items():
                try:
                    if pd.Timestamp(raw_q).normalize() == qts:
                        return val
                except Exception:
                    continue
            return None

        def _parse_buyback_maturity_precompute_local(txt: Optional[str]) -> Optional[str]:
            if not txt:
                return None
            s = str(txt)
            m = re.search(
                r"(?:by|through|until|matur(?:e|ity)|expir(?:e|es|ation)|end(?:ing)?\s+of)\s+"
                r"((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s*\d{4}|\d{4})",
                s,
                re.I,
            )
            if m:
                return str(m.group(1)).strip()
            return None

        buyback_ttm_resolved_map: Dict[pd.Timestamp, Any] = {}
        dividend_ttm_resolved_map: Dict[pd.Timestamp, Any] = {}
        buyback_shares_ttm_resolved_map: Dict[pd.Timestamp, Any] = {}
        capital_return_resolved: Dict[pd.Timestamp, Dict[str, Any]] = {}
        buyback_cash_ttm_displayable_map: Dict[pd.Timestamp, bool] = {}
        buyback_shares_ttm_displayable_map: Dict[pd.Timestamp, bool] = {}
        sorted_quarters_for_dividend = sorted(pd.Timestamp(q).normalize() for q in quarter_key)
        previous_quarter_for_dividend: Dict[pd.Timestamp, pd.Timestamp] = {
            qv: sorted_quarters_for_dividend[idx - 1]
            for idx, qv in enumerate(sorted_quarters_for_dividend)
            if idx > 0
        }
        for q in quarter_key:
            last4 = list(last4_quarters_map.get(pd.Timestamp(q)) or [])
            cash_ttm_displayable = bool(last4) and all(
                buyback_map.get(pd.Timestamp(qv)) is not None for qv in last4
            )
            shares_ttm_displayable = bool(last4) and all(
                buyback_shares_doc_map.get(pd.Timestamp(qv)) is not None for qv in last4
            )
            buyback_cash_ttm_displayable_map[q] = cash_ttm_displayable
            buyback_shares_ttm_displayable_map[q] = shares_ttm_displayable
            buyback_ttm_resolved_map[q] = (
                _ttm_zero_fill_local(buyback_map, q, buyback_quarter_coverage_map)
                if cash_ttm_displayable
                else None
            )
            dividend_ttm_resolved_map[q] = _ttm_zero_fill_local(dividend_map, q)
            buyback_shares_ttm_resolved_map[q] = (
                _ttm_zero_fill_local(buyback_shares_map, q, buyback_quarter_coverage_map)
                if shares_ttm_displayable
                else None
            )

        for q in quarter_key:
            q_prev = pd.Timestamp(q) - pd.DateOffset(years=1)
            if is_anf_profile:
                q_prev_resolved = _anf_prior_year_quarter(pd.Timestamp(q), sorted_quarters_for_dividend)
                if q_prev_resolved is not None:
                    q_prev = pd.Timestamp(q_prev_resolved)
            bb_cash_q = buyback_map.get(q)
            bb_cash_ttm = buyback_ttm_resolved_map.get(q)
            bb_cash_ttm_ly = buyback_ttm_resolved_map.get(q_prev)
            bb_cash_yoy_delta = (
                float(bb_cash_ttm) - float(bb_cash_ttm_ly)
                if bb_cash_ttm is not None and bb_cash_ttm_ly is not None
                else None
            )
            if bb_cash_yoy_delta is not None and abs(float(bb_cash_yoy_delta)) < 50_000.0:
                bb_cash_yoy_delta = None
            show_bb_cash_trend = bool(
                bb_cash_ttm is not None
                and bool(buyback_cash_ttm_displayable_map.get(q))
                and bool(buyback_cash_ttm_displayable_map.get(q_prev))
                and bb_cash_yoy_delta is not None
            )
            if not show_bb_cash_trend:
                bb_cash_ttm = None
                bb_cash_yoy_delta = None
            bb_shares_q = buyback_shares_map.get(q)
            bb_shares_ttm = buyback_shares_ttm_resolved_map.get(q)
            bb_shares_ttm_ly = buyback_shares_ttm_resolved_map.get(q_prev)
            bb_shares_yoy_delta = (
                float(bb_shares_ttm) - float(bb_shares_ttm_ly)
                if bb_shares_ttm is not None and bb_shares_ttm_ly is not None
                else None
            )
            if bb_shares_yoy_delta is not None and abs(float(bb_shares_yoy_delta)) < 500.0:
                bb_shares_yoy_delta = None
            show_bb_shares_trend = bool(
                bb_shares_ttm is not None
                and bool(buyback_shares_ttm_displayable_map.get(q))
                and bool(buyback_shares_ttm_displayable_map.get(q_prev))
                and bb_shares_yoy_delta is not None
            )
            if not show_bb_shares_trend:
                bb_shares_ttm = None
                bb_shares_yoy_delta = None
            if bb_shares_q is None and not _has_no_buyback_execution_disclosure_for_q_local(pd.Timestamp(q)):
                note_txt = str((buyback_doc_note_map or {}).get(q) or "")
                note_low = note_txt.lower()
                note_quarter_safe = bool(
                    note_txt
                    and not re.search(r"\b(to date|since inception|under the program|authorization remained|authorized up to|may repurchase)\b", note_low, re.I)
                    and re.search(
                        r"\bon\s+[A-Z][a-z]+\s+\d{1,2},\s+\d{4}\b|\bduring the quarter\b|\bin q[1-4]\b|three months ended|common stock purchases during the three months ended",
                        note_txt,
                        re.I,
                    )
                )
                if note_quarter_safe:
                    m_bb_sh = re.search(
                        r"repurchased(?:\s+(?:approximately|approx\.?|about))?\s+([0-9]+(?:\.\d+)?)\s*(million|m)?\s+shares",
                        note_txt,
                        re.I,
                    )
                    if m_bb_sh:
                        try:
                            bb_shares_q = float(str(m_bb_sh.group(1) or "").replace(",", ""))
                            if str(m_bb_sh.group(2) or "").strip().lower() in {"million", "m"}:
                                bb_shares_q *= 1_000_000.0
                        except Exception:
                            bb_shares_q = None
            bb_avg_price = buyback_avg_price_doc_map.get(q)
            try:
                if bb_avg_price is None and bb_cash_q is not None and bb_shares_q not in (None, 0):
                    bb_avg_price = float(bb_cash_q) / float(bb_shares_q)
            except Exception:
                bb_avg_price = None
            dv_cash_q = dividend_map.get(q)
            dv_cash_ttm = dividend_ttm_resolved_map.get(q)
            dv_ps_q = dividend_ps_doc_map.get(q)
            prev_div_q = previous_quarter_for_dividend.get(pd.Timestamp(q).normalize())
            prev_div_ps_q = dividend_ps_doc_map.get(prev_div_q) if prev_div_q is not None else None
            buyback_display_bits: List[str] = []
            show_bb_shares_trend = bool(
                bb_shares_ttm is not None
                and bool(buyback_shares_ttm_displayable_map.get(q))
                and bool(buyback_shares_ttm_displayable_map.get(q_prev))
                and bb_shares_yoy_delta is not None
            )
            if bb_shares_q is not None:
                buyback_display_bits.append(f"QoQ {_shares_m_local(bb_shares_q)}")
            if show_bb_shares_trend:
                buyback_display_bits.append(f"TTM {_shares_m_local(bb_shares_ttm)}")
            if (
                bb_shares_yoy_delta is not None
                and bool(buyback_shares_ttm_displayable_map.get(q))
                and bool(buyback_shares_ttm_displayable_map.get(q_prev))
            ):
                buyback_display_bits.append(f"YoY Δ {_shares_m_local(bb_shares_yoy_delta)}")
            buybacks_text = " | ".join(buyback_display_bits) if buyback_display_bits else "n/a"
            if bb_cash_q is None and bb_cash_ttm is None:
                buyback_note_summary = "Cash buybacks not directly observed in quarter-safe execution sources."
            else:
                buyback_note_bits: List[str] = []
                if bb_cash_q is not None:
                    buyback_note_bits.append(f"Cash buybacks spent latest quarter {_money_m_local(bb_cash_q)}")
                else:
                    buyback_note_bits.append("Cash buybacks spent latest quarter n/a")
                if bb_cash_ttm is not None and bool(buyback_cash_ttm_displayable_map.get(q)):
                    buyback_note_bits.append(f"TTM {_money_m_local(bb_cash_ttm)}")
                if (
                    bb_cash_yoy_delta is not None
                    and bool(buyback_cash_ttm_displayable_map.get(q))
                    and bool(buyback_cash_ttm_displayable_map.get(q_prev))
                ):
                    buyback_note_bits.append(f"YoY Δ {_delta_m_local(bb_cash_yoy_delta)}")
                buyback_note_summary = " | ".join(buyback_note_bits)
            if bb_shares_q is not None:
                if is_anf_profile and _anf_buyback_execution_is_year_or_ttm(
                    q,
                    str((buyback_doc_note_map or {}).get(q) or ""),
                    cash_amount=bb_cash_q,
                    shares_amount=bb_shares_q,
                ):
                    buyback_note_summary += " | " + _anf_format_year_ttm_buyback_summary(
                        q,
                        shares_amount=bb_shares_q,
                        cash_amount=bb_cash_q,
                        avg_price=bb_avg_price,
                    )
                elif bb_avg_price is not None:
                    buyback_note_summary += f" | Latest quarter {_shares_m_local(bb_shares_q)} at ${float(bb_avg_price):.2f}/share"
                else:
                    buyback_note_summary += f" | Latest quarter {_shares_m_local(bb_shares_q)}"
            ytd_buyback_split = _pbi_ytd_buyback_split_for_q_local(
                pd.Timestamp(q),
                str((buyback_doc_note_map or {}).get(q) or ""),
            )
            if ytd_buyback_split:
                if bb_shares_q is None:
                    bb_shares_q = ytd_buyback_split.get("quarter_shares")
                if bb_cash_q is None:
                    bb_cash_q = ytd_buyback_split.get("quarter_amount")
                if bb_avg_price is None:
                    bb_avg_price = ytd_buyback_split.get("quarter_avg_price")
                split_note_summary = str(ytd_buyback_split.get("quarter_note") or "").strip()
                split_q_shares = ytd_buyback_split.get("quarter_shares")
                split_q_cash = ytd_buyback_split.get("quarter_amount")
                try:
                    if (
                        bb_cash_q is not None
                        and split_q_cash is not None
                        and abs(float(bb_cash_q) - float(split_q_cash)) <= 5_000_000.0
                    ):
                        split_q_cash = bb_cash_q
                except Exception:
                    split_q_cash = ytd_buyback_split.get("quarter_amount")
                try:
                    if split_q_shares not in (None, 0) and split_q_cash not in (None, 0):
                        split_avg = float(split_q_cash) / float(split_q_shares)
                        if is_anf_profile and _anf_buyback_execution_is_year_or_ttm(
                            q,
                            str((buyback_doc_note_map or {}).get(q) or ""),
                            cash_amount=split_q_cash,
                            shares_amount=split_q_shares,
                        ):
                            split_note_summary = _anf_format_year_ttm_buyback_summary(
                                q,
                                shares_amount=split_q_shares,
                                cash_amount=split_q_cash,
                                avg_price=split_avg,
                            )
                        else:
                            split_note_summary = (
                                f"Latest quarter +{float(split_q_shares) / 1_000_000.0:,.3f}m"
                                f" at ${float(split_avg):.2f}/share for {_money_m_local(float(split_q_cash))}"
                            )
                except Exception:
                    pass
                if split_note_summary:
                    buyback_note_summary = split_note_summary
            bb_maturity = _parse_buyback_maturity_precompute_local(
                str((buyback_doc_note_map or {}).get(q) or "")
            )
            execution_buyback_note_summary = str(buyback_note_summary or "").strip()
            auth_buyback_note_summary = _build_buyback_auth_only_summary_local(
                remaining_dollars=None,
                auth_snapshot={},
                auth_context_text="",
                note_source_text=str((buyback_doc_note_map or {}).get(q) or ""),
                maturity_text=bb_maturity,
            )
            buyback_note_parts: List[str] = []
            if ytd_buyback_split and str(ytd_buyback_split.get("post_note") or "").strip():
                buyback_note_parts.append(str(ytd_buyback_split.get("post_note") or "").strip())
            elif execution_buyback_note_summary and "not directly observed" not in execution_buyback_note_summary.lower():
                buyback_note_parts.append(execution_buyback_note_summary)
            if (
                auth_buyback_note_summary
                and auth_buyback_note_summary != "No current authorization / remaining-capacity disclosure."
                and auth_buyback_note_summary not in buyback_note_parts
            ):
                buyback_note_parts.append(auth_buyback_note_summary)
            buyback_note_summary = " | ".join(buyback_note_parts) or auth_buyback_note_summary
            buyback_qa_bits: List[str] = []
            if bb_shares_q is not None:
                buyback_qa_bits.append(f"shares {_shares_m_local(bb_shares_q)}")
            if bb_cash_q is not None:
                buyback_qa_bits.append(f"cash {_money_m_local(bb_cash_q)}")
            if bb_avg_price is not None:
                buyback_qa_bits.append(f"avg price ${float(bb_avg_price):.2f}/share")
            if _has_no_buyback_execution_disclosure_for_q_local(pd.Timestamp(q)):
                buyback_qa_summary = (
                    f"No shares repurchased during {pd.Timestamp(q).date()} quarter; cumulative program totals are context only."
                )
            elif buyback_qa_bits:
                if is_anf_profile and _anf_buyback_execution_is_year_or_ttm(
                    q,
                    str((buyback_doc_note_map or {}).get(q) or ""),
                    cash_amount=bb_cash_q,
                    shares_amount=bb_shares_q,
                ):
                    buyback_qa_summary = (
                        f"{_anf_fiscal_year_from_quarter_end(q) or pd.Timestamp(q).year} year / TTM buyback evidence "
                        f"for {pd.Timestamp(q).date()}: {' | '.join(buyback_qa_bits)}"
                    )
                else:
                    buyback_qa_summary = f"Latest quarter buyback evidence for {pd.Timestamp(q).date()}: {' | '.join(buyback_qa_bits)}"
            else:
                suppress_reason = "no explicit quarter-safe execution"
                if buyback_text_map.get(q) is not None or buyback_shares_text_map.get(q) is not None:
                    suppress_reason = "context/program text blocked for execution metrics"
                buyback_qa_summary = f"No explicit latest-quarter buyback execution resolved for {pd.Timestamp(q).date()}: {suppress_reason}."
            q_is_pbi_cash_dividend_note = bool(
                is_pbi_profile
                and pd.Timestamp(q).normalize() >= pd.Timestamp("2026-03-31")
                and dv_cash_q is not None
            )
            cash_paid_div_ps_q = _pbi_cash_dividend_ps_for_q_local(
                pd.Timestamp(q),
                doc_ps=dv_ps_q,
                prev_doc_ps=prev_div_ps_q,
            )
            if q_is_pbi_cash_dividend_note and dv_cash_q is not None:
                cash_implied_div_ps_q = None
                share_den = None
                try:
                    share_den = _precompute_qmap_get_local(shares_map_for_dividend, pd.Timestamp(q))
                except Exception:
                    share_den = None
                if share_den in (None, 0):
                    try:
                        share_den = _precompute_qmap_get_local(shares_out_map_for_dividend, pd.Timestamp(q))
                    except Exception:
                        share_den = None
                try:
                    if share_den not in (None, 0) and pd.notna(share_den):
                        implied_ps = float(dv_cash_q) / float(share_den)
                        rounded_ps = round(float(implied_ps) + 1e-9, 2)
                        if 0.0 < rounded_ps < 5.0:
                            cash_implied_div_ps_q = float(rounded_ps)
                except Exception:
                    cash_implied_div_ps_q = None
                if cash_implied_div_ps_q is not None:
                    try:
                        if cash_paid_div_ps_q is None or float(cash_implied_div_ps_q) < float(cash_paid_div_ps_q) - 0.004:
                            cash_paid_div_ps_q = float(cash_implied_div_ps_q)
                    except Exception:
                        cash_paid_div_ps_q = float(cash_implied_div_ps_q)
            dividend_note_summary = ""
            if q_is_pbi_cash_dividend_note:
                q_num_label = ((int(pd.Timestamp(q).month) - 1) // 3) + 1
                ps_piece = f" / ${float(cash_paid_div_ps_q):.2f} per share" if cash_paid_div_ps_q is not None else ""
                dividends_text = (
                    f"Q{q_num_label} cash dividends paid {_money_m_local(dv_cash_q)}{ps_piece} | "
                    f"TTM cash dividends {_money_m_local(dv_cash_ttm)}"
                )
                dividend_note_summary = (
                    "Board approved next quarterly dividend increase to $0.10/share, payable June 5, 2026. "
                    "Dividend approval remains at Board discretion."
                )
            elif dv_ps_q is not None:
                dividends_text = (
                    f"Latest quarter div/share {_ps_local(dv_ps_q)} | "
                    f"TTM dividend cash {_money_m_local(dv_cash_ttm)}"
                )
            else:
                dividends_text = "No current common dividend/share signal."
            capital_return_resolved[q] = {
                "buyback_cash_q": bb_cash_q,
                "buyback_cash_ttm": bb_cash_ttm,
                "buyback_cash_yoy_delta": bb_cash_yoy_delta,
                "buyback_shares_q": bb_shares_q,
                "buyback_shares_ttm": bb_shares_ttm,
                "buyback_shares_yoy_delta": bb_shares_yoy_delta,
                "buyback_avg_price": bb_avg_price,
                "buybacks_text": buybacks_text,
                "buyback_note_summary": buyback_note_summary,
                "buyback_qa_summary": buyback_qa_summary,
                "buyback_cash_ttm_displayable": show_bb_cash_trend,
                "buyback_shares_ttm_displayable": show_bb_shares_trend,
                "dividend_cash_q": dv_cash_q,
                "dividend_cash_ttm": dv_cash_ttm,
                "dividend_ps_q": dv_ps_q,
                "cash_paid_dividend_ps_q": cash_paid_div_ps_q,
                "dividends_text": dividends_text,
                "dividend_note_summary": dividend_note_summary,
                "buyback_note_source": str((buyback_doc_note_map or {}).get(q) or ""),
                "dividend_note_source": str((dividend_doc_note_map or {}).get(q) or ""),
            }

        leverage_indexed = render_bundle.get("leverage_indexed")
        if leverage_indexed is None:
            leverage_indexed = pd.DataFrame()
        leverage_basis_lookup: Dict[pd.Timestamp, Dict[str, Any]] = {}
        if leverage_indexed is not None and not leverage_indexed.empty:
            lev_src = leverage_indexed.reset_index().copy()
            if "quarter" in lev_src.columns:
                lev_src["quarter"] = pd.to_datetime(lev_src["quarter"], errors="coerce")
                lev_src = lev_src[lev_src["quarter"].notna()].drop_duplicates(subset=["quarter"], keep="last")
                leverage_basis_lookup = {
                    pd.Timestamp(row["quarter"]).normalize(): row
                    for _, row in lev_src.iterrows()
                }
        debt_core_map = dict(render_bundle.get("debt_core_map") or {})
        cash_map = dict(render_bundle.get("cash_map") or {})
        ebitda_ttm_map = dict(render_bundle.get("ebitda_ttm_map") or {})
        adj_ebit_q = dict(getattr(getattr(ctx_ref, "derived", None), "valuation_adj_ebit_q", {}) or {})
        adj_ebit_ttm_q = dict(getattr(getattr(ctx_ref, "derived", None), "valuation_adj_ebit_ttm_q", {}) or {})
        adj_ebitda_q = dict(getattr(getattr(ctx_ref, "derived", None), "valuation_adj_ebitda_q", {}) or {})
        adj_ebitda_ttm_q = dict(getattr(getattr(ctx_ref, "derived", None), "valuation_adj_ebitda_ttm_q", {}) or {})

        valuation_audit: Dict[pd.Timestamp, Dict[str, Any]] = {}
        for q in quarter_key:
            lev_row = leverage_basis_lookup.get(pd.Timestamp(q).normalize())
            if lev_row is None:
                lev_row = {}
            debt_core_val = debt_core_map.get(q)
            cash_val = cash_map.get(q)
            gaap_ebitda_ttm_val = pd.to_numeric(ebitda_ttm_map.get(q), errors="coerce")
            gaap_ebitda_ttm_val = float(gaap_ebitda_ttm_val) if pd.notna(gaap_ebitda_ttm_val) else None
            adj_ebitda_ttm_val = pd.to_numeric(adj_ebitda_ttm_q.get(q), errors="coerce")
            adj_ebitda_ttm_val = float(adj_ebitda_ttm_val) if pd.notna(adj_ebitda_ttm_val) else None
            net_lev_val = pd.to_numeric(lev_row.get("corporate_net_leverage"), errors="coerce")
            net_lev_val = float(net_lev_val) if pd.notna(net_lev_val) else None
            net_lev_adj_val = pd.to_numeric(lev_row.get("corporate_net_leverage_adj"), errors="coerce")
            net_lev_adj_val = float(net_lev_adj_val) if pd.notna(net_lev_adj_val) else None
            cov_pnl_val = pd.to_numeric(lev_row.get("interest_coverage_pnl"), errors="coerce")
            cov_pnl_val = float(cov_pnl_val) if pd.notna(cov_pnl_val) else None
            cov_cash_val = pd.to_numeric(lev_row.get("interest_coverage_cash"), errors="coerce")
            cov_cash_val = float(cov_cash_val) if pd.notna(cov_cash_val) else None
            if gaap_ebitda_ttm_val is not None and gaap_ebitda_ttm_val <= 0:
                net_lev_val = None
                cov_pnl_val = None
                cov_cash_val = None
            if adj_ebitda_ttm_val is not None and adj_ebitda_ttm_val <= 0:
                net_lev_adj_val = None
            net_lev_suppress_reason = (
                ""
                if net_lev_val is not None
                else ("EBITDA denominator <= 0" if gaap_ebitda_ttm_val is not None and gaap_ebitda_ttm_val <= 0 else str(lev_row.get("corporate_net_leverage_basis") or "missing GAAP EBITDA TTM"))
            )
            net_lev_adj_suppress_reason = (
                ""
                if net_lev_adj_val is not None
                else ("Adjusted EBITDA denominator <= 0" if adj_ebitda_ttm_val is not None and adj_ebitda_ttm_val <= 0 else str(lev_row.get("corporate_net_leverage_adj_basis") or "missing adjusted EBITDA TTM"))
            )
            cov_pnl_suppress_reason = (
                ""
                if cov_pnl_val is not None
                else ("EBITDA denominator <= 0" if gaap_ebitda_ttm_val is not None and gaap_ebitda_ttm_val <= 0 else str(lev_row.get("interest_coverage_pnl_basis") or "missing GAAP EBITDA or P&L interest"))
            )
            cov_cash_suppress_reason = (
                ""
                if cov_cash_val is not None
                else ("EBITDA denominator <= 0" if gaap_ebitda_ttm_val is not None and gaap_ebitda_ttm_val <= 0 else str(lev_row.get("interest_coverage_cash_basis") or "missing GAAP EBITDA or cash interest"))
            )
            net_debt_val = (
                float(debt_core_val) - float(cash_val)
                if debt_core_val is not None and cash_val is not None and pd.notna(debt_core_val) and pd.notna(cash_val)
                else None
            )
            valuation_audit[q] = {
                "buyback_cash": {
                    "value": buyback_map.get(q),
                    "upstream": "sec_doc_execution" if buyback_map.get(q) is not None else "",
                    "scope": "quarter-safe execution" if buyback_map.get(q) is not None else "",
                    "kind": "cash",
                    "basis": "quarter",
                    "suppress_reason": "" if buyback_map.get(q) is not None else ("context/program text blocked for execution metrics" if buyback_text_map.get(q) is not None else "no explicit quarter-safe execution"),
                },
                "buyback_shares": {
                    "value": buyback_shares_map.get(q),
                    "upstream": "sec_doc_execution" if buyback_shares_map.get(q) is not None else "",
                    "scope": "quarter-safe execution" if buyback_shares_map.get(q) is not None else "",
                    "kind": "shares",
                    "basis": "quarter",
                    "suppress_reason": "" if buyback_shares_map.get(q) is not None else ("derived share delta blocked for execution metrics" if buyback_shares_q_map.get(q) is not None else ("context/program text blocked for execution metrics" if buyback_shares_text_map.get(q) is not None else "no explicit quarter-safe execution")),
                },
                "dividend_cash": {
                    "value": dividend_map.get(q),
                    "upstream": "history_q_common_dividends_cash" if dividend_cash_facts_map.get(q) is not None else ("sec_doc_common_dividend" if dividend_doc_map.get(q) is not None else ""),
                    "scope": "explicit common-stock support" if dividend_map.get(q) is not None else "",
                    "kind": "cash",
                    "basis": "quarter",
                    "suppress_reason": "" if dividend_map.get(q) is not None else ("generic dividends/distributions blocked for common dividend logic" if dividend_text_map.get(q) is not None else "no explicit common-stock dividend support"),
                },
                "adj_ebit": {
                    "value": adj_ebit_q.get(q),
                    "upstream": "adj_metrics.explicit_or_merged" if adj_ebit_q.get(q) is not None else "",
                    "scope": "quarter adjusted metric" if adj_ebit_q.get(q) is not None else "",
                    "kind": "adjusted",
                    "basis": "quarter",
                    "suppress_reason": "" if adj_ebit_q.get(q) is not None else "no explicit or auditable adjusted EBIT support",
                },
                "adj_ebitda": {
                    "value": adj_ebitda_q.get(q),
                    "upstream": "adj_metrics.explicit_or_merged" if adj_ebitda_q.get(q) is not None else "",
                    "scope": "quarter adjusted metric" if adj_ebitda_q.get(q) is not None else "",
                    "kind": "adjusted",
                    "basis": "quarter",
                    "suppress_reason": "" if adj_ebitda_q.get(q) is not None else "no explicit or auditable adjusted EBITDA support",
                },
                "adj_ebit_ttm": {
                    "value": adj_ebit_ttm_q.get(q),
                    "upstream": "adj_metrics.explicit_or_merged" if adj_ebit_ttm_q.get(q) is not None else "",
                    "scope": "ttm adjusted metric" if adj_ebit_ttm_q.get(q) is not None else "",
                    "kind": "adjusted",
                    "basis": "ttm",
                    "suppress_reason": "" if adj_ebit_ttm_q.get(q) is not None else "no four-quarter adjusted EBIT series",
                },
                "adj_ebitda_ttm": {
                    "value": adj_ebitda_ttm_q.get(q),
                    "upstream": "adj_metrics.explicit_or_merged" if adj_ebitda_ttm_q.get(q) is not None else "",
                    "scope": "ttm adjusted metric" if adj_ebitda_ttm_q.get(q) is not None else "",
                    "kind": "adjusted",
                    "basis": "ttm",
                    "suppress_reason": "" if adj_ebitda_ttm_q.get(q) is not None else "no four-quarter adjusted EBITDA series",
                },
                "debt_core": {
                    "value": debt_core_val,
                    "upstream": "history_q.debt_core" if debt_core_val is not None else "",
                    "scope": "debt core",
                    "kind": "gaap",
                    "basis": "quarter",
                    "suppress_reason": "" if debt_core_val is not None else "missing debt_core",
                },
                "net_debt": {
                    "value": net_debt_val,
                    "upstream": "history_q.debt_core_minus_cash" if net_debt_val is not None else "",
                    "scope": "derived from debt_core and cash",
                    "kind": "derived",
                    "basis": "quarter",
                    "suppress_reason": "" if net_debt_val is not None else "missing debt_core and/or cash",
                },
                "net_leverage": {
                    "value": net_lev_val,
                    "upstream": "leverage_df.corporate_net_leverage" if net_lev_val is not None else "",
                    "scope": str(lev_row.get("corporate_net_leverage_basis") or ""),
                    "kind": "gaap",
                    "basis": "ttm",
                    "suppress_reason": net_lev_suppress_reason,
                },
                "net_leverage_adj": {
                    "value": net_lev_adj_val,
                    "upstream": "leverage_df.corporate_net_leverage_adj" if net_lev_adj_val is not None else "",
                    "scope": str(lev_row.get("corporate_net_leverage_adj_basis") or ""),
                    "kind": "adjusted",
                    "basis": "ttm",
                    "suppress_reason": net_lev_adj_suppress_reason,
                },
                "interest_coverage_pnl": {
                    "value": cov_pnl_val,
                    "upstream": "leverage_df.interest_coverage_pnl" if cov_pnl_val is not None else "",
                    "scope": str(lev_row.get("interest_coverage_pnl_basis") or ""),
                    "kind": "gaap",
                    "basis": "ttm",
                    "suppress_reason": cov_pnl_suppress_reason,
                },
                "cash_interest_coverage": {
                    "value": cov_cash_val,
                    "upstream": "leverage_df.interest_coverage_cash" if cov_cash_val is not None else "",
                    "scope": str(lev_row.get("interest_coverage_cash_basis") or ""),
                    "kind": "gaap",
                    "basis": "ttm",
                    "suppress_reason": cov_cash_suppress_reason,
                },
                "gaap_ebitda_ttm": {
                    "value": gaap_ebitda_ttm_val,
                    "upstream": "leverage_df.ebitda_ttm" if gaap_ebitda_ttm_val is not None else "",
                    "scope": "gaap ebitda",
                    "kind": "gaap",
                    "basis": "ttm",
                    "suppress_reason": "" if gaap_ebitda_ttm_val is not None else "no four-quarter GAAP EBITDA series",
                },
            }

        self.valuation_precompute_bundle_cache = {
            "quarter_key": quarter_key,
            "buyback_map": buyback_map,
            "dividend_map": dividend_map,
            "buyback_cash_facts_map": buyback_cash_facts_map,
            "dividend_cash_facts_map": dividend_cash_facts_map,
            "buyback_shares_q_map": buyback_shares_q_map,
            "buyback_text_map": buyback_text_map,
            "buyback_shares_text_map": buyback_shares_text_map,
            "dividend_text_map": dividend_text_map,
            "buyback_cash_doc_map": buyback_cash_doc_map,
            "dividend_doc_map": dividend_doc_map,
            "buyback_doc_note_map": buyback_doc_note_map,
            "dividend_doc_note_map": dividend_doc_note_map,
            "dividend_ps_doc_map": dividend_ps_doc_map,
            "buyback_shares_doc_map": buyback_shares_doc_map,
            "buyback_avg_price_doc_map": buyback_avg_price_doc_map,
            "buyback_shares_map": buyback_shares_map,
            "buyback_ttm_resolved_map": buyback_ttm_resolved_map,
            "dividend_ttm_resolved_map": dividend_ttm_resolved_map,
            "buyback_shares_ttm_resolved_map": buyback_shares_ttm_resolved_map,
            "capital_return_resolved": capital_return_resolved,
            "docs_by_quarter": docs_by_quarter,
            "valuation_audit": valuation_audit,
        }
        if ctx_ref is not None:
            ctx_ref.derived.valuation_precompute_bundle = self.valuation_precompute_bundle_cache
        return self.valuation_precompute_bundle_cache
