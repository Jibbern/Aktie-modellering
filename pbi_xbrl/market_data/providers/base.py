"""Shared provider utilities for discovery, downloads, and text extraction.

Concrete providers inherit these helpers so they can focus on source-specific
URL patterns and table parsing while the common HTTP, PDF, and HTML handling
stays in one place.
"""
from __future__ import annotations

import html
import json
import re
import shutil
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from ..cache import file_fingerprint, raw_source_dir


class BaseMarketProvider:
    source = ""
    provider_parse_version = "v1"
    local_patterns: tuple[str, ...] = tuple()
    landing_page_url = ""
    report_token = ""
    stable_name_prefix = ""
    local_dir_name = ""
    remote_timeout_seconds = 20
    remote_user_agent = "Mozilla/5.0 (compatible; Codex market-data refresh)"
    data_suffixes = {".csv", ".xlsx", ".xls", ".zip", ".txt", ".json"}
    _anchor_re = re.compile(r"<a\b[^>]*href=[\"'](?P<href>[^\"']+)[\"'][^>]*>(?P<label>.*?)</a>", re.I | re.S)
    _ajax_release_re = re.compile(r"/get_(?:latest|previous)_release/\d+", re.I)
    _slug_id_re = re.compile(r"const\s+slugId\s*=\s*(?P<slug>\d+)\s*;", re.I)

    def _today(self) -> date:
        return date.today()

    def _local_dir(self, ticker_root: Path) -> Path:
        # Providers write newly fetched assets into ticker-local working folders first.
        # The later `sync_raw()` step copies those files into `sec_cache/market_data/raw`,
        # which remains the canonical raw cache consumed by reparsing/export rebuilds.
        local_name = str(self.local_dir_name or f"{self.source}_pdfs").strip()
        out = ticker_root / local_name
        out.mkdir(parents=True, exist_ok=True)
        return out

    def _is_direct_asset_url(self, url: str) -> bool:
        suffix = Path(urllib.parse.urlparse(str(url or "")).path).suffix.lower()
        return suffix == ".pdf" or suffix in self.data_suffixes

    def _asset_type_for_name(self, name: str) -> str:
        return "pdf" if Path(str(name or "")).suffix.lower() == ".pdf" else "data"

    def _looks_like_documents_page(self, url: str, label: str) -> bool:
        url_low = str(url or "").lower()
        label_low = re.sub(r"\s+", " ", html.unescape(str(label or ""))).strip().lower()
        if "filerepo/reports" in url_low:
            return True
        return any(token in label_low for token in ("report documents", "latest releases", "easier access", "documents"))

    def _extract_links(self, html_text: str, base_url: str) -> List[Dict[str, str]]:
        out: List[Dict[str, str]] = []
        for match in self._anchor_re.finditer(str(html_text or "")):
            href = html.unescape(str(match.group("href") or "").strip())
            if not href:
                continue
            label_html = str(match.group("label") or "")
            label = re.sub(r"<[^>]+>", " ", html.unescape(label_html))
            label = re.sub(r"\s+", " ", label).strip()
            abs_url = urllib.parse.urljoin(base_url, href)
            out.append({"url": abs_url, "label": label})
        return out

    def _fetch_bytes(self, url: str, *, extra_headers: Optional[Dict[str, str]] = None) -> bytes:
        headers = {
            "User-Agent": self.remote_user_agent,
            "Accept": "text/html,application/pdf,text/csv,application/octet-stream,*/*",
        }
        if extra_headers:
            headers.update({str(k): str(v) for k, v in extra_headers.items() if str(k or "").strip()})
        req = urllib.request.Request(
            url,
            headers=headers,
        )
        with urllib.request.urlopen(req, timeout=float(self.remote_timeout_seconds)) as resp:
            return resp.read()

    def _fetch_text(self, url: str, *, extra_headers: Optional[Dict[str, str]] = None) -> str:
        blob = self._fetch_bytes(url, extra_headers=extra_headers)
        try:
            return blob.decode("utf-8")
        except UnicodeDecodeError:
            return blob.decode("latin-1", errors="ignore")

    def _release_fragment_urls(self, landing_html: str, landing_url: str) -> List[str]:
        # USDA's current Drupal report pages no longer expose the latest/previous
        # documents as static anchors in the landing HTML. Instead they bootstrap a
        # `slugId` and fetch release fragments from `/get_latest_release/<id>` and
        # `/get_previous_release/<id>`. We accept either explicit fragment URLs in the
        # page source or reconstruct them from the inline `slugId`.
        urls: List[str] = []
        for match in self._ajax_release_re.finditer(str(landing_html or "")):
            abs_url = urllib.parse.urljoin(landing_url, str(match.group(0) or ""))
            if abs_url and abs_url not in urls:
                urls.append(abs_url)
        if urls:
            return urls
        slug_match = self._slug_id_re.search(str(landing_html or ""))
        if not slug_match:
            return []
        slug_id = str(slug_match.group("slug") or "").strip()
        if not slug_id:
            return []
        for endpoint in (f"/get_latest_release/{slug_id}", f"/get_previous_release/{slug_id}"):
            abs_url = urllib.parse.urljoin(landing_url, endpoint)
            if abs_url not in urls:
                urls.append(abs_url)
        return urls

    def _fragment_html_from_payload(self, payload_text: str) -> str:
        # Release fragments sometimes arrive as raw HTML and sometimes as a small JSON
        # object with an `html` field. The discovery layer normalizes both shapes so
        # source-specific providers can keep working with plain link extraction.
        txt = str(payload_text or "").strip()
        if not txt:
            return ""
        try:
            parsed = json.loads(txt)
        except Exception:
            return txt
        if isinstance(parsed, dict):
            html_blob = str(parsed.get("html") or "").strip()
            if html_blob:
                return html_blob
        return txt

    def _quarter_bounds(self, as_of: Optional[date] = None) -> tuple[date, date]:
        ref = as_of or self._today()
        start_month = ((int(ref.month) - 1) // 3) * 3 + 1
        q_start = date(int(ref.year), start_month, 1)
        if start_month == 10:
            q_end = date(int(ref.year), 12, 31)
        else:
            next_q_start = date(int(ref.year), start_month + 3, 1)
            q_end = next_q_start - timedelta(days=1)
        return q_start, q_end

    def _remote_date_from_text(self, *values: Any) -> Optional[pd.Timestamp]:
        token = " ".join(str(v or "") for v in values if str(v or "").strip())
        if not token:
            return None
        m = re.search(r"(20\d{2})[-_/](\d{2})[-_/](\d{2})", token)
        if m:
            try:
                return pd.Timestamp(year=int(m.group(1)), month=int(m.group(2)), day=int(m.group(3)))
            except Exception:
                return None
        return self._date_from_value(token)

    def _looks_like_source_asset(self, url: str) -> bool:
        url_low = str(url or "").lower()
        if not self._is_direct_asset_url(url_low):
            return False
        token = str(self.report_token or "").strip().lower()
        if token and token in url_low:
            return True
        return str(self.source or "").strip().lower() in url_low

    def _stable_local_name(self, report_date: pd.Timestamp, asset_type: str, source_url: str) -> str:
        suffix = Path(urllib.parse.urlparse(str(source_url or "")).path).suffix.lower() or (".pdf" if asset_type == "pdf" else ".bin")
        stem = f"{str(self.stable_name_prefix or self.source).strip()}_{report_date.date().isoformat()}"
        if asset_type != "pdf":
            stem += "_data"
        return f"{stem}{suffix}"

    def discover_remote_assets(self, as_of: Optional[date] = None) -> List[Dict[str, Any]]:
        landing_url = str(self.landing_page_url or "").strip()
        if not landing_url:
            return []
        try:
            landing_html = self._fetch_text(landing_url)
        except Exception as exc:
            print(f"[market_data:{self.source}] remote_discovery_failed url={landing_url} error={type(exc).__name__}: {exc}", flush=True)
            return []
        pages_to_scan = [landing_url]
        seen_pages = {landing_url}
        direct_candidates: List[Dict[str, Any]] = []
        for link in self._extract_links(landing_html, landing_url):
            url = str(link.get("url") or "")
            label = str(link.get("label") or "")
            if self._looks_like_source_asset(url):
                direct_candidates.append(
                    {
                        "url": url,
                        "label": label,
                        "asset_type": self._asset_type_for_name(url),
                        "report_date": self._remote_date_from_text(url, label),
                    }
                )
            elif self._looks_like_documents_page(url, label) and url not in seen_pages:
                pages_to_scan.append(url)
                seen_pages.add(url)
        # Current USDA pages expose the freshest downloadable assets through AJAX
        # fragments. Those fragments are now part of normal discovery so live refreshes
        # continue to work even when the landing page itself contains no direct file
        # links. This path is intentionally only for "latest refresh" discovery; the
        # deeper month-by-month archive walk lives in `Code/usda_backfill.py` so
        # normal refreshes stay cheap and predictable.
        for fragment_url in self._release_fragment_urls(landing_html, landing_url):
            try:
                fragment_payload = self._fetch_text(
                    fragment_url,
                    extra_headers={
                        "Accept": "application/json,text/html,*/*",
                        "X-Requested-With": "XMLHttpRequest",
                        "Referer": landing_url,
                    },
                )
            except Exception as exc:
                print(f"[market_data:{self.source}] ajax_release_failed url={fragment_url} error={type(exc).__name__}: {exc}", flush=True)
                continue
            fragment_html = self._fragment_html_from_payload(fragment_payload)
            for link in self._extract_links(fragment_html, fragment_url):
                url = str(link.get("url") or "")
                label = str(link.get("label") or "")
                if not self._looks_like_source_asset(url):
                    continue
                direct_candidates.append(
                    {
                        "url": url,
                        "label": label,
                        "asset_type": self._asset_type_for_name(url),
                        "report_date": self._remote_date_from_text(url, label, fragment_url),
                    }
                )
        for page_url in pages_to_scan[1:]:
            try:
                page_html = self._fetch_text(page_url)
            except Exception as exc:
                print(f"[market_data:{self.source}] documents_page_failed url={page_url} error={type(exc).__name__}: {exc}", flush=True)
                continue
            for link in self._extract_links(page_html, page_url):
                url = str(link.get("url") or "")
                label = str(link.get("label") or "")
                if not self._looks_like_source_asset(url):
                    continue
                direct_candidates.append(
                    {
                        "url": url,
                        "label": label,
                        "asset_type": self._asset_type_for_name(url),
                        "report_date": self._remote_date_from_text(url, label, page_url),
                    }
                )
        deduped: Dict[tuple[str, str], Dict[str, Any]] = {}
        for cand in direct_candidates:
            url = str(cand.get("url") or "").strip()
            asset_type = str(cand.get("asset_type") or "").strip()
            if not url or not asset_type:
                continue
            deduped[(url, asset_type)] = cand
        candidates = list(deduped.values())
        if not candidates:
            return []
        q_start, q_end = self._quarter_bounds(as_of=as_of)
        current_q = [
            cand for cand in candidates
            if isinstance(cand.get("report_date"), pd.Timestamp)
            and q_start <= cand["report_date"].date() <= q_end
        ]
        if current_q:
            return sorted(
                current_q,
                key=lambda item: (
                    item["report_date"],
                    0 if str(item.get("asset_type") or "") == "pdf" else 1,
                    str(item.get("url") or ""),
                ),
            )
        latest_by_type: Dict[str, Dict[str, Any]] = {}
        for cand in sorted(
            candidates,
            key=lambda item: (
                item.get("report_date") or pd.Timestamp("1900-01-01"),
                str(item.get("url") or ""),
            ),
            reverse=True,
        ):
            asset_type = str(cand.get("asset_type") or "").strip()
            latest_by_type.setdefault(asset_type, cand)
        return list(latest_by_type.values())

    def discover_available(self, ticker_root: Path, refresh: bool = False) -> List[Dict[str, Any]]:
        if refresh:
            local_dir = self._local_dir(ticker_root)
            for cand in self.discover_remote_assets(as_of=self._today()):
                url = str(cand.get("url") or "").strip()
                report_date = self._date_from_value(cand.get("report_date"))
                asset_type = str(cand.get("asset_type") or "").strip() or self._asset_type_for_name(url)
                if not url or report_date is None:
                    continue
                local_name = self._stable_local_name(report_date, asset_type, url)
                local_path = local_dir / local_name
                if local_path.exists():
                    print(
                        f"[market_data:{self.source}] asset={asset_type} date={report_date.date().isoformat()} status=skipped path={local_path}",
                        flush=True,
                    )
                    continue
                try:
                    payload = self._fetch_bytes(url)
                    local_path.write_bytes(payload)
                    print(
                        f"[market_data:{self.source}] asset={asset_type} date={report_date.date().isoformat()} status=updated path={local_path}",
                        flush=True,
                    )
                except Exception as exc:
                    print(
                        f"[market_data:{self.source}] asset={asset_type} date={report_date.date().isoformat()} status=failed path={local_path} error={type(exc).__name__}: {exc}",
                        flush=True,
                    )
        out: List[Dict[str, Any]] = []
        seen: set[Path] = set()
        for pattern in self.local_patterns:
            for path in sorted(ticker_root.glob(pattern)):
                if not path.is_file():
                    continue
                resolved = path.resolve()
                if resolved in seen:
                    continue
                seen.add(resolved)
                report_date = self._date_from_name(path)
                out.append(
                    {
                        "source": self.source,
                        "source_id": path.stem,
                        "report_date": report_date.isoformat() if report_date is not None else "",
                        "publication_date": report_date.isoformat() if report_date is not None else "",
                        "path": resolved,
                        "asset_type": self._asset_type_for_name(path.name),
                    }
                )
        return out

    def sync_raw(self, cache_root: Path, ticker_root: Path, refresh: bool = False) -> Dict[str, Any]:
        # `sync_raw()` is the handoff from ticker-local working folders into the shared
        # raw cache under `sec_cache/market_data/raw/<source>/<year>/`. Once files are in
        # raw cache, later rebuilds can reparse/export without re-downloading from USDA.
        discovered = self.discover_available(ticker_root, refresh=refresh)
        entries: List[Dict[str, Any]] = []
        added = 0
        refreshed = 0
        skipped = 0
        for item in discovered:
            src = Path(item.get("path") or "")
            if not src.exists():
                continue
            report_date = self._date_from_value(item.get("report_date"))
            year = int(report_date.year) if report_date is not None else int(pd.Timestamp.now().year)
            dst_dir = raw_source_dir(cache_root, self.source, year)
            dst = dst_dir / src.name
            src_fp = file_fingerprint(src)
            dst_fp = file_fingerprint(dst) if dst.exists() else ""
            if not dst.exists():
                shutil.copy2(src, dst)
                added += 1
            elif src_fp and src_fp != dst_fp:
                shutil.copy2(src, dst)
                refreshed += 1
            else:
                skipped += 1
            dst_fp = file_fingerprint(dst)
            st = dst.stat()
            entries.append(
                {
                    "source": self.source,
                    "source_id": str(item.get("source_id") or src.stem),
                    "report_date": str(item.get("report_date") or ""),
                    "publication_date": str(item.get("publication_date") or item.get("report_date") or ""),
                    "local_path": str(dst),
                    "size": int(st.st_size),
                    "checksum": dst_fp,
                    "download_status": "cached",
                    "asset_type": str(item.get("asset_type") or self._asset_type_for_name(src.name)),
                }
            )
        return {
            "entries": entries,
            "raw_added": added,
            "raw_refreshed": refreshed,
            "raw_skipped": skipped,
        }

    def parse_raw_to_rows(self, cache_root: Path, ticker_root: Path, raw_entries: List[Dict[str, Any]]) -> pd.DataFrame:
        del cache_root, ticker_root, raw_entries
        return pd.DataFrame()

    @staticmethod
    def _date_from_name(path: Path) -> Optional[pd.Timestamp]:
        m = re.search(r"(20\d{2})[-_](\d{2})[-_](\d{2})", path.name)
        if not m:
            return None
        try:
            return pd.Timestamp(year=int(m.group(1)), month=int(m.group(2)), day=int(m.group(3)))
        except Exception:
            return None

    @staticmethod
    def _date_from_value(value: Any) -> Optional[pd.Timestamp]:
        if value is None or value == "":
            return None
        ts = pd.to_datetime(value, errors="coerce")
        if pd.isna(ts):
            return None
        return ts
