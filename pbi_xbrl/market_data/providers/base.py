"""Shared provider utilities for discovery, downloads, and text extraction.

Concrete providers inherit these helpers so they can focus on source-specific
URL patterns and table parsing while the common HTTP, PDF, and HTML handling
stays in one place.
"""
from __future__ import annotations

import base64
import html
import json
import re
import shutil
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from ..cache import file_fingerprint, raw_source_dir, remote_debug_path


class RemoteFetchError(RuntimeError):
    def __init__(self, url: str, attempts: List[Dict[str, Any]], final_exc: Exception):
        self.url = str(url or "")
        self.attempts = list(attempts or [])
        self.final_exc = final_exc
        super().__init__(f"remote fetch failed for {self.url}: {type(final_exc).__name__}: {final_exc}")


class BaseMarketProvider:
    source = ""
    provider_parse_version = "v1"
    local_patterns: tuple[str, ...] = tuple()
    landing_page_url = ""
    public_data_url = ""
    public_data_slug_id = ""
    public_data_sections: tuple[str, ...] = ("Report Detail",)
    report_token = ""
    stable_name_prefix = ""
    local_dir_name = ""
    remote_timeout_seconds = 45
    remote_retry_attempts = 3
    remote_backoff_seconds = (1.0, 2.0, 4.0)
    remote_user_agent = "Mozilla/5.0 (compatible; Codex market-data refresh)"
    data_suffixes = {".csv", ".xlsx", ".xls", ".zip", ".txt", ".json"}
    _anchor_re = re.compile(r"<a\b[^>]*href=[\"'](?P<href>[^\"']+)[\"'][^>]*>(?P<label>.*?)</a>", re.I | re.S)
    _ajax_release_re = re.compile(r"/get_(?:latest|previous)_release/\d+", re.I)
    _slug_id_re = re.compile(
        r"(?:const\s+slugId\s*=\s*|slug\s*id\s*:?\s*)(?P<slug>\d+)\b",
        re.I,
    )

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

    def _default_request_headers(self, extra_headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        headers = {
            "User-Agent": self.remote_user_agent,
            "Accept": "text/html,application/pdf,text/csv,application/octet-stream,*/*",
            "Accept-Encoding": "identity",
            "Connection": "close",
        }
        if extra_headers:
            headers.update({str(k): str(v) for k, v in extra_headers.items() if str(k or "").strip()})
        return headers

    def _classify_remote_error(self, exc: Exception) -> str:
        if isinstance(exc, urllib.error.HTTPError):
            if int(getattr(exc, "code", 0) or 0) == 403:
                return "http_forbidden"
            if int(getattr(exc, "code", 0) or 0) == 404:
                return "http_not_found"
        err_txt = f"{type(exc).__name__}: {exc}"
        err_low = err_txt.lower()
        if isinstance(exc, TimeoutError) or "timed out" in err_low:
            return "network_timeout"
        if isinstance(exc, urllib.error.URLError):
            reason_txt = str(getattr(exc, "reason", "") or "")
            reason_low = reason_txt.lower()
            if isinstance(getattr(exc, "reason", None), TimeoutError) or "timed out" in reason_low:
                return "network_timeout"
            if "10013" in reason_low or "access permissions" in reason_low or "åtkomstbehörigheterna" in reason_low:
                return "environment_blocked"
        if "10013" in err_low or "access permissions" in err_low or "åtkomstbehörigheterna" in err_low:
            return "environment_blocked"
        return "fetch_error"

    def _fetch_bytes_diagnostic(
        self,
        url: str,
        *,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> tuple[bytes, List[Dict[str, Any]]]:
        attempts: List[Dict[str, Any]] = []
        max_attempts = max(int(self.remote_retry_attempts or 1), 1)
        backoff_schedule = tuple(float(x) for x in (self.remote_backoff_seconds or ()))
        for attempt_no in range(1, max_attempts + 1):
            attempt_entry: Dict[str, Any] = {
                "attempt": attempt_no,
                "url": str(url or ""),
                "status": "pending",
            }
            try:
                req = urllib.request.Request(
                    url,
                    headers=self._default_request_headers(extra_headers=extra_headers),
                )
                with urllib.request.urlopen(req, timeout=float(self.remote_timeout_seconds)) as resp:
                    payload = resp.read()
                    attempt_entry["status"] = "ok"
                    attempt_entry["http_status"] = int(getattr(resp, "status", 200) or 200)
                    attempt_entry["bytes"] = len(payload)
                    attempts.append(attempt_entry)
                    return payload, attempts
            except Exception as exc:
                attempt_entry["status"] = "error"
                attempt_entry["error"] = f"{type(exc).__name__}: {exc}"
                attempt_entry["classification"] = self._classify_remote_error(exc)
                if isinstance(exc, urllib.error.HTTPError):
                    attempt_entry["http_status"] = int(getattr(exc, "code", 0) or 0)
                attempts.append(attempt_entry)
                if attempt_no >= max_attempts:
                    raise RemoteFetchError(str(url or ""), attempts, exc) from exc
                backoff_seconds = backoff_schedule[min(attempt_no - 1, len(backoff_schedule) - 1)] if backoff_schedule else 0.0
                attempt_entry["retry_in_seconds"] = float(backoff_seconds)
                if backoff_seconds > 0:
                    time.sleep(backoff_seconds)
        raise RuntimeError(f"unexpected remote fetch state for {url}")

    def _fetch_text_diagnostic(
        self,
        url: str,
        *,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> tuple[str, List[Dict[str, Any]]]:
        blob, attempts = self._fetch_bytes_diagnostic(url, extra_headers=extra_headers)
        try:
            return blob.decode("utf-8"), attempts
        except UnicodeDecodeError:
            return blob.decode("latin-1", errors="ignore"), attempts

    def _asset_type_for_name(self, name: str) -> str:
        suffix = Path(str(name or "")).suffix.lower()
        if suffix == ".pdf":
            return "pdf"
        if suffix == ".json":
            return "json"
        return "data"

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
        payload, _ = self._fetch_bytes_diagnostic(url, extra_headers=extra_headers)
        return payload

    def _fetch_text(self, url: str, *, extra_headers: Optional[Dict[str, str]] = None) -> str:
        text, _ = self._fetch_text_diagnostic(url, extra_headers=extra_headers)
        return text

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

    @staticmethod
    def _debug_json_ready(value: Any) -> Any:
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, bytes):
            return f"<bytes:{len(value)}>"
        if isinstance(value, dict):
            return {str(k): BaseMarketProvider._debug_json_ready(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [BaseMarketProvider._debug_json_ready(v) for v in value]
        return value

    def _sanitize_candidate_debug(self, candidate: Dict[str, Any]) -> Dict[str, Any]:
        payload = {
            "url": str(candidate.get("url") or ""),
            "label": str(candidate.get("label") or ""),
            "asset_type": str(candidate.get("asset_type") or ""),
            "report_date": self._date_from_value(candidate.get("report_date")),
        }
        return self._debug_json_ready(payload)

    def _load_remote_debug(self, cache_root: Optional[Path]) -> Dict[str, Any]:
        if not isinstance(cache_root, Path):
            return {}
        path = remote_debug_path(cache_root, self.source)
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    def _write_remote_debug(self, cache_root: Optional[Path], payload: Dict[str, Any], *, merge: bool = False) -> Optional[Path]:
        if not isinstance(cache_root, Path):
            return None
        path = remote_debug_path(cache_root, self.source)
        final_payload = dict(payload)
        if merge:
            existing = self._load_remote_debug(cache_root)
            existing.update(final_payload)
            final_payload = existing
        final_payload.setdefault("source", str(self.source or ""))
        try:
            path.write_text(
                json.dumps(self._debug_json_ready(final_payload), ensure_ascii=True, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            return path
        except Exception:
            return None

    def _record_parse_debug(self, cache_root: Optional[Path], raw_entries: List[Dict[str, Any]], out_rows: List[Dict[str, Any]]) -> None:
        rows_by_file: Dict[str, int] = {}
        for row in out_rows:
            source_file = str(row.get("source_file") or "").strip()
            if source_file:
                rows_by_file[source_file] = rows_by_file.get(source_file, 0) + 1
        parse_payload = {
            "latest_parse": {
                "input_entries": len(list(raw_entries or [])),
                "parsed_rows": len(list(out_rows or [])),
                "status": "success" if len(list(out_rows or [])) > 0 else "no_rows",
                "rows_by_source_file": [
                    {"source_file": key, "parsed_rows": rows_by_file[key]}
                    for key in sorted(rows_by_file)
                ],
            }
        }
        self._write_remote_debug(cache_root, parse_payload, merge=True)

    def _select_remote_candidates(self, candidates: List[Dict[str, Any]], *, as_of: Optional[date] = None) -> List[Dict[str, Any]]:
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
        suffix = Path(urllib.parse.urlparse(str(source_url or "")).path).suffix.lower()
        if not suffix:
            suffix = ".pdf" if asset_type == "pdf" else ".json" if asset_type == "json" else ".bin"
        stem = f"{str(self.stable_name_prefix or self.source).strip()}_{report_date.date().isoformat()}"
        if asset_type != "pdf":
            stem += "_data"
        return f"{stem}{suffix}"

    def _public_data_slug(self) -> str:
        explicit = str(self.public_data_slug_id or "").strip()
        if explicit:
            return explicit
        token = str(self.report_token or "").strip()
        match = re.search(r"\d+", token)
        return str(match.group(0) or "").strip() if match else ""

    def _public_data_base_url(self) -> str:
        configured = str(self.public_data_url or "").strip()
        if configured:
            return configured
        slug = self._public_data_slug()
        if not slug:
            return ""
        return f"https://mymarketnews.ams.usda.gov/public_data?slug_id={slug}"

    def _public_data_filter_url(self, slug: str) -> str:
        return urllib.parse.urljoin(
            self._public_data_base_url() or "https://mymarketnews.ams.usda.gov/",
            f"/public_data/ajax-get-conditions-by-report/{slug}",
        )

    def _public_data_search_url(self, slug: str, section: str, begin_date: date, end_date: date) -> str:
        section_b64 = base64.b64encode(str(section or "").encode("utf-8")).decode("ascii")
        q = (
            f"report_begin_date={begin_date.strftime('%m/%d/%Y')}:"
            f"{end_date.strftime('%m/%d/%Y')}"
        )
        return urllib.parse.urljoin(
            self._public_data_base_url() or "https://mymarketnews.ams.usda.gov/",
            f"/public_data/ajax-search-data-by-report-section/{slug}/{section_b64}?q={urllib.parse.quote(q, safe='=:/;')}",
        )

    def _public_data_date_pairs(self, payload: Dict[str, Any], *, as_of: Optional[date]) -> List[tuple[date, date]]:
        # `public_data` is now the first-choice USDA latest-refresh path. The filter
        # endpoint exposes every report begin/end pair, so we explicitly keep the
        # newest current-quarter pair not after `as_of`; this prevents a browser-visible
        # future report date from being pulled into a workbook build before the run date.
        begin_values = list(payload.get("reportBeginDates") or [])
        end_values = list(payload.get("reportEndDates") or [])
        pairs: List[tuple[date, date]] = []
        for idx, begin_value in enumerate(begin_values):
            begin_ts = self._date_from_value(begin_value)
            if begin_ts is None:
                continue
            end_ts = self._date_from_value(end_values[idx] if idx < len(end_values) else begin_value)
            if end_ts is None:
                end_ts = begin_ts
            begin = begin_ts.date()
            end = end_ts.date()
            if as_of is not None and end > as_of:
                continue
            pairs.append((begin, end))
        if not pairs:
            return []
        q_start, _q_end = self._quarter_bounds(as_of=as_of)
        current_q = [(begin, end) for begin, end in pairs if end >= q_start]
        selected_pool = current_q or pairs
        # Normal refresh only needs the freshest report; archive/backfill code remains
        # responsible for deliberate multi-period history downloads.
        return [max(selected_pool, key=lambda item: item[1])]

    def _discover_public_data_assets(self, *, as_of: Optional[date], cache_root: Optional[Path]) -> List[Dict[str, Any]]:
        # Priority 1: structured USDA public_data JSON. It is more stable than the
        # viewReport landing-page HTML because the app's filter/search endpoints return
        # normalized rows directly. If this returns no candidates, `discover_remote_assets`
        # falls through to the older release-fragment/PDF discovery below.
        slug = self._public_data_slug()
        public_url = self._public_data_base_url()
        if not slug or not public_url:
            return []
        debug_payload: Dict[str, Any] = {
            "source": str(self.source or ""),
            "latest_refresh": {
                "as_of": as_of,
                "public_data_url": public_url,
                "public_data_slug_id": slug,
                "public_data_filter_fetch": {"status": "pending"},
                "public_data_search_fetches": [],
                "selected_candidates": [],
                "chosen_url": "",
                "final_classification": "pending",
            },
        }
        filter_url = self._public_data_filter_url(slug)
        try:
            filter_text, filter_attempts = self._fetch_text_diagnostic(
                filter_url,
                extra_headers={
                    "Accept": "application/json,*/*",
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": public_url,
                },
            )
            filter_payload = json.loads(filter_text)
            if not isinstance(filter_payload, dict):
                filter_payload = {}
        except RemoteFetchError as exc:
            failure_class = str(self._classify_remote_error(exc.final_exc) or "public_data_filter_failure")
            debug_payload["latest_refresh"]["public_data_filter_fetch"] = {
                "status": "error",
                "classification": failure_class,
                "attempts": list(exc.attempts or []),
                "error": f"{type(exc.final_exc).__name__}: {exc.final_exc}",
            }
            debug_payload["latest_refresh"]["final_classification"] = failure_class
            self._write_remote_debug(cache_root, debug_payload, merge=True)
            return []
        except Exception as exc:
            debug_payload["latest_refresh"]["public_data_filter_fetch"] = {
                "status": "error",
                "classification": "public_data_filter_failure",
                "error": f"{type(exc).__name__}: {exc}",
            }
            debug_payload["latest_refresh"]["final_classification"] = "public_data_filter_failure"
            self._write_remote_debug(cache_root, debug_payload, merge=True)
            return []

        debug_payload["latest_refresh"]["public_data_filter_fetch"] = {
            "status": "ok",
            "classification": "success",
            "attempts": list(filter_attempts or []),
            "report_begin_dates": len(list(filter_payload.get("reportBeginDates") or [])),
            "report_end_dates": len(list(filter_payload.get("reportEndDates") or [])),
        }
        date_pairs = self._public_data_date_pairs(filter_payload, as_of=as_of)
        if not date_pairs:
            debug_payload["latest_refresh"]["final_classification"] = "public_data_no_dates"
            self._write_remote_debug(cache_root, debug_payload, merge=True)
            return []

        candidates: List[Dict[str, Any]] = []
        for begin_date, end_date in date_pairs:
            for section in tuple(self.public_data_sections or ("Report Detail",)):
                search_url = self._public_data_search_url(slug, section, begin_date, end_date)
                try:
                    payload, search_attempts = self._fetch_bytes_diagnostic(
                        search_url,
                        extra_headers={
                            "Accept": "application/json,*/*",
                            "X-Requested-With": "XMLHttpRequest",
                            "Referer": public_url,
                        },
                    )
                    parsed = json.loads(payload.decode("utf-8"))
                    if not isinstance(parsed, dict):
                        parsed = {}
                    rows = list(parsed.get("results") or [])
                    if not rows:
                        classification = "public_data_no_rows"
                    else:
                        classification = "success"
                    debug_payload["latest_refresh"]["public_data_search_fetches"].append(
                        {
                            "url": search_url,
                            "section": section,
                            "begin_date": begin_date,
                            "end_date": end_date,
                            "status": "ok",
                            "classification": classification,
                            "attempts": list(search_attempts or []),
                            "rows": len(rows),
                        }
                    )
                    if not rows:
                        continue
                    candidates.append(
                        {
                            "url": search_url,
                            "label": f"USDA public_data {section} {begin_date.isoformat()} to {end_date.isoformat()}",
                            "asset_type": "json",
                            "report_date": pd.Timestamp(end_date),
                            "prefetched_payload": payload,
                        }
                    )
                except RemoteFetchError as exc:
                    failure_class = str(self._classify_remote_error(exc.final_exc) or "public_data_search_failure")
                    debug_payload["latest_refresh"]["public_data_search_fetches"].append(
                        {
                            "url": search_url,
                            "section": section,
                            "begin_date": begin_date,
                            "end_date": end_date,
                            "status": "error",
                            "classification": failure_class,
                            "attempts": list(exc.attempts or []),
                            "error": f"{type(exc.final_exc).__name__}: {exc.final_exc}",
                        }
                    )
                    continue
                except Exception as exc:
                    debug_payload["latest_refresh"]["public_data_search_fetches"].append(
                        {
                            "url": search_url,
                            "section": section,
                            "begin_date": begin_date,
                            "end_date": end_date,
                            "status": "error",
                            "classification": "public_data_search_failure",
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )
                    continue

        if not candidates:
            debug_payload["latest_refresh"]["final_classification"] = "public_data_no_candidates"
            self._write_remote_debug(cache_root, debug_payload, merge=True)
            return []
        debug_payload["latest_refresh"]["selected_candidates"] = [
            self._sanitize_candidate_debug(cand) for cand in candidates
        ]
        debug_payload["latest_refresh"]["chosen_url"] = str(candidates[0].get("url") or "")
        debug_payload["latest_refresh"]["final_classification"] = "success"
        self._write_remote_debug(cache_root, debug_payload, merge=True)
        return candidates

    def discover_remote_assets(self, as_of: Optional[date] = None, cache_root: Optional[Path] = None) -> List[Dict[str, Any]]:
        public_candidates = self._discover_public_data_assets(as_of=as_of, cache_root=cache_root)
        if public_candidates:
            return public_candidates
        landing_url = str(self.landing_page_url or "").strip()
        if not landing_url:
            return []
        debug_payload: Dict[str, Any] = {
            "source": str(self.source or ""),
            "latest_refresh": {
                "as_of": as_of,
                "landing_page_url": landing_url,
                "landing_fetch": {"status": "pending"},
                "slug_id": "",
                "fragment_urls": [],
                "fragment_fetches": [],
                "documents_pages_scanned": [],
                "documents_page_fetches": [],
                "direct_asset_urls_discovered": [],
                "selected_candidates": [],
                "download_attempts": [],
                "chosen_url": "",
                "saved_local_path": "",
                "final_classification": "pending",
            },
        }
        try:
            landing_html, landing_attempts = self._fetch_text_diagnostic(landing_url)
        except RemoteFetchError as exc:
            print(f"[market_data:{self.source}] remote_discovery_failed url={landing_url} error={type(exc).__name__}: {exc}", flush=True)
            landing_class = str(self._classify_remote_error(exc.final_exc) or "landing_fetch_failure")
            debug_payload["latest_refresh"]["landing_fetch"] = {
                "status": "error",
                "classification": landing_class,
                "attempts": list(exc.attempts or []),
                "error": f"{type(exc.final_exc).__name__}: {exc.final_exc}",
            }
            debug_payload["latest_refresh"]["final_classification"] = landing_class if landing_class in {"network_timeout", "environment_blocked", "http_forbidden"} else "landing_fetch_failure"
            self._write_remote_debug(cache_root, debug_payload, merge=True)
            return []
        debug_payload["latest_refresh"]["landing_fetch"] = {
            "status": "ok",
            "classification": "success",
            "attempts": list(landing_attempts or []),
            "html_chars": len(str(landing_html or "")),
        }
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
        slug_match = self._slug_id_re.search(str(landing_html or ""))
        if slug_match:
            debug_payload["latest_refresh"]["slug_id"] = str(slug_match.group("slug") or "").strip()
        # Current USDA pages expose the freshest downloadable assets through AJAX
        # fragments. Those fragments are now part of normal discovery so live refreshes
        # continue to work even when the landing page itself contains no direct file
        # links. This path is intentionally only for "latest refresh" discovery; the
        # deeper month-by-month archive walk lives in `Code/usda_backfill.py` so
        # normal refreshes stay cheap and predictable.
        fragment_urls = self._release_fragment_urls(landing_html, landing_url)
        debug_payload["latest_refresh"]["fragment_urls"] = list(fragment_urls)
        fragment_successes = 0
        for fragment_url in fragment_urls:
            try:
                fragment_payload, fragment_attempts = self._fetch_text_diagnostic(
                    fragment_url,
                    extra_headers={
                        "Accept": "application/json,text/html,*/*",
                        "X-Requested-With": "XMLHttpRequest",
                        "Referer": landing_url,
                    },
                )
            except RemoteFetchError as exc:
                fragment_class = str(self._classify_remote_error(exc.final_exc) or "fragment_fetch_failure")
                print(f"[market_data:{self.source}] ajax_release_failed url={fragment_url} error={type(exc.final_exc).__name__}: {exc.final_exc}", flush=True)
                debug_payload["latest_refresh"]["fragment_fetches"].append(
                    {
                        "url": fragment_url,
                        "status": "error",
                        "classification": fragment_class,
                        "attempts": list(exc.attempts or []),
                        "error": f"{type(exc.final_exc).__name__}: {exc.final_exc}",
                    }
                )
                continue
            fragment_html = self._fragment_html_from_payload(fragment_payload)
            fragment_successes += 1
            debug_payload["latest_refresh"]["fragment_fetches"].append(
                {
                    "url": fragment_url,
                    "status": "ok",
                    "classification": "success",
                    "attempts": list(fragment_attempts or []),
                    "html_chars": len(str(fragment_html or "")),
                }
            )
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
        debug_payload["latest_refresh"]["documents_pages_scanned"] = list(pages_to_scan[1:])
        for page_url in pages_to_scan[1:]:
            try:
                page_html, page_attempts = self._fetch_text_diagnostic(page_url)
            except RemoteFetchError as exc:
                page_class = str(self._classify_remote_error(exc.final_exc) or "landing_fetch_failure")
                print(f"[market_data:{self.source}] documents_page_failed url={page_url} error={type(exc.final_exc).__name__}: {exc.final_exc}", flush=True)
                debug_payload["latest_refresh"]["documents_page_fetches"].append(
                    {
                        "url": page_url,
                        "status": "error",
                        "classification": page_class,
                        "attempts": list(exc.attempts or []),
                        "error": f"{type(exc.final_exc).__name__}: {exc.final_exc}",
                    }
                )
                continue
            debug_payload["latest_refresh"]["documents_page_fetches"].append(
                {
                    "url": page_url,
                    "status": "ok",
                    "classification": "success",
                    "attempts": list(page_attempts or []),
                    "html_chars": len(str(page_html or "")),
                }
            )
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
        debug_payload["latest_refresh"]["direct_asset_urls_discovered"] = [
            self._sanitize_candidate_debug(cand) for cand in candidates
        ]
        if not candidates:
            if not str(debug_payload["latest_refresh"].get("slug_id") or "").strip() and not fragment_urls:
                debug_payload["latest_refresh"]["final_classification"] = "slug_not_found"
            elif fragment_urls and fragment_successes == 0:
                debug_payload["latest_refresh"]["final_classification"] = "fragment_fetch_failure"
            else:
                debug_payload["latest_refresh"]["final_classification"] = "no_candidates_found"
            self._write_remote_debug(cache_root, debug_payload, merge=True)
            return []
        selected = self._select_remote_candidates(candidates, as_of=as_of)
        debug_payload["latest_refresh"]["selected_candidates"] = [
            self._sanitize_candidate_debug(cand) for cand in selected
        ]
        debug_payload["latest_refresh"]["chosen_url"] = str((selected[0] or {}).get("url") or "") if selected else ""
        debug_payload["latest_refresh"]["final_classification"] = "success" if selected else "no_candidates_found"
        self._write_remote_debug(cache_root, debug_payload, merge=True)
        return selected

    def discover_available(self, ticker_root: Path, refresh: bool = False, cache_root: Optional[Path] = None) -> List[Dict[str, Any]]:
        if refresh:
            local_dir = self._local_dir(ticker_root)
            remote_candidates = self.discover_remote_assets(as_of=self._today(), cache_root=cache_root)
            refresh_debug = self._load_remote_debug(cache_root)
            refresh_payload = dict(refresh_debug.get("latest_refresh") or {})
            refresh_payload.setdefault("download_attempts", [])
            refresh_payload.setdefault("selected_candidates", [])
            refresh_payload.setdefault("chosen_url", "")
            refresh_payload.setdefault("saved_local_path", "")
            refresh_payload.setdefault("final_classification", "pending")
            for cand in remote_candidates:
                url = str(cand.get("url") or "").strip()
                report_date = self._date_from_value(cand.get("report_date"))
                asset_type = str(cand.get("asset_type") or "").strip() or self._asset_type_for_name(url)
                if not url or report_date is None:
                    continue
                local_name = self._stable_local_name(report_date, asset_type, url)
                local_path = local_dir / local_name
                if local_path.exists():
                    refresh_payload["download_attempts"].append(
                        {
                            "stage": "asset_download",
                            "url": url,
                            "asset_type": asset_type,
                            "report_date": report_date,
                            "status": "skipped",
                            "classification": "success",
                            "saved_local_path": str(local_path),
                        }
                    )
                    if not str(refresh_payload.get("chosen_url") or "").strip():
                        refresh_payload["chosen_url"] = url
                    if not str(refresh_payload.get("saved_local_path") or "").strip():
                        refresh_payload["saved_local_path"] = str(local_path)
                    print(
                        f"[market_data:{self.source}] asset={asset_type} date={report_date.date().isoformat()} status=skipped path={local_path}",
                        flush=True,
                    )
                    continue
                try:
                    payload = cand.get("prefetched_payload")
                    fetch_attempts: List[Dict[str, Any]] = []
                    if not isinstance(payload, (bytes, bytearray)):
                        payload, fetch_attempts = self._fetch_bytes_diagnostic(url)
                    local_path.write_bytes(payload)
                    refresh_payload["download_attempts"].append(
                        {
                            "stage": "asset_download",
                            "url": url,
                            "asset_type": asset_type,
                            "report_date": report_date,
                            "status": "updated",
                            "classification": "success",
                            "fetch_attempts": list(fetch_attempts or []),
                            "saved_local_path": str(local_path),
                            "bytes": len(payload),
                        }
                    )
                    if not str(refresh_payload.get("chosen_url") or "").strip():
                        refresh_payload["chosen_url"] = url
                    if not str(refresh_payload.get("saved_local_path") or "").strip():
                        refresh_payload["saved_local_path"] = str(local_path)
                    print(
                        f"[market_data:{self.source}] asset={asset_type} date={report_date.date().isoformat()} status=updated path={local_path}",
                        flush=True,
                    )
                except RemoteFetchError as exc:
                    failure_class = str(self._classify_remote_error(exc.final_exc) or "download_failure")
                    refresh_payload["download_attempts"].append(
                        {
                            "stage": "asset_download",
                            "url": url,
                            "asset_type": asset_type,
                            "report_date": report_date,
                            "status": "error",
                            "classification": failure_class,
                            "fetch_attempts": list(exc.attempts or []),
                            "saved_local_path": str(local_path),
                            "error": f"{type(exc.final_exc).__name__}: {exc.final_exc}",
                        }
                    )
                    print(
                        f"[market_data:{self.source}] asset={asset_type} date={report_date.date().isoformat()} status=failed path={local_path} error={type(exc.final_exc).__name__}: {exc.final_exc}",
                        flush=True,
                    )
                except Exception as exc:
                    refresh_payload["download_attempts"].append(
                        {
                            "stage": "asset_download",
                            "url": url,
                            "asset_type": asset_type,
                            "report_date": report_date,
                            "status": "error",
                            "classification": "download_failure",
                            "saved_local_path": str(local_path),
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )
                    print(
                        f"[market_data:{self.source}] asset={asset_type} date={report_date.date().isoformat()} status=failed path={local_path} error={type(exc).__name__}: {exc}",
                        flush=True,
                    )
            download_attempts = list(refresh_payload.get("download_attempts") or [])
            if any(str(item.get("status") or "") in {"updated", "skipped"} for item in download_attempts):
                refresh_payload["final_classification"] = "success"
            elif remote_candidates:
                refresh_payload["final_classification"] = "download_failure"
            elif not str(refresh_payload.get("final_classification") or "").strip() or str(refresh_payload.get("final_classification") or "").strip() == "pending":
                refresh_payload["final_classification"] = "no_candidates_found"
            self._write_remote_debug(cache_root, {"latest_refresh": refresh_payload}, merge=True)
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
        discovered = self.discover_available(ticker_root, refresh=refresh, cache_root=cache_root)
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
