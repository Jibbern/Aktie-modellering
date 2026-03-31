"""SEC download and local statement-materialization helpers.

This module handles two related jobs:
- downloading filing-package content into `sec_cache`
- materializing good 10-Q / 10-K statement documents into local
  `financial_statement` folders for downstream parsing

The local statement folder complements the SEC cache; it does not replace it.
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import re
import shutil
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests

SEC_BASE = "https://data.sec.gov"
EDGAR_BASE = "https://www.sec.gov"


def _sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def _sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_json_file(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json_file(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def normalize_accession(accn: str) -> str:
    return str(accn).replace("-", "").strip()


def cik10_from_int(cik_int: int) -> str:
    return f"{int(cik_int):010d}"


@dataclasses.dataclass(frozen=True)
class IngestConfig:
    cache_dir: Path
    user_agent: str
    timeout_s: int = 30
    max_retries: int = 4
    backoff: float = 0.75
    min_interval_s: float = 0.3
    forms: Tuple[str, ...] = ("10-Q", "10-K", "8-K", "DEF 14A", "DEFA14A")
    include_exhibits: bool = True
    exhibit_type_prefixes: Tuple[str, ...] = ("EX-",)
    exhibit_exts_allow: Tuple[str, ...] = (".htm", ".html", ".txt", ".pdf", ".xlsx", ".xls", ".xml", ".csv")
    max_file_mb: int = 25
    materialize: bool = True
    materialize_dir: Optional[Path] = None
    attachment_mode: str = "smart"
    verify_cache_sha256: bool = False
    reuse_sha256_from_previous_index: bool = True
    materialize_method: str = "hardlink"
    quiet_download_logs: bool = True
    max_filings: Optional[int] = None


@dataclasses.dataclass(frozen=True)
class FinancialStatementSyncSummary:
    output_dir: Path
    manifest_path: Path
    materialized_count: int
    primary_count: int
    exhibit_count: int
    skipped_missing: int
    skipped_decorative: int
    skipped_noncandidate: int


def _normalize_form_token(form: str) -> str:
    return re.sub(r"\s+", " ", str(form or "").upper()).strip()


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value in (None, ""):
            return None
        return int(float(value))
    except Exception:
        return None


def _allowed_ext(filename: str, allow_exts: Tuple[str, ...]) -> bool:
    ext = Path(str(filename or "")).suffix.lower()
    if not ext:
        return False
    allow = {str(x).lower() for x in allow_exts}
    return ext in allow


def _log_download(cfg: IngestConfig, message: str) -> None:
    if not cfg.quiet_download_logs:
        print(message)


def _looks_generic_sec_type(sec_type: str) -> bool:
    typ = str(sec_type or "").strip().upper()
    if not typ:
        return True
    return typ in {"GRAPHIC", "TEXT", "HTML", "TEXT-EXTRACT", "TXT", "XML", "PDF", "ZIP", "TEXT.GIF"}


def _is_attachment_like_item(name: str, sec_type: str, form: str, cfg: IngestConfig) -> bool:
    mode = str(cfg.attachment_mode or "smart").lower()
    if mode == "off":
        return False
    lname = str(name or "").lower()
    if not lname or _is_xbrl_item(name, sec_type):
        return False
    if not _allowed_ext(name, cfg.exhibit_exts_allow):
        return False
    if mode == "all":
        return True
    if str(form or "").upper() not in {"8-K", "8-K/A"}:
        return False
    if not _looks_generic_sec_type(sec_type):
        return False
    return bool(
        re.search(
            r"(earnings[_ -]?release|financial[_ -]?report|presentation|slides|supplement|"
            r"shareholder[_ -]?letter|news[_ -]?release|press[_ -]?release)",
            lname,
            re.I,
        )
    )


def _is_xbrl_item(name: str, sec_type: str) -> bool:
    lname = str(name or "").lower()
    typ = str(sec_type or "").upper().strip()
    if typ.startswith("EX-101"):
        return True
    if lname.endswith((".xml", ".xsd")) and "filingsummary" not in lname:
        return True
    return False


def _is_exhibit_item(name: str, sec_type: str, cfg: IngestConfig) -> bool:
    lname = str(name or "").lower()
    stem = Path(lname).stem
    typ = str(sec_type or "").upper().strip()
    if typ.startswith("EX-101"):
        return False
    if typ and any(typ.startswith(str(p).upper()) for p in cfg.exhibit_type_prefixes):
        return True
    # Fallback: exhibit-like filename (common when SEC type is generic, e.g. "text.gif").
    if re.match(r"^ex[-_ ]?\d{1,3}[a-z0-9._-]*$", stem, re.I):
        return True
    if re.search(r"\bex[-_ ]?\d{1,3}(?:\.\d+)?\b", lname, re.I):
        return True
    return False


def _prior_hash_key(accession: str, filename: str, bytes_count: int, local_path: str) -> Tuple[str, str, int, str]:
    return (str(accession or ""), str(filename or ""), int(bytes_count or 0), str(local_path or ""))


def _load_prior_hash_lookup(*paths: Path) -> Dict[Tuple[str, str, int, str], str]:
    out: Dict[Tuple[str, str, int, str], str] = {}
    for path in paths:
        try:
            if not path.exists():
                continue
            df = pd.read_csv(path)
        except Exception:
            continue
        required = {"accession", "filename", "bytes", "local_path", "sha256"}
        if not required.issubset(set(df.columns)):
            continue
        for row in df.to_dict("records"):
            sha = str(row.get("sha256") or "").strip()
            if not sha:
                continue
            key = _prior_hash_key(
                str(row.get("accession") or ""),
                str(row.get("filename") or ""),
                _safe_int(row.get("bytes")) or 0,
                str(row.get("local_path") or ""),
            )
            out[key] = sha
    return out


def _infer_exhibit_type_from_filename(name: str) -> Optional[str]:
    stem = Path(str(name or "")).stem
    token = re.split(r"[-_.\s]", stem, maxsplit=1)[0]
    m = re.match(r"^ex[-_ ]?(\d{1,4})([a-z]*)$", token, re.I)
    if not m:
        return None
    main = str(m.group(1) or "")
    suf = str(m.group(2) or "").upper()
    if len(main) <= 2:
        ex_type = f"EX-{int(main)}"
    elif len(main) == 3:
        ex_type = f"EX-{int(main[:2])}.{main[2]}"
    else:
        ex_type = f"EX-{int(main[:2])}.{main[2:]}"
    if suf:
        ex_type = f"{ex_type}{suf}"
    return ex_type


def _sanitize_token(text: str) -> str:
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", str(text or ""))
    s = s.strip("._-")
    return s or "unknown"


def _safe_materialize(src: Path, dst: Path, method: str = "hardlink") -> Path:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        return dst
    chosen = str(method or "hardlink").lower()
    if chosen == "hardlink":
        try:
            os.link(src, dst)
            return dst
        except Exception:
            pass
    shutil.copy2(src, dst)
    return dst


def _candidate_submissions_paths(cache_root: Path, cik10: str) -> List[Path]:
    # The workspace has historically used a few submissions aliases. Keep them all as
    # valid read locations so ingest remains tolerant of older cache layouts.
    return [
        cache_root / cik10 / "submissions.json",
        cache_root / f"submissions_{cik10}.json",
        cache_root / f"submissions_CIK{cik10}-submissions-001.json",
    ]


def _infer_local_cik_from_cache(cache_root: Path) -> Optional[int]:
    root = Path(cache_root)
    candidates: set[int] = set()
    try:
        for path in root.glob("submissions_*.json"):
            m = re.fullmatch(r"submissions_(\d{10})\.json", path.name, re.I)
            if m:
                candidates.add(int(m.group(1)))
        for path in root.glob("submissions_CIK*-submissions-001.json"):
            m = re.fullmatch(r"submissions_CIK(\d{10})-submissions-001\.json", path.name, re.I)
            if m:
                candidates.add(int(m.group(1)))
        for path in root.iterdir():
            if path.is_dir() and re.fullmatch(r"\d{10}", path.name):
                candidates.add(int(path.name))
    except Exception:
        return None
    if len(candidates) == 1:
        return next(iter(candidates))
    return None


def _canonical_name(
    ticker: str,
    form: str,
    report_date: Optional[str],
    filed_date: Optional[str],
    accession: str,
    *,
    kind: str,
    sec_type: str = "",
    filename: str = "",
) -> str:
    tkr = _sanitize_token(str(ticker or "UNK").upper())
    form_tok = _sanitize_token(re.sub(r"\s+", "", str(form or "").upper()))
    date_tok = _sanitize_token((report_date or filed_date or "unknown")[:10])
    acc_tok = _sanitize_token(normalize_accession(accession))
    sec_tok = _sanitize_token(sec_type.upper() if sec_type else "EX")
    src_name = _sanitize_token(Path(str(filename or "")).name or "file")
    ext = Path(str(filename or "")).suffix.lower()
    if kind == "primary":
        ext = ext if ext else ".htm"
        return f"{tkr}_{form_tok}_{date_tok}_{acc_tok}{ext}"
    if kind == "xbrl":
        return f"{tkr}_{form_tok}_{date_tok}_{acc_tok}__instance.xml"
    # exhibit
    return f"{tkr}_{form_tok}_{date_tok}_{acc_tok}__{sec_tok}__{src_name}"


class SecClient:
    def __init__(self, cfg: IngestConfig) -> None:
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": cfg.user_agent,
            "Accept": "application/json,text/html,*/*",
        })
        self._last_req_ts = 0.0

    def _rate_limit(self) -> None:
        elapsed = time.time() - self._last_req_ts
        if elapsed < self.cfg.min_interval_s:
            time.sleep(self.cfg.min_interval_s - elapsed)

    def _sleep_retry(self, resp: Optional[requests.Response], attempt: int) -> None:
        retry_after = None
        if resp is not None:
            ra = resp.headers.get("Retry-After")
            if ra:
                try:
                    retry_after = float(ra)
                except Exception:
                    retry_after = None
        if retry_after is None:
            retry_after = self.cfg.backoff * (2 ** attempt)
        time.sleep(retry_after)

    def get(self, url: str, *, as_json: bool = False) -> bytes | Dict[str, Any]:
        last_err: Optional[Exception] = None
        for i in range(self.cfg.max_retries):
            try:
                self._rate_limit()
                resp = self.session.get(url, timeout=self.cfg.timeout_s)
                self._last_req_ts = time.time()
                if resp.status_code in (429, 403) or resp.status_code >= 500:
                    self._sleep_retry(resp, i)
                    continue
                resp.raise_for_status()
                if as_json:
                    return resp.json()
                return resp.content
            except Exception as e:
                last_err = e
                self._sleep_retry(None, i)
        raise RuntimeError(f"SEC GET failed after retries: {url}") from last_err


def ticker_to_cik(sec: SecClient, ticker: str) -> int:
    url = f"{EDGAR_BASE}/files/company_tickers.json"
    data = sec.get(url, as_json=True)
    t = ticker.upper().strip()
    for _, row in data.items():
        if str(row.get("ticker", "")).upper() == t:
            return int(row.get("cik_str"))
    raise RuntimeError(f"Could not find CIK for ticker {ticker}")


def _filings_df(submissions: Dict[str, Any], forms: Iterable[str]) -> pd.DataFrame:
    recent = submissions.get("filings", {}).get("recent", {})
    forms_norm = {_normalize_form_token(f) for f in forms}
    rows: List[Dict[str, Any]] = []
    n = len(recent.get("form", []))
    for i in range(n):
        form = recent["form"][i]
        base_form = str(form).split("/")[0]
        base_form_norm = _normalize_form_token(base_form)
        if base_form_norm not in forms_norm:
            continue
        rows.append({
            "accession": recent["accessionNumber"][i],
            "form": form,
            "base_form": base_form,
            "is_amendment": form.endswith("/A"),
            "filedDate": recent["filingDate"][i],
            "reportDate": recent["reportDate"][i] if i < len(recent.get("reportDate", [])) else None,
            "periodEnd": recent["reportDate"][i] if i < len(recent.get("reportDate", [])) else None,
            "primaryDoc": recent["primaryDocument"][i] if i < len(recent.get("primaryDocument", [])) else None,
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("filedDate", ascending=False)
    return df


def list_filings(cfg: IngestConfig, *, ticker: Optional[str] = None, cik: Optional[int] = None) -> Tuple[int, pd.DataFrame, Path]:
    sec = SecClient(cfg)
    if cik is not None:
        cik_int = int(cik)
    else:
        local_cik = _infer_local_cik_from_cache(cfg.cache_dir)
        cik_int = int(local_cik) if local_cik is not None else ticker_to_cik(sec, ticker or "")
    cik10 = cik10_from_int(cik_int)
    cache_dir = cfg.cache_dir / cik10
    cache_dir.mkdir(parents=True, exist_ok=True)
    sub_path = cache_dir / "submissions.json"
    existing_sub_path = next((p for p in _candidate_submissions_paths(cfg.cache_dir, cik10) if p.exists()), None)
    if existing_sub_path is not None:
        submissions = _read_json_file(existing_sub_path)
        _log_download(cfg, f"[CACHE HIT] {existing_sub_path}")
        if existing_sub_path != sub_path and not sub_path.exists():
            try:
                _write_json_file(sub_path, submissions)
            except Exception:
                pass
    else:
        url = f"{SEC_BASE}/submissions/CIK{cik10}.json"
        _log_download(cfg, f"[DOWNLOAD] {url}")
        submissions = sec.get(url, as_json=True)
        _write_json_file(sub_path, submissions)
    df = _filings_df(submissions, cfg.forms)
    if cfg.max_filings:
        df = df.head(cfg.max_filings).copy()
    return cik_int, df, sub_path


def _write_manifest(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    df = pd.DataFrame(rows)
    if path.suffix.lower() == ".parquet":
        df.to_parquet(path, index=False)
    else:
        df.to_csv(path, index=False)


def download_filing_package(
    cfg: IngestConfig,
    sec: SecClient,
    cik_int: int,
    filing: Dict[str, Any],
    prior_hash_lookup: Optional[Dict[Tuple[str, str, int, str], str]] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Download package for a single accession.
    Returns: files_rows, exhibits_rows, instance_paths
    """
    accn = filing["accession"]
    accn_nd = normalize_accession(accn)
    cik10 = cik10_from_int(cik_int)
    form = str(filing.get("form") or "")
    filed_date = filing.get("filedDate")
    report_date = filing.get("reportDate")
    primary_doc = str(filing.get("primaryDoc") or "").strip()
    ticker = str(filing.get("ticker") or "").strip().upper()
    size_limit = int(max(1, cfg.max_file_mb)) * 1024 * 1024

    accn_dir = cfg.cache_dir / cik10 / accn_nd
    docs_dir = accn_dir / "docs"
    xbrl_dir = accn_dir / "xbrl"
    exh_dir = accn_dir / "exhibits"
    accn_dir.mkdir(parents=True, exist_ok=True)
    docs_dir.mkdir(parents=True, exist_ok=True)
    xbrl_dir.mkdir(parents=True, exist_ok=True)
    exh_dir.mkdir(parents=True, exist_ok=True)

    files_rows: List[Dict[str, Any]] = []
    exhibits_rows: List[Dict[str, Any]] = []
    instance_paths: List[Dict[str, Any]] = []
    material_root = Path(cfg.materialize_dir) if cfg.materialize_dir is not None else (cfg.cache_dir / "materials")
    sec_primary_dir = material_root / "sec_primary"
    sec_exhibits_dir = material_root / "sec_exhibits"
    sec_xbrl_dir = material_root / "sec_xbrl"
    prior_hashes = dict(prior_hash_lookup or {})
    prior_hashes.update(_load_prior_hash_lookup(accn_dir / "files_items.csv"))

    def _append_row(
        *,
        kind: str,
        sec_type: str,
        filename: str,
        url: str,
        local_path: Path,
        status: str,
        bytes_count: int,
        sha256_hex: str,
        error: str,
    ) -> Dict[str, Any]:
        row = {
            "accession": accn,
            "form": form,
            "filedDate": filed_date,
            "reportDate": report_date,
            "primaryDoc": primary_doc,
            "kind": kind,
            "sec_type": sec_type,
            "filename": filename,
            "url": url,
            "local_path": str(local_path),
            "bytes": int(bytes_count or 0),
            "sha256": sha256_hex or "",
            "status": status,
            "error": error or "",
            "materialized_path": "",
        }
        files_rows.append(row)
        if kind == "exhibit":
            exhibits_rows.append(row.copy())
        return row

    def _cached_sha256(filename: str, local_path: Path, bytes_count: int) -> str:
        if cfg.verify_cache_sha256:
            try:
                return _sha256_path(local_path)
            except Exception:
                return ""
        if cfg.reuse_sha256_from_previous_index:
            key = _prior_hash_key(accn, filename, bytes_count, str(local_path))
            return str(prior_hashes.get(key) or "")
        return ""

    # index.json
    index_url = f"{EDGAR_BASE}/Archives/edgar/data/{cik_int}/{accn_nd}/index.json"
    index_path = accn_dir / "index.json"
    if index_path.exists():
        index_json = _read_json_file(index_path)
        _append_row(
            kind="meta",
            sec_type="INDEX",
            filename=index_path.name,
            url=index_url,
            local_path=index_path,
            status="cache_hit",
            bytes_count=index_path.stat().st_size,
            sha256_hex=_cached_sha256(index_path.name, index_path, index_path.stat().st_size),
            error="",
        )
    else:
        try:
            _log_download(cfg, f"[DOWNLOAD] {index_url}")
            index_json = sec.get(index_url, as_json=True)
            _write_json_file(index_path, index_json)
            _append_row(
                kind="meta",
                sec_type="INDEX",
                filename=index_path.name,
                url=index_url,
                local_path=index_path,
                status="ok",
                bytes_count=index_path.stat().st_size,
                sha256_hex=_sha256_path(index_path),
                error="",
            )
        except Exception as e:
            _append_row(
                kind="meta",
                sec_type="INDEX",
                filename=index_path.name,
                url=index_url,
                local_path=index_path,
                status="error",
                bytes_count=0,
                sha256_hex="",
                error=f"{type(e).__name__}: {e}",
            )
            return files_rows, exhibits_rows, instance_paths

    items = index_json.get("directory", {}).get("item", [])

    def _download_file(
        *,
        url: str,
        local_path: Path,
        kind: str,
        sec_type: str,
        filename: str,
        size_hint: Optional[int] = None,
    ) -> Tuple[bool, Optional[Path], Dict[str, Any]]:
        if size_hint is not None and size_hint > size_limit:
            row = _append_row(
                kind=kind,
                sec_type=sec_type,
                filename=filename,
                url=url,
                local_path=local_path,
                status="skipped_size",
                bytes_count=size_hint,
                sha256_hex="",
                error=f"size>{cfg.max_file_mb}MB",
            )
            return False, None, row
        if kind == "exhibit" and not _allowed_ext(filename, cfg.exhibit_exts_allow):
            row = _append_row(
                kind=kind,
                sec_type=sec_type,
                filename=filename,
                url=url,
                local_path=local_path,
                status="skipped_ext",
                bytes_count=size_hint or 0,
                sha256_hex="",
                error="extension not allowed",
            )
            return False, None, row
        if local_path.exists():
            bytes_count = local_path.stat().st_size
            row = _append_row(
                kind=kind,
                sec_type=sec_type,
                filename=filename,
                url=url,
                local_path=local_path,
                status="cache_hit",
                bytes_count=bytes_count,
                sha256_hex=_cached_sha256(filename, local_path, bytes_count),
                error="",
            )
            return True, local_path, row
        try:
            _log_download(cfg, f"[DOWNLOAD] {url}")
            data = sec.get(url, as_json=False)
            local_path.write_bytes(data)
            row = _append_row(
                kind=kind,
                sec_type=sec_type,
                filename=filename,
                url=url,
                local_path=local_path,
                status="ok",
                bytes_count=len(data),
                sha256_hex=_sha256_bytes(data),
                error="",
            )
            return True, local_path, row
        except Exception as e:
            row = _append_row(
                kind=kind,
                sec_type=sec_type,
                filename=filename,
                url=url,
                local_path=local_path,
                status="error",
                bytes_count=0,
                sha256_hex="",
                error=f"{type(e).__name__}: {e}",
            )
            return False, None, row

    item_by_name: Dict[str, Dict[str, Any]] = {}
    for it in items:
        nm = str(it.get("name") or "").strip()
        if nm:
            item_by_name[nm] = it

    # primary document
    if primary_doc:
        it = item_by_name.get(primary_doc, {})
        size_hint = _safe_int(it.get("size")) if it else None
        url = f"{EDGAR_BASE}/Archives/edgar/data/{cik_int}/{accn_nd}/{primary_doc}"
        ok, local, row = _download_file(
            url=url,
            local_path=docs_dir / primary_doc,
            kind="primary",
            sec_type=str(it.get("type") or "PRIMARY"),
            filename=primary_doc,
            size_hint=size_hint,
        )
        if ok and local is not None and cfg.materialize:
            canon = _canonical_name(
                ticker=ticker or cik10,
                form=form,
                report_date=str(report_date or ""),
                filed_date=str(filed_date or ""),
                accession=accn,
                kind="primary",
                sec_type=str(it.get("type") or "PRIMARY"),
                filename=primary_doc,
            )
            dst = _safe_materialize(local, sec_primary_dir / canon, method=cfg.materialize_method)
            row["materialized_path"] = str(dst)

    # FilingSummary.xml
    for it in items:
        if str(it.get("name", "")).lower() == "filingsummary.xml":
            url = f"{EDGAR_BASE}/Archives/edgar/data/{cik_int}/{accn_nd}/{it['name']}"
            _download_file(
                url=url,
                local_path=docs_dir / it["name"],
                kind="meta",
                sec_type=str(it.get("type") or "FILING_SUMMARY"),
                filename=it["name"],
                size_hint=_safe_int(it.get("size")),
            )

    for it in items:
        name = it.get("name", "")
        if not name:
            continue
        lname = name.lower()
        if name == primary_doc or lname == "filingsummary.xml":
            continue
        url = f"{EDGAR_BASE}/Archives/edgar/data/{cik_int}/{accn_nd}/{name}"
        typ = str(it.get("type", "")).upper()
        size_hint = _safe_int(it.get("size"))

        # XBRL: EX-101.* or xml/xsd (excluding FilingSummary)
        if _is_xbrl_item(name, typ):
            local = xbrl_dir / name
            ok, _local, row = _download_file(
                url=url,
                local_path=local,
                kind="xbrl",
                sec_type=typ or "XBRL",
                filename=name,
                size_hint=size_hint,
            )
            is_instance = (
                typ.endswith("INS")
                or typ == "EX-101.INS"
                or "_ins" in lname
                or "instance" in lname
                or (lname.endswith(".xml") and not any(k in lname for k in ("cal", "def", "lab", "pre", "xsd")))
            )
            if ok and is_instance:
                instance_paths.append({
                    "path": local,
                    "accession": accn,
                    "form": form,
                    "filedDate": filed_date,
                    "reportDate": report_date,
                    "primaryDoc": primary_doc,
                    "ticker": ticker,
                })
                if cfg.materialize:
                    canon = _canonical_name(
                        ticker=ticker or cik10,
                        form=form,
                        report_date=str(report_date or ""),
                        filed_date=str(filed_date or ""),
                        accession=accn,
                        kind="xbrl",
                        sec_type=typ or "XBRL",
                        filename=name,
                    )
                    dst = _safe_materialize(local, sec_xbrl_dir / canon, method=cfg.materialize_method)
                    row["materialized_path"] = str(dst)
            continue

        is_exhibit = cfg.include_exhibits and _is_exhibit_item(name, typ, cfg)
        is_attachment = cfg.include_exhibits and _is_attachment_like_item(name, typ, form, cfg)
        if is_exhibit or is_attachment:
            inferred_sec_type = _infer_exhibit_type_from_filename(name)
            if is_exhibit:
                sec_type = typ if typ.startswith("EX-") else (inferred_sec_type or (typ if typ else "EX-UNKNOWN"))
            else:
                sec_type = inferred_sec_type or ("ATTACHMENT" if cfg.attachment_mode == "all" else "ATTACHMENT-SMART")
            local = exh_dir / name
            ok, local_saved, row = _download_file(
                url=url,
                local_path=local,
                kind="exhibit",
                sec_type=sec_type,
                filename=name,
                size_hint=size_hint,
            )
            if ok and local_saved is not None and cfg.materialize:
                canon = _canonical_name(
                    ticker=ticker or cik10,
                    form=form,
                    report_date=str(report_date or ""),
                    filed_date=str(filed_date or ""),
                    accession=accn,
                    kind="exhibit",
                    sec_type=sec_type,
                    filename=name,
                )
                dst = _safe_materialize(local_saved, sec_exhibits_dir / canon, method=cfg.materialize_method)
                row["materialized_path"] = str(dst)

    # write per-accession files index
    _write_manifest(accn_dir / "files_items.csv", files_rows)
    return files_rows, exhibits_rows, instance_paths


def download_all(
    cfg: IngestConfig,
    *,
    ticker: Optional[str] = None,
    cik: Optional[int] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, List[Dict[str, Any]]]:
    sec = SecClient(cfg)
    cik_int, filings_df, sub_path = list_filings(cfg, ticker=ticker, cik=cik)
    prior_hash_lookup = _load_prior_hash_lookup(cfg.cache_dir / "sec_index" / "files.csv")

    files_rows: List[Dict[str, Any]] = []
    exhibits_rows: List[Dict[str, Any]] = []
    instance_paths: List[Dict[str, Any]] = []

    if sub_path.exists():
        sub_bytes = sub_path.stat().st_size
        sub_sha = ""
        if cfg.verify_cache_sha256:
            sub_sha = _sha256_path(sub_path)
        elif cfg.reuse_sha256_from_previous_index:
            sub_sha = str(
                prior_hash_lookup.get(
                    _prior_hash_key("", sub_path.name, sub_bytes, str(sub_path))
                )
                or ""
            )
        files_rows.append({
            "accession": "",
            "form": "submissions",
            "filedDate": "",
            "reportDate": "",
            "primaryDoc": "",
            "kind": "meta",
            "sec_type": "SUBMISSIONS",
            "filename": sub_path.name,
            "url": str(sub_path),
            "local_path": str(sub_path),
            "bytes": sub_bytes,
            "sha256": sub_sha,
            "status": "cache_hit",
            "error": "",
            "materialized_path": "",
        })

    for filing in filings_df.to_dict("records"):
        filing["ticker"] = str(ticker or "").upper()
        m_rows, e_rows, i_paths = download_filing_package(
            cfg,
            sec,
            cik_int,
            filing,
            prior_hash_lookup=prior_hash_lookup,
        )
        files_rows.extend(m_rows)
        exhibits_rows.extend(e_rows)
        instance_paths.extend(i_paths)

    files_df = pd.DataFrame(files_rows)
    exhibits_df = pd.DataFrame(exhibits_rows)
    return filings_df, files_df, exhibits_df, instance_paths


_FINANCIAL_STATEMENT_FORMS = {"10-Q", "10-Q/A", "10-K", "10-K/A"}
_FINANCIAL_STATEMENT_DECORATIVE_RE = re.compile(
    r"(favicon|logo|icon|header|footer|banner|watermark|thumb|thumbnail|seal|graphic)",
    re.I,
)
_FINANCIAL_STATEMENT_HINT_RE = re.compile(
    r"(annual[_ -]?report|quarterly[_ -]?report|financial|statement|balance[_ -]?sheet|cash[_ -]?flow|"
    r"operations|schedule|supplement|appendix|debt|credit|term[_ -]?loan|note|indenture|mezzanine|convertible)",
    re.I,
)


def _financial_statement_period_label(form: str, report_date: Any, filed_date: Any) -> str:
    ts = pd.to_datetime(report_date or filed_date, errors="coerce")
    if pd.isna(ts):
        return "UNK"
    q_end = pd.Timestamp(ts).date()
    if q_end.month == 12 and q_end.day == 31 and str(form or "").upper().startswith("10-K"):
        return f"FY{q_end.year}"
    quarter_num = ((int(q_end.month) - 1) // 3) + 1
    return f"Q{quarter_num}_{q_end.year}"


def _financial_statement_canonical_name(
    *,
    ticker: str,
    form: str,
    report_date: Any,
    filed_date: Any,
    kind: str,
    sec_type: str,
    filename: str,
) -> str:
    tkr = _sanitize_token(str(ticker or "").upper() or "UNK")
    form_txt = str(form or "").upper()
    form_tok = "10K" if form_txt.startswith("10-K") else "10Q" if form_txt.startswith("10-Q") else _sanitize_token(form_txt or "FORM")
    date_tok = _sanitize_token(str(report_date or filed_date or "unknown")[:10])
    period_tok = _sanitize_token(_financial_statement_period_label(form, report_date, filed_date))
    ext = Path(str(filename or "")).suffix.lower() or ".htm"
    if kind == "primary":
        return f"{tkr}_{period_tok}_{form_tok}_{date_tok}_financial_statement{ext}"
    sec_tok = _sanitize_token(str(sec_type or "EX").upper())
    stem_tok = _sanitize_token(Path(str(filename or "")).stem or "file")
    return f"{tkr}_{period_tok}_{form_tok}_{date_tok}_financial_statement__{sec_tok}__{stem_tok}{ext}"


def _is_financial_statement_candidate(row: Dict[str, Any]) -> Tuple[bool, str]:
    # The statement folder should keep real primary docs and a very small set of useful
    # exhibits, while decorative SEC viewer assets are filtered out here.
    form = str(row.get("form") or "").upper()
    if form not in _FINANCIAL_STATEMENT_FORMS:
        return False, "non_form"
    status = str(row.get("status") or "").lower()
    if status not in {"ok", "cache_hit"}:
        return False, "bad_status"
    filename = str(row.get("filename") or "")
    if not filename:
        return False, "no_filename"
    if _FINANCIAL_STATEMENT_DECORATIVE_RE.search(filename):
        return False, "decorative"
    ext = Path(filename).suffix.lower()
    if ext not in {".pdf", ".htm", ".html", ".txt"}:
        return False, "bad_ext"
    kind = str(row.get("kind") or "").lower()
    if kind == "primary":
        return True, "primary"
    if kind != "exhibit":
        return False, "noncandidate"
    sec_type = str(row.get("sec_type") or "").upper()
    byte_count = _safe_int(row.get("bytes")) or 0
    if byte_count and byte_count < 4096:
        return False, "decorative"
    if sec_type.startswith("EX-13"):
        return True, "exhibit"
    if sec_type.startswith(("EX-4", "EX-10", "EX-12", "EX-99")) and _FINANCIAL_STATEMENT_HINT_RE.search(filename):
        return True, "exhibit"
    if _FINANCIAL_STATEMENT_HINT_RE.search(filename):
        return True, "exhibit"
    return False, "noncandidate"


def materialize_financial_statement_files(
    files_df: pd.DataFrame,
    *,
    output_dir: Path,
    ticker: str,
    method: str = "hardlink",
) -> Tuple[pd.DataFrame, FinancialStatementSyncSummary]:
    # Materialization writes a durable manifest because the resulting folder becomes a
    # curated local source family that later pipeline and writer stages consume.
    out_dir = Path(output_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    rows: List[Dict[str, Any]] = []
    seen_targets: set[str] = set()
    primary_count = 0
    exhibit_count = 0
    skipped_missing = 0
    skipped_decorative = 0
    skipped_noncandidate = 0

    if files_df is None or files_df.empty:
        manifest_path = out_dir / f"{str(ticker or 'SEC').upper()}_financial_statement_manifest.csv"
        pd.DataFrame().to_csv(manifest_path, index=False)
        return pd.DataFrame(), FinancialStatementSyncSummary(
            output_dir=out_dir,
            manifest_path=manifest_path,
            materialized_count=0,
            primary_count=0,
            exhibit_count=0,
            skipped_missing=0,
            skipped_decorative=0,
            skipped_noncandidate=0,
        )

    for row in files_df.to_dict("records"):
        is_candidate, reason = _is_financial_statement_candidate(row)
        if not is_candidate:
            if reason == "decorative":
                skipped_decorative += 1
            else:
                skipped_noncandidate += 1
            continue
        local_path = Path(str(row.get("local_path") or ""))
        if not local_path.exists():
            skipped_missing += 1
            continue
        kind = str(row.get("kind") or "").lower()
        canon = _financial_statement_canonical_name(
            ticker=str(ticker or "").upper(),
            form=str(row.get("form") or ""),
            report_date=row.get("reportDate"),
            filed_date=row.get("filedDate"),
            kind=kind,
            sec_type=str(row.get("sec_type") or ""),
            filename=str(row.get("filename") or local_path.name),
        )
        if canon in seen_targets:
            continue
        seen_targets.add(canon)
        dst = _safe_materialize(local_path, out_dir / canon, method=method)
        rows.append(
            {
                "ticker": str(ticker or "").upper(),
                "form": row.get("form"),
                "reportDate": row.get("reportDate"),
                "filedDate": row.get("filedDate"),
                "kind": kind,
                "sec_type": row.get("sec_type"),
                "filename": row.get("filename"),
                "source_local_path": str(local_path),
                "materialized_path": str(dst),
                "bytes": _safe_int(row.get("bytes")) or 0,
                "status": row.get("status"),
            }
        )
        if kind == "primary":
            primary_count += 1
        else:
            exhibit_count += 1

    manifest_df = pd.DataFrame(rows)
    manifest_path = out_dir / f"{str(ticker or 'SEC').upper()}_financial_statement_manifest.csv"
    manifest_df.to_csv(manifest_path, index=False)
    summary = FinancialStatementSyncSummary(
        output_dir=out_dir,
        manifest_path=manifest_path,
        materialized_count=len(rows),
        primary_count=primary_count,
        exhibit_count=exhibit_count,
        skipped_missing=skipped_missing,
        skipped_decorative=skipped_decorative,
        skipped_noncandidate=skipped_noncandidate,
    )
    return manifest_df, summary


def download_and_materialize_financial_statements(
    cfg: IngestConfig,
    *,
    ticker: Optional[str] = None,
    cik: Optional[int] = None,
    output_dir: Path,
    method: Optional[str] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, FinancialStatementSyncSummary]:
    # Operator-friendly path: discover filing files first, then immediately keep only
    # the subset worth carrying forward as local statement evidence.
    sync_cfg = dataclasses.replace(
        cfg,
        forms=("10-Q", "10-K"),
        include_exhibits=True,
        materialize=False,
        attachment_mode="smart",
    )
    filings_df, files_df, exhibits_df, _ = download_all(sync_cfg, ticker=ticker, cik=cik)
    manifest_df, summary = materialize_financial_statement_files(
        files_df,
        output_dir=output_dir,
        ticker=str(ticker or ""),
        method=str(method or cfg.materialize_method or "hardlink").lower(),
    )
    return filings_df, files_df, manifest_df, summary
