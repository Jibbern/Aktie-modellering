from __future__ import annotations

import dataclasses
import json
import os
import random
import re
import time
from pathlib import Path
from typing import Any, Dict, Optional, Iterable, List

import pandas as pd
import requests

SEC_BASE = "https://data.sec.gov"
EDGAR_BASE = "https://www.sec.gov"
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".tif", ".tiff", ".bmp", ".svg", ".webp"}


def parse_date(s: Any) -> Optional[pd.Timestamp]:
    try:
        if s is None or (isinstance(s, float) and pd.isna(s)):
            return None
        return pd.to_datetime(str(s), errors="coerce").date()
    except Exception:
        return None


def normalize_accession(accn: str) -> str:
    return str(accn).replace("-", "").strip()


def cik10_from_int(cik_int: int) -> str:
    return f"{int(cik_int):010d}"


def strip_html(txt: str) -> str:
    return re.sub(r"<[^>]+>", " ", txt or "")


def _extract_asset_filenames(html: str, exts: Iterable[str]) -> List[str]:
    exts = {e.lower() for e in exts}
    if not html:
        return []
    urls = re.findall(r"""(?:src|href)\s*=\s*["']([^"']+)["']""", html, flags=re.IGNORECASE)
    files: List[str] = []
    for u in urls:
        if not u or u.startswith(("data:", "mailto:", "#")):
            continue
        name = u.split("?")[0].split("#")[0].split("/")[-1]
        if not name:
            continue
        ext = Path(name).suffix.lower()
        if ext in exts:
            files.append(name)
    return sorted(set(files))


def _extract_index_images(index_json: Dict[str, Any]) -> List[str]:
    items = index_json.get("directory", {}).get("item", []) if index_json else []
    names = [it.get("name", "") for it in items]
    imgs = []
    for n in names:
        if not n:
            continue
        ext = Path(n).suffix.lower()
        if ext in IMAGE_EXTS:
            imgs.append(n)
    return sorted(set(imgs))


def _ocr_images(paths: List[Path]) -> str:
    try:
        from PIL import Image, ImageOps  # type: ignore
        import pytesseract  # type: ignore
    except Exception:
        return ""

    tcmd = os.getenv("TESSERACT_CMD")
    if not tcmd:
        default = Path(r"C:\Users\Jibbe\Python-tools\Tesseract")
        if default.is_dir():
            default = default / "tesseract.exe"
        if default.exists():
            tcmd = str(default)
    if tcmd:
        pytesseract.pytesseract.tesseract_cmd = tcmd

    texts: List[str] = []
    for p in paths:
        try:
            with Image.open(p) as im:
                im = im.convert("L")
                im = ImageOps.autocontrast(im)
                # upscale for better OCR on small scans
                im = im.resize((im.width * 2, im.height * 2), Image.LANCZOS)
                bw = im.point(lambda x: 0 if x < 180 else 255, "1")
                txt = pytesseract.image_to_string(bw, config="--psm 6")
                if not txt or len(txt.strip()) < 5:
                    txt = pytesseract.image_to_string(im, config="--psm 6")
            if txt:
                texts.append(txt)
        except Exception:
            continue
    return "\n".join(texts)


@dataclasses.dataclass(frozen=True)
class SecConfig:
    user_agent: str
    timeout_s: int = 30
    max_retries: int = 4
    backoff: float = 0.75
    refresh_submissions: bool = True
    refresh_companyfacts: bool = True
    allow_stale_fallback: bool = True


class SecClient:
    def __init__(self, cache_dir: Path, cfg: SecConfig) -> None:
        self.cache_dir = Path(cache_dir).expanduser().resolve()
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": cfg.user_agent,
            "Accept": "application/json,text/html,*/*",
        })
        self.manifest_rows: list[dict[str, Any]] = []
        self.ocr_log_rows: list[dict[str, Any]] = []

    def _cache_path(self, key: str) -> Path:
        return self.cache_dir / key

    def _read_cache(self, path: Path, *, as_json: bool) -> Any:
        if as_json:
            return json.loads(path.read_text(encoding="utf-8"))
        return path.read_bytes()

    def get(
        self,
        url: str,
        *,
        as_json: bool,
        cache_key: str,
        force_refresh: bool = False,
        allow_stale_on_error: bool = False,
    ) -> Any:
        path = self._cache_path(cache_key)
        if path.exists() and not force_refresh:
            try:
                return self._read_cache(path, as_json=as_json)
            except Exception:
                pass

        def _stale_fallback(last_err: Optional[Exception]) -> Any:
            stale = self._read_cache(path, as_json=as_json)
            self.manifest_rows.append({
                "url": url,
                "cache_key": cache_key,
                "path": str(path),
                "status": f"stale_fallback:{type(last_err).__name__ if last_err is not None else 'unknown'}",
            })
            return stale

        has_cache = path.exists()
        # Connect timeout should stay short; read timeout is shorter when we can fail open to stale cache.
        base_read_timeout = max(12, int(self.cfg.timeout_s))
        req_timeout: Any = (6, base_read_timeout)
        if force_refresh and allow_stale_on_error and has_cache:
            # Avoid long hangs when network is flaky and we already have usable cache.
            req_timeout = (4, min(base_read_timeout, 10))
        # If we already have cache and only want freshness, fail open quickly.
        retries = int(self.cfg.max_retries)
        if force_refresh and allow_stale_on_error and has_cache:
            retries = 1

        last_err: Optional[Exception] = None
        for i in range(retries):
            try:
                r = self.session.get(url, timeout=req_timeout)
                r.raise_for_status()
                if as_json:
                    data = r.json()
                    path.write_text(json.dumps(data), encoding="utf-8")
                else:
                    data = r.content
                    path.write_bytes(data)
                self.manifest_rows.append({
                    "url": url,
                    "cache_key": cache_key,
                    "path": str(path),
                    "status": "ok",
                })
                return data
            except Exception as e:
                last_err = e
                # If network is flaky and we already have cached payload, don't block for long.
                if allow_stale_on_error and has_cache and isinstance(
                    e,
                    (
                        requests.exceptions.Timeout,
                        requests.exceptions.ConnectionError,
                    ),
                ):
                    try:
                        return _stale_fallback(last_err)
                    except Exception:
                        pass
                time.sleep(self.cfg.backoff * (2 ** i) + random.random() * 0.1)
        if allow_stale_on_error and path.exists():
            try:
                return _stale_fallback(last_err)
            except Exception:
                pass
        self.manifest_rows.append({
            "url": url,
            "cache_key": cache_key,
            "path": str(path),
            "status": f"error:{type(last_err).__name__}",
        })
        raise RuntimeError(f"SEC GET failed after retries: {url}") from last_err

    def ticker_cik_map(self) -> Dict[str, Any]:
        url = f"{EDGAR_BASE}/files/company_tickers.json"
        return self.get(url, as_json=True, cache_key="company_tickers.json")

    def companyfacts(self, cik10: str) -> Dict[str, Any]:
        url = f"{SEC_BASE}/api/xbrl/companyfacts/CIK{cik10}.json"
        return self.get(
            url,
            as_json=True,
            cache_key=f"companyfacts_{cik10}.json",
            force_refresh=bool(self.cfg.refresh_companyfacts),
            allow_stale_on_error=bool(self.cfg.allow_stale_fallback),
        )

    def submissions(self, cik10: str) -> Dict[str, Any]:
        # SEC submissions endpoint lives on data.sec.gov (www.sec.gov often returns 404).
        primary_url = f"{SEC_BASE}/submissions/CIK{cik10}.json"
        cache_key = f"submissions_{cik10}.json"
        try:
            return self.get(
                primary_url,
                as_json=True,
                cache_key=cache_key,
                force_refresh=bool(self.cfg.refresh_submissions),
                allow_stale_on_error=bool(self.cfg.allow_stale_fallback),
            )
        except Exception:
            # Fallback kept for compatibility if SEC changes routing again.
            fallback_url = f"{EDGAR_BASE}/submissions/CIK{cik10}.json"
            return self.get(
                fallback_url,
                as_json=True,
                cache_key=cache_key,
                force_refresh=bool(self.cfg.refresh_submissions),
                allow_stale_on_error=bool(self.cfg.allow_stale_fallback),
            )

    def accession_index_json(self, cik_int: int, accn_no_dashes: str) -> Dict[str, Any]:
        url = f"{EDGAR_BASE}/Archives/edgar/data/{cik_int}/{accn_no_dashes}/index.json"
        return self.get(url, as_json=True, cache_key=f"index_{accn_no_dashes}.json")

    def download_document(self, cik_int: int, accn_no_dashes: str, filename: str) -> bytes:
        url = f"{EDGAR_BASE}/Archives/edgar/data/{cik_int}/{accn_no_dashes}/{filename}"
        return self.get(url, as_json=False, cache_key=f"doc_{accn_no_dashes}_{filename}")

    def download_html_assets(
        self,
        cik_int: int,
        accn_no_dashes: str,
        html_bytes: bytes,
        *,
        exts: Optional[Iterable[str]] = None,
        max_assets: int = 50,
    ) -> List[str]:
        html = html_bytes.decode("utf-8", errors="ignore")
        files = _extract_asset_filenames(html, exts or IMAGE_EXTS)
        downloaded: List[str] = []
        for fn in files[:max_assets]:
            try:
                self.download_document(cik_int, accn_no_dashes, fn)
                downloaded.append(fn)
            except Exception:
                continue
        return downloaded

    def download_index_images(
        self,
        cik_int: int,
        accn_no_dashes: str,
        index_json: Dict[str, Any],
        *,
        max_images: int = 50,
    ) -> List[str]:
        files = _extract_index_images(index_json)
        downloaded: List[str] = []
        for fn in files[:max_images]:
            try:
                self.download_document(cik_int, accn_no_dashes, fn)
                downloaded.append(fn)
            except Exception:
                continue
        return downloaded

    def ocr_html_assets(
        self,
        accn_no_dashes: str,
        html_bytes: Optional[bytes] = None,
        *,
        max_images: int = 12,
        min_bytes: int = 12000,
        context: Optional[Dict[str, Any]] = None,
    ) -> str:
        # Return cached OCR text if available (speeds up reruns)
        if context is not None and context.get("doc"):
            safe_doc = re.sub(r"[^\w\-.]+", "_", str(context.get("doc") or "doc"))
            cache_txt = self.cache_dir / f"ocr_{accn_no_dashes}_{safe_doc}.txt"
            if cache_txt.exists():
                try:
                    cached = cache_txt.read_text(encoding="utf-8", errors="ignore")
                except Exception:
                    cached = ""
                if cached:
                    row = {
                        "accn": accn_no_dashes,
                        "doc": context.get("doc"),
                        "quarter": context.get("quarter"),
                        "purpose": context.get("purpose"),
                        "status": "cache_hit",
                        "image_files": "",
                        "n_images": 0,
                        "text_len": len(cached),
                        "text_excerpt": cached[:1000],
                        "ocr_tokens": context.get("ocr_tokens", ""),
                        "report_date": context.get("report_date"),
                        "filing_date": context.get("filing_date"),
                    }
                    self.ocr_log_rows.append(row)
                    return cached
        paths: List[Path] = []
        files: List[str] = []
        if html_bytes is not None:
            html = html_bytes.decode("utf-8", errors="ignore")
            files = _extract_asset_filenames(html, IMAGE_EXTS)
            for fn in files:
                p = self._cache_path(f"doc_{accn_no_dashes}_{fn}")
                if p.exists():
                    paths.append(p)
                if len(paths) >= max_images:
                    break
        if not paths:
            for p in self.cache_dir.glob(f"doc_{accn_no_dashes}_*"):
                if p.suffix.lower() in IMAGE_EXTS:
                    paths.append(p)
                if len(paths) >= max_images:
                    break
        if paths:
            scored: List[tuple[int, Path]] = []
            for p in paths:
                try:
                    sz = p.stat().st_size
                except Exception:
                    continue
                if sz >= min_bytes:
                    scored.append((sz, p))
            if not scored:
                scored = [(p.stat().st_size, p) for p in paths if p.exists()]
            scored.sort(reverse=True)
            paths = [p for _, p in scored][:max_images]
        text = _ocr_images(paths)
        if context is not None:
            row = {
                "accn": accn_no_dashes,
                "doc": context.get("doc"),
                "quarter": context.get("quarter"),
                "purpose": context.get("purpose"),
                "status": "ok" if text else ("no_images" if not paths else "no_text"),
                "image_files": ";".join(files),
                "n_images": len(paths),
                "text_len": len(text or ""),
                "text_excerpt": (text or "")[:1000],
                "ocr_tokens": context.get("ocr_tokens", ""),
                "report_date": context.get("report_date"),
                "filing_date": context.get("filing_date"),
            }
            self.ocr_log_rows.append(row)
            if context.get("save_text") and text:
                safe_doc = re.sub(r"[^\w\-.]+", "_", str(context.get("doc") or "doc"))
                out_path = self.cache_dir / f"ocr_{accn_no_dashes}_{safe_doc}.txt"
                try:
                    out_path.write_text(text, encoding="utf-8")
                except Exception:
                    pass
        return text


def cik_from_ticker(sec: SecClient, ticker: str) -> int:
    data = sec.ticker_cik_map()
    t = ticker.upper().strip()
    for _, row in data.items():
        if str(row.get("ticker", "")).upper() == t:
            return int(row.get("cik_str"))
    raise RuntimeError(f"Could not find CIK for ticker {ticker}")


def companyfacts_to_df(cf: Dict[str, Any], namespace: str = "us-gaap") -> pd.DataFrame:
    """
    Normalize SEC companyfacts JSON to tidy rows:
    tag, unit, val, start/end/filed (string) + start_d/end_d/filed_d (date),
    fy, fp, form, accn, frame
    """
    facts = cf.get("facts", {}).get(namespace, {})
    rows = []
    for tag, payload in facts.items():
        units = payload.get("units", {})
        for unit, series in units.items():
            for it in series:
                rows.append({
                    "tag": tag,
                    "unit": unit,
                    "val": it.get("val"),
                    "start": it.get("start"),
                    "end": it.get("end"),
                    "fy": it.get("fy"),
                    "fp": it.get("fp"),
                    "form": it.get("form"),
                    "filed": it.get("filed"),
                    "frame": it.get("frame"),
                    "accn": it.get("accn"),
                })
    df = pd.DataFrame(rows)
    for c in ("start", "end", "filed"):
        df[c + "_d"] = df[c].map(parse_date)
    df["val"] = pd.to_numeric(df["val"], errors="coerce")

    # Derive fiscal year from end date to avoid inconsistent SEC FY labels.
    fy_end_mmdd = (12, 31)
    if "fp" in df.columns and "end_d" in df.columns:
        fy_rows = df[df["fp"].astype(str).str.upper() == "FY"].copy()
        fy_rows = fy_rows[fy_rows["end_d"].notna()]
        if not fy_rows.empty:
            mmdd = fy_rows["end_d"].map(lambda d: (d.month, d.day))
            if not mmdd.empty:
                try:
                    fy_end_mmdd = mmdd.value_counts().idxmax()
                except Exception:
                    fy_end_mmdd = (12, 31)

    def _calc_fy(end_d: Any) -> Optional[int]:
        if end_d is None or pd.isna(end_d):
            return None
        if (end_d.month, end_d.day) > fy_end_mmdd:
            return int(end_d.year) + 1
        return int(end_d.year)

    df["fy_calc"] = df["end_d"].map(_calc_fy)
    return df
