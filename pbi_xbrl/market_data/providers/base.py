from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from ..cache import file_fingerprint, raw_source_dir


class BaseMarketProvider:
    source = ""
    provider_parse_version = "v1"
    local_patterns: tuple[str, ...] = tuple()

    def discover_available(self, ticker_root: Path, refresh: bool = False) -> List[Dict[str, Any]]:
        del refresh
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
                    }
                )
        return out

    def sync_raw(self, cache_root: Path, ticker_root: Path, refresh: bool = False) -> Dict[str, Any]:
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
