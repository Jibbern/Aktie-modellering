"""Local source-material discovery, normalization, manifest, and coverage refresh."""
from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import re
import shutil
import time
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urljoin, urlparse

import requests

from .cache_layout import canonical_ticker_cache_root
from .company_profiles import CompanyProfile, SourceMaterialSeed, get_company_profile
from .doc_intel import extract_pdf_text_cached
from .excel_writer_sources import infer_q_from_name
from .non_gaap import find_ex99_docs, infer_quarter_end_from_text, strip_html
from .sec_ingest import (
    EDGAR_BASE,
    IngestConfig,
    SecClient,
    _canonical_name,
    _filings_df,
    _write_json_file,
    cik10_from_int,
    download_filing_package,
    normalize_accession,
    ticker_to_cik,
)

try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover
    BeautifulSoup = None


CANONICAL_FAMILY_ALIASES: Dict[str, Tuple[str, ...]] = {
    "earnings_release": ("earnings_release", "Earnings Release", "Earnings Releases"),
    "press_release": ("press_release", "Press Release"),
    "earnings_presentation": ("earnings_presentation", "slides", "Earnings Presentation"),
    "earnings_transcripts": ("earnings_transcripts", "earnings_transcript", "transcripts", "Earnings Transcripts"),
    "annual_reports": ("annual_reports", "Annual Reports"),
}
LOCAL_MANUAL_FAMILY_ALIASES: Dict[str, Tuple[str, ...]] = {
    "earnings_release": ("earnings_release", "Earnings Release", "Earnings Releases"),
    "press_release": ("press_release", "Press Release"),
    "earnings_presentation": ("earnings_presentation", "slides", "Earnings Presentation", "presentation", "presentations"),
    "earnings_transcripts": ("earnings_transcripts", "earnings_transcript", "transcripts", "Earnings Transcripts"),
    "ceo_letters": ("ceo_letters", "CEO_letters", "CEO letters"),
    "annual_reports": ("annual_reports", "Annual Reports"),
}
DIRECT_ASSET_EXTS = {".htm", ".html", ".pdf", ".txt"}
SKIP_EXTS = {".ico"}
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp"}
SEC_PRIMARY_FORMS = ("10-Q", "10-K", "8-K")
MANIFEST_RELATIVE_PATH = Path("materials") / "source_material_manifest.json"
COVERAGE_REPORT_RELATIVE_PATH = Path("materials") / "source_material_coverage_report.json"
RESULTS_FAMILY_SET = {"earnings_release", "earnings_presentation", "earnings_transcripts"}
LOCAL_SOURCE_EXTS = {".htm", ".html", ".pdf", ".txt", ".md"}


@dataclasses.dataclass
class FilingPackagePresence:
    status: str
    index_path: Optional[Path]
    primary_path: Optional[Path]
    candidate_paths: Tuple[Path, ...] = tuple()


@dataclasses.dataclass(frozen=True)
class QuarterAssignment:
    quarter: Optional[date]
    status: str
    reason: str


@dataclasses.dataclass
class MaterialCandidate:
    canonical_family: str
    quarter: Optional[date]
    local_path: Optional[Path]
    source_url: str
    title: str
    origin: str
    accession: str = ""
    form: str = ""
    report_date: str = ""
    filed_date: str = ""
    exhibit_type: str = ""
    selection_reason: str = ""
    source_doc_title: str = ""
    quarter_assignment_status: str = ""
    quarter_assignment_reason: str = ""
    subject_slug: str = ""


@dataclasses.dataclass
class MaterialEvent:
    ticker: str
    family: str
    status: str
    origin: str
    quarter: str = ""
    source_url: str = ""
    destination_path: str = ""
    reason: str = ""
    title: str = ""


@dataclasses.dataclass
class IRSeedDiagnostic:
    family: str
    seed_url: str
    outcome: str
    detail: str = ""
    asset_count: int = 0
    ambiguous_count: int = 0
    webcast_only_count: int = 0
    detail_failures: int = 0


@dataclasses.dataclass
class IRDiscoveryResult:
    candidates: List[MaterialCandidate] = dataclasses.field(default_factory=list)
    diagnostics: List[IRSeedDiagnostic] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class IRFetchResult:
    outcome: str
    html: str = ""
    detail: str = ""
    status_code: int = 0


@dataclasses.dataclass
class LocalMaterialScanResult:
    candidates: List[MaterialCandidate] = dataclasses.field(default_factory=list)
    moved_files: List[Dict[str, Any]] = dataclasses.field(default_factory=list)
    renamed_files: List[Dict[str, Any]] = dataclasses.field(default_factory=list)
    duplicate_files: List[Dict[str, Any]] = dataclasses.field(default_factory=list)
    manual_review_files: List[Dict[str, Any]] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class SourceMaterialRefreshSummary:
    ticker: str
    skipped_reason: str = ""
    filings_added: int = 0
    filings_refreshed: int = 0
    filings_skipped: int = 0
    filings_failed: int = 0
    material_added: int = 0
    material_skipped: int = 0
    material_failed: int = 0
    manual_transcript_fallbacks: int = 0
    coverage_report_path: str = ""
    coverage_lines: List[str] = dataclasses.field(default_factory=list)
    events: List[MaterialEvent] = dataclasses.field(default_factory=list)

    def add_event(self, **kwargs: Any) -> None:
        self.events.append(MaterialEvent(ticker=self.ticker, **kwargs))

    @property
    def supported(self) -> bool:
        return not bool(self.skipped_reason)


def supports_source_material_refresh(profile: CompanyProfile) -> Tuple[bool, str]:
    if not tuple(getattr(profile, "official_source_seeds", ()) or ()):
        return False, "no official source seeds configured"
    return True, ""


def refresh_source_materials(
    *,
    repo_root: Path,
    tickers: Sequence[str],
    user_agent: str,
    max_filings: Optional[int] = None,
    cache_dir_override: Optional[Path] = None,
    dry_run: bool = False,
) -> List[SourceMaterialRefreshSummary]:
    summaries: List[SourceMaterialRefreshSummary] = []
    tickers_u = [str(t or "").strip().upper() for t in tickers if str(t or "").strip()]
    single_override = cache_dir_override if cache_dir_override is not None and len(tickers_u) == 1 else None
    for ticker in tickers_u:
        profile = get_company_profile(ticker)
        ok, reason = supports_source_material_refresh(profile)
        if not ok:
            summaries.append(SourceMaterialRefreshSummary(ticker=ticker, skipped_reason=reason))
            continue
        cache_dir = single_override if single_override is not None else canonical_ticker_cache_root(repo_root, ticker).resolve()
        summary = _refresh_ticker_source_materials(
            repo_root=repo_root,
            ticker=ticker,
            profile=profile,
            cache_dir=cache_dir,
            user_agent=user_agent,
            max_filings=max_filings,
            dry_run=dry_run,
        )
        summaries.append(summary)
    return summaries


def format_refresh_summary(summary: SourceMaterialRefreshSummary) -> str:
    if not summary.supported:
        return f"[source_materials] ticker={summary.ticker} skipped reason={summary.skipped_reason}"
    parts = [
        f"[source_materials] ticker={summary.ticker}",
        f"filings_added={summary.filings_added}",
        f"filings_refreshed={summary.filings_refreshed}",
        f"filings_skipped={summary.filings_skipped}",
        f"filings_failed={summary.filings_failed}",
        f"material_added={summary.material_added}",
        f"material_skipped={summary.material_skipped}",
        f"material_failed={summary.material_failed}",
        f"manual_transcript_fallbacks={summary.manual_transcript_fallbacks}",
    ]
    return " ".join(parts)


def _refresh_ticker_source_materials(
    *,
    repo_root: Path,
    ticker: str,
    profile: CompanyProfile,
    cache_dir: Path,
    user_agent: str,
    max_filings: Optional[int],
    dry_run: bool,
) -> SourceMaterialRefreshSummary:
    summary = SourceMaterialRefreshSummary(ticker=ticker)
    cache_dir.mkdir(parents=True, exist_ok=True)
    material_root = _ticker_material_root(repo_root, ticker)
    material_root.mkdir(parents=True, exist_ok=True)
    manifest_path = cache_dir / MANIFEST_RELATIVE_PATH
    manifest = _load_manifest(manifest_path)
    local_scan = _normalize_and_collect_local_materials(
        repo_root=repo_root,
        ticker=ticker,
        manifest=manifest,
        dry_run=dry_run,
    )
    for row in local_scan.moved_files:
        summary.add_event(
            family=str(row.get("canonical_family") or row.get("family") or ""),
            status="moved",
            origin="manual_local",
            quarter=str(row.get("quarter") or ""),
            destination_path=str(row.get("to_path") or ""),
            reason=f"moved from {row.get('from_path') or ''}",
            title=str(row.get("title") or ""),
        )
    for row in local_scan.renamed_files:
        summary.add_event(
            family=str(row.get("canonical_family") or row.get("family") or ""),
            status="renamed",
            origin="manual_local",
            quarter=str(row.get("quarter") or ""),
            destination_path=str(row.get("to_path") or ""),
            reason=f"renamed from {row.get('from_path') or ''}",
            title=str(row.get("title") or ""),
        )
    for row in local_scan.duplicate_files:
        summary.add_event(
            family=str(row.get("canonical_family") or row.get("family") or ""),
            status="duplicate",
            origin="manual_local",
            quarter=str(row.get("quarter") or ""),
            destination_path=str(row.get("existing_path") or ""),
            reason=f"exact duplicate preserved at {row.get('duplicate_path') or ''}",
            title=str(row.get("title") or ""),
        )
    for row in local_scan.manual_review_files:
        summary.add_event(
            family=str(row.get("canonical_family") or row.get("family_hint") or ""),
            status="manual_review",
            origin="manual_local",
            quarter=str(row.get("quarter") or ""),
            destination_path=str(row.get("path") or ""),
            reason=str(row.get("reason") or "manual review needed"),
            title=str(row.get("title") or ""),
        )

    ingest_cfg = IngestConfig(
        cache_dir=cache_dir,
        user_agent=user_agent,
        forms=SEC_PRIMARY_FORMS,
        include_exhibits=True,
        materialize=False,
        quiet_download_logs=True,
        max_filings=max_filings,
    )
    cik_int, filings_df, _sub_path = _list_recent_filings_with_legacy_support(ingest_cfg, ticker=ticker)
    sec_client: Optional[SecClient] = None
    quarter_targets = _quarter_targets_from_filings(filings_df)
    quarter_family_sources: Dict[Tuple[str, str], str] = {}
    selected_manifest_keys: set[str] = set()

    for filing in filings_df.to_dict("records"):
        filing["ticker"] = ticker
        presence = _detect_filing_package_presence(cache_dir, cik_int, filing)
        if presence.status == "present_complete":
            summary.filings_skipped += 1
        else:
            if dry_run:
                if presence.status == "present_incomplete":
                    summary.filings_refreshed += 1
                else:
                    summary.filings_added += 1
            else:
                try:
                    if sec_client is None:
                        sec_client = SecClient(ingest_cfg)
                    download_filing_package(ingest_cfg, sec_client, cik_int, filing)
                    if presence.status == "present_incomplete":
                        summary.filings_refreshed += 1
                    else:
                        summary.filings_added += 1
                except Exception as exc:
                    summary.filings_failed += 1
                    summary.add_event(
                        family=str(filing.get("form") or ""),
                        status="failed",
                        origin="sec_filing",
                        quarter=str(filing.get("reportDate") or ""),
                        reason=f"{type(exc).__name__}: {exc}",
                    )
                    continue

        if str(filing.get("base_form") or filing.get("form") or "").upper().split("/")[0] != "8-K":
            continue
        for cand in _collect_sec_material_candidates(cache_dir, cik_int, filing):
            qkey = cand.quarter.isoformat() if cand.quarter else ""
            if cand.quarter_assignment_status == "matched_quarter_end" and quarter_targets and qkey not in quarter_targets:
                continue
            selected_manifest_keys.add(_manifest_key(cand))
            res = _materialize_candidate(
                repo_root=repo_root,
                ticker=ticker,
                manifest=manifest,
                candidate=cand,
                dry_run=dry_run,
            )
            _apply_material_event(summary, res)
            if cand.quarter_assignment_status == "matched_quarter_end" and qkey:
                quarter_family_sources[(qkey, cand.canonical_family)] = "sec_exhibit"

    ir_session = _build_ir_session(user_agent)
    ir_result = _discover_official_ir_candidates(
        profile=profile,
        quarter_targets=quarter_targets,
        session=ir_session,
    )
    for cand in ir_result.candidates:
        if cand.quarter is None:
            continue
        qkey = cand.quarter.isoformat()
        if quarter_targets and qkey not in quarter_targets:
            continue
        if quarter_family_sources.get((qkey, cand.canonical_family)) == "sec_exhibit":
            summary.material_skipped += 1
            summary.add_event(
                family=cand.canonical_family,
                status="skipped",
                origin=cand.origin,
                quarter=qkey,
                source_url=cand.source_url,
                reason="sec exhibit already present for quarter/family",
                title=cand.title or cand.source_doc_title,
            )
            continue
        selected_manifest_keys.add(_manifest_key(cand))
        res = _materialize_candidate(
            repo_root=repo_root,
            ticker=ticker,
            manifest=manifest,
            candidate=cand,
            dry_run=dry_run,
            download_session=ir_session,
        )
        if res.status == "added":
            quarter_family_sources[(qkey, cand.canonical_family)] = "official_ir"
        _apply_material_event(summary, res)

    for cand in local_scan.candidates:
        selected_manifest_keys.add(_manifest_key(cand))
        qkey = cand.quarter.isoformat() if cand.quarter else ""
        if cand.quarter is not None and (qkey, cand.canonical_family) in quarter_family_sources:
            continue
        if cand.canonical_family == "earnings_transcripts":
            summary.manual_transcript_fallbacks += 1
        event = _upsert_manual_local_candidate(
            manifest=manifest,
            candidate=cand,
            ticker=ticker,
            dry_run=dry_run,
        )
        summary.events.append(event)

    if not dry_run:
        _prune_stale_manifest_entries(manifest, selected_keys=selected_manifest_keys, material_root=material_root)
        _save_manifest(manifest_path, manifest)
    coverage_report = _build_coverage_report(
        ticker=ticker,
        manifest=manifest,
        filings_df=filings_df,
        max_quarters=16,
        ir_diagnostics=ir_result.diagnostics,
        local_scan=local_scan,
    )
    summary.coverage_lines = _format_coverage_lines(coverage_report)
    if not dry_run:
        coverage_path = cache_dir / COVERAGE_REPORT_RELATIVE_PATH
        coverage_path.parent.mkdir(parents=True, exist_ok=True)
        coverage_path.write_text(json.dumps(coverage_report, indent=2), encoding="utf-8")
        summary.coverage_report_path = str(coverage_path)
    return summary


def _apply_material_event(summary: SourceMaterialRefreshSummary, event: MaterialEvent) -> None:
    if event.status == "added":
        summary.material_added += 1
    elif event.status == "failed":
        summary.material_failed += 1
    else:
        summary.material_skipped += 1
    summary.events.append(event)


def _list_recent_filings_with_legacy_support(
    cfg: IngestConfig,
    *,
    ticker: Optional[str] = None,
    cik: Optional[int] = None,
) -> Tuple[int, Any, Path]:
    sec = SecClient(cfg)
    cik_int = int(cik) if cik is not None else ticker_to_cik(sec, ticker or "")
    cik10 = cik10_from_int(cik_int)
    nested_path = cfg.cache_dir / cik10 / "submissions.json"
    flat_candidates = [
        cfg.cache_dir / f"submissions_{cik10}.json",
        cfg.cache_dir / f"submissions_CIK{cik10}-submissions-001.json",
    ]
    sub_path = nested_path if nested_path.exists() else next((p for p in flat_candidates if p.exists()), nested_path)
    if sub_path.exists():
        submissions = json.loads(sub_path.read_text(encoding="utf-8"))
    else:
        url = f"{EDGAR_BASE}/submissions/CIK{cik10}.json"
        submissions = sec.get(url, as_json=True)
        nested_path.parent.mkdir(parents=True, exist_ok=True)
        _write_json_file(nested_path, submissions)
        sub_path = nested_path
    filings_df = _filings_df(submissions, cfg.forms)
    if cfg.max_filings:
        filings_df = filings_df.head(cfg.max_filings).copy()
    return cik_int, filings_df, sub_path


def _detect_filing_package_presence(cache_dir: Path, cik_int: int, filing: Dict[str, Any]) -> FilingPackagePresence:
    accn = normalize_accession(str(filing.get("accession") or ""))
    if not accn:
        return FilingPackagePresence(status="missing", index_path=None, primary_path=None)
    cik10 = cik10_from_int(cik_int)
    primary_doc = str(filing.get("primaryDoc") or "").strip()
    nested_root = cache_dir / cik10 / accn
    nested_index = nested_root / "index.json"
    nested_docs_dir = nested_root / "docs"
    flat_index = cache_dir / f"index_{accn}.json"
    flat_docs = sorted(cache_dir.glob(f"doc_{accn}_*"))
    nested_doc_paths = sorted(nested_docs_dir.glob("*")) if nested_docs_dir.exists() else []
    primary_path = _resolve_primary_doc_path(primary_doc, nested_doc_paths, flat_docs)
    has_index = nested_index.exists() or flat_index.exists()
    status = (
        "present_complete"
        if has_index and primary_path is not None
        else "present_incomplete"
        if has_index or primary_path is not None or bool(nested_doc_paths) or bool(flat_docs)
        else "missing"
    )
    index_path = nested_index if nested_index.exists() else flat_index if flat_index.exists() else None
    return FilingPackagePresence(
        status=status,
        index_path=index_path,
        primary_path=primary_path,
        candidate_paths=tuple(list(nested_doc_paths) + list(flat_docs)),
    )


def _resolve_primary_doc_path(primary_doc: str, nested_docs: Sequence[Path], flat_docs: Sequence[Path]) -> Optional[Path]:
    primary_low = primary_doc.lower()
    if primary_low:
        for path_in in list(nested_docs) + list(flat_docs):
            nm = path_in.name.lower()
            if nm == primary_low or nm.endswith(f"_{primary_low}") or nm.endswith(primary_low):
                return path_in
    for path_in in nested_docs:
        if path_in.is_file():
            return path_in
    for path_in in flat_docs:
        if path_in.is_file():
            return path_in
    return None


def _quarter_targets_from_filings(filings_df: Any) -> set[str]:
    out: set[str] = set()
    if filings_df is None or getattr(filings_df, "empty", True):
        return out
    for row in filings_df.to_dict("records"):
        base_form = str(row.get("base_form") or row.get("form") or "").upper().split("/")[0]
        if base_form not in {"10-Q", "10-K"}:
            continue
        qd = _parse_quarter_date(row.get("reportDate"))
        if qd is not None:
            out.add(qd.isoformat())
    return out


def _parse_quarter_date(value: Any) -> Optional[date]:
    try:
        if value in (None, "", "NaT"):
            return None
        return date.fromisoformat(str(value)[:10])
    except Exception:
        return None


def _collect_sec_material_candidates(cache_dir: Path, cik_int: int, filing: Dict[str, Any]) -> List[MaterialCandidate]:
    accn = normalize_accession(str(filing.get("accession") or ""))
    if not accn:
        return []
    _index_path, index_json = _load_index_json(cache_dir, cik_int, accn)
    if index_json is None:
        return []
    items = list(index_json.get("directory", {}).get("item", []) or [])
    flat_docs = {p.name.lower(): p for p in cache_dir.glob(f"doc_{accn}_*")}
    nested_root = cache_dir / cik10_from_int(cik_int) / accn
    exhibit_dir = nested_root / "exhibits"
    docs_dir = nested_root / "docs"
    ex99_names = {str(name or "").strip() for name in find_ex99_docs(index_json)}
    primary_doc = str(filing.get("primaryDoc") or "").strip()
    primary_path = _resolve_primary_doc_path(primary_doc, sorted(docs_dir.glob("*")) if docs_dir.exists() else [], tuple(flat_docs.values()))
    primary_excerpt = _load_material_text_excerpt(primary_path) if primary_path is not None else ""
    filing_is_earnings_relevant = _is_earnings_relevant_8k(primary_excerpt)
    default_q = _parse_quarter_date(filing.get("reportDate"))
    candidates: List[MaterialCandidate] = []
    for item in items:
        nm = str(item.get("name") or "").strip()
        if not nm:
            continue
        sec_type = str(item.get("type") or "").strip().upper()
        if nm not in ex99_names and not sec_type.startswith("EX-99"):
            continue
        local_path: Optional[Path] = None
        for cand in (exhibit_dir / nm, docs_dir / nm):
            if cand.exists():
                local_path = cand
                break
        if local_path is None:
            for flat_name, flat_path in flat_docs.items():
                if flat_name.endswith(f"_{nm.lower()}") or flat_name == nm.lower():
                    local_path = flat_path
                    break
        if local_path is None or not local_path.exists() or _is_decorative_asset(local_path):
            continue
        title = _source_title_from_local_doc(local_path)
        text_excerpt = _load_material_text_excerpt(local_path)
        source_url = _sec_item_url(cik_int, accn, nm)
        family = _classify_material_family(
            nm=nm,
            title=title,
            sec_type=sec_type,
            seed_family_hint="",
            text_excerpt=text_excerpt,
            default_q=default_q,
            filing_is_earnings_relevant=filing_is_earnings_relevant,
            source_url=source_url,
        )
        if family is None:
            continue
        if family in {"earnings_release", "earnings_presentation", "earnings_transcripts"}:
            strong_family_signals = _has_family_specific_markers(family, f"{nm} {title} {text_excerpt}")
            if not filing_is_earnings_relevant and not strong_family_signals:
                continue
        assignment = _assign_quarter_from_source(
            title=title,
            source_name=nm,
            source_url=source_url,
            text_excerpt=text_excerpt,
            default_q=default_q,
            allow_non_quarter_default=(family == "press_release"),
            canonical_family=family,
        )
        candidates.append(
            MaterialCandidate(
                canonical_family=family,
                quarter=assignment.quarter,
                local_path=local_path,
                source_url=source_url,
                title=title or nm,
                origin="sec_exhibit",
                accession=str(filing.get("accession") or ""),
                form=str(filing.get("form") or ""),
                report_date=str(filing.get("reportDate") or ""),
                filed_date=str(filing.get("filedDate") or ""),
                exhibit_type=sec_type or _infer_exhibit_type_from_name(nm),
                selection_reason="SEC exhibit classification with earnings-context filter",
                source_doc_title=title or nm,
                quarter_assignment_status=assignment.status,
                quarter_assignment_reason=assignment.reason,
            )
        )
    return candidates


def _load_index_json(cache_dir: Path, cik_int: int, accn_nd: str) -> Tuple[Optional[Path], Optional[Dict[str, Any]]]:
    cik10 = cik10_from_int(cik_int)
    nested_index = cache_dir / cik10 / accn_nd / "index.json"
    flat_index = cache_dir / f"index_{accn_nd}.json"
    for path_in in (nested_index, flat_index):
        if not path_in.exists():
            continue
        try:
            return path_in, json.loads(path_in.read_text(encoding="utf-8"))
        except Exception:
            continue
    return None, None


def _source_title_from_local_doc(path_in: Path) -> str:
    try:
        if path_in.suffix.lower() in {".htm", ".html"}:
            raw = path_in.read_text(encoding="utf-8", errors="ignore")
            if BeautifulSoup is not None:
                soup = BeautifulSoup(raw, "html.parser")
                if soup.title and str(soup.title.text or "").strip():
                    title_text = str(soup.title.text).strip()
                    if not _is_generic_doc_title(title_text):
                        return title_text
                h1 = soup.find("h1")
                if h1 and str(h1.get_text(" ", strip=True) or "").strip():
                    return str(h1.get_text(" ", strip=True)).strip()
            text = strip_html(raw)
            for line in str(text or "").strip().splitlines():
                line_clean = str(line or "").strip()
                if line_clean and not _is_generic_doc_title(line_clean):
                    return line_clean
            return path_in.stem
        if path_in.suffix.lower() == ".pdf":
            pdf_txt = extract_pdf_text_cached(path_in, cache_root=None, quiet_pdf_warnings=True)
            for line in str(pdf_txt or "").splitlines():
                line_clean = re.sub(r"\s+", " ", str(line or "").strip())
                if not line_clean:
                    continue
                if line_clean.lower() in {"refinitiv streetevents", "thomson reuters streetevents", "edited transcript"}:
                    continue
                if _is_generic_doc_title(line_clean):
                    continue
                return line_clean
        return path_in.stem
    except Exception:
        return path_in.stem


def _classify_material_family(
    *,
    nm: str,
    title: str,
    sec_type: str,
    seed_family_hint: str,
    text_excerpt: str = "",
    default_q: Optional[date] = None,
    filing_is_earnings_relevant: bool = False,
    source_url: str = "",
) -> Optional[str]:
    core_blob = _normalize_material_blob(nm, title, sec_type, source_url=source_url)
    rich_blob = _normalize_material_blob(nm, title, sec_type, text_excerpt=text_excerpt, source_url=source_url)
    explicit_release = _has_explicit_earnings_release_markers(core_blob) or _has_results_quarter_markers(core_blob)
    if _has_transcript_markers(core_blob):
        return "earnings_transcripts"
    if _is_low_value_non_source_blob(core_blob):
        return None
    if _looks_transaction_only_exhibit(rich_blob):
        return None
    if seed_family_hint == "earnings_presentation" and _has_presentation_markers(rich_blob) and not _looks_non_results_press_release(rich_blob):
        return "earnings_presentation"
    if _looks_non_results_press_release(rich_blob):
        return "press_release"
    if _looks_letter_style_doc(core_blob):
        if filing_is_earnings_relevant and (explicit_release or _has_results_markers(rich_blob) or _is_quarter_end_date(default_q)):
            return "earnings_release"
        return "press_release"
    if explicit_release or _has_results_quarter_markers(rich_blob):
        return "earnings_release"
    if re.search(r"(press release|news release)", core_blob):
        if filing_is_earnings_relevant and (_has_results_quarter_markers(rich_blob) or _has_explicit_earnings_release_markers(rich_blob)):
            return "earnings_release"
        return "press_release"
    if _has_presentation_markers(core_blob):
        return "earnings_presentation"
    if filing_is_earnings_relevant and _has_presentation_markers(rich_blob) and not explicit_release and not _looks_non_results_press_release(rich_blob):
        return "earnings_presentation"
    if filing_is_earnings_relevant and _has_results_markers(rich_blob) and _is_quarter_end_date(default_q):
        return "earnings_release"
    if seed_family_hint == "earnings_transcripts" and _has_transcript_markers(rich_blob):
        return "earnings_transcripts"
    return None


def _load_material_text_excerpt(path_in: Optional[Path], *, max_chars: int = 20000) -> str:
    if path_in is None or not path_in.exists():
        return ""
    try:
        ext = path_in.suffix.lower()
        if ext in {".htm", ".html", ".txt"}:
            raw = path_in.read_text(encoding="utf-8", errors="ignore")
            text = strip_html(raw) if ext in {".htm", ".html"} else raw
            return re.sub(r"\s+", " ", text).strip()[:max_chars]
        if ext == ".pdf":
            text = extract_pdf_text_cached(path_in, cache_root=None, quiet_pdf_warnings=True)
            return re.sub(r"\s+", " ", str(text or "")).strip()[:max_chars]
    except Exception:
        return ""
    return ""


def _has_strong_earnings_markers(text: str) -> bool:
    blob = str(text or "").lower()
    patterns = (
        r"\bearnings release\b",
        r"\bfinancial results\b",
        r"\bquarterly results\b",
        r"\bquarter results\b",
        r"\bfull year results\b",
        r"\bfourth quarter and full year\b",
        r"\bresults for the (?:first|second|third|fourth) quarter\b",
        r"\bannounces .* financial results\b",
        r"\bresults of operations and financial condition\b",
        r"\bquarter ended\b",
    )
    return any(re.search(pat, blob) for pat in patterns)


def _normalize_material_blob(*parts: str, text_excerpt: str = "", source_url: str = "") -> str:
    blob = " ".join(str(part or "") for part in parts if str(part or "").strip())
    if text_excerpt:
        blob = f"{blob} {text_excerpt}"
    if source_url:
        blob = f"{blob} {Path(urlparse(source_url).path).name}"
    return re.sub(r"\s+", " ", blob).strip().lower()


def _is_generic_doc_title(text: str) -> bool:
    blob = re.sub(r"\s+", " ", str(text or "")).strip().lower()
    if blob in {"document", "edgarfiling", "edgar filing", "ex-99.1", "ex-99.2", "ex-99.3", "ex 99.1", "ex 99.2", "ex 99.3", "exhibit"}:
        return True
    if re.fullmatch(r"\d{1,2}", blob):
        return True
    if re.fullmatch(r"ex(?:hibit)?[ .-]*99(?:\.\d+)?", blob):
        return True
    return False


def _has_results_markers(text: str) -> bool:
    blob = str(text or "").lower()
    if _has_strong_earnings_markers(blob):
        return True
    return bool(re.search(r"\b(earnings|financial results|quarterly results|full year)\b", blob))


def _has_explicit_earnings_release_markers(text: str) -> bool:
    blob = str(text or "").lower()
    return bool(
        re.search(
            r"(earnings press relea|earnings release|earningsrelease|financial results|quarterly results|annual results|results release|q[1-4].*earnings)",
            blob,
        )
    )


def _has_period_markers(text: str) -> bool:
    blob = str(text or "").lower()
    if re.search(r"\bq[1-4]\s*[- ]?\s*20\d{2}\b", blob):
        return True
    if re.search(r"\b(first|second|third|fourth)\s+quarter\b", blob):
        return True
    if re.search(r"\b(full year|fy\s*[- ]?20\d{2}|fy20\d{2}|year ended|quarter ended)\b", blob):
        return True
    return False


def _has_results_quarter_markers(text: str) -> bool:
    return _has_results_markers(text) and _has_period_markers(text)


def _has_presentation_markers(text: str) -> bool:
    blob = str(text or "").lower()
    return bool(
        re.search(
            r"(investor presentation|earnings presentation|results presentation|quarterly presentation|presentation|slides|slide deck|deck|supplemental presentation|earnings supplement|webcast presentation|supplement)",
            blob,
        )
    )


def _has_transcript_markers(text: str) -> bool:
    return bool(re.search(r"(conference call transcript|earnings call transcript|prepared remarks|transcript)", str(text or "").lower()))


def _looks_letter_style_doc(text: str) -> bool:
    return bool(re.search(r"(shareholder letter|stockholder letter|ceo letter|investor letter|annual letter)", str(text or "").lower()))


def _looks_non_results_press_release(text: str) -> bool:
    blob = str(text or "").lower()
    if _has_explicit_earnings_release_markers(blob) or _has_results_quarter_markers(blob):
        return False
    return bool(
        re.search(
            r"(chair change|board changes|board committee|retirement|appoint|appointment|offering|tender offer|tender|separation agreement|transaction|sale agreement|governance|employment agreement|indenture|tax credit purchase|director transition|director appointment|chief financial officer transition|chief financial officer appointment|cfo transition|cfo appointment|chairman transition|chairperson transition)",
            blob,
        )
    )


def _looks_transaction_only_exhibit(text: str) -> bool:
    blob = str(text or "").lower()
    if re.search(r"\b(unaudited pro forma|pro forma financial|separation agreement|sale agreement|employment agreement|indenture|tax credit purchase agreement)\b", blob):
        return True
    return False


def _is_low_value_non_source_blob(text: str) -> bool:
    blob = str(text or "").lower().strip()
    if not blob:
        return True
    if blob in {"document", "edgarfiling", "ex-99.1", "ex-99.2", "ex-99.3"}:
        return True
    return False


def _has_family_specific_markers(family: str, text: str) -> bool:
    blob = str(text or "").lower()
    if family == "earnings_release":
        return _has_results_quarter_markers(blob)
    if family == "earnings_presentation":
        return _has_presentation_markers(blob)
    if family == "earnings_transcripts":
        return _has_transcript_markers(blob)
    return False


def _is_earnings_relevant_8k(primary_excerpt: str) -> bool:
    blob = str(primary_excerpt or "").lower()
    if not blob:
        return False
    if re.search(r"\bitem\s+2\.02\b", blob):
        return True
    if _has_results_quarter_markers(blob):
        return True
    if re.search(r"\b(results of operations and financial condition|earnings release|quarterly results|investor presentation)\b", blob):
        return True
    return False


def _infer_exhibit_type_from_name(name: str) -> str:
    m = re.search(r"(ex[-_ ]?\d{1,3}(?:\.\d+)?)", str(name or ""), re.I)
    return str(m.group(1) or "").upper().replace("_", "-").replace(" ", "") if m else ""


def _sec_item_url(cik_int: int, accn_nd: str, item_name: str) -> str:
    return f"{EDGAR_BASE}/Archives/edgar/data/{cik_int}/{accn_nd}/{item_name}"


def _is_quarter_end_date(qd: Optional[date]) -> bool:
    return bool(qd and (qd.month, qd.day) in {(3, 31), (6, 30), (9, 30), (12, 31)})


def _coerce_quarter_end(qd: Optional[date]) -> Optional[date]:
    if not qd:
        return None
    month_day = {(1, 2, 3): (3, 31), (4, 5, 6): (6, 30), (7, 8, 9): (9, 30), (10, 11, 12): (12, 31)}
    for months, md in month_day.items():
        if qd.month in months:
            return date(qd.year, md[0], md[1])
    return qd


def _infer_source_quarter(path_in: Path, *, title: str, default_q: Optional[date]) -> Optional[date]:
    assignment = _assign_quarter_from_source(
        title=title,
        source_name=path_in.name,
        source_url="",
        text_excerpt=_load_material_text_excerpt(path_in, max_chars=12000),
        default_q=default_q,
        allow_non_quarter_default=False,
        canonical_family="",
    )
    return assignment.quarter


def _assign_quarter_from_source(
    *,
    title: str,
    source_name: str,
    source_url: str,
    text_excerpt: str,
    default_q: Optional[date],
    allow_non_quarter_default: bool,
    canonical_family: str,
) -> QuarterAssignment:
    title_q = _infer_quarter_signal_from_text(title or "")
    if title_q is not None and _is_quarter_end_date(title_q):
        return QuarterAssignment(quarter=title_q, status="matched_quarter_end", reason="title_quarter_signal")
    source_q = _infer_quarter_signal_from_text(source_url or "")
    if source_q is not None and _is_quarter_end_date(source_q):
        return QuarterAssignment(quarter=source_q, status="matched_quarter_end", reason="source_url_quarter_signal")
    for label, candidate in (
        ("filename", infer_q_from_name(source_name)),
        ("title", infer_quarter_end_from_text(title or "")),
    ):
        qd = _coerce_quarter_end(candidate)
        if qd is not None and _is_quarter_end_date(qd):
            return QuarterAssignment(quarter=qd, status="matched_quarter_end", reason=f"{label}_quarter_signal")
    body_q = _infer_quarter_signal_from_text(str(text_excerpt or "")[:12000])
    if body_q is None:
        body_q = _coerce_quarter_end(infer_quarter_end_from_text(str(text_excerpt or "")[:12000]))
    if body_q is not None and _is_quarter_end_date(body_q) and _allow_body_quarter_match(canonical_family=canonical_family, title=title, source_name=source_name, source_url=source_url, text_excerpt=text_excerpt):
        return QuarterAssignment(quarter=body_q, status="matched_quarter_end", reason="body_quarter_signal")
    if default_q is not None and _is_quarter_end_date(default_q):
        return QuarterAssignment(quarter=default_q, status="matched_quarter_end", reason="quarter_end_report_date_fallback")
    if allow_non_quarter_default and default_q is not None:
        return QuarterAssignment(quarter=default_q, status="non_quarter_event", reason="non_quarter_report_date")
    return QuarterAssignment(quarter=None, status="unknown", reason="no_quarter_signal")


def _quarter_end_from_parts(qnum: int, year: int) -> Optional[date]:
    if qnum not in {1, 2, 3, 4}:
        return None
    if qnum == 1:
        return date(year, 3, 31)
    if qnum == 2:
        return date(year, 6, 30)
    if qnum == 3:
        return date(year, 9, 30)
    return date(year, 12, 31)


def _infer_quarter_signal_from_text(text: str) -> Optional[date]:
    blob = re.sub(r"\s+", " ", str(text or "")).strip()
    if not blob:
        return None
    inferred = _coerce_quarter_end(infer_quarter_end_from_text(blob))
    if inferred is not None:
        return inferred
    ordinal_map = {
        "first": 1,
        "1st": 1,
        "q1": 1,
        "second": 2,
        "2nd": 2,
        "q2": 2,
        "third": 3,
        "3rd": 3,
        "q3": 3,
        "fourth": 4,
        "4th": 4,
        "q4": 4,
    }
    m = re.search(
        r"\b(first|1st|q1|second|2nd|q2|third|3rd|q3|fourth|4th|q4)\s+quarter(?:\s+of)?(?:\s+fiscal\s+year|\s+fiscal)?\s+(20\d{2})\b",
        blob,
        re.I,
    )
    if m:
        qnum = ordinal_map.get(str(m.group(1) or "").strip().lower(), 0)
        year = int(m.group(2))
        return _quarter_end_from_parts(qnum, year)
    m = re.search(r"\bq([1-4])(?:\s+of)?(?:\s+fiscal\s+year|\s+fiscal)?\s*(20\d{2})\b", blob, re.I)
    if m:
        return _quarter_end_from_parts(int(m.group(1)), int(m.group(2)))
    m = re.search(r"\b([1-4])q(?:\s+of)?(?:\s+fiscal\s+year|\s+fiscal)?\s*(20\d{2})\b", blob, re.I)
    if m:
        return _quarter_end_from_parts(int(m.group(1)), int(m.group(2)))
    m = re.search(r"\b(20\d{2})\s*[-_ ]*([1-4])q\b", blob, re.I)
    if m:
        return _quarter_end_from_parts(int(m.group(2)), int(m.group(1)))
    m = re.search(
        r"\b(first|1st|second|2nd|third|3rd|fourth|4th)\s+quarter\b.{0,40}?\b(20\d{2})\b",
        blob,
        re.I,
    )
    if m:
        qnum = ordinal_map.get(str(m.group(1) or "").strip().lower(), 0)
        year = int(m.group(2))
        return _quarter_end_from_parts(qnum, year)
    m = re.search(r"\bq([1-4])\b.{0,24}?\b(20\d{2})\b", blob, re.I)
    if m:
        return _quarter_end_from_parts(int(m.group(1)), int(m.group(2)))
    m = re.search(r"\bq([1-4])\s*[-_ ]*([0-9]{2})\b", blob, re.I)
    if m:
        return _quarter_end_from_parts(int(m.group(1)), 2000 + int(m.group(2)))
    m = re.search(r"\b([1-4])q\s*[-_ ]*([0-9]{2})\b", blob, re.I)
    if m:
        return _quarter_end_from_parts(int(m.group(1)), 2000 + int(m.group(2)))
    m = re.search(r"\b(?:full\s+year|fy\s*[- ]?)(20\d{2})\b", blob, re.I)
    if m:
        return date(int(m.group(1)), 12, 31)
    return None


def _allow_body_quarter_match(
    *,
    canonical_family: str,
    title: str,
    source_name: str,
    source_url: str,
    text_excerpt: str,
) -> bool:
    if canonical_family != "press_release":
        return True
    title_blob = _normalize_material_blob(title, source_name, source_url=source_url)
    if _has_results_quarter_markers(title_blob) or _has_explicit_earnings_release_markers(title_blob):
        return True
    return False


def _record_sec_family_source(source_map: Dict[Tuple[str, str], str], quarter_iso: str, family: str) -> bool:
    key = (quarter_iso, family)
    if source_map.get(key) == "sec_exhibit":
        return False
    source_map[key] = "sec_exhibit"
    return True


def _materialize_candidate(
    *,
    repo_root: Path,
    ticker: str,
    manifest: Dict[str, Dict[str, Any]],
    candidate: MaterialCandidate,
    dry_run: bool,
    download_session: Optional[requests.Session] = None,
) -> MaterialEvent:
    resolved_dir = _resolved_destination_dir(repo_root, ticker, candidate.canonical_family)
    quarter_iso = candidate.quarter.isoformat() if candidate.quarter else ""
    source_doc_title = candidate.source_doc_title or candidate.title or Path(str(candidate.source_url or "")).name or "source_material"
    ext = _candidate_extension(candidate)
    dst_path = resolved_dir / _destination_name(candidate, ext=ext)
    manifest_key = _manifest_key(candidate)
    existing = manifest.get(manifest_key)
    if existing:
        existing_dst = Path(str(existing.get("destination_path") or ""))
        if existing_dst.exists() and existing_dst.is_file() and existing_dst.stat().st_size > 0:
            final_path, reconcile_reason = _reconcile_existing_destination(existing_dst, dst_path, dry_run=dry_run)
            manifest[manifest_key] = _manifest_entry(
                candidate,
                resolved_dir,
                final_path,
                sha256=str(existing.get("sha256") or ""),
                status="ok" if not dry_run else "dry_run",
            )
            return MaterialEvent(ticker=ticker, family=candidate.canonical_family, status="skipped", origin=candidate.origin, quarter=quarter_iso, source_url=candidate.source_url, destination_path=str(final_path), reason=reconcile_reason or "manifest key already present with non-empty file", title=source_doc_title)
        if dst_path.exists() and dst_path.is_file() and dst_path.stat().st_size > 0:
            manifest[manifest_key] = _manifest_entry(candidate, resolved_dir, dst_path, sha256=_sha256_path(dst_path) if not dry_run else str(existing.get("sha256") or ""), status="ok" if not dry_run else "dry_run")
            return MaterialEvent(ticker=ticker, family=candidate.canonical_family, status="skipped", origin=candidate.origin, quarter=quarter_iso, source_url=candidate.source_url, destination_path=str(dst_path), reason="manifest updated to normalized destination", title=source_doc_title)
    if dst_path.exists() and dst_path.stat().st_size > 0:
        manifest[manifest_key] = _manifest_entry(candidate, resolved_dir, dst_path)
        return MaterialEvent(ticker=ticker, family=candidate.canonical_family, status="skipped", origin=candidate.origin, quarter=quarter_iso, source_url=candidate.source_url, destination_path=str(dst_path), reason="destination file already exists", title=source_doc_title)
    if dry_run:
        manifest.setdefault(manifest_key, _manifest_entry(candidate, resolved_dir, dst_path, status="dry_run"))
        return MaterialEvent(ticker=ticker, family=candidate.canonical_family, status="added", origin=candidate.origin, quarter=quarter_iso, source_url=candidate.source_url, destination_path=str(dst_path), reason="dry-run planned add", title=source_doc_title)
    try:
        resolved_dir.mkdir(parents=True, exist_ok=True)
        if candidate.local_path is not None and candidate.local_path.exists():
            _hardlink_or_copy(candidate.local_path, dst_path)
        else:
            if download_session is None:
                raise RuntimeError("download session required for remote material")
            resp = download_session.get(candidate.source_url, timeout=30)
            resp.raise_for_status()
            dst_path.write_bytes(resp.content)
        if dst_path.stat().st_size <= 0:
            raise RuntimeError("downloaded or materialized file is empty")
        manifest[manifest_key] = _manifest_entry(candidate, resolved_dir, dst_path, sha256=_sha256_path(dst_path))
        return MaterialEvent(ticker=ticker, family=candidate.canonical_family, status="added", origin=candidate.origin, quarter=quarter_iso, source_url=candidate.source_url, destination_path=str(dst_path), reason=candidate.selection_reason, title=source_doc_title)
    except Exception as exc:
        return MaterialEvent(ticker=ticker, family=candidate.canonical_family, status="failed", origin=candidate.origin, quarter=quarter_iso, source_url=candidate.source_url, destination_path=str(dst_path), reason=f"{type(exc).__name__}: {exc}", title=source_doc_title)


def _manifest_entry(candidate: MaterialCandidate, resolved_dir: Path, dst_path: Path, *, status: str = "ok", sha256: str = "") -> Dict[str, Any]:
    return {
        "canonical_family": candidate.canonical_family,
        "resolved_destination_dir": str(resolved_dir),
        "origin": candidate.origin,
        "accession": candidate.accession,
        "form": candidate.form,
        "report_date": candidate.report_date,
        "filed_date": candidate.filed_date,
        "quarter": candidate.quarter.isoformat() if candidate.quarter else "",
        "quarter_assignment_status": candidate.quarter_assignment_status,
        "quarter_assignment_reason": candidate.quarter_assignment_reason,
        "source_url": candidate.source_url,
        "exhibit_type": candidate.exhibit_type,
        "source_doc_title": candidate.source_doc_title or candidate.title,
        "subject_slug": candidate.subject_slug or _subject_slug(candidate),
        "destination_path": str(dst_path),
        "sha256": sha256,
        "status": status,
        "selection_reason": candidate.selection_reason,
    }


def _candidate_extension(candidate: MaterialCandidate) -> str:
    if candidate.local_path is not None:
        ext = candidate.local_path.suffix.lower()
        if ext:
            return ext
    ext = Path(urlparse(candidate.source_url or "").path).suffix.lower()
    return ext or ".html"


def _destination_name(candidate: MaterialCandidate, *, ext: str) -> str:
    form_tok = re.sub(r"\s+", "", str(candidate.form or "8-K").upper()) or "8-K"
    date_tok = str(candidate.filed_date or candidate.report_date or "unknown")[:10]
    quarter_tok = _quarter_label(candidate.quarter)
    subject_slug = candidate.subject_slug or _subject_slug(candidate)
    bits = [form_tok, date_tok, subject_slug]
    if quarter_tok and quarter_tok not in subject_slug:
        bits.append(quarter_tok)
    out = "_".join(bit for bit in bits if bit).strip("_")
    return f"{out}{ext}"


def _quarter_label(qd: Optional[date]) -> str:
    if not _is_quarter_end_date(qd):
        return ""
    qnum = ((int(qd.month) - 1) // 3) + 1
    return f"q{qnum}_{int(qd.year)}"


def _subject_slug(candidate: MaterialCandidate) -> str:
    blob = _normalize_material_blob(candidate.title, candidate.source_doc_title, candidate.exhibit_type, source_url=candidate.source_url)
    if candidate.canonical_family == "earnings_presentation":
        return "earnings_presentation"
    if candidate.canonical_family == "earnings_transcripts":
        return "earnings_transcript"
    if candidate.canonical_family == "earnings_release":
        if re.search(r"(ceo letter|shareholder letter|stockholder letter|investor letter)", blob):
            return "ceo_letter"
        if re.search(r"(annual letter)", blob):
            return "annual_letter"
        return "earnings_release"
    topic = _clean_topic_slug(candidate)
    if topic:
        return f"press_release_{topic}"
    return "press_release"


def _clean_topic_slug(candidate: MaterialCandidate) -> str:
    source_name = ""
    if candidate.local_path is not None:
        source_name = candidate.local_path.name
    elif candidate.source_url:
        source_name = Path(urlparse(candidate.source_url).path).name
    else:
        source_name = candidate.title or candidate.source_doc_title
    slug = re.sub(r"(?i)\.(htm|html|pdf|txt)$", "", source_name)
    slug = re.sub(r"(?i)\b(document|edgarfiling|exhibit|ex|text|gif|r1|pressrelease|press_release|newsrelease|news_release|earningsrelease|earnings_release|q[1-4]\d{4}|fy\d{4}|20\d{2})\b", " ", slug)
    slug = re.sub(r"(?i)[^a-z0-9]+", "_", slug.lower()).strip("_")
    if slug:
        return slug[:80]
    blob = _normalize_material_blob(candidate.title, candidate.source_doc_title, source_url=candidate.source_url)
    patterns = (
        ("chair_change", r"(chair change|chairman|chairperson)"),
        ("cfo_change", r"(cfo|chief financial officer)"),
        ("board_changes", r"(board changes|director)"),
        ("retirement", r"(retirement)"),
        ("offering", r"(offering)"),
        ("tender_offer", r"(tender offer|tender)"),
        ("governance", r"(governance)"),
        ("transaction", r"(transaction|sale agreement|separation agreement)"),
        ("pricing", r"(pricing)"),
    )
    for topic, pattern in patterns:
        if re.search(pattern, blob):
            return topic
    return ""


def _reconcile_existing_destination(existing_dst: Path, desired_dst: Path, *, dry_run: bool) -> Tuple[Path, str]:
    if str(existing_dst).lower() == str(desired_dst).lower():
        return existing_dst, "manifest key already present with non-empty file"
    if desired_dst.exists() and desired_dst.is_file() and desired_dst.stat().st_size > 0:
        return desired_dst, "normalized destination already existed"
    if not existing_dst.exists():
        return desired_dst, "existing manifest destination missing; normalized destination will be used"
    if dry_run:
        return desired_dst, f"dry-run would rename existing file to normalized destination {desired_dst.name}"
    desired_dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        existing_dst.rename(desired_dst)
        return desired_dst, f"renamed existing file to normalized destination {desired_dst.name}"
    except Exception:
        return existing_dst, "kept existing destination because normalized rename was not safe"


def _manifest_key(candidate: MaterialCandidate) -> str:
    if candidate.accession and candidate.origin == "sec_exhibit":
        return f"sec_exhibit|{normalize_accession(candidate.accession)}|{candidate.exhibit_type}|{candidate.source_url}"
    if candidate.accession:
        return f"sec_filing|{normalize_accession(candidate.accession)}|{candidate.form}"
    if candidate.origin == "manual_local" and candidate.local_path is not None:
        return f"local|{candidate.canonical_family}|{_path_text(candidate.local_path)}"
    source_url = candidate.source_url.strip().lower()
    if source_url:
        return f"ir|{source_url}"
    return f"local|{candidate.canonical_family}|{candidate.title.strip().lower()}|{candidate.quarter.isoformat() if candidate.quarter else ''}"


def _ticker_material_root(repo_root: Path, ticker: str) -> Path:
    return repo_root / str(ticker or "").strip().upper()


def _resolved_destination_dir(repo_root: Path, ticker: str, canonical_family: str) -> Path:
    root = _ticker_material_root(repo_root, ticker)
    aliases = CANONICAL_FAMILY_ALIASES.get(canonical_family, (canonical_family,))
    for alias in aliases:
        cand = root / alias
        if cand.exists() and cand.is_dir():
            return cand
    return root / aliases[0]


def _normalize_and_collect_local_materials(
    *,
    repo_root: Path,
    ticker: str,
    manifest: Dict[str, Dict[str, Any]],
    dry_run: bool,
) -> LocalMaterialScanResult:
    result = LocalMaterialScanResult()
    material_root = _ticker_material_root(repo_root, ticker)
    official_paths = _official_manifest_destination_paths(manifest)
    _normalize_local_family_dirs(
        material_root=material_root,
        dry_run=dry_run,
        result=result,
    )
    _normalize_local_family_files(
        material_root=material_root,
        ticker=ticker,
        dry_run=dry_run,
        result=result,
        official_paths=official_paths,
    )
    result.candidates.extend(
        _collect_manual_local_candidates(
            material_root=material_root,
            ticker=ticker,
            official_paths=official_paths,
            result=result,
        )
    )
    return result


def _official_manifest_destination_paths(manifest: Dict[str, Dict[str, Any]]) -> set[str]:
    out: set[str] = set()
    for row in manifest.values():
        if str(row.get("origin") or "") not in {"sec_exhibit", "official_ir"}:
            continue
        dst = str(row.get("destination_path") or "").strip()
        if not dst:
            continue
        out.add(_path_text(dst))
    return out


def _path_text(path_in: Any) -> str:
    try:
        return str(Path(path_in).resolve()).lower()
    except Exception:
        return str(path_in or "").strip().lower()


def _normalize_local_family_dirs(
    *,
    material_root: Path,
    dry_run: bool,
    result: LocalMaterialScanResult,
) -> None:
    for canonical_name, aliases in LOCAL_MANUAL_FAMILY_ALIASES.items():
        canonical_dir = material_root / canonical_name
        for alias in aliases:
            alias_dir = material_root / alias
            if not alias_dir.exists() or not alias_dir.is_dir():
                continue
            if _path_text(alias_dir) == _path_text(canonical_dir):
                continue
            for path_in in sorted([p for p in alias_dir.iterdir() if p.is_file()]):
                dst = canonical_dir / path_in.name
                move_rec = _move_local_file(
                    source_path=path_in,
                    destination_path=dst,
                    canonical_family=canonical_name,
                    dry_run=dry_run,
                )
                if move_rec is None:
                    continue
                bucket = move_rec.pop("bucket", "")
                if bucket == "moved":
                    result.moved_files.append(move_rec)
                elif bucket == "duplicate":
                    result.duplicate_files.append(move_rec)


def _move_local_file(
    *,
    source_path: Path,
    destination_path: Path,
    canonical_family: str,
    dry_run: bool,
) -> Optional[Dict[str, Any]]:
    if not source_path.exists() or not source_path.is_file():
        return None
    if _path_text(source_path) == _path_text(destination_path):
        return None
    rec: Dict[str, Any] = {
        "canonical_family": canonical_family,
        "from_path": str(source_path),
        "to_path": str(destination_path),
        "title": source_path.name,
        "quarter": "",
    }
    if destination_path.exists():
        src_sha = _sha256_path(source_path)
        dst_sha = _sha256_path(destination_path)
        if src_sha == dst_sha:
            rec.update(
                {
                    "bucket": "duplicate",
                    "existing_path": str(destination_path),
                    "duplicate_path": str(source_path),
                    "sha256": src_sha,
                    "reason": "exact_duplicate_same_content",
                }
            )
            return rec
        qualified_dst = _collision_safe_path(destination_path, qualifier=_sanitized_slug(source_path.stem)[:24] or "moved")
        if qualified_dst.exists():
            qualified_dst = _collision_safe_path(destination_path, qualifier=f"alt_{_sha256_path(source_path)[:8]}")
        destination_path = qualified_dst
        rec["to_path"] = str(destination_path)
    if dry_run:
        rec["bucket"] = "moved"
        return rec
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.replace(destination_path)
    rec["bucket"] = "moved"
    return rec


def _normalize_local_family_files(
    *,
    material_root: Path,
    ticker: str,
    dry_run: bool,
    result: LocalMaterialScanResult,
    official_paths: set[str],
) -> None:
    for family_hint in LOCAL_MANUAL_FAMILY_ALIASES.keys():
        folder = material_root / family_hint
        if not folder.exists() or not folder.is_dir():
            continue
        for path_in in sorted([p for p in folder.iterdir() if p.is_file()]):
            if path_in.suffix.lower() not in LOCAL_SOURCE_EXTS:
                continue
            if _path_text(path_in) in official_paths:
                continue
            inspected = _inspect_manual_local_file(
                path_in=path_in,
                ticker=ticker,
                family_hint=family_hint,
            )
            if inspected.get("review_reason"):
                result.manual_review_files.append(
                    {
                        "path": str(path_in),
                        "family_hint": family_hint,
                        "reason": str(inspected.get("review_reason") or ""),
                        "title": str(inspected.get("title") or path_in.name),
                        "quarter": str((inspected.get("quarter") or "")),
                    }
                )
                continue
            desired_name = str(inspected.get("desired_name") or "").strip()
            if not desired_name:
                continue
            canonical_family = str(inspected.get("canonical_family") or family_hint)
            desired_dir = material_root / canonical_family if canonical_family in LOCAL_MANUAL_FAMILY_ALIASES else path_in.parent
            if str(inspected.get("subject_slug") or "") == "ceo_letter" and canonical_family == "earnings_release":
                desired_dir = material_root / "ceo_letters"
            dst = desired_dir / desired_name
            if _path_text(path_in) == _path_text(dst):
                continue
            rename_rec = _rename_local_file(
                source_path=path_in,
                destination_path=dst,
                canonical_family=canonical_family,
                dry_run=dry_run,
                title=str(inspected.get("title") or path_in.name),
                quarter=str((inspected.get("quarter") or "")),
            )
            if rename_rec is None:
                continue
            bucket = rename_rec.pop("bucket", "")
            if bucket == "renamed":
                result.renamed_files.append(rename_rec)
            elif bucket == "duplicate":
                result.duplicate_files.append(rename_rec)


def _rename_local_file(
    *,
    source_path: Path,
    destination_path: Path,
    canonical_family: str,
    dry_run: bool,
    title: str,
    quarter: str,
) -> Optional[Dict[str, Any]]:
    if _path_text(source_path) == _path_text(destination_path):
        return None
    rec: Dict[str, Any] = {
        "canonical_family": canonical_family,
        "from_path": str(source_path),
        "to_path": str(destination_path),
        "title": title,
        "quarter": quarter,
    }
    if destination_path.exists():
        src_sha = _sha256_path(source_path)
        dst_sha = _sha256_path(destination_path)
        if src_sha == dst_sha:
            rec.update(
                {
                    "bucket": "duplicate",
                    "existing_path": str(destination_path),
                    "duplicate_path": str(source_path),
                    "sha256": src_sha,
                    "reason": "exact_duplicate_same_content",
                }
            )
            return rec
        destination_path = _collision_safe_path(destination_path, qualifier=_collision_qualifier(source_path))
        rec["to_path"] = str(destination_path)
    if dry_run:
        rec["bucket"] = "renamed"
        return rec
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.replace(destination_path)
    rec["bucket"] = "renamed"
    return rec


def _collision_safe_path(path_in: Path, *, qualifier: str) -> Path:
    qualifier_txt = _sanitized_slug(qualifier) or "alt"
    stem = path_in.stem
    suffix = path_in.suffix
    candidate = path_in.with_name(f"{stem}_{qualifier_txt}{suffix}")
    if not candidate.exists():
        return candidate
    return path_in.with_name(f"{stem}_{qualifier_txt}_{hashlib.sha1(str(path_in).encode('utf-8')).hexdigest()[:8]}{suffix}")


def _collision_qualifier(path_in: Path) -> str:
    blob = _manual_name_hint(path_in).lower()
    for token in ("corrected", "final", "financial_schedules", "slides", "deck", "call", "conference", "prepared_remarks"):
        if token.replace("_", " ") in blob:
            return token
    return _sanitized_slug(path_in.stem)[:24] or "alt"


def _collect_manual_local_candidates(
    *,
    material_root: Path,
    ticker: str,
    official_paths: set[str],
    result: LocalMaterialScanResult,
) -> List[MaterialCandidate]:
    out: List[MaterialCandidate] = []
    seen_paths: set[str] = set()
    for family_hint in LOCAL_MANUAL_FAMILY_ALIASES.keys():
        folder = material_root / family_hint
        if not folder.exists() or not folder.is_dir():
            continue
        for path_in in sorted([p for p in folder.iterdir() if p.is_file()]):
            if path_in.suffix.lower() not in LOCAL_SOURCE_EXTS:
                continue
            path_key = _path_text(path_in)
            if path_key in seen_paths or path_key in official_paths:
                continue
            seen_paths.add(path_key)
            inspected = _inspect_manual_local_file(
                path_in=path_in,
                ticker=ticker,
                family_hint=family_hint,
            )
            if inspected.get("review_reason"):
                continue
            if str(inspected.get("canonical_family") or "") == "annual_reports":
                continue
            candidate = MaterialCandidate(
                canonical_family=str(inspected.get("canonical_family") or ""),
                quarter=inspected.get("quarter"),
                local_path=path_in,
                source_url="",
                title=str(inspected.get("stable_title") or inspected.get("title") or path_in.name),
                origin="manual_local",
                selection_reason="manual/local rescan",
                source_doc_title=str(inspected.get("title") or path_in.name),
                quarter_assignment_status=str(inspected.get("quarter_assignment_status") or ""),
                quarter_assignment_reason=str(inspected.get("quarter_assignment_reason") or ""),
                subject_slug=str(inspected.get("subject_slug") or ""),
            )
            out.append(candidate)
    return out


def _inspect_manual_local_file(
    *,
    path_in: Path,
    ticker: str,
    family_hint: str,
) -> Dict[str, Any]:
    title = _best_local_title(path_in)
    name_hint = _manual_name_hint(path_in)
    text_excerpt = _load_material_text_excerpt(path_in, max_chars=12000)
    blob = _normalize_material_blob(name_hint, title, text_excerpt=text_excerpt)
    core_blob = _normalize_material_blob(name_hint, title)
    title_blob = _normalize_material_blob(title)
    quarter = _infer_quarter_signal_from_text(name_hint) or _infer_quarter_signal_from_text(title)
    if quarter is None and text_excerpt:
        quarter = _infer_quarter_signal_from_text(str(text_excerpt)[:12000])
    if _is_annual_report_material(name_hint=name_hint, title=title, text_excerpt=text_excerpt):
        return _manual_local_inspection_payload(
            ticker=ticker,
            path_in=path_in,
            canonical_family="annual_reports",
            title=title,
            quarter=None,
            assignment=QuarterAssignment(quarter=None, status="unknown", reason="annual_report_reference"),
            subject_slug="annual_report",
        )
    if family_hint == "ceo_letters":
        if _has_transcript_markers(text_excerpt):
            assignment = _assign_quarter_from_source(
                title=title,
                source_name=name_hint,
                source_url="",
                text_excerpt=text_excerpt,
                default_q=quarter,
                allow_non_quarter_default=False,
                canonical_family="earnings_transcripts",
            )
            if assignment.quarter is None:
                return {"review_reason": "quarter_not_clear", "title": title}
            return _manual_local_inspection_payload(
                ticker=ticker,
                path_in=path_in,
                canonical_family="earnings_transcripts",
                title=title,
                quarter=assignment.quarter,
                assignment=assignment,
                subject_slug="",
            )
        if _has_explicit_earnings_release_markers(title_blob):
            pass
        elif not re.search(r"(ceo|shareholder|stockholder|investor letter)", title_blob):
            return {"review_reason": "ceo_letter_not_clear", "title": title}
        elif quarter is None:
            return {"review_reason": "quarter_not_clear", "title": title}
        else:
            assignment = QuarterAssignment(quarter=quarter, status="matched_quarter_end", reason="title_quarter_signal")
            return _manual_local_inspection_payload(
                ticker=ticker,
                path_in=path_in,
                canonical_family="earnings_release",
                title=title,
                quarter=quarter,
                assignment=assignment,
                subject_slug="ceo_letter",
            )
    if quarter is not None and re.search(r"(ceo letter|shareholder letter|stockholder letter|investor letter)", title_blob):
        assignment = _assign_quarter_from_source(
            title=title,
            source_name=name_hint,
            source_url="",
            text_excerpt=text_excerpt,
            default_q=quarter,
            allow_non_quarter_default=False,
            canonical_family="earnings_release",
        )
        if assignment.quarter is None:
            return {"review_reason": "quarter_not_clear", "title": title}
        return _manual_local_inspection_payload(
            ticker=ticker,
            path_in=path_in,
            canonical_family="earnings_release",
            title=title,
            quarter=assignment.quarter,
            assignment=assignment,
            subject_slug="ceo_letter",
        )
    family = _classify_material_family(
        nm=name_hint,
        title=title,
        sec_type="",
        seed_family_hint=family_hint if family_hint != "ceo_letters" else "",
        text_excerpt=text_excerpt,
        source_url="",
    )
    if family_hint in {"earnings_presentation", "annual_reports"}:
        if (
            quarter is not None
            and _has_presentation_markers(core_blob)
            and not _has_transcript_markers(blob)
            and not _looks_non_results_press_release(blob)
        ):
            family = "earnings_presentation"
    if family not in {"earnings_transcripts", "earnings_presentation"} and (_has_explicit_earnings_release_markers(title_blob) or _has_results_quarter_markers(title_blob)):
        family = "earnings_release"
    if family_hint == "earnings_presentation":
        if (
            quarter is not None
            and family in {None, "earnings_release"}
            and not _has_transcript_markers(blob)
            and not _looks_non_results_press_release(blob)
        ):
            family = "earnings_presentation"
    if family_hint == "earnings_transcripts" and family in {None, "earnings_release"}:
        if (
            quarter is not None
            and not re.search(r"(annual report|presentation|slides)", blob)
            and not re.search(r"(ceo letter|shareholder letter|stockholder letter|investor letter)", blob)
        ):
            family = "earnings_transcripts"
    if family_hint == "earnings_release" and family is None and quarter is not None and _has_results_markers(blob):
        family = "earnings_release"
    if family_hint == "press_release" and family is None:
        family = "press_release"
    if family is None:
        return {"review_reason": "family_not_clear", "title": title}
    assignment = _assign_quarter_from_source(
        title=title,
        source_name=name_hint,
        source_url="",
        text_excerpt=text_excerpt,
        default_q=quarter,
        allow_non_quarter_default=(family == "press_release"),
        canonical_family=family,
    )
    if family in RESULTS_FAMILY_SET and assignment.quarter is None:
        return {"review_reason": "quarter_not_clear", "title": title}
    return _manual_local_inspection_payload(
        ticker=ticker,
        path_in=path_in,
        canonical_family=family,
        title=title,
        quarter=assignment.quarter,
        assignment=assignment,
        subject_slug="ceo_letter" if family == "earnings_release" and re.search(r"(ceo letter|shareholder letter|stockholder letter|investor letter)", title_blob) else "",
    )


def _manual_local_inspection_payload(
    *,
    ticker: str,
    path_in: Path,
    canonical_family: str,
    title: str,
    quarter: Optional[date],
    assignment: QuarterAssignment,
    subject_slug: str,
) -> Dict[str, Any]:
    desired_name = _manual_local_destination_name(
        ticker=ticker,
        canonical_family=canonical_family,
        quarter=quarter,
        path_in=path_in,
        subject_slug=subject_slug,
    )
    stable_title = f"{ticker} {(_quarter_label_human(quarter) or '').strip()} {subject_slug or canonical_family}".strip()
    return {
        "canonical_family": canonical_family,
        "title": title,
        "stable_title": stable_title,
        "quarter": quarter,
        "quarter_assignment_status": assignment.status,
        "quarter_assignment_reason": assignment.reason,
        "desired_name": desired_name,
        "subject_slug": subject_slug,
        "review_reason": "",
    }


def _manual_local_destination_name(
    *,
    ticker: str,
    canonical_family: str,
    quarter: Optional[date],
    path_in: Path,
    subject_slug: str = "",
) -> str:
    ext = path_in.suffix.lower()
    ticker_txt = str(ticker or "").strip().upper()
    quarter_txt = _quarter_label_human(quarter)
    role_hint = _manual_local_role_hint(path_in)
    if canonical_family == "annual_reports":
        return f"{ticker_txt}_{_manual_local_year_hint(path_in)}_annual_report{ext}"
    if canonical_family == "press_release" and quarter is None:
        date_hint = _non_quarter_date_hint(path_in)
        topic = _collision_qualifier(path_in)
        return f"{ticker_txt}_{date_hint}_press_release_{topic}{ext}"
    if quarter_txt:
        if subject_slug == "ceo_letter":
            return f"{ticker_txt}_{quarter_txt}_ceo_letter{ext}"
        if canonical_family == "earnings_release":
            suffix = f"_{role_hint}" if role_hint in {"financial_schedules"} else ""
            return f"{ticker_txt}_{quarter_txt}_earnings_release{suffix}{ext}"
        if canonical_family == "earnings_presentation":
            suffix = f"_{role_hint}" if role_hint in {"financial_schedules"} else ""
            return f"{ticker_txt}_{quarter_txt}_earnings_presentation{suffix}{ext}"
        if canonical_family == "earnings_transcripts":
            return f"{ticker_txt}_{quarter_txt}_transcript{ext}"
    return path_in.name


def _quarter_label_human(qd: Optional[date]) -> str:
    if not _is_quarter_end_date(qd):
        return ""
    qnum = ((int(qd.month) - 1) // 3) + 1
    return f"Q{qnum}_{int(qd.year)}"


def _non_quarter_date_hint(path_in: Path) -> str:
    m = re.search(r"\b(20\d{2})[-_ ]?([01]\d)[-_ ]?([0-3]\d)\b", _manual_name_hint(path_in), re.I)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    try:
        return date.fromtimestamp(path_in.stat().st_mtime).isoformat()
    except Exception:
        return "unknown_date"


def _best_local_title(path_in: Path) -> str:
    title = _source_title_from_local_doc(path_in)
    return title if title and not _is_generic_doc_title(title) else _manual_name_hint(path_in)


def _manual_name_hint(path_in: Path) -> str:
    stem = str(path_in.stem or "")
    stem = stem.replace("%20", " ").replace("%2F", "/")
    stem = re.sub(r"_20(?=[A-Za-z])", " ", stem)
    stem = re.sub(r"\b([1-4])q[\s_-]*([0-9]{2})\b", r"Q\1 20\2", stem, flags=re.I)
    stem = re.sub(r"\bq([1-4])[\s_-]*([0-9]{2})\b", r"Q\1 20\2", stem, flags=re.I)
    stem = re.sub(r"\b([1-4])q[\s_-]*(20\d{2})\b", r"Q\1 \2", stem, flags=re.I)
    stem = re.sub(r"\bq([1-4])[\s_-]*(20\d{2})\b", r"Q\1 \2", stem, flags=re.I)
    stem = re.sub(r"\bq([1-4])[\s_-]*20(20\d{2})\b", r"Q\1 \2", stem, flags=re.I)
    stem = re.sub(r"[_-]+", " ", stem)
    stem = re.sub(r"\s+", " ", stem).strip()
    return stem or path_in.name


def _sanitized_slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(text or "").strip().lower()).strip("_")


def _manual_local_role_hint(path_in: Path) -> str:
    blob = _manual_name_hint(path_in).lower()
    if "financial schedules" in blob or "financial schedule" in blob:
        return "financial_schedules"
    return ""


def _manual_local_year_hint(path_in: Path) -> str:
    blob = _normalize_material_blob(_manual_name_hint(path_in), _source_title_from_local_doc(path_in))
    m = re.search(r"\b(20\d{2})\b", blob)
    if m:
        return m.group(1)
    return "unknown_year"


def _is_annual_report_material(*, name_hint: str, title: str, text_excerpt: str) -> bool:
    core_blob = _normalize_material_blob(name_hint, title)
    text_head = str(text_excerpt or "")[:1600]
    rich_blob = _normalize_material_blob(name_hint, title, text_excerpt=text_head)
    annual_markers = bool(re.search(r"(annual report|annual-report|\bar[_ -]?wr\b|\bform 10-k\b)", core_blob))
    if not annual_markers:
        if not re.search(r"(annual report|annual-report|\bform 10-k\b)", text_head.lower()):
            return False
        annual_markers = True
    if not annual_markers:
        return False
    if _has_transcript_markers(rich_blob) and _has_period_markers(rich_blob):
        return False
    if _has_results_quarter_markers(rich_blob) and (_has_presentation_markers(rich_blob) or _has_explicit_earnings_release_markers(rich_blob)):
        return False
    if _has_explicit_earnings_release_markers(_normalize_material_blob(title)) or _has_results_quarter_markers(_normalize_material_blob(title)):
        return False
    return True


def _upsert_manual_local_candidate(
    *,
    manifest: Dict[str, Dict[str, Any]],
    candidate: MaterialCandidate,
    ticker: str,
    dry_run: bool,
) -> MaterialEvent:
    local_path = candidate.local_path
    if local_path is None or not local_path.exists():
        return MaterialEvent(
            ticker=ticker,
            family=candidate.canonical_family,
            status="failed",
            origin="manual_local",
            quarter=candidate.quarter.isoformat() if candidate.quarter else "",
            destination_path=str(local_path or ""),
            reason="manual local path missing",
            title=candidate.title or candidate.source_doc_title,
        )
    manifest_key = _manifest_key(candidate)
    entry = _manifest_entry(
        candidate,
        local_path.parent,
        local_path,
        status="dry_run" if dry_run else "ok",
        sha256="" if dry_run else _sha256_path(local_path),
    )
    manifest[manifest_key] = entry
    return MaterialEvent(
        ticker=ticker,
        family=candidate.canonical_family,
        status="recognized",
        origin="manual_local",
        quarter=candidate.quarter.isoformat() if candidate.quarter else "",
        destination_path=str(local_path),
        reason=candidate.selection_reason,
        title=candidate.title or candidate.source_doc_title,
    )


def _load_manifest(path_in: Path) -> Dict[str, Dict[str, Any]]:
    if not path_in.exists():
        return {}
    try:
        payload = json.loads(path_in.read_text(encoding="utf-8"))
    except Exception:
        return {}
    entries = payload.get("entries", []) if isinstance(payload, dict) else []
    out: Dict[str, Dict[str, Any]] = {}
    for row in entries:
        if isinstance(row, dict):
            out[_manifest_key_from_entry(row)] = row
    return out


def _save_manifest(path_in: Path, manifest: Dict[str, Dict[str, Any]]) -> None:
    path_in.parent.mkdir(parents=True, exist_ok=True)
    entries = sorted(manifest.values(), key=lambda row: (str(row.get("canonical_family") or ""), str(row.get("quarter") or ""), str(row.get("destination_path") or "")))
    path_in.write_text(json.dumps({"entries": entries}, indent=2), encoding="utf-8")


def _prune_stale_manifest_entries(manifest: Dict[str, Dict[str, Any]], *, selected_keys: set[str], material_root: Path) -> None:
    try:
        material_root_resolved = material_root.resolve()
    except Exception:
        material_root_resolved = material_root
    for key in list(manifest.keys()):
        if key in selected_keys:
            continue
        entry = manifest.get(key) or {}
        origin = str(entry.get("origin") or "")
        if origin == "manual_local":
            manifest.pop(key, None)
            continue
        if origin not in {"sec_exhibit", "official_ir"}:
            continue
        dst_str = str(entry.get("destination_path") or "")
        if dst_str:
            dst_path = Path(dst_str)
            try:
                dst_resolved = dst_path.resolve()
            except Exception:
                dst_resolved = dst_path
            try:
                if dst_path.exists() and dst_resolved.is_relative_to(material_root_resolved):
                    dst_path.unlink()
            except Exception:
                pass
        manifest.pop(key, None)


def _build_coverage_report(
    *,
    ticker: str,
    manifest: Dict[str, Dict[str, Any]],
    filings_df: Any,
    max_quarters: int = 16,
    ir_diagnostics: Sequence[IRSeedDiagnostic] = tuple(),
    local_scan: Optional[LocalMaterialScanResult] = None,
) -> Dict[str, Any]:
    quarter_rows: List[date] = []
    seen: set[str] = set()
    if filings_df is not None and not getattr(filings_df, "empty", True):
        for row in filings_df.to_dict("records"):
            base_form = str(row.get("base_form") or row.get("form") or "").upper().split("/")[0]
            if base_form not in {"10-Q", "10-K"}:
                continue
            qd = _parse_quarter_date(row.get("reportDate"))
            if qd is None:
                continue
            qkey = qd.isoformat()
            if qkey in seen:
                continue
            seen.add(qkey)
            quarter_rows.append(qd)
            if len(quarter_rows) >= max_quarters:
                break
    entries = list(manifest.values())
    quarter_map: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    for qd in quarter_rows:
        quarter_map[qd.isoformat()] = {family: [] for family in ("earnings_release", "press_release", "earnings_presentation", "earnings_transcripts")}
    non_quarter_entries: List[Dict[str, Any]] = []
    ambiguous_entries: List[Dict[str, Any]] = []
    for entry in entries:
        family = str(entry.get("canonical_family") or "")
        qkey = str(entry.get("quarter") or "")
        assignment_status = str(entry.get("quarter_assignment_status") or "")
        if qkey in quarter_map and assignment_status == "matched_quarter_end" and family in quarter_map[qkey]:
            quarter_map[qkey][family].append(_coverage_material_brief(entry))
        elif assignment_status in {"non_quarter_event", "unknown", "ambiguous"}:
            target = non_quarter_entries if assignment_status == "non_quarter_event" else ambiguous_entries
            target.append(_coverage_material_brief(entry))
    ir_diag_by_family: Dict[str, IRSeedDiagnostic] = {}
    for diag in ir_diagnostics:
        ir_diag_by_family[diag.family] = diag
    quarters_payload: List[Dict[str, Any]] = []
    for qd in quarter_rows:
        qkey = qd.isoformat()
        fam_rows = quarter_map.get(qkey, {})
        missing = [family for family in ("earnings_release", "earnings_presentation", "earnings_transcripts") if not fam_rows.get(family)]
        missing_reasons = {
            family: _coverage_missing_reason(
                family=family,
                qkey=qkey,
                fam_rows=fam_rows,
                ir_diag_by_family=ir_diag_by_family,
            )
            for family in ("earnings_release", "press_release", "earnings_presentation", "earnings_transcripts")
            if not fam_rows.get(family)
        }
        quarters_payload.append(
            {
                "quarter": qkey,
                "release_found": bool(fam_rows.get("earnings_release")),
                "press_found": bool(fam_rows.get("press_release")),
                "presentation_found": bool(fam_rows.get("earnings_presentation")),
                "transcript_found": bool(fam_rows.get("earnings_transcripts")),
                "missing_expected_families": missing,
                "missing_reasons_by_family": missing_reasons,
                "materials": fam_rows,
            }
        )
    return {
        "ticker": ticker,
        "latest_quarter": quarter_rows[0].isoformat() if quarter_rows else "",
        "quarters": quarters_payload,
        "ambiguous_materials": ambiguous_entries[:50],
        "non_quarter_materials": non_quarter_entries[:100],
        "moved_files": list((local_scan.moved_files if local_scan is not None else [])[:200]),
        "renamed_files": list((local_scan.renamed_files if local_scan is not None else [])[:200]),
        "duplicate_files": list((local_scan.duplicate_files if local_scan is not None else [])[:200]),
        "manual_review_files": list((local_scan.manual_review_files if local_scan is not None else [])[:200]),
        "ir_diagnostics": [
            {
                "family": diag.family,
                "seed_url": diag.seed_url,
                "outcome": diag.outcome,
                "detail": diag.detail,
                "asset_count": diag.asset_count,
                "ambiguous_count": diag.ambiguous_count,
                "webcast_only_count": diag.webcast_only_count,
                "detail_failures": diag.detail_failures,
            }
            for diag in ir_diagnostics
        ],
    }


def _coverage_material_brief(entry: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "canonical_family": str(entry.get("canonical_family") or ""),
        "quarter": str(entry.get("quarter") or ""),
        "filed_date": str(entry.get("filed_date") or ""),
        "report_date": str(entry.get("report_date") or ""),
        "source_doc_title": str(entry.get("source_doc_title") or ""),
        "destination_path": str(entry.get("destination_path") or ""),
        "source_url": str(entry.get("source_url") or ""),
        "quarter_assignment_status": str(entry.get("quarter_assignment_status") or ""),
        "quarter_assignment_reason": str(entry.get("quarter_assignment_reason") or ""),
        "origin": str(entry.get("origin") or ""),
    }


def _format_coverage_lines(report: Dict[str, Any]) -> List[str]:
    lines: List[str] = []
    lines.append(
        "[source_materials] local "
        f"ticker={report.get('ticker') or ''} "
        f"moved={len(report.get('moved_files') or [])} "
        f"renamed={len(report.get('renamed_files') or [])} "
        f"duplicates={len(report.get('duplicate_files') or [])} "
        f"manual_review={len(report.get('manual_review_files') or [])}"
    )
    for row in report.get("quarters", []) or []:
        reason_bits = [
            f"{family}:{reason}"
            for family, reason in (row.get("missing_reasons_by_family") or {}).items()
            if str(reason or "").strip()
        ]
        lines.append(
            "[source_materials] coverage "
            f"ticker={report.get('ticker') or ''} "
            f"quarter={row.get('quarter') or ''} "
            f"release={'Y' if row.get('release_found') else 'N'} "
            f"press={'Y' if row.get('press_found') else 'N'} "
            f"presentation={'Y' if row.get('presentation_found') else 'N'} "
            f"transcript={'Y' if row.get('transcript_found') else 'N'} "
            f"missing={','.join(row.get('missing_expected_families') or []) or 'none'} "
            f"reasons={';'.join(reason_bits) or 'none'}"
        )
    for row in (report.get("ambiguous_materials") or [])[:10]:
        lines.append(
            "[source_materials] ambiguous "
            f"ticker={report.get('ticker') or ''} "
            f"quarter={row.get('quarter') or ''} "
            f"title={row.get('source_doc_title') or ''} "
            f"path={row.get('destination_path') or ''}"
        )
    for row in (report.get("non_quarter_materials") or [])[:10]:
        lines.append(
            "[source_materials] non-quarter "
            f"ticker={report.get('ticker') or ''} "
            f"date={row.get('quarter') or ''} "
            f"family={row.get('canonical_family') or ''} "
            f"title={row.get('source_doc_title') or ''} "
            f"path={row.get('destination_path') or ''}"
        )
    for row in (report.get("manual_review_files") or [])[:10]:
        lines.append(
            "[source_materials] manual-review "
            f"ticker={report.get('ticker') or ''} "
            f"family={row.get('family_hint') or ''} "
            f"title={row.get('title') or ''} "
            f"path={row.get('path') or ''} "
            f"reason={row.get('reason') or ''}"
        )
    for row in (report.get("duplicate_files") or [])[:10]:
        lines.append(
            "[source_materials] duplicate "
            f"ticker={report.get('ticker') or ''} "
            f"family={row.get('canonical_family') or ''} "
            f"path={row.get('duplicate_path') or ''} "
            f"matches={row.get('existing_path') or ''}"
        )
    for diag in report.get("ir_diagnostics", []) or []:
        lines.append(
            "[source_materials] ir "
            f"ticker={report.get('ticker') or ''} "
            f"family={diag.get('family') or ''} "
            f"outcome={diag.get('outcome') or ''} "
            f"assets={diag.get('asset_count') or 0} "
            f"ambiguous={diag.get('ambiguous_count') or 0} "
            f"webcast_only={diag.get('webcast_only_count') or 0} "
            f"seed={diag.get('seed_url') or ''} "
            f"detail={diag.get('detail') or 'none'}"
        )
    return lines


def _coverage_missing_reason(
    *,
    family: str,
    qkey: str,
    fam_rows: Dict[str, List[Dict[str, Any]]],
    ir_diag_by_family: Dict[str, IRSeedDiagnostic],
) -> str:
    if fam_rows.get(family):
        return ""
    if family == "earnings_release":
        return "no SEC exhibit found"
    diag = ir_diag_by_family.get(family)
    if diag is None:
        return "no SEC exhibit found" if family == "press_release" else "no official fallback configured"
    if diag.outcome == "timeout":
        return "IR timeout"
    if diag.outcome == "forbidden_403":
        return "IR 403"
    if diag.outcome == "not_found_404":
        return "IR 404"
    if diag.outcome == "blocked_or_challenged":
        return "IR blocked or challenged"
    if diag.outcome == "parse_failure":
        return "IR parsing failure"
    if diag.outcome == "http_error":
        return "IR HTTP error"
    if diag.ambiguous_count > 0:
        return "quarter matching ambiguous"
    if family == "earnings_transcripts":
        if diag.webcast_only_count > 0:
            return "only webcast/event page was found without downloadable transcript"
        if diag.asset_count > 0:
            return "no quarter-matched official transcript asset found"
        return "no official transcript asset exposed"
    if family == "earnings_presentation":
        if diag.asset_count > 0:
            return "no quarter-matched official IR asset found"
        return "no official presentation asset exposed"
    if family == "press_release":
        return "non-results document excluded by classification"
    return "reachable but no quarter-matched assets found"


def _manifest_key_from_entry(entry: Dict[str, Any]) -> str:
    origin = str(entry.get("origin") or "")
    accession = str(entry.get("accession") or "")
    if origin == "sec_exhibit" and accession:
        return f"sec_exhibit|{normalize_accession(accession)}|{entry.get('exhibit_type') or ''}|{entry.get('source_url') or ''}"
    if accession:
        return f"sec_filing|{normalize_accession(accession)}|{entry.get('form') or ''}"
    if origin == "manual_local":
        return f"local|{entry.get('canonical_family') or ''}|{_path_text(entry.get('destination_path') or '')}"
    source_url = str(entry.get("source_url") or "").strip().lower()
    if source_url:
        return f"ir|{source_url}"
    return f"local|{entry.get('canonical_family') or ''}|{str(entry.get('source_doc_title') or '').strip().lower()}|{entry.get('quarter') or ''}"


def _recognized_manual_transcript_candidates(material_root: Path, quarter_targets: set[str]) -> List[MaterialCandidate]:
    out: List[MaterialCandidate] = []
    for dir_name in CANONICAL_FAMILY_ALIASES["earnings_transcripts"]:
        folder = material_root / dir_name
        if not folder.exists() or not folder.is_dir():
            continue
        for path_in in sorted([p for p in folder.glob("*") if p.is_file()]):
            if path_in.suffix.lower() not in {".txt", ".md", ".htm", ".html", ".pdf"}:
                continue
            assignment = _assign_quarter_from_source(
                title=path_in.name,
                source_name=path_in.name,
                source_url="",
                text_excerpt=_load_material_text_excerpt(path_in, max_chars=12000),
                default_q=None,
                allow_non_quarter_default=False,
                canonical_family="earnings_transcripts",
            )
            qd = assignment.quarter
            if quarter_targets and qd is not None and qd.isoformat() not in quarter_targets:
                continue
            out.append(MaterialCandidate(canonical_family="earnings_transcripts", quarter=qd, local_path=path_in, source_url="", title=path_in.name, origin="manual_local", selection_reason="manual/local transcript fallback", source_doc_title=path_in.name, quarter_assignment_status=assignment.status, quarter_assignment_reason=assignment.reason))
    return out


def _discover_official_ir_candidates(*, profile: CompanyProfile, quarter_targets: set[str], session: requests.Session) -> IRDiscoveryResult:
    seeds = tuple(getattr(profile, "official_source_seeds", ()) or ())
    if not seeds:
        return IRDiscoveryResult()
    seen_urls: set[str] = set()
    out = IRDiscoveryResult()
    for seed in seeds:
        seed_result = _discover_ir_candidates_for_seed(session, seed, quarter_targets)
        out.diagnostics.extend(seed_result.diagnostics)
        for cand in seed_result.candidates:
            url_key = cand.source_url.strip().lower()
            if not url_key or url_key in seen_urls:
                continue
            seen_urls.add(url_key)
            out.candidates.append(cand)
    return out


def _discover_ir_candidates_for_seed(session: requests.Session, seed: SourceMaterialSeed, quarter_targets: set[str]) -> IRDiscoveryResult:
    result = IRDiscoveryResult()
    seed_url = str(seed.seed_url or "").strip()
    if not seed_url:
        return result
    fetched = _fetch_ir_html(session, seed_url)
    if fetched.outcome != "ok":
        result.diagnostics.append(
            IRSeedDiagnostic(
                family=seed.family,
                seed_url=seed_url,
                outcome=fetched.outcome,
                detail=fetched.detail,
            )
        )
        return result
    html = fetched.html
    seed_host = urlparse(seed_url).netloc.lower()
    allowed_hosts = tuple(str(h or "").strip().lower() for h in (seed.allowed_hosts or ()) if str(h or "").strip()) or (seed_host,)
    pages: List[Tuple[str, str, str]] = [(seed_url, html, "seed")]
    ambiguous_count = 0
    webcast_only_count = 0
    detail_failures = 0
    seed_links, parse_error = _safe_extract_page_links(seed_url, html)
    if parse_error:
        result.diagnostics.append(
            IRSeedDiagnostic(
                family=seed.family,
                seed_url=seed_url,
                outcome="parse_failure",
                detail=parse_error,
            )
        )
        return result
    if seed.follow_detail_pages:
        for href, label in seed_links:
            if not _allowed_host(href, allowed_hosts):
                continue
            blob = f"{label} {href}".lower()
            if seed.family == "earnings_transcripts" and re.search(r"(webcast|event|conference call|listen)", blob) and not _has_transcript_markers(blob):
                webcast_only_count += 1
            if not re.search(r"(quarter|q[1-4]|full year|fy20|results|earnings|presentation|slides|transcript)", blob):
                continue
            if not _matches_any_quarter(blob, quarter_targets):
                continue
            det = _fetch_ir_html(session, href)
            if det.outcome != "ok":
                detail_failures += 1
                continue
            pages.append((href, det.html, "detail"))
    for page_url, page_html, page_kind in pages:
        page_title = _page_title_from_html(page_html)
        page_text = re.sub(r"\s+", " ", strip_html(page_html or "")).strip()[:12000]
        page_links, parse_error = _safe_extract_page_links(page_url, page_html)
        if parse_error:
            detail_failures += 1
            continue
        for href, label in page_links:
            if not _allowed_host(href, allowed_hosts):
                continue
            ext = Path(urlparse(href).path).suffix.lower()
            combined = f"{label} {href} {page_title}".lower()
            if seed.family == "earnings_transcripts" and re.search(r"(webcast|event|conference call|listen)", combined) and not _has_transcript_markers(combined):
                webcast_only_count += 1
            if ext and ext not in DIRECT_ASSET_EXTS:
                continue
            family = _classify_material_family(
                nm=Path(urlparse(href).path).name,
                title=f"{label} {page_title}",
                sec_type="",
                seed_family_hint=seed.family,
                text_excerpt=page_text,
                source_url=href,
            )
            if family is None:
                continue
            assignment = _assign_quarter_from_source(
                title=f"{label} {page_title}",
                source_name=Path(urlparse(href).path).name,
                source_url=href,
                text_excerpt=page_text,
                default_q=_infer_ir_quarter(combined),
                allow_non_quarter_default=False,
                canonical_family=family,
            )
            qd = assignment.quarter
            if qd is None or (quarter_targets and qd.isoformat() not in quarter_targets):
                ambiguous_count += 1
                continue
            result.candidates.append(MaterialCandidate(canonical_family=family, quarter=qd, local_path=None, source_url=href, title=label or page_title or Path(urlparse(href).path).name, origin="official_ir", selection_reason=f"official IR {page_kind} page", source_doc_title=label or page_title or Path(urlparse(href).path).name, quarter_assignment_status=assignment.status, quarter_assignment_reason=assignment.reason))
        if page_kind == "detail":
            combined = f"{page_title} {page_url}".lower()
            family = _classify_material_family(nm=Path(urlparse(page_url).path).name, title=page_title, sec_type="", seed_family_hint=seed.family, text_excerpt=page_text, source_url=page_url)
            assignment = _assign_quarter_from_source(
                title=page_title,
                source_name=Path(urlparse(page_url).path).name,
                source_url=page_url,
                text_excerpt=page_text,
                default_q=_infer_ir_quarter(combined),
                allow_non_quarter_default=False,
                canonical_family=family or seed.family,
            )
            qd = assignment.quarter
            if family == "earnings_transcripts" and qd is not None and (not quarter_targets or qd.isoformat() in quarter_targets):
                result.candidates.append(MaterialCandidate(canonical_family="earnings_transcripts", quarter=qd, local_path=None, source_url=page_url, title=page_title or Path(urlparse(page_url).path).name, origin="official_ir", selection_reason="official IR detail page transcript", source_doc_title=page_title or Path(urlparse(page_url).path).name, quarter_assignment_status=assignment.status, quarter_assignment_reason=assignment.reason))
            elif family == "earnings_transcripts":
                ambiguous_count += 1
    result.diagnostics.append(
        IRSeedDiagnostic(
            family=seed.family,
            seed_url=seed_url,
            outcome="ok_assets_found" if result.candidates else "ok_no_matching_assets",
            detail=_ir_diagnostic_detail(
                ambiguous_count=ambiguous_count,
                webcast_only_count=webcast_only_count,
                detail_failures=detail_failures,
            ),
            asset_count=len(result.candidates),
            ambiguous_count=ambiguous_count,
            webcast_only_count=webcast_only_count,
            detail_failures=detail_failures,
        )
    )
    return result


def _build_ir_session(user_agent: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/pdf,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
        }
    )
    return session


def _fetch_ir_html(session: requests.Session, url: str, *, retries: int = 2) -> IRFetchResult:
    last_detail = ""
    for attempt in range(retries + 1):
        try:
            resp = session.get(url, timeout=(10, 25))
            status = int(resp.status_code or 0)
            text = resp.text or ""
            if status == 403:
                return IRFetchResult(outcome="forbidden_403", detail=f"403 for {url}", status_code=status)
            if status == 404:
                return IRFetchResult(outcome="not_found_404", detail=f"404 for {url}", status_code=status)
            if status >= 400:
                return IRFetchResult(outcome="http_error", detail=f"HTTP {status} for {url}", status_code=status)
            if _looks_ir_blocked_or_challenged(text):
                return IRFetchResult(outcome="blocked_or_challenged", detail=f"challenge page for {url}", status_code=status)
            return IRFetchResult(outcome="ok", html=text, status_code=status)
        except requests.exceptions.Timeout as exc:
            last_detail = f"{type(exc).__name__}: {exc}"
            if attempt < retries:
                time.sleep(0.5 * (attempt + 1))
                continue
            return IRFetchResult(outcome="timeout", detail=last_detail)
        except requests.exceptions.RequestException as exc:
            return IRFetchResult(outcome="http_error", detail=f"{type(exc).__name__}: {exc}")
        except Exception as exc:
            return IRFetchResult(outcome="parse_failure", detail=f"{type(exc).__name__}: {exc}")
    return IRFetchResult(outcome="timeout", detail=last_detail or f"timeout for {url}")


def _safe_extract_page_links(base_url: str, html: str) -> Tuple[List[Tuple[str, str]], str]:
    try:
        return _extract_page_links(base_url, html), ""
    except Exception as exc:
        return [], f"{type(exc).__name__}: {exc}"


def _looks_ir_blocked_or_challenged(text: str) -> bool:
    blob = str(text or "").lower()
    return bool(
        re.search(
            r"(access denied|forbidden|just a moment|attention required|captcha|verify you are human|enable javascript|cf-chl|bot detection|request unsuccessful)",
            blob,
        )
    )


def _ir_diagnostic_detail(*, ambiguous_count: int, webcast_only_count: int, detail_failures: int) -> str:
    parts: List[str] = []
    if ambiguous_count:
        parts.append(f"ambiguous={ambiguous_count}")
    if webcast_only_count:
        parts.append(f"webcast_only={webcast_only_count}")
    if detail_failures:
        parts.append(f"detail_failures={detail_failures}")
    return ", ".join(parts)


def _extract_page_links(base_url: str, html: str) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    if BeautifulSoup is not None:
        try:
            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all("a", href=True):
                href = str(a.get("href") or "").strip()
                if href:
                    out.append((urljoin(base_url, href), a.get_text(" ", strip=True)))
            return out
        except Exception:
            pass
    for m in re.finditer(r"<a[^>]+href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", html or "", re.I | re.S):
        href = urljoin(base_url, str(m.group(1) or "").strip())
        label = re.sub(r"<[^>]+>", " ", str(m.group(2) or ""))
        out.append((href, re.sub(r"\s+", " ", label).strip()))
    return out


def _page_title_from_html(html: str) -> str:
    if BeautifulSoup is not None:
        try:
            soup = BeautifulSoup(html, "html.parser")
            if soup.title and str(soup.title.text or "").strip():
                return str(soup.title.text).strip()
            h1 = soup.find("h1")
            if h1:
                return str(h1.get_text(" ", strip=True) or "").strip()
        except Exception:
            pass
    m = re.search(r"<title[^>]*>(.*?)</title>", html or "", re.I | re.S)
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", str(m.group(1) or ""))).strip() if m else ""


def _allowed_host(url: str, allowed_hosts: Sequence[str]) -> bool:
    host = urlparse(url).netloc.lower()
    return bool(host and any(host == h or host.endswith(f".{h}") for h in allowed_hosts))


def _matches_any_quarter(text: str, quarter_targets: set[str]) -> bool:
    if not quarter_targets:
        return True
    return any((_parse_quarter_date(qiso) is not None and _quarter_match_text(text, _parse_quarter_date(qiso))) for qiso in quarter_targets)


def _infer_ir_quarter(text: str) -> Optional[date]:
    qd = infer_quarter_end_from_text(str(text or ""))
    if qd is not None:
        return qd
    txt = str(text or "")
    qd = infer_q_from_name(txt)
    if qd is not None:
        return qd
    m = re.search(r"\b(?:full\s+year|fy\s*[- ]?)(20\d{2})\b", txt, re.I)
    if m:
        return date(int(m.group(1)), 12, 31)
    return None


def _quarter_match_text(text: str, qd: Optional[date]) -> bool:
    if qd is None:
        return False
    txt = str(text or "").lower()
    qnum = ((qd.month - 1) // 3) + 1
    year = qd.year
    ordinals = {1: "first", 2: "second", 3: "third", 4: "fourth"}
    if re.search(rf"\bq{qnum}\s*[- ]?\s*{year}\b", txt) or re.search(rf"\b{ordinals[qnum]}\s+quarter\s+{year}\b", txt):
        return True
    return bool(qnum == 4 and (re.search(rf"\bfull\s+year\s+{year}\b", txt) or re.search(rf"\bfy\s*[- ]?\s*{year}\b", txt) or re.search(rf"\bfy{year}\b", txt)))


def _is_decorative_asset(path_in: Path) -> bool:
    nm = path_in.name.lower()
    ext = path_in.suffix.lower()
    if ext in SKIP_EXTS or "favicon" in nm:
        return True
    if ext in IMAGE_EXTS:
        if re.search(r"(logo|brand|icon|thumbnail|thumb|banner)", nm):
            return True
        try:
            if path_in.stat().st_size < 25_000:
                return True
        except Exception:
            return True
    return False


def _hardlink_or_copy(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        return
    try:
        os.link(src, dst)
    except Exception:
        shutil.copy2(src, dst)


def _sha256_path(path_in: Path) -> str:
    h = hashlib.sha256()
    with path_in.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
