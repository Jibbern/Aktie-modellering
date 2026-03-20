from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .debt_parser import build_debt_schedule_tier2, build_debt_tranches_tier2, coerce_number
from .doc_intel import build_doc_intel_outputs, extract_pdf_text_cached, validate_quarter_notes
from .legacy_support import (
    PIPELINE_STAGE_CACHE_VERSION,
    _coerce_prev_quarter_end,
    build_bridge_q,
    build_company_overview,
    build_debt_buckets,
    build_debt_credit_notes,
    build_debt_profile,
    build_debt_qa_checks,
    build_gaap_history,
    build_interest_qa_checks,
    build_local_main_revolver_history,
    build_qa_checks,
    build_revolver_availability,
    build_revolver_capacity_map,
    build_revolver_history,
    compute_long_term_debt_instant,
    compute_total_debt_instant,
)
from .metrics import GAAP_SPECS, MetricSpec
from .non_gaap import build_non_gaap_tier3, infer_quarter_end_from_text, parse_adjusted_from_plain_text, strip_html
from .period_resolver import _duration_days, _filter_unit, classify_duration, pick_best_instant, self_check_period_logic
from .pdf_utils import silence_pdfminer_warnings
from .sec_xbrl import SecClient, SecConfig, cik10_from_int, cik_from_ticker, companyfacts_to_df, parse_date
from .validators import info_log_from_audit, needs_review_from_audit, validate_debt_tieout, validate_history
from .cache_layout import preferred_ticker_cache_root_from_base_dir

from .pipeline_qa import (
    build_non_gaap_cred_qa,
    build_promise_qa_checks,
    concat_frames,
    finalize_needs_review,
    finalize_qa_checks,
)
from .pipeline_runtime import (
    PipelineStageCache,
    dataframe_quick_signature,
    material_dirs_signature,
    resolve_pipeline_roots,
    submissions_recent_signature,
    timed_stage,
)
from .pipeline_types import PipelineArtifacts, PipelineConfig


LOCAL_NON_GAAP_FALLBACK_VERSION = 4
LOCAL_NON_GAAP_PDF_PAGE_CACHE_VERSION = 1
DOC_INTEL_BEHAVIOR_VERSION = "v17_workbook_dataflow_hardening"
COMPANY_OVERVIEW_BEHAVIOR_VERSION = "v8_topic_aware_summary_dataflow_hardening"


def _module_code_signature(*relative_names: str) -> str:
    rows: List[str] = []
    base_dir = Path(__file__).resolve().parent
    for rel_name in relative_names:
        try:
            mod_path = (base_dir / rel_name).resolve()
            st = mod_path.stat()
            rows.append(f"{mod_path.name}:{int(st.st_size)}:{int(st.st_mtime)}")
        except Exception:
            rows.append(f"{Path(rel_name).name}:missing")
    if not rows:
        return "none"
    return hashlib.sha1("||".join(rows).encode("utf-8", errors="ignore")).hexdigest()
LOCAL_NON_GAAP_CANONICAL_METRICS: Tuple[str, ...] = ("adj_ebitda", "adj_ebit", "adj_eps", "adj_fcf")


def _normalized_quarter_timestamps(values: Any) -> set[pd.Timestamp]:
    if values is None:
        return set()
    q_series = pd.to_datetime(values, errors="coerce")
    if not isinstance(q_series, pd.Series):
        q_series = pd.Series(q_series)
    return {pd.Timestamp(v) for v in q_series.dropna()}


def _filter_missing_local_non_gaap_quarters(
    q_targets: List[dt.date],
    existing_q: set[pd.Timestamp],
) -> List[dt.date]:
    out: List[dt.date] = []
    seen: set[pd.Timestamp] = set()
    for qd in q_targets:
        try:
            q_ts = pd.Timestamp(qd)
        except Exception:
            continue
        if q_ts in existing_q or q_ts in seen:
            continue
        seen.add(q_ts)
        out.append(qd)
    return out


def _existing_local_non_gaap_metrics_by_quarter(
    df: Optional[pd.DataFrame],
    *,
    metrics: Tuple[str, ...] = LOCAL_NON_GAAP_CANONICAL_METRICS,
) -> Dict[pd.Timestamp, set[str]]:
    out: Dict[pd.Timestamp, set[str]] = {}
    if df is None or df.empty or "quarter" not in df.columns:
        return out
    q_series = pd.to_datetime(df["quarter"], errors="coerce")
    if not isinstance(q_series, pd.Series):
        q_series = pd.Series(q_series, index=df.index)
    available_metrics = [metric for metric in metrics if metric in df.columns]
    if not available_metrics:
        return out
    for idx, q_val in q_series.items():
        if pd.isna(q_val):
            continue
        present_metrics = {metric for metric in available_metrics if pd.notna(df.at[idx, metric])}
        if present_metrics:
            out.setdefault(pd.Timestamp(q_val), set()).update(present_metrics)
    return out


def _filter_missing_local_non_gaap_metric_quarters(
    q_targets: List[dt.date],
    existing_metrics_by_quarter: Dict[pd.Timestamp, set[str]],
    *,
    metrics: Tuple[str, ...] = LOCAL_NON_GAAP_CANONICAL_METRICS,
) -> List[dt.date]:
    out: List[dt.date] = []
    seen: set[pd.Timestamp] = set()
    wanted_metrics = {metric for metric in metrics if metric}
    for qd in q_targets:
        try:
            q_ts = pd.Timestamp(qd)
        except Exception:
            continue
        if q_ts in seen:
            continue
        seen.add(q_ts)
        if wanted_metrics.difference(existing_metrics_by_quarter.get(q_ts, set())):
            out.append(qd)
    return out


def _prune_local_non_gaap_metrics_against_existing(
    local_metrics: Optional[pd.DataFrame],
    existing_metrics_by_quarter: Dict[pd.Timestamp, set[str]],
    *,
    metrics: Tuple[str, ...] = LOCAL_NON_GAAP_CANONICAL_METRICS,
) -> pd.DataFrame:
    if local_metrics is None or local_metrics.empty:
        return pd.DataFrame() if local_metrics is None else local_metrics
    if "quarter" not in local_metrics.columns:
        return local_metrics

    pruned = local_metrics.copy()
    pruned["quarter"] = pd.to_datetime(pruned["quarter"], errors="coerce")
    pruned = pruned[pruned["quarter"].notna()].copy()
    if pruned.empty:
        return pruned

    tracked_metrics = [metric for metric in metrics if metric in pruned.columns]
    if not tracked_metrics:
        return pruned

    keep_indices: List[int] = []
    for idx, quarter in pruned["quarter"].items():
        existing_metrics = existing_metrics_by_quarter.get(pd.Timestamp(quarter), set())
        for metric in tracked_metrics:
            if metric in existing_metrics:
                pruned.at[idx, metric] = pd.NA
        if any(pd.notna(pruned.at[idx, metric]) for metric in tracked_metrics):
            keep_indices.append(idx)

    if not keep_indices:
        return pruned.iloc[0:0].copy()
    return pruned.loc[keep_indices].reset_index(drop=True)


def _local_non_gaap_pdf_cache_dirs(base_dir: Path, src_name: str) -> Tuple[Path, Path]:
    cache_root = preferred_ticker_cache_root_from_base_dir(base_dir)
    if str(src_name or "").strip().lower() == "slides":
        return cache_root / "slides_text", cache_root / "slides_ocr"
    src_key = re.sub(r"[^a-z0-9]+", "_", str(src_name or "other").strip().lower()).strip("_") or "other"
    return cache_root / f"local_non_gaap_{src_key}_text", cache_root / f"local_non_gaap_{src_key}_ocr"


def _local_non_gaap_pdf_cache_key(path_in: Path, *, src_name: str, page_number: int) -> str:
    if str(src_name or "").strip().lower() == "slides":
        return f"{path_in.stem}_p{page_number}"
    try:
        raw_key = str(path_in.resolve())
    except Exception:
        raw_key = str(path_in)
    digest = hashlib.sha1(
        f"{LOCAL_NON_GAAP_PDF_PAGE_CACHE_VERSION}|{str(src_name or '').strip().lower()}|{raw_key}".encode(
            "utf-8",
            errors="ignore",
        )
    ).hexdigest()[:16]
    src_key = re.sub(r"[^a-z0-9]+", "_", str(src_name or "other").strip().lower()).strip("_") or "other"
    return f"{src_key}_{digest}_p{page_number}"


def run_pipeline_impl(
    config: PipelineConfig,
    sec_config: SecConfig,
    *,
    ticker: Optional[str] = None,
    cik: Optional[str] = None,
) -> PipelineArtifacts:
    sec = SecClient(cache_dir=config.cache_dir, cfg=sec_config)
    repo_root = (
        Path(config.repo_root).expanduser().resolve()
        if config.repo_root is not None
        else Path(__file__).resolve().parents[2]
    )
    default_base_dir = (
        Path(config.material_root).expanduser().resolve()
        if config.material_root is not None
        else Path(__file__).resolve().parents[1]
    )
    tkr_raw, tkr_u, base_dir = resolve_pipeline_roots(
        repo_root=repo_root,
        default_base_dir=default_base_dir,
        ticker=ticker,
        material_root=config.material_root,
    )

    if cik:
        cik_int = int(cik)
    elif ticker:
        cik_int = cik_from_ticker(sec, ticker)
    else:
        raise RuntimeError("Must provide ticker or cik")

    cik10 = cik10_from_int(cik_int)

    cf = sec.companyfacts(cik10)
    sub = sec.submissions(cik10)
    df_all = companyfacts_to_df(cf, namespace=config.namespace)
    # Defensive: keep fy_calc available even if upstream normalization changes.
    if df_all is not None and not df_all.empty and "fy_calc" not in df_all.columns:
        fy_end_mmdd = (12, 31)
        try:
            if "fp" in df_all.columns and "end_d" in df_all.columns:
                fy_rows = df_all[df_all["fp"].astype(str).str.upper().isin(["FY", "Q4"])].copy()
                fy_rows = fy_rows[fy_rows["end_d"].notna()]
                if not fy_rows.empty:
                    mmdd = fy_rows["end_d"].map(lambda d: (d.month, d.day))
                    if not mmdd.empty:
                        fy_end_mmdd = mmdd.value_counts().idxmax()
        except Exception:
            fy_end_mmdd = (12, 31)
        def _calc_fy_fallback(end_d: Any) -> Optional[int]:
            if end_d is None or pd.isna(end_d):
                return None
            try:
                if (int(end_d.month), int(end_d.day)) > (int(fy_end_mmdd[0]), int(fy_end_mmdd[1])):
                    return int(end_d.year) + 1
                return int(end_d.year)
            except Exception:
                return None
        df_all["fy_calc"] = df_all["end_d"].map(_calc_fy_fallback)
    stage_timings: Dict[str, float] = {}
    local_material_sig = material_dirs_signature(base_dir, ticker)
    stage_cache = PipelineStageCache(Path(config.cache_dir) / "pipeline_stage_cache", cik10, PIPELINE_STAGE_CACHE_VERSION)
    _sub_recent_signature = submissions_recent_signature
    _df_quick_sig = dataframe_quick_signature
    _load_stage_cache = stage_cache.load
    _save_stage_cache = stage_cache.save
    _timed_stage = timed_stage

    submissions_sig = _sub_recent_signature(sub, forms_prefix=("10-Q", "10-K", "8-K", "DEF 14A", "DEFA14A"), max_rows=600)
    df_all_sig = _df_quick_sig(df_all, ["concept", "end_d", "start_d", "val", "fy_calc", "fp", "frame"])
    gaap_history_key = "|".join(
        [
            "v1",
            f"sub={submissions_sig}",
            f"facts={df_all_sig}",
            f"max_q={config.max_quarters}",
            f"min_year={config.min_year}",
            f"strict={config.strictness}",
            f"ticker={str(ticker or '').upper()}",
        ]
    )
    gaap_cached = _load_stage_cache("gaap_history_bundle", gaap_history_key)
    if isinstance(gaap_cached, dict):
        hist = gaap_cached.get("hist", pd.DataFrame())
        audit = gaap_cached.get("audit", pd.DataFrame())
        qfd_preview = gaap_cached.get("qfd_preview", pd.DataFrame())
        qfd_unused = gaap_cached.get("qfd_unused", pd.DataFrame())
    else:
        with _timed_stage(stage_timings, "gaap_history", enabled=config.profile_timings):
            hist, audit, qfd_preview, qfd_unused = build_gaap_history(
                df_all,
                max_quarters=config.max_quarters,
                strictness=config.strictness,
                min_year=config.min_year,
                sec=sec,
                cik_int=cik_int,
                submissions=sub,
                ticker=ticker,
                quiet_pdf_warnings=config.quiet_pdf_warnings,
                stage_timings=stage_timings,
                profile_timings=config.profile_timings,
            )
        _save_stage_cache(
            "gaap_history_bundle",
            gaap_history_key,
            {
                "hist": hist,
                "audit": audit,
                "qfd_preview": qfd_preview,
                "qfd_unused": qfd_unused,
            },
        )
    # Imported lazily to avoid widening the pipeline/module dependency cycle.
    from .pipeline import build_tag_coverage

    tag_coverage = build_tag_coverage(df_all)
    period_checks = self_check_period_logic(
        df_all,
        audit,
        metric_name="revenue",
        strictness=config.strictness,
    )
    debt_tranches = pd.DataFrame()
    if config.enable_tier2_debt:
        debt_tranches_key = "|".join(
            [
                "v1",
                f"sub={submissions_sig}",
                f"max_q={config.max_quarters}",
                f"min_year={config.min_year}",
            ]
        )
        debt_tranches_cached = _load_stage_cache("debt_tranches_tier2", debt_tranches_key)
        if isinstance(debt_tranches_cached, pd.DataFrame):
            debt_tranches = debt_tranches_cached
        else:
            with _timed_stage(stage_timings, "debt_tranches_tier2", enabled=config.profile_timings):
                debt_tranches = build_debt_tranches_tier2(
                    sec,
                    cik_int,
                    sub,
                    max_quarters=config.max_quarters,
                    min_year=config.min_year,
                )
            _save_stage_cache("debt_tranches_tier2", debt_tranches_key, debt_tranches)
    qa_checks = build_qa_checks(df_all, hist, audit=audit)
    debt_qa = build_debt_qa_checks(debt_tranches)
    if debt_qa is not None and not debt_qa.empty:
        if qa_checks is None or qa_checks.empty:
            qa_checks = debt_qa
        else:
            qa_checks = pd.concat([qa_checks, debt_qa], ignore_index=True)
    interest_qa = build_interest_qa_checks(hist, audit)
    if interest_qa is not None and not interest_qa.empty:
        if qa_checks is None or qa_checks.empty:
            qa_checks = interest_qa
        else:
            qa_checks = pd.concat([qa_checks, interest_qa], ignore_index=True)
    debt_buckets = pd.DataFrame()
    debt_bucket_qa = pd.DataFrame()
    bridge_q = build_bridge_q(hist)

    adj_metrics = pd.DataFrame()
    adj_breakdown = pd.DataFrame()
    non_gaap_files = pd.DataFrame()
    adj_metrics_relaxed = pd.DataFrame()
    adj_breakdown_relaxed = pd.DataFrame()
    non_gaap_files_relaxed = pd.DataFrame()
    slides_segments = pd.DataFrame()
    slides_debt = pd.DataFrame()
    slides_guidance = pd.DataFrame()

    if config.enable_tier3_non_gaap:
        def _load_or_build_tier3(mode_name: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
            tier3_key = "|".join(
                [
                    "v1",
                    f"sub={submissions_sig}",
                    f"mode={mode_name}",
                    f"max_q={config.max_quarters}",
                ]
            )
            cached = _load_stage_cache(f"tier3_non_gaap_{mode_name}", tier3_key)
            if isinstance(cached, dict):
                return (
                    cached.get("metrics", pd.DataFrame()),
                    cached.get("breakdown", pd.DataFrame()),
                    cached.get("files", pd.DataFrame()),
                )
            with _timed_stage(stage_timings, f"tier3_non_gaap_{mode_name}", enabled=config.profile_timings):
                m_df, b_df, f_df = build_non_gaap_tier3(sec, cik_int, sub, max_quarters=config.max_quarters, mode=mode_name)
            _save_stage_cache(
                f"tier3_non_gaap_{mode_name}",
                tier3_key,
                {"metrics": m_df, "breakdown": b_df, "files": f_df},
            )
            return m_df, b_df, f_df

        if config.non_gaap_mode == "relaxed":
            adj_metrics, adj_breakdown, non_gaap_files = _load_or_build_tier3("relaxed")
        else:
            adj_metrics, adj_breakdown, non_gaap_files = _load_or_build_tier3("strict")
            if config.non_gaap_preview:
                adj_metrics_relaxed, adj_breakdown_relaxed, non_gaap_files_relaxed = _load_or_build_tier3("relaxed")

    # Last-resort local fallback for adjusted metrics (slides/transcripts) when EX-99 missing
    def _infer_q_from_filename(name: str) -> Optional[dt.date]:
        m = re.search(r"(20\d{2})[-_]?([01]\d)[-_]?([0-3]\d)", name)
        if m:
            try:
                d = dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                return _coerce_prev_quarter_end(d)
            except Exception:
                pass
        # handle both "Q2 2024" and "2024_Q2"
        m2 = re.search(r"Q([1-4])\s*(20\d{2})", name, re.IGNORECASE)
        if m2:
            q = int(m2.group(1))
            y = int(m2.group(2))
            return dt.date(y, 3 * q, 30 if q in (2, 3) else 31)
        m3 = re.search(r"(20\d{2})\s*[_-]?Q([1-4])", name, re.IGNORECASE)
        if m3:
            y = int(m3.group(1))
            q = int(m3.group(2))
            return dt.date(y, 3 * q, 30 if q in (2, 3) else 31)
        return None

    def _extract_text_from_file(p: Path) -> str:
        suf = p.suffix.lower()
        if suf in (".txt",):
            return p.read_text(encoding="utf-8", errors="ignore")
        if suf in (".htm", ".html"):
            return strip_html(p.read_text(encoding="utf-8", errors="ignore"))
        if suf == ".pdf":
            return extract_pdf_text_cached(
                p,
                cache_root=config.cache_dir,
                rebuild_cache=config.rebuild_doc_text_cache,
                quiet_pdf_warnings=config.quiet_pdf_warnings,
            )
        return ""

    existing_metrics_by_quarter_for_local_fallback: Dict[pd.Timestamp, set[str]] = {}

    def build_non_gaap_local_fallback() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        rows_m: List[Dict[str, Any]] = []
        rows_f: List[Dict[str, Any]] = []
        rows_seg: List[Dict[str, Any]] = []
        rows_debt: List[Dict[str, Any]] = []
        rows_guid: List[Dict[str, Any]] = []
        sources = [
            ("earnings_release", base_dir / "earnings_release"),
            ("earnings_release", base_dir / "Earnings Release"),
            ("earnings_release", base_dir / "Earnings Releases"),
            ("earnings_release", base_dir / "press_release"),
            ("earnings_release", base_dir / "Press Release"),
            ("slides", base_dir / "slides"),
            ("slides", base_dir / "earnings_presentation"),
            ("slides", base_dir / "Earnings Presentation"),
            ("transcripts", base_dir / "Earnings Transcripts"),
            ("transcripts", base_dir / "transcripts"),
            ("transcripts", base_dir / "earnings_transcripts"),
            ("other", base_dir / "annual_reports"),
            ("other", base_dir / "financial_statement"),
        ]
        if tkr_u:
            sources.extend(
                [
                    ("other", base_dir / f"{tkr_u}-10K"),
                    ("other", base_dir / f"{tkr_u}_10K"),
                    ("other", base_dir / f"{tkr_u} 10K"),
                ]
            )
        seen_q: set[pd.Timestamp] = set()
        def _detect_scale_txt(t: str) -> float:
            if re.search(r"\(\s*\$?\s*0{3}s?\s*\)|\$\s*0{3}s?\b|in\s+\$?0{3}s?", t, re.I):
                return 1000.0
            if re.search(r"in\s+millions", t, re.I):
                return 1_000_000.0
            if re.search(r"in\s+thousands", t, re.I):
                return 1000.0
            return 1.0

        def _years_3m_from_text(lines: List[str]) -> List[int]:
            years: List[int] = []
            for i, ln in enumerate(lines[:40]):
                if re.search(r"three months|quarter ended", ln, re.I):
                    yrs = [int(y) for y in re.findall(r"(20\d{2})", ln)]
                    if not yrs:
                        for j in range(1, 3):
                            if i + j < len(lines):
                                yrs.extend([int(y) for y in re.findall(r"(20\d{2})", lines[i + j])])
                    for y in yrs:
                        if y not in years:
                            years.append(y)
                    if years:
                        break
            return years

        def _slice_three_month_block_local(lines: List[str]) -> List[str]:
            start = None
            end = None
            for i, ln in enumerate(lines):
                if re.search(r"three\\s+months\\s+ended|quarter\\s+ended", ln, re.I):
                    start = i
                    continue
                if start is not None and re.search(r"six\\s+months|nine\\s+months|twelve\\s+months|year\\s+ended|fiscal\\s+year", ln, re.I):
                    # Allow header lines that show 3M/6M side by side before the data rows.
                    if i - start <= 3:
                        continue
                    end = i
                    break
            if start is not None:
                return lines[start:end] if end is not None else lines[start:]
            return lines

        def _three_month_end_from_text(txt: str) -> Optional[dt.date]:
            if not txt:
                return None
            # Prefer explicit "Three Months Ended <Month> <day>, <year>"
            m = re.search(r"three\s+months\s+ended\s+([A-Za-z]+)\s+(\d{1,2}),?\s*(\d{4})", txt, re.I)
            if m:
                try:
                    return pd.Timestamp(f"{m.group(1)} {m.group(2)} {m.group(3)}").date()
                except Exception:
                    pass
            # If year is not on the same line, try to infer from nearby year headers
            m2 = re.search(r"three\s+months\s+ended\s+([A-Za-z]+)\s+(\d{1,2})", txt, re.I)
            if m2:
                years = [int(y) for y in re.findall(r"(20\d{2})", txt[:800])]
                if years:
                    try:
                        y = max(years)
                        return pd.Timestamp(f"{m2.group(1)} {m2.group(2)} {y}").date()
                    except Exception:
                        pass
            return None

        def _pick_num_by_year(nums: List[float], years: List[int], q_end: Optional[dt.date]) -> Optional[float]:
            if not nums:
                return None
            if q_end is None or not years or len(nums) < 2:
                return nums[0]
            y = int(q_end.year)
            if y == years[0]:
                return nums[0]
            if len(years) > 1 and y == years[1]:
                return nums[1]
            return nums[0]

        def _parse_fcf_from_text(txt: str, q_end: Optional[dt.date]) -> Optional[float]:
            if not txt:
                return None
            lines = [re.sub(r"\s+", " ", ln).strip() for ln in txt.splitlines() if ln.strip()]
            lines_3m = _slice_three_month_block_local(lines)
            years = _years_3m_from_text(lines_3m)
            scale = _detect_scale_txt(txt)
            def _nums(line: str) -> List[float]:
                tokens = re.findall(r"\(?-?\d{1,3}(?:,\d{3})*(?:\.\d+)?\)?", line)
                nums: List[float] = []
                for t in tokens:
                    v = coerce_number(t)
                    if v is None:
                        continue
                    if 1900 <= float(v) <= 2100 and len(str(int(v))) == 4:
                        continue
                    nums.append(float(v) * scale)
                return nums

            for i, ln in enumerate(lines_3m):
                if "free cash flow" not in ln.lower():
                    continue
                nums = _nums(ln)
                if not nums:
                    for j in range(1, 3):
                        if i + j < len(lines_3m):
                            nums = _nums(lines_3m[i + j])
                            if nums:
                                break
                return _pick_num_by_year(nums, years, q_end)
            return None

        def _expand_quarter_ends(txt: str, q_end: Optional[dt.date]) -> List[dt.date]:
            if q_end is None:
                return []
            try:
                lines = [re.sub(r"\s+", " ", ln).strip() for ln in (txt or "").splitlines() if ln.strip()]
                lines_3m = _slice_three_month_block_local(lines)
                years = _years_3m_from_text(lines_3m)
            except Exception:
                years = []
            outs: List[dt.date] = []
            if years:
                for y in years:
                    try:
                        outs.append(dt.date(int(y), q_end.month, q_end.day))
                    except Exception:
                        continue
            if not outs:
                outs = [q_end]
            # preserve order (current-year first), unique
            seen: set[dt.date] = set()
            uniq: List[dt.date] = []
            for d in outs:
                if d not in seen:
                    uniq.append(d)
                    seen.add(d)
            return uniq

        def _missing_non_gaap_quarters(txt: str, q_end: Optional[dt.date]) -> List[dt.date]:
            return _filter_missing_local_non_gaap_metric_quarters(
                _expand_quarter_ends(txt, q_end),
                existing_metrics_by_quarter_for_local_fallback,
            )

        def _score_page(text: str) -> Dict[str, int]:
            t = (text or "").lower()
            score = {"non_gaap": 0, "segment": 0, "debt": 0, "guidance": 0}
            if "reconciliation of reported net income" in t:
                score["non_gaap"] += 5
            if "reconciliation of reported" in t or "reconciliation of reported consolidated results" in t:
                score["non_gaap"] += 4
            if "reconciliation" in t and "adjusted ebitda" in t:
                score["non_gaap"] += 3
            if "adjusted ebitda" in t and "adjusted ebit" in t:
                score["non_gaap"] += 2
            if "adjusted ebitda" in t and ("net income" in t or "net loss" in t):
                score["non_gaap"] += 2
            if "adjusted diluted earnings per share" in t:
                score["non_gaap"] += 2
            if "free cash flow" in t and "capital expenditures" in t:
                score["non_gaap"] += 2
            if "adjusted segment ebit" in t or "reportable segments" in t or "adjusted segment ebitda" in t:
                score["segment"] += 3
            if "sending technology" in t or "presort" in t:
                score["segment"] += 2
            if "debt profile" in t or "credit agreement" in t:
                score["debt"] += 3
            if "revolving credit facility" in t or "aggregate commitments" in t:
                score["debt"] += 2
            if "guidance" in t or "outlook" in t:
                score["guidance"] += 2
            if "fy" in t and ("guidance" in t or "outlook" in t):
                score["guidance"] += 1
            # Avoid treating segment pages as consolidated adjusted results unless explicitly reconciled.
            if ("adjusted segment" in t or "reportable segments" in t) and "reconciliation of reported" not in t:
                score["non_gaap"] = min(score["non_gaap"], 1)
            return score

        def _parse_segment_from_text(txt: str, q_end: Optional[dt.date]) -> List[Dict[str, Any]]:
            if not txt or q_end is None:
                return []
            lines = [re.sub(r"\s+", " ", ln).strip() for ln in txt.splitlines() if ln.strip()]
            lines_3m = _slice_three_month_block_local(lines)
            years = _years_3m_from_text(lines_3m)
            scale = _detect_scale_txt(txt)

            def _pick3(nums: List[float]) -> Optional[List[float]]:
                if not nums:
                    return None
                if len(nums) >= 6:
                    nums = nums[:6]
                if len(nums) >= 3:
                    if len(nums) >= 6 and years:
                        y = int(q_end.year)
                        if y == years[0]:
                            return nums[:3]
                        if len(years) > 1 and y == years[1]:
                            return nums[3:6]
                    return nums[:3]
                return None

            seg_rows: List[Dict[str, Any]] = []
            for ln in lines_3m:
                l = ln.lower()
                if not ("sending technology" in l or "presort" in l or "total reportable" in l):
                    continue
                ln_clean = re.sub(r"\(?-?\d+(?:\.\d+)?%\)?", "", ln)
                nums = []
                for t in re.findall(r"\(?-?\d{1,3}(?:,\d{3})*(?:\.\d+)?\)?", ln_clean):
                    v = coerce_number(t)
                    if v is None:
                        continue
                    if 1900 <= float(v) <= 2100 and len(str(int(v))) == 4:
                        continue
                    nums.append(float(v) * scale)
                trio = _pick3(nums)
                if not trio:
                    continue
                seg_name = "Total reportable segments"
                if "sending technology" in l:
                    seg_name = "Sending Technology Solutions"
                elif "presort" in l:
                    seg_name = "Presort Services"
                seg_rows.append({"quarter": q_end, "segment": seg_name, "metric": "adj_segment_ebit", "value": trio[0], "unit": "USD"})
                seg_rows.append({"quarter": q_end, "segment": seg_name, "metric": "adj_segment_da", "value": trio[1], "unit": "USD"})
                seg_rows.append({"quarter": q_end, "segment": seg_name, "metric": "adj_segment_ebitda", "value": trio[2], "unit": "USD"})
            return seg_rows

        def _parse_debt_profile_from_text(txt: str, q_end: Optional[dt.date]) -> List[Dict[str, Any]]:
            if not txt:
                return []
            lines = [re.sub(r"\s+", " ", ln).strip() for ln in txt.splitlines() if ln.strip()]
            out: List[Dict[str, Any]] = []
            if not lines:
                return out
            scale = _detect_scale_txt(txt)

            def _parse_mmddyyyy(token: str) -> Optional[dt.date]:
                m = re.match(r"^\s*(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})\s*$", token)
                if not m:
                    return None
                mm = int(m.group(1))
                dd = int(m.group(2))
                yy_raw = int(m.group(3))
                yy = yy_raw if yy_raw >= 100 else (2000 + yy_raw if yy_raw <= 69 else 1900 + yy_raw)
                try:
                    return dt.date(yy, mm, dd)
                except Exception:
                    return None

            def _amount_tokens(line_txt: str) -> List[str]:
                # Keep only amount-like columns ($000-style with commas) and dash placeholders.
                return re.findall(r"(?:\(?\d{1,3}(?:,\d{3})+\)?|[—–-])", line_txt)

            as_of_idx = 0
            as_of_match_found = False
            for ln in lines[:35]:
                dt_tokens = re.findall(r"(?:0?[1-9]|1[0-2])[/-](?:0?[1-9]|[12]\d|3[01])[/-](?:\d{2}|\d{4})", ln)
                if len(dt_tokens) < 2:
                    continue
                dates = [_parse_mmddyyyy(tk) for tk in dt_tokens]
                dates = [d for d in dates if d is not None]
                if len(dates) < 2:
                    continue
                if q_end is not None:
                    exact = [i for i, d in enumerate(dates) if abs((d - q_end).days) <= 7]
                    if exact:
                        as_of_idx = int(exact[0])
                        as_of_match_found = True
                        break
                    same_year = [i for i, d in enumerate(dates) if d.year == q_end.year]
                    if same_year:
                        as_of_idx = int(same_year[0])
                        as_of_match_found = True
                        break
                as_of_idx = 0
                as_of_match_found = True
                break

            for ln in lines:
                l = ln.lower()
                amt_cols = _amount_tokens(ln)
                if "principal amount" in l and amt_cols:
                    if as_of_idx < len(amt_cols):
                        v = coerce_number(amt_cols[as_of_idx])
                        if v is not None and float(v) > 0:
                            out.append(
                                {
                                    "quarter": q_end,
                                    "tranche": "Principal amount",
                                    "amount": float(v) * scale,
                                    "maturity_year": None,
                                    "unit": "USD",
                                    "is_table_total": True,
                                    "asof_col_idx": as_of_idx,
                                    "asof_match_found": as_of_match_found,
                                }
                            )
                    continue
                if "due" not in l:
                    continue
                if not re.search(r"\b(20\d{2})\b", l):
                    continue
                if not amt_cols:
                    continue
                if as_of_idx >= len(amt_cols):
                    continue
                tok = amt_cols[as_of_idx]
                if tok in {"-", "—", "–"}:
                    continue
                v = coerce_number(tok)
                if v is None or float(v) <= 0:
                    continue
                # Ensure this is a real debt instrument row, not textual boilerplate.
                if not re.search(r"(term\s+loan|notes?\s+due|convertible|debentures?)", l):
                    continue
                m = re.search(r"\b(20\d{2})\b", ln)
                my = int(m.group(1)) if m else None
                out.append(
                    {
                        "quarter": q_end,
                        "tranche": ln[:180],
                        "amount": float(v) * scale,
                        "maturity_year": my,
                        "unit": "USD",
                        "is_table_total": False,
                        "asof_col_idx": as_of_idx,
                        "asof_match_found": as_of_match_found,
                    }
                )
            return out

        def _parse_guidance_from_text(txt: str, q_end: Optional[dt.date]) -> List[Dict[str, Any]]:
            if not txt:
                return []
            lines = [re.sub(r"\s+", " ", ln).strip() for ln in txt.splitlines() if ln.strip()]
            out: List[Dict[str, Any]] = []
            if not lines:
                return out
            anchor_re = re.compile(
                r"\b(guidance|outlook|financial outlook|updated guidance|full[- ]year outlook|next[- ]year outlook|targets?)\b",
                re.I,
            )
            intent_re = re.compile(
                r"\b(guidance|outlook|expect|expects|forecast|target|targets|plan|plans|intend|anticipate|reaffirm|maintain|raise|lower)\b",
                re.I,
            )
            metric_terms: Dict[str, List[str]] = {
                "Revenue": ["revenue", "sales", "top line"],
                "Adj EBITDA": ["adjusted ebitda", "adj ebitda", "ebitda"],
                "Adj EPS": [
                    "adjusted eps",
                    "adj eps",
                    "adjusted diluted eps",
                    "adjusted diluted earnings per share",
                    "adjusted earnings per share",
                    "earnings per share",
                    "eps",
                ],
                "FCF": ["free cash flow", "fcf", "free cash flow excluding"],
                "Capex": ["capex", "capital expenditures", "capital spending"],
                "Cost savings": ["cost savings", "savings", "run-rate savings", "annualized savings"],
                "Restructuring charges": ["restructuring", "transformation charges", "special items"],
                "Net debt / leverage": ["net leverage", "leverage", "net debt", "debt/ebitda"],
            }
            anti_re = re.compile(
                r"\b(performance obligations|transaction price allocated|recognized as follows|"
                r"securities act|registration exempt|forward-looking statements|safe harbor|"
                r"private securities litigation reform act|pslra|costs are amortized in a manner consistent|"
                r"contract performance period|not anticipated to be material|not expected to be material|"
                r"expected credit losses|reasonable and supportable forecast)\b",
                re.I,
            )
            num_re = re.compile(
                r"(\$?\s*[0-9]{1,4}(?:,[0-9]{3})*(?:\.[0-9]+)?\s*(?:bn|billion|m|million|%|bps|x)?"
                r"(?:\s*(?:to|through|\-|–|—)\s*\$?\s*[0-9]{1,4}(?:,[0-9]{3})*(?:\.[0-9]+)?\s*(?:bn|billion|m|million|%|bps|x)?)?)",
                re.I,
            )

            def _metric_hint(line_low: str) -> str:
                if "revenue" in line_low or "sales" in line_low or "top line" in line_low:
                    return "Revenue"
                if "cost savings" in line_low:
                    return "Cost savings"
                if "free cash flow" in line_low or re.search(r"\bfcf\b", line_low):
                    return "FCF"
                if "capex" in line_low or "capital expenditures" in line_low:
                    return "Capex"
                for m_name, kws in metric_terms.items():
                    if any(kw in line_low for kw in kws):
                        return m_name
                return "Other"

            def _num_tokens(line_txt: str) -> List[Tuple[str, float]]:
                out_nums: List[Tuple[str, float]] = []
                for m in num_re.finditer(line_txt):
                    raw = str(m.group(0) or "").strip()
                    raw_clean = raw.replace("$", "").replace(",", "").replace("%", "").replace("x", "").replace("bps", "")
                    try:
                        v = float(raw_clean)
                    except Exception:
                        continue
                    # Drop standalone years used as headers (2025/2026 etc.).
                    if re.fullmatch(r"\d{4}", raw_clean) and 1900 <= int(float(raw_clean)) <= 2100:
                        continue
                    out_nums.append((raw, v))
                return out_nums

            def _extract_low_high_metric_rows(line_txt: str, near_anchor_idx: Optional[int]) -> List[Tuple[str, str, str, Optional[str]]]:
                out_rows: List[Tuple[str, str, str, Optional[str]]] = []
                if not line_txt:
                    return out_rows
                ll = line_txt.lower()
                if not re.search(r"\blow\b.{0,40}\bhigh\b", ll):
                    return out_rows
                work = line_txt
                m_outlook = re.search(r"(full[- ]year\s+outlook|guidance|outlook)", work, re.I)
                if m_outlook:
                    work = work[m_outlook.start():]
                m_lh = re.search(r"\blow\b.{0,20}\bhigh\b", work, re.I)
                if m_lh:
                    work = work[m_lh.end():]
                year_hint = None
                for probe in [line_txt, lines[near_anchor_idx] if near_anchor_idx is not None and 0 <= near_anchor_idx < len(lines) else ""]:
                    m_y = re.search(r"\b(20\d{2})\b", str(probe or ""))
                    if m_y:
                        year_hint = m_y.group(1)
                        break

                metric_map = [
                    ("Revenue", r"revenue|sales|top line"),
                    ("Adj EBIT", r"adjusted\s+ebit|adj\.?\s+ebit"),
                    ("Adj EBITDA", r"adjusted\s+ebitda|adj\.?\s+ebitda"),
                    ("Adj EPS", r"adjusted\s+eps|adj\.?\s+eps|earnings\s+per\s+share|eps"),
                    ("FCF", r"free\s+cash\s+flow|\bfcf\b"),
                    ("Capex", r"capex|capital expenditures|capital spending"),
                    ("Cost savings", r"cost savings|savings"),
                ]
                for metric_name, mpat in metric_map:
                    pat = re.compile(
                        rf"(?:{mpat})[\s:\-,$()%]{{0,16}}\$?\s*([0-9]{{1,4}}(?:,[0-9]{{3}})*(?:\.[0-9]+)?)\s*(bn|billion|m|million|%|bps|x)?"
                        rf"[\s:\-,$()%]{{0,12}}\$?\s*([0-9]{{1,4}}(?:,[0-9]{{3}})*(?:\.[0-9]+)?)\s*(bn|billion|m|million|%|bps|x)?",
                        re.I,
                    )
                    mm = pat.search(work)
                    if not mm:
                        continue
                    lo = str(mm.group(1) or "")
                    hi = str(mm.group(3) or "")
                    unit = str(mm.group(2) or mm.group(4) or "").lower()
                    lo_disp = f"${lo}" if unit not in {"%", "bps", "x"} else f"{lo}{unit}"
                    hi_disp = f"${hi}" if unit not in {"%", "bps", "x"} else f"{hi}{unit}"
                    per = f" for FY {year_hint}" if year_hint else ""
                    out_rows.append((f"{metric_name} guidance {lo_disp} to {hi_disp}{per}", lo_disp, hi_disp, year_hint))
                return out_rows

            anchor_idx: List[int] = [i for i, ln in enumerate(lines) if anchor_re.search(ln)]
            seen: set[str] = set()
            for i, ln in enumerate(lines):
                ll = ln.lower()
                if anti_re.search(ll):
                    continue
                if re.search(r"\$\s*change|%\s*change", ll):
                    continue
                near_anchor = any(abs(i - j) <= 18 for j in anchor_idx)
                has_intent = bool(intent_re.search(ll))
                metric_hint = _metric_hint(ll)
                num_parts = _num_tokens(ln)
                nums = [x[0] for x in num_parts]
                has_numeric = len(nums) > 0
                nearest_anchor = min(anchor_idx, key=lambda j: abs(i - j)) if (near_anchor and anchor_idx) else None

                # Handle "Low / High" outlook tables where ranges are listed without "to".
                table_rows = _extract_low_high_metric_rows(ln, nearest_anchor)
                if table_rows and (near_anchor or has_intent):
                    for row_text, lo_disp, hi_disp, _yh in table_rows:
                        line_disp = row_text[:320]
                        key = re.sub(r"\s+", " ", line_disp).strip().lower()
                        if key in seen:
                            continue
                        seen.add(key)
                        out.append(
                            {
                                "quarter": q_end,
                                "line": line_disp,
                                "numbers": f"{lo_disp}, {hi_disp}",
                                "metric_hint": _metric_hint(line_disp.lower()),
                            }
                        )
                    continue

                if not has_numeric and i + 1 < len(lines):
                    nxt = lines[i + 1]
                    nxt_low = nxt.lower()
                    if not anti_re.search(nxt_low):
                        nxt_parts = _num_tokens(nxt)
                        nxt_nums = [x[0] for x in nxt_parts]
                        if nxt_nums and (metric_hint != "Other" or has_intent or near_anchor):
                            ln = f"{ln} {nxt}"
                            ll = ln.lower()
                            nums = nxt_nums
                            num_parts = nxt_parts
                            has_numeric = True

                if not has_numeric:
                    continue
                if not (has_intent or near_anchor):
                    continue
                if metric_hint == "Other" and not (has_intent or near_anchor):
                    continue
                if re.search(r"\bprovides?\s+the\s+following\s+guidance\s+for\b", ll) and metric_hint == "Other":
                    continue
                if len(re.findall(r"[A-Za-z]", ln)) < 16:
                    continue

                # Guidance tables often show "Metric | Low | High" without "to/-".
                if metric_hint != "Other" and len(nums) >= 2 and not re.search(r"\b(to|through|between)\b|\-|\u2013|\u2014", ln):
                    year_hint = None
                    nearest_anchor = None
                    if near_anchor and anchor_idx:
                        nearest_anchor = min(anchor_idx, key=lambda j: abs(i - j))
                    if nearest_anchor is not None:
                        m_y = re.search(r"\b(20\d{2})\b", lines[nearest_anchor])
                        if m_y:
                            year_hint = m_y.group(1)
                    n1 = float(num_parts[0][1]) if num_parts else None
                    n2 = float(num_parts[1][1]) if len(num_parts) > 1 else None
                    if (
                        metric_hint in {"Revenue", "Adj EBITDA", "FCF", "Capex", "Cost savings", "Restructuring charges"}
                        and n1 is not None
                        and n2 is not None
                        and abs(n1) < 10
                        and abs(n2) > 50
                    ):
                        continue
                    if year_hint and not re.search(r"\b20\d{2}\b", ln):
                        ln = f"{metric_hint} guidance {nums[0]} to {nums[1]} for FY {year_hint}"
                    else:
                        ln = f"{metric_hint} guidance {nums[0]} to {nums[1]}"

                line_disp = ln[:320]
                key = re.sub(r"\s+", " ", line_disp).strip().lower()
                if key in seen:
                    continue
                seen.add(key)
                out.append(
                    {
                        "quarter": q_end,
                        "line": line_disp,
                        "numbers": ", ".join(nums[:8]),
                        "metric_hint": metric_hint,
                    }
                )
            return out

        def _page_is_recon(txt: str) -> bool:
            t = (txt or "").lower()
            if "adjusted segment" in t or "reportable segments" in t:
                return False
            if "reconciliation of reported net income" in t and "adjusted ebitda" in t:
                return True
            if "reconciliation of reported consolidated results" in t and "adjusted ebitda" in t:
                return True
            if "reconciliation of reported" in t and "adjusted ebitda" in t:
                return True
            if "adjusted ebitda" in t and "adjusted ebit" in t and "reported net income" in t:
                return True
            if "adjusted ebitda" in t and "adjusted net income" in t:
                return True
            # Fallback: adjusted EBITDA + net income on the same page (common in slides)
            if "adjusted ebitda" in t and ("net income" in t or "net loss" in t) and ("adjusted ebit" in t or "adjusted net income" in t):
                return True
            return False

        def _ocr_page_text(page, cache_key: Optional[str] = None, cache_dir: Optional[Path] = None) -> str:
            try:
                import pytesseract  # type: ignore
                from PIL import Image  # type: ignore
            except Exception:
                return ""
            cache_path = None
            if cache_key:
                try:
                    cache_dir_use = cache_dir or (preferred_ticker_cache_root_from_base_dir(base_dir) / "slides_ocr")
                    cache_dir_use.mkdir(parents=True, exist_ok=True)
                    cache_path = cache_dir_use / f"{cache_key}.txt"
                    if cache_path.exists():
                        return cache_path.read_text(encoding="utf-8", errors="ignore")
                except Exception:
                    cache_path = None
            try:
                im = page.to_image(resolution=300).original
            except Exception:
                return ""
            try:
                txt = pytesseract.image_to_string(im)
                if cache_path is not None and txt:
                    try:
                        cache_path.write_text(txt, encoding="utf-8", errors="ignore")
                    except Exception:
                        pass
                return txt
            except Exception:
                return ""

        pages_per_q: Dict[pd.Timestamp, int] = {}
        rows_m_candidates: List[Dict[str, Any]] = []
        pdf_manifest_path = preferred_ticker_cache_root_from_base_dir(base_dir) / "local_non_gaap_pdf_manifest.json"
        pdf_manifest: Dict[str, Any] = {"version": 1, "files": {}}
        try:
            if pdf_manifest_path.exists():
                pdf_manifest = json.loads(pdf_manifest_path.read_text(encoding="utf-8", errors="ignore"))
        except Exception:
            pdf_manifest = {"version": 1, "files": {}}
        for src_name, folder in sources:
            if not folder.exists():
                continue
            files = sorted(folder.rglob("*"))
            for p in files[:200]:
                if not p.is_file():
                    continue
                if p.suffix.lower() not in (".txt", ".htm", ".html", ".pdf"):
                    continue
                # For slides, skip very old decks outside the configured window (perf).
                if src_name == "slides" and config.min_year:
                    q_hint = _infer_q_from_filename(p.name)
                    if q_hint is not None and q_hint.year < int(config.min_year):
                        continue
                if p.suffix.lower() == ".pdf":
                    try:
                        import pdfplumber  # type: ignore
                    except Exception:
                        continue
                    try:
                        use_cache_only = False
                        cached_pages = None
                        text_cache_dir = None
                        ocr_cache_dir = None
                        try:
                            st = p.stat()
                            try:
                                manifest_key = str(p.resolve())
                            except Exception:
                                manifest_key = str(p)
                            entry = (pdf_manifest.get("files", {}) or {}).get(manifest_key)
                            if entry and entry.get("mtime") == st.st_mtime and entry.get("size") == st.st_size and entry.get("pages"):
                                cached_pages = int(entry.get("pages"))
                                use_cache_only = True
                        except Exception:
                            use_cache_only = False
                        try:
                            text_cache_dir, ocr_cache_dir = _local_non_gaap_pdf_cache_dirs(base_dir, src_name)
                            text_cache_dir.mkdir(parents=True, exist_ok=True)
                            ocr_cache_dir.mkdir(parents=True, exist_ok=True)
                        except Exception:
                            text_cache_dir = None
                            ocr_cache_dir = None

                        def _read_cached_page(cache_key: str) -> str:
                            for d in (text_cache_dir, ocr_cache_dir):
                                if d is None:
                                    continue
                                try:
                                    pth = d / f"{cache_key}.txt"
                                    if pth.exists():
                                        return pth.read_text(encoding="utf-8", errors="ignore")
                                except Exception:
                                    continue
                            return ""

                        if use_cache_only and cached_pages:
                            for idx in range(cached_pages):
                                cache_key = _local_non_gaap_pdf_cache_key(p, src_name=src_name, page_number=idx + 1)
                                txt = _read_cached_page(cache_key)
                                if not txt:
                                    continue
                                scores = _score_page(txt)
                                if max(scores.values()) == 0:
                                    continue
                                q_end = _three_month_end_from_text(txt) or _infer_q_from_filename(p.name) or infer_quarter_end_from_text(txt)
                                if q_end is None:
                                    continue
                                if scores.get("non_gaap", 0) >= 2 and _page_is_recon(txt):
                                    for q_end_use in _missing_non_gaap_quarters(txt, q_end):
                                        q_ts = pd.Timestamp(q_end_use)
                                        if pages_per_q.get(q_ts, 0) >= 2:
                                            continue
                                        aebit, aebitda, aeps, adj, status, col_label = parse_adjusted_from_plain_text(txt, q_end_use, mode="relaxed")
                                        if status in ("ok_ocr", "ok_relaxed_ocr"):
                                            fcf_val = _parse_fcf_from_text(txt, q_end_use)
                                            rows_m_candidates.append({
                                                "quarter": q_end_use,
                                                "adj_ebit": aebit,
                                                "adj_ebitda": aebitda,
                                                "adj_eps": aeps,
                                                "adj_fcf": fcf_val,
                                                "source": src_name,
                                                "source_type": "earnings_deck",
                                                "accn": None,
                                                "filed": None,
                                                "doc": str(p),
                                                "page": idx + 1,
                                                "confidence": "low",
                                                "col": col_label,
                                                "source_snippet": "Adjusted EBITDA row",
                                                "score": scores.get("non_gaap", 0),
                                            })
                                            rows_f.append({
                                                "accn": None,
                                                "filed": None,
                                                "status": "ok_local",
                                                "doc": str(p),
                                                "quarter": str(q_end_use),
                                                "col": col_label,
                                                "source": src_name,
                                                "page": idx + 1,
                                            })
                                            seen_q.add(q_ts)
                                            pages_per_q[q_ts] = pages_per_q.get(q_ts, 0) + 1
                                if scores.get("segment", 0) >= 2:
                                    seg_rows = _parse_segment_from_text(txt, q_end)
                                    if seg_rows:
                                        for r0 in seg_rows:
                                            r0.update({"doc": str(p), "page": idx + 1, "source": src_name})
                                        rows_seg.extend(seg_rows)
                                if scores.get("debt", 0) >= 2:
                                    debt_rows = _parse_debt_profile_from_text(txt, q_end)
                                    if debt_rows:
                                        for r0 in debt_rows:
                                            r0.update({"doc": str(p), "page": idx + 1, "source": src_name})
                                        rows_debt.extend(debt_rows)
                                if scores.get("guidance", 0) >= 2:
                                    guid_rows = _parse_guidance_from_text(txt, q_end)
                                    if guid_rows:
                                        for r0 in guid_rows:
                                            r0.update({"doc": str(p), "page": idx + 1, "source": src_name})
                                        rows_guid.extend(guid_rows)
                            continue
                        with silence_pdfminer_warnings(enabled=config.quiet_pdf_warnings):
                            with pdfplumber.open(str(p)) as pdf:
                                n_pages = len(pdf.pages)
                                for idx, page in enumerate(pdf.pages):
                                    txt = ""
                                    cache_key = _local_non_gaap_pdf_cache_key(p, src_name=src_name, page_number=idx + 1)
                                    if text_cache_dir is not None:
                                        try:
                                            cache_path = text_cache_dir / f"{cache_key}.txt"
                                            if cache_path.exists():
                                                txt = cache_path.read_text(encoding="utf-8", errors="ignore")
                                        except Exception:
                                            txt = ""
                                    if not txt:
                                        txt = page.extract_text() or ""
                                        if text_cache_dir is not None:
                                            try:
                                                cache_path = text_cache_dir / f"{cache_key}.txt"
                                                if not cache_path.exists():
                                                    cache_path.write_text(txt or "", encoding="utf-8", errors="ignore")
                                            except Exception:
                                                pass
                                    # text-first, OCR only if low text
                                    if txt is None:
                                        txt = ""
                                    hint_txt = txt.lower()
                                    has_hint = any(
                                        k in hint_txt
                                        for k in (
                                            "adjusted ebitda",
                                            "adjusted ebit",
                                            "reconciliation",
                                            "adjusted diluted earnings",
                                            "free cash flow",
                                            "appendix: financial information",
                                        )
                                    )
                                    if len(txt.strip()) < 200:
                                        # OCR only if the page is likely relevant or near the end (slides appendices).
                                        if src_name == "slides" and (has_hint or idx >= max(0, n_pages - 6)):
                                            ocr_txt = _ocr_page_text(page, cache_key=cache_key, cache_dir=ocr_cache_dir)
                                            if ocr_txt and len(ocr_txt) > len(txt):
                                                txt = ocr_txt
                                        elif src_name != "slides" and has_hint:
                                            ocr_txt = _ocr_page_text(page, cache_key=cache_key, cache_dir=ocr_cache_dir)
                                            if ocr_txt and len(ocr_txt) > len(txt):
                                                txt = ocr_txt
                                    scores = _score_page(txt)
                                    # For slides: OCR can recover key lines even when text exists but is low-signal.
                                    if src_name == "slides" and scores.get("non_gaap", 0) < 2 and (has_hint or idx >= max(0, n_pages - 6)):
                                        ocr_txt = _ocr_page_text(page, cache_key=cache_key, cache_dir=ocr_cache_dir)
                                        if ocr_txt and len(ocr_txt) > len(txt):
                                            txt = ocr_txt
                                            scores = _score_page(txt)
                                    if max(scores.values()) == 0 and "appendix: financial information" in txt.lower():
                                        ocr_txt = _ocr_page_text(page, cache_key=cache_key, cache_dir=ocr_cache_dir)
                                        if ocr_txt and len(ocr_txt) > len(txt):
                                            txt = ocr_txt
                                            scores = _score_page(txt)
                                    if max(scores.values()) == 0:
                                        continue
                                    q_end = _three_month_end_from_text(txt) or _infer_q_from_filename(p.name) or infer_quarter_end_from_text(txt)
                                if q_end is None:
                                    continue
                                if scores.get("non_gaap", 0) >= 2 and _page_is_recon(txt):
                                    for q_end_use in _missing_non_gaap_quarters(txt, q_end):
                                        q_ts = pd.Timestamp(q_end_use)
                                        if pages_per_q.get(q_ts, 0) >= 2:
                                            continue
                                        aebit, aebitda, aeps, adj, status, col_label = parse_adjusted_from_plain_text(txt, q_end_use, mode="relaxed")
                                        if status in ("ok_ocr", "ok_relaxed_ocr"):
                                            fcf_val = _parse_fcf_from_text(txt, q_end_use)
                                            rows_m_candidates.append({
                                                "quarter": q_end_use,
                                                "adj_ebit": aebit,
                                                "adj_ebitda": aebitda,
                                                "adj_eps": aeps,
                                                "adj_fcf": fcf_val,
                                                "source": src_name,
                                                "source_type": "earnings_deck",
                                                "accn": None,
                                                "filed": None,
                                                "doc": str(p),
                                                "page": idx + 1,
                                                "confidence": "low",
                                                "col": col_label,
                                                "source_snippet": "Adjusted EBITDA row",
                                                "score": scores.get("non_gaap", 0),
                                            })
                                            rows_f.append({
                                                "accn": None,
                                                "filed": None,
                                                "status": "ok_local",
                                                "doc": str(p),
                                                "quarter": str(q_end_use),
                                                "col": col_label,
                                                "source": src_name,
                                                "page": idx + 1,
                                            })
                                            seen_q.add(q_ts)
                                            pages_per_q[q_ts] = pages_per_q.get(q_ts, 0) + 1
                                if scores.get("segment", 0) >= 2:
                                    seg_rows = _parse_segment_from_text(txt, q_end)
                                    if seg_rows:
                                        for r0 in seg_rows:
                                            r0.update({"doc": str(p), "page": idx + 1, "source": src_name})
                                        rows_seg.extend(seg_rows)
                                if scores.get("debt", 0) >= 2:
                                    debt_rows = _parse_debt_profile_from_text(txt, q_end)
                                    if debt_rows:
                                        for r0 in debt_rows:
                                            r0.update({"doc": str(p), "page": idx + 1, "source": src_name})
                                        rows_debt.extend(debt_rows)
                                if scores.get("guidance", 0) >= 2:
                                    guid_rows = _parse_guidance_from_text(txt, q_end)
                                    if guid_rows:
                                        for r0 in guid_rows:
                                            r0.update({"doc": str(p), "page": idx + 1, "source": src_name})
                                        rows_guid.extend(guid_rows)
                            try:
                                st = p.stat()
                                try:
                                    manifest_key = str(p.resolve())
                                except Exception:
                                    manifest_key = str(p)
                                pdf_manifest.setdefault("files", {})[manifest_key] = {
                                    "mtime": st.st_mtime,
                                    "size": st.st_size,
                                    "pages": n_pages,
                                }
                            except Exception:
                                pass
                    except Exception:
                        continue
                else:
                    txt = _extract_text_from_file(p)
                    if not txt or len(txt) < 50:
                        continue
                    scores = _score_page(txt)
                    if max(scores.values()) == 0:
                        continue
                    q_end = _three_month_end_from_text(txt) or _infer_q_from_filename(p.name) or infer_quarter_end_from_text(txt)
                    if q_end is None:
                        continue
                    if scores.get("non_gaap", 0) >= 2 and _page_is_recon(txt):
                        for q_end_use in _missing_non_gaap_quarters(txt, q_end):
                            q_ts = pd.Timestamp(q_end_use)
                            if pages_per_q.get(q_ts, 0) >= 2:
                                continue
                            aebit, aebitda, aeps, adj, status, col_label = parse_adjusted_from_plain_text(txt, q_end_use, mode="relaxed")
                            if status in ("ok_ocr", "ok_relaxed_ocr"):
                                fcf_val = _parse_fcf_from_text(txt, q_end_use)
                                rows_m_candidates.append({
                                    "quarter": q_end_use,
                                    "adj_ebit": aebit,
                                    "adj_ebitda": aebitda,
                                    "adj_eps": aeps,
                                    "adj_fcf": fcf_val,
                                    "source": src_name,
                                    "source_type": "earnings_deck",
                                    "accn": None,
                                    "filed": None,
                                    "doc": str(p),
                                    "page": None,
                                    "confidence": "low",
                                    "col": col_label,
                                    "source_snippet": "Adjusted EBITDA row",
                                    "score": scores.get("non_gaap", 0),
                                })
                                rows_f.append({
                                    "accn": None,
                                    "filed": None,
                                    "status": "ok_local",
                                    "doc": str(p),
                                    "quarter": str(q_end_use),
                                    "col": col_label,
                                    "source": src_name,
                                    "page": None,
                                })
                                seen_q.add(q_ts)
                                pages_per_q[q_ts] = pages_per_q.get(q_ts, 0) + 1
                    if scores.get("segment", 0) >= 2:
                        seg_rows = _parse_segment_from_text(txt, q_end)
                        if seg_rows:
                            for r0 in seg_rows:
                                r0.update({"doc": str(p), "page": None, "source": src_name})
                            rows_seg.extend(seg_rows)
                    if scores.get("debt", 0) >= 2:
                        debt_rows = _parse_debt_profile_from_text(txt, q_end)
                        if debt_rows:
                            for r0 in debt_rows:
                                r0.update({"doc": str(p), "page": None, "source": src_name})
                            rows_debt.extend(debt_rows)
                    if scores.get("guidance", 0) >= 2:
                        guid_rows = _parse_guidance_from_text(txt, q_end)
                        if guid_rows:
                            for r0 in guid_rows:
                                r0.update({"doc": str(p), "page": None, "source": src_name})
                            rows_guid.extend(guid_rows)
        # Deduplicate adjusted metrics per quarter without collapsing distinct metrics
        # into a single winning row. We keep the best available value per metric.
        df_m = pd.DataFrame(rows_m_candidates)
        if not df_m.empty and "quarter" in df_m.columns:
            df_m["quarter"] = pd.to_datetime(df_m["quarter"], errors="coerce")
            df_m = df_m[df_m["quarter"].notna()]
            if not df_m.empty:
                df_m["score"] = pd.to_numeric(df_m.get("score"), errors="coerce").fillna(0)
                metric_cols = ["adj_ebit", "adj_ebitda", "adj_eps", "adj_fcf"]
                merged_rows: List[Dict[str, Any]] = []
                for qv, sub in df_m.groupby("quarter", sort=True):
                    sub = sub.copy()
                    for metric_col in metric_cols:
                        sub[f"{metric_col}_num"] = pd.to_numeric(sub.get(metric_col), errors="coerce")
                        sub[f"{metric_col}_nonnull"] = sub[f"{metric_col}_num"].notna().astype(int)
                        sub[f"{metric_col}_abs"] = sub[f"{metric_col}_num"].abs()
                    sub["_metric_count"] = sub[[f"{metric_col}_nonnull" for metric_col in metric_cols]].sum(axis=1)
                    base = (
                        sub.sort_values(["score", "_metric_count"], ascending=[False, False])
                        .iloc[0]
                        .to_dict()
                    )
                    merged = dict(base)
                    merged["quarter"] = qv
                    merged["confidence"] = merged.get("confidence") or "low"
                    merged_sources: List[str] = []
                    for metric_col in metric_cols:
                        metric_sub = sub[sub[f"{metric_col}_nonnull"] > 0].copy()
                        if metric_sub.empty:
                            merged[metric_col] = pd.NA
                            continue
                        metric_best = (
                            metric_sub.sort_values(
                                ["score", f"{metric_col}_abs"],
                                ascending=[False, False],
                            )
                            .iloc[0]
                            .to_dict()
                        )
                        merged[metric_col] = metric_best.get(metric_col)
                        metric_doc = str(metric_best.get("doc") or "").strip()
                        metric_page = metric_best.get("page")
                        if metric_doc:
                            merged_sources.append(f"{metric_col}:{metric_doc}{f' p.{metric_page}' if pd.notna(metric_page) else ''}")
                    if merged_sources:
                        merged["source_snippet"] = "Merged local fallback metrics | " + " | ".join(merged_sources[:4])
                    merged_rows.append(merged)
                df_m = pd.DataFrame(merged_rows)
                df_m = df_m.drop(
                    columns=[
                        *[f"{metric_col}_num" for metric_col in metric_cols],
                        *[f"{metric_col}_nonnull" for metric_col in metric_cols],
                        *[f"{metric_col}_abs" for metric_col in metric_cols],
                        "_metric_count",
                    ],
                    errors="ignore",
                )
        try:
            if pdf_manifest_path:
                pdf_manifest_path.parent.mkdir(parents=True, exist_ok=True)
                pdf_manifest_path.write_text(json.dumps(pdf_manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass
        return df_m, pd.DataFrame(rows_f), pd.DataFrame(rows_seg), pd.DataFrame(rows_debt), pd.DataFrame(rows_guid)

    # Use local fallback only when EX-99 (strict/relaxed) missing for a quarter
    if config.enable_tier3_non_gaap:
        def _has_any_metric(df: pd.DataFrame) -> pd.Series:
            if df is None or df.empty:
                return pd.Series([], dtype=bool)
            cols = [c for c in LOCAL_NON_GAAP_CANONICAL_METRICS if c in df.columns]
            if not cols:
                return pd.Series([False] * len(df), index=df.index)
            return df[cols].notna().any(axis=1)
        # Only treat strict EX-99 metrics as canonical; relaxed preview should not block local fallback.
        canonical_adj_metrics = (
            adj_metrics.loc[_has_any_metric(adj_metrics)].copy()
            if adj_metrics is not None and not adj_metrics.empty
            else pd.DataFrame()
        )
        existing_q = _normalized_quarter_timestamps(
            canonical_adj_metrics["quarter"]
            if not canonical_adj_metrics.empty
            else pd.Series([], dtype="datetime64[ns]")
        )
        existing_metrics_by_quarter_for_local_fallback = _existing_local_non_gaap_metrics_by_quarter(
            canonical_adj_metrics
        )
        local_fallback_key = "|".join(
            [
                f"v{LOCAL_NON_GAAP_FALLBACK_VERSION}",
                f"materials={local_material_sig}",
                f"max_q={config.max_quarters}",
                f"quiet_pdf={int(bool(config.quiet_pdf_warnings))}",
                "doc_text_cache=v2",
            ]
        )
        local_fallback_cached = None if config.rebuild_doc_text_cache else _load_stage_cache("local_non_gaap_fallback", local_fallback_key)
        if isinstance(local_fallback_cached, dict):
            local_metrics = local_fallback_cached.get("metrics", pd.DataFrame())
            local_files = local_fallback_cached.get("files", pd.DataFrame())
            local_segments = local_fallback_cached.get("segments", pd.DataFrame())
            local_debt = local_fallback_cached.get("debt", pd.DataFrame())
            local_guidance = local_fallback_cached.get("guidance", pd.DataFrame())
        else:
            with _timed_stage(stage_timings, "local_non_gaap_fallback", enabled=config.profile_timings):
                local_metrics, local_files, local_segments, local_debt, local_guidance = build_non_gaap_local_fallback()
            _save_stage_cache(
                "local_non_gaap_fallback",
                local_fallback_key,
                {
                    "metrics": local_metrics,
                    "files": local_files,
                    "segments": local_segments,
                    "debt": local_debt,
                    "guidance": local_guidance,
                },
            )
        if not local_metrics.empty:
            local_metrics = _prune_local_non_gaap_metrics_against_existing(
                local_metrics,
                existing_metrics_by_quarter_for_local_fallback,
            )
            if not local_metrics.empty:
                adj_metrics = pd.concat([adj_metrics, local_metrics], ignore_index=True)
                kept_local_quarters = _normalized_quarter_timestamps(local_metrics["quarter"])
                if kept_local_quarters and local_files is not None and not local_files.empty and "quarter" in local_files.columns:
                    local_files = local_files.copy()
                    local_files["quarter"] = pd.to_datetime(local_files["quarter"], errors="coerce")
                    local_files = local_files[local_files["quarter"].isin(kept_local_quarters)]
                if non_gaap_files is None or non_gaap_files.empty:
                    non_gaap_files = local_files
                else:
                    non_gaap_files = pd.concat([non_gaap_files, local_files], ignore_index=True)
        # stash local slide/segment/debt/guidance extracts for Excel (if any)
        slides_segments = local_segments
        slides_debt = local_debt
        slides_guidance = local_guidance

    non_gaap_needs = pd.DataFrame()
    non_gaap_info = pd.DataFrame()
    if config.non_gaap_mode == "strict":
        if non_gaap_files is not None and not non_gaap_files.empty:
            # Only flag truly ambiguous cases in strict mode (e.g., parse errors).
            needs_statuses = {"parse_error"}
            ng_warn = non_gaap_files[non_gaap_files["status"].astype(str).isin(needs_statuses)].copy()
            if not ng_warn.empty:
                # Non-GAAP file logs may not have a 'quarter' column; fall back to quarter_end if present.
                if "quarter" not in ng_warn.columns:
                    if "quarter_end" in ng_warn.columns:
                        ng_warn["quarter"] = ng_warn["quarter_end"]
                    else:
                        ng_warn["quarter"] = pd.NaT
                non_gaap_needs = ng_warn.assign(
                    metric="non_gaap",
                    severity="warn",
                    message="Non-GAAP parsing error.",
                    source=ng_warn["status"],
                )[["quarter", "metric", "severity", "message", "source"]]
            # In strict mode, "no_matching_column" is informational (strict empty is OK).
            ng_info = non_gaap_files[non_gaap_files["status"].astype(str).isin({"no_matching_column"})].copy()
            if not ng_info.empty:
                if "quarter" not in ng_info.columns:
                    if "quarter_end" in ng_info.columns:
                        ng_info["quarter"] = ng_info["quarter_end"]
                    else:
                        ng_info["quarter"] = pd.NaT
                non_gaap_info = ng_info.assign(
                    metric="non_gaap",
                    severity="info",
                    message="Non-GAAP strict: no matching column (expected for many files).",
                    source=ng_info["status"],
                )[["quarter", "metric", "severity", "message", "source"]]
    if config.non_gaap_mode == "relaxed" and not adj_metrics.empty:
        relaxed_warn = adj_metrics.assign(
            metric="non_gaap_relaxed",
            severity="warn",
            message="Non-GAAP relaxed mode (low confidence).",
            source="relaxed_mode",
        )[["quarter", "metric", "severity", "message", "source"]]
        non_gaap_needs = pd.concat([non_gaap_needs, relaxed_warn], ignore_index=True) if not non_gaap_needs.empty else relaxed_warn
    elif not adj_metrics_relaxed.empty:
        relaxed_warn = adj_metrics_relaxed.assign(
            metric="non_gaap_relaxed",
            severity="warn",
            message="Non-GAAP relaxed mode (low confidence).",
            source="relaxed_mode",
        )[["quarter", "metric", "severity", "message", "source"]]
        non_gaap_needs = pd.concat([non_gaap_needs, relaxed_warn], ignore_index=True) if not non_gaap_needs.empty else relaxed_warn

    issues1 = validate_history(hist)

    lt_debt_map = pd.DataFrame()
    if not debt_tranches.empty:
        lt_rows: List[Dict[str, Any]] = []
        for q in sorted(debt_tranches["quarter"].dropna().unique()):
            pr_lt = compute_long_term_debt_instant(df_all, end=q, prefer_forms=["10-Q", "10-K"])
            if pr_lt is not None and pr_lt.value is not None:
                lt_rows.append({"quarter": q, "long_term_debt": float(pr_lt.value)})
        if lt_rows:
            lt_debt_map = pd.DataFrame(lt_rows)

    issues2 = validate_debt_tieout(hist, debt_tranches, lt_debt_map)
    issues3 = needs_review_from_audit(audit)
    info_log = info_log_from_audit(audit)
    if non_gaap_info is not None and not non_gaap_info.empty:
        info_log = pd.concat([info_log, non_gaap_info], ignore_index=True) if not info_log.empty else non_gaap_info

    def _append_fy_q4_qa_rows(info_df: pd.DataFrame) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []
        if hist is None or hist.empty or "quarter" not in hist.columns:
            return info_df

        hq = hist.copy()
        hq["quarter"] = pd.to_datetime(hq["quarter"], errors="coerce")
        hq = hq[hq["quarter"].notna()].sort_values("quarter")
        if hq.empty:
            return info_df

        q_target = pd.Timestamp(dt.date(2025, 12, 31))
        if not (hq["quarter"] == q_target).any():
            rows.append(
                {
                    "quarter": q_target.date(),
                    "metric": "QA_FY_Q4",
                    "severity": "warn",
                    "message": "2025-12-31 row missing in History_Q.",
                    "source": "history",
                }
            )
            qa_df = pd.DataFrame(rows)
            return pd.concat([info_df, qa_df], ignore_index=True) if info_df is not None and not info_df.empty else qa_df

        q9 = pd.Timestamp(dt.date(2025, 9, 30))
        spec_map = {s.name: s for s in GAAP_SPECS}

        def _form_rank(v: Any) -> int:
            f = str(v or "").upper()
            if f.startswith("10-K"):
                return 0
            if f.startswith("10-Q"):
                return 1
            return 9

        def _pick_cf_duration(tags: List[str], end_d: dt.date, dur_class: str, preferred_tag: Optional[str] = None) -> Optional[float]:
            if not tags:
                return None
            cand = df_all[(df_all["tag"].isin(tags)) & (df_all["end_d"] == end_d) & df_all["start_d"].notna()].copy()
            if cand.empty:
                return None
            spec_tmp = MetricSpec("tmp", tags, "duration", "USD", ["10-K", "10-Q"])
            cand = _filter_unit(cand, spec_tmp)
            if cand.empty:
                return None
            cand["dur"] = _duration_days(cand["end_d"], cand["start_d"])
            cand["dur_class"] = cand["dur"].apply(classify_duration)
            cand = cand[cand["dur_class"] == dur_class].copy()
            if cand.empty:
                return None
            pref_tag = str(preferred_tag or "").strip()
            if pref_tag:
                pref = cand[cand["tag"].astype(str) == pref_tag].copy()
                if not pref.empty:
                    cand = pref
            cand["form_rank"] = cand["form"].apply(_form_rank)
            cand = cand.sort_values(["form_rank", "filed_d"], ascending=[True, False])
            try:
                return float(cand.iloc[0]["val"])
            except Exception:
                return None

        def _pick_cf_instant(tags: List[str], end_d: dt.date) -> Optional[float]:
            if not tags:
                return None
            cand = df_all[(df_all["tag"].isin(tags)) & (df_all["end_d"] == end_d)].copy()
            if cand.empty:
                return None
            spec_tmp = MetricSpec("tmp", tags, "instant", "USD", ["10-K", "10-Q"])
            cand = _filter_unit(cand, spec_tmp)
            if cand.empty:
                return None
            rec = pick_best_instant(cand, end=end_d, prefer_forms=["10-K", "10-Q"])
            if rec is None:
                return None
            try:
                return float(rec["val"])
            except Exception:
                return None

        def _hist_at(q: pd.Timestamp, metric: str) -> Optional[float]:
            if metric not in hq.columns:
                return None
            sub = hq[hq["quarter"] == q]
            if sub.empty:
                return None
            v = pd.to_numeric(sub.iloc[-1].get(metric), errors="coerce")
            return float(v) if pd.notna(v) else None

        def _add(sev: str, metric: str, msg: str, src: str) -> None:
            rows.append(
                {
                    "quarter": q_target.date(),
                    "metric": metric,
                    "severity": sev,
                    "message": msg,
                    "source": src,
                }
            )

        # Imported lazily to avoid widening the pipeline/module dependency cycle.
        from .pipeline import _pick_first_instant_tag, _pick_instant_tag

        # FY anchors + Q4 derivation sanity (Q4 = FY - 9M).
        dur_checks = [
            ("revenue", "Revenue"),
            ("net_income", "Net income"),
            ("cfo", "CFO"),
            ("capex", "Capex"),
        ]
        for metric_key, label in dur_checks:
            tags = list(spec_map.get(metric_key).tags if spec_map.get(metric_key) is not None else [])
            q4_tag = None
            q4_v = _hist_at(q_target, metric_key)
            if audit is not None and not audit.empty:
                aud_q4 = audit.copy()
                aud_q4["quarter"] = pd.to_datetime(aud_q4["quarter"], errors="coerce")
                aud_q4 = aud_q4[
                    (aud_q4["metric"] == metric_key)
                    & (aud_q4["quarter"] == q_target)
                ].copy()
                if not aud_q4.empty:
                    aud_q4["source_rank"] = aud_q4["source"].map(lambda s: 0 if str(s or "").lower() != "missing" else 1)
                    aud_q4 = aud_q4.sort_values(["source_rank"], ascending=[True])
                    q4_source = str(aud_q4.iloc[0].get("source") or "").lower()
                    q4_tag = str(aud_q4.iloc[0].get("tag") or "").strip()
                    if q4_source and q4_source not in {"derived_ytd_q4", "missing"}:
                        _add("info", "QA_Q4_Derivation", f"{label}: direct Q4 fact selected; FY-9M comparison skipped.", "history/audit")
                        continue
            fy_v = _pick_cf_duration(tags, q_target.date(), "FY", preferred_tag=q4_tag)
            y9_v = _pick_cf_duration(tags, q9.date(), "9M", preferred_tag=q4_tag)
            if metric_key == "capex":
                fy_v = abs(fy_v) if fy_v is not None else None
                y9_v = abs(y9_v) if y9_v is not None else None
                q4_v = abs(q4_v) if q4_v is not None else None
            if fy_v is None:
                _add("warn", "QA_FY_Anchor", f"{label}: FY fact missing for 2025-12-31.", "companyfacts")
                continue
            _add("info", "QA_FY_Anchor", f"{label}: FY fact {fy_v/1e6:,.1f}m found.", "companyfacts")
            if y9_v is None or q4_v is None:
                _add("warn", "QA_Q4_Derivation", f"{label}: missing 9M or Q4 value for FY-9M check.", "history/companyfacts")
                continue
            implied_q4 = fy_v - y9_v
            tol = max(5_000_000.0, 0.01 * max(1.0, abs(implied_q4)))
            diff = abs(q4_v - implied_q4)
            sev = "info" if diff <= tol else "warn"
            _add(
                sev,
                "QA_Q4_Derivation",
                f"{label}: Q4 {q4_v/1e6:,.1f}m vs (FY-9M) {implied_q4/1e6:,.1f}m (diff {diff/1e6:,.1f}m).",
                "history/companyfacts",
            )
            if metric_key in {"revenue", "cfo"} and q4_v < 0:
                _add("warn", "QA_Q4_Derivation", f"{label}: Q4 is negative ({q4_v/1e6:,.1f}m).", "history")

        # 12/31 instants should match direct companyfacts facts.
        cash_hist = _hist_at(q_target, "cash")
        cash_cf = _pick_cf_instant(list(spec_map.get("cash").tags if spec_map.get("cash") is not None else []), q_target.date())
        if cash_hist is None or cash_cf is None:
            _add("warn", "QA_FY_Anchor", "Cash@12/31 missing in history or companyfacts.", "history/companyfacts")
        else:
            diff = abs(cash_hist - cash_cf)
            tol = max(2_000_000.0, 0.001 * max(1.0, abs(cash_cf)))
            _add(
                "info" if diff <= tol else "warn",
                "QA_FY_Anchor",
                f"Cash@12/31 history {cash_hist/1e6:,.1f}m vs companyfacts {cash_cf/1e6:,.1f}m.",
                "history/companyfacts",
            )

        debt_hist = _hist_at(q_target, "total_debt")
        debt_pick = compute_total_debt_instant(df_all, end=q_target.date(), prefer_forms=["10-K", "10-Q"])
        debt_cf = float(debt_pick.value) if debt_pick is not None and debt_pick.value is not None else None
        if debt_hist is None or debt_cf is None:
            _add("warn", "QA_FY_Anchor", "Debt@12/31 missing in history or companyfacts.", "history/companyfacts")
        else:
            diff = abs(debt_hist - debt_cf)
            tol = max(5_000_000.0, 0.001 * max(1.0, abs(debt_cf)))
            _add(
                "info" if diff <= tol else "warn",
                "QA_FY_Anchor",
                f"Debt@12/31 history {debt_hist/1e6:,.1f}m vs companyfacts {debt_cf/1e6:,.1f}m.",
                "history/companyfacts",
            )

        # Double-count guard: when noncurrent + current exists, total debt must not equal LT + current.
        rec_non = _pick_first_instant_tag(
            df_all,
            end=q_target.date(),
            tags=["LongTermDebtNoncurrent", "LongTermDebtAndCapitalLeaseObligations"],
            prefer_forms=["10-K", "10-Q"],
        )
        rec_long = _pick_instant_tag(df_all, end=q_target.date(), tag="LongTermDebt", prefer_forms=["10-K", "10-Q"])
        rec_cur = _pick_first_instant_tag(
            df_all,
            end=q_target.date(),
            tags=["LongTermDebtCurrent", "DebtCurrent"],
            prefer_forms=["10-K", "10-Q"],
        )
        non_v = float(rec_non["val"]) if rec_non is not None and pd.notna(rec_non.get("val")) else None
        long_v = float(rec_long["val"]) if rec_long is not None and pd.notna(rec_long.get("val")) else None
        cur_v = float(rec_cur["val"]) if rec_cur is not None and pd.notna(rec_cur.get("val")) else None
        if debt_cf is not None and non_v is not None and cur_v is not None and long_v is not None:
            tol = max(5_000_000.0, 0.01 * max(1.0, abs(debt_cf)))
            bad_double = abs(debt_cf - (long_v + cur_v)) <= tol and abs(debt_cf - (non_v + cur_v)) > tol
            if bad_double:
                _add(
                    "fail",
                    "QA_Debt_DoubleCount",
                    "FAIL: total_debt matches LongTermDebt + current while noncurrent + current exists.",
                    "companyfacts",
                )
            else:
                _add(
                    "info",
                    "QA_Debt_DoubleCount",
                    "PASS: debt aggregation avoids LongTermDebt + current double-count path.",
                    "companyfacts",
                )

        if not rows:
            return info_df
        qa_df = pd.DataFrame(rows)
        return pd.concat([info_df, qa_df], ignore_index=True) if info_df is not None and not info_df.empty else qa_df

    def _append_key_metric_coverage_rows(info_df: pd.DataFrame) -> pd.DataFrame:
        if hist is None or hist.empty or "quarter" not in hist.columns:
            return info_df
        hcov = hist.copy()
        hcov["quarter"] = pd.to_datetime(hcov["quarter"], errors="coerce")
        hcov = hcov[hcov["quarter"].notna()].sort_values("quarter")
        if hcov.empty:
            return info_df
        if "fcf" not in hcov.columns and {"cfo", "capex"}.issubset(hcov.columns):
            hcov["fcf"] = pd.to_numeric(hcov["cfo"], errors="coerce") - pd.to_numeric(hcov["capex"], errors="coerce")
        key_metrics = ["cfo", "capex", "da", "ebitda", "fcf"]
        present_metrics = [m for m in key_metrics if m in hcov.columns]
        if not present_metrics:
            return info_df
        # Keep signal high: last 40 quarters and any quarter >=2015.
        htail = hcov.tail(40).copy()
        h2015 = hcov[hcov["quarter"].dt.year >= 2015].copy()
        check = (
            pd.concat([htail, h2015], ignore_index=True)
            .drop_duplicates(subset=["quarter"])
            .sort_values("quarter")
        )
        rows: List[Dict[str, Any]] = []
        for _, rr in check.iterrows():
            qd = pd.Timestamp(rr["quarter"]).date()
            missing = []
            for m in present_metrics:
                v = pd.to_numeric(rr.get(m), errors="coerce")
                if pd.isna(v):
                    missing.append(m)
            if missing:
                rows.append(
                    {
                        "quarter": qd,
                        "metric": "QA_Coverage",
                        "severity": "warn",
                        "message": f"Missing key metrics: {', '.join(missing)}",
                        "source": "history_coverage",
                    }
                )
            else:
                rows.append(
                    {
                        "quarter": qd,
                        "metric": "QA_Coverage",
                        "severity": "info",
                        "message": f"Key metrics present: {', '.join(present_metrics)}",
                        "source": "history_coverage",
                    }
                )
        if not rows:
            return info_df
        cov_df = pd.DataFrame(rows)
        return pd.concat([info_df, cov_df], ignore_index=True) if info_df is not None and not info_df.empty else cov_df

    info_log = _append_fy_q4_qa_rows(info_log)
    info_log = _append_key_metric_coverage_rows(info_log)
    if period_checks is not None and not period_checks.empty:
        fails = period_checks[period_checks["status"] == "fail"].copy()
        if not fails.empty:
            pr = fails.assign(
                severity="fail",
                source=fails["check"],
                message=fails["message"],
            )[["quarter", "metric", "severity", "message", "source"]]
            issues3 = pd.concat([issues3, pr], ignore_index=True)
    if not debt_tranches.empty and "scale_applied" in debt_tranches.columns:
        scaled = debt_tranches[debt_tranches["scale_applied"] != 1.0]
        if not scaled.empty:
            scale_rows = (
                scaled[["quarter", "scale_applied"]]
                .drop_duplicates()
                .assign(metric="debt_tranches", severity="warn",
                        message="Debt table scale inferred (thousands/millions). Review scaling.",
                        source="scale_inferred")
            )
            issues3 = pd.concat([issues3, scale_rows], ignore_index=True)
    if not non_gaap_needs.empty:
        issues3 = pd.concat([issues3, non_gaap_needs], ignore_index=True)

    if qa_checks is not None and not qa_checks.empty:
        qa_issues = qa_checks[
            ((qa_checks["status"].isin(["fail"])) | (qa_checks["check"] == "capex_negative"))
            & (qa_checks["check"] != "cash_identity")
        ].copy()
        if not qa_issues.empty:
            qa_rows = qa_issues.assign(
                severity=qa_issues["status"],
                source=qa_issues["check"],
                message=qa_issues["message"],
            )[["quarter", "metric", "severity", "message", "source"]]
            issues3 = pd.concat([issues3, qa_rows], ignore_index=True)

    needs_review = finalize_needs_review(
        concat_frames(issues1, issues2, issues3),
        review_quarters=config.qa_review_quarters,
    )

    debt_recon = issues2
    manifest_df = pd.DataFrame(getattr(sec, "manifest_rows", []))
    ocr_log_df = pd.DataFrame(getattr(sec, "ocr_log_rows", []))
    if ocr_log_df.empty:
        ocr_log_df = pd.DataFrame(columns=[
            "accn", "doc", "quarter", "purpose", "status",
            "image_files", "n_images", "text_len", "text_excerpt", "ocr_tokens",
            "report_date", "filing_date",
        ])
    else:
        for col in ["accn", "doc", "quarter", "purpose", "status", "image_files", "n_images", "text_len", "text_excerpt", "ocr_tokens", "report_date", "filing_date"]:
            if col not in ocr_log_df.columns:
                ocr_log_df[col] = None

    debt_schedule_key = (
        f"v2|max_quarters={config.max_quarters}|sub={_sub_recent_signature(sub, forms_prefix=('10-Q', '10-K'), max_rows=300)}"
    )
    debt_schedule_cached = _load_stage_cache("debt_schedule", debt_schedule_key)
    if isinstance(debt_schedule_cached, pd.DataFrame):
        debt_schedule = debt_schedule_cached
    else:
        debt_schedule = build_debt_schedule_tier2(sec, cik_int, sub, max_quarters=config.max_quarters, min_year=config.min_year)
        _save_stage_cache("debt_schedule", debt_schedule_key, debt_schedule)

    debt_profile, debt_tranches_latest, debt_maturity, debt_profile_qa, debt_profile_info = build_debt_profile(
        hist,
        df_all,
        debt_tranches,
        slides_debt=slides_debt,
        debt_schedule=debt_schedule,
    )
    debt_notes_key = (
        f"v1|max_docs=8|sub={_sub_recent_signature(sub, forms_prefix=('10-Q', '10-K', '8-K'), max_rows=300)}"
    )
    debt_credit_notes_cached = _load_stage_cache("debt_credit_notes", debt_notes_key)
    if isinstance(debt_credit_notes_cached, pd.DataFrame):
        debt_credit_notes = debt_credit_notes_cached
    else:
        debt_credit_notes = build_debt_credit_notes(sec, cik_int, sub, max_docs=8)
        _save_stage_cache("debt_credit_notes", debt_notes_key, debt_credit_notes)

    revolver_key = (
        f"v2|max_docs=80|lookback=7|sub={_sub_recent_signature(sub, forms_prefix=('10-Q', '10-K', '8-K'), max_rows=500)}"
    )
    revolver_df_cached = _load_stage_cache("revolver_df", revolver_key)
    if isinstance(revolver_df_cached, pd.DataFrame):
        revolver_df = revolver_df_cached
    else:
        revolver_df = build_revolver_availability(sec, cik_int, sub, max_docs=80, lookback_years=7)
        _save_stage_cache("revolver_df", revolver_key, revolver_df)
    rev_capacity_map, rev_capacity_meta = build_revolver_capacity_map(df_all, hist)
    revolver_history = build_revolver_history(
        revolver_df,
        hist,
        capacity_map=rev_capacity_map,
        capacity_meta=rev_capacity_meta,
        max_quarters=20,
    )
    local_main_revolver = build_local_main_revolver_history(
        base_dir,
        ticker=ticker,
        cache_root=Path(config.cache_dir) if config.cache_dir is not None else None,
        rebuild_doc_text_cache=config.rebuild_doc_text_cache,
        quiet_pdf_warnings=config.quiet_pdf_warnings,
    )
    if local_main_revolver is not None and not local_main_revolver.empty:
        local_main_revolver = local_main_revolver.copy()
        local_main_revolver["quarter"] = pd.to_datetime(local_main_revolver["quarter"], errors="coerce")
        if revolver_history is None or revolver_history.empty:
            revolver_history = local_main_revolver.copy()
        else:
            revolver_history = revolver_history.copy()
            revolver_history["quarter"] = pd.to_datetime(revolver_history["quarter"], errors="coerce")
            overlay_cols = [
                "revolver_commitment",
                "revolver_facility_size",
                "revolver_drawn",
                "revolver_letters_of_credit",
                "revolver_availability",
                "commitment_source_type",
                "facility_source_type",
                "drawn_source_type",
                "lc_source_type",
                "availability_source_type",
                "commitment_snippet",
                "drawn_snippet",
                "lc_snippet",
                "availability_snippet",
                "source_type",
                "source_snippet",
                "note",
            ]
            local_by_q = {
                pd.Timestamp(r["quarter"]).normalize(): r
                for _, r in local_main_revolver.dropna(subset=["quarter"]).iterrows()
            }
            existing_q = set()
            for idx, row in revolver_history.iterrows():
                qk = pd.Timestamp(row.get("quarter")).normalize() if pd.notna(row.get("quarter")) else None
                if qk is None or qk not in local_by_q:
                    continue
                existing_q.add(qk)
                local_row = local_by_q[qk]
                for col in overlay_cols:
                    if col in local_row.index:
                        revolver_history.at[idx, col] = local_row.get(col)
                commit = pd.to_numeric(revolver_history.at[idx, "revolver_commitment"], errors="coerce")
                drawn = pd.to_numeric(revolver_history.at[idx, "revolver_drawn"], errors="coerce")
                avail = pd.to_numeric(revolver_history.at[idx, "revolver_availability"], errors="coerce")
                lc = pd.to_numeric(revolver_history.at[idx, "revolver_letters_of_credit"], errors="coerce")
                if pd.notna(commit) and pd.notna(drawn):
                    revolver_history.at[idx, "revolver_utilization"] = float(drawn) / float(commit) if float(commit) else None
                if pd.notna(commit) and pd.notna(drawn) and pd.notna(avail) and pd.isna(lc):
                    residual = float(commit) - float(drawn) - float(avail)
                    if residual >= -1_000_000.0:
                        revolver_history.at[idx, "revolver_letters_of_credit"] = max(residual, 0.0)
                        revolver_history.at[idx, "lc_source_type"] = "derived"
            missing_rows: List[Dict[str, Any]] = []
            for qk, local_row in local_by_q.items():
                if qk in existing_q:
                    continue
                new_row = {"quarter": qk}
                for col in overlay_cols:
                    if col in local_row.index:
                        new_row[col] = local_row.get(col)
                commit = pd.to_numeric(new_row.get("revolver_commitment"), errors="coerce")
                drawn = pd.to_numeric(new_row.get("revolver_drawn"), errors="coerce")
                avail = pd.to_numeric(new_row.get("revolver_availability"), errors="coerce")
                lc = pd.to_numeric(new_row.get("revolver_letters_of_credit"), errors="coerce")
                if pd.notna(commit) and pd.notna(drawn):
                    new_row["revolver_utilization"] = float(drawn) / float(commit) if float(commit) else None
                if pd.notna(commit) and pd.notna(drawn) and pd.notna(avail) and pd.isna(lc):
                    residual = float(commit) - float(drawn) - float(avail)
                    if residual >= -1_000_000.0:
                        new_row["revolver_letters_of_credit"] = max(residual, 0.0)
                        new_row["lc_source_type"] = "derived"
                missing_rows.append(new_row)
            if missing_rows:
                revolver_history = pd.concat([revolver_history, pd.DataFrame(missing_rows)], ignore_index=True, sort=False)
            revolver_history = revolver_history.sort_values("quarter").reset_index(drop=True)
    debt_buckets, debt_bucket_qa = build_debt_buckets(debt_tranches_latest, hist, maturity_df=debt_maturity)
    try:
        if debt_buckets is not None and not debt_buckets.empty and debt_profile is not None and not debt_profile.empty:
            src = str(debt_buckets.iloc[0].get("Source") or "")
            if src == "scheduled_repayments_fallback":
                dpf = debt_profile.copy()
                if "metric" in dpf.columns and "value" in dpf.columns:
                    basis_vals = pd.to_numeric(
                        dpf.loc[dpf["metric"].astype(str).isin(["debt_long_term", "debt_principal_total"]), "value"],
                        errors="coerce",
                    ).dropna()
                    basis_val = float(basis_vals.iloc[-1]) if not basis_vals.empty else None
                    total_bucketed = pd.to_numeric(debt_buckets.iloc[0].get("Total_bucketed"), errors="coerce")
                    if basis_val not in (None, 0) and pd.notna(total_bucketed):
                        debt_buckets.loc[:, "Debt_long_term"] = basis_val
                        debt_buckets.loc[:, "Coverage_basis_metric"] = "debt_long_term"
                        debt_buckets.loc[:, "Coverage_basis_value"] = basis_val
                        debt_buckets.loc[:, "Bucket_coverage_pct"] = float(total_bucketed) / float(basis_val)
    except Exception:
        pass
    if debt_bucket_qa is not None and not debt_bucket_qa.empty:
        if qa_checks is None or qa_checks.empty:
            qa_checks = debt_bucket_qa
        else:
            qa_checks = pd.concat([qa_checks, debt_bucket_qa], ignore_index=True)
    if debt_profile_qa is not None and not debt_profile_qa.empty:
        if qa_checks is None or qa_checks.empty:
            qa_checks = debt_profile_qa
        else:
            qa_checks = pd.concat([qa_checks, debt_profile_qa], ignore_index=True)
    if debt_profile_info is not None and not debt_profile_info.empty:
        if info_log is None or info_log.empty:
            info_log = debt_profile_info
        else:
            info_log = pd.concat([info_log, debt_profile_info], ignore_index=True)

    earnings_release_candidates = [
        base_dir / "earnings_release",
        base_dir / "Earnings Release",
        base_dir / "Earnings Releases",
        base_dir / "press_release",
        base_dir / "Press Release",
    ]
    earnings_release_dir = next((p for p in earnings_release_candidates if p.exists() and p.is_dir()), None)
    earnings_release_sig = "none"
    if earnings_release_dir is not None:
        try:
            rel_rows: List[str] = []
            for fp in sorted([x for x in earnings_release_dir.glob("*.pdf") if x.is_file()], key=lambda x: x.name.lower())[:200]:
                st = fp.stat()
                rel_rows.append(f"{fp.name}:{int(st.st_size)}:{int(st.st_mtime)}")
            earnings_release_sig = hashlib.sha1("||".join(rel_rows).encode("utf-8", errors="ignore")).hexdigest()
        except Exception:
            earnings_release_sig = "err"

    doc_intel_key = "|".join(
        [
            DOC_INTEL_BEHAVIOR_VERSION,
            f"sub={_sub_recent_signature(sub, forms_prefix=('10-Q', '10-K', '8-K'), max_rows=500)}",
            f"hist={_df_quick_sig(hist, ['quarter', 'revenue', 'ebitda', 'fcf', 'debt', 'cash'])}",
            f"adj={_df_quick_sig(adj_metrics, ['quarter', 'adj_ebitda', 'adj_ebit', 'adj_eps'])}",
            f"revh={_df_quick_sig(revolver_history, ['quarter', 'revolver_commitment', 'revolver_drawn', 'revolver_availability'])}",
            f"db={_df_quick_sig(debt_buckets, ['quarter', 'maturity_year', 'amount_total'])}",
            f"er={earnings_release_sig}",
            f"max_docs={config.doc_intel_max_docs}",
            f"max_quarters={config.doc_intel_max_quarters}",
            "doc_text_cache=v2",
            f"code={_module_code_signature('doc_intel.py', 'quarter_notes.py')}",
            f"quiet_pdf={int(bool(config.quiet_pdf_warnings))}",
        ]
    )
    doc_intel_cached = _load_stage_cache("doc_intel_bundle", doc_intel_key)
    if isinstance(doc_intel_cached, dict):
        quarter_notes = doc_intel_cached.get("quarter_notes", pd.DataFrame())
        promises = doc_intel_cached.get("promises", pd.DataFrame())
        promise_progress = doc_intel_cached.get("promise_progress", pd.DataFrame())
        non_gaap_cred = doc_intel_cached.get("non_gaap_cred", pd.DataFrame())
    else:
        if config.use_cached_doc_intel_only:
            raise RuntimeError("doc_intel cache required but missing for --skip-doc-intel run.")
        with _timed_stage(stage_timings, "doc_intel_bundle", enabled=config.profile_timings):
            quarter_notes, promises, promise_progress, non_gaap_cred = build_doc_intel_outputs(
                sec=sec,
                cik_int=cik_int,
                submissions=sub,
                hist=hist,
                adj_metrics=adj_metrics,
                adj_breakdown=adj_breakdown,
                non_gaap_files=non_gaap_files,
                revolver_history=revolver_history,
                debt_buckets=debt_buckets,
                earnings_release_dir=earnings_release_dir,
                max_docs=config.doc_intel_max_docs,
                max_quarters=config.doc_intel_max_quarters,
                cache_dir=config.cache_dir,
                rebuild_doc_text_cache=config.rebuild_doc_text_cache,
                quiet_pdf_warnings=config.quiet_pdf_warnings,
                stage_timings=stage_timings,
                profile_timings=config.profile_timings,
            )
        _save_stage_cache(
            "doc_intel_bundle",
            doc_intel_key,
            {
                "quarter_notes": quarter_notes,
                "promises": promises,
                "promise_progress": promise_progress,
                "non_gaap_cred": non_gaap_cred,
            },
        )
    quarter_notes_qa = validate_quarter_notes(quarter_notes, hist)
    promise_qa_df = build_promise_qa_checks(promises, promise_progress)
    non_gaap_qa_df = build_non_gaap_cred_qa(non_gaap_cred)
    qa_checks = finalize_qa_checks(
        concat_frames(
            qa_checks,
            quarter_notes_qa,
            promise_qa_df,
            non_gaap_qa_df,
        ),
        review_quarters=config.qa_review_quarters,
    )

    company_overview_key = "|".join(
        [
            COMPANY_OVERVIEW_BEHAVIOR_VERSION,
            f"sub={submissions_sig}",
            f"materials={local_material_sig}",
            f"ticker={str(ticker or '').upper()}",
            f"code={_module_code_signature('summary_overview.py')}",
        ]
    )
    company_overview_cached = _load_stage_cache("company_overview", company_overview_key)
    if isinstance(company_overview_cached, dict):
        company_overview = company_overview_cached
    else:
        try:
            with _timed_stage(stage_timings, "company_overview", enabled=config.profile_timings):
                company_overview = build_company_overview(sec, cik_int, sub, ticker=ticker)
            _save_stage_cache("company_overview", company_overview_key, company_overview)
        except Exception as exc:
            err = f"{type(exc).__name__}: {exc}"
            company_overview = {
                "what_it_does": "N/A",
                "what_it_does_source": f"Source: N/A ({err})",
                "current_strategic_context": "N/A",
                "current_strategic_context_source": f"Source: N/A ({err})",
                "key_advantage": "N/A",
                "key_advantage_source": f"Source: N/A ({err})",
                "revenue_streams": [],
                "revenue_streams_source": f"Source: N/A ({err})",
                "asof_fy_end": None,
            }

    if config.profile_timings and stage_timings:
        summary = " | ".join(f"{k}={v:.2f}s" for k, v in sorted(stage_timings.items(), key=lambda kv: (-kv[1], kv[0])))
        print(f"[run_pipeline timing] {summary}", flush=True)

    # Hard regression gate: fail fast on key invariants.
    from .pipeline import _regression_gate

    _regression_gate(
        hist,
        audit,
        df_all,
        ticker=tkr_u,
        cache_dir=config.cache_dir,
        debug=config.debug_regression_gate,
        allow_fail=config.allow_regression_gate_fail,
    )
    return PipelineArtifacts(
        hist=hist,
        audit=audit,
        debt_tranches=debt_tranches,
        debt_recon=debt_recon,
        adj_metrics=adj_metrics,
        adj_breakdown=adj_breakdown,
        non_gaap_files=non_gaap_files,
        adj_metrics_relaxed=adj_metrics_relaxed,
        adj_breakdown_relaxed=adj_breakdown_relaxed,
        non_gaap_files_relaxed=non_gaap_files_relaxed,
        needs_review=needs_review,
        info_log=info_log,
        tag_coverage=tag_coverage,
        period_checks=period_checks,
        qa_checks=qa_checks,
        bridge_q=bridge_q,
        manifest_df=manifest_df,
        ocr_log=ocr_log_df,
        qfd_preview=qfd_preview,
        qfd_unused=qfd_unused,
        debt_profile=debt_profile,
        debt_tranches_latest=debt_tranches_latest,
        debt_maturity=debt_maturity,
        debt_credit_notes=debt_credit_notes,
        revolver_df=revolver_df,
        revolver_history=revolver_history,
        debt_buckets=debt_buckets,
        slides_segments=slides_segments,
        slides_debt=slides_debt,
        slides_guidance=slides_guidance,
        quarter_notes=quarter_notes,
        promises=promises,
        promise_progress=promise_progress,
        non_gaap_cred=non_gaap_cred,
        company_overview=company_overview,
        stage_timings=stage_timings,
    )
