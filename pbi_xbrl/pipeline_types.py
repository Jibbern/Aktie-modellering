from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd


@dataclass(frozen=True)
class PipelineConfig:
    cache_dir: Path
    max_quarters: int = 80
    enable_tier2_debt: bool = True
    enable_tier3_non_gaap: bool = True
    namespace: str = "us-gaap"
    non_gaap_mode: str = "strict"  # strict | relaxed
    strictness: str = "ytd"  # ytd | only3m
    non_gaap_preview: bool = True
    min_year: Optional[int] = None
    price: Optional[float] = None
    quiet_pdf_warnings: bool = True
    rebuild_doc_text_cache: bool = False
    use_cached_doc_intel_only: bool = False
    profile_timings: bool = True
    debug_regression_gate: bool = False
    allow_regression_gate_fail: bool = False
    repo_root: Optional[Path] = None
    material_root: Optional[Path] = None
    qa_review_quarters: int = 8
    doc_intel_max_docs: int = 80
    doc_intel_max_quarters: int = 24


@dataclass
class PipelineArtifacts:
    hist: pd.DataFrame
    audit: pd.DataFrame
    debt_tranches: pd.DataFrame
    debt_recon: pd.DataFrame
    adj_metrics: pd.DataFrame
    adj_breakdown: pd.DataFrame
    non_gaap_files: pd.DataFrame
    adj_metrics_relaxed: pd.DataFrame
    adj_breakdown_relaxed: pd.DataFrame
    non_gaap_files_relaxed: pd.DataFrame
    needs_review: pd.DataFrame
    info_log: pd.DataFrame
    tag_coverage: pd.DataFrame
    period_checks: pd.DataFrame
    qa_checks: pd.DataFrame
    bridge_q: pd.DataFrame
    manifest_df: pd.DataFrame
    ocr_log: pd.DataFrame
    qfd_preview: pd.DataFrame
    qfd_unused: pd.DataFrame
    debt_profile: pd.DataFrame
    debt_tranches_latest: pd.DataFrame
    debt_maturity: pd.DataFrame
    debt_credit_notes: pd.DataFrame
    revolver_df: pd.DataFrame
    revolver_history: pd.DataFrame
    debt_buckets: pd.DataFrame
    slides_segments: pd.DataFrame
    slides_debt: pd.DataFrame
    slides_guidance: pd.DataFrame
    quarter_notes: pd.DataFrame
    promises: pd.DataFrame
    promise_progress: pd.DataFrame
    non_gaap_cred: pd.DataFrame
    company_overview: Optional[Dict[str, Any]] = None
    stage_timings: Dict[str, float] = field(default_factory=dict)

    def as_legacy_tuple(self) -> Tuple[pd.DataFrame, ...]:
        return (
            self.hist,
            self.audit,
            self.debt_tranches,
            self.debt_recon,
            self.adj_metrics,
            self.adj_breakdown,
            self.non_gaap_files,
            self.adj_metrics_relaxed,
            self.adj_breakdown_relaxed,
            self.non_gaap_files_relaxed,
            self.needs_review,
            self.info_log,
            self.tag_coverage,
            self.period_checks,
            self.qa_checks,
            self.bridge_q,
            self.manifest_df,
            self.ocr_log,
            self.qfd_preview,
            self.qfd_unused,
            self.debt_profile,
            self.debt_tranches_latest,
            self.debt_maturity,
            self.debt_credit_notes,
            self.revolver_df,
            self.revolver_history,
            self.debt_buckets,
            self.slides_segments,
            self.slides_debt,
            self.slides_guidance,
            self.quarter_notes,
            self.promises,
            self.promise_progress,
            self.non_gaap_cred,
            self.company_overview,
        )


@dataclass
class WorkbookInputs:
    out_path: Path
    hist: pd.DataFrame
    audit: pd.DataFrame
    needs_review: pd.DataFrame
    debt_tranches: pd.DataFrame
    debt_recon: pd.DataFrame
    adj_metrics: pd.DataFrame
    adj_breakdown: pd.DataFrame
    non_gaap_files: pd.DataFrame
    adj_metrics_relaxed: pd.DataFrame
    adj_breakdown_relaxed: pd.DataFrame
    non_gaap_files_relaxed: pd.DataFrame
    info_log: pd.DataFrame
    tag_coverage: pd.DataFrame
    period_checks: pd.DataFrame
    qa_checks: pd.DataFrame
    bridge_q: pd.DataFrame
    manifest_df: pd.DataFrame
    ocr_log: pd.DataFrame
    qfd_preview: pd.DataFrame
    qfd_unused: pd.DataFrame
    debt_profile: pd.DataFrame
    debt_tranches_latest: pd.DataFrame
    debt_maturity: pd.DataFrame
    debt_credit_notes: pd.DataFrame
    revolver_df: pd.DataFrame
    revolver_history: pd.DataFrame
    debt_buckets: pd.DataFrame
    slides_segments: pd.DataFrame
    slides_debt: pd.DataFrame
    slides_guidance: pd.DataFrame
    quarter_notes: pd.DataFrame
    promises: pd.DataFrame
    promise_progress: pd.DataFrame
    non_gaap_cred: pd.DataFrame
    company_overview: Optional[Dict[str, Any]] = None
    ticker: Optional[str] = None
    price: Optional[float] = None
    strictness: str = "ytd"
    excel_mode: str = "clean"
    is_rules: Optional[Dict[str, Any]] = None
    cache_dir: Optional[Path] = None
    quiet_pdf_warnings: bool = True
    rebuild_doc_text_cache: bool = False
    profile_timings: bool = False
    quarter_notes_audit: bool = False
    capture_saved_workbook_provenance: bool = True

    @classmethod
    def from_artifacts(
        cls,
        artifacts: PipelineArtifacts,
        *,
        out_path: Path,
        ticker: Optional[str] = None,
        price: Optional[float] = None,
        strictness: str = "ytd",
        excel_mode: str = "clean",
        is_rules: Optional[Dict[str, Any]] = None,
        cache_dir: Optional[Path] = None,
        quiet_pdf_warnings: bool = True,
        rebuild_doc_text_cache: bool = False,
        profile_timings: bool = False,
        quarter_notes_audit: bool = False,
        capture_saved_workbook_provenance: bool = True,
    ) -> "WorkbookInputs":
        return cls(
            out_path=out_path,
            hist=artifacts.hist,
            audit=artifacts.audit,
            needs_review=artifacts.needs_review,
            debt_tranches=artifacts.debt_tranches,
            debt_recon=artifacts.debt_recon,
            adj_metrics=artifacts.adj_metrics,
            adj_breakdown=artifacts.adj_breakdown,
            non_gaap_files=artifacts.non_gaap_files,
            adj_metrics_relaxed=artifacts.adj_metrics_relaxed,
            adj_breakdown_relaxed=artifacts.adj_breakdown_relaxed,
            non_gaap_files_relaxed=artifacts.non_gaap_files_relaxed,
            info_log=artifacts.info_log,
            tag_coverage=artifacts.tag_coverage,
            period_checks=artifacts.period_checks,
            qa_checks=artifacts.qa_checks,
            bridge_q=artifacts.bridge_q,
            manifest_df=artifacts.manifest_df,
            ocr_log=artifacts.ocr_log,
            qfd_preview=artifacts.qfd_preview,
            qfd_unused=artifacts.qfd_unused,
            debt_profile=artifacts.debt_profile,
            debt_tranches_latest=artifacts.debt_tranches_latest,
            debt_maturity=artifacts.debt_maturity,
            debt_credit_notes=artifacts.debt_credit_notes,
            revolver_df=artifacts.revolver_df,
            revolver_history=artifacts.revolver_history,
            debt_buckets=artifacts.debt_buckets,
            slides_segments=artifacts.slides_segments,
            slides_debt=artifacts.slides_debt,
            slides_guidance=artifacts.slides_guidance,
            quarter_notes=artifacts.quarter_notes,
            promises=artifacts.promises,
            promise_progress=artifacts.promise_progress,
            non_gaap_cred=artifacts.non_gaap_cred,
            company_overview=artifacts.company_overview,
            ticker=ticker,
            price=price,
            strictness=strictness,
            excel_mode=excel_mode,
            is_rules=is_rules,
            cache_dir=cache_dir,
            quiet_pdf_warnings=quiet_pdf_warnings,
            rebuild_doc_text_cache=rebuild_doc_text_cache,
            profile_timings=profile_timings,
            quarter_notes_audit=quarter_notes_audit,
            capture_saved_workbook_provenance=capture_saved_workbook_provenance,
        )
