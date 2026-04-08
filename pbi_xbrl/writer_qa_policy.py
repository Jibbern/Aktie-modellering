"""Writer-specific QA policy for latest-quarter checks and curated review sheets.

The pipeline-level QA surfaces stay intentionally broad. This module owns the
writer-facing policy that decides which QA issues should be curated into
``Needs_Review`` and how noisy latest-quarter support gaps should be presented in
``QA_Checks``.

The goal is to keep the policy declarative and shared between the workbook
writer's latest-quarter QA and the exported curated queue without changing the
underlying numeric truth.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Set, Tuple

import pandas as pd


NOISY_LATEST_QUARTER_SUPPORT_GAP_METRICS = {
    "EBITDA (Q)",
    "Adj EBITDA (Q)",
    "Net income (Q)",
    "FCF (Q)",
    "Debt core (Q)",
    "Net debt (Q)",
}

STRONG_EXPECTED_NO_EXPLICIT_SUPPORT_METRICS = {
    "revenue_q",
    "cash_q",
    "total_debt_q",
}

OFTEN_NOT_EXPLICIT_NO_EXPLICIT_SUPPORT_METRICS = {
    "ebitda_q",
    "adj_ebitda_q",
    "fcf_q",
    "net_income_q",
    "debt_core_q",
    "net_debt_q",
}

DEBT_INTEGRITY_ISSUE_FAMILIES = {
    "debt_recon_coverage_check",
    "carrying_debt_tieout",
    "principal_tranche_tieout",
    "revolver_and_other_debt_presence_check",
    "debt_tranches",
}


@dataclass(frozen=True)
class WriterQAPolicyContext:
    coverage_set: Set[pd.Timestamp]
    latest_coverage_q: pd.Timestamp | Any
    material_warn_fn: Callable[[pd.Series], bool]


def normalize_metric_family(metric_in: Any) -> str:
    metric_txt = str(metric_in or "").strip()
    if not metric_txt:
        return ""
    metric_txt = re.sub(r"^QA_", "", metric_txt, flags=re.I)
    metric_txt = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", metric_txt)
    metric_txt = metric_txt.replace("/", "_").replace("-", "_").replace(" ", "_")
    metric_txt = re.sub(r"_+", "_", metric_txt).strip("_").lower()
    metric_txt = metric_txt.replace("tie_out", "tieout")
    return metric_txt


def normalize_message_family(message_in: Any) -> str:
    msg = str(message_in or "").strip().lower()
    if not msg:
        return ""
    msg = re.sub(r"\b20\d{2}-\d{2}-\d{2}\b", "<quarter>", msg)
    msg = re.sub(r"\$-?\d[\d,]*(?:\.\d+)?(?:m|bn|b)?", "$#", msg, flags=re.I)
    msg = re.sub(r"\b\d+(?:\.\d+)?%?\b", "#", msg)
    msg = re.sub(r"\s+", " ", msg)
    return msg.strip(" .|")


def latest_quarter_support_gap_severity(metric_name: Any) -> str:
    return "info" if str(metric_name or "").strip() in NOISY_LATEST_QUARTER_SUPPORT_GAP_METRICS else "warn"


def issue_family(row_in: pd.Series) -> str:
    explicit_issue_family_raw = row_in.get("issue_family")
    explicit_issue_family = str(explicit_issue_family_raw).strip() if pd.notna(explicit_issue_family_raw) else ""
    if explicit_issue_family:
        return explicit_issue_family
    metric_family = normalize_metric_family(row_in.get("metric"))
    message_low = str(row_in.get("message") or "").strip().lower()
    if metric_family == "qtr":
        return "quarter_text_model_mismatch"
    if metric_family in {"debt_tieout", "debt_recon", "debt_tranches"}:
        return metric_family
    if metric_family in {"bank_deposits", "bank_finance_receivables"}:
        return "bank_extraction_fallback"
    if metric_family == "shares_diluted":
        return "share_count_fallback"
    if metric_family == "sources":
        return "source_coverage_gap"
    if metric_family == "non_gaap_relaxed":
        return "non_gaap_relaxed_mode"
    if metric_family == "tax_paid":
        return "cash_tax_fallback"
    if metric_family == "cogs":
        return "cogs_fallback"
    if metric_family == "capex" and "sign convention" in message_low:
        return "sign_convention_check"
    return metric_family or "review_issue"


def recommended_action(row_in: pd.Series) -> str:
    explicit_action_raw = row_in.get("recommended_action")
    explicit_action = str(explicit_action_raw).strip() if pd.notna(explicit_action_raw) else ""
    if explicit_action:
        return explicit_action
    issue_family_txt = issue_family(row_in)
    if issue_family_txt == "quarter_text_numeric_conflict":
        return "fix parser"
    if issue_family_txt == "quarter_text_definition_mismatch":
        return "review metric definition"
    if issue_family_txt == "quarter_text_no_explicit_support":
        return "review source coverage"
    if issue_family_txt == "quarter_text_low_confidence_support":
        return "watch only"
    if issue_family_txt in {
        "principal_tranche_tieout",
        "carrying_debt_tieout",
        "revolver_and_other_debt_presence_check",
    }:
        return "review debt definition"
    if issue_family_txt == "debt_recon_coverage_check":
        return "fix source preference"
    if issue_family_txt == "debt_tranches":
        return "watch only"
    return ""


def no_explicit_support_expectation(row_in: pd.Series) -> str:
    raw_metric_family = normalize_metric_family(row_in.get("raw_metric") or row_in.get("metric"))
    raw_metric_key = re.sub(r"[^a-z0-9]+", "_", raw_metric_family).strip("_")
    if raw_metric_key in STRONG_EXPECTED_NO_EXPLICIT_SUPPORT_METRICS:
        return "strong_expected"
    if raw_metric_key in OFTEN_NOT_EXPLICIT_NO_EXPLICIT_SUPPORT_METRICS:
        return "often_not_explicit"
    return "normal_expected"


def no_explicit_support_expectation_sort(row_in: pd.Series) -> int:
    expectation = no_explicit_support_expectation(row_in)
    return {
        "strong_expected": 0,
        "normal_expected": 1,
        "often_not_explicit": 2,
    }.get(expectation, 1)


def review_status(row_in: pd.Series, *, policy_ctx: Optional[WriterQAPolicyContext] = None) -> str:
    explicit_status_raw = row_in.get("review_status")
    explicit_status = str(explicit_status_raw).strip() if pd.notna(explicit_status_raw) else ""
    if explicit_status:
        return explicit_status
    if bool(row_in.get("is_expected_legacy")):
        return "Legacy"
    issue_family_txt = issue_family(row_in)
    if issue_family_txt in {"quarter_text_definition_mismatch", "revolver_and_other_debt_presence_check"}:
        return "Definition mismatch"
    if issue_family_txt == "quarter_text_no_explicit_support":
        expectation = no_explicit_support_expectation(row_in)
        return "Watch" if expectation == "often_not_explicit" else "Source gap"
    if issue_family_txt in {
        "bank_extraction_fallback",
        "share_count_fallback",
        "source_coverage_gap",
        "non_gaap_relaxed_mode",
        "cash_tax_fallback",
        "cogs_fallback",
        "sign_convention_check",
    }:
        return "Watch"
    if is_methodology_watch_issue(row_in):
        return "Watch"
    if policy_ctx is not None and is_current_review_relevant(row_in, policy_ctx=policy_ctx):
        return "Action required"
    severity_low = str(row_in.get("severity") or row_in.get("status") or "").strip().lower()
    if severity_low == "fail":
        return "Action required"
    return "Watch"


def review_status_sort(status_in: Any) -> int:
    status_txt = str(status_in or "").strip()
    return {
        "Action required": 0,
        "Definition mismatch": 1,
        "Source gap": 2,
        "Watch": 3,
        "Legacy": 4,
    }.get(status_txt, 5)


def is_methodology_watch_issue(row_in: pd.Series) -> bool:
    issue_family_txt = issue_family(row_in)
    message_low = str(row_in.get("message") or "").strip().lower()
    if issue_family_txt in {
        "quarter_text_low_confidence_support",
        "bank_extraction_fallback",
        "share_count_fallback",
        "source_coverage_gap",
        "non_gaap_relaxed_mode",
        "cash_tax_fallback",
        "cogs_fallback",
        "sign_convention_check",
    }:
        return True
    if issue_family_txt == "debt_tranches" and re.search(r"\b(scale inferred|fallback|heuristic|verify scaling)\b", message_low, re.I):
        return True
    return False


def is_current_review_relevant(row_in: pd.Series, *, policy_ctx: WriterQAPolicyContext) -> bool:
    severity_low = str(row_in.get("severity") or row_in.get("status") or "").strip().lower()
    if severity_low not in {"warn", "fail"}:
        return False
    q_norm = row_in.get("_quarter_norm")
    within_coverage = pd.notna(q_norm) and pd.Timestamp(q_norm).normalize() in policy_ctx.coverage_set
    is_latest_quarter = (
        pd.notna(q_norm)
        and pd.notna(policy_ctx.latest_coverage_q)
        and pd.Timestamp(q_norm).normalize() == pd.Timestamp(policy_ctx.latest_coverage_q).normalize()
    )
    if bool(row_in.get("is_expected_legacy")):
        return False
    if not bool(policy_ctx.material_warn_fn(row_in)):
        return False
    issue_family_txt = issue_family(row_in)
    if issue_family_txt in {
        "bank_extraction_fallback",
        "share_count_fallback",
        "source_coverage_gap",
        "non_gaap_relaxed_mode",
        "cash_tax_fallback",
        "cogs_fallback",
        "sign_convention_check",
    }:
        return False
    if severity_low == "fail" and (within_coverage or is_latest_quarter):
        return True
    if issue_family_txt in {
        "quarter_text_numeric_conflict",
        "quarter_text_definition_mismatch",
        "principal_tranche_tieout",
        "carrying_debt_tieout",
        "debt_recon_coverage_check",
    }:
        return bool(within_coverage or is_latest_quarter)
    if issue_family_txt in {"quarter_text_no_explicit_support", "revolver_and_other_debt_presence_check"}:
        if issue_family_txt == "quarter_text_no_explicit_support":
            if no_explicit_support_expectation(row_in) != "strong_expected":
                return False
        return bool(is_latest_quarter or within_coverage)
    if issue_family_txt in {"debt_tieout", "debt_recon"}:
        return bool(within_coverage or is_latest_quarter)
    if issue_family_txt == "debt_tranches":
        return bool(is_latest_quarter)
    if is_methodology_watch_issue(row_in):
        return bool(is_latest_quarter)
    return bool(within_coverage)


def quarter_bucket_index(q_in: Any) -> int:
    q_ts = pd.to_datetime(q_in, errors="coerce")
    if pd.isna(q_ts):
        return -1
    q_num = ((int(q_ts.month) - 1) // 3) + 1
    return int(q_ts.year) * 4 + q_num


def priority_for_cluster(
    latest_row_in: pd.Series,
    quarter_count: int,
    *,
    latest_coverage_q: pd.Timestamp | Any,
) -> Tuple[int, str]:
    severity_low = str(latest_row_in.get("severity") or latest_row_in.get("status") or "").strip().lower()
    q_norm = pd.to_datetime(latest_row_in.get("_quarter_norm"), errors="coerce")
    is_current = (
        pd.notna(q_norm)
        and pd.notna(latest_coverage_q)
        and pd.Timestamp(q_norm).normalize() == pd.Timestamp(latest_coverage_q).normalize()
    )
    issue_family_txt = issue_family(latest_row_in)
    is_methodology_watch = is_methodology_watch_issue(latest_row_in)
    if is_current and (
        severity_low == "fail"
        or issue_family_txt in {
            "quarter_text_numeric_conflict",
            "principal_tranche_tieout",
            "carrying_debt_tieout",
            "debt_recon_coverage_check",
        }
    ):
        return (0, "Current-quarter critical")
    if is_current and issue_family_txt in {"quarter_text_definition_mismatch", "revolver_and_other_debt_presence_check"}:
        return (1, "Current-quarter basis / definition issues")
    if is_current and issue_family_txt in {"quarter_text_no_explicit_support"}:
        return (2, "Current-quarter source gaps")
    if issue_family_txt in {
        "quarter_text_numeric_conflict",
        "quarter_text_definition_mismatch",
        "quarter_text_no_explicit_support",
        "principal_tranche_tieout",
        "carrying_debt_tieout",
        "debt_recon_coverage_check",
        "debt_tieout",
        "debt_recon",
    } or (quarter_count > 1 and not is_methodology_watch):
        return (3, "Persistent unresolved material")
    if is_methodology_watch:
        return (4, "Methodology / heuristic watch")
    return (3, "Persistent unresolved material")


def coalesce_curated_definition_rows(df_in: pd.DataFrame) -> pd.DataFrame:
    if df_in.empty:
        return df_in
    work = df_in.copy()
    work["_raw_metric_family_tmp"] = work["raw_metric"].apply(normalize_metric_family) if "raw_metric" in work.columns else ""
    work["_source_norm_tmp"] = work["source"].astype(str).str.strip().str.lower() if "source" in work.columns else ""
    work["_quarter_norm_tmp"] = pd.to_datetime(work.get("quarter"), errors="coerce").apply(
        lambda v: pd.Timestamp(v).normalize() if pd.notna(v) else pd.NaT
    )
    definition_rows = work[
        work["issue_family"].astype(str).eq("quarter_text_definition_mismatch")
        & work["_raw_metric_family_tmp"].astype(str).str.contains("total_debt", regex=False)
    ]
    debt_basis_rows = work[
        work["issue_family"].astype(str).eq("revolver_and_other_debt_presence_check")
    ]
    if not definition_rows.empty and not debt_basis_rows.empty:
        drop_idx: set[int] = set()
        for idx, row in definition_rows.iterrows():
            quarter_norm = row.get("_quarter_norm_tmp")
            source_norm = str(row.get("_source_norm_tmp") or "")
            matches = debt_basis_rows[
                debt_basis_rows["_quarter_norm_tmp"].eq(quarter_norm)
                & debt_basis_rows["_source_norm_tmp"].eq(source_norm)
            ]
            if not matches.empty:
                readable_metric = str(row.get("raw_metric") or row.get("metric") or "").strip()
                for match_idx in matches.index:
                    current_raw_metric = str(work.at[match_idx, "raw_metric"] or "").strip()
                    if normalize_metric_family(current_raw_metric) in {"", "debt_tieout"}:
                        work.at[match_idx, "raw_metric"] = readable_metric or "Debt basis"
                drop_idx.add(int(idx))
        if drop_idx:
            work = work.drop(index=list(drop_idx)).copy()

    debt_rows = work[work["issue_family"].astype(str).isin(DEBT_INTEGRITY_ISSUE_FAMILIES)].copy()
    if not debt_rows.empty:
        drop_idx = set()
        merged_rows = []
        debt_rows = debt_rows.sort_values(
            by=["_quarter_norm_tmp", "_priority_sort", "severity", "issue_family", "last_seen_q"],
            ascending=[False, True, True, True, False],
            kind="stable",
        )
        for quarter_key, grp in debt_rows.groupby(["_quarter_norm_tmp"], dropna=False, sort=False):
            if grp.empty or grp["issue_family"].nunique() < 2:
                continue
            quarter_norm = quarter_key[0] if isinstance(quarter_key, tuple) else quarter_key
            families = {str(x or "").strip() for x in grp["issue_family"].tolist()}
            anchor = grp.sort_values(
                by=["_priority_sort", "_review_status_sort", "severity", "last_seen_q"],
                ascending=[True, True, True, False],
                kind="stable",
            ).iloc[0]
            parts = []
            if "debt_recon_coverage_check" in families:
                parts.append("source preference / reconciliation coverage needs review")
            if {"carrying_debt_tieout", "principal_tranche_tieout"} & families:
                if {"carrying_debt_tieout", "principal_tranche_tieout"} <= families:
                    parts.append("carrying-value and principal debt totals do not tie cleanly")
                elif "carrying_debt_tieout" in families:
                    parts.append("carrying-value debt totals do not tie cleanly")
                else:
                    parts.append("principal tranche totals do not tie cleanly")
            if "revolver_and_other_debt_presence_check" in families:
                parts.append("reported debt basis differs from the modeled debt-core basis")
            if "debt_tranches" in families:
                parts.append("tranche scaling / heuristic follow-up remains")
            compact_message = "Debt integrity: " + "; ".join(parts[:4]) + "." if parts else str(anchor.get("latest_message") or anchor.get("message") or "").strip()
            recommended_action_txt = "watch only"
            if "debt_recon_coverage_check" in families:
                recommended_action_txt = "fix source preference"
            elif {"carrying_debt_tieout", "principal_tranche_tieout", "revolver_and_other_debt_presence_check"} & families:
                recommended_action_txt = "review debt definition"
            status_candidates = [str(x or "").strip() for x in grp.get("review_status", pd.Series([], dtype=object)).tolist() if str(x or "").strip()]
            severity_low = "fail" if any(str(x or "").strip().lower() == "fail" for x in grp.get("severity", pd.Series([], dtype=object)).tolist()) else str(anchor.get("severity") or "").strip().lower()
            merged_status = next(
                (
                    label
                    for label in ["Action required", "Definition mismatch", "Source gap", "Watch", "Legacy"]
                    if label in status_candidates
                ),
                "Watch",
            )
            source_values = []
            for source_txt in grp.get("source", pd.Series([], dtype=object)).tolist():
                source_clean = str(source_txt or "").strip()
                if source_clean and source_clean not in source_values:
                    source_values.append(source_clean)
            merged = anchor.to_dict()
            merged["issue_family"] = "debt_integrity"
            merged["metric"] = "Debt integrity"
            merged["raw_metric"] = "Debt integrity"
            merged["review_status"] = merged_status
            merged["severity"] = severity_low or str(anchor.get("severity") or "")
            merged["latest_message"] = compact_message
            merged["message"] = compact_message
            merged["recommended_action"] = recommended_action_txt
            merged["canonical_issue_key"] = (
                f"debt_integrity|{pd.Timestamp(quarter_norm).strftime('%Y-%m-%d') if pd.notna(quarter_norm) else ''}"
            ).strip("|")
            merged["source"] = " | ".join(source_values[:2])
            quarter_count_series = grp["quarter_count"] if "quarter_count" in grp.columns else pd.Series([1])
            merged["quarter_count"] = int(pd.to_numeric(quarter_count_series, errors="coerce").fillna(1).max())
            merged_rows.append(merged)
            drop_idx.update(int(idx) for idx in grp.index.tolist())
        if drop_idx:
            work = work.drop(index=list(drop_idx)).copy()
        if merged_rows:
            work = pd.concat([work, pd.DataFrame(merged_rows)], ignore_index=True, sort=False)
    return work.drop(columns=["_raw_metric_family_tmp", "_source_norm_tmp", "_quarter_norm_tmp"], errors="ignore")
