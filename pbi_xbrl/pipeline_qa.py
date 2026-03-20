from __future__ import annotations

from typing import Any, Iterable, List, Optional

import pandas as pd


LOW_VALUE_REVIEW_METRICS = {"non_gaap", "non_gaap_relaxed"}
LOW_VALUE_REVIEW_SOURCES = {"relaxed_mode", "scale_inferred", "history_coverage"}
LOW_VALUE_QA_CHECKS = {"promise_scorable", "non_gaap_cred"}


def concat_frames(*frames: Optional[pd.DataFrame]) -> pd.DataFrame:
    valid = [frame for frame in frames if frame is not None and not frame.empty]
    if not valid:
        return pd.DataFrame()
    return pd.concat(valid, ignore_index=True, sort=False)


def build_promise_qa_checks(promises: pd.DataFrame, promise_progress: pd.DataFrame) -> pd.DataFrame:
    rows: List[dict[str, Any]] = []
    if promises is not None and not promises.empty:
        for _, row in promises.iterrows():
            severity = str(row.get("qa_severity") or "").strip().lower()
            if severity not in {"warn", "fail"}:
                continue
            rows.append(
                {
                    "quarter": row.get("first_seen_quarter"),
                    "metric": "promise_tracker",
                    "check": "promise_scorable",
                    "status": severity,
                    "message": str(row.get("qa_message") or "Promise QA issue."),
                    "promise_id": row.get("promise_id"),
                }
            )
    if promise_progress is not None and not promise_progress.empty:
        for _, row in promise_progress.iterrows():
            severity = str(row.get("qa_severity") or "").strip().lower()
            if severity not in {"warn", "fail"}:
                continue
            rows.append(
                {
                    "quarter": row.get("quarter"),
                    "metric": "promise_progress",
                    "check": "promise_scorable",
                    "status": severity,
                    "message": str(row.get("qa_message") or "Promise progress QA issue."),
                    "promise_id": row.get("promise_id"),
                }
            )
    return pd.DataFrame(rows)


def build_non_gaap_cred_qa(non_gaap_cred: pd.DataFrame) -> pd.DataFrame:
    rows: List[dict[str, Any]] = []
    if non_gaap_cred is None or non_gaap_cred.empty:
        return pd.DataFrame()
    for _, row in non_gaap_cred.iterrows():
        severity = str(row.get("qa_status") or "").strip().lower()
        if severity not in {"warn", "fail"}:
            continue
        rows.append(
            {
                "quarter": row.get("quarter"),
                "metric": "non_gaap_credibility",
                "check": "non_gaap_cred",
                "status": severity,
                "message": str(row.get("qa_reasons_text") or "Non-GAAP credibility QA issue."),
                "promise_id": "",
            }
        )
    return pd.DataFrame(rows)


def _latest_quarter_window(values: Iterable[Any], keep_quarters: int) -> set[pd.Timestamp]:
    parsed = sorted({pd.Timestamp(v).normalize() for v in values if pd.notna(v)})
    if not parsed or keep_quarters <= 0:
        return set(parsed)
    return set(parsed[-keep_quarters:])


def _dedupe_rows(frame: pd.DataFrame, key_cols: list[str]) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame() if frame is None else frame
    cols = [col for col in key_cols if col in frame.columns]
    if not cols:
        return frame.reset_index(drop=True)
    return frame.drop_duplicates(subset=cols, keep="last").reset_index(drop=True)


def finalize_qa_checks(qa_checks: pd.DataFrame, *, review_quarters: int = 8) -> pd.DataFrame:
    if qa_checks is None or qa_checks.empty:
        return pd.DataFrame()
    frame = qa_checks.copy()
    if "quarter" in frame.columns:
        frame["quarter"] = pd.to_datetime(frame["quarter"], errors="coerce")
    frame = _dedupe_rows(frame, ["quarter", "metric", "check", "status", "message", "promise_id"])
    if "status" not in frame.columns or "check" not in frame.columns or "quarter" not in frame.columns:
        return frame

    keep_quarters = _latest_quarter_window(frame["quarter"], review_quarters)
    low_value_mask = (
        frame["status"].astype(str).str.lower().isin({"warn", "info"})
        & frame["check"].astype(str).isin(LOW_VALUE_QA_CHECKS)
        & frame["quarter"].notna()
        & ~frame["quarter"].dt.normalize().isin(keep_quarters)
    )
    return frame.loc[~low_value_mask].reset_index(drop=True)


def finalize_needs_review(needs_review: pd.DataFrame, *, review_quarters: int = 8) -> pd.DataFrame:
    if needs_review is None or needs_review.empty:
        return pd.DataFrame()
    frame = needs_review.copy()
    if "quarter" in frame.columns:
        frame["quarter"] = pd.to_datetime(frame["quarter"], errors="coerce")
    frame = _dedupe_rows(frame, ["quarter", "metric", "severity", "message", "source"])
    if not {"severity", "quarter"}.issubset(frame.columns):
        return frame

    keep_quarters = _latest_quarter_window(frame["quarter"], review_quarters)
    severity = frame["severity"].astype(str).str.lower()
    metric = frame.get("metric", pd.Series([""] * len(frame))).astype(str)
    source = frame.get("source", pd.Series([""] * len(frame))).astype(str)
    low_value_mask = (
        severity.isin({"warn", "info"})
        & frame["quarter"].notna()
        & (~frame["quarter"].dt.normalize().isin(keep_quarters))
        & (metric.isin(LOW_VALUE_REVIEW_METRICS) | source.isin(LOW_VALUE_REVIEW_SOURCES))
    )
    return frame.loc[~low_value_mask].reset_index(drop=True)
