from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd


def validate_history(hist: pd.DataFrame) -> pd.DataFrame:
    issues: List[Dict[str, Any]] = []
    if hist is None or hist.empty:
        return pd.DataFrame()

    h = hist.sort_values("quarter").copy()

    # Revenue spike heuristic: catches YTD/annual accidentally included
    if "revenue" in h.columns:
        rev = pd.to_numeric(h["revenue"], errors="coerce")
        med = rev.rolling(8, min_periods=4).median()
        spike = (rev > 3.5 * med) & (rev > 1.0e9)
        for q in h.loc[spike.fillna(False), "quarter"]:
            issues.append({
                "quarter": str(q),
                "metric": "revenue",
                "severity": "fail",
                "message": "Revenue spike vs rolling median (might be YTD/annual).",
            })

        if "cogs" in h.columns:
            cogs = pd.to_numeric(h["cogs"], errors="coerce")
            gp = rev - cogs
            bad = (gp > 1.05 * rev) & rev.notna() & cogs.notna()
            for q in h.loc[bad.fillna(False), "quarter"]:
                issues.append({
                    "quarter": str(q),
                    "metric": "gross_profit",
                    "severity": "warn",
                    "message": "Gross profit > revenue (tag mismatch?).",
                })

    # Shares sanity
    if "shares_diluted" in h.columns:
        sh = pd.to_numeric(h["shares_diluted"], errors="coerce")
        bad = (sh < 10e6) | (sh > 500e6)
        for q in h.loc[bad.fillna(False), "quarter"]:
            issues.append({
                "quarter": str(q),
                "metric": "shares_diluted",
                "severity": "warn",
                "message": "Diluted shares outside typical range; verify tag/unit.",
            })

    return pd.DataFrame(issues)


def validate_debt_tieout(
    hist: pd.DataFrame,
    debt_tranches: pd.DataFrame,
    long_term_debt: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if hist is None or debt_tranches is None or hist.empty or debt_tranches.empty:
        return pd.DataFrame()
    s = debt_tranches.groupby("quarter")["amount"].sum().reset_index().rename(columns={"amount": "tranche_sum"})
    ttd = None
    if "table_total_debt" in debt_tranches.columns:
        ttd = (
            debt_tranches[["quarter", "table_total_debt"]]
            .dropna(subset=["table_total_debt"])
            .groupby("quarter")["table_total_debt"]
            .max()
            .reset_index()
        )

    m = hist[["quarter", "total_debt"]].merge(s, on="quarter", how="left")
    if ttd is not None and not ttd.empty:
        m = m.merge(ttd, on="quarter", how="left")
        base = "table_total_debt"
    else:
        return pd.DataFrame()

    if long_term_debt is not None and not long_term_debt.empty:
        m = m.merge(long_term_debt, on="quarter", how="left")

    # Infer scale if table totals look like thousands/millions vs XBRL
    def _infer_scale(row: pd.Series) -> float:
        try:
            td = float(row.get("total_debt")) if pd.notna(row.get("total_debt")) else None
            ttd_val = float(row.get("table_total_debt")) if pd.notna(row.get("table_total_debt")) else None
            if not td or not ttd_val:
                return 1.0
            ratio = td / ttd_val if ttd_val != 0 else 1.0
            if 500 <= ratio <= 2000:
                return 1000.0
            if 500_000 <= ratio <= 2_000_000:
                return 1_000_000.0
        except Exception:
            pass
        return 1.0

    m["scale_applied"] = m.apply(_infer_scale, axis=1)
    m["table_total_debt_scaled"] = m["table_total_debt"] * m["scale_applied"]
    m["tranche_sum_scaled"] = m["tranche_sum"] * m["scale_applied"]

    m["diff"] = m["table_total_debt_scaled"] - m["total_debt"]

    denom = pd.to_numeric(m["total_debt"], errors="coerce").replace(0, pd.NA)
    m["diff_pct"] = m["diff"] / denom
    issues = m[(m["total_debt"].notna()) & (m[base].notna()) & (m["diff_pct"].abs() > 0.02)].copy()
    if issues.empty:
        return pd.DataFrame()

    out_rows = []
    if "long_term_debt" in issues.columns:
        denom_lt = pd.to_numeric(issues["long_term_debt"], errors="coerce").replace(0, pd.NA)
        issues["lt_diff_pct"] = (issues["table_total_debt_scaled"] - issues["long_term_debt"]) / denom_lt
        close_lt = issues["long_term_debt"].notna() & issues["lt_diff_pct"].abs().le(0.05)
        if close_lt.any():
            lt_rows = issues[close_lt].copy()
            lt_rows["metric"] = "debt_tieout"
            lt_rows["severity"] = "warn"
            lt_rows["message"] = "Table total aligns with long-term debt; total_debt tieout skipped."
            lt_rows["diff"] = lt_rows["table_total_debt_scaled"] - lt_rows["long_term_debt"]
            lt_rows["diff_pct"] = lt_rows["lt_diff_pct"]
            out_rows.append(lt_rows)
            issues = issues[~close_lt].copy()

    if not issues.empty:
        issues["metric"] = "debt_tieout"
        issues["severity"] = issues["diff_pct"].abs().apply(lambda x: "fail" if x >= 0.10 else "warn")
        issues["message"] = "Total debt from table differs from XBRL total_debt by >2%."
        out_rows.append(issues)

    if not out_rows:
        return pd.DataFrame()
    out = pd.concat(out_rows, ignore_index=True)
    return out[["quarter", "metric", "severity", "message", "total_debt", "table_total_debt", "tranche_sum", "diff", "diff_pct", "scale_applied"]]


def needs_review_from_audit(audit: pd.DataFrame) -> pd.DataFrame:
    if audit is None or audit.empty:
        return pd.DataFrame()

    rows: List[Dict[str, Any]] = []
    for _, r in audit.iterrows():
        src = str(r.get("source", "") or "").lower()
        metric = r.get("metric")
        quarter = r.get("quarter")
        if src == "missing":
            if metric == "shares_diluted":
                rows.append({
                    "quarter": str(quarter),
                    "metric": metric,
                    "severity": "warn",
                    "message": "Missing direct 3M share count; not interpolated.",
                    "source": src,
                })
                continue
            rows.append({
                "quarter": str(quarter),
                "metric": metric,
                "severity": "fail",
                "message": "Missing direct fact for quarter; value blank.",
                "source": src,
            })
        elif src.startswith("derived_ytd"):
            rows.append({
                "quarter": str(quarter),
                "metric": metric,
                "severity": "warn",
                "message": "Derived from YTD (no reliable direct 3M fact).",
                "source": src,
            })
        elif src.startswith("tier2"):
            rows.append({
                "quarter": str(quarter),
                "metric": metric,
                "severity": "warn",
                "message": "Fallback from SEC filing table/note (heuristic).",
                "source": src,
            })
        elif src.startswith("tier3"):
            rows.append({
                "quarter": str(quarter),
                "metric": metric,
                "severity": "warn",
                "message": "Tier 3 fallback (exhibit/slides/OCR). Verify.",
                "source": src,
            })

    return pd.DataFrame(rows)


def info_log_from_audit(audit: pd.DataFrame) -> pd.DataFrame:
    if audit is None or audit.empty:
        return pd.DataFrame()

    rows: List[Dict[str, Any]] = []
    for _, r in audit.iterrows():
        src = str(r.get("source", "") or "").lower()
        metric = r.get("metric")
        quarter = r.get("quarter")
        if src == "derived_formula":
            rows.append({
                "quarter": str(quarter),
                "metric": metric,
                "severity": "info",
                "message": "Derived from formula using GAAP components.",
                "source": src,
            })

    return pd.DataFrame(rows)
