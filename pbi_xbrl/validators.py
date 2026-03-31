"""Validation helpers that turn raw audit checks into user-facing QA frames."""
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

    def _fmt_amt(val_in: Any) -> str:
        try:
            val = float(val_in)
        except Exception:
            return "n/a"
        sign = "-" if val < 0 else ""
        aval = abs(val)
        if aval >= 1_000_000_000:
            body = f"{aval / 1_000_000_000:,.2f}".rstrip("0").rstrip(".")
            return f"{sign}${body}bn"
        body = f"{aval / 1_000_000:,.1f}".rstrip("0").rstrip(".")
        return f"{sign}${body}m"

    def _safe_ratio(num_in: Any, den_in: Any) -> float | None:
        try:
            den = float(den_in)
            if den == 0:
                return None
            return float(num_in) / den
        except Exception:
            return None

    def _build_diagnostic_message(row: pd.Series) -> str:
        total_debt = pd.to_numeric(row.get("total_debt"), errors="coerce")
        table_total = pd.to_numeric(row.get("table_total_debt_scaled"), errors="coerce")
        tranche_sum = pd.to_numeric(row.get("tranche_sum_scaled"), errors="coerce")
        long_term_total = pd.to_numeric(row.get("long_term_debt"), errors="coerce")
        diff_val = pd.to_numeric(row.get("diff"), errors="coerce")
        if pd.isna(total_debt) or pd.isna(table_total):
            return "Total debt from table differs from XBRL total_debt by >2%."

        parts: List[str] = [
            f"Debt table total {_fmt_amt(table_total)} vs XBRL total debt {_fmt_amt(total_debt)} (gap {_fmt_amt(diff_val)})."
        ]
        tranche_ratio = _safe_ratio((tranche_sum - total_debt) if pd.notna(tranche_sum) else None, total_debt)
        if pd.notna(tranche_sum) and tranche_ratio is not None and abs(tranche_ratio) <= 0.03:
            parts.append(
                f"Tranche principal sum {_fmt_amt(tranche_sum)} is close to XBRL total debt, so the mismatch appears to sit in the debt-table total rather than tranche math."
            )
        elif pd.notna(tranche_sum):
            parts.append(f"Tranche principal sum is {_fmt_amt(tranche_sum)}.")

        lt_ratio = _safe_ratio((table_total - long_term_total) if pd.notna(long_term_total) else None, long_term_total)
        if pd.notna(long_term_total) and lt_ratio is not None and abs(lt_ratio) <= 0.05:
            parts.append(
                f"Debt table total is close to long-term debt {_fmt_amt(long_term_total)}, which suggests current portion and/or short-term debt may sit outside the table total."
            )
            current_portion = float(total_debt) - float(long_term_total)
            if current_portion > 0:
                gap_match_ratio = _safe_ratio(abs(abs(diff_val) - current_portion), max(current_portion, 1.0))
                if gap_match_ratio is not None and gap_match_ratio <= 0.20:
                    parts.append(f"Gap is broadly similar to current portion ({_fmt_amt(current_portion)}).")

        if pd.notna(tranche_sum) and abs(float(tranche_sum) - float(table_total)) >= 25_000_000.0:
            parts.append(
                "Table total also sits below principal tranche sum, which can happen when the table is carrying-value based or excludes a current portion."
            )

        return " ".join(parts)

    out_rows: List[Dict[str, Any]] = []

    for _, row in issues.iterrows():
        quarter_val = row.get("quarter")
        total_debt = pd.to_numeric(row.get("total_debt"), errors="coerce")
        table_total = pd.to_numeric(row.get("table_total_debt_scaled"), errors="coerce")
        tranche_sum = pd.to_numeric(row.get("tranche_sum_scaled"), errors="coerce")
        long_term_total = pd.to_numeric(row.get("long_term_debt"), errors="coerce")
        diff_val = pd.to_numeric(row.get("diff"), errors="coerce")
        diff_pct = pd.to_numeric(row.get("diff_pct"), errors="coerce")

        tranche_diff = None
        tranche_diff_pct = None
        tranche_close_to_total = False
        if pd.notna(tranche_sum) and pd.notna(total_debt):
            tranche_diff = float(tranche_sum) - float(total_debt)
            tranche_diff_pct = tranche_diff / max(abs(float(total_debt)), 1.0)
            tranche_close_to_total = abs(float(tranche_diff_pct)) <= 0.03

        table_close_to_lt = False
        current_portion = None
        if pd.notna(long_term_total) and pd.notna(table_total):
            lt_diff_pct = (float(table_total) - float(long_term_total)) / max(abs(float(long_term_total)), 1.0)
            table_close_to_lt = abs(float(lt_diff_pct)) <= 0.05
            if pd.notna(total_debt):
                current_portion = float(total_debt) - float(long_term_total)

        if tranche_diff_pct is not None and abs(float(tranche_diff_pct)) > 0.02:
            out_rows.append(
                {
                    "quarter": quarter_val,
                    "metric": "debt_tieout",
                    "issue_family": "principal_tranche_tieout",
                    "severity": "fail" if abs(float(tranche_diff_pct)) >= 0.10 else "warn",
                    "message": (
                        f"Tranche principal sum {_fmt_amt(tranche_sum)} vs XBRL total debt {_fmt_amt(total_debt)}; "
                        "principal-oriented debt totals do not align."
                    ),
                    "total_debt": total_debt,
                    "table_total_debt": row.get("table_total_debt"),
                    "tranche_sum": row.get("tranche_sum"),
                    "diff": tranche_diff,
                    "diff_pct": tranche_diff_pct,
                    "scale_applied": row.get("scale_applied"),
                    "long_term_debt": long_term_total,
                }
            )

        if pd.notna(table_total) and pd.notna(total_debt):
            carrying_message = _build_diagnostic_message(row)
            out_rows.append(
                {
                    "quarter": quarter_val,
                    "metric": "debt_tieout",
                    "issue_family": "carrying_debt_tieout",
                    "severity": "fail" if abs(float(diff_pct)) >= 0.10 else "warn",
                    "message": carrying_message,
                    "total_debt": total_debt,
                    "table_total_debt": row.get("table_total_debt"),
                    "tranche_sum": row.get("tranche_sum"),
                    "diff": diff_val,
                    "diff_pct": diff_pct,
                    "scale_applied": row.get("scale_applied"),
                    "long_term_debt": long_term_total,
                }
            )

        if tranche_close_to_total and (table_close_to_lt or (current_portion is not None and abs(float(diff_val or 0.0) - float(current_portion)) <= max(5_000_000.0, abs(float(current_portion)) * 0.25))):
            msg = "Revolver/current portion appears outside tranche principal table."
            if table_close_to_lt and pd.notna(long_term_total):
                msg = (
                    f"Debt-table total {_fmt_amt(table_total)} is close to long-term debt {_fmt_amt(long_term_total)}, "
                    "which suggests current portion and/or revolver borrowings sit outside the tranche table."
                )
            out_rows.append(
                {
                    "quarter": quarter_val,
                    "metric": "debt_tieout",
                    "issue_family": "revolver_and_other_debt_presence_check",
                    "severity": "warn",
                    "message": msg,
                    "total_debt": total_debt,
                    "table_total_debt": row.get("table_total_debt"),
                    "tranche_sum": row.get("tranche_sum"),
                    "diff": diff_val,
                    "diff_pct": diff_pct,
                    "scale_applied": row.get("scale_applied"),
                    "long_term_debt": long_term_total,
                }
            )

    if not out_rows:
        return pd.DataFrame()
    out = pd.DataFrame(out_rows)
    keep_cols = [
        "quarter",
        "metric",
        "issue_family",
        "severity",
        "message",
        "total_debt",
        "table_total_debt",
        "tranche_sum",
        "diff",
        "diff_pct",
        "scale_applied",
    ]
    if "long_term_debt" in out.columns:
        keep_cols.append("long_term_debt")
    return out[keep_cols]


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
