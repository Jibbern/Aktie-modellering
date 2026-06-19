"""Valuation debt/source-backed display support helpers."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, MutableMapping, Optional, Tuple


@dataclass(frozen=True)
class ValuationDebtSupportDeps:
    runtime: MutableMapping[str, Any]


def source_backed_debt_tranches_from_slides(
    deps: ValuationDebtSupportDeps,
    slides_debt: Any,
    latest_quarter: Any,
) -> Any:
    """Return a deduped, source-backed tranche display table for debt detail.

    This is intentionally a display fallback for cases where the stricter tranche
    tie-out guardrail suppresses Debt_Tranches_Latest.  It does not override the
    carrying debt basis; it gives the user the current source schedule plus a
    reconciliation row.
    """
    runtime = deps.runtime
    pd = runtime["pd"]
    re = runtime["re"]

    if slides_debt is None or getattr(slides_debt, "empty", True):
        return pd.DataFrame()
    q = pd.to_datetime(latest_quarter, errors="coerce")
    if pd.isna(q):
        return pd.DataFrame()
    df = slides_debt.copy()
    if "quarter" not in df.columns:
        return pd.DataFrame()
    df["quarter"] = pd.to_datetime(df["quarter"], errors="coerce")
    df = df[df["quarter"].dt.normalize().eq(pd.Timestamp(q).normalize())]
    if df.empty:
        return pd.DataFrame()
    if "is_table_total" in df.columns:
        df = df[~df["is_table_total"].fillna(False).astype(bool)]
    if "amount" not in df.columns or "tranche" not in df.columns:
        return pd.DataFrame()
    df["amount_num"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df[df["amount_num"].notna() & (df["amount_num"] > 0)]
    if df.empty:
        return pd.DataFrame()
    # Prefer the parsed statement table over PDF/text fragments when both are
    # available for the same quarter.  The PDF extractor often emits footnote
    # rows such as "2.25% ... 1,897 --" or generic Tallgrass rows that are
    # useful evidence but must not be added to the tranche principal schedule.
    if "doc" in df.columns:
        doc_txt = df["doc"].astype(str).str.lower()
        html_mask = doc_txt.str.endswith((".htm", ".html"))
        if "asof_match_found" in df.columns:
            asof_mask = df["asof_match_found"].fillna(False).astype(bool)
        else:
            asof_mask = pd.Series(True, index=df.index)
        preferred = df[html_mask & asof_mask]
        if not preferred.empty:
            df = preferred
        else:
            matched = df[asof_mask]
            if not matched.empty:
                df = matched

    def _clean_tranche_name(v: Any) -> str:
        txt = str(v or "").strip()
        txt = re.sub(r"\s+\$\s*[0-9,]+(?:\s+\$\s*[0-9,]+)*\s*$", "", txt)
        txt = re.sub(r"\s+[0-9]{1,3}(?:,[0-9]{3})+(?:\s+[0-9]{1,3}(?:,[0-9]{3})+)*\s*$", "", txt)
        txt = re.sub(r"\s+", " ", txt).strip()
        return txt

    def _dedup_key(row: Any) -> Tuple[str, Optional[int], int]:
        name = _clean_tranche_name(row.get("tranche"))
        name_norm = re.sub(r"\s*\(\d+\)\s*", "", name.lower())
        name_norm = re.sub(r"[^a-z0-9.%]+", " ", name_norm).strip()
        mat = pd.to_numeric(row.get("maturity_year"), errors="coerce")
        mat_key = int(mat) if pd.notna(mat) else None
        amt_key = int(round(float(row.get("amount_num") or 0.0), -3))
        return name_norm, mat_key, amt_key

    def _priority(row: Any) -> Tuple[int, int, int]:
        doc = str(row.get("doc") or "").lower()
        source = str(row.get("source") or "").lower()
        asof = bool(row.get("asof_match_found")) if "asof_match_found" in row.index else False
        htmlish = doc.endswith((".htm", ".html"))
        return (1 if asof else 0, 1 if htmlish else 0, 1 if source == "financial_statement" else 0)

    rows: Dict[Tuple[str, Optional[int], int], Any] = {}
    for _, row in df.iterrows():
        key = _dedup_key(row)
        if not key[0]:
            continue
        prev = rows.get(key)
        if prev is None or _priority(row) > _priority(prev):
            rows[key] = row

    out_rows: List[Dict[str, Any]] = []
    latest_year = int(pd.Timestamp(q).year)
    for _, row in sorted(
        rows.items(),
        key=lambda kv: (
            9999 if kv[1].get("maturity_year") is None or pd.isna(pd.to_numeric(kv[1].get("maturity_year"), errors="coerce")) else int(pd.to_numeric(kv[1].get("maturity_year"), errors="coerce")),
            str(kv[1].get("tranche") or ""),
        ),
    ):
        name = _clean_tranche_name(row.get("tranche"))
        mat = pd.to_numeric(row.get("maturity_year"), errors="coerce")
        mat_year = int(mat) if pd.notna(mat) else None
        coupon = None
        m_coupon = re.search(r"\b([0-9]+(?:\.[0-9]+)?)\s*%", name)
        if m_coupon:
            try:
                coupon = float(m_coupon.group(1)) / 100.0
            except Exception:
                coupon = None
        near_term = bool(mat_year is not None and mat_year <= latest_year + 1)
        out_rows.append(
            {
                "tranche_name": name,
                "amount_principal": float(row.get("amount_num")),
                "amount_carrying": None,
                "maturity_display": str(mat_year) if mat_year is not None else "",
                "maturity_year": mat_year,
                "rate_type": "fixed" if coupon is not None else None,
                "coupon_pct": coupon,
                "spread_pct": None,
                "near_term": near_term,
                "source_kind": "Slides_Debt_Profile",
                "source_basis": (
                    "source-backed principal; near-term = within 24 months of latest quarter end; "
                    "year-based conservative classification when exact maturity date is unavailable"
                ),
                "qa_status": "WARN",
                "review_note": "Fallback source-backed debt schedule shown because tranche tie-out guardrail suppressed Debt_Tranches_Latest.",
            }
        )
    return pd.DataFrame(out_rows)
