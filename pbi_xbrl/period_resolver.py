from __future__ import annotations

import dataclasses
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .metrics import MetricSpec, GAAP_SPECS


def classify_duration(days: Optional[float]) -> Optional[str]:
    """Classify start/end duration to guard against 3M vs 9M selection errors."""
    if days is None or pd.isna(days):
        return None
    d = float(days)
    if 75 <= d <= 115:
        return "3M"
    if 150 <= d <= 225:
        return "6M"
    if 235 <= d <= 320:
        return "9M"
    if 330 <= d <= 420:
        return "FY"
    return "OTHER"


def _filter_unit(df: pd.DataFrame, spec: MetricSpec) -> pd.DataFrame:
    if df.empty:
        return df
    if spec.unit.lower() == "usd":
        u = df["unit"].astype(str).str.upper()
        return df[u.str.startswith("USD", na=False) & ~u.str.contains("/", na=False)].copy()
    if spec.unit.lower() == "shares":
        u = df["unit"].astype(str).str.lower()
        return df[u.str.contains("share", na=False) & ~u.str.contains("usd", na=False)].copy()
    return df.copy()


def _rank_form(form: str, prefer: List[str]) -> int:
    try:
        return prefer.index(form)
    except ValueError:
        return len(prefer) + 10


def _duration_days(end_series: pd.Series, start_series: pd.Series) -> pd.Series:
    e = pd.to_datetime(end_series, errors="coerce")
    s = pd.to_datetime(start_series, errors="coerce")
    return (e - s).dt.days


def _fy_col(df: pd.DataFrame) -> str:
    return "fy_calc" if "fy_calc" in df.columns else "fy"


def _infer_fy_end_mmdd(facts: pd.DataFrame) -> Tuple[int, int]:
    """Infer fiscal-year end month/day from facts, defaulting to 12/31."""
    try:
        if facts is None or facts.empty or "end_d" not in facts.columns:
            return (12, 31)
        tmp = facts.copy()
        if "fp" in tmp.columns:
            fp = tmp["fp"].astype(str).str.upper().str.strip()
            tmp = tmp[(fp == "FY") | (fp == "Q4")]
        tmp = tmp[tmp["end_d"].notna()]
        if tmp.empty:
            return (12, 31)
        mmdd = tmp["end_d"].map(lambda d: (d.month, d.day))
        if mmdd.empty:
            return (12, 31)
        return mmdd.value_counts().idxmax()
    except Exception:
        return (12, 31)


def _calc_fy_from_end(end_d: Any, fy_end_mmdd: Tuple[int, int] = (12, 31)) -> Optional[int]:
    if end_d is None or pd.isna(end_d):
        return None
    try:
        mmdd = fy_end_mmdd if isinstance(fy_end_mmdd, tuple) and len(fy_end_mmdd) == 2 else (12, 31)
        if (int(end_d.month), int(end_d.day)) > (int(mmdd[0]), int(mmdd[1])):
            return int(end_d.year) + 1
        return int(end_d.year)
    except Exception:
        return None


def _fy_val(row: pd.Series, fy_end_mmdd: Tuple[int, int] = (12, 31)) -> Optional[int]:
    if "fy_calc" in row and pd.notna(row.get("fy_calc")):
        return int(row.get("fy_calc"))
    end_d = row.get("end_d")
    fy_from_end = _calc_fy_from_end(end_d, fy_end_mmdd=fy_end_mmdd)
    if fy_from_end is not None:
        return int(fy_from_end)
    if "fy" in row and pd.notna(row.get("fy")):
        return int(row.get("fy"))
    return None


def quarter_ends_for_fy(fy_end_date: dt.date) -> Dict[str, dt.date]:
    """
    Build deterministic quarter-end dates for a fiscal year-end date.
    Works for non-calendar fiscal years by shifting from FY end.
    """
    ts = pd.Timestamp(fy_end_date)

    def _minus_months_eom(months: int) -> dt.date:
        return (ts - pd.DateOffset(months=months) + pd.offsets.MonthEnd(0)).date()

    return {
        "Q1": _minus_months_eom(9),
        "Q2": _minus_months_eom(6),
        "Q3": _minus_months_eom(3),
        "FY": fy_end_date,
    }


def choose_best_tag(df: pd.DataFrame, spec: MetricSpec) -> Optional[str]:
    """
    Choose the 'best' tag among candidates:
    - duration metrics: prefer tags (in spec order) that have 3M coverage
    - if none have 3M, choose tag with best YTD/FY coverage (proxy)
    - instant metrics: maximize count of ends
    - recency tiebreaker
    """
    if df.empty:
        return None

    # Build coverage stats per tag
    stats: Dict[str, Tuple[int, int, int]] = {}
    for tag in df["tag"].unique():
        s = df[df["tag"] == tag].copy()
        if spec.kind == "instant":
            stats[tag] = (int(s["end_d"].nunique()), 0, 0)
            continue
        s = s[s["start_d"].notna() & s["end_d"].notna()]
        if s.empty:
            stats[tag] = (0, 0, 0)
            continue
        s["dur"] = _duration_days(s["end_d"], s["start_d"])
        s["dur_class"] = s["dur"].apply(classify_duration)
        n_3m = int((s["dur_class"] == "3M").sum())
        n_ytd = int(s["dur_class"].isin(["6M", "9M", "FY"]).sum())
        rec = int(s["end_d"].max().toordinal()) if pd.notna(s["end_d"].max()) else 0
        stats[tag] = (n_3m, n_ytd, rec)

    # Prefer tags in spec order with 3M coverage
    if spec.kind != "instant" and spec.tags:
        for tag in spec.tags:
            if tag in stats and stats[tag][0] > 0:
                return tag

    # Otherwise score by coverage + recency
    scored: List[Tuple[int, int, str]] = []
    for tag, (n_3m, n_ytd, rec) in stats.items():
        if spec.kind == "instant":
            n = n_3m
        else:
            n = n_3m if n_3m > 0 else (n_ytd // 2)
        scored.append((n, rec, tag))

    scored.sort(reverse=True)
    return scored[0][2] if scored else None


def pick_best_instant(facts: pd.DataFrame, end: dt.date, prefer_forms: List[str]) -> Optional[pd.Series]:
    sub = facts[facts["end_d"] == end].copy()
    if sub.empty:
        return None
    sub["form_rank"] = sub["form"].fillna("").apply(lambda x: _rank_form(x, prefer_forms))
    sub = sub.sort_values(["form_rank", "filed_d"], ascending=[True, False])
    return sub.iloc[0]


def pick_best_duration(facts: pd.DataFrame, end: dt.date, target: str, prefer_forms: List[str]) -> Optional[pd.Series]:
    sub = facts[(facts["end_d"] == end) & facts["start_d"].notna()].copy()
    if sub.empty:
        return None
    sub["dur"] = _duration_days(sub["end_d"], sub["start_d"])
    sub["dur_class"] = sub["dur"].apply(classify_duration)
    sub = sub[sub["dur_class"] == target]
    if sub.empty:
        return None
    sub["form_rank"] = sub["form"].fillna("").apply(lambda x: _rank_form(x, prefer_forms))
    sub = sub.sort_values(["form_rank", "filed_d"], ascending=[True, False])
    return sub.iloc[0]


@dataclasses.dataclass
class PickResult:
    value: Optional[float]
    source: str  # direct/derived_ytd/missing
    source_choice: Optional[str] = None
    tag: Optional[str] = None
    accn: Optional[str] = None
    form: Optional[str] = None
    filed: Optional[dt.date] = None
    start: Optional[dt.date] = None
    end: Optional[dt.date] = None
    unit: Optional[str] = None
    duration_days: Optional[int] = None
    note: str = ""


def derive_quarter_from_ytd(
    facts: pd.DataFrame,
    end: dt.date,
    quarter_index: int,
    fy_fp_to_end: Dict[Tuple[int, str], dt.date],
    prefer_forms: List[str],
    *,
    allow_negative: bool = True,
    allow_override: bool = True,
    max_filed_gap_days: int = 200,
) -> Optional[PickResult]:
    fy_end_mmdd = _infer_fy_end_mmdd(facts)

    def _direct_pick() -> Optional[PickResult]:
        direct = pick_best_duration(facts, end=end, target="3M", prefer_forms=prefer_forms)
        if direct is None:
            return None
        return PickResult(
            value=float(direct["val"]),
            source="direct",
            tag=str(direct["tag"]),
            accn=str(direct["accn"]),
            form=str(direct["form"]),
            filed=direct["filed_d"],
            start=direct["start_d"],
            end=direct["end_d"],
            unit=str(direct["unit"]),
            duration_days=int((direct["end_d"] - direct["start_d"]).days),
            note="picked direct 3M",
        )

    def _derived_pick() -> Optional[PickResult]:
        def _infer_fy_end_for(target_end: dt.date, qi: int, fy_v: Optional[int]) -> Optional[dt.date]:
            # Prefer explicit FY mapping when present.
            if fy_v is not None:
                fy_end = fy_fp_to_end.get((int(fy_v), "FY"))
                if fy_end is not None:
                    return fy_end
            if target_end is None:
                return None
            if qi <= 0:
                return None
            # Deterministic fallback: move from current quarter-end to Q4 end.
            ts = pd.Timestamp(target_end) + pd.DateOffset(months=(4 - int(qi)) * 3)
            return (ts + pd.offsets.MonthEnd(0)).date()

        def _quarter_end_from_map_or_fallback(
            *,
            target_end: dt.date,
            qi: int,
            fy_v: Optional[int],
            wanted_fp: str,
        ) -> Optional[dt.date]:
            if fy_v is not None:
                q_end = fy_fp_to_end.get((int(fy_v), wanted_fp))
                if q_end is not None:
                    return q_end
            fy_end = _infer_fy_end_for(target_end=target_end, qi=qi, fy_v=fy_v)
            if fy_end is None:
                return None
            return quarter_ends_for_fy(fy_end).get(wanted_fp)

        def _candidates(target_end: dt.date, target: str) -> pd.DataFrame:
            sub = facts[(facts["end_d"] == target_end) & facts["start_d"].notna()].copy()
            if sub.empty:
                return sub
            sub["dur"] = _duration_days(sub["end_d"], sub["start_d"])
            sub["dur_class"] = sub["dur"].apply(classify_duration)
            sub = sub[sub["dur_class"] == target]
            if sub.empty:
                return sub
            sub["form_rank"] = sub["form"].fillna("").apply(lambda x: _rank_form(x, prefer_forms))
            sub = sub.sort_values(["form_rank", "filed_d"], ascending=[True, False])
            return sub

        def _pick_pair(a: pd.DataFrame, b: pd.DataFrame) -> Optional[Tuple[pd.Series, pd.Series, float]]:
            if a.empty or b.empty:
                return None
            best = None
            best_score = None
            for _, ra in a.iterrows():
                for _, rb in b.iterrows():
                    fya = _fy_val(ra, fy_end_mmdd=fy_end_mmdd)
                    fyb = _fy_val(rb, fy_end_mmdd=fy_end_mmdd)
                    if fya is not None and fyb is not None and fya != fyb:
                        continue
                    if str(ra.get("unit")) != str(rb.get("unit")):
                        continue
                    try:
                        val = float(ra["val"]) - float(rb["val"])
                    except Exception:
                        continue
                    if not allow_negative and val < 0:
                        continue
                    # Prefer closer filed dates, then most recent filing
                    da = ra.get("filed_d")
                    db = rb.get("filed_d")
                    diff = abs((da - db).days) if pd.notna(da) and pd.notna(db) else 9999
                    if diff > max_filed_gap_days:
                        continue
                    rec = max(da, db) if pd.notna(da) and pd.notna(db) else da or db
                    score = (diff, -(rec.toordinal() if rec else 0))
                    if best_score is None or score < best_score:
                        best_score = score
                        best = (ra, rb, val)
            return best

        if quarter_index == 1:
            return None
        if quarter_index == 2:
            y6_c = _candidates(end, "6M")
            fy_v = _fy_val(y6_c.iloc[0], fy_end_mmdd=fy_end_mmdd) if not y6_c.empty else None
            q1_end = _quarter_end_from_map_or_fallback(
                target_end=end,
                qi=2,
                fy_v=fy_v,
                wanted_fp="Q1",
            )
            q1_c = _candidates(q1_end, "3M") if q1_end else pd.DataFrame()
            pair = _pick_pair(y6_c, q1_c)
            if pair is None:
                return None
            y6, q1_3m, val = pair
            return PickResult(
                value=float(val),
                source="derived_ytd",
                tag=str(y6["tag"]),
                accn=str(y6["accn"]),
                form=str(y6["form"]),
                filed=y6["filed_d"],
                start=y6["start_d"],
                end=y6["end_d"],
                unit=str(y6["unit"]),
                duration_days=int((y6["end_d"] - y6["start_d"]).days),
                note="derived Q2 = 6M YTD - Q1 3M",
            )
        if quarter_index == 3:
            y9_c = _candidates(end, "9M")
            fy_v = _fy_val(y9_c.iloc[0], fy_end_mmdd=fy_end_mmdd) if not y9_c.empty else None
            q2_end = _quarter_end_from_map_or_fallback(
                target_end=end,
                qi=3,
                fy_v=fy_v,
                wanted_fp="Q2",
            )
            y6_c = _candidates(q2_end, "6M") if q2_end else pd.DataFrame()
            pair = _pick_pair(y9_c, y6_c)
            if pair is None:
                return None
            y9, y6, val = pair
            return PickResult(
                value=float(val),
                source="derived_ytd",
                tag=str(y9["tag"]),
                accn=str(y9["accn"]),
                form=str(y9["form"]),
                filed=y9["filed_d"],
                start=y9["start_d"],
                end=y9["end_d"],
                unit=str(y9["unit"]),
                duration_days=int((y9["end_d"] - y9["start_d"]).days),
                note="derived Q3 = 9M YTD - 6M YTD",
            )
        if quarter_index == 4:
            fy_c = _candidates(end, "FY")
            fy_v = _fy_val(fy_c.iloc[0], fy_end_mmdd=fy_end_mmdd) if not fy_c.empty else None
            q3_end = _quarter_end_from_map_or_fallback(
                target_end=end,
                qi=4,
                fy_v=fy_v,
                wanted_fp="Q3",
            )
            y9_c = _candidates(q3_end, "9M") if q3_end else pd.DataFrame()
            pair = _pick_pair(fy_c, y9_c)
            if pair is None:
                return None
            fy, y9, val = pair
            q4_end_ts = pd.Timestamp(end)
            q4_start_ts = (q4_end_ts - pd.DateOffset(months=3)) + pd.Timedelta(days=1)
            return PickResult(
                value=float(val),
                source="derived_ytd_q4",
                tag=str(fy["tag"]),
                accn=str(fy["accn"]),
                form=str(fy["form"]),
                filed=fy["filed_d"],
                start=q4_start_ts.date(),
                end=q4_end_ts.date(),
                unit=str(fy["unit"]),
                duration_days=int((q4_end_ts.date() - q4_start_ts.date()).days),
                note="derived Q4 = FY - 9M YTD",
            )
        return None

    def _should_override(direct_val: float, derived_val: float) -> bool:
        if derived_val is None or pd.isna(derived_val):
            return False
        if direct_val is None or pd.isna(direct_val):
            return False
        if abs(derived_val) < 1e-9:
            return False
        ratio = direct_val / derived_val
        if ratio < 0:
            return True
        ar = abs(ratio)
        return ar < 0.35 or ar > 2.85

    direct = _direct_pick()
    derived = _derived_pick()

    if direct is not None and derived is not None and allow_override:
        if _should_override(float(direct.value), float(derived.value)):
            src = "derived_ytd_override" if derived.source == "derived_ytd" else "derived_ytd_q4_override"
            derived.source = src
            derived.note = f"{derived.note} (override direct 3M outlier)"
            return derived
        return direct

    if derived is not None:
        return derived
    if direct is not None:
        return direct
    return None


def build_quarter_calendar_from_revenue(
    df_all: pd.DataFrame, max_quarters: int
) -> Tuple[List[dt.date], Dict[Tuple[int, str], dt.date]]:
    """
    Use revenue tag as anchor:
    - choose best revenue tag by coverage
    - quarter ends = unique end dates from that tag
    - mapping (fy, fp)->end for derivations
    """
    rev_spec = next(s for s in GAAP_SPECS if s.name == "revenue")
    cand = df_all[df_all["tag"].isin(rev_spec.tags)].copy()
    cand = _filter_unit(cand, rev_spec)
    best_tag = choose_best_tag(cand, rev_spec)
    if not best_tag:
        raise RuntimeError("No suitable revenue tag found in companyfacts.")

    facts = cand[cand["tag"] == best_tag].copy()
    if facts.empty:
        raise RuntimeError("No revenue facts available.")

    ends = sorted([d for d in facts["end_d"].dropna().unique()])
    if max_quarters and len(ends) > max_quarters:
        ends = ends[-max_quarters:]

    fy_fp_to_end: Dict[Tuple[int, str], dt.date] = {}
    fy_col = _fy_col(facts)
    tmp = facts[facts[fy_col].notna() & facts["fp"].notna()].copy()
    if not tmp.empty:
        tmp = tmp.sort_values([fy_col, "fp", "filed_d"], ascending=[True, True, False])
        for _, r in tmp.iterrows():
            k = (int(r[fy_col]), str(r["fp"]))
            if k not in fy_fp_to_end and pd.notna(r["end_d"]):
                fy_fp_to_end[k] = r["end_d"]

    return ends, fy_fp_to_end


def self_check_period_logic(
    df_all: pd.DataFrame,
    audit: pd.DataFrame,
    metric_name: str = "revenue",
    max_filed_gap_days: int = 200,
    strictness: str = "ytd",
) -> pd.DataFrame:
    if audit is None or audit.empty:
        return pd.DataFrame()

    spec = next(s for s in GAAP_SPECS if s.name == metric_name)
    cand = df_all[df_all["tag"].isin(spec.tags)].copy()
    cand = _filter_unit(cand, spec)
    best_tag = choose_best_tag(cand, spec)
    if not best_tag:
        return pd.DataFrame()

    facts = cand[cand["tag"] == best_tag].copy()
    if facts.empty:
        return pd.DataFrame()
    fy_end_mmdd = _infer_fy_end_mmdd(facts)

    fy_fp_to_end: Dict[Tuple[int, str], dt.date] = {}
    fy_col = _fy_col(facts)
    tmp = facts[facts[fy_col].notna() & facts["fp"].notna()].copy()
    if not tmp.empty:
        tmp = tmp.sort_values([fy_col, "fp", "filed_d"], ascending=[True, True, False])
        for _, r in tmp.iterrows():
            k = (int(r[fy_col]), str(r["fp"]))
            if k not in fy_fp_to_end and pd.notna(r["end_d"]):
                fy_fp_to_end[k] = r["end_d"]

    def _fp_for_end(end: dt.date) -> Optional[str]:
        rows = facts[(facts["end_d"] == end) & facts["fp"].notna()]
        if rows.empty:
            return None
        return str(rows["fp"].dropna().iloc[0])

    def _quarter_index(end: dt.date) -> Optional[int]:
        fp = _fp_for_end(end)
        if fp in ("Q1", "Q2", "Q3"):
            return int(fp[-1])
        if fp == "FY":
            return 4
        return None

    rows: List[Dict[str, Any]] = []
    aud = audit[
        (audit["metric"] == metric_name)
        & (audit["source"].isin(["derived_ytd", "derived_ytd_q4", "derived_ytd_override", "derived_ytd_q4_override"]))
    ].copy()
    for _, r in aud.iterrows():
        end = r["quarter"]
        if pd.isna(end):
            continue
        end = pd.to_datetime(end).date()
        qi = _quarter_index(end) or 0
        src = str(r.get("source", ""))

        direct = pick_best_duration(facts, end=end, target="3M", prefer_forms=spec.prefer_forms)
        if direct is not None:
            direct_filed = direct.get("filed_d")
            derived_filed = r.get("filed")
            gap_ok = True
            if pd.notna(direct_filed) and pd.notna(derived_filed):
                try:
                    diff = abs((pd.to_datetime(direct_filed) - pd.to_datetime(derived_filed)).days)
                    gap_ok = diff <= max_filed_gap_days
                except Exception:
                    gap_ok = True
            if "override" in src:
                rows.append({
                    "quarter": end,
                    "metric": metric_name,
                    "check": "direct_3m_exists",
                    "status": "warn",
                    "message": "Direct 3M fact exists but was overridden due to mismatch.",
                })
            else:
                if strictness == "ytd":
                    rows.append({
                        "quarter": end,
                        "metric": metric_name,
                        "check": "direct_3m_exists",
                        "status": "warn",
                        "message": "Derived value used (strictness=ytd) despite available direct 3M fact.",
                    })
                else:
                    rows.append({
                        "quarter": end,
                        "metric": metric_name,
                        "check": "direct_3m_exists",
                        "status": "fail" if gap_ok else "warn",
                        "message": "Derived value used despite available direct 3M fact."
                        if gap_ok
                        else "Direct 3M exists but different vintage (filed gap).",
                    })
        else:
            rows.append({
                "quarter": end,
                "metric": metric_name,
                "check": "direct_3m_exists",
                "status": "pass",
                "message": "No direct 3M fact; derivation allowed.",
            })

        if qi == 2:
            y6 = pick_best_duration(facts, end=end, target="6M", prefer_forms=spec.prefer_forms)
            fy_v = _fy_val(y6, fy_end_mmdd=fy_end_mmdd) if y6 is not None else None
            q1_end = fy_fp_to_end.get((fy_v, "Q1")) if fy_v is not None else None
            if q1_end is None:
                fy_end = fy_fp_to_end.get((fy_v, "FY")) if fy_v is not None else None
                if fy_end is None:
                    fy_end = (pd.Timestamp(end) + pd.DateOffset(months=6) + pd.offsets.MonthEnd(0)).date()
                q1_end = quarter_ends_for_fy(fy_end).get("Q1")
            q1_3m = pick_best_duration(facts, end=q1_end, target="3M", prefer_forms=spec.prefer_forms) if q1_end else None
            ok = y6 is not None and q1_3m is not None
        elif qi == 3:
            y9 = pick_best_duration(facts, end=end, target="9M", prefer_forms=spec.prefer_forms)
            fy_v = _fy_val(y9, fy_end_mmdd=fy_end_mmdd) if y9 is not None else None
            q2_end = fy_fp_to_end.get((fy_v, "Q2")) if fy_v is not None else None
            if q2_end is None:
                fy_end = fy_fp_to_end.get((fy_v, "FY")) if fy_v is not None else None
                if fy_end is None:
                    fy_end = (pd.Timestamp(end) + pd.DateOffset(months=3) + pd.offsets.MonthEnd(0)).date()
                q2_end = quarter_ends_for_fy(fy_end).get("Q2")
            y6 = pick_best_duration(facts, end=q2_end, target="6M", prefer_forms=spec.prefer_forms) if q2_end else None
            ok = y9 is not None and y6 is not None
        elif qi == 4:
            fy = pick_best_duration(facts, end=end, target="FY", prefer_forms=spec.prefer_forms)
            fy_v = _fy_val(fy, fy_end_mmdd=fy_end_mmdd) if fy is not None else None
            q3_end = fy_fp_to_end.get((fy_v, "Q3")) if fy_v is not None else None
            if q3_end is None:
                fy_end = fy_fp_to_end.get((fy_v, "FY")) if fy_v is not None else end
                q3_end = quarter_ends_for_fy(fy_end).get("Q3")
            y9 = pick_best_duration(facts, end=q3_end, target="9M", prefer_forms=spec.prefer_forms) if q3_end else None
            ok = fy is not None and y9 is not None
        else:
            ok = True

        rows.append({
            "quarter": end,
            "metric": metric_name,
            "check": "ytd_components",
            "status": "pass" if ok else "fail",
            "message": "Required YTD components present." if ok else "Missing required YTD components.",
        })

    aud_direct = audit[(audit["metric"] == metric_name) & (audit["source"] == "direct")].copy()
    if not aud_direct.empty and "duration_days" in aud_direct.columns:
        for _, r in aud_direct.iterrows():
            dd = r.get("duration_days")
            if dd is None or pd.isna(dd):
                continue
            if classify_duration(dd) != "3M":
                rows.append({
                    "quarter": r.get("quarter"),
                    "metric": metric_name,
                    "check": "direct_duration",
                    "status": "fail",
                    "message": f"Direct fact duration is not 3M (duration_days={dd}).",
                })

    return pd.DataFrame(rows)
