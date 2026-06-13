"""SUMMARY dataframe builder for workbook writer."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable, Dict, Mapping, Optional, List

import pandas as pd


@dataclass(frozen=True)
class SummaryBuilderDeps:
    hist: Any
    leverage_df: Any
    needs_review: Any
    company_overview: Mapping[str, Any] | None
    price: Any
    ctx_ref: Any
    hist_view: Callable[[], Any]
    audit_view: Callable[..., Any]


def build_summary_dataframe(deps: SummaryBuilderDeps) -> pd.DataFrame:
    hist = deps.hist
    leverage_df = deps.leverage_df
    needs_review = deps.needs_review
    company_overview = deps.company_overview
    price = deps.price
    ctx_ref = deps.ctx_ref
    _hist_view = deps.hist_view
    _audit_view = deps.audit_view
    rows: List[Dict[str, Any]] = []
    if hist is None or hist.empty:
        return pd.DataFrame()
    h = _hist_view().copy()
    if "_quarter" in h.columns:
        h["quarter"] = h["_quarter"]
    h = h[h["quarter"].notna()].sort_values("quarter")
    if h.empty:
        return pd.DataFrame()

    latest_q = pd.Timestamp(h["quarter"].max())
    latest_qd = latest_q.date()
    q_list = sorted(pd.Timestamp(q) for q in h["quarter"].unique().tolist())
    latest_pos = q_list.index(latest_q) if latest_q in q_list else len(q_list) - 1
    ly_q = q_list[latest_pos - 4] if latest_pos >= 4 else None

    aud = _audit_view(quarter_mode="date")
    if not aud.empty:
        if "filed" in aud.columns:
            aud["filed_d"] = pd.to_datetime(aud["filed"], errors="coerce")

    source_rank_map = {
        "direct": 0,
        "derived_parts": 1,
        "tier2_table": 1,
        "derived_ytd": 2,
        "derived_ytd_q4": 2,
        "derived_ytd_q4_table": 2,
        "derived_ytd_tax_paid": 2,
        "derived_formula": 3,
        "text": 4,
        "table": 4,
        "missing": 9,
    }

    def _source_rank(src: Any) -> int:
        s = str(src or "").strip().lower()
        if s in source_rank_map:
            return source_rank_map[s]
        if "text" in s:
            return 4
        if "table" in s:
            return 4
        if "missing" in s:
            return 9
        return 5

    def pick_best_audit_row(metric: str, quarter_date: Optional[date]) -> Optional[Dict[str, Any]]:
        if not metric or quarter_date is None or aud.empty or "metric" not in aud.columns:
            return None
        sub = aud.copy()
        sub["metric"] = sub["metric"].astype(str)
        sub = sub[sub["metric"] == metric].copy()
        if sub.empty or "quarter" not in sub.columns:
            return None
        sub = sub[sub["quarter"] == quarter_date].copy()
        if sub.empty:
            return None
        sub["src_rank"] = sub.get("source", "").apply(_source_rank)
        if "filed_d" not in sub.columns:
            sub["filed_d"] = pd.NaT
        sub["filed_ord"] = pd.to_datetime(sub["filed_d"], errors="coerce")
        sub = sub.sort_values(["src_rank", "filed_ord"], ascending=[True, False])
        return sub.iloc[0].to_dict()

    def format_source(row: Optional[Dict[str, Any]]) -> str:
        if not row:
            return "Source: N/A (metric not found in SEC_Audit_Log)"
        form = str(row.get("form") or "n/a")
        accn = str(row.get("accn") or "n/a")
        tag = str(row.get("tag") or "n/a")
        src = str(row.get("source") or "n/a")
        filed = pd.to_datetime(row.get("filed"), errors="coerce")
        filed_txt = filed.strftime("%Y-%m-%d") if pd.notna(filed) else "n/a"
        return f"Source: SEC {form} accn={accn} filed={filed_txt} tag={tag} ({src})"

    def _val_at(q: Optional[pd.Timestamp], col: str) -> Optional[float]:
        if q is None or col not in h.columns:
            return None
        sub = h[h["quarter"] == q]
        if sub.empty:
            return None
        v = pd.to_numeric(sub.iloc[-1].get(col), errors="coerce")
        return float(v) if pd.notna(v) else None

    def _ttm(col: str) -> Optional[float]:
        if col not in h.columns or latest_pos < 3:
            return None
        w = h.iloc[latest_pos - 3 : latest_pos + 1]
        s = pd.to_numeric(w[col], errors="coerce")
        if s.isna().any():
            return None
        return float(s.sum())

    def _scale_m(val: Optional[float]) -> Optional[float]:
        if val is None or pd.isna(val):
            return None
        return float(val) / 1e6

    def _q_yoy(
        col: str,
        *,
        positive_prev_only: bool = False,
        positive_cur_only: bool = False,
    ) -> Optional[float]:
        cur = _val_at(latest_q, col)
        prev = _val_at(ly_q, col) if ly_q is not None else None
        if cur is None or prev in (None, 0):
            return None
        if positive_prev_only and float(prev) <= 0:
            return None
        if positive_cur_only and float(cur) <= 0:
            return None
        return (float(cur) - float(prev)) / abs(float(prev))

    def _row(
        section: str,
        metric: str,
        value: Any,
        note: str,
    ) -> None:
        rows.append({"Section": section, "Metric": metric, "Value": value, "Note": note})

    overview = company_overview or {}
    streams = overview.get("revenue_streams") if isinstance(overview.get("revenue_streams"), list) else []
    segment_models = overview.get("segment_operating_model") if isinstance(overview.get("segment_operating_model"), list) else []
    key_dependencies = overview.get("key_dependencies") if isinstance(overview.get("key_dependencies"), list) else []
    wrong_thesis_bullets = overview.get("wrong_thesis_bullets") if isinstance(overview.get("wrong_thesis_bullets"), list) else []
    what_it_does = str(overview.get("what_it_does") or "N/A")
    current_context = str(overview.get("current_strategic_context") or "N/A")
    key_adv = str(overview.get("key_advantage") or "N/A")
    what_src = str(overview.get("what_it_does_source") or "Source: N/A")
    current_src = str(overview.get("current_strategic_context_source") or "Source: N/A")
    key_src = str(overview.get("key_advantage_source") or "Source: N/A")
    stream_src = str(overview.get("revenue_streams_source") or "Source: N/A")
    segment_src = str(overview.get("segment_operating_model_source") or what_src)
    deps_src = str(overview.get("key_dependencies_source") or "Source: N/A")
    wrong_src = str(overview.get("wrong_thesis_source") or deps_src)
    if ctx_ref is not None:
        ctx_ref.derived.summary_export_expectation = {
            "rows": {
                "What the company does": {
                    "value": (what_it_does if what_it_does else "N/A"),
                    "source": what_src,
                },
                "Current strategic context": {
                    "value": (current_context if current_context else "N/A"),
                    "source": current_src,
                },
                "Key competitive advantage": {
                    "value": (key_adv if key_adv else "N/A"),
                    "source": key_src,
                },
            }
        }
    fy_end = overview.get("asof_fy_end")
    fy_txt = ""
    if fy_end is not None:
        fy_dt = pd.to_datetime(fy_end, errors="coerce")
        if pd.notna(fy_dt):
            fy_txt = fy_dt.strftime("%Y-%m-%d")

    _row("Company Overview", "What the company does", what_it_does if what_it_does else "N/A", what_src)
    _row(
        "Company Overview",
        "Current strategic context",
        current_context if current_context else "N/A",
        current_src,
    )
    _row("Company Overview", "Key competitive advantage", key_adv if key_adv else "N/A", key_src)
    streams_metric = "Business model / revenue streams (% of total revenue)"
    streams_period = overview.get("revenue_streams_period")
    sp = pd.to_datetime(streams_period, errors="coerce")
    if pd.notna(sp):
        if isinstance(fy_end, (date, datetime)) and pd.to_datetime(fy_end, errors="coerce").date() == sp.date():
            streams_metric = f"{streams_metric} (FY end {sp.strftime('%Y-%m-%d')})"
        else:
            streams_metric = f"{streams_metric} (Quarter end {sp.strftime('%Y-%m-%d')})"
    elif fy_txt:
        streams_metric = f"{streams_metric} (FY end {fy_txt})"
    if streams:
        def _display_stream_name(name: str) -> str:
            n = str(name or "").strip()
            l = n.lower()
            if "send" in l and "tech" in l:
                return "SendTech Solutions"
            if "presort" in l:
                return "Presort Services"
            return n
        def _norm_name_tokens(name: str) -> str:
            s = re.sub(r"[^a-z0-9]+", " ", str(name or "").lower()).strip()
            s = re.sub(r"\b(solutions?|services?|segment|business|global)\b", " ", s)
            s = re.sub(r"\s+", " ", s).strip()
            return s.replace(" ", "")

        stream_lines: List[str] = []
        stream_name_keys: List[str] = []
        for s in streams:
            nm = _display_stream_name(str(s.get("name") or "").strip())
            pct = pd.to_numeric(s.get("pct"), errors="coerce")
            if not nm or pd.isna(pct):
                continue
            pct_txt = f"{float(pct) * 100.0:.1f}".replace(".", ",")
            stream_lines.append(f"{nm}: {pct_txt}%")
            nk = _norm_name_tokens(nm)
            if nk:
                stream_name_keys.append(nk)
        seg_name_keys: List[str] = []
        for sm in segment_models[:6]:
            sk = _norm_name_tokens(str(sm.get("segment") or "").strip())
            if sk:
                seg_name_keys.append(sk)
        stream_overlap = 0
        if stream_name_keys and seg_name_keys:
            for sk in stream_name_keys:
                if any((sk in sg) or (sg in sk) for sg in seg_name_keys):
                    stream_overlap += 1
        stream_text = "\n".join(stream_lines) if stream_lines else "N/A"
        _row("Company Overview", streams_metric, stream_text, stream_src)
    else:
        _row("Company Overview", streams_metric, "N/A", stream_src)

    if segment_models:
        seg_lines: List[str] = []
        for sm in segment_models[:5]:
            seg = str(sm.get("segment") or "").strip()
            txt = str(sm.get("text") or "").strip()
            if not txt:
                continue
            if seg:
                seg_lines.append(f"{seg}: {txt}")
            else:
                seg_lines.append(txt)
        _row(
            "Company Overview",
            "Operating model per segment",
            "\n".join(seg_lines) if seg_lines else "N/A",
            segment_src,
        )

    if key_dependencies:
        dep_lines = []
        for dep in key_dependencies[:5]:
            d_txt = str(dep or "").strip()
            if d_txt:
                dep_lines.append(d_txt)
        if dep_lines:
            for i_dep, dep_txt in enumerate(dep_lines):
                _row(
                    "Company Overview",
                    "Key dependencies (3-5)" if i_dep == 0 else "",
                    f"- {dep_txt}",
                    deps_src,
                )
        else:
            _row(
                "Company Overview",
                "Key dependencies (3-5)",
                "N/A",
                deps_src,
            )
    else:
        _row(
            "Company Overview",
            "Key dependencies (3-5)",
            "N/A",
            deps_src or "Source: N/A (dependencies not extracted from Item 1A)",
        )

    if wrong_thesis_bullets:
        wt_lines = []
        for dep in wrong_thesis_bullets[:5]:
            d_txt = str(dep or "").strip()
            if d_txt:
                wt_lines.append(d_txt)
        if wt_lines:
            for i_w, wrong_txt in enumerate(wt_lines):
                _row(
                    "Company Overview",
                    "What would make me wrong" if i_w == 0 else "",
                    f"- {wrong_txt}",
                    wrong_src,
                )
        else:
            _row(
                "Company Overview",
                "What would make me wrong",
                "N/A",
                wrong_src,
            )
    else:
        _row(
            "Company Overview",
            "What would make me wrong",
            "N/A",
            wrong_src or "Source: N/A (wrong-thesis bullets not extracted from Item 1A)",
        )

    revenue_ttm = _ttm("revenue")
    revenue_latest = _val_at(latest_q, "revenue")
    revenue_yoy = _q_yoy("revenue")
    net_income_latest = _val_at(latest_q, "net_income")
    net_income_yoy = _q_yoy("net_income")
    shares_dil_latest = _val_at(latest_q, "shares_diluted")
    shares_out_latest = _val_at(latest_q, "shares_outstanding")
    eps_latest = None
    eps_note_suffix = "derived = net_income / shares_diluted."
    if net_income_latest is not None and shares_dil_latest not in (None, 0):
        eps_latest = float(net_income_latest) / float(shares_dil_latest)
    elif net_income_latest is not None and shares_out_latest not in (None, 0):
        eps_latest = float(net_income_latest) / float(shares_out_latest)
        eps_note_suffix = "derived = net_income / shares_outstanding (shares_diluted missing)."

    debt_latest = _val_at(latest_q, "total_debt")
    debt_metric = "total_debt"
    if debt_latest is None:
        debt_latest = _val_at(latest_q, "debt_core")
        debt_metric = "debt_core"
    equity_latest = _val_at(latest_q, "total_equity")
    debt_to_equity_value: Any = None
    debt_to_equity_unit = "x"
    if debt_latest is not None and equity_latest is not None:
        try:
            equity_latest_num = float(equity_latest)
            if equity_latest_num > 0:
                debt_to_equity_value = float(debt_latest) / equity_latest_num
            elif equity_latest_num < 0:
                debt_to_equity_value = "N/M (neg equity)"
                debt_to_equity_unit = "neg equity"
            else:
                debt_to_equity_value = "N/M (zero equity)"
                debt_to_equity_unit = "zero equity"
        except Exception:
            debt_to_equity_value = None

    cfo_ttm = _ttm("cfo")
    capex_ttm = _ttm("capex")
    fcf_ttm = (cfo_ttm - capex_ttm) if (cfo_ttm is not None and capex_ttm is not None) else None
    fcf_latest = None
    fcf_ly = None
    cfo_latest = _val_at(latest_q, "cfo")
    capex_latest = _val_at(latest_q, "capex")
    if cfo_latest is not None and capex_latest is not None:
        fcf_latest = cfo_latest - capex_latest
    if ly_q is not None:
        cfo_ly = _val_at(ly_q, "cfo")
        capex_ly = _val_at(ly_q, "capex")
        if cfo_ly is not None and capex_ly is not None:
            fcf_ly = cfo_ly - capex_ly
    fcf_yoy = None
    if fcf_latest is not None and fcf_ly not in (None, 0):
        fcf_yoy = (float(fcf_latest) - float(fcf_ly)) / abs(float(fcf_ly))

    shares_for_price = shares_out_latest if shares_out_latest not in (None, 0) else shares_dil_latest
    market_cap = (float(price) * float(shares_for_price)) if (price is not None and shares_for_price not in (None, 0)) else None
    net_income_ttm = _ttm("net_income")
    pe_ratio = (market_cap / net_income_ttm) if (market_cap is not None and net_income_ttm not in (None, 0)) else None
    ps_ratio = (market_cap / revenue_ttm) if (market_cap is not None and revenue_ttm not in (None, 0)) else None

    rev_anchor = pick_best_audit_row("revenue", latest_qd)
    net_anchor = pick_best_audit_row("net_income", latest_qd)
    sh_anchor = pick_best_audit_row("shares_diluted", latest_qd) or pick_best_audit_row("shares_outstanding", latest_qd)
    debt_anchor = pick_best_audit_row(debt_metric, latest_qd)
    eq_anchor = pick_best_audit_row("total_equity", latest_qd)
    cfo_anchor = pick_best_audit_row("cfo", latest_qd)
    capex_anchor = pick_best_audit_row("capex", latest_qd)
    rev_ly_anchor = pick_best_audit_row("revenue", ly_q.date()) if ly_q is not None else None
    net_ly_anchor = pick_best_audit_row("net_income", ly_q.date()) if ly_q is not None else None
    cfo_ly_anchor = pick_best_audit_row("cfo", ly_q.date()) if ly_q is not None else None
    capex_ly_anchor = pick_best_audit_row("capex", ly_q.date()) if ly_q is not None else None

    _row("Key Financials", "As of quarter", latest_qd, "date")
    _row(
        "Key Financials",
        "Revenue (TTM)",
        _scale_m(revenue_ttm) if revenue_ttm is not None else "N/A",
        "$m" if revenue_ttm is not None else "N/A (fewer than 4 complete revenue quarters)",
    )
    _row(
        "Key Financials",
        "Revenue (latest quarter)",
        _scale_m(revenue_latest) if revenue_latest is not None else "N/A",
        "$m",
    )
    _row(
        "Key Financials",
        "Revenue YoY (latest vs LY quarter)",
        revenue_yoy if revenue_yoy is not None else "N/A",
        "%" if revenue_yoy is not None else "N/A (missing latest/LY revenue)",
    )
    _row(
        "Key Financials",
        "Net income (latest quarter)",
        _scale_m(net_income_latest) if net_income_latest is not None else "N/A",
        "$m",
    )
    _row(
        "Key Financials",
        "Net income YoY (latest vs LY quarter)",
        net_income_yoy if net_income_yoy is not None else "N/A",
        "%" if net_income_yoy is not None else "N/A (missing latest/LY net income)",
    )
    _row(
        "Key Financials",
        "EPS (latest quarter, GAAP diluted)",
        eps_latest if eps_latest is not None else "N/A",
        "$/share" if eps_latest is not None else "N/A (missing net income or shares)",
    )
    eps_ly = None
    if ly_q is not None:
        net_income_ly = _val_at(ly_q, "net_income")
        shares_dil_ly = _val_at(ly_q, "shares_diluted")
        shares_out_ly = _val_at(ly_q, "shares_outstanding")
        if net_income_ly is not None and shares_dil_ly not in (None, 0):
            eps_ly = float(net_income_ly) / float(shares_dil_ly)
        elif net_income_ly is not None and shares_out_ly not in (None, 0):
            eps_ly = float(net_income_ly) / float(shares_out_ly)
    eps_yoy_delta = (float(eps_latest) - float(eps_ly)) if (eps_latest is not None and eps_ly is not None) else None
    _row(
        "Key Financials",
        "EPS YoY Δ (latest vs LY quarter)",
        eps_yoy_delta if eps_yoy_delta is not None else "N/A",
        "$/share" if eps_yoy_delta is not None else "N/A (missing latest/LY EPS base)",
    )
    _row(
        "Key Financials",
        "Total debt (latest quarter)",
        _scale_m(debt_latest) if debt_latest is not None else "N/A",
        "$m",
    )
    _row(
        "Key Financials",
        "Debt-to-equity (latest quarter)",
        debt_to_equity_value if debt_to_equity_value is not None else "N/A",
        debt_to_equity_unit if debt_to_equity_value is not None else "N/A (missing debt/equity)",
    )
    _row(
        "Key Financials",
        "Free cash flow (TTM)",
        _scale_m(fcf_ttm) if fcf_ttm is not None else "N/A",
        "$m" if fcf_ttm is not None else "N/A (missing CFO/Capex in last4 quarters)",
    )
    _row(
        "Key Financials",
        "FCF YoY (latest quarter vs LY quarter)",
        fcf_yoy if fcf_yoy is not None else "N/A",
        "%" if fcf_yoy is not None else "N/A (missing latest/LY FCF base)",
    )
    _row(
        "Key Financials",
        "P/E (price-linked)",
        pe_ratio if pe_ratio is not None else "N/A",
        "x (price-linked)" if pe_ratio is not None else "N/A (price or denominator unavailable)",
    )
    _row(
        "Key Financials",
        "P/S (price-linked)",
        ps_ratio if ps_ratio is not None else "N/A",
        "x (price-linked)" if ps_ratio is not None else "N/A (price or denominator unavailable)",
    )

    if leverage_df is not None and not leverage_df.empty:
        lv = leverage_df.copy()
        lv["quarter"] = pd.to_datetime(lv["quarter"], errors="coerce")
        lv = lv[lv["quarter"] == latest_q]
        if not lv.empty:
            row = lv.iloc[-1]
            _row(
                "Leverage / Liquidity",
                "Net leverage",
                row.get("corporate_net_leverage"),
                "Units: x.",
            )
            _row(
                "Leverage / Liquidity",
                "Interest coverage (P&L)",
                row.get("interest_coverage_pnl"),
                "Units: x.",
            )
            _row(
                "Leverage / Liquidity",
                "Interest coverage (cash)",
                row.get("interest_coverage_cash"),
                "Units: x.",
            )
            _row(
                "Leverage / Liquidity",
                "Revolver availability",
                _scale_m(pd.to_numeric(row.get("revolver_availability"), errors="coerce")),
                "Units: $m.",
            )
            _row(
                "Leverage / Liquidity",
                "Liquidity (cash + revolver)",
                _scale_m(pd.to_numeric(row.get("liquidity"), errors="coerce")),
                "Units: $m.",
            )

    warn_ct = 0
    fail_ct = 0
    if needs_review is not None and not needs_review.empty and "quarter" in needs_review.columns:
        nr = needs_review.copy()
        nr["quarter"] = pd.to_datetime(nr["quarter"], errors="coerce")
        recent = sorted(h["quarter"].unique())[-8:]
        nr = nr[nr["quarter"].isin(recent)]
        if "severity" in nr.columns:
            warn_ct = int((nr["severity"].astype(str).str.lower() == "warn").sum())
            fail_ct = int((nr["severity"].astype(str).str.lower() == "fail").sum())
    _row("QA", "WARN (last 8Q)", warn_ct, "Count from Needs_Review severity=warn.")
    _row("QA", "FAIL (last 8Q)", fail_ct, "Count from Needs_Review severity=fail.")

    spaced: List[Dict[str, Any]] = []
    last_section = None
    last_nonempty_metric = ""
    for row in rows:
        section = row.get("Section")
        metric = str(row.get("Metric") or "").strip()
        if last_section and section and section != last_section:
            spaced.append({"Section": "", "Metric": "", "Value": None, "Note": ""})
            if str(last_section).strip().lower() == "company overview" and str(section).strip().lower() == "key financials":
                spaced.append({"Section": "", "Metric": "", "Value": None, "Note": ""})
        if str(section or "").strip().lower() == "company overview" and metric:
            metric_l = metric.lower()
            prev_l = str(last_nonempty_metric or "").lower()
            if (
                metric_l.startswith("business model / revenue streams")
                and prev_l.startswith("key competitive advantage")
            ):
                spaced.append({"Section": "", "Metric": "", "Value": None, "Note": ""})
            if (
                metric_l.startswith("operating model per segment")
                and prev_l.startswith("key competitive advantage")
            ):
                spaced.append({"Section": "", "Metric": "", "Value": None, "Note": ""})
            if (
                metric_l.startswith("key dependencies")
                and (
                    prev_l.startswith("operating model per segment")
                    or prev_l.startswith("business model / revenue streams")
                )
            ):
                spaced.append({"Section": "", "Metric": "", "Value": None, "Note": ""})
            if (
                metric_l.startswith("what would make me wrong")
                and prev_l.startswith("key dependencies")
            ):
                spaced.append({"Section": "", "Metric": "", "Value": None, "Note": ""})
        spaced.append(row)
        if section:
            last_section = section
        if metric:
            last_nonempty_metric = metric
    return pd.DataFrame(spaced)
