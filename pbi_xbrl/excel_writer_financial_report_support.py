"""Financial report/data sheet support for the workbook writer."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, MutableMapping, Tuple

import pandas as pd


@dataclass(frozen=True)
class FinancialReportSupportDeps:
    runtime: MutableMapping[str, Any]


class FinancialReportSupport:
    def __init__(self, deps: FinancialReportSupportDeps) -> None:
        self.deps = deps
        self.runtime = deps.runtime

    def _rt(self, name: str) -> Any:
        return self.runtime[name]

    def _pd(self) -> Any:
        return self.runtime.get("pd", pd)

    def period_type(self, row: Any) -> str:
        pd_mod = self._pd()
        dd = row.get("duration_days")
        if dd is None or pd_mod.isna(dd):
            return "Instant"
        return self._rt("classify_duration")(dd)

    def build_facts_long(self) -> Any:
        pd_mod = self._pd()
        audit = self._rt("audit")
        needs_review = self._rt("needs_review")
        if audit is None or audit.empty:
            return pd_mod.DataFrame()
        base = audit.copy()
        base["period_end"] = base["quarter"]
        base["period_start"] = base["start"]
        base["period_type"] = base.apply(self.period_type, axis=1)
        base["source_class"] = base["source"].apply(self._rt("_source_class"))
        base["method"] = base["source"].apply(self._rt("_source_method"))
        base["qa_severity"] = base["source"].apply(self._rt("_source_qa"))
        base["source_tier"] = base["source"].apply(self._rt("_source_tier"))
        base["source_type"] = base["source"]
        base["filed_date"] = base["filed"]
        base["doc_ref"] = base["note"]
        base["qa_flag"] = "pass"
        if needs_review is not None and not needs_review.empty:
            nr = needs_review.copy()
            nr["quarter"] = pd_mod.to_datetime(nr["quarter"], errors="coerce")
            sev_map: Dict[Tuple[str, pd.Timestamp], str] = {}
            for _, r in nr.iterrows():
                key = (r.get("metric"), r.get("quarter"))
                sev = r.get("severity")
                if key not in sev_map or sev == "fail":
                    sev_map[key] = sev
            base["qa_flag"] = base.apply(
                lambda r: sev_map.get((r.get("metric"), pd_mod.to_datetime(r.get("quarter"), errors="coerce")), "pass"),
                axis=1,
            )
        facts = base[
            [
                "metric",
                "period_end",
                "period_type",
                "value",
                "unit",
                "source_class",
                "method",
                "qa_severity",
                "source_tier",
                "source_type",
                "tag",
                "accn",
                "filed_date",
                "doc_ref",
                "qa_flag",
                "period_start",
                "form",
            ]
        ].copy()
        return facts

    def build_lineitem_map(self) -> Any:
        pd_mod = self._pd()
        company_profile = self._rt("company_profile")
        bank_metrics_enabled = self._rt("bank_metrics_enabled")
        total_debt_label = "Total debt (legacy)" if str(getattr(company_profile, "ticker", "")).upper() == "PBI" else "Total debt"
        rows = [
            # IS
            {"metric": "revenue", "statement": "IS", "display_label": "Revenue", "section": "Revenue", "sort": 10, "sign": 1, "preferred_period": "3M", "notes": ""},
            {"metric": "cogs", "statement": "IS", "display_label": "Cost of revenue", "section": "Cost", "sort": 20, "sign": 1, "preferred_period": "3M", "notes": ""},
            {"metric": "gross_profit", "statement": "IS", "display_label": "Gross profit", "section": "Profitability", "sort": 30, "sign": 1, "preferred_period": "3M", "notes": "Revenue - COGS"},
            {"metric": "op_income", "statement": "IS", "display_label": "Operating income", "section": "Profitability", "sort": 40, "sign": 1, "preferred_period": "3M", "notes": ""},
            {"metric": "ebitda", "statement": "IS", "display_label": "EBITDA", "section": "Profitability", "sort": 50, "sign": 1, "preferred_period": "3M", "notes": ""},
            {"metric": "net_income", "statement": "IS", "display_label": "Net income", "section": "Profitability", "sort": 60, "sign": 1, "preferred_period": "3M", "notes": ""},
            {"metric": "shares_diluted", "statement": "IS", "display_label": "Diluted shares", "section": "Per share", "sort": 70, "sign": 1, "preferred_period": "Instant", "notes": ""},
            # BS
            {"metric": "cash", "statement": "BS", "display_label": "Cash & equivalents", "section": "Assets", "sort": 10, "sign": 1, "preferred_period": "Instant", "notes": ""},
            {"metric": "total_debt", "statement": "BS", "display_label": total_debt_label, "section": "Liabilities", "sort": 20, "sign": 1, "preferred_period": "Instant", "notes": "Legacy total_debt metric"},
            {"metric": "debt_core", "statement": "BS", "display_label": "Debt core", "section": "Liabilities", "sort": 25, "sign": 1, "preferred_period": "Instant", "notes": "Corporate borrowings (excludes bank deposits)"},
            {"metric": "lease_liabilities", "statement": "BS", "display_label": "Lease liabilities", "section": "Liabilities", "sort": 30, "sign": 1, "preferred_period": "Instant", "notes": "Operating + finance leases"},
            {"metric": "shares_diluted", "statement": "BS", "display_label": "Diluted shares", "section": "Equity", "sort": 50, "sign": 1, "preferred_period": "Instant", "notes": ""},
            # CF
            {"metric": "cfo", "statement": "CF", "display_label": "Cash from operations", "section": "Operating", "sort": 10, "sign": 1, "preferred_period": "3M", "notes": ""},
            {"metric": "capex", "statement": "CF", "display_label": "Capex", "section": "Investing", "sort": 20, "sign": -1, "preferred_period": "3M", "notes": ""},
            {"metric": "interest_paid", "statement": "CF", "display_label": "Cash interest", "section": "Financing", "sort": 30, "sign": -1, "preferred_period": "3M", "notes": ""},
            {"metric": "tax_paid", "statement": "CF", "display_label": "Cash taxes", "section": "Operating", "sort": 40, "sign": -1, "preferred_period": "3M", "notes": ""},
            {"metric": "da", "statement": "CF", "display_label": "D&A", "section": "Operating", "sort": 50, "sign": 1, "preferred_period": "3M", "notes": ""},
            {"metric": "fcf", "statement": "CF", "display_label": "Free cash flow", "section": "Summary", "sort": 60, "sign": 1, "preferred_period": "3M", "notes": "CFO - Capex"},
        ]
        if bank_metrics_enabled:
            rows.extend(
                [
                    {"metric": "bank_deposits", "statement": "BS", "display_label": "Bank deposits", "section": "Liabilities", "sort": 35, "sign": 1, "preferred_period": "Instant", "notes": "Customer deposits at bank entity"},
                    {"metric": "bank_finance_receivables", "statement": "BS", "display_label": "Bank finance receivables", "section": "Assets", "sort": 15, "sign": 1, "preferred_period": "Instant", "notes": "Short + long-term finance receivables"},
                    {"metric": "bank_net_funding", "statement": "BS", "display_label": "Bank net funding", "section": "Liabilities", "sort": 40, "sign": 1, "preferred_period": "Instant", "notes": "Deposits - finance receivables"},
                ]
            )
        return pd_mod.DataFrame(rows)

    def build_period_index(self, max_periods: int = 12) -> Any:
        pd_mod = self._pd()
        hist = self._rt("hist")
        strictness = self._rt("strictness")
        if hist is None or hist.empty or "quarter" not in hist.columns:
            return pd_mod.DataFrame()
        qs = sorted(pd_mod.to_datetime(hist["quarter"], errors="coerce").dropna().unique())
        if len(qs) > max_periods:
            qs = qs[-max_periods:]
        rows = []
        for i, q in enumerate(qs, 1):
            rows.append(
                {
                    "period_end": q,
                    "display_order": i,
                    "vintage_policy": f"strictness={strictness}",
                }
            )
        return pd_mod.DataFrame(rows)

    def build_report(self, statement: str, scale: float = 1e6) -> Any:
        pd_mod = self._pd()
        hist = self._rt("hist")
        audit = self._rt("audit")
        _audit_view = self._rt("_audit_view")
        _hist_view = self._rt("_hist_view")
        _source_label = self._rt("_source_label")
        _source_qa = self._rt("_source_qa")
        if hist is None or hist.empty:
            return pd_mod.DataFrame()
        lim = self.build_lineitem_map()
        lim = lim[lim["statement"] == statement].sort_values(["sort", "metric"]).copy()
        periods = self.build_period_index(12)
        if periods.empty:
            return pd_mod.DataFrame()
        period_cols = [pd_mod.to_datetime(p) for p in periods["period_end"]]
        # metadata from audit (latest quarter within window)
        meta: Dict[str, Dict[str, str]] = {}
        if audit is not None and not audit.empty:
            aud = _audit_view().copy()
            if "_quarter" in aud.columns:
                aud["quarter"] = aud["_quarter"]

            def _report_source_label_from_audit(row: Any) -> str:
                source_choice = str(row.get("source_choice") or "").strip().lower()
                if source_choice == "fallback_fy":
                    return "FY fallback"
                if source_choice == "carry_forward_q3":
                    return "Carry-forward"
                return _source_label(row.get("source"))

            for metric in lim["metric"].unique():
                sub = aud[aud["metric"] == metric].copy()
                sub = sub[sub["quarter"].isin(period_cols)].copy()
                if sub.empty:
                    meta[metric] = {"source": "Missing", "tag": "", "accn": "", "qa": "FAIL"}
                    continue
                sub["source_rank"] = sub["source"].map(lambda s: 0 if str(s or "").lower() != "missing" else 1)
                latest = sub.sort_values(["quarter", "source_rank"], ascending=[False, True]).iloc[0]
                meta[metric] = {
                    "source": _report_source_label_from_audit(latest),
                    "tag": latest.get("tag", ""),
                    "accn": latest.get("accn", ""),
                    "qa": latest.get("qa_severity") or _source_qa(latest.get("source")),
                }
        rows = []
        h = _hist_view().copy()
        if "_quarter" in h.columns:
            h["quarter"] = h["_quarter"]
        ttm_vals: Dict[str, float] = {}
        if statement == "IS":
            for m in ["revenue", "ebitda", "cfo", "capex"]:
                if m in h.columns:
                    last4 = h.sort_values("quarter").tail(4)
                    ttm_vals[m] = pd_mod.to_numeric(last4[m], errors="coerce").sum()
            if "cfo" in ttm_vals and "capex" in ttm_vals:
                ttm_vals["fcf"] = ttm_vals["cfo"] - ttm_vals["capex"]
        for _, r in lim.iterrows():
            metric = r["metric"]
            row = {
                "Section": r["section"],
                "Line Item": r["display_label"],
                "Source": meta.get(metric, {}).get("source", ""),
                "QA": meta.get(metric, {}).get("qa", ""),
                "Tag": meta.get(metric, {}).get("tag", ""),
                "Accn": meta.get(metric, {}).get("accn", ""),
            }
            for p in period_cols:
                if metric == "fcf":
                    val = None
                    if "cfo" in h.columns and "capex" in h.columns:
                        sub = h[h["quarter"] == p]
                        if not sub.empty:
                            cfo = pd_mod.to_numeric(sub["cfo"], errors="coerce").iloc[0]
                            capex = pd_mod.to_numeric(sub["capex"], errors="coerce").iloc[0]
                            if pd_mod.notna(cfo) and pd_mod.notna(capex):
                                val = cfo - capex
                else:
                    val = None
                    if metric in h.columns:
                        sub = h[h["quarter"] == p]
                        if not sub.empty:
                            val = pd_mod.to_numeric(sub[metric], errors="coerce").iloc[0]
                row[p.date()] = (val / scale) if pd_mod.notna(val) else None
            if metric == "fcf":
                has_fcf = any(pd_mod.notna(row.get(p.date())) for p in period_cols)
                if has_fcf:
                    row["Source"] = "Derived"
                    row["QA"] = "PASS"
                    row["Tag"] = ""
                    row["Accn"] = ""
            if statement == "IS":
                ttm = ttm_vals.get(metric)
                row["TTM"] = (ttm / scale) if ttm is not None else None
            rows.append(row)
        if statement == "IS":
            rev_ttm = ttm_vals.get("revenue")
            ebitda_ttm = ttm_vals.get("ebitda")
            fcf_ttm = ttm_vals.get("fcf")
            # TTM rows
            for label, val in [
                ("Revenue TTM", rev_ttm),
                ("EBITDA TTM", ebitda_ttm),
                ("FCF TTM", fcf_ttm),
            ]:
                rows.append(
                    {
                        "Section": "TTM",
                        "Line Item": label,
                        "Source": "Derived",
                        "QA": "PASS",
                        "Tag": "",
                        "Accn": "",
                        "TTM": (val / scale) if val is not None else None,
                    }
                )
            # Margin rows
            if rev_ttm and ebitda_ttm is not None:
                rows.append(
                    {
                        "Section": "TTM",
                        "Line Item": "EBITDA margin",
                        "Source": "Derived",
                        "QA": "PASS",
                        "Tag": "",
                        "Accn": "",
                        "TTM": (ebitda_ttm / rev_ttm) if rev_ttm else None,
                    }
                )
            if rev_ttm and fcf_ttm is not None:
                rows.append(
                    {
                        "Section": "TTM",
                        "Line Item": "FCF margin",
                        "Source": "Derived",
                        "QA": "PASS",
                        "Tag": "",
                        "Accn": "",
                        "TTM": (fcf_ttm / rev_ttm) if rev_ttm else None,
                    }
                )
            # QoQ/YoY rows
            def _growth(series: Any, periods_back: int) -> Dict[str, float]:
                out: Dict[str, float] = {}
                series = series.sort_index()
                for idx in range(periods_back, len(series)):
                    cur = series.iloc[idx]
                    prev = series.iloc[idx - periods_back]
                    if pd_mod.notna(cur) and pd_mod.notna(prev) and prev != 0:
                        out[series.index[idx]] = (float(cur) - float(prev)) / abs(float(prev))
                return out

            if "revenue" in h.columns:
                series = h.set_index("quarter")["revenue"]
                qoq = _growth(series, 1)
                yoy = _growth(series, 4)
                row_qoq = {"Section": "Growth", "Line Item": "Revenue QoQ %", "Source": "Derived", "QA": "PASS", "Tag": "", "Accn": ""}
                row_yoy = {"Section": "Growth", "Line Item": "Revenue YoY %", "Source": "Derived", "QA": "PASS", "Tag": "", "Accn": ""}
                for p in period_cols:
                    row_qoq[p.date()] = qoq.get(p)
                    row_yoy[p.date()] = yoy.get(p)
                rows.append(row_qoq)
                rows.append(row_yoy)
            if "ebitda" in h.columns:
                series = h.set_index("quarter")["ebitda"]
                qoq = _growth(series, 1)
                yoy = _growth(series, 4)
                row_qoq = {"Section": "Growth", "Line Item": "EBITDA QoQ %", "Source": "Derived", "QA": "PASS", "Tag": "", "Accn": ""}
                row_yoy = {"Section": "Growth", "Line Item": "EBITDA YoY %", "Source": "Derived", "QA": "PASS", "Tag": "", "Accn": ""}
                for p in period_cols:
                    row_qoq[p.date()] = qoq.get(p)
                    row_yoy[p.date()] = yoy.get(p)
                rows.append(row_qoq)
                rows.append(row_yoy)
        return pd_mod.DataFrame(rows)

    def write_report_sheet(self, name: str, df: Any, scale_label: str) -> None:
        pd_mod = self._pd()
        wb = self._rt("wb")
        Font = self._rt("Font")
        Alignment = self._rt("Alignment")
        Table = self._rt("Table")
        TableStyleInfo = self._rt("TableStyleInfo")
        get_column_letter = self._rt("get_column_letter")
        _safe_cell = self._rt("_safe_cell")
        _autowidth = self._rt("_autowidth")
        _updated_font = self._rt("_updated_font")
        header_size = self._rt("header_size")
        font_size = self._rt("font_size")
        datetime_cls = self._rt("datetime")
        date_cls = self._rt("date")
        ws = wb.create_sheet(title=name)
        if df is None or df.empty:
            ws["A1"] = "No data."
            return
        ws["A1"] = "Scale"
        ws["B1"] = scale_label
        ws["A2"] = "Values below are scaled."
        # header
        # preserve date headers as actual dates (not strings) for reliable lookups
        ws.append(list(df.columns))
        for _, row in df.iterrows():
            ws.append([None if pd_mod.isna(row.get(c)) else _safe_cell(row.get(c)) for c in df.columns])
        # style
        header_row = 3
        ws.freeze_panes = "A4"
        for c in ws[header_row]:
            c.font = Font(bold=True, size=header_size)
            c.alignment = Alignment(vertical="center")
        _autowidth(ws, len(df.columns))
        ws.sheet_format.defaultRowHeight = 18
        ws.sheet_view.zoomScale = 110
        # date headers formatting
        for cell in ws[header_row]:
            if isinstance(cell.value, (datetime_cls, date_cls)):
                cell.number_format = "yyyy-mm-dd"
        for row in ws.iter_rows(min_row=1, max_row=ws.max_row, max_col=ws.max_column):
            for cell in row:
                size = header_size if cell.row == header_row else font_size
                cell.font = _updated_font(cell.font, size=size, bold=cell.font.b)
        try:
            headers = [c.value for c in ws[header_row]]
            if any(h is None or isinstance(h, (datetime_cls, date_cls)) or not isinstance(h, str) for h in headers):
                raise ValueError("Non-string headers; skip table")
            if len(headers) != len(set(headers)):
                raise ValueError("Duplicate headers; skip table")
            ref = f"A{header_row}:{get_column_letter(len(df.columns))}{ws.max_row}"
            t = Table(displayName=name.replace(" ", "").replace("-", ""), ref=ref)
            t.tableStyleInfo = TableStyleInfo(name="TableStyleMedium9", showRowStripes=True)
            ws.add_table(t)
        except Exception:
            pass
