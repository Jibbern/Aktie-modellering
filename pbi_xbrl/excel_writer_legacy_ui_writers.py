from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, MutableMapping, Optional, Set, Tuple


@dataclass(frozen=True)
class LegacyUIWriterDeps:
    runtime: MutableMapping[str, Any]


class LegacyUIWriters:
    def __init__(self, deps: LegacyUIWriterDeps) -> None:
        self._runtime = deps.runtime

    def _rt(self, name: str) -> Any:
        return self._runtime[name]

    @staticmethod
    def _severity_label_weight(v: Any) -> Tuple[str, int]:
        s = str(v or "").strip().lower()
        if "fail" in s:
            return "FAIL", 3
        if "warn" in s:
            return "WARN", 2
        return "INFO", 1

    def write_quarter_notes_ui(self, top_k: int = 5) -> List[Dict[str, Any]]:
        wb = self._rt("wb")
        pd = self._rt("pd")
        re = self._rt("re")
        json = self._rt("json")
        hashlib = self._rt("hashlib")
        datetime = self._rt("datetime")
        dt = self._rt("dt")
        Font = self._rt("Font")
        Alignment = self._rt("Alignment")
        PatternFill = self._rt("PatternFill")
        FormulaRule = self._rt("FormulaRule")
        get_column_letter = self._rt("get_column_letter")
        header_size = self._rt("header_size")
        quarter_notes = self._rt("quarter_notes")
        _quarter_notes_view = self._rt("_quarter_notes_view")
        _resolve_col = self._rt("_resolve_col")
        _parse_first_evidence = self._rt("_parse_first_evidence")
        _write_sheet = self._rt("_write_sheet")
        _apply_hyperlink_look = self._rt("_apply_hyperlink_look")

        ws = wb.create_sheet("Quarter_Notes_UI")
        qa_rows: List[Dict[str, Any]] = []
        ts = datetime.now(dt.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
        ws["A1"] = f"Generated at {ts} | Category #rank"
        ws["A1"].font = Font(bold=True, size=header_size)

        if quarter_notes is None or quarter_notes.empty:
            ws["A2"] = "No data."
            ws.freeze_panes = "B2"
            ws.column_dimensions["A"].width = 30
            return qa_rows

        df = _quarter_notes_view()
        q_col = _resolve_col(df, ["quarter", "quarter_end", "as_of_quarter"])
        cat_col = _resolve_col(df, ["category", "tag", "topic"])
        claim_col = _resolve_col(df, ["claim", "headline", "note", "body", "statement"])
        sev_col = _resolve_col(df, ["severity", "qa_severity", "status"])
        score_col = _resolve_col(df, ["severity_score", "score"])
        metric_col = _resolve_col(df, ["metric_ref", "metric", "metric_tag"])
        metric_val_col = _resolve_col(df, ["metric_value", "value", "extracted_value"])
        note_id_col = _resolve_col(df, ["note_id", "id"])
        ev_doc_col = _resolve_col(df, ["evidence_doc", "doc_path", "doc"])
        ev_loc_col = _resolve_col(df, ["evidence_loc", "section_or_page", "page", "section"])
        ev_snip_col = _resolve_col(df, ["evidence_snippet", "snippet"])

        if q_col is None or cat_col is None or claim_col is None:
            ws["A2"] = "Missing required source columns."
            qa_rows.append(
                {
                    "quarter": None,
                    "metric": "Quarter_Notes_UI",
                    "check": "quarter_notes_ui_source_columns",
                    "status": "fail",
                    "message": "Quarter_Notes missing required columns for UI matrix.",
                }
            )
            ws.freeze_panes = "B2"
            ws.column_dimensions["A"].width = 30
            return qa_rows

        time_anchor_re = re.compile(
            r"\b(by\s+20\d{2}|next quarter|this quarter|this year|next year|during\s+20\d{2}|in\s+20\d{2}|q[1-4]\s*20\d{2}|fy\s*20\d{2})\b",
            re.I,
        )
        records: List[Dict[str, Any]] = []

        def _extract_numeric_hint(text: str) -> str:
            if not text:
                return ""
            pat = re.compile(
                r"[$]?\s*\(?[-+]?\d[\d,]*(?:\.\d+)?\)?\s*(?:%|bps|x|m|mm|bn|b)?",
                re.I,
            )
            for m in pat.finditer(str(text)):
                tok = str(m.group(0) or "").strip()
                if not tok:
                    continue
                tok = tok.strip(",.;:")
                core = re.sub(r"[^0-9.]", "", tok)
                if not core:
                    continue
                try:
                    if re.fullmatch(r"\d{4}", core):
                        yr = int(core)
                        if 1900 <= yr <= 2100:
                            continue
                except Exception:
                    pass
                return tok
            return ""

        def _fmt_note_metric(metric_name: str, raw_val: Any) -> str:
            v = pd.to_numeric(raw_val, errors="coerce")
            if pd.isna(v):
                return ""
            x = float(v)
            m = str(metric_name or "").lower()
            if abs(x) >= 1_000_000:
                return f"${x / 1e6:,.1f}m"
            if "bps" in m:
                return f"{x:+.0f} bps"
            if "share" in m and abs(x) < 50:
                return f"${x:,.2f}/sh"
            if ("yoy" in m or "margin" in m or "growth" in m or "yield" in m) and abs(x) <= 5:
                return f"{x * 100:+.1f}%"
            if abs(x) < 1000:
                return f"{x:,.2f}"
            return f"{x:,.0f}"

        for _, row in df.iterrows():
            q = pd.to_datetime(row.get(q_col), errors="coerce")
            if pd.isna(q):
                continue
            qd = pd.Timestamp(q).date()
            category = str(row.get(cat_col) or "Uncategorized").strip() or "Uncategorized"
            claim = str(row.get(claim_col) or "").strip()
            sev_label, sev_weight = self._severity_label_weight(row.get(sev_col) if sev_col else "INFO")
            score_val = pd.to_numeric(row.get(score_col), errors="coerce") if score_col else pd.NA
            score = float(score_val) if pd.notna(score_val) else 0.0
            metric = str(row.get(metric_col) or "").strip() if metric_col else ""
            ev = _parse_first_evidence(row)
            metric_value = row.get(metric_val_col) if metric_val_col else None
            if metric_value is None and isinstance(ev, dict):
                metric_value = ev.get("extracted_value")
            evidence_doc = str(row.get(ev_doc_col) or ev.get("doc_path") or ev.get("doc_name") or "").strip() if ev_doc_col else str(ev.get("doc_path") or ev.get("doc_name") or "").strip()
            evidence_loc = str(row.get(ev_loc_col) or ev.get("section_or_page") or ev.get("page") or "").strip() if ev_loc_col else str(ev.get("section_or_page") or ev.get("page") or "").strip()
            evidence_snippet = str(row.get(ev_snip_col) or ev.get("snippet") or claim).strip() if ev_snip_col else str(ev.get("snippet") or claim).strip()

            note_id = str(row.get(note_id_col) or "").strip() if note_id_col else ""
            generated_id = False
            if not note_id:
                generated_id = True
                note_id = hashlib.sha1(f"{qd.isoformat()}|{category}|{claim}".encode("utf-8")).hexdigest()[:12]

            if not claim:
                qa_rows.append(
                    {
                        "quarter": qd,
                        "metric": "Quarter_Notes_UI",
                        "check": "quarter_notes_ui_missing_claim",
                        "status": "fail",
                        "message": f"UI note missing claim (note_id={note_id}).",
                    }
                )
                continue
            if generated_id:
                qa_rows.append(
                    {
                        "quarter": qd,
                        "metric": "Quarter_Notes_UI",
                        "check": "quarter_notes_ui_note_id_generated",
                        "status": "fail",
                        "message": f"UI note had no source note_id; generated stable id {note_id}.",
                    }
                )

            if sev_label in {"FAIL", "WARN"} and (not evidence_doc or not evidence_snippet):
                qa_rows.append(
                    {
                        "quarter": qd,
                        "metric": "Quarter_Notes_UI",
                        "check": "quarter_notes_ui_missing_evidence",
                        "status": "warn",
                        "message": f"UI note {note_id} ({sev_label}) missing evidence doc/snippet.",
                    }
                )
            if not bool(time_anchor_re.search(f"{claim} {evidence_snippet}")):
                qa_rows.append(
                    {
                        "quarter": qd,
                        "metric": "Quarter_Notes_UI",
                        "check": "quarter_notes_ui_time_anchor",
                        "status": "warn",
                        "message": f"UI note {note_id} has no explicit time anchor.",
                    }
                )

            records.append(
                {
                    "quarter": qd,
                    "category": category,
                    "claim": claim,
                    "severity": sev_label,
                    "severity_weight": sev_weight,
                    "score": score,
                    "metric": metric,
                    "metric_value": metric_value,
                    "note_id": note_id,
                    "evidence_doc": evidence_doc,
                    "evidence_loc": evidence_loc,
                    "evidence_snippet": evidence_snippet,
                }
            )

        if not records:
            ws["A2"] = "No notes after filtering."
            ws.freeze_panes = "B2"
            ws.column_dimensions["A"].width = 30
            return qa_rows

        rec_df = pd.DataFrame(records)
        rec_df["quarter"] = pd.to_datetime(rec_df["quarter"], errors="coerce").dt.date
        quarters = sorted(rec_df["quarter"].dropna().unique().tolist(), reverse=True)
        categories = sorted(rec_df["category"].dropna().astype(str).unique().tolist())
        for i, qd in enumerate(quarters, start=2):
            c = ws.cell(row=1, column=i, value=str(qd))
            c.font = Font(bold=True, size=header_size)
            c.alignment = Alignment(horizontal="center", vertical="center")

        rec_df = rec_df.sort_values(["category", "quarter", "score", "severity_weight"], ascending=[True, False, False, False]).reset_index(drop=True)
        grouped: Dict[Tuple[str, date], List[Dict[str, Any]]] = {}
        for _, r in rec_df.iterrows():
            qk = r.get("quarter")
            if pd.isna(qk):
                continue
            grouped.setdefault((str(r["category"]), qk), []).append(r.to_dict())

        row_idx = 2
        note_link_cells: List[Tuple[str, str]] = []
        used_notes: Dict[str, Dict[str, Any]] = {}
        for cat in categories:
            cat_max = 0
            for qd in quarters:
                cat_max = max(cat_max, len(grouped.get((cat, qd), [])))
            rank_max = max(1, min(top_k, cat_max))
            for rank in range(1, rank_max + 1):
                ws.cell(row=row_idx, column=1, value=f"{cat} #{rank}")
                ws.cell(row=row_idx, column=1).alignment = Alignment(vertical="top")
                for i, qd in enumerate(quarters, start=2):
                    notes = grouped.get((cat, qd), [])
                    if len(notes) < rank:
                        continue
                    n = notes[rank - 1]
                    metric_txt = _fmt_note_metric(str(n.get("metric") or ""), n.get("metric_value"))
                    if not metric_txt:
                        metric_txt = _extract_numeric_hint(
                            f"{n.get('claim') or ''} {n.get('evidence_snippet') or ''}"
                        )
                    txt = str(n["claim"])
                    if metric_txt:
                        txt += f" ({metric_txt})"
                    txt = txt[:220]
                    cell = ws.cell(row=row_idx, column=i, value=txt)
                    cell.alignment = Alignment(wrap_text=True, vertical="top")
                    note_link_cells.append((cell.coordinate, str(n["note_id"])))
                    used_notes[str(n["note_id"])] = n
                row_idx += 1
            # compact layout: no extra spacer row between categories

        last_col = get_column_letter(max(2, 1 + len(quarters)))
        last_row = max(2, row_idx - 1)
        ws.freeze_panes = "B2"
        ws.column_dimensions["A"].width = 30
        for cidx in range(2, 2 + len(quarters)):
            ws.column_dimensions[get_column_letter(cidx)].width = 72
        for rr in range(2, last_row + 1):
            ws.row_dimensions[rr].height = 56

        if last_row >= 2 and len(quarters) > 0:
            rng = f"B2:{last_col}{last_row}"
            ws.conditional_formatting.add(
                rng,
                FormulaRule(formula=["ISNUMBER(SEARCH(\"[FAIL]\",B2))"], fill=PatternFill("solid", fgColor="FFC7CE")),
            )
            ws.conditional_formatting.add(
                rng,
                FormulaRule(
                    formula=["AND(ISNUMBER(SEARCH(\"[WARN]\",B2)),ISERROR(SEARCH(\"[FAIL]\",B2)))"],
                    fill=PatternFill("solid", fgColor="FFEB9C"),
                ),
            )

        evidence_rows = []
        for note_id, n in used_notes.items():
            evidence_rows.append(
                {
                    "note_id": note_id,
                    "quarter": n.get("quarter"),
                    "category": n.get("category"),
                    "claim": n.get("claim"),
                    "metric": n.get("metric"),
                    "doc_path": n.get("evidence_doc"),
                    "evidence_loc": n.get("evidence_loc"),
                    "snippet": n.get("evidence_snippet"),
                }
            )
        evidence_df = pd.DataFrame(evidence_rows).sort_values(["quarter", "category", "note_id"]).reset_index(drop=True) if evidence_rows else pd.DataFrame()
        _write_sheet("Quarter_Notes_Evidence", evidence_df)

        if not evidence_df.empty and "note_id" in evidence_df.columns and "Quarter_Notes_Evidence" in wb.sheetnames:
            note_to_row: Dict[str, int] = {}
            for i, nid in enumerate(evidence_df["note_id"].astype(str).tolist(), start=2):
                note_to_row[nid] = i
            for coord, nid in note_link_cells:
                rr = note_to_row.get(nid)
                if rr is None:
                    continue
                c = ws[coord]
                _apply_hyperlink_look(c, f"#'Quarter_Notes_Evidence'!A{rr}")

        return qa_rows

    def write_promise_tracker_ui(self) -> List[Dict[str, Any]]:
        wb = self._rt("wb")
        pd = self._rt("pd")
        re = self._rt("re")
        json = self._rt("json")
        datetime = self._rt("datetime")
        dt = self._rt("dt")
        Font = self._rt("Font")
        Alignment = self._rt("Alignment")
        PatternFill = self._rt("PatternFill")
        FormulaRule = self._rt("FormulaRule")
        get_column_letter = self._rt("get_column_letter")
        header_size = self._rt("header_size")
        promises = self._rt("promises")
        promise_progress = self._rt("promise_progress")
        _promises_view = self._rt("_promises_view")
        _resolve_col = self._rt("_resolve_col")
        _write_sheet = self._rt("_write_sheet")
        _apply_hyperlink_look = self._rt("_apply_hyperlink_look")

        ws = wb.create_sheet("Promise_Tracker_UI")
        qa_rows: List[Dict[str, Any]] = []
        ts = datetime.now(dt.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
        ws["A1"] = f"Promise (metric | text | target | created | id) | Generated at {ts}"
        ws["A1"].font = Font(bold=True, size=header_size)

        def _qend_date(x: Any) -> Optional[date]:
            t = pd.to_datetime(x, errors="coerce")
            if pd.isna(t):
                return None
            return pd.Timestamp(t).to_period("Q").end_time.date()

        if promises is None or promises.empty:
            ws["A2"] = "No data."
            ws.freeze_panes = "B2"
            ws.column_dimensions["A"].width = 74
            return qa_rows

        p = _promises_view().copy()
        pid_col = _resolve_col(p, ["promise_id", "id"])
        metric_col = _resolve_col(p, ["metric_tag", "metric"])
        text_col = _resolve_col(p, ["promise_text", "statement", "claim"])
        target_time_col = _resolve_col(p, ["target_time", "deadline"])
        target_val_col = _resolve_col(p, ["target_value", "value"])
        units_col = _resolve_col(p, ["units", "target_unit", "unit"])
        created_col = _resolve_col(p, ["created_quarter", "first_seen_quarter", "quarter"])

        if pid_col is None:
            ws["A2"] = "Missing promise_id in source."
            qa_rows.append(
                {
                    "quarter": None,
                    "metric": "Promise_Tracker_UI",
                    "check": "promise_tracker_ui_source_columns",
                    "status": "fail",
                    "message": "Promise_Tracker missing promise_id column.",
                }
            )
            ws.freeze_panes = "B2"
            ws.column_dimensions["A"].width = 74
            return qa_rows

        p["_pid"] = p[pid_col].astype(str)
        p["_metric"] = p[metric_col].astype(str) if metric_col else ""
        p["_text"] = p[text_col].astype(str) if text_col else ""
        p["_target_time"] = pd.to_datetime(p[target_time_col], errors="coerce") if target_time_col else pd.NaT
        p["_target_val"] = pd.to_numeric(p[target_val_col], errors="coerce") if target_val_col else pd.NA
        p["_units"] = p[units_col].astype(str) if units_col else ""
        p["_created"] = pd.to_datetime(p[created_col], errors="coerce") if created_col else pd.NaT
        p = p.sort_values(["_metric", "_created", "_pid"], na_position="last").reset_index(drop=True)

        prog = promise_progress.copy() if promise_progress is not None else pd.DataFrame()
        q_col = _resolve_col(prog, ["quarter", "as_of"])
        prog_pid_col = _resolve_col(prog, ["promise_id", "id"])
        status_col = _resolve_col(prog, ["status"])
        progress_col = _resolve_col(prog, ["progress_pct"])
        src_doc_col = _resolve_col(prog, ["doc_path", "doc", "evidence_doc"])
        src_loc_col = _resolve_col(prog, ["section_or_page", "evidence_loc", "page"])
        src_snip_col = _resolve_col(prog, ["evidence_snippet", "snippet"])
        src_json_col = _resolve_col(prog, ["source_evidence_json", "evidence_json", "evidence"])

        quarters: List[date] = []
        if prog is not None and not prog.empty and q_col and prog_pid_col and status_col:
            prog["_quarter"] = pd.to_datetime(prog[q_col], errors="coerce")
            prog = prog[prog["_quarter"].notna()].copy()
        qset: Set[date] = set()
        if prog is not None and not prog.empty and "_quarter" in prog.columns:
            for qv in pd.to_datetime(prog["_quarter"], errors="coerce").dropna().tolist():
                qe = _qend_date(qv)
                if qe is not None:
                    qset.add(qe)
        for qv in pd.to_datetime(p["_created"], errors="coerce").dropna().tolist():
            qe = _qend_date(qv)
            if qe is not None:
                qset.add(qe)
        quarters = sorted(qset, reverse=True)
        for i, qd in enumerate(quarters, start=2):
            h = ws.cell(row=1, column=i, value=str(qd))
            h.font = Font(bold=True, size=header_size)
            h.alignment = Alignment(horizontal="center", vertical="center")

        status_map: Dict[Tuple[str, date], Dict[str, Any]] = {}
        evidence_rows: List[Dict[str, Any]] = []
        if prog is not None and not prog.empty and q_col and prog_pid_col and status_col:
            for _, r in prog.iterrows():
                pid = str(r.get(prog_pid_col) or "").strip()
                qv = pd.to_datetime(r.get("_quarter"), errors="coerce")
                if not pid or pd.isna(qv):
                    continue
                qd = _qend_date(qv)
                if qd is None:
                    continue
                status = str(r.get(status_col) or "").strip().lower()
                if not status:
                    continue
                ev_obj = {}
                if src_json_col:
                    raw = r.get(src_json_col)
                    if isinstance(raw, str) and raw.strip():
                        try:
                            parsed = json.loads(raw)
                            if isinstance(parsed, dict):
                                ev_obj = parsed
                            elif isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
                                ev_obj = parsed[0]
                        except Exception:
                            ev_obj = {}
                doc_path = str(r.get(src_doc_col) or ev_obj.get("doc_path") or "").strip() if src_doc_col else str(ev_obj.get("doc_path") or "").strip()
                loc = str(r.get(src_loc_col) or ev_obj.get("section_or_page") or "").strip() if src_loc_col else str(ev_obj.get("section_or_page") or "").strip()
                snippet = str(r.get(src_snip_col) or ev_obj.get("snippet") or "").strip() if src_snip_col else str(ev_obj.get("snippet") or "").strip()
                if not doc_path or not snippet:
                    qa_rows.append(
                        {
                            "quarter": qd,
                            "metric": "Promise_Tracker_UI",
                            "check": "promise_progress_missing_evidence",
                            "status": "fail",
                            "message": f"promise {pid} status {status} missing evidence doc/snippet.",
                        }
                    )
                status_map[(pid, qd)] = {
                    "status": status,
                    "progress": pd.to_numeric(r.get(progress_col), errors="coerce") if progress_col else pd.NA,
                    "doc_path": doc_path,
                    "evidence_loc": loc,
                    "snippet": snippet,
                    "qa_message": str(r.get("qa_message") or ""),
                }
                evidence_rows.append(
                    {
                        "promise_id": pid,
                        "quarter": qd,
                        "status": status,
                        "doc_path": doc_path,
                        "evidence_loc": loc,
                        "snippet": snippet,
                    }
                )

        row_idx = 2
        status_cell_refs: List[Tuple[str, str, date]] = []
        for idx, (_, r) in enumerate(p.iterrows(), start=1):
            pid = str(r["_pid"])
            metric = str(r["_metric"] or "").strip()
            txt = str(r["_text"] or "").strip()
            metric_pref = f"[{metric}] " if metric else ""
            promise_short = re.sub(r"\s+", " ", f"{metric_pref}{txt}".strip())
            if len(promise_short) > 140:
                promise_short = f"{promise_short[:137]}..."
            t_time = pd.to_datetime(r["_target_time"], errors="coerce")
            target_q = _qend_date(t_time)
            tv = pd.to_numeric(r["_target_val"], errors="coerce")
            units = str(r["_units"] or "").strip()
            created_q = pd.to_datetime(r["_created"], errors="coerce")
            created_qe = _qend_date(created_q)
            created_txt = created_qe.isoformat() if created_qe else "n/a"
            target_txt = f"{tv:,.3f} {units}".strip() if pd.notna(tv) else ("qualitative" if target_q else "n/a")
            left_txt = f"Promise #{idx} | id:{pid}"
            ws.cell(row=row_idx, column=1, value=left_txt[:140])
            ws.cell(row=row_idx, column=1).alignment = Alignment(wrap_text=True, vertical="top")

            if target_q and pd.isna(tv):
                qa_rows.append(
                    {
                        "quarter": created_qe if created_qe else None,
                        "metric": "Promise_Tracker_UI",
                        "check": "promise_qualitative",
                        "status": "warn",
                        "message": f"promise {pid} has target_time but no target_value (qualitative).",
                    }
                )

            unclear_run = 0
            for i, qd in enumerate(quarters, start=2):
                st = status_map.get((pid, qd))
                cell_txt = ""
                if st:
                    status_txt = str(st["status"])
                    pct = st.get("progress")
                    if pd.notna(pct):
                        cell_txt = f"{status_txt} ({float(pct) * 100:.0f}%)"
                    else:
                        cell_txt = status_txt
                    if target_q is not None and qd == target_q:
                        if status_txt == "achieved":
                            cell_txt += " | TARGET HIT"
                        elif status_txt in {"broken", "missed"}:
                            cell_txt += " | TARGET MISS"
                        else:
                            cell_txt += " | TARGET Q"
                if created_qe is not None and qd == created_qe:
                    created_block = f"{promise_short}\ncreated {created_qe.isoformat()}"
                    cell_txt = f"{created_block}\n{cell_txt}".strip() if cell_txt else created_block
                if cell_txt:
                    ws.cell(row=row_idx, column=i, value=cell_txt[:220])
                    ws.cell(row=row_idx, column=i).alignment = Alignment(wrap_text=True, vertical="top")
                    if st:
                        status_cell_refs.append((ws.cell(row=row_idx, column=i).coordinate, pid, qd))
                        is_unclear = (
                            status_txt == "unclear"
                            or ("fallback" in status_txt)
                            or ("derived" in status_txt)
                            or ("fallback" in st.get("qa_message", "").lower())
                            or ("derived" in st.get("qa_message", "").lower())
                        )
                        if is_unclear:
                            unclear_run += 1
                            if unclear_run >= 5:
                                qa_rows.append(
                                    {
                                        "quarter": qd,
                                        "metric": "Promise_Tracker_UI",
                                        "check": "promise_unclear_streak",
                                        "status": "warn",
                                        "message": f"promise {pid} has >4 quarters unclear/fallback streak.",
                                    }
                                )
                        else:
                            unclear_run = 0
                elif target_q is not None and qd == target_q:
                    ws.cell(row=row_idx, column=i, value="TARGET Q (no update)")
                    ws.cell(row=row_idx, column=i).alignment = Alignment(wrap_text=True, vertical="top")
            row_idx += 1

        ws.freeze_panes = "B2"
        ws.column_dimensions["A"].width = 78
        for cidx in range(2, 2 + len(quarters)):
            ws.column_dimensions[get_column_letter(cidx)].width = 44
        for rr in range(2, row_idx):
            ws.row_dimensions[rr].height = 86

        if row_idx > 2 and len(quarters) > 0:
            last_col = get_column_letter(1 + len(quarters))
            rng = f"B2:{last_col}{row_idx-1}"
            ws.conditional_formatting.add(rng, FormulaRule(formula=["ISNUMBER(SEARCH(\"broken\",B2))"], fill=PatternFill("solid", fgColor="FFC7CE")))
            ws.conditional_formatting.add(rng, FormulaRule(formula=["ISNUMBER(SEARCH(\"at_risk\",B2))"], fill=PatternFill("solid", fgColor="FFEB9C")))
            ws.conditional_formatting.add(rng, FormulaRule(formula=["ISNUMBER(SEARCH(\"achieved\",B2))"], fill=PatternFill("solid", fgColor="C6EFCE")))
            ws.conditional_formatting.add(rng, FormulaRule(formula=["ISNUMBER(SEARCH(\"on_track\",B2))"], fill=PatternFill("solid", fgColor="E2F0D9")))
            ws.conditional_formatting.add(rng, FormulaRule(formula=["ISNUMBER(SEARCH(\"unclear\",B2))"], fill=PatternFill("solid", fgColor="D9D9D9")))

        evidence_df = pd.DataFrame(evidence_rows)
        if not evidence_df.empty:
            evidence_df = evidence_df.sort_values(["promise_id", "quarter"]).drop_duplicates(["promise_id", "quarter"], keep="last").reset_index(drop=True)
        _write_sheet("Promise_Evidence", evidence_df)

        if not evidence_df.empty and "Promise_Evidence" in wb.sheetnames:
            ev_map: Dict[Tuple[str, date], int] = {}
            for i, rr in evidence_df.iterrows():
                qv = pd.to_datetime(rr.get("quarter"), errors="coerce")
                if pd.isna(qv):
                    continue
                qd = _qend_date(qv)
                if qd is None:
                    continue
                ev_map[(str(rr.get("promise_id")), qd)] = i + 2
            for coord, pid, qd in status_cell_refs:
                rnum = ev_map.get((pid, qd))
                if rnum is None:
                    continue
                c = ws[coord]
                _apply_hyperlink_look(c, f"#'Promise_Evidence'!A{rnum}")

        return qa_rows
