"""Quarter Notes and Promise evidence source DataFrame builders."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, MutableMapping


@dataclass(frozen=True)
class EvidenceSourceSupportDeps:
    runtime: MutableMapping[str, Any]


class EvidenceSourceSupport:
    def __init__(self, deps: EvidenceSourceSupportDeps) -> None:
        self._runtime = deps.runtime

    def build_qn_evidence_src(self) -> Any:
        runtime = self._runtime
        pd = runtime["pd"]
        hashlib = runtime["hashlib"]
        quarter_notes = runtime["quarter_notes"]
        quarter_notes_view = runtime["_quarter_notes_view"]
        resolve_col = runtime["_resolve_col"]
        qend_date = runtime["_qend_date"]
        parse_first_evidence = runtime["_parse_first_evidence"]

        if quarter_notes is None or quarter_notes.empty:
            return pd.DataFrame(columns=["note_id", "quarter", "category", "claim", "metric", "doc_path", "evidence_id", "snippet"])
        src = quarter_notes_view()
        q_col = resolve_col(src, ["quarter", "quarter_end", "as_of_quarter"])
        cat_col = resolve_col(src, ["category", "tag", "topic"])
        claim_col = resolve_col(src, ["claim", "headline", "note", "body"])
        metric_col = resolve_col(src, ["metric_ref", "metric", "metric_tag"])
        note_id_col = resolve_col(src, ["note_id", "id"])
        doc_col = resolve_col(src, ["doc_path", "evidence_doc", "doc"])
        snip_col = resolve_col(src, ["evidence_snippet", "snippet"])
        out_rows: List[Dict[str, Any]] = []
        for _, r in src.iterrows():
            qd = qend_date(r.get(q_col) if q_col else None)
            if qd is None:
                continue
            claim = str(r.get(claim_col) or "").strip() if claim_col else ""
            note_id = str(r.get(note_id_col) or "").strip() if note_id_col else ""
            if not note_id:
                note_id = hashlib.sha1(f"{qd}|{claim}".encode("utf-8")).hexdigest()[:12]
            ev = parse_first_evidence(r)
            doc_path = str(r.get(doc_col) or ev.get("doc_path") or "").strip() if doc_col else str(ev.get("doc_path") or "").strip()
            snippet = str(r.get(snip_col) or ev.get("snippet") or claim).strip() if snip_col else str(ev.get("snippet") or claim).strip()
            out_rows.append(
                {
                    "note_id": note_id,
                    "quarter": qd,
                    "category": str(r.get(cat_col) or "").strip() if cat_col else "",
                    "claim": claim,
                    "metric": str(r.get(metric_col) or "").strip() if metric_col else "",
                    "doc_path": doc_path,
                    "evidence_id": hashlib.sha1(f"{note_id}|{doc_path}|{snippet[:64]}".encode("utf-8")).hexdigest()[:12],
                    "snippet": snippet,
                }
            )
        out = pd.DataFrame(out_rows)
        if out.empty:
            return pd.DataFrame(columns=["note_id", "quarter", "category", "claim", "metric", "doc_path", "evidence_id", "snippet"])
        return out.sort_values(["quarter", "category", "note_id"]).reset_index(drop=True)

    def build_promise_evidence_src(self) -> Any:
        runtime = self._runtime
        pd = runtime["pd"]
        json = runtime["json"]
        hashlib = runtime["hashlib"]
        promise_progress = runtime["promise_progress"]
        promises = runtime["promises"]
        promises_view = runtime["_promises_view"]
        resolve_col = runtime["_resolve_col"]
        qend_date = runtime["_qend_date"]
        parse_first_evidence = runtime["_parse_first_evidence"]

        out_rows: List[Dict[str, Any]] = []
        if promise_progress is not None and not promise_progress.empty:
            src = promise_progress.copy()
            pid_col = resolve_col(src, ["promise_id", "id"])
            q_col = resolve_col(src, ["quarter", "as_of"])
            st_col = resolve_col(src, ["status"])
            doc_col = resolve_col(src, ["doc_path", "evidence_doc", "doc"])
            snip_col = resolve_col(src, ["evidence_snippet", "snippet"])
            if pid_col is not None and q_col is not None:
                for _, r in src.iterrows():
                    pid = str(r.get(pid_col) or "").strip()
                    qd = qend_date(r.get(q_col))
                    if not pid or qd is None:
                        continue
                    ev = parse_first_evidence(r)
                    doc_path = (
                        str(r.get(doc_col) or ev.get("doc_path") or "").strip()
                        if doc_col
                        else str(ev.get("doc_path") or "").strip()
                    )
                    snippet = (
                        str(r.get(snip_col) or ev.get("snippet") or "").strip()
                        if snip_col
                        else str(ev.get("snippet") or "").strip()
                    )
                    out_rows.append(
                        {
                            "promise_id": pid,
                            "quarter": qd,
                            "status": str(r.get(st_col) or "").strip() if st_col else "",
                            "doc_path": doc_path,
                            "evidence_id": hashlib.sha1(f"{pid}|{qd}|{doc_path}|{snippet[:64]}".encode("utf-8")).hexdigest()[:12],
                            "snippet": snippet,
                        }
                    )

        if promises is not None and not promises.empty:
            ps = promises_view()
            pid_col = resolve_col(ps, ["promise_id", "id"])
            q_col = resolve_col(ps, ["first_seen_evidence_quarter", "created_quarter", "first_seen_q", "first_seen_quarter", "quarter"])
            ev_json_col = resolve_col(ps, ["source_evidence_json", "evidence_history_json", "evidence_json"])
            snip_col = resolve_col(ps, ["evidence_snippet", "snippet", "promise_text", "statement"])
            if pid_col is not None and q_col is not None:
                for _, r in ps.iterrows():
                    pid = str(r.get(pid_col) or "").strip()
                    qd = qend_date(r.get(q_col))
                    if not pid or qd is None:
                        continue
                    doc_path = ""
                    snippet = str(r.get(snip_col) or "").strip() if snip_col else ""
                    if ev_json_col:
                        raw = r.get(ev_json_col)
                        if isinstance(raw, str) and raw.strip():
                            try:
                                parsed = json.loads(raw)
                                ev = parsed[0] if isinstance(parsed, list) and parsed else (parsed if isinstance(parsed, dict) else {})
                                if isinstance(ev, dict):
                                    doc_path = str(ev.get("doc_path") or ev.get("doc_name") or "").strip()
                                    snippet = str(ev.get("snippet") or snippet).strip()
                            except Exception:
                                pass
                    out_rows.append(
                        {
                            "promise_id": pid,
                            "quarter": qd,
                            "status": "",
                            "doc_path": doc_path,
                            "evidence_id": hashlib.sha1(f"{pid}|{qd}|{doc_path}|{snippet[:64]}".encode("utf-8")).hexdigest()[:12],
                            "snippet": snippet,
                        }
                    )

        if not out_rows:
            return pd.DataFrame(columns=["promise_id", "quarter", "status", "doc_path", "evidence_id", "snippet"])
        out = pd.DataFrame(out_rows)
        out = out.sort_values(["promise_id", "quarter"]).drop_duplicates(["promise_id", "quarter"], keep="first")
        return out.reset_index(drop=True)
