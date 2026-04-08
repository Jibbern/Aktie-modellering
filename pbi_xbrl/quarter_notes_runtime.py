"""Quarter-notes runtime helpers shared within one workbook export.

The visible quarter-notes sheet still orchestrates selection and rendering inside
``excel_writer_context.py``. This module owns the lower-level filing/doc caches and
document analysis helpers that are safe to reuse across repeated sheet writes within
the same export run.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
import html
from pathlib import Path
import re
from typing import Any, Callable, Dict, List, Optional, Pattern, Tuple

import pandas as pd


@dataclass
class QuarterNotesRuntime:
    submissions_recent_rows_cache: Optional[List[Dict[str, Any]]] = None
    filing_quarter_end_cache: Dict[str, Optional[date]] = field(default_factory=dict)
    filings_for_quarter_cache: Dict[date, List[Dict[str, Any]]] = field(default_factory=dict)
    filings_for_quarter_forms_cache: Dict[Tuple[date, Tuple[str, ...]], List[Dict[str, Any]]] = field(
        default_factory=dict
    )
    quarter_doc_pool_cache: Dict[Tuple[str, Tuple[str, ...], int, str, str, int], List[Dict[str, Any]]] = field(
        default_factory=dict
    )
    docs_for_accn_ranked_cache: Dict[str, List[Path]] = field(default_factory=dict)
    docs_for_accn_sorted_cache: Dict[Tuple[str, int], List[Path]] = field(default_factory=dict)
    doc_analysis_cache: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    doc_plain_cache: Dict[str, str] = field(default_factory=dict)
    doc_source_priority_cache: Dict[str, Tuple[int, str]] = field(default_factory=dict)
    heading_regex_cache: Dict[Tuple[str, ...], Pattern[str]] = field(default_factory=dict)
    heading_sentence_cache: Dict[Tuple[str, Tuple[str, ...], int, int], List[str]] = field(default_factory=dict)
    action_chunk_cache: Dict[str, List[str]] = field(default_factory=dict)
    cashflow_section_sentence_cache: Dict[Tuple[str, str, str], List[List[str]]] = field(default_factory=dict)
    doc_quarter_match_cache: Dict[Tuple[str, str, str, str], bool] = field(default_factory=dict)

    _bullet_splitter: Pattern[str] = field(
        default_factory=lambda: re.compile(r"[Ã¢â‚¬Â¢Ã¢â€”ÂÃ¢â€“ÂªÃ¢â€”Â¦]+"),
        init=False,
        repr=False,
    )

    @staticmethod
    def filing_cache_key(filing_row: Dict[str, Any]) -> str:
        return "|".join(
            [
                str(filing_row.get("accn") or ""),
                str(filing_row.get("form") or ""),
                str(filing_row.get("doc") or ""),
                str(filing_row.get("report") or ""),
                str(filing_row.get("filed") or ""),
            ]
        )

    def load_submissions_recent_rows(self, rows_loader: Callable[[], List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        if self.submissions_recent_rows_cache is None:
            self.submissions_recent_rows_cache = list(rows_loader() or [])
        return list(self.submissions_recent_rows_cache)

    def filing_quarter_end(
        self,
        filing_row: Dict[str, Any],
        *,
        parse_date: Callable[[Any], Optional[date]],
    ) -> Optional[date]:
        cache_key = self.filing_cache_key(filing_row)
        if cache_key in self.filing_quarter_end_cache:
            return self.filing_quarter_end_cache[cache_key]
        rep_d = parse_date(filing_row.get("report"))
        rep_ts = pd.to_datetime(rep_d, errors="coerce")
        if pd.notna(rep_ts):
            q_end = pd.Timestamp(rep_ts).to_period("Q").end_time.date()
            self.filing_quarter_end_cache[cache_key] = q_end
            return q_end
        filed_d = parse_date(filing_row.get("filed"))
        filed_ts = pd.to_datetime(filed_d, errors="coerce")
        if pd.notna(filed_ts):
            q_end = pd.Timestamp(filed_ts - timedelta(days=60)).to_period("Q").end_time.date()
            self.filing_quarter_end_cache[cache_key] = q_end
            return q_end
        self.filing_quarter_end_cache[cache_key] = None
        return None

    def filings_for_quarter_forms(
        self,
        quarter_end: date,
        forms: Any,
        *,
        rows_loader: Callable[[], List[Dict[str, Any]]],
        parse_date: Callable[[Any], Optional[date]],
    ) -> List[Dict[str, Any]]:
        form_key = tuple(sorted(str(x or "").upper() for x in (forms or []) if str(x or "").strip()))
        cache_key = (quarter_end, form_key)
        cached = self.filings_for_quarter_forms_cache.get(cache_key)
        if cached is not None:
            return list(cached)
        quarter_cached = self.filings_for_quarter_cache.get(quarter_end)
        if quarter_cached is None:
            quarter_rows: List[Dict[str, Any]] = []
            for filing_row in self.load_submissions_recent_rows(rows_loader):
                if self.filing_quarter_end(filing_row, parse_date=parse_date) != quarter_end:
                    continue
                quarter_rows.append(filing_row)
            self.filings_for_quarter_cache[quarter_end] = list(quarter_rows)
            quarter_cached = quarter_rows
        filtered: List[Dict[str, Any]] = []
        for filing_row in quarter_cached:
            form = str(filing_row.get("form") or "").upper()
            if form_key and form not in form_key:
                continue
            filtered.append(filing_row)
        self.filings_for_quarter_forms_cache[cache_key] = list(filtered)
        return list(filtered)

    def docs_for_accn_sorted(
        self,
        accession: str,
        *,
        sec_docs_for_accession: Callable[[str], List[Path]],
        max_docs: int = 16,
    ) -> List[Path]:
        accession_key = str(accession or "")
        cache_key = (accession_key, int(max_docs))
        cached = self.docs_for_accn_sorted_cache.get(cache_key)
        if cached is not None:
            return list(cached)
        ranked = self.docs_for_accn_ranked_cache.get(accession_key)
        if ranked is None:
            uniq = sec_docs_for_accession(accession)
            if not uniq:
                self.docs_for_accn_ranked_cache[accession_key] = []
                self.docs_for_accn_sorted_cache[cache_key] = []
                return []

            def _score_doc(path_in: Path) -> Tuple[int, int]:
                name_low = path_in.name.lower()
                score = 0
                if "ex99" in name_low or "press" in name_low or "earnings" in name_low:
                    score += 30
                if "ceo" in name_low or "letter" in name_low or "annualletter" in name_low or "shareholder" in name_low:
                    score += 22
                if "slide" in name_low or "presentation" in name_low:
                    score += 18
                if "10k" in name_low:
                    score += 16
                if "10q" in name_low:
                    score += 14
                if "_pbi-" in name_low:
                    score += 12
                if "ex10" in name_low or "agreement" in name_low or "indenture" in name_low:
                    score -= 20
                if "ex31" in name_low or "ex32" in name_low:
                    score -= 12
                return (score, -len(name_low))

            ranked = sorted(uniq, key=_score_doc, reverse=True)
            self.docs_for_accn_ranked_cache[accession_key] = list(ranked)
        if not ranked:
            self.docs_for_accn_sorted_cache[cache_key] = []
            return []
        ranked_limited = list(ranked[: max(1, int(max_docs))])
        self.docs_for_accn_sorted_cache[cache_key] = list(ranked_limited)
        return ranked_limited

    def quarter_doc_pool(
        self,
        quarter_end: date,
        forms: Any,
        *,
        rows_loader: Callable[[], List[Dict[str, Any]]],
        parse_date: Callable[[Any], Optional[date]],
        sec_docs_for_accession: Callable[[str], List[Path]],
        locate_cached_doc_path: Callable[[str, str], Optional[Path]],
        path_cache_key: Callable[[Path], str],
        read_cached_doc_text: Callable[[Path], str],
        normalize_text: Callable[[Any], str],
        max_docs: int = 16,
        doc_scope: str = "ranked",
        row_scope: str = "quarter_filtered",
        require_quarter_match: bool = True,
    ) -> List[Dict[str, Any]]:
        form_key = tuple(sorted(str(x or "").upper() for x in (forms or []) if str(x or "").strip()))
        doc_scope_key = str(doc_scope or "ranked").strip().lower() or "ranked"
        row_scope_key = str(row_scope or "quarter_filtered").strip().lower() or "quarter_filtered"
        # Run-scoped only: a fresh QuarterNotesRuntime is built for each export, so
        # this pool can safely reuse doc discovery/parsing within one run without
        # risking stale results after filings, parser output, or options change.
        cache_key = (
            str(quarter_end),
            form_key,
            int(max_docs),
            doc_scope_key,
            row_scope_key,
            int(bool(require_quarter_match)),
        )
        cached = self.quarter_doc_pool_cache.get(cache_key)
        if cached is not None:
            return list(cached)

        if row_scope_key == "quarter_filtered":
            filing_rows = self.filings_for_quarter_forms(
                quarter_end,
                form_key,
                rows_loader=rows_loader,
                parse_date=parse_date,
            )
        else:
            filing_rows = []
            for filing_row in self.load_submissions_recent_rows(rows_loader):
                form = str(filing_row.get("form") or "").upper()
                if form_key and form not in form_key:
                    continue
                filing_rows.append(filing_row)

        doc_rows: List[Dict[str, Any]] = []
        for filing_row in filing_rows:
            form = str(filing_row.get("form") or "").upper()
            accn = str(filing_row.get("accn") or "")
            if not accn:
                continue
            if doc_scope_key == "primary":
                doc_path = locate_cached_doc_path(accn, str(filing_row.get("doc") or ""))
                docs = [doc_path] if doc_path is not None else []
            else:
                docs = self.docs_for_accn_sorted(
                    accn,
                    sec_docs_for_accession=sec_docs_for_accession,
                    max_docs=max_docs,
                )
                if not docs:
                    doc_path = locate_cached_doc_path(accn, str(filing_row.get("doc") or ""))
                    docs = [doc_path] if doc_path is not None else []
            for doc_path in docs:
                if doc_path is None or not doc_path.exists():
                    continue
                doc_analysis = self.doc_analysis(
                    doc_path,
                    path_cache_key=path_cache_key,
                    read_cached_doc_text=read_cached_doc_text,
                    normalize_text=normalize_text,
                )
                plain = str(doc_analysis.get("plain") or "")
                if not plain:
                    continue
                matches_quarter = True
                if require_quarter_match:
                    matches_quarter = self.doc_matches_quarter(
                        filing_row,
                        form=form,
                        doc_path=doc_path,
                        plain_text=plain,
                        quarter_end=quarter_end,
                        path_cache_key=path_cache_key,
                        read_cached_doc_text=read_cached_doc_text,
                        normalize_text=normalize_text,
                        parse_date=parse_date,
                    )
                    if not matches_quarter:
                        continue
                doc_priority, source_type = self.doc_source_priority(
                    doc_path,
                    path_cache_key=path_cache_key,
                    read_cached_doc_text=read_cached_doc_text,
                    normalize_text=normalize_text,
                )
                doc_rows.append(
                    {
                        # This pool is run-scoped only. It intentionally caches
                        # reusable filing/doc analysis inside one export and is
                        # recreated for every new writer context.
                        "filing_row": filing_row,
                        "form": form,
                        "accn": accn,
                        "doc_path": doc_path,
                        "doc_name": doc_path.name.lower(),
                        "doc_analysis": doc_analysis,
                        "plain": plain,
                        "plain_low": str(doc_analysis.get("plain_low") or ""),
                        "doc_priority": int(doc_priority),
                        "source_type": str(source_type or ""),
                        "matches_quarter": bool(matches_quarter),
                    }
                )
        self.quarter_doc_pool_cache[cache_key] = list(doc_rows)
        return list(doc_rows)

    def doc_analysis(
        self,
        path_in: Path,
        *,
        path_cache_key: Callable[[Path], str],
        read_cached_doc_text: Callable[[Path], str],
        normalize_text: Callable[[Any], str],
    ) -> Dict[str, Any]:
        cache_key = path_cache_key(path_in)
        cached = self.doc_analysis_cache.get(cache_key)
        if cached is not None:
            return cached
        plain = normalize_text(html.unescape(read_cached_doc_text(path_in)))
        cached = {
            "path_key": cache_key,
            "plain": plain,
            "plain_low": plain.lower(),
            "name_low": path_in.name.lower(),
        }
        self.doc_analysis_cache[cache_key] = cached
        return cached

    def doc_plain(
        self,
        path_in: Path,
        *,
        path_cache_key: Callable[[Path], str],
        read_cached_doc_text: Callable[[Path], str],
        normalize_text: Callable[[Any], str],
    ) -> str:
        analysis = self.doc_analysis(
            path_in,
            path_cache_key=path_cache_key,
            read_cached_doc_text=read_cached_doc_text,
            normalize_text=normalize_text,
        )
        path_key = str(analysis.get("path_key") or "")
        cached = self.doc_plain_cache.get(path_key)
        if cached is not None:
            return cached
        plain = str(analysis.get("plain") or "")
        self.doc_plain_cache[path_key] = plain
        return plain

    def doc_source_priority(
        self,
        path_in: Path,
        *,
        path_cache_key: Callable[[Path], str],
        read_cached_doc_text: Callable[[Path], str],
        normalize_text: Callable[[Any], str],
    ) -> Tuple[int, str]:
        analysis = self.doc_analysis(
            path_in,
            path_cache_key=path_cache_key,
            read_cached_doc_text=read_cached_doc_text,
            normalize_text=normalize_text,
        )
        path_key = str(analysis.get("path_key") or "")
        cached = self.doc_source_priority_cache.get(path_key)
        if cached is not None:
            return cached
        name_low = str(analysis.get("name_low") or path_in.name.lower())
        if "ex99" in name_low or "press" in name_low or "earnings" in name_low:
            out = (100, "earnings_release")
        elif "ceo" in name_low or "letter" in name_low or "annualletter" in name_low or "shareholder" in name_low:
            out = (90, "ceo_letter")
        elif "slide" in name_low or "presentation" in name_low:
            out = (80, "slides")
        elif "10q" in name_low:
            out = (70, "10q_mdna")
        elif "10k" in name_low or "_pbi-" in name_low:
            out = (60, "10k_mdna")
        else:
            out = (50, "filing_doc")
        self.doc_source_priority_cache[path_key] = out
        return out

    def heading_regex(self, heading_terms: Tuple[str, ...]) -> Pattern[str]:
        cache_key = tuple(str(x) for x in heading_terms if str(x))
        cached = self.heading_regex_cache.get(cache_key)
        if cached is not None:
            return cached
        cached = re.compile("|".join(re.escape(x) for x in cache_key), re.I)
        self.heading_regex_cache[cache_key] = cached
        return cached

    def doc_heading_sentences(
        self,
        path_in: Path,
        heading_terms: Tuple[str, ...],
        *,
        before_chars: int,
        after_chars: int,
        path_cache_key: Callable[[Path], str],
        read_cached_doc_text: Callable[[Path], str],
        normalize_text: Callable[[Any], str],
        split_sentences: Callable[[Any], List[str]],
    ) -> List[str]:
        analysis = self.doc_analysis(
            path_in,
            path_cache_key=path_cache_key,
            read_cached_doc_text=read_cached_doc_text,
            normalize_text=normalize_text,
        )
        path_key = str(analysis.get("path_key") or "")
        terms_key = tuple(str(x) for x in heading_terms if str(x))
        cache_key = (path_key, terms_key, int(before_chars), int(after_chars))
        cached = self.heading_sentence_cache.get(cache_key)
        if cached is not None:
            return list(cached)
        plain = str(analysis.get("plain") or "")
        if not plain or not terms_key:
            self.heading_sentence_cache[cache_key] = []
            return []
        heading_re = self.heading_regex(terms_key)
        out: List[str] = []
        for mm in heading_re.finditer(plain):
            s0 = max(0, mm.start() - int(before_chars))
            e0 = min(len(plain), mm.start() + int(after_chars))
            out.extend([str(sent) for sent in (split_sentences(plain[s0:e0]) or [])])
        self.heading_sentence_cache[cache_key] = list(out)
        return list(out)

    def doc_cashflow_section_sentence_groups(
        self,
        path_in: Path,
        *,
        section_label: str,
        section_re: Pattern[str],
        stop_heading_re: Pattern[str],
        path_cache_key: Callable[[Path], str],
        read_cached_doc_text: Callable[[Path], str],
        normalize_text: Callable[[Any], str],
        split_sentences: Callable[[Any], List[str]],
    ) -> List[List[str]]:
        analysis = self.doc_analysis(
            path_in,
            path_cache_key=path_cache_key,
            read_cached_doc_text=read_cached_doc_text,
            normalize_text=normalize_text,
        )
        path_key = str(analysis.get("path_key") or "")
        cache_key = (path_key, section_label, str(section_re.pattern))
        cached = self.cashflow_section_sentence_cache.get(cache_key)
        if cached is not None:
            return [list(group) for group in cached]
        plain = str(analysis.get("plain") or "")
        if not plain:
            self.cashflow_section_sentence_cache[cache_key] = []
            return []
        groups: List[List[str]] = []
        for mm in section_re.finditer(plain):
            tail = plain[mm.end() : min(len(plain), mm.end() + 5000)]
            stop_mm = stop_heading_re.search(tail)
            if stop_mm is not None and int(stop_mm.start()) > 120:
                tail = tail[: int(stop_mm.start())]
            sentences = [str(sent) for sent in (split_sentences(tail) or [])]
            if sentences:
                groups.append(sentences)
        self.cashflow_section_sentence_cache[cache_key] = [list(group) for group in groups]
        return [list(group) for group in self.cashflow_section_sentence_cache[cache_key]]

    def doc_action_chunks(
        self,
        path_in: Path,
        *,
        path_cache_key: Callable[[Path], str],
        read_cached_doc_text: Callable[[Path], str],
        normalize_text: Callable[[Any], str],
        split_sentences: Callable[[Any], List[str]],
    ) -> List[str]:
        analysis = self.doc_analysis(
            path_in,
            path_cache_key=path_cache_key,
            read_cached_doc_text=read_cached_doc_text,
            normalize_text=normalize_text,
        )
        path_key = str(analysis.get("path_key") or "")
        cached = self.action_chunk_cache.get(path_key)
        if cached is not None:
            return list(cached)
        plain = str(analysis.get("plain") or "")
        if not plain:
            self.action_chunk_cache[path_key] = []
            return []
        chunks: List[str] = []
        base = split_sentences(plain) or [plain]
        for sent in base:
            sent_txt = str(sent or "")
            parts = self._bullet_splitter.split(sent_txt) if self._bullet_splitter.search(sent_txt) else [sent_txt]
            for part in parts:
                part_norm = normalize_text(part)
                if not part_norm:
                    continue
                chunks.append(part_norm)
        self.action_chunk_cache[path_key] = list(chunks)
        return list(chunks)

    def doc_matches_quarter(
        self,
        filing_row: Dict[str, Any],
        *,
        form: str,
        doc_path: Path,
        plain_text: str,
        quarter_end: date,
        path_cache_key: Callable[[Path], str],
        read_cached_doc_text: Callable[[Path], str],
        normalize_text: Callable[[Any], str],
        parse_date: Callable[[Any], Optional[date]],
    ) -> bool:
        doc_analysis = self.doc_analysis(
            doc_path,
            path_cache_key=path_cache_key,
            read_cached_doc_text=read_cached_doc_text,
            normalize_text=normalize_text,
        )
        cache_key = (
            self.filing_cache_key(filing_row),
            str(doc_analysis.get("path_key") or ""),
            str(form or "").upper(),
            str(quarter_end),
        )
        cached = self.doc_quarter_match_cache.get(cache_key)
        if cached is not None:
            return bool(cached)
        if self.filing_quarter_end(filing_row, parse_date=parse_date) == quarter_end:
            self.doc_quarter_match_cache[cache_key] = True
            return True
        if not str(form).upper().startswith("8-K"):
            self.doc_quarter_match_cache[cache_key] = False
            return False
        filed_d = parse_date(filing_row.get("filed"))
        if filed_d is None:
            self.doc_quarter_match_cache[cache_key] = False
            return False
        if filed_d < quarter_end or filed_d > (quarter_end + timedelta(days=150)):
            self.doc_quarter_match_cache[cache_key] = False
            return False
        q_num = ((int(quarter_end.month) - 1) // 3) + 1
        quarter_words = {1: "first", 2: "second", 3: "third", 4: "fourth"}
        plain_low = str(doc_analysis.get("plain_low") or str(plain_text or "").lower())
        doc_low = str(doc_analysis.get("name_low") or doc_path.name.lower())
        tokens = [
            f"q{q_num} {quarter_end.year}",
            f"q{q_num}{quarter_end.year}",
            f"{quarter_words.get(q_num, 'quarter')} quarter {quarter_end.year}",
            f"full year {quarter_end.year}",
            f"fy {quarter_end.year}",
            f"fiscal {quarter_end.year}",
        ]
        if any(tok in plain_low for tok in tokens):
            self.doc_quarter_match_cache[cache_key] = True
            return True
        if any(tok.replace(" ", "") in doc_low for tok in tokens):
            self.doc_quarter_match_cache[cache_key] = True
            return True
        if "earningspress" in doc_low or "earnings" in doc_low or "ex99" in doc_low:
            self.doc_quarter_match_cache[cache_key] = True
            return True
        self.doc_quarter_match_cache[cache_key] = False
        return False
