"""Promise Progress source/follow-through support helpers."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from .guidance_lexicon import normalize_text as glx_normalize_text
from .quarter_notes_lexicon import compact_snippet as qn_compact_snippet


@dataclass(frozen=True)
class PromiseProgressSourceDeps:
    material_roots: Sequence[Path]
    ticker: str
    ticker_roots: Sequence[Path]
    pdf_text_cache_root: Any
    rebuild_doc_text_cache: bool
    quiet_pdf_warnings: bool
    path_belongs_to_ticker: Callable[..., bool]
    extract_pdf_text_cached: Callable[..., str]
    strip_html: Callable[..., str]
    parse_quarter_from_filename: Callable[..., Any]
    parse_quarter_from_follow_text: Callable[..., Any]
    infer_quarter_end_from_text: Callable[..., Any]
    coerce_prev_quarter_end: Callable[..., Any]
    source_rank: Callable[..., Any]
    build_follow_through_candidate: Callable[..., Any]
    follow_candidate_sort_key: Callable[..., Any]
    read_cached_doc_raw: Callable[..., str]
    slide_text_paths: Callable[..., Any]
    parse_dollar_amount: Callable[..., Any]
    coerce_amount_with_unit: Callable[..., Any]
    coerce_amount_with_unit_local: Callable[..., Any]
    fmt_short_money_value: Callable[..., str]
    fmt_short_money_value_local: Callable[..., str]
    q_label: Callable[..., str]
    extract_45z_realized_progress_text: Callable[..., str]


class PromiseProgressSourceSupport:
    def __init__(self, deps: PromiseProgressSourceDeps) -> None:
        self.deps = deps
        self._cost_savings_follow_cache: Optional[List[Dict[str, Any]]] = None
        self._local_45z_realized_cache: Dict[date, str] = {}
        self._local_45z_period_records_cache: Optional[List[Dict[str, Any]]] = None
        self._local_45z_outcome_cache: Dict[str, Dict[str, Any]] = {}

    def extract_progress_latest_basis(self, metric_name: str, text_in: str) -> str:
        d = self.deps
        txt = glx_normalize_text(text_in)
        if not txt:
            return ""
        low = txt.lower()
        metric_low = str(metric_name or "").strip().lower()
        period_match = re.search(r"\b(FY\s*20\d{2}|Q[1-4]\s*20\d{2}|20\d{2})\b", txt, re.I)
        period_lbl = str(period_match.group(1) or "").upper().replace("  ", " ") if period_match else ""
        period_suffix = f" ({period_lbl})" if period_lbl else ""
        if re.search(r"\b45z\b|tax credit", metric_low, re.I):
            realized_45z = d.extract_45z_realized_progress_text(txt)
            if realized_45z:
                return realized_45z
        if re.search(r"\bsale completed\b", low, re.I):
            if "obion" in low:
                return f"Obion sale completed{period_suffix}"
            return f"Sale completed{period_suffix}"
        if re.search(r"\bagreement executed\b", low, re.I):
            if "45z" in low or "monetization" in low:
                return f"45Z agreement executed{period_suffix}"
            return f"Agreement executed{period_suffix}"
        if re.search(r"\bclass vi well permit\b", low, re.I):
            return f"Class VI permit received{period_suffix}"
        if re.search(r"\bexecuted construction management agreements?\b", low, re.I) and re.search(r"\bordered major equipment\b", low, re.I):
            return f"Agreements executed / equipment ordered{period_suffix}"
        if re.search(r"\bexecuted construction management agreements?\b", low, re.I):
            return f"Construction agreements executed{period_suffix}"
        if re.search(r"\bconstruction progressing\b", low, re.I):
            return f"Construction progressing{(' toward ' + period_lbl + ' start-up') if period_lbl else ''}".strip()
        if re.search(r"\b(fully operational|fully online)\b", low, re.I):
            if "york" in low:
                return f"York fully operational{period_suffix}"
            if "advantage nebraska" in low:
                return f"Advantage Nebraska fully operational{period_suffix}"
            return f"Plant fully operational{period_suffix}"
        if re.search(r"\bonline\b", low, re.I) and re.search(r"\b(ramping|capture|plant|system|facility)\b", low, re.I):
            if "central city" in low or "wood river" in low:
                return f"Central City/Wood River online and ramping{period_suffix}"
            return f"Plant online / ramping{period_suffix}"
        if re.search(r"\bonline\b", low, re.I) and re.search(r"\b(capture|system|facility)\b", low, re.I):
            return f"Capture system online{period_suffix}"
        if re.search(r"\bramping\b", low, re.I):
            if "central city" in low or "wood river" in low:
                return f"Central City/Wood River online and ramping{period_suffix}"
            return f"Plant online / ramping{period_suffix}"
        if re.search(r"\b(repaid|fully repay|used to fully repay)\b", low, re.I):
            return f"Debt repaid{period_suffix}"
        if re.search(r"\bexecuted on\b", low, re.I) and re.search(r"\b(initiative|program)\b", low, re.I):
            return f"Initiative launched{period_suffix}"

        m_range = re.search(
            r"\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?\s*(?:to|-)\s*\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?",
            txt,
            re.I,
        )
        if m_range:
            lo = d.coerce_amount_with_unit(m_range.group(1), m_range.group(2) or m_range.group(4))
            hi = d.coerce_amount_with_unit(m_range.group(3), m_range.group(4) or m_range.group(2))
            if lo is not None and hi is not None:
                range_txt = f"{d.fmt_short_money_value(lo)}-{d.fmt_short_money_value(hi)}"
                if re.search(r"\b(guidance|guide|expected|target|targets)\b", low, re.I):
                    return f"{range_txt} {period_lbl} guide".strip()
                return f"{range_txt} disclosed".strip()

        amt = d.parse_dollar_amount(txt)
        if amt is not None:
            amt_txt = d.fmt_short_money_value(amt)
            if re.search(r"\b(surpass(?:ed|ing)?|exceed(?:ed|ing)?|ahead of plan|above plan)\b", low, re.I) and re.search(r"\b(cost reduction|cost savings|annualized)\b", low, re.I):
                return f"On pace to exceed {amt_txt} target{(' (' + period_lbl + ')') if period_lbl else ''}".strip()
            if re.search(r"\b(realized|realised)\b", low, re.I) and re.search(r"\b(annualized|annualised)\b", low, re.I) and re.search(r"\b(savings|cost reduction)\b", low, re.I):
                return f"{amt_txt} realized{(' by ' + period_lbl) if period_lbl else ''}".strip()
            if re.search(r"\b(accomplished|achieved)\b", low, re.I) and re.search(r"\b(annualized|annualised)\b", low, re.I) and re.search(r"\b(savings|cost reduction)\b", low, re.I):
                return f"{amt_txt} annualized accomplished{(' as of ' + period_lbl) if period_lbl else ''}".strip()
            if re.search(r"\b(annualized|annualised)\b", low, re.I) and re.search(r"\b(savings|cost reduction)\b", low, re.I):
                return f"{amt_txt} annualized savings{(' as of ' + period_lbl) if period_lbl else ''}".strip()
            if "45z" in low and re.search(r"\b(opportunity|ebitda)\b", low, re.I):
                return f"{amt_txt} 45Z EBITDA opportunity{period_suffix}"
            if period_lbl:
                return f"{amt_txt} disclosed in {period_lbl}"
            if metric_low in {"debt reduction", "cost savings", "45z monetization / ebitda"}:
                return f"{amt_txt} disclosed"

        if re.search(r"\b(expected|planned|target(?:ed|ing)?|guidance|outlook|opportunity)\b", low, re.I) and period_lbl:
            return f"Expected in {period_lbl.replace('FY ', '').replace('Q', 'Q ')}".replace("Q ", "Q")
        return ""

    def evidence_time_label(self, text_in: str, qd: Optional[date]) -> str:
        txt = glx_normalize_text(text_in)
        month_match = re.search(
            r"\b(january|february|march|april|may|june|july|august|september|october|november|december|"
            r"jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)\.?\s+20\d{2}\b",
            txt,
            re.I,
        )
        if month_match:
            parts = str(month_match.group(0) or "").replace(".", "").split()
            if len(parts) == 2:
                month_map = {
                    "january": "Jan",
                    "jan": "Jan",
                    "february": "Feb",
                    "feb": "Feb",
                    "march": "Mar",
                    "mar": "Mar",
                    "april": "Apr",
                    "apr": "Apr",
                    "may": "May",
                    "june": "Jun",
                    "jun": "Jun",
                    "july": "Jul",
                    "jul": "Jul",
                    "august": "Aug",
                    "aug": "Aug",
                    "september": "Sep",
                    "sep": "Sep",
                    "sept": "Sep",
                    "october": "Oct",
                    "oct": "Oct",
                    "november": "Nov",
                    "nov": "Nov",
                    "december": "Dec",
                    "dec": "Dec",
                }
                month_lbl = month_map.get(parts[0].lower(), parts[0].title())
                return f"{month_lbl} {parts[1]}"
        if isinstance(qd, date):
            return self.deps.q_label(qd)
        return ""

    def promise_follow_source_dirs(self) -> List[Tuple[str, Path]]:
        d = self.deps
        dirs: List[Tuple[str, Path]] = []
        seen: set[str] = set()
        source_name_map = [
            ("earnings_release", ["earnings_release", "Earnings Release", "Earnings Releases", "press_release", "Press Release"]),
            ("earnings_presentation", ["earnings_presentation", "Earnings Presentation", "slides"]),
            ("transcript", ["earnings_transcripts", "Earnings Transcripts", "transcripts"]),
        ]
        for root in d.material_roots:
            for source_type, names in source_name_map:
                for name in names:
                    src_dir = root / name
                    if not src_dir.exists() or not src_dir.is_dir():
                        continue
                    if not d.path_belongs_to_ticker(src_dir, d.ticker, d.ticker_roots):
                        continue
                    try:
                        key = str(src_dir.resolve())
                    except Exception:
                        key = str(src_dir)
                    if key in seen:
                        continue
                    seen.add(key)
                    dirs.append((source_type, src_dir))
        return dirs

    def read_promise_follow_text(self, path_in: Path) -> str:
        d = self.deps
        suf = path_in.suffix.lower()
        try:
            if suf == ".txt":
                return path_in.read_text(encoding="utf-8", errors="ignore")
            if suf in {".htm", ".html"}:
                return d.strip_html(path_in.read_text(encoding="utf-8", errors="ignore"))
            if suf == ".pdf":
                return d.extract_pdf_text_cached(
                    path_in,
                    cache_root=d.pdf_text_cache_root,
                    rebuild_cache=d.rebuild_doc_text_cache,
                    quiet_pdf_warnings=d.quiet_pdf_warnings,
                )
        except Exception:
            return ""
        return ""

    def financial_statement_source_files(self) -> List[Path]:
        d = self.deps
        files: List[Path] = []
        seen: set[str] = set()
        for root in d.material_roots:
            fs_dir = root / "financial_statement"
            if not fs_dir.exists() or not fs_dir.is_dir():
                continue
            try:
                cand_files = sorted([p for p in fs_dir.iterdir() if p.is_file()])
            except Exception:
                continue
            for path_in in cand_files:
                if path_in.suffix.lower() not in {".pdf", ".txt", ".htm", ".html"}:
                    continue
                if not d.path_belongs_to_ticker(path_in, d.ticker, d.ticker_roots):
                    continue
                try:
                    key = str(path_in.resolve())
                except Exception:
                    key = str(path_in)
                if key in seen:
                    continue
                seen.add(key)
                files.append(path_in)
        return files

    def extract_local_45z_period_records_from_text(
        self,
        raw_txt: str,
        doc_qd: Optional[date],
        path_in: Path,
    ) -> List[Dict[str, Any]]:
        txt = glx_normalize_text(raw_txt)
        if not txt or not re.search(r"\b45z\b|production tax credits?", txt, re.I):
            return []
        date_pat = r"([A-Za-z]+\s+\d{1,2},\s+\d{4})"
        amount_pat = (
            r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?|[0-9]+(?:\.\d+)?)\s*"
            r"(million|billion|m|bn)\b"
        )
        period_months = {
            "three months": 3,
            "six months": 6,
            "nine months": 9,
            "year": 12,
        }
        patterns = [
            re.compile(
                rf"(?:based on production and ci scores for|for)\s+the\s+"
                rf"(year|three months|six months|nine months)\s+ended\s+{date_pat}"
                rf".{{0,260}}?(?:recorded|recognized)(?:\s+an)?\s+income tax benefit(?:\s+of)?\s+"
                rf"{amount_pat}.{{0,180}}?(?:section\s+)?45z\s+production\s+tax\s+credits",
                re.I,
            ),
            re.compile(
                rf"(?:recognized|recorded)\s+{amount_pat}.{{0,120}}?"
                rf"(?:year[- ]to[- ]date\s+)?income tax benefit(?:[^.{{}}]{{0,180}}?)?"
                rf"related to\s+(?:section\s+)?45z\s+production\s+tax\s+credits(?:[^.{{}}]{{0,220}}?)?"
                rf"during the\s+(year|three months|six months|nine months)\s+ended\s+{date_pat}",
                re.I,
            ),
        ]
        out_rows: List[Dict[str, Any]] = []
        seen_keys: set[Tuple[date, int, int]] = set()

        def _record(period_txt: str, date_txt: str, amt_txt: str, unit_txt: str, snippet: str) -> None:
            try:
                end_dt = datetime.strptime(str(date_txt), "%B %d, %Y").date()
            except Exception:
                return
            months = int(period_months.get(str(period_txt or "").strip().lower(), 0))
            if months <= 0:
                return
            amt = self.deps.coerce_amount_with_unit_local(amt_txt, unit_txt)
            if amt is None or amt <= 0:
                return
            dedupe_key = (end_dt, months, int(round(float(amt))))
            if dedupe_key in seen_keys:
                return
            seen_keys.add(dedupe_key)
            out_rows.append(
                {
                    "period_end": end_dt,
                    "months_covered": months,
                    "doc_quarter": doc_qd or end_dt,
                    "value": float(amt),
                    "source_doc": str(path_in),
                    "source_type": "financial_statement",
                    "text": qn_compact_snippet(snippet, 260),
                }
            )

        for pat in patterns:
            for match in pat.finditer(txt):
                if len(match.groups()) == 4:
                    period_txt, date_txt, amt_txt, unit_txt = match.groups()
                else:
                    amt_txt, unit_txt, period_txt, date_txt = match.groups()
                snippet = txt[max(0, match.start() - 40) : min(len(txt), match.end() + 80)]
                _record(period_txt, date_txt, amt_txt, unit_txt, snippet)
        return out_rows

    def load_local_45z_period_records(self) -> List[Dict[str, Any]]:
        d = self.deps
        if self._local_45z_period_records_cache is not None:
            return self._local_45z_period_records_cache
        records: List[Dict[str, Any]] = []
        for path_in in self.financial_statement_source_files():
            raw_txt = self.read_promise_follow_text(path_in)
            if not raw_txt or not re.search(r"\b45z\b|production tax credits?", raw_txt, re.I):
                continue
            qd_name = d.parse_quarter_from_filename(path_in.name)
            qd_text = d.parse_quarter_from_follow_text(raw_txt) or d.infer_quarter_end_from_text(raw_txt)
            doc_qd = qd_name or qd_text
            try:
                records.extend(self.extract_local_45z_period_records_from_text(raw_txt, doc_qd, path_in))
            except Exception:
                continue
        records.sort(
            key=lambda rec: (
                rec.get("period_end") if isinstance(rec.get("period_end"), date) else date.min,
                int(rec.get("months_covered") or 0),
                float(rec.get("value") or 0.0),
            )
        )
        self._local_45z_period_records_cache = records
        return records

    def load_local_45z_closed_period_outcome(self, target_qd: Optional[date]) -> Dict[str, Any]:
        d = self.deps
        if not isinstance(target_qd, date):
            return {}
        cache_key = str(target_qd.isoformat())
        if cache_key in self._local_45z_outcome_cache:
            return dict(self._local_45z_outcome_cache.get(cache_key) or {})
        records = self.load_local_45z_period_records()
        if not records:
            self._local_45z_outcome_cache[cache_key] = {}
            return {}
        q_num = int(((target_qd.month - 1) // 3) + 1)
        months_needed = q_num * 3
        current_rec: Optional[Dict[str, Any]] = None
        for rec in records:
            if rec.get("period_end") == target_qd and int(rec.get("months_covered") or 0) == months_needed:
                current_rec = rec
                break
        if current_rec is None and months_needed == 12:
            for rec in records:
                if rec.get("period_end") == target_qd and int(rec.get("months_covered") or 0) == 12:
                    current_rec = rec
                    break
        if current_rec is None:
            self._local_45z_outcome_cache[cache_key] = {}
            return {}
        prior_rec: Optional[Dict[str, Any]] = None
        if months_needed > 3:
            prior_candidates = [
                rec
                for rec in records
                if isinstance(rec.get("period_end"), date)
                and rec["period_end"].year == target_qd.year
                and rec["period_end"] < target_qd
                and int(rec.get("months_covered") or 0) < months_needed
            ]
            if prior_candidates:
                prior_rec = sorted(
                    prior_candidates,
                    key=lambda rec: (
                        int(rec.get("months_covered") or 0),
                        rec.get("period_end"),
                    ),
                    reverse=True,
                )[0]
        current_val = float(current_rec.get("value") or 0.0)
        if current_val <= 0:
            self._local_45z_outcome_cache[cache_key] = {}
            return {}
        if prior_rec is not None and float(prior_rec.get("value") or 0.0) >= 0:
            quarter_val = current_val - float(prior_rec.get("value") or 0.0)
            latest_txt = f"~{d.fmt_short_money_value_local(quarter_val)} realized in {d.q_label(target_qd)} (FY less {int(prior_rec.get('months_covered') or 0)}M)"
            detail_txt = (
                f"{d.q_label(target_qd)} implied 45Z outcome derived from "
                f"{d.fmt_short_money_value_local(current_val)} cumulative through {target_qd.isoformat()} "
                f"less {d.fmt_short_money_value_local(float(prior_rec.get('value') or 0.0))} cumulative through "
                f"{str(prior_rec.get('period_end') or '')}."
            )
        else:
            quarter_val = current_val
            latest_txt = f"{d.fmt_short_money_value_local(quarter_val)} realized in {d.q_label(target_qd)}"
            detail_txt = f"{d.fmt_short_money_value_local(current_val)} 45Z outcome for {d.q_label(target_qd)} from local financial statements."
        result = {
            "value": float(quarter_val),
            "latest": latest_txt,
            "quality": 5,
            "text": detail_txt,
            "quarter": target_qd,
            "source_doc": str(current_rec.get("source_doc") or ""),
            "source_type": "financial_statement",
            "derived": prior_rec is not None,
        }
        self._local_45z_outcome_cache[cache_key] = result
        return dict(result)

    def load_local_cost_savings_follow_candidates(self) -> List[Dict[str, Any]]:
        d = self.deps
        if self._cost_savings_follow_cache is not None:
            return self._cost_savings_follow_cache
        candidates: List[Dict[str, Any]] = []
        seen: set[Tuple[date, str]] = set()
        snippet_re = re.compile(
            r"[^.]{0,180}\b(cost savings|cost reduction|annualized savings|annualized cost reductions?)\b[^.]{0,260}(?:\.|$)",
            re.I,
        )
        for source_type, src_dir in self.promise_follow_source_dirs():
            files = sorted(
                [x for x in src_dir.iterdir() if x.is_file()],
                key=lambda x: x.stat().st_mtime if x.exists() else 0.0,
                reverse=True,
            )[:120]
            for path_in in files:
                if path_in.suffix.lower() not in {".pdf", ".txt", ".htm", ".html"}:
                    continue
                raw_txt = glx_normalize_text(self.read_promise_follow_text(path_in))
                if not raw_txt or not re.search(r"\b(cost savings|cost reduction|annualized savings|annualized cost)\b", raw_txt, re.I):
                    continue
                qd_name = d.parse_quarter_from_filename(path_in.name)
                qd_text = d.parse_quarter_from_follow_text(raw_txt)
                qd_file = qd_name
                if qd_text is not None and (qd_name is None or source_type in {"earnings_release", "earnings_presentation", "transcript"}):
                    qd_file = qd_text
                if qd_file is None:
                    try:
                        qd_file = d.coerce_prev_quarter_end(date.fromtimestamp(path_in.stat().st_mtime))
                    except Exception:
                        qd_file = None
                if qd_file is None:
                    continue
                for m in snippet_re.finditer(raw_txt):
                    snippet = qn_compact_snippet(str(m.group(0) or ""), 260)
                    if not snippet or not re.search(r"\$\s*\d", snippet):
                        continue
                    key = (qd_file, snippet.lower())
                    if key in seen:
                        continue
                    seen.add(key)
                    cand = d.build_follow_through_candidate(
                        qd_file,
                        "Cost savings",
                        "cost_savings",
                        "operational",
                        snippet,
                        source_type,
                        str(path_in),
                        "on_track",
                        "",
                        "",
                        50.0,
                        d.source_rank(source_type, str(path_in)),
                    )
                    if cand:
                        candidates.append(cand)
        self._cost_savings_follow_cache = sorted(candidates, key=d.follow_candidate_sort_key)
        return self._cost_savings_follow_cache

    def load_local_45z_realized_basis(self, qd_target: Optional[date]) -> str:
        d = self.deps
        if not isinstance(qd_target, date):
            return ""
        if qd_target in self._local_45z_realized_cache:
            return self._local_45z_realized_cache[qd_target]
        best_txt = ""
        best_rank = 99

        def _line_level_45z_basis(raw_txt: str) -> str:
            lines = [glx_normalize_text(x) for x in str(raw_txt or "").splitlines() if glx_normalize_text(x)]
            amount_re = re.compile(
                r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\b",
                re.I,
            )

            def _fmt_amt_from_text(txt_in: str) -> str:
                for mm in amount_re.finditer(txt_in):
                    amt = d.coerce_amount_with_unit_local(mm.group(1), mm.group(2))
                    if amt is None:
                        continue
                    return f"{d.fmt_short_money_value_local(amt)} 45Z value realized"
                return ""

            for idx, line in enumerate(lines):
                low = line.lower()
                if not re.search(r"\b45z\b|production tax credits?", low, re.I):
                    continue
                if re.search(r"\b(processed|bushels|gallons|decommissioning|operating income|depreciation|intercompany|distillers grains)\b", low, re.I):
                    continue
                if re.search(r"\binclusive of 45z production tax credits?\b", low, re.I):
                    amt_match = re.search(
                        r"\binclusive of 45z production tax credits?\s+of\s+\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)\b",
                        line,
                        re.I,
                    )
                    if amt_match:
                        amt = d.coerce_amount_with_unit_local(amt_match.group(1), amt_match.group(2))
                        if amt is not None:
                            return f"{d.fmt_short_money_value_local(amt)} 45Z value realized"
                if re.search(r"\b45z production tax credits?\b", low, re.I):
                    amt_txt = _fmt_amt_from_text(line)
                    if amt_txt:
                        if idx + 1 < len(lines) and re.search(r"\bincome tax benefit\b", lines[idx + 1], re.I):
                            return amt_txt
                        return amt_txt
            return ""

        def _maybe_take_candidate(raw_txt: str, src_rank: int) -> None:
            nonlocal best_txt, best_rank
            latest_txt = d.extract_45z_realized_progress_text(raw_txt, qd_target) or _line_level_45z_basis(raw_txt)
            if latest_txt and src_rank < best_rank:
                best_rank = src_rank
                best_txt = latest_txt

        for source_type, src_dir in self.promise_follow_source_dirs():
            if source_type not in {"earnings_release", "earnings_presentation", "transcript"}:
                continue
            try:
                files = sorted(
                    [x for x in src_dir.iterdir() if x.is_file()],
                    key=lambda x: x.stat().st_mtime if x.exists() else 0.0,
                    reverse=True,
                )[:120]
            except Exception:
                continue
            for path_in in files:
                if path_in.suffix.lower() not in {".pdf", ".txt", ".htm", ".html"}:
                    continue
                raw_txt = glx_normalize_text(self.read_promise_follow_text(path_in))
                if not raw_txt or not re.search(r"\b45z\b|production tax credits?", raw_txt, re.I):
                    continue
                qd_name = d.parse_quarter_from_filename(path_in.name)
                qd_text = d.parse_quarter_from_follow_text(raw_txt)
                qd_file = qd_text if qd_text is not None else qd_name
                if qd_file != qd_target:
                    continue
                _maybe_take_candidate(raw_txt, d.source_rank(source_type, str(path_in)))
        for slide_kind in ("text", "ocr"):
            for path_in in d.slide_text_paths(kind=slide_kind, quarter=qd_target):
                raw_txt = glx_normalize_text(d.read_cached_doc_raw(path_in))
                if not raw_txt or not re.search(r"\b45z\b|production tax credits?", raw_txt, re.I):
                    continue
                _maybe_take_candidate(raw_txt, 1 if slide_kind == "text" else 2)
        self._local_45z_realized_cache[qd_target] = best_txt
        return best_txt
