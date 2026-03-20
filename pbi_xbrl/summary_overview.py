from __future__ import annotations

import html
import re
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from .company_profiles import get_company_profile
from .debt_parser import coerce_number, read_html_tables_any
from .guidance_lexicon import dedup_text_key as glx_dedup_text_key
from .non_gaap import find_ex99_docs
from .sec_xbrl import SecClient, normalize_accession, parse_date, strip_html

_NOISE_RE = re.compile(
    r"\b(table of contents|part\s+[ivx]+|item\s*\d+[a-z]?\.?|forward-looking statements|"
    r"safe harbor|private securities litigation reform act|market for common stock|"
    r"controls and procedures|xbrl|instance document|schema|linkbase|commission file number|"
    r"washington, d\.c\.|exact name of registrant|current report)\b",
    re.I,
)
_SOURCE_BLOCK_RE = re.compile(
    r"\b(proxy|def 14a|s-1|s-3|s-4|registration statement|exhibit index|exhibits? list|"
    r"officer appointment|director appointment|compensation committee)\b",
    re.I,
)
_GLOSSARY_RE = re.compile(r"\bmeans\b|\brefers to\b|\bdefined as\b", re.I)
_ADMIN_AMEND_RE = re.compile(
    r"(filed solely for the purpose of correcting|solely to correct|office location|auditor office|"
    r"currently[- ]dated certifications only|consent correction|clerical correction|administrative amendment)",
    re.I,
)
_BUSINESS_VERB_RE = re.compile(
    r"\b(provide|provides|offer|offers|sell|sells|serve|serves|enable|enables|support|supports|"
    r"operate|operates|process|processes|sort|sorts|produce|produces|convert|converts|market|markets|"
    r"handle|handles|distribute|distributes|combine|combines)\b",
    re.I,
)
_CONTEXT_VERB_RE = re.compile(
    r"\b(expect|expects|focus|focused|priorit|positioned|drive|driving|improv|monetiz|market|marketing|"
    r"advance|advancing|reduce|reducing|allocate|allocation|discipl|execute|execution|strengthen|commercial)\b",
    re.I,
)
_ADVANTAGE_RE = re.compile(
    r"\b(competitive|advantage|scale|scaled|installed base|workflow|network|retention|recurring|"
    r"financing|open architecture|platform|cost position|integrated|low carbon|carbon intensity|"
    r"co-product|protein|corn oil|biorefin)\b",
    re.I,
)
_PRESS_RELEASE_DOC_RE = re.compile(r"(earnings(?:press)?releas|pressrelea|resultsrelease|newsrelease)", re.I)
_CEO_LETTER_DOC_RE = re.compile(r"(ceoletter|shareholderletter|stockholderletter|investorletter|letter)", re.I)
_GENERIC_PRESS_LEDE_RE = re.compile(
    r"\b(today announced|announced (?:its|their) (?:financial results|results)|reported (?:its|their) results)\b",
    re.I,
)
_NARROW_FINANCE_COMP_RE = re.compile(
    r"\b(financing operations|leasing companies|commercial finance companies|commercial banks|"
    r"large, diversified financial institutions)\b",
    re.I,
)


def _iter_submission_batches_local(sec: SecClient, submissions: Dict[str, Any]) -> List[Dict[str, Any]]:
    def _coerce_batch(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(data, dict):
            return None
        if "filings" in data and isinstance(data.get("filings"), dict):
            recent = data.get("filings", {}).get("recent", {})
            if recent:
                return recent
        if "accessionNumber" in data and "form" in data:
            return data
        return None

    batches: List[Dict[str, Any]] = []
    base = _coerce_batch(submissions)
    if base:
        batches.append(base)

    files = submissions.get("filings", {}).get("files", []) or []
    for file_meta in files:
        name = file_meta.get("name")
        if not name:
            continue
        url = f"https://data.sec.gov/submissions/{name}"
        try:
            data = sec.get(url, as_json=True, cache_key=f"submissions_{name}")
        except Exception:
            continue
        rec = _coerce_batch(data)
        if rec:
            batches.append(rec)
    return batches


def _default_overview_output() -> Dict[str, Any]:
    return {
        "what_it_does": "N/A",
        "what_it_does_source": "Source: N/A (topic-aware overview source not found)",
        "current_strategic_context": "N/A",
        "current_strategic_context_source": "Source: N/A (current strategic context not found)",
        "key_advantage": "N/A",
        "key_advantage_source": "Source: N/A (competitive advantage source not found)",
        "segment_operating_model": [],
        "segment_operating_model_source": "Source: N/A (segment operating model not found)",
        "key_dependencies": [],
        "key_dependencies_source": "Source: N/A (risk dependency themes not found)",
        "wrong_thesis_bullets": [],
        "wrong_thesis_source": "Source: N/A (wrong-thesis translation not found)",
        "revenue_streams": [],
        "revenue_streams_source": "Source: N/A (revenue stream table not parsed)",
        "asof_fy_end": None,
        "revenue_streams_period": None,
        "source_manifest": {},
    }


def _norm_text(text_in: Any) -> str:
    return re.sub(r"\s+", " ", html.unescape(str(text_in or ""))).strip()


def _word_count(text_in: Any) -> int:
    return len(re.findall(r"\b[\w\-]+\b", str(text_in or "")))


def _split_sentences(text_in: Any) -> List[str]:
    txt = _norm_text(text_in)
    if not txt:
        return []
    parts = re.split(r"(?<=[\.\!\?])\s+(?=[A-Z0-9])", txt)
    out: List[str] = []
    for part in parts:
        sent = _norm_text(part)
        if not sent:
            continue
        if len(sent) < 35 or len(sent) > 460:
            continue
        out.append(sent)
    return out


def _contains_any(text_in: str, terms: Iterable[str]) -> bool:
    low_txt = str(text_in or "").lower()
    return any(str(term).lower() in low_txt for term in terms)


def _looks_noise_sentence(text_in: Any) -> bool:
    txt = _norm_text(text_in)
    if not txt:
        return True
    low_txt = txt.lower()
    if _NOISE_RE.search(low_txt) or _SOURCE_BLOCK_RE.search(low_txt):
        return True
    if _GLOSSARY_RE.search(low_txt):
        return True
    if txt.count("|") >= 2:
        return True
    if re.match(r"^[A-Z0-9\W_]{12,}$", txt):
        return True
    return False


def _sector_pack_keys_for_profile(profile: Any, ticker_u: str) -> Tuple[str, ...]:
    blob = " ".join(
        [
            ticker_u,
            " ".join(str(x) for x in getattr(profile, "industry_keywords", tuple()) or tuple()),
            " ".join(str(x) for x in getattr(profile, "quarter_note_priority_terms", tuple()) or tuple()),
            " ".join(str(x) for x in getattr(profile, "promise_priority_terms", tuple()) or tuple()),
        ]
    ).lower()
    ordered: List[str] = []
    if re.search(r"\b(ethanol|corn oil|protein|45z|carbon capture|ccs|low carbon|biorefin)\b", blob, re.I):
        ordered.append("biofuels")
    if re.search(r"\b(presort|sendtech|mail|shipping|postal|buyback|dividend|deleverag|capital allocation)\b", blob, re.I):
        ordered.append("industrial_capital_return")
    if re.search(r"\b(convertible|notes due|refinanc|subscription|capped call|proceeds)\b", blob, re.I):
        ordered.append("capital_markets")
    return tuple(dict.fromkeys(ordered))


def _clean_source_note_rows(rows_in: Sequence[Dict[str, Any]], detail: str) -> str:
    clean_bits: List[str] = []
    seen: set[str] = set()
    for row_in in rows_in:
        if not isinstance(row_in, dict):
            continue
        form_txt = str(row_in.get("form") or row_in.get("base_form") or "doc")
        accn_txt = str(row_in.get("accn") or "n/a")
        filed_txt = str(row_in.get("filed") or "n/a")
        role_txt = str(row_in.get("doc_role_label") or row_in.get("doc_role") or "").strip()
        note_txt = f"SEC {form_txt} accn={accn_txt} filed={filed_txt}"
        if role_txt:
            note_txt = f"{note_txt} ({role_txt})"
        if note_txt not in seen:
            seen.add(note_txt)
            clean_bits.append(note_txt)
    if not clean_bits:
        return f"Source: N/A ({detail})"
    if len(clean_bits) == 1:
        return f"Source: {clean_bits[0]} ({detail})"
    return f"Source: {' + '.join(clean_bits)} ({detail})"


def build_company_overview(
    sec: SecClient,
    cik_int: int,
    submissions: Dict[str, Any],
    *,
    ticker: Optional[str] = None,
) -> Dict[str, Any]:
    company_name = str((submissions or {}).get("name") or (ticker or "The company")).strip()
    profile = get_company_profile(ticker)
    ticker_u = str(getattr(profile, "ticker", "") or ticker or "").strip().upper()
    is_pbi_profile = ticker_u == "PBI"
    sector_packs = _sector_pack_keys_for_profile(profile, ticker_u)
    annual_segment_labels = tuple(str(x) for x in (getattr(profile, "annual_segment_labels", tuple()) or tuple()) if str(x).strip())
    annual_segment_alias_patterns = tuple(getattr(profile, "annual_segment_alias_patterns", tuple()) or tuple())
    segment_alias_patterns = tuple(getattr(profile, "segment_alias_patterns", tuple()) or tuple())
    out = _default_overview_output()

    def canonical_segment_display_name(text_in: Any) -> str:
        txt = _norm_text(text_in)
        if not txt:
            return ""
        for pat, label in annual_segment_alias_patterns:
            try:
                if pat.search(txt):
                    return str(label)
            except Exception:
                continue
        for pat, label in segment_alias_patterns:
            try:
                if pat.search(txt):
                    base_label = str(label)
                    for annual_label in annual_segment_labels:
                        low_annual = _norm_text(annual_label).lower()
                        low_base = _norm_text(base_label).lower()
                        if low_annual == low_base or low_annual.startswith(f"{low_base} "):
                            return annual_label
                    return base_label
            except Exception:
                continue
        low_txt = _norm_text(txt).lower()
        for annual_label in annual_segment_labels:
            low_annual = _norm_text(annual_label).lower()
            if low_txt == low_annual or low_annual.startswith(f"{low_txt} "):
                return annual_label
        return txt

    def clean_summary_sentence(text_in: Any) -> str:
        txt = _norm_text(text_in)
        if not txt:
            return ""
        txt = re.sub(r"^(overview|business|general)\s+", "", txt, flags=re.I)
        txt = re.sub(r"^incorporated in [A-Za-z][A-Za-z ,\.\-&]{0,80},\s*", "", txt, flags=re.I)
        txt = re.sub(r"^[A-Z][A-Za-z\.\- ]{2,40},\s+[A-Z][a-z]{2,9}\.?\s+\d{1,2},\s+\d{4}\s+(?:/PRNewswire/|--)\s*", "", txt)
        if company_name:
            txt = re.sub(
                rf"^\s*{re.escape(company_name)}\s*\((?:NASDAQ|NYSE|AMEX)[^)]*\)\s*",
                f"{company_name} ",
                txt,
                flags=re.I,
            )
        txt = re.sub(r"\s+", " ", txt).strip(" -;:,")
        if txt and txt[-1] not in ".!?":
            txt = f"{txt}."
        return txt

    def normalized_sentences(text_in: Any) -> List[str]:
        out_sents: List[str] = []
        seen: set[str] = set()
        for sent in _split_sentences(text_in):
            clean = clean_summary_sentence(sent)
            if not clean or _looks_noise_sentence(clean):
                continue
            key = glx_dedup_text_key(clean)
            if key in seen:
                continue
            seen.add(key)
            out_sents.append(clean)
        return out_sents

    def join_phrases(parts: Sequence[str]) -> str:
        clean_parts = [str(part).strip() for part in parts if str(part).strip()]
        if not clean_parts:
            return ""
        if len(clean_parts) == 1:
            return clean_parts[0]
        if len(clean_parts) == 2:
            return f"{clean_parts[0]} and {clean_parts[1]}"
        return f"{', '.join(clean_parts[:-1])}, and {clean_parts[-1]}"

    def looks_company_description_sentence(sent: Any) -> bool:
        txt = clean_summary_sentence(sent)
        low_txt = txt.lower()
        if not txt:
            return False
        return bool(
            re.search(r"\bis\s+an?\b", low_txt)
            and (
                _contains_any(low_txt, getattr(profile, "industry_keywords", tuple()) or tuple())
                or "technology-driven company" in low_txt
                or "renewable fuels" in low_txt
                or "shipping and mailing" in low_txt
            )
        )

    filing_rows: List[Dict[str, Any]] = []
    for batch in _iter_submission_batches_local(sec, submissions):
        forms = batch.get("form", []) or []
        accns = batch.get("accessionNumber", []) or []
        report_dates = batch.get("reportDate", []) or []
        filing_dates = batch.get("filingDate", []) or []
        primary_docs = batch.get("primaryDocument", []) or []
        n = min(len(forms), len(accns))
        for i in range(n):
            form = str(forms[i] or "").upper().strip()
            if form not in {"10-K", "10-K/A", "10-Q", "10-Q/A", "8-K", "8-K/A"}:
                continue
            accn = str(accns[i] or "").strip()
            doc = str(primary_docs[i] or "") if i < len(primary_docs) else ""
            if not accn or not doc:
                continue
            filing_rows.append(
                {
                    "form": form,
                    "base_form": form.replace("/A", ""),
                    "accn": accn,
                    "doc": doc,
                    "filed": parse_date(filing_dates[i]) if i < len(filing_dates) else None,
                    "report": parse_date(report_dates[i]) if i < len(report_dates) else None,
                    "is_amendment": form.endswith("/A"),
                    "doc_role": "primary",
                    "doc_role_label": "primary filing",
                }
            )
    if not filing_rows:
        return out

    filing_rows = sorted(
        filing_rows,
        key=lambda row: (
            pd.Timestamp(row.get("filed") or "1900-01-01"),
            pd.Timestamp(row.get("report") or "1900-01-01"),
        ),
        reverse=True,
    )

    doc_cache: Dict[Tuple[str, str], Dict[str, Any]] = {}
    admin_cache: Dict[Tuple[str, str], bool] = {}
    exhibit_cache: Optional[List[Dict[str, Any]]] = None
    source_manifest: Dict[str, Any] = {}

    def load_doc_bundle(row: Dict[str, Any]) -> Dict[str, Any]:
        key = (str(row.get("accn") or ""), str(row.get("doc") or ""))
        if key in doc_cache:
            return doc_cache[key]
        try:
            data = sec.download_document(cik_int, normalize_accession(key[0]), key[1])
        except Exception:
            data = b""
        html_txt = data.decode("utf-8", errors="ignore") if data else ""
        plain_txt = _norm_text(strip_html(html_txt))
        bundle = {"bytes": data, "plain": plain_txt, "low": plain_txt.lower(), "html": html_txt}
        doc_cache[key] = bundle
        return bundle

    def is_administrative_amendment(row: Dict[str, Any]) -> bool:
        if not bool(row.get("is_amendment")):
            return False
        key = (str(row.get("accn") or ""), str(row.get("doc") or ""))
        if key not in admin_cache:
            preview = str(load_doc_bundle(row).get("plain") or "")[:8000]
            admin_cache[key] = bool(_ADMIN_AMEND_RE.search(preview))
        return bool(admin_cache[key])

    def classify_ex99_doc(base_row: Dict[str, Any], doc_name: str) -> Dict[str, Any]:
        nm = str(doc_name or "").lower()
        role = "ex99_other"
        role_label = "EX-99 exhibit"
        if re.search(r"(?:^|[^0-9])99[\-_. ]?1(?:[^0-9]|$)|ex99[\-_. ]?1", nm) or _PRESS_RELEASE_DOC_RE.search(nm):
            role = "earnings_release"
            role_label = "EX-99.1 earnings release"
        elif re.search(r"(?:^|[^0-9])99[\-_. ]?2(?:[^0-9]|$)|ex99[\-_. ]?2", nm) or _CEO_LETTER_DOC_RE.search(nm):
            role = "ceo_letter"
            role_label = "EX-99.2 CEO letter"
        return {**base_row, "doc": doc_name, "doc_role": role, "doc_role_label": role_label}

    def expand_recent_8k_exhibits(max_filings: int = 10) -> List[Dict[str, Any]]:
        nonlocal exhibit_cache
        if exhibit_cache is not None:
            return exhibit_cache
        rows_8k = [row for row in filing_rows if str(row.get("base_form") or "") == "8-K"][:max_filings]
        expanded: List[Dict[str, Any]] = []
        for row in rows_8k:
            expanded.append(row)
            try:
                idx = sec.accession_index_json(cik_int, normalize_accession(str(row.get("accn") or "")))
            except Exception:
                continue
            for ex_doc in find_ex99_docs(idx):
                expanded.append(classify_ex99_doc(row, ex_doc))
        exhibit_cache = expanded
        return expanded

    def extract_item_slice(
        plain_txt: str,
        start_patterns: Sequence[str],
        stop_patterns: Sequence[str],
        *,
        min_pos: int = 0,
        fallback_chars: int = 12000,
    ) -> str:
        low_txt = str(plain_txt or "").lower()
        starts: List[re.Match[str]] = []
        for pat in start_patterns:
            starts.extend(list(re.finditer(pat, low_txt, re.I)))
        if not starts:
            return _norm_text(plain_txt[:fallback_chars])
        stops: List[re.Match[str]] = []
        for pat in stop_patterns:
            stops.extend(list(re.finditer(pat, low_txt, re.I)))
        later = [m for m in starts if int(m.start()) >= int(min_pos)]
        start_candidates = later or starts
        for start_match in start_candidates:
            start_pos = int(start_match.start())
            stop_pos = min(len(plain_txt), start_pos + int(fallback_chars))
            later_stops = [m for m in stops if int(m.start()) > int(start_pos + 100)]
            if later_stops:
                stop_pos = min(stop_pos, int(later_stops[0].start()))
            candidate_txt = _norm_text(plain_txt[start_pos:stop_pos])
            if len(candidate_txt) < 400:
                continue
            low_candidate = candidate_txt.lower()
            toc_like = bool(
                re.search(r"\btable of contents\b|\bpage number\b", low_candidate)
                or len(re.findall(r"\bitem\b", candidate_txt, re.I)) >= 5
            )
            if toc_like and len(start_candidates) > 1:
                continue
            return candidate_txt
        start_pos = int(start_candidates[-1].start())
        stop_pos = min(len(plain_txt), start_pos + int(fallback_chars))
        later_stops = [m for m in stops if int(m.start()) > int(start_pos + 100)]
        if later_stops:
            stop_pos = min(stop_pos, int(later_stops[0].start()))
        return _norm_text(plain_txt[start_pos:stop_pos])

    def business_section(row: Dict[str, Any]) -> Tuple[str, str]:
        plain_txt = str(load_doc_bundle(row).get("plain") or "")
        if str(row.get("base_form") or "") == "10-K":
            return (
                extract_item_slice(
                    plain_txt,
                    (r"\bitem\s*1\.?\s*business\b",),
                    (r"\bitem\s*1a\.?\b", r"\bitem\s*2\.?\b"),
                    min_pos=3500,
                    fallback_chars=18000,
                ),
                "10-K Item 1",
            )
        if str(row.get("base_form") or "") == "10-Q":
            return (
                extract_item_slice(
                    plain_txt,
                    (r"\bitem\s*2\.?\s*management'?s discussion\b", r"\bmanagement'?s discussion and analysis\b"),
                    (r"\bitem\s*3\.?\b", r"\bquantitative and qualitative disclosures\b", r"\bitem\s*4\.?\b"),
                    min_pos=3500,
                    fallback_chars=9000,
                ),
                "10-Q MD&A/business overview",
            )
        about_match = re.search(rf"\babout\s+{re.escape(company_name.lower())}\b", plain_txt.lower())
        if about_match:
            seg = plain_txt[about_match.start() : min(len(plain_txt), about_match.start() + 1600)]
            return _norm_text(seg), "8-K About section"
        return _norm_text(plain_txt[:4000]), "8-K narrative fallback"

    def competition_section(row: Dict[str, Any]) -> Tuple[str, str]:
        plain_txt = str(load_doc_bundle(row).get("plain") or "")
        if str(row.get("base_form") or "") == "10-K":
            comp = extract_item_slice(
                plain_txt,
                (r"\bcompetition\b",),
                (r"\bregulation\b", r"\bgovernment regulation\b", r"\bemployees\b", r"\bitem\s*1a\.?\b"),
                min_pos=4000,
                fallback_chars=4500,
            )
            if comp and "competition" in comp.lower():
                return comp, "10-K Competition"
        return business_section(row)

    def risk_section(row: Dict[str, Any]) -> Tuple[str, str]:
        plain_txt = str(load_doc_bundle(row).get("plain") or "")
        if str(row.get("base_form") or "") == "10-K":
            return (
                extract_item_slice(
                    plain_txt,
                    (r"\bitem\s*1a\.?\s*risk\s+factors?\b", r"\brisk factors\b"),
                    (r"\bitem\s*1b\.?\b", r"\bitem\s*2\.?\b"),
                    min_pos=4500,
                    fallback_chars=22000,
                ),
                "10-K Item 1A",
            )
        if str(row.get("base_form") or "") == "10-Q":
            return (
                extract_item_slice(
                    plain_txt,
                    (r"\bitem\s*1a\.?\s*risk\s+factors?\b", r"\brisk factors\b"),
                    (r"\bitem\s*2\.?\b", r"\bitem\s*3\.?\b"),
                    min_pos=3500,
                    fallback_chars=12000,
                ),
                "10-Q risk-factor update",
            )
        return "", "n/a"

    def mda_opening(row: Dict[str, Any]) -> Tuple[str, str]:
        plain_txt = str(load_doc_bundle(row).get("plain") or "")
        if str(row.get("base_form") or "") != "10-Q":
            return "", "n/a"
        return (
            extract_item_slice(
                plain_txt,
                (r"\bitem\s*2\.?\s*management'?s discussion\b", r"\bmanagement'?s discussion and analysis\b"),
                (r"\bresults of operations\b", r"\bliquidity and capital resources\b", r"\bitem\s*3\.?\b"),
                min_pos=3500,
                fallback_chars=5000,
            ),
            "10-Q MD&A opening",
        )

    def segment_hits(text_in: Any) -> int:
        hits = 0
        txt = str(text_in or "")
        for _seg_name, seg_re in list(getattr(profile, "segment_patterns", tuple()) or tuple()):
            try:
                if seg_re.search(txt):
                    hits += 1
            except Exception:
                continue
        return hits

    def business_sentence_score(sent: str) -> int:
        txt = clean_summary_sentence(sent)
        low_txt = txt.lower()
        if _looks_noise_sentence(txt):
            return -999
        if not _BUSINESS_VERB_RE.search(txt):
            return -50
        score = 0
        score += 3 if company_name and company_name.lower() in low_txt else 0
        score += 2 if _contains_any(low_txt, getattr(profile, "industry_keywords", tuple()) or tuple()) else 0
        score += min(3, segment_hits(txt))
        score += 1 if re.search(r"\b(segment|operates through|businesses|solutions|services|bank|financing)\b", low_txt, re.I) else 0
        wc = _word_count(txt)
        if 12 <= wc <= 40:
            score += 2
        elif 41 <= wc <= 55:
            score += 1
        return score

    def current_sentence_score(sent: str) -> int:
        txt = clean_summary_sentence(sent)
        low_txt = txt.lower()
        if _looks_noise_sentence(txt):
            return -999
        if re.search(r"\babout\b", low_txt) and company_name.lower() in low_txt:
            return -200
        if looks_company_description_sentence(txt):
            return -180
        if re.search(r"\b(nasdaq|nyse|headquartered|based in|located in)\b", low_txt) and not re.search(
            r"\b(45z|ccs|carbon capture|low-carbon|capital allocation|debt reduction|cost discipline|execution|outlook|guidance|commercializ|monetiz)\b",
            low_txt,
        ):
            return -160
        if _GENERIC_PRESS_LEDE_RE.search(low_txt) and not re.search(
            r"\b(45z|ccs|carbon capture|low-carbon|capital allocation|debt reduction|cost discipline|execution|outlook|guidance|commercializ|monetiz|liquidity)\b",
            low_txt,
        ):
            return -120
        if not _CONTEXT_VERB_RE.search(txt):
            return -50
        score = 0
        score += 2 if re.search(r"\b(2026|next quarter|next quarters|full year|outlook|guidance)\b", low_txt) else 0
        score += 2 if re.search(r"\b(capital allocation|debt reduction|deleverag|cost discipline|execution|45z|ccs|carbon capture|low-carbon|commercializ|monetiz|liquidity)\b", low_txt) else 0
        score += 1 if re.search(r"\b(margin|cash flow|profitability|balance sheet|interest expense)\b", low_txt) else 0
        if "biofuels" in sector_packs and re.search(r"\b(45z|ccs|carbon capture|low-carbon|ci|protein|corn oil)\b", low_txt):
            score += 2
        if "industrial_capital_return" in sector_packs and re.search(r"\b(capital allocation|debt reduction|cost discipline|execution|sendtech|presort)\b", low_txt):
            score += 2
        wc = _word_count(txt)
        if 10 <= wc <= 34:
            score += 2
        elif 35 <= wc <= 50:
            score += 1
        return score

    def advantage_sentence_score(sent: str) -> int:
        txt = clean_summary_sentence(sent)
        low_txt = txt.lower()
        if _looks_noise_sentence(txt):
            return -999
        if not _ADVANTAGE_RE.search(txt):
            return -50
        score = 0
        score += 2 if re.search(r"\b(customer|client|retention|recurring|installed base|network|workflow|financing)\b", low_txt) else 0
        score += 2 if re.search(r"\b(platform|biorefin|low carbon|carbon intensity|co-products?|corn oil|protein|open architecture)\b", low_txt) else 0
        score += 2 if re.search(r"\b(operational excellence|cost leadership|positioned to benefit|streamlined platform|disciplined capital allocation)\b", low_txt) else 0
        score += min(
            3,
            sum(
                1
                for term in tuple(getattr(profile, "key_adv_require_keywords", tuple()) or tuple())
                if term and str(term).lower() in low_txt
            ),
        )
        if _NARROW_FINANCE_COMP_RE.search(low_txt) and not re.search(
            r"\b(installed base|workflow|network|retention|recurring|software|open architecture|presort|shipping|mailing|scale)\b",
            low_txt,
        ):
            score -= 8
        if re.search(r"\bsegment includes\b|\bour ethanol production segment includes\b|\bour agribusiness and energy services segment includes\b", low_txt):
            score -= 6
        if re.search(r"\b(msc|ultra-high protein)\b", low_txt) and not re.search(r"\b(platform|low carbon|co-products?|corn oil|ethanol|network|installed base)\b", low_txt):
            score -= 2
        wc = _word_count(txt)
        if 10 <= wc <= 34:
            score += 2
        elif 35 <= wc <= 45:
            score += 1
        return score

    def candidate_rows_for_topic(topic: str) -> List[Dict[str, Any]]:
        candidates: List[Dict[str, Any]] = []
        if topic in {"what_it_does", "segment_operating_model", "key_advantage", "revenue_streams", "key_dependencies"}:
            candidates.extend(filing_rows)
        if topic == "current_strategic_context":
            exhibit_rows = [
                row
                for row in expand_recent_8k_exhibits()
                if str(row.get("doc_role") or "") in {"earnings_release", "ceo_letter"}
            ]
            candidates.extend(exhibit_rows or expand_recent_8k_exhibits())
            candidates.extend([row for row in filing_rows if str(row.get("base_form") or "") == "10-Q"][:3])
        if topic == "what_it_does":
            candidates.extend(expand_recent_8k_exhibits()[:4])

        def score(row: Dict[str, Any]) -> Tuple[int, pd.Timestamp, pd.Timestamp]:
            base_form = str(row.get("base_form") or "")
            doc_role = str(row.get("doc_role") or "primary")
            filed_ts = pd.Timestamp(row.get("filed") or "1900-01-01")
            report_ts = pd.Timestamp(row.get("report") or "1900-01-01")
            sc = 0
            if topic in {"what_it_does", "segment_operating_model"}:
                sc += 120 if base_form == "10-K" else 90 if base_form == "10-Q" else 20 if base_form == "8-K" and doc_role == "earnings_release" else -40
            elif topic == "key_advantage":
                sc += 125 if base_form == "10-K" else 80 if base_form == "10-Q" else 30 if base_form == "8-K" and doc_role in {"earnings_release", "ceo_letter"} else -35
            elif topic == "revenue_streams":
                sc += 120 if base_form == "10-K" else 95 if base_form == "10-Q" else -100
            elif topic == "current_strategic_context":
                sc += (
                    155
                    if base_form == "8-K" and doc_role == "earnings_release"
                    else 145
                    if base_form == "8-K" and doc_role == "ceo_letter"
                    else 95
                    if base_form == "10-Q"
                    else -25
                    if base_form == "8-K"
                    else -30
                )
            elif topic == "key_dependencies":
                sc += 130 if base_form == "10-K" else 110 if base_form == "10-Q" else -80
            if bool(row.get("is_amendment")):
                sc -= 12
                if is_administrative_amendment(row) and topic in {"what_it_does", "segment_operating_model", "key_advantage", "key_dependencies", "revenue_streams"}:
                    sc -= 80
            return sc, filed_ts, report_ts

        deduped: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for cand in candidates:
            key = (str(cand.get("accn") or ""), str(cand.get("doc") or ""))
            if key not in deduped:
                deduped[key] = cand
        return sorted(deduped.values(), key=score, reverse=True)

    def best_source_text(topic: str) -> Tuple[Optional[Dict[str, Any]], str, str]:
        for cand in candidate_rows_for_topic(topic):
            if topic == "current_strategic_context":
                txt, part = (
                    mda_opening(cand)
                    if str(cand.get("base_form") or "") == "10-Q"
                    else (str(load_doc_bundle(cand).get("plain") or ""), str(cand.get("doc_role_label") or "8-K narrative"))
                )
            elif topic == "key_advantage":
                txt, part = competition_section(cand)
            elif topic == "key_dependencies":
                txt, part = risk_section(cand)
            else:
                txt, part = business_section(cand)
            if txt and len(txt) >= 120:
                return cand, txt, part
        return None, "", ""

    def build_segment_rows(text_in: str) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        sents = normalized_sentences(text_in)
        for seg_name, seg_re in list(getattr(profile, "segment_patterns", tuple()) or tuple()):
            best_sent = ""
            best_score = -999
            for sent in sents:
                if not seg_re.search(sent):
                    continue
                if _looks_noise_sentence(sent):
                    continue
                score = 0
                score += 2 if _BUSINESS_VERB_RE.search(sent) else 0
                score += 2 if re.search(r"\b(revenue|software|services|technology|financing|mail|shipping|ethanol|grain|marketing|trading|protein|corn oil)\b", sent, re.I) else 0
                score -= 4 if re.search(r"\b(face competition|competition)\b", sent, re.I) else 0
                if 8 <= _word_count(sent) <= 36:
                    score += 1
                if score > best_score:
                    best_score = score
                    best_sent = _norm_text(sent)
            if best_sent:
                rows.append({"segment": canonical_segment_display_name(seg_name), "text": best_sent[:360]})
        filtered_rows: List[Dict[str, Any]] = []
        for row in rows:
            seg_txt = str(row.get("segment") or "").strip()
            txt = str(row.get("text") or "").strip()
            low_txt = txt.lower()
            if not txt:
                continue
            if re.match(r"^(general|competition)\b", low_txt):
                continue
            if re.search(r"\b(other other|prior .* did not qualify for discontinued operations|operations that were dissolved or sold)\b", low_txt):
                continue
            filtered_rows.append({"segment": seg_txt, "text": txt})
        return filtered_rows

    def compose_what_it_does(text_in: str, segment_rows: List[Dict[str, Any]]) -> str:
        sents = normalized_sentences(text_in)
        scored = sorted(
            [(business_sentence_score(sent), sent) for sent in sents if business_sentence_score(sent) > 0],
            key=lambda item: (item[0], -abs(_word_count(item[1]) - 26)),
            reverse=True,
        )
        chosen: List[str] = []
        used: set[str] = set()
        explicit_company_descriptions = [
            sent
            for sent in sents
            if re.search(r"\bis\s+an?\b", sent, re.I)
            and (
                _contains_any(str(sent).lower(), getattr(profile, "industry_keywords", tuple()) or tuple())
                or "company" in str(sent).lower()
            )
        ]
        for sent in explicit_company_descriptions[:1]:
            key = glx_dedup_text_key(sent)
            if key not in used:
                chosen.append(clean_summary_sentence(sent))
                used.add(key)
        company_scored = [
            (_score, sent)
            for _score, sent in scored
            if company_name and company_name.lower() in str(sent).lower()
        ]
        declarative_scored = [
            (_score, sent)
            for _score, sent in scored
            if re.search(r"\b[A-Z][A-Za-z&\.\- ]{2,60}\s+is\s+an?\b", str(sent))
        ]
        first_pool = company_scored or declarative_scored or scored
        if not chosen:
            for _score, sent in first_pool:
                key = glx_dedup_text_key(sent)
                if key in used:
                    continue
                chosen.append(clean_summary_sentence(sent))
                used.add(key)
                break
        seg_sentence = ""
        if segment_rows:
            seg_names = [str(row.get("segment") or "").strip() for row in segment_rows[:3] if str(row.get("segment") or "").strip()]
            if len(seg_names) >= 2:
                seg_sentence = f"It operates through {', '.join(seg_names[:-1])} and {seg_names[-1]}."
            elif len(seg_names) == 1:
                seg_sentence = f"It operates through {seg_names[0]}."
        if not seg_sentence:
            for _score, sent in scored:
                if segment_hits(sent) >= 2 or re.search(r"\boperates through\b|\bsegments?\b", sent, re.I):
                    if glx_dedup_text_key(sent) not in used:
                        seg_sentence = clean_summary_sentence(sent)
                        break
        if seg_sentence:
            chosen.append(seg_sentence)
        out_txt = " ".join(chosen[:2]).strip()
        words = re.findall(r"\b[\w\-]+\b", out_txt)
        if len(words) > 90:
            out_txt = " ".join(words[:90]).rstrip(" ,;:") + "."
        return out_txt[:560]

    def rescue_what_it_does(text_in: str, segment_rows: List[Dict[str, Any]]) -> str:
        sents = normalized_sentences(text_in)
        company_sent = next((sent for sent in sents if looks_company_description_sentence(sent)), "")
        if not company_sent:
            company_sent = next(
                (
                    sent
                    for sent in sents
                    if company_name.lower() in sent.lower() and _BUSINESS_VERB_RE.search(sent)
                ),
                "",
            )
        seg_sentence = ""
        if segment_rows:
            seg_names = [str(row.get("segment") or "").strip() for row in segment_rows[:3] if str(row.get("segment") or "").strip()]
            if len(seg_names) >= 2:
                seg_sentence = f"It operates through {', '.join(seg_names[:-1])} and {seg_names[-1]}."
            elif len(seg_names) == 1:
                seg_sentence = f"It operates through {seg_names[0]}."
        out_bits = [bit for bit in [company_sent, seg_sentence] if bit]
        return " ".join(out_bits[:2])[:560]

    def compose_key_advantage() -> Tuple[str, Optional[Dict[str, Any]], str]:
        sentence_rows: List[Tuple[int, str, Dict[str, Any], str]] = []
        source_blobs: List[str] = []
        for cand in candidate_rows_for_topic("key_advantage")[:6]:
            source_blocks: List[Tuple[str, str]] = []
            comp_txt, comp_part = competition_section(cand)
            if comp_txt:
                source_blocks.append((comp_txt, comp_part))
            biz_txt, biz_part = business_section(cand)
            if biz_txt and glx_dedup_text_key(biz_txt) != glx_dedup_text_key(comp_txt):
                source_blocks.append((biz_txt, biz_part))
            if str(cand.get("doc_role") or "") in {"earnings_release", "ceo_letter"}:
                plain_txt = str(load_doc_bundle(cand).get("plain") or "")
                if plain_txt:
                    source_blocks.append((plain_txt, str(cand.get("doc_role_label") or "8-K narrative")))
            for txt, part in source_blocks:
                source_blobs.append(str(txt or ""))
                for sent in normalized_sentences(txt):
                    score = advantage_sentence_score(sent)
                    if score <= 0:
                        continue
                    low_txt = sent.lower()
                    if part == "10-K Competition":
                        score += 1
                    if str(cand.get("doc_role") or "") in {"earnings_release", "ceo_letter"} and re.search(
                        r"\b(low carbon|45z|carbon capture|platform|co-products?|corn oil|protein)\b",
                        low_txt,
                    ):
                        score += 1
                    sentence_rows.append((score, sent, cand, part))
        if not sentence_rows:
            return "", None, ""
        sentence_rows.sort(key=lambda item: (item[0], -abs(_word_count(item[1]) - 30)), reverse=True)
        best_score, best_sent, best_row, best_part = sentence_rows[0]
        joined_blob = " ".join(source_blobs).lower()
        if "biofuels" in sector_packs and (
            best_score < 5
            or re.search(r"\b(while faced with|aim to utilize|segment includes|includes the production, storage and transportation)\b", best_sent.lower())
        ):
            has_platform = bool(re.search(r"\b(biorefin|streamlined platform|platform)\b", joined_blob))
            has_low_carbon = bool(re.search(r"\b(low carbon|45z|carbon intensity|carbon reduction|ccs)\b", joined_blob))
            has_coprods = bool(re.search(r"\b(protein|corn oil|co-products?)\b", joined_blob))
            if has_low_carbon and has_coprods:
                synth = "The company's edge is its biorefinery platform, combining low-carbon fuel production with higher-value protein and corn-oil co-products and carbon-intensity improvement that can matter more as low-carbon fuel markets develop."
                return synth[:360], best_row, "10-K Item 1 / biofuels pack synthesis"
        if "industrial_capital_return" in sector_packs:
            has_installed_base = bool(re.search(r"\binstalled base\b", joined_blob))
            has_financing = bool(re.search(r"\b(financing|payment solutions?|financial services)\b", joined_blob))
            has_workflows = bool(re.search(r"\b(workflows?|software-enabled|software driven|software)\b", joined_blob))
            has_network = bool(re.search(r"\b(presort network|network|scale)\b", joined_blob))
            best_low = best_sent.lower()
            missing_supported_term = bool(
                (has_installed_base and "installed base" not in best_low)
                or (has_financing and "financing" not in best_low)
                or (has_workflows and not re.search(r"\b(workflow|software-enabled|software)\b", best_low))
                or (has_network and not re.search(r"\b(presort network|network|scale)\b", best_low))
            )
            if sum(int(flag) for flag in (has_installed_base, has_financing, has_workflows, has_network)) >= 3 and (
                best_score < 6 or missing_supported_term
            ):
                edge_parts: List[str] = []
                if has_installed_base:
                    edge_parts.append("a large installed base")
                if has_financing:
                    edge_parts.append("financing capability")
                if has_workflows:
                    edge_parts.append("software-enabled workflows")
                if has_network and "presort" in joined_blob:
                    edge_parts.append("a national presort network")
                elif has_network:
                    edge_parts.append("network scale")
                tail = "which can support recurring service revenue, customer retention and scale advantages"
                if has_workflows:
                    tail += " in mail and parcel workflows"
                synth = f"The company's edge is {join_phrases(edge_parts)}, {tail}."
                return synth[:360], best_row, "10-K Competition / industrial-capital-return pack synthesis"
        if best_score <= 0:
            return "", None, ""
        return str(best_sent)[:360], best_row, best_part

    def compose_current_context() -> Tuple[str, List[Dict[str, Any]]]:
        doc_rows: List[Tuple[float, List[Tuple[int, str]], Dict[str, Any], str]] = []
        for cand in candidate_rows_for_topic("current_strategic_context")[:8]:
            txt, _part = (
                mda_opening(cand)
                if str(cand.get("base_form") or "") == "10-Q"
                else (str(load_doc_bundle(cand).get("plain") or ""), str(cand.get("doc_role_label") or "8-K narrative"))
            )
            scored_sents: List[Tuple[int, str]] = []
            for sent in normalized_sentences(txt):
                score = current_sentence_score(sent)
                if score <= 0:
                    continue
                scored_sents.append((score, sent))
            if not scored_sents:
                continue
            scored_sents.sort(key=lambda item: (item[0], -abs(_word_count(item[1]) - 22)), reverse=True)
            doc_score = float(scored_sents[0][0]) + (0.35 * float(scored_sents[1][0]) if len(scored_sents) > 1 else 0.0)
            doc_rows.append((doc_score, scored_sents[:3], cand, txt))
        doc_rows.sort(key=lambda item: item[0], reverse=True)
        chosen: List[str] = []
        source_rows: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for _score, sents, cand, _txt in doc_rows:
            added_from_doc = False
            for sent_score, sent in sents:
                if chosen and looks_company_description_sentence(sent):
                    continue
                if chosen and (
                    sent_score < 6
                    or not re.search(
                        r"\b(2026|2027|outlook|guidance|capital allocation|debt reduction|cost discipline|execution|45z|ccs|carbon capture|liquidity|commercializ|monetiz|repurchase|authorization|dividend|balance sheet)\b",
                        sent.lower(),
                    )
                ):
                    continue
                key = glx_dedup_text_key(sent)
                if key in seen:
                    continue
                chosen.append(sent)
                seen.add(key)
                added_from_doc = True
                if len(chosen) >= 2:
                    break
            if added_from_doc:
                source_rows.append(cand)
            if len(chosen) >= 2:
                break
        joined_blob = " ".join(str(txt or "") for _score, _sents, _cand, txt in doc_rows[:3]).lower()
        if "biofuels" in sector_packs:
            bio_parts: List[str] = []
            if re.search(r"\b45z\b", joined_blob):
                bio_parts.append("45Z monetization")
            if re.search(r"\b(ccs|carbon capture)\b", joined_blob):
                bio_parts.append("CCS execution")
            if re.search(r"\b(carbon intensity|carbon-intensity|ci score|low-carbon)\b", joined_blob):
                bio_parts.append("broader low-carbon value realization")
            if len(bio_parts) >= 2:
                context_txt = f"Management is focused on {join_phrases(bio_parts[:3])}"
                if re.search(r"\b2026\b", joined_blob):
                    context_txt += " into 2026"
                context_txt += "."
                synth_rows = [cand for _score, _sents, cand, _txt in doc_rows[:2]]
                return context_txt[:420], synth_rows
        if "industrial_capital_return" in sector_packs:
            industrial_parts: List[str] = []
            if re.search(r"\bcapital allocation\b", joined_blob):
                industrial_parts.append("capital allocation")
            if re.search(r"\b(debt reduction|deleverag)\b", joined_blob):
                industrial_parts.append("debt reduction")
            if re.search(r"\b(cost discipline|cost savings?|cost reduction)\b", joined_blob):
                industrial_parts.append("cost discipline")
            if re.search(r"\b(execution|operational execution)\b", joined_blob):
                industrial_parts.append("execution")
            if re.search(r"\b(more accurate guidance|guidance accuracy|accurate guidance)\b", joined_blob):
                industrial_parts.append("improving guidance accuracy")
            elif re.search(r"\bguidance\b", joined_blob):
                industrial_parts.append("guidance discipline")
            if len(industrial_parts) >= 2:
                context_txt = f"Management is focused on {join_phrases(industrial_parts[:4])}"
                if re.search(r"\b2026\b", joined_blob):
                    context_txt += " into 2026"
                context_txt += "."
                synth_rows = [cand for _score, _sents, cand, _txt in doc_rows[:2]]
                return context_txt[:420], synth_rows
        if len(chosen) >= 2 and re.search(
            r"\b(45z|ccs|carbon capture|low-carbon|capital allocation|debt reduction|cost discipline|guidance)\b",
            chosen[0].lower(),
        ):
            if not re.search(
                r"\b(45z|ccs|carbon capture|low-carbon|capital allocation|debt reduction|cost discipline|guidance|authorization|dividend|liquidity|execution|commercializ|monetiz|2026|2027)\b",
                chosen[1].lower(),
            ):
                chosen = chosen[:1]
        context_txt = " ".join(chosen[:2])[:420]
        parts = re.split(r"(?<=[\.\!\?])\s+", context_txt)
        if len(parts) >= 2 and re.search(
            r"\b(45z|ccs|carbon capture|low-carbon|capital allocation|debt reduction|cost discipline|guidance)\b",
            parts[0].lower(),
        ):
            if re.search(r"\b(the consolidated|is the .*segment['’]s|which includes .* plus)\b", parts[1].lower()):
                context_txt = parts[0]
        return context_txt, source_rows

    def dependency_defs() -> List[Dict[str, Any]]:
        defs: List[Dict[str, Any]] = [
            {
                "label": "Balance-sheet flexibility and refinancing discipline",
                "wrong": "Liquidity or refinancing flexibility tightens",
                "patterns": (r"\b(debt|refinanc|maturity|covenant|interest rate|liquidity|capital resources?|ratings?)\b",),
            },
            {
                "label": "Carrier, logistics and service execution reliability",
                "wrong": "Execution or logistics issues disrupt service and margins",
                "patterns": (r"\b(logistics|rail|carrier|transportation|service levels?|uptime|yield|availability)\b",),
            },
            {
                "label": "Customer demand, retention and end-market pressure",
                "wrong": "Demand or retention weakens faster than expected",
                "patterns": (r"\b(customer|retention|demand|volume|end market|major customer)\b",),
            },
        ]
        if "biofuels" in sector_packs:
            defs.extend(
                [
                    {
                        "label": "Crush spreads between corn, natural gas and ethanol/co-products",
                        "wrong": "Crush spreads stay weak or volatile for longer",
                        "patterns": (r"\b(corn|natural gas|commodity|ethanol|spread|co-products?)\b",),
                    },
                    {
                        "label": "45Z, CCS and carbon-intensity monetization",
                        "wrong": "45Z or CCS economics fail to monetize as expected",
                        "patterns": (r"\b(45z|ccs|carbon capture|carbon intensity|ci score|tax credit|monetiz)\b",),
                    },
                    {
                        "label": "Plant yields, uptime and rail/logistics reliability",
                        "wrong": "Plant uptime, yields or logistics cap utilization",
                        "patterns": (r"\b(yield|uptime|plant|maintenance|rail|logistics|utilization)\b",),
                    },
                    {
                        "label": "Demand realization for higher-value protein and oil outputs",
                        "wrong": "Co-product mix shift does not lift profitability enough",
                        "patterns": (r"\b(protein|ultra-high protein|corn oil|co-product|high-value)\b",),
                    },
                ]
            )
        if "industrial_capital_return" in sector_packs:
            defs.extend(
                [
                    {
                        "label": "USPS rules, postal pricing and mailing-market economics",
                        "wrong": "Postal changes hurt mailing and presort economics",
                        "patterns": (r"\b(usps|postal service|postal rates?|postal pricing|mailing market|workshare)\b",),
                    },
                    {
                        "label": "Continued physical-mail decline and shipping-volume pressure",
                        "wrong": "Mail and parcel volumes weaken faster than productivity offsets",
                        "patterns": (r"\b(mail volume|mailing volumes?|decline in mail|shipping volume|parcel volume)\b",),
                    },
                    {
                        "label": "Presort transportation, labor and service execution",
                        "wrong": "Presort transportation or labor issues pressure service and margins",
                        "patterns": (r"\b(presort|transportation|labor|sortation|service execution)\b",),
                    },
                    {
                        "label": "SendTech execution, product recovery and cost discipline",
                        "wrong": "SendTech recovery or cost actions fail to improve earnings",
                        "patterns": (r"\b(sendtech|cost savings?|cost discipline|execution|product cycle|recovery)\b",),
                    },
                ]
            )
        return defs

    def extract_dependency_themes(primary_row: Optional[Dict[str, Any]], latest_q_row: Optional[Dict[str, Any]]) -> Tuple[List[str], List[str], str]:
        best: Dict[str, Tuple[float, str, Dict[str, Any]]] = {}
        for row in [primary_row, latest_q_row]:
            if not isinstance(row, dict):
                continue
            risk_txt, _part = risk_section(row)
            for sent in _split_sentences(risk_txt):
                low_txt = sent.lower()
                if _looks_noise_sentence(sent):
                    continue
                for theme in dependency_defs():
                    if not any(re.search(pat, low_txt, re.I) for pat in tuple(theme.get("patterns") or tuple())):
                        continue
                    score = 1.0
                    score += 1.0 if re.search(r"\b(may|could|might|adverse|material|depend|subject to|pressure)\b", low_txt) else 0.0
                    score += 0.8 if re.search(r"\b(revenue|margin|cash flow|profit|liquidity|value)\b", low_txt) else 0.0
                    score += min(1.0, float(len(sent)) / 240.0)
                    theme_label = str(theme.get("label") or "")
                    if theme_label == "45Z, CCS and carbon-intensity monetization":
                        score += 1.5
                    elif theme_label == "Crush spreads between corn, natural gas and ethanol/co-products":
                        score += 0.8
                    cur = best.get(str(theme["label"]))
                    if cur is None or score > cur[0]:
                        best[str(theme["label"])] = (score, str(theme["wrong"]), row)
        if not best:
            return [], [], "Source: N/A (risk dependency themes not found)"
        ordered = sorted(best.items(), key=lambda item: item[1][0], reverse=True)[:5]
        deps = [label for label, (_score, _wrong, _row) in ordered]
        wrong = [wrong_txt for _label, (_score, wrong_txt, _row) in ordered]
        src_rows = [row for _label, (_score, _wrong, row) in ordered]
        return deps, wrong, _clean_source_note_rows(src_rows, "risk dependencies / wrong-thesis translation")

    def extract_revenue_streams(row: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Optional[date], str]:
        bundle = load_doc_bundle(row)
        html_bytes = bundle.get("bytes") or b""
        if not html_bytes:
            return [], None, "Source: N/A (revenue stream document unavailable)"
        tables = read_html_tables_any(html_bytes)
        segment_patterns: List[Tuple[str, re.Pattern[str]]] = list(getattr(profile, "segment_patterns", tuple()) or tuple())
        alias_patterns = list(annual_segment_alias_patterns) or []
        report_year = row.get("report").year if isinstance(row.get("report"), date) else None
        best_streams: List[Dict[str, Any]] = []
        best_score = -1
        best_idx: Optional[int] = None
        quarterly_fallback = str(row.get("base_form") or "") == "10-Q"
        for tidx, tdf in enumerate(tables):
            if tdf is None or tdf.empty:
                continue
            df = tdf.copy().dropna(axis=0, how="all").dropna(axis=1, how="all")
            if df.shape[0] < 3 or df.shape[1] < 2:
                continue
            table_text = re.sub(
                r"\s+",
                " ",
                " ".join(str(x) for x in list(df.columns) + list(df.fillna("").astype(str).values.flatten())),
            ).lower()
            if quarterly_fallback:
                if not ("segment revenue" in table_text or "segment results" in table_text or any(seg_re.search(table_text) for _seg, seg_re in segment_patterns)):
                    continue
            else:
                if not any(k in table_text for k in ("revenue", "net sales", "disaggregation")):
                    continue
            labels = df[df.columns[0]].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
            numeric_cols: List[Any] = []
            for col in df.columns[1:]:
                vals = df[col].apply(lambda x: coerce_number(x))
                if vals.notna().sum() >= max(2, int(len(df) * 0.35)):
                    numeric_cols.append(col)
            if not numeric_cols:
                continue
            col_pick = numeric_cols[-1]
            if report_year is not None:
                for col in numeric_cols:
                    if re.search(rf"\b{int(report_year)}\b", str(col)):
                        col_pick = col
                        break
            vals_pick = df[col_pick].apply(lambda x: coerce_number(x))
            label_low = labels.str.lower()
            total_mask = label_low.str.contains(r"\btotal\b", regex=True)
            total_val = None
            if total_mask.any():
                tv = pd.to_numeric(vals_pick[total_mask], errors="coerce").dropna()
                if not tv.empty:
                    total_val = float(tv.iloc[0])
            if total_val in (None, 0):
                pos = pd.to_numeric(vals_pick, errors="coerce").dropna()
                pos = pos[pos > 0]
                total_val = float(pos.sum()) if not pos.empty else None
            if total_val in (None, 0):
                continue
            cand_rows: List[Dict[str, Any]] = []
            bad_labels = 0
            matched_segments: set[str] = set()
            for lbl, vv in zip(labels.tolist(), vals_pick.tolist()):
                if vv is None or pd.isna(vv):
                    continue
                lbl_s = str(lbl).strip()
                lbl_l = lbl_s.lower()
                if not lbl_s or re.fullmatch(r"[$€£]", lbl_s):
                    continue
                if any(x in lbl_l for x in ("total", "elimination", "intersegment", "inter-company", "intercompany")):
                    continue
                canonical_name = ""
                for pat, label in alias_patterns:
                    try:
                        if pat.search(lbl_s):
                            canonical_name = str(label)
                            break
                    except Exception:
                        continue
                if not canonical_name:
                    canonical_name = canonical_segment_display_name(lbl_s)
                if any(x in lbl_l for x in ("interest", "tax", "income", "expense", "depreciation", "amortization", "gain", "loss", "assets", "liabil", "adjusted segment ebit", "adjusted ebit", "segment ebit", "segment profit")):
                    bad_labels += 1
                    continue
                amt = float(vv)
                if amt <= 0:
                    continue
                is_segment_named = False
                if annual_segment_labels:
                    is_segment_named = canonical_name in set(annual_segment_labels)
                else:
                    is_segment_named = any(seg_re.search(lbl_s) for _seg, seg_re in segment_patterns)
                if annual_segment_labels and not is_segment_named:
                    bad_labels += 1
                    continue
                if is_segment_named:
                    matched_segments.add(canonical_name)
                cand_rows.append({"name": canonical_name or lbl_s, "amount": amt, "pct": amt / float(total_val)})
            if len(cand_rows) < 2:
                continue
            if annual_segment_labels and len(matched_segments) < 2:
                continue
            pct_sum = float(sum(r["pct"] for r in cand_rows))
            if pct_sum < 0.70 or pct_sum > 1.25:
                continue
            score = 0
            score += 3 if any(k in table_text for k in ("revenue", "net sales", "disaggregation", "segment")) else 0
            score += 2 if 0.80 <= pct_sum <= 1.20 else 0
            score += 1 if any(seg_re.search(table_text) for _seg, seg_re in segment_patterns) else 0
            score += 5 if len(matched_segments) >= 2 else 0
            if bad_labels > 0:
                score -= 2
            if score > best_score:
                best_score = score
                best_streams = cand_rows
                best_idx = tidx
        if not best_streams and annual_segment_labels:
            for tidx, tdf in enumerate(tables):
                if tdf is None or tdf.empty:
                    continue
                df = tdf.copy().dropna(axis=0, how="all").dropna(axis=1, how="all")
                if df.shape[0] < 3 or df.shape[1] < 2:
                    continue
                table_text = re.sub(
                    r"\s+",
                    " ",
                    " ".join(str(x) for x in list(df.columns) + list(df.fillna("").astype(str).values.flatten())),
                ).lower()
                if not any(k in table_text for k in ("revenue", "net sales", "segment revenue", "total revenue")):
                    continue
                labels = df[df.columns[0]].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
                numeric_cols: List[Any] = []
                for col in df.columns[1:]:
                    vals = df[col].apply(lambda x: coerce_number(x))
                    if vals.notna().sum() >= max(2, int(len(df) * 0.35)):
                        numeric_cols.append(col)
                if not numeric_cols:
                    continue
                vals_pick = df[numeric_cols[-1]].apply(lambda x: coerce_number(x))
                total_mask = labels.str.lower().str.contains(r"\btotal\b", regex=True)
                total_val = None
                if total_mask.any():
                    tv = pd.to_numeric(vals_pick[total_mask], errors="coerce").dropna()
                    if not tv.empty:
                        total_val = float(tv.iloc[0])
                rescue_rows: List[Dict[str, Any]] = []
                matched_segments: set[str] = set()
                for lbl, vv in zip(labels.tolist(), vals_pick.tolist()):
                    if vv is None or pd.isna(vv):
                        continue
                    canonical_name = canonical_segment_display_name(lbl)
                    if canonical_name not in set(annual_segment_labels):
                        continue
                    amt = float(vv)
                    if amt <= 0:
                        continue
                    matched_segments.add(canonical_name)
                    rescue_rows.append({"name": canonical_name, "amount": amt, "pct": 0.0})
                if len(matched_segments) < 2 or len(rescue_rows) < 2:
                    continue
                if total_val in (None, 0):
                    total_val = float(sum(float(row.get("amount") or 0.0) for row in rescue_rows))
                if total_val in (None, 0):
                    continue
                for entry in rescue_rows:
                    entry["pct"] = float(entry.get("amount") or 0.0) / float(total_val)
                best_streams = rescue_rows
                best_idx = tidx
                break
        if not best_streams:
            return [], None, "Source: N/A (revenue stream table not parsed)"
        best_streams = sorted(best_streams, key=lambda item: float(item.get("pct") or 0), reverse=True)
        if len(best_streams) > 6:
            head = best_streams[:5]
            tail = best_streams[5:]
            head.append(
                {
                    "name": "Other",
                    "amount": float(sum(float(t.get("amount") or 0.0) for t in tail)),
                    "pct": float(sum(float(t.get("pct") or 0.0) for t in tail)),
                }
            )
            best_streams = head
        detail = f"{str(row.get('base_form') or row.get('form') or 'doc')} revenue/segment table #{int(best_idx or 0) + 1}"
        return best_streams, row.get("report"), _clean_source_note_rows([row], detail)

    business_row, business_text, business_part = best_source_text("what_it_does")
    if business_row:
        out["asof_fy_end"] = business_row.get("report")
        segment_rows = build_segment_rows(business_text)
        what_it_does = compose_what_it_does(business_text, segment_rows)
        if not what_it_does:
            what_it_does = rescue_what_it_does(business_text, segment_rows)
        if what_it_does:
            out["what_it_does"] = what_it_does
            out["what_it_does_source"] = _clean_source_note_rows([business_row], business_part)
            source_manifest["what_it_does"] = {"accn": business_row.get("accn"), "doc": business_row.get("doc"), "part": business_part}
        if segment_rows:
            out["segment_operating_model"] = segment_rows
            out["segment_operating_model_source"] = _clean_source_note_rows([business_row], business_part)
            source_manifest["segment_operating_model"] = {"accn": business_row.get("accn"), "doc": business_row.get("doc"), "part": business_part}

    current_text, current_rows = compose_current_context()
    if current_text:
        out["current_strategic_context"] = current_text
        out["current_strategic_context_source"] = _clean_source_note_rows(current_rows, "current strategic context")
        source_manifest["current_strategic_context"] = [
            {"accn": row.get("accn"), "doc": row.get("doc"), "role": row.get("doc_role")}
            for row in current_rows
        ]

    key_adv, adv_row, adv_part = compose_key_advantage()
    if adv_row and key_adv:
        out["key_advantage"] = key_adv
        out["key_advantage_source"] = _clean_source_note_rows([adv_row], adv_part)
        source_manifest["key_advantage"] = {"accn": adv_row.get("accn"), "doc": adv_row.get("doc"), "part": adv_part}

    dep_candidates = candidate_rows_for_topic("key_dependencies")
    dep_primary = dep_candidates[0] if dep_candidates else None
    dep_q = next((row for row in dep_candidates if str(row.get("base_form") or "") == "10-Q"), None)
    deps, wrong, dep_src = extract_dependency_themes(dep_primary, dep_q)
    if deps:
        out["key_dependencies"] = deps
        out["wrong_thesis_bullets"] = wrong
        out["key_dependencies_source"] = dep_src
        out["wrong_thesis_source"] = dep_src
        source_manifest["dependencies"] = [
            {"accn": row.get("accn"), "doc": row.get("doc")}
            for row in [dep_primary, dep_q]
            if isinstance(row, dict)
        ]

    rev_candidates = candidate_rows_for_topic("revenue_streams")
    for form_group in ("10-K", "10-Q"):
        form_rows = [row for row in rev_candidates if str(row.get("base_form") or "") == form_group]
        if not form_rows:
            continue
        latest_report = max(pd.Timestamp(row.get("report") or "1900-01-01") for row in form_rows)
        latest_rows = [row for row in form_rows if pd.Timestamp(row.get("report") or "1900-01-01") == latest_report]
        for rev_row in latest_rows:
            streams, period, rev_src = extract_revenue_streams(rev_row)
            if streams:
                out["revenue_streams"] = streams
                out["revenue_streams_period"] = period
                out["revenue_streams_source"] = rev_src
                source_manifest["revenue_streams"] = {"accn": rev_row.get("accn"), "doc": rev_row.get("doc")}
                break
        if out.get("revenue_streams"):
            break

    if is_pbi_profile:
        pbi_desc_fallback = str(getattr(profile, "summary_description_fallback", "") or "").strip()
        pbi_adv_fallback = str(getattr(profile, "summary_key_advantage_fallback", "") or "").strip()
        pbi_seg_fallbacks = list(getattr(profile, "summary_segment_operating_model_fallbacks", tuple()) or tuple())
        pbi_dep_fallbacks = list(getattr(profile, "summary_dependency_fallbacks", tuple()) or tuple())
        pbi_wrong_fallbacks = list(getattr(profile, "summary_wrong_thesis_fallbacks", tuple()) or tuple())
        if (
            not str(out.get("what_it_does") or "").strip()
            or str(out.get("what_it_does") or "").startswith("N/A")
            or len(re.findall(r"\b(sendtech|presort|shipping|mailing|postal)\b", str(out.get("what_it_does") or ""), re.I)) < 2
        ) and pbi_desc_fallback:
            out["what_it_does"] = pbi_desc_fallback
            out["what_it_does_source"] = "Source: Profile fallback (topic-aware business description not sufficiently source-clean)"
        if (not out.get("segment_operating_model") or len(out.get("segment_operating_model") or []) < 2) and pbi_seg_fallbacks:
            rows: List[Dict[str, Any]] = []
            for raw_row in pbi_seg_fallbacks[:4]:
                row_txt = _norm_text(raw_row)
                if not row_txt:
                    continue
                rows.append({"segment": row_txt.split(":", 1)[0].strip(), "text": row_txt[:360]})
            if rows:
                out["segment_operating_model"] = rows
                out["segment_operating_model_source"] = "Source: Profile fallback (topic-aware segment model incomplete)"
        if (
            not str(out.get("key_advantage") or "").strip()
            or str(out.get("key_advantage") or "").startswith("N/A")
            or not re.search(r"\b(installed base|network|software|workflow|recurring|presort|shipping|mailing|retention|financing)\b", str(out.get("key_advantage") or ""), re.I)
        ) and pbi_adv_fallback:
            out["key_advantage"] = pbi_adv_fallback
            out["key_advantage_source"] = "Source: Profile fallback (topic-aware competitive advantage sentence unavailable)"
        dep_hits = sum(
            1
            for row_txt in list(out.get("key_dependencies") or [])
            if re.search(r"\b(usps|postal|mail decline|presort|transportation|labor|liquidity|ratings|covenants|refinanc|sendtech|cost discipline)\b", str(row_txt or ""), re.I)
        )
        if dep_hits < 3 and pbi_dep_fallbacks:
            out["key_dependencies"] = list(pbi_dep_fallbacks[:5])
            out["key_dependencies_source"] = "Source: Profile fallback (topic-aware dependency themes insufficiently specific)"
        wrong_hits = sum(
            1
            for row_txt in list(out.get("wrong_thesis_bullets") or [])
            if re.search(r"\b(mail|postal|liquidity|refinanc|sendtech|presort|cost)\b", str(row_txt or ""), re.I)
        )
        if wrong_hits < 3 and pbi_wrong_fallbacks:
            out["wrong_thesis_bullets"] = list(pbi_wrong_fallbacks[:5])
            out["wrong_thesis_source"] = "Source: Profile fallback (topic-aware bear-case translation insufficiently specific)"

    final_segment_rows = list(out.get("segment_operating_model") or [])
    if final_segment_rows and not str(out.get("what_it_does") or "").startswith("N/A"):
        preferred_seg_names = [
            str(row.get("segment") or "").strip()
            for row in final_segment_rows
            if str(row.get("segment") or "").strip()
            and not re.search(r"\b(other|corporate|global ecommerce)\b", str(row.get("segment") or ""), re.I)
        ]
        preferred_seg_names = list(dict.fromkeys(preferred_seg_names))
        if len(preferred_seg_names) >= 2:
            if not all(seg.lower() in str(out.get("what_it_does") or "").lower() for seg in preferred_seg_names[:2]):
                first_sentence = normalized_sentences(str(out.get("what_it_does") or ""))[:1]
                if first_sentence:
                    out["what_it_does"] = f"{first_sentence[0]} It operates through {preferred_seg_names[0]} and {preferred_seg_names[1]}."

    out["source_manifest"] = source_manifest
    return out
