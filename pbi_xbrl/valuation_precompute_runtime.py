"""Runtime helpers for valuation document precompute."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
import html
from pathlib import Path
import re
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover
    BeautifulSoup = None

from .doc_intel import extract_pdf_text_cached
from .guidance_lexicon import normalize_text as glx_normalize_text
from .non_gaap import strip_html


@dataclass
class ValuationPrecomputeRuntime:
    filing_doc_text_cache: Dict[str, str] = field(default_factory=dict)
    cap_alloc_doc_analysis_cache: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    buyback_execution_doc_cache: Dict[Tuple[str, str], Dict[str, Any]] = field(default_factory=dict)


def extract_valuation_filing_doc_text(
    runtime: ValuationPrecomputeRuntime,
    path_in: Path,
    *,
    path_cache_key: Callable[[Path], str],
    read_cached_doc_raw: Callable[[Path], str],
) -> str:
    cache_key = path_cache_key(path_in)
    cached = runtime.filing_doc_text_cache.get(cache_key)
    if cached is not None:
        return cached
    suffix = path_in.suffix.lower()
    if suffix == ".pdf":
        try:
            text = glx_normalize_text(extract_pdf_text_cached(path_in))
        except Exception:
            text = ""
        runtime.filing_doc_text_cache[cache_key] = text
        return text
    raw = read_cached_doc_raw(path_in)
    if suffix in {".htm", ".html"}:
        try:
            soup = BeautifulSoup(raw, "html.parser") if BeautifulSoup is not None else None
        except Exception:
            soup = None
        if soup is not None:
            for tag in list(soup.find_all(["script", "style", "ix:header", "ix:hidden"])):
                try:
                    tag.decompose()
                except Exception:
                    continue
            text = " ".join(soup.stripped_strings)
        else:
            text = strip_html(raw)
    else:
        raw_plain = glx_normalize_text(raw)
        runtime.filing_doc_text_cache[cache_key] = raw_plain
        return raw_plain
    text = glx_normalize_text(html.unescape(text))
    runtime.filing_doc_text_cache[cache_key] = text
    return text


def cap_alloc_unit_mult(text_low: str) -> float:
    if re.search(r"\bin\s+thousands\b", text_low):
        return 1000.0
    if re.search(r"\(\$\s*millions\)|\$\s*millions|\bdollars\s+in\s+millions\b|\bin\s+millions\b", text_low):
        return 1e6
    return 1.0


def extract_cap_alloc_row_cash(text: str, row_pat: str, mult: float) -> Optional[float]:
    match = re.search(row_pat, text, re.I)
    if not match:
        return None
    context_text = text[max(0, match.start() - 220) : min(len(text), match.end() + 360)].lower()
    if (
        re.search(r"\byear\s+ended\b|\bfull[\s-]?year\b|\bfirst\s+nine\s+months\b|\bsince\s+starting\b", context_text)
        and not re.search(r"\bthree\s+months\s+ended\b|\bquarter\b|\bq[1-4]\b", context_text)
    ):
        return None
    window = text[match.end() : match.end() + 260]
    candidates: List[float] = []
    for num_match in re.finditer(r"[\(\-]?\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\)?", window):
        raw = str(num_match.group(1) or "").replace(",", "")
        try:
            value = float(raw)
        except Exception:
            continue
        if "," not in num_match.group(0) and value < 1000:
            continue
        candidates.append(abs(value) * mult)
    return candidates[0] if candidates else None


def is_debt_repurchase_noise(text_in: Any) -> bool:
    text_local = glx_normalize_text(str(text_in or ""))
    if not text_local:
        return False
    low_local = text_local.lower()
    if not re.search(r"\brepurchas\w*\b", low_local, re.I):
        return False
    equity_context_local = bool(
        re.search(
            r"\b(common stock|share repurchase|buyback|shares?\b|treasury stock|repurchase program)\b",
            low_local,
            re.I,
        )
    )
    debt_context_local = bool(
        re.search(
            r"\b(fundamental change|indenture|convertible|senior notes?|2027 notes?|2030 notes?|holders?\b|subscription transactions?)\b",
            low_local,
            re.I,
        )
    )
    noteholder_put_local = bool(
        re.search(
            r"\b(require the company to repurchase|repurchase their\b[^.]{0,120}\bnotes?\b|holders?\b[^.]{0,120}\bnotes?\b[^.]{0,120}\brepurchase)\b",
            low_local,
            re.I,
        )
    )
    return bool(noteholder_put_local or (debt_context_local and not equity_context_local))


def extract_cap_alloc_quarter_cash_sentence(
    text: str,
    kw_pat: str,
    must_have_pat: Optional[str] = None,
    deny_pat: Optional[str] = None,
) -> Tuple[Optional[float], Optional[str]]:
    if not text:
        return None, None
    sentence_parts = re.split(r"(?<=[\.\!\?])\s+", text)
    kw_re = re.compile(kw_pat, re.I)
    must_re = re.compile(must_have_pat, re.I) if must_have_pat else None
    deny_re = re.compile(deny_pat, re.I) if deny_pat else None
    amt_re = re.compile(
        r"(?:\$\s*)?([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|m|bn)?",
        re.I,
    )
    best: Optional[float] = None
    best_sent: Optional[str] = None
    for sent in sentence_parts:
        ss = str(sent or "").strip()
        if not ss:
            continue
        sl = ss.lower()
        if is_debt_repurchase_noise(ss):
            continue
        if not kw_re.search(sl):
            continue
        if must_re is not None and not must_re.search(sl):
            continue
        if deny_re is not None and deny_re.search(sl):
            continue
        if not re.search(r"\b(q[1-4]|quarter|three\s+months\s+ended)\b", sl):
            continue
        if re.search(r"\b(full[\s-]?year|year\s+ended|first\s+nine\s+months|since\s+starting|since\s+the\s+program)\b", sl):
            continue
        vals: List[float] = []
        for match in amt_re.finditer(ss):
            raw_match = str(match.group(0) or "")
            raw = str(match.group(1) or "").replace(",", "")
            try:
                amount = float(raw)
            except Exception:
                continue
            unit = str(match.group(2) or "").lower()
            if not unit and "$" not in raw_match:
                continue
            if not unit and 1900 <= amount <= 2100:
                continue
            if unit in {"billion", "bn"}:
                amount *= 1e9
            elif unit in {"million", "m"}:
                amount *= 1e6
            elif amount < 1000:
                continue
            vals.append(abs(float(amount)))
        if not vals:
            continue
        pick = max(vals)
        if best is None or pick > best:
            best = pick
            best_sent = ss
    return best, best_sent


def parse_cap_alloc_amount(raw_num: str, unit: str) -> Optional[float]:
    try:
        value = float(str(raw_num).replace(",", ""))
    except Exception:
        return None
    unit_low = str(unit or "").strip().lower()
    if unit_low in {"billion", "bn"}:
        value *= 1e9
    elif unit_low in {"million", "m"}:
        value *= 1e6
    else:
        if value < 2000:
            value *= 1e6
    return float(value) if value > 0 else None


def classify_distribution_signal(note_text: str, source_hint: str = "") -> str:
    blob = glx_normalize_text(" ".join([str(note_text or ""), str(source_hint or "")]))
    if not blob:
        return "other_distribution"
    low = blob.lower()
    if re.search(
        r"\b(non[- ]?controlling interests?|noncontrolling interests?|nci|partners?'?\s+capital|partner distributions?|member distributions?)\b",
        low,
        re.I,
    ):
        return "distribution_to_nci"
    if re.search(
        r"\b(common stock dividend|common[- ]stock dividend|dividend per share|per common share|common shareholders?|common stockholders?|stockholders of record)\b",
        low,
        re.I,
    ):
        return "common_dividend"
    if re.search(
        r"\b(cash dividends and distributions declared|payments of dividends and distributions|dividends and distributions)\b",
        low,
        re.I,
    ):
        return "other_distribution"
    if re.search(r"\bdividend\b", low, re.I) and re.search(r"\bcommon\b", low, re.I):
        return "common_dividend"
    return "other_distribution"


def is_cumulative_buyback_context(text_in: Any) -> bool:
    text = glx_normalize_text(str(text_in or ""))
    if not text:
        return False
    return bool(
        re.search(
            r"\b(since inception|to date|since starting(?:\s+the\s+program)?|since the beginning|authorized up to|authorization remained|remaining authorization|remaining capacity|may repurchase|under the program we may repurchase|no other repurchase was made during|no repurchase was made during)\b",
            text,
            re.I,
        )
    )


def buyback_execution_scope_text(text_in: Any, qd_ref: Optional[date] = None) -> str:
    text = glx_normalize_text(html.unescape(str(text_in or "")).replace("\xa0", " "))
    if not text:
        return ""
    table_match = re.search(r"\bcommon stock purchases during the three months ended\b", text, re.I)
    if table_match is None:
        issuer_match = re.search(r"\b(?:issuer purchases of equity securities|repurchases? of equity securities)\b", text, re.I)
        if issuer_match is not None:
            issuer_window = text[int(issuer_match.start()) : min(len(text), int(issuer_match.end()) + 2600)]
            has_program_columns = bool(
                re.search(
                    r"\btotal number of shares purchased as part of publicly announced plans or programs\b",
                    issuer_window,
                    re.I,
                )
                and re.search(
                    r"\bapproximate dollar value of shares that may yet be purchased under the plans or programs\b",
                    issuer_window,
                    re.I,
                )
            )
            if has_program_columns:
                table_match = issuer_match
    if table_match:
        table_chunk = glx_normalize_text(text[max(0, int(table_match.start())) : min(len(text), int(table_match.end()) + 2600)])
        if table_chunk:
            return table_chunk
    return text


def has_buyback_execution_table_context(text_in: Any) -> bool:
    text = glx_normalize_text(str(text_in or ""))
    if not text:
        return False
    has_common_stock_table = bool(re.search(r"\bcommon stock purchases during the three months ended\b", text, re.I))
    has_issuer_header = bool(re.search(r"\b(?:issuer purchases of equity securities|repurchases? of equity securities)\b", text, re.I))
    has_program_columns = bool(
        re.search(
            r"\btotal number of shares purchased as part of publicly announced plans or programs\b",
            text,
            re.I,
        )
        and re.search(
            r"\bapproximate dollar value of shares that may yet be purchased under the plans or programs\b",
            text,
            re.I,
        )
    )
    return bool(has_common_stock_table or (has_issuer_header and has_program_columns) or has_program_columns)


def analyze_cap_alloc_doc(
    runtime: ValuationPrecomputeRuntime,
    path_in: Path,
    *,
    path_cache_key: Callable[[Path], str],
    extract_doc_text: Callable[[Path], str],
    text: Optional[str] = None,
    include_core: bool = True,
    include_auth_details: bool = False,
    compose_buyback_execution_summary: Optional[Callable[[Any, Optional[date]], str]] = None,
    extract_buyback_execution_components: Optional[Callable[[Any, Optional[date]], Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    cache_key = path_cache_key(path_in)
    analysis = runtime.cap_alloc_doc_analysis_cache.get(cache_key)
    if analysis is None:
        txt = str(text or extract_doc_text(path_in) or "")
        txt = re.sub(r"\s+", " ", txt).strip()
        txt_low = txt.lower()
        analysis = {
            "text": txt,
            "text_low": txt_low,
            "name_low": path_in.name.lower(),
            "mentions_cap_alloc": bool(
                "repurchase" in txt_low or "buyback" in txt_low or "dividend" in txt_low or "authorization" in txt_low or "bought back" in txt_low
            ),
            "buyback_quarter_sentence_amount": None,
            "buyback_quarter_sentence_text": None,
            "buyback_row_cash": None,
            "dividend_row_cash": None,
            "dividend_ps_candidates": [],
            "buyback_note_text": None,
            "buyback_execution_candidates": [],
            "dividend_note_text": None,
            "remaining_candidates": [],
            "authorization_candidates": [],
            "spent_since_start_candidates": [],
            "_core_ready": False,
            "_auth_details_ready": False,
        }
        runtime.cap_alloc_doc_analysis_cache[cache_key] = analysis
    if ((not include_core or bool(analysis.get("_core_ready"))) and (not include_auth_details or bool(analysis.get("_auth_details_ready")))):
        return analysis
    txt = str(analysis.get("text") or "")
    txt_low = str(analysis.get("text_low") or txt.lower())
    if not txt or not analysis.get("mentions_cap_alloc"):
        if include_core:
            analysis["_core_ready"] = True
        if include_auth_details:
            analysis["_auth_details_ready"] = True
        runtime.cap_alloc_doc_analysis_cache[cache_key] = analysis
        return analysis
    if include_core and not bool(analysis.get("_core_ready")):
        mult = cap_alloc_unit_mult(txt_low)
        bb, bb_sent = extract_cap_alloc_quarter_cash_sentence(
            txt,
            r"(repurchas|buyback|common\s+stock\s+repurchases?)",
            must_have_pat=r"\b(repurchased|repurchasing|bought\s+back|at\s+total\s+cost|cash\s+flow\s+into\s+repurchasing|spent)\b",
            deny_pat=r"\b(authoriz|capacity|remaining|increased?\s+.*repurchase\s+authorization)\b",
        )
        analysis["buyback_quarter_sentence_amount"] = bb
        analysis["buyback_quarter_sentence_text"] = bb_sent
        if bb is None:
            analysis["buyback_row_cash"] = extract_cap_alloc_row_cash(
                txt,
                r"(repurchase(?:s)?\s+of\s+common\s+stock|common\s+stock\s+repurchases?)",
                mult,
            )
        buyback_exec_summary = compose_buyback_execution_summary(txt) if compose_buyback_execution_summary is not None else ""
        exec_parts = extract_buyback_execution_components(txt) if buyback_exec_summary and extract_buyback_execution_components is not None else {}
        if buyback_exec_summary:
            analysis["buyback_execution_candidates"] = [
                {
                    "summary": buyback_exec_summary,
                    "shares": exec_parts.get("shares"),
                    "amount": exec_parts.get("amount"),
                    "avg_price": exec_parts.get("avg_price"),
                    "quarter_scoped": bool(exec_parts.get("quarter_scoped")),
                    "from_table": bool(exec_parts.get("from_table")),
                    "has_avg_price": exec_parts.get("avg_price") is not None,
                    "has_share_count": exec_parts.get("shares") is not None,
                    "has_amount": exec_parts.get("amount") is not None,
                    "explicit_count": int(exec_parts.get("explicit_count") or 0),
                }
            ]
        dividend_row_cash = extract_cap_alloc_row_cash(txt, r"dividends?\s+paid(?:\s*\([^)]*\))?", mult)
        if classify_distribution_signal(txt, str(path_in)) == "common_dividend":
            analysis["dividend_row_cash"] = dividend_row_cash
        analysis["_core_ready"] = True
    if include_auth_details and not bool(analysis.get("_auth_details_ready")):
        analysis["remaining_candidates"] = analysis.get("remaining_candidates") or []
        analysis["authorization_candidates"] = analysis.get("authorization_candidates") or []
        analysis["spent_since_start_candidates"] = analysis.get("spent_since_start_candidates") or []
        analysis["_auth_details_ready"] = True
    runtime.cap_alloc_doc_analysis_cache[cache_key] = analysis
    return analysis
