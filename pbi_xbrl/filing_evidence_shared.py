from __future__ import annotations

import html
import io
import re
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Pattern, Tuple

import pandas as pd

from .non_gaap import strip_html
from .pdf_utils import silence_pdfminer_warnings
from .sec_xbrl import SecClient, parse_date


@dataclass(frozen=True)
class RenderableNote:
    summary: str
    bucket: str
    display_score: float
    change_state: str
    source_type: str
    narrative_source: bool
    preferred_source: bool
    candidate_type: str = "investor_note_candidate"
    source_class: str = ""
    statement_class: str = ""
    parent_subject_key: str = ""
    route_reason: str = "quarter_notes"
    drop_reason: str = ""
    quality_drop_reason: str = ""
    merge_reason: str = ""
    collapse_reason: str = ""
    conflict_resolution_reason: str = ""
    lifecycle_state: str = ""
    status_resolution_reason: str = ""


@dataclass(frozen=True)
class QualifiedPromiseCandidate:
    scope: str
    summary: str
    source_type: str
    narrative_source: bool
    preferred_source: bool
    candidate_type: str = "measurable_promise_candidate"
    source_class: str = ""
    statement_class: str = ""
    parent_subject_key: str = ""
    lifecycle_subject_key: str = ""
    evidence_role: str = "promise_origin"
    route_reason: str = "promise_tracker"
    drop_reason: str = ""
    quality_drop_reason: str = ""
    merge_reason: str = ""
    collapse_reason: str = ""
    conflict_resolution_reason: str = ""
    lifecycle_state: str = "stated"
    status_resolution_reason: str = ""


@dataclass(frozen=True)
class EvidenceEvent:
    event_key: str
    event_type: str
    metric_family: str
    entity_scope: str
    period_norm: str
    direction: str
    summary: str
    bucket: str
    display_score: float
    source_type: str
    source_doc: str
    narrative_source: bool
    preferred_source: bool
    candidate_type: str = "investor_note_candidate"
    source_class: str = ""
    statement_class: str = ""
    parent_subject_key: str = ""
    canonical_subject_key: str = ""
    lifecycle_key: str = ""
    lifecycle_subject_key: str = ""
    evidence_role: str = "note"
    topic_family: str = ""
    confidence_score: float = 0.0
    route_reason: str = ""
    routing_reason: str = ""
    drop_reason: str = ""
    merge_reason: str = ""
    collapse_reason: str = ""
    conflict_resolution_reason: str = ""
    lifecycle_state: str = ""
    status_resolution_reason: str = ""
    quality_drop_reason: str = ""


@dataclass(frozen=True)
class EventRenderCandidate:
    event_key: str
    event_type: str
    metric_family: str
    entity_scope: str
    period_norm: str
    direction: str
    summary: str
    bucket: str
    display_score: float
    candidate_type: str = "investor_note_candidate"
    source_class: str = ""
    statement_class: str = ""
    parent_subject_key: str = ""
    canonical_subject_key: str = ""
    lifecycle_key: str = ""
    lifecycle_subject_key: str = ""
    evidence_role: str = "note"
    route_reason: str = ""
    merge_reason: str = ""
    collapse_reason: str = ""
    conflict_resolution_reason: str = ""
    lifecycle_state: str = ""
    status_resolution_reason: str = ""


@dataclass(frozen=True)
class FollowThroughSignal:
    event_key: str
    event_type: str
    metric_family: str
    entity_scope: str
    period_norm: str
    direction: str
    summary: str
    display_score: float
    source_type: str
    source_doc: str
    candidate_type: str = "follow_through_event"
    source_class: str = ""
    statement_class: str = ""
    parent_subject_key: str = ""
    canonical_subject_key: str = ""
    lifecycle_key: str = ""
    lifecycle_subject_key: str = ""
    evidence_role: str = "later_evidence"
    topic_family: str = ""
    confidence_score: float = 0.0
    route_reason: str = ""
    routing_reason: str = ""
    drop_reason: str = ""
    merge_reason: str = ""
    collapse_reason: str = ""
    conflict_resolution_reason: str = ""
    lifecycle_state: str = ""
    status_resolution_reason: str = ""
    quarter_end: str = ""


@dataclass(frozen=True)
class InvestorNoteCandidate:
    quarter: str
    source_type: str
    source_doc: str
    source_rank: int
    raw_text: str
    statement_summary: str
    candidate_type: str
    source_class: str
    statement_class: str
    metric_family: str
    entity_scope: str
    time_anchor: str
    target_period_norm: str
    promise_type: str
    drop_reason: str
    quality_drop_reason: str
    route_reason: str
    routing_reason: str
    parent_subject_key: str
    canonical_subject_key: str
    lifecycle_subject_key: str
    evidence_role: str
    lifecycle_state: str
    topic_family: str
    confidence_score: float
    event_key: str = ""
    lifecycle_key: str = ""
    merge_reason: str = ""
    collapse_reason: str = ""
    conflict_resolution_reason: str = ""
    status_resolution_reason: str = ""
    narrative_source: bool = False
    preferred_source: bool = False


@dataclass(frozen=True)
class MeasurablePromiseCandidate:
    quarter: str
    source_type: str
    source_doc: str
    source_rank: int
    raw_text: str
    statement_summary: str
    candidate_type: str
    source_class: str
    statement_class: str
    metric_family: str
    entity_scope: str
    time_anchor: str
    target_period_norm: str
    promise_type: str
    drop_reason: str
    quality_drop_reason: str
    route_reason: str
    routing_reason: str
    parent_subject_key: str
    canonical_subject_key: str
    lifecycle_subject_key: str
    evidence_role: str
    lifecycle_state: str
    topic_family: str
    confidence_score: float
    candidate_scope: str = ""
    event_key: str = ""
    lifecycle_key: str = ""
    merge_reason: str = ""
    collapse_reason: str = ""
    conflict_resolution_reason: str = ""
    status_resolution_reason: str = ""
    narrative_source: bool = False
    preferred_source: bool = False


@dataclass(frozen=True)
class FollowThroughEvent:
    quarter: str
    source_type: str
    source_doc: str
    source_rank: int
    raw_text: str
    statement_summary: str
    candidate_type: str
    source_class: str
    statement_class: str
    metric_family: str
    entity_scope: str
    time_anchor: str
    target_period_norm: str
    promise_type: str
    drop_reason: str
    quality_drop_reason: str
    route_reason: str
    routing_reason: str
    parent_subject_key: str
    canonical_subject_key: str
    lifecycle_subject_key: str
    evidence_role: str
    lifecycle_state: str
    topic_family: str
    confidence_score: float
    event_key: str = ""
    lifecycle_key: str = ""
    merge_reason: str = ""
    collapse_reason: str = ""
    conflict_resolution_reason: str = ""
    status_resolution_reason: str = ""
    narrative_source: bool = False
    preferred_source: bool = False


PREFERRED_NARRATIVE_SOURCE_RE = re.compile(
    r"(earnings_release|press_release|transcript|ceo|shareholder|presentation|slides|mda|management discussion)",
    re.I,
)

RENDERABLE_NARRATIVE_SOURCE_RE = re.compile(
    r"(earnings_release|press_release|transcript|ceo|shareholder|presentation|slides|mda|management discussion|10-q|10-k|8-k|html|pdf|filing)",
    re.I,
)

NON_RENDERABLE_SUPPORT_SOURCE_RE = re.compile(
    r"(ocr|table|appendix|raw|model_metric|non_gaap|revolver|debt_buckets|history_q)",
    re.I,
)

INTERNAL_RENDERABLE_SOURCE_TYPES = {
    "guidance_snapshot",
    "pbi_guidance_structured",
    "pbi_promise_structured",
    "pbi_quarter_notes_structured",
    "quarter_notes_ui",
    "promise_tracker_ui",
    "promise_progress_ui",
}

LEGAL_BOILERPLATE_RE = re.compile(
    r"\b("
    r"safe harbor|forward[- ]looking statements?|private securities litigation reform act|"
    r"registration statement|securities act|indenture|holders?\s+of\s+the\s+.*notes?|"
    r"administrative agent|base salary|target bonus|employment agreement|"
    r"conference call|webcast|cautionary statement|no obligation to update|"
    r"beneficial owner|compensation committee|stock option|vesting"
    r")\b",
    re.I,
)

TARGET_SCAFFOLD_RE = re.compile(
    r"\b("
    r"provided the following|management target|management targets|would be incremental to|"
    r"low revenue|high revenue|low adjusted|high adjusted|target assumptions?|"
    r"for reference[, ]+the company.?s previously disclosed guidance|"
    r"guidance ranges? are|provided the following guidance"
    r")\b",
    re.I,
)

TABLE_FRAGMENT_RE = re.compile(
    r"^\s*[A-Za-z][A-Za-z/&,\- ]{3,}\s+(?:[$(]?\d[\d,]*(?:\.\d+)?%?\)?\s+){2,}[A-Za-z0-9%$.,()\- ]*$",
    re.I,
)

LIST_MAP_FRAGMENT_RE = re.compile(
    r"\b(map|permit list|parcel|latitude|longitude|township|range|section|county map|site map)\b",
    re.I,
)

PROMISE_METRIC_RE = re.compile(
    r"\b("
    r"revenue|sales|adjusted ebitda|ebitda|adjusted ebit|ebit|eps|earnings per share|"
    r"free cash flow|fcf|capex|cost savings?|liquidity|deleverag|debt|leverage|"
    r"buyback|repurchase|dividend|45z|tax credit|sendtech|presort|segment|margin"
    r")\b",
    re.I,
)

PROMISE_TARGET_RE = re.compile(
    r"\b(target|targets|guidance|outlook|expect|expects|expected|plan|plans|forecast|goal|objective|opportunity)\b",
    re.I,
)

PROMISE_TIME_RE = re.compile(
    r"\b("
    r"q[1-4]\s*20\d{2}|fy\s*20\d{2}|20\d{2}|full[- ]?year|next quarter|next year|"
    r"by\b|through\b|until\b|over the next|during\b|annualized|run[- ]?rate"
    r")\b",
    re.I,
)

MILESTONE_ACTION_RE = re.compile(
    r"\b("
    r"complete|completed|close|closed|launch|launched|deliver|delivered|achieve|achieved|"
    r"begin|began|online|ramping|fully operational|execute|executed|implement|implemented|"
    r"repay|repaid|finalize|finalized|commissioning|construction progressing"
    r")\b",
    re.I,
)

NUMERIC_TOKEN_RE = re.compile(
    r"(?<![A-Za-z])(?:[$]|[+-]?\d[\d,]*(?:\.\d+)?\s*(?:%|x|bps|m|mm|bn|billion|million|b)?)",
    re.I,
)

VERB_RE = re.compile(
    r"\b("
    r"is|are|was|were|has|have|had|will|would|should|could|may|might|"
    r"improved|improve|improving|increased|increase|increasing|decreased|decrease|"
    r"declined|decline|expanded|expand|compressed|compress|released|release|"
    r"generated|generate|delivered|deliver|achieved|achieve|expects|expect|targets?|planned|plan|"
    r"repaid|repay|operational|ramping"
    r")\b",
    re.I,
)

SHORT_INVESTOR_PHRASE_RE = re.compile(
    r"\b("
    r"fully operational|online and ramping|agreement executed|risk management supports?|"
    r"utilization\b|liquidity\b|cash release|margin(?:s)?\b|45z|monetization|"
    r"cost savings?|debt repaid|deleverag|pricing|mix|volume"
    r")\b",
    re.I,
)

CLEAN_NUMERIC_BRIDGE_RE = re.compile(
    r"\b("
    r"bridge|included in|incremental|uplift|monetization|45z|rin sale|crush|"
    r"interest savings?|debt-paydown|liquidity release|cost savings?|"
    r"free cash flow|fcf(?:\s+ttm)?|net debt|revolver(?:\s+(?:availability|usage))?|"
    r"credit facility|adjusted ebitda|ebitda|margin(?:s)?"
    r")\b",
    re.I,
)


DEFAULT_DOC_INCLUDE_RE = re.compile(
    r"ex-?99|99\.1|99\.2|exhibit99|earnings|presentation|slides|shareholder|letter|guidance|outlook|"
    r"credit|facility|debt|revolver|liquidity|supplement|deck|press|release|10q|10k",
    re.I,
)


def is_quarter_end(d: Optional[date]) -> bool:
    return bool(d and (d.month, d.day) in {(3, 31), (6, 30), (9, 30), (12, 31)})


def coerce_prev_quarter_end(d: Optional[date]) -> Optional[date]:
    if d is None:
        return None
    if d.month <= 3:
        q_end = date(d.year, 3, 31)
    elif d.month <= 6:
        q_end = date(d.year, 6, 30)
    elif d.month <= 9:
        q_end = date(d.year, 9, 30)
    else:
        q_end = date(d.year, 12, 31)
    if d >= q_end:
        return q_end
    if q_end.month == 3:
        return date(d.year - 1, 12, 31)
    if q_end.month == 6:
        return date(d.year, 3, 31)
    if q_end.month == 9:
        return date(d.year, 6, 30)
    return date(d.year, 9, 30)


def coerce_next_quarter_end(d: Optional[date]) -> Optional[date]:
    if d is None:
        return None
    if d.month <= 3:
        q_end = date(d.year, 3, 31)
    elif d.month <= 6:
        q_end = date(d.year, 6, 30)
    elif d.month <= 9:
        q_end = date(d.year, 9, 30)
    else:
        q_end = date(d.year, 12, 31)
    if d <= q_end:
        return q_end
    if q_end.month == 3:
        return date(d.year, 6, 30)
    if q_end.month == 6:
        return date(d.year, 9, 30)
    if q_end.month == 9:
        return date(d.year, 12, 31)
    return date(d.year + 1, 3, 31)


def filing_quarter_end(form: Any, report_date: Any, filing_date: Any) -> Optional[date]:
    q_end = parse_date(report_date) or parse_date(filing_date)
    if is_quarter_end(q_end):
        return q_end
    return coerce_prev_quarter_end(q_end)


def iter_submission_batches(sec: SecClient, submissions: Dict[str, Any]) -> List[Dict[str, Any]]:
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
    for entry in files:
        name = entry.get("name")
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


def confidence_rank(value: Any) -> int:
    return {"high": 3, "med": 2, "low": 1}.get(str(value or "").strip().lower(), 0)


def decode_blob_text(blob: bytes) -> str:
    if not blob:
        return ""
    best_txt = ""
    best_score = -10**12
    for enc in ("utf-8", "utf-8-sig", "cp1252", "latin-1"):
        try:
            txt = blob.decode(enc, errors="strict")
        except Exception:
            txt = blob.decode(enc, errors="ignore")
        letters = len(re.findall(r"[A-Za-z]", txt))
        bad = txt.count("\ufffd")
        score = letters - (bad * 50) - abs(len(txt) - len(blob)) * 0.01
        if score > best_score:
            best_score = score
            best_txt = txt
    return best_txt


def extract_document_text(doc_name: str, blob: bytes, *, quiet_pdf_warnings: bool = True) -> str:
    ext = Path(doc_name).suffix.lower()
    raw = decode_blob_text(blob)
    if ext in {".htm", ".html", ".xhtml", ".xml"}:
        html_src = re.sub(r"<script[^>]*>.*?</script>", " ", raw, flags=re.I | re.S)
        html_src = re.sub(r"<style[^>]*>.*?</style>", " ", html_src, flags=re.I | re.S)
        html_src = re.sub(r"(?i)<br\s*/?>", ". ", html_src)
        html_src = re.sub(r"(?i)</(p|div|li|tr|h[1-6]|table|td|th)>", ". ", html_src)
        txt = strip_html(html_src)
    elif ext == ".txt":
        txt = raw
    elif ext == ".pdf":
        try:
            import pdfplumber  # type: ignore
        except Exception:
            return ""
        try:
            with silence_pdfminer_warnings(enabled=quiet_pdf_warnings):
                with pdfplumber.open(io.BytesIO(blob)) as pdf:
                    txt = "\n".join((pg.extract_text() or "") for pg in pdf.pages)
        except Exception:
            txt = ""
    else:
        txt = raw
    txt = html.unescape(txt).replace("\xa0", " ")
    return re.sub(r"\s+", " ", txt).strip()


def format_pct(value: Optional[float]) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{float(value) * 100:.1f}%"


def split_sentences(text: str, *, min_len: int = 30, max_len: int = 500) -> List[str]:
    if not text:
        return []
    txt = re.sub(r"\s+", " ", text).strip()
    sents = re.split(r"(?<=[\.\?!])\s+", txt)
    out: List[str] = []
    for sent in sents:
        sent_clean = sent.strip()
        if len(sent_clean) < min_len:
            continue
        out.append(sent_clean[:max_len])
    return out


def is_preferred_narrative_source(source_type: Any) -> bool:
    src = str(source_type or "").strip().lower()
    if src in INTERNAL_RENDERABLE_SOURCE_TYPES:
        return True
    return bool(src and PREFERRED_NARRATIVE_SOURCE_RE.search(src))


def is_renderable_narrative_source(source_type: Any) -> bool:
    src = str(source_type or "").strip().lower()
    if not src:
        return False
    if src in INTERNAL_RENDERABLE_SOURCE_TYPES:
        return True
    if NON_RENDERABLE_SUPPORT_SOURCE_RE.search(src):
        return False
    return bool(RENDERABLE_NARRATIVE_SOURCE_RE.search(src))


def _clean_summary_text(text: str, *, max_len: int = 220) -> str:
    cleaned = re.sub(r"^[\u2022*\-]+\s*", "", str(text or "")).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    if len(cleaned) > max_len:
        cleaned = cleaned[: max_len - 1].rstrip(" ,;:-") + "..."
    if cleaned and cleaned[-1] not in ".!?":
        cleaned = cleaned.rstrip(" ,;:-") + "."
    return cleaned


def normalize_quality_drop_reason(reason: Any) -> str:
    raw = str(reason or "").strip().lower()
    return {
        "": "",
        "empty_text": "fragmentary_text",
        "legal_boilerplate": "legal_boilerplate",
        "target_scaffolding": "scaffolding",
        "tabular_fragment": "table_fragment",
        "list_map_fragment": "fragmentary_text",
        "non_renderable_source": "unclear_source",
        "fragmentary_text": "fragmentary_text",
        "no_clean_summary": "no_clean_summary",
        "not_investor_relevant": "not_investor_relevant",
    }.get(raw, raw or "fragmentary_text")


def _is_short_investor_phrase(text: Any) -> bool:
    txt = re.sub(r"\s+", " ", str(text or "")).strip(" -:;,.")
    if not txt:
        return False
    if LEGAL_BOILERPLATE_RE.search(txt) or TARGET_SCAFFOLD_RE.search(txt):
        return False
    if looks_like_tabular_fragment(txt) or LIST_MAP_FRAGMENT_RE.search(txt):
        return False
    alpha_words = re.findall(r"[A-Za-z]{3,}", txt)
    if not (3 <= len(alpha_words) <= 14):
        return False
    if SHORT_INVESTOR_PHRASE_RE.search(txt):
        return True
    if VERB_RE.search(txt) and re.search(
        r"\b(margin|cash flow|liquidity|utilization|operational|ramping|pricing|mix|demand|volume|guidance|target|45z)\b",
        txt,
        re.I,
    ):
        return True
    return False


def _is_clean_numeric_bridge(text: Any) -> bool:
    txt = re.sub(r"\s+", " ", str(text or "")).strip(" -:;,.")
    if not txt:
        return False
    if LEGAL_BOILERPLATE_RE.search(txt) or TARGET_SCAFFOLD_RE.search(txt):
        return False
    if LIST_MAP_FRAGMENT_RE.search(txt):
        return False
    numeric_hits = NUMERIC_TOKEN_RE.findall(txt)
    if not numeric_hits:
        return False
    if not CLEAN_NUMERIC_BRIDGE_RE.search(txt):
        return False
    alpha_words = re.findall(r"[A-Za-z]{3,}", txt)
    if len(alpha_words) < 3:
        return False
    if looks_like_tabular_fragment(txt):
        return bool(
            len(numeric_hits) <= 4
            and re.search(
                r"\b(included|include|incremental|uplift|supports?|reflects?|driven|bridge|monetization|sale|savings?|release|"
                r"free cash flow|fcf(?:\s+ttm)?|net debt|revolver(?:\s+(?:availability|usage))?|credit facility|adjusted ebitda|ebitda|margin(?:s)?)\b",
                txt,
                re.I,
            )
        )
    return True


def _first_clean_sentence(text: Any, *, max_len: int = 220) -> str:
    candidates = split_sentences(str(text or ""), min_len=20, max_len=max_len + 80)
    if candidates:
        return _clean_summary_text(candidates[0], max_len=max_len)
    return _clean_summary_text(str(text or ""), max_len=max_len)


def looks_like_tabular_fragment(text: Any) -> bool:
    txt = str(text or "").strip()
    if not txt:
        return False
    if VERB_RE.search(txt) and re.search(r"[.!?]", txt) and len(re.findall(r"[A-Za-z]{3,}", txt)) >= 4:
        return False
    if TABLE_FRAGMENT_RE.search(txt):
        return True
    numeric_hits = NUMERIC_TOKEN_RE.findall(txt)
    punctuation_hits = len(re.findall(r"[.;:]", txt))
    if len(numeric_hits) >= 4 and punctuation_hits <= 1 and len(re.findall(r"[A-Za-z]{3,}", txt)) <= 12:
        return True
    return False


def looks_like_fragmentary_text(text: Any) -> bool:
    txt = str(text or "").strip()
    if not txt:
        return True
    if _is_clean_numeric_bridge(txt) or _is_short_investor_phrase(txt):
        return False
    if looks_like_tabular_fragment(txt):
        return True
    if LIST_MAP_FRAGMENT_RE.search(txt):
        return True
    if TARGET_SCAFFOLD_RE.search(txt):
        return True
    alpha_words = re.findall(r"[A-Za-z]{3,}", txt)
    if VERB_RE.search(txt) and re.search(r"[.!?]", txt) and len(alpha_words) >= 4:
        return False
    if len(alpha_words) < 4:
        return True
    if not VERB_RE.search(txt):
        return True
    if txt[:1].islower() and len(txt) < 120:
        return True
    return False


def narrative_drop_reason(text: Any, source_type: Any) -> str:
    txt = str(text or "").strip()
    if not txt:
        return normalize_quality_drop_reason("empty_text")
    if LEGAL_BOILERPLATE_RE.search(txt):
        return normalize_quality_drop_reason("legal_boilerplate")
    if TARGET_SCAFFOLD_RE.search(txt):
        return normalize_quality_drop_reason("target_scaffolding")
    if looks_like_tabular_fragment(txt) and not _is_clean_numeric_bridge(txt):
        return normalize_quality_drop_reason("tabular_fragment")
    if LIST_MAP_FRAGMENT_RE.search(txt):
        return normalize_quality_drop_reason("list_map_fragment")
    if not is_renderable_narrative_source(source_type) and not _is_clean_numeric_bridge(txt):
        return normalize_quality_drop_reason("non_renderable_source")
    if looks_like_fragmentary_text(txt):
        return normalize_quality_drop_reason("fragmentary_text")
    return ""


def renderable_note_drop_reason(text: Any, *, source_type: Any) -> str:
    txt = str(text or "").strip()
    base_reason = narrative_drop_reason(txt, source_type)
    if base_reason:
        return base_reason
    summary = _first_clean_sentence(txt)
    if not summary:
        return normalize_quality_drop_reason("no_clean_summary")
    if looks_like_fragmentary_text(summary) and not (
        _is_short_investor_phrase(summary) or _is_clean_numeric_bridge(summary)
    ):
        return normalize_quality_drop_reason("no_clean_summary")
    return ""


def promise_candidate_drop_reason(
    text: Any,
    *,
    source_type: Any,
    metric_hint: Any = "",
) -> str:
    txt = str(text or "").strip()
    base_reason = renderable_note_drop_reason(txt, source_type=source_type)
    if base_reason:
        return base_reason
    stmt_class = statement_class(txt, source_type=source_type, metric_hint=metric_hint)
    src_class = source_class(source_type)
    blob = " | ".join([str(metric_hint or ""), txt])
    has_metric = bool(PROMISE_METRIC_RE.search(blob))
    metric_hint_low = str(metric_hint or "").strip().lower()
    has_target_language = bool(PROMISE_TARGET_RE.search(txt))
    has_time = bool(PROMISE_TIME_RE.search(txt))
    has_numeric = bool(NUMERIC_TOKEN_RE.search(txt))
    has_milestone = bool(MILESTONE_ACTION_RE.search(txt) and has_time)
    has_bridge_amount = bool(re.search(r"\$\s*\d|\b\d+(?:\.\d+)?\s*(?:million|billion|m|bn|%|x|bps)\b", txt, re.I))
    short_phrase_milestone = bool((has_metric or "strategic milestone" in metric_hint_low) and _is_short_investor_phrase(txt) and MILESTONE_ACTION_RE.search(txt))
    blob_low = blob.lower()
    if re.search(r"^\s*(year ended|evaluation\b|in all of our\b|company has\b)", metric_hint_low, re.I):
        return normalize_quality_drop_reason("not_investor_relevant")
    if re.search(r"\b(federal r&d|research and development credits|federal research and development|net operating losses?)\b", blob_low, re.I):
        if not re.search(r"\b(target|guidance|outlook|expected|qualify|opportunity|monetization)\b", blob_low, re.I):
            return normalize_quality_drop_reason("not_investor_relevant")
    if re.search(r"\bat december 31\b", blob_low, re.I) and not re.search(r"\b(target|guidance|outlook|expected|qualify|opportunity)\b", blob_low, re.I):
        return normalize_quality_drop_reason("not_investor_relevant")
    if stmt_class in {"boilerplate", "scaffolding", "fragmentary_text", "weak_forward_looking"}:
        return normalize_quality_drop_reason("too_vague_for_tracker")
    if src_class in {"weak_support", "support"} and not _is_clean_numeric_bridge(txt):
        return normalize_quality_drop_reason("too_vague_for_tracker")
    if not ((has_metric and has_target_language and has_time and has_numeric) or has_milestone):
        if has_metric and _is_clean_numeric_bridge(txt) and has_bridge_amount and (has_numeric or has_target_language):
            return ""
        if short_phrase_milestone:
            return ""
        if has_metric and not has_time:
            return normalize_quality_drop_reason("no_time_anchor")
        return normalize_quality_drop_reason("not_investor_relevant")
    return ""


def _note_bucket(metric_hint: Any, theme_hint: Any, text: Any, *, quietly_removed: bool = False) -> str:
    blob = " | ".join([str(metric_hint or ""), str(theme_hint or ""), str(text or "")]).lower()
    if quietly_removed:
        return "Quietly removed"
    if re.search(r"\b(guidance|outlook|target|expect|forecast|plan|reaffirm|raised|lowered|tightened)\b", blob, re.I):
        return "Guidance / outlook"
    if re.search(r"\b(revenue|ebitda|ebit|eps|margin|improv|worsen|declin|increase|decrease|headwind|tailwind)\b", blob, re.I):
        return "Better / worse vs prior"
    if re.search(r"\b(sendtech|presort|segment|pricing|volume|mix|utilization|operations?|ramping|online|fully operational)\b", blob, re.I):
        return "Operational drivers"
    if re.search(r"\b(cash|fcf|free cash flow|liquidity|debt|leverage|deleverag|revolver|maturity)\b", blob, re.I):
        return "Cash / liquidity / leverage"
    if re.search(r"\b(confiden|cautious|optimis|tone|visibility|demand)\b", blob, re.I):
        return "Management tone / confidence"
    if re.search(r"\b(45z|tax credit|permit|advantage|obion|hidden|quiet)\b", blob, re.I):
        return "Hidden but important"
    return "Operational drivers"


def _topic_family_from_bucket(bucket: Any) -> str:
    bucket_txt = str(bucket or "").strip().lower()
    return {
        "guidance / outlook": "guidance",
        "better / worse vs prior": "results",
        "operational drivers": "operations",
        "cash / liquidity / leverage": "capital_structure",
        "management tone / confidence": "tone",
        "hidden but important": "hidden_signal",
        "quietly removed": "quietly_removed",
    }.get(bucket_txt, bucket_txt or "general")


def source_class(source_type: Any) -> str:
    src = str(source_type or "").strip().lower()
    if not src:
        return "unknown"
    if is_preferred_narrative_source(src):
        return "preferred_narrative"
    if is_renderable_narrative_source(src):
        return "narrative"
    if re.search(r"\b(ocr|raw|appendix)\b", src, re.I):
        return "weak_support"
    if re.search(r"\b(table|bridge|segment table|waterfall)\b", src, re.I):
        return "structured_numeric"
    if src in INTERNAL_RENDERABLE_SOURCE_TYPES:
        return "internal_structured"
    return "support"


def statement_class(text: Any, source_type: Any = "", metric_hint: Any = "") -> str:
    blob = " | ".join([str(metric_hint or ""), str(source_type or ""), str(text or "")])
    if LEGAL_BOILERPLATE_RE.search(blob):
        return "boilerplate"
    if TARGET_SCAFFOLD_RE.search(blob):
        return "scaffolding"
    if looks_like_tabular_fragment(blob):
        return "table_fragment"
    if looks_like_fragmentary_text(blob):
        return "fragmentary_text"
    if re.search(r"\b(risk factor|risks include|subject to risks?|may|could|might|intend to evaluate|considering)\b", blob, re.I):
        return "weak_forward_looking"
    if _is_clean_numeric_bridge(blob):
        return "structured_numeric_bridge"
    if _is_short_investor_phrase(blob):
        return "investor_phrase"
    return "narrative"


def build_parent_subject_key(
    *,
    entity_scope: Any,
    metric_family: Any,
    program_token: Any = "",
    topic_family: Any = "",
) -> str:
    parts = [
        _event_slug(program_token or entity_scope or "company_total"),
        _event_slug(topic_family or metric_family or "general"),
    ]
    while parts and parts[-1] == "na":
        parts.pop()
    return "|".join(parts) or "company_total|general"


def _lifecycle_stage_token(stage_token: Any) -> str:
    stage = _event_slug(stage_token)
    if stage in {"startup_or_commissioning", "online_or_operational"}:
        return "operationalization"
    if stage in {"monetization_agreement", "monetization_realization"}:
        return "monetization"
    if stage in {"facility_target", "program_target"}:
        return "target"
    return stage or "base"


def build_lifecycle_subject_key(
    *,
    parent_subject_key: Any,
    canonical_subject_key: Any,
    stage_token: Any = "",
    target_period_norm: Any = "",
) -> str:
    canonical = str(canonical_subject_key or "").strip()
    if not canonical:
        canonical = str(parent_subject_key or "").strip()
    lifecycle_stage = _lifecycle_stage_token(stage_token)
    period_key = _event_slug(target_period_norm or "open")
    return "|".join([canonical or "general", lifecycle_stage or "base", period_key])


def evidence_role(
    candidate_type: Any,
    *,
    route_reason: Any = "",
    promise_type: Any = "",
    current_status: Any = "",
) -> str:
    candidate = str(candidate_type or "").strip().lower()
    route = str(route_reason or "").strip().lower()
    promise = str(promise_type or "").strip().lower()
    status = str(current_status or "").strip().lower()
    if candidate == "investor_note_candidate" or route == "quarter_notes":
        return "note"
    if candidate == "follow_through_event" or route == "promise_progress":
        if promise == "guidance_range" and status in {"resolved_pass", "resolved_beat", "resolved_fail", "beat", "missed", "broken", "completed"}:
            return "result_evidence"
        return "later_evidence"
    if promise == "guidance_range":
        return "promise_origin"
    return "promise_origin"


def classify_statement_evidence_role(
    text: Any,
    *,
    source_type: Any = "",
    metric_hint: Any = "",
    target_period_norm: Any = "",
    promise_type: Any = "",
) -> Tuple[str, str]:
    txt = re.sub(r"\s+", " ", str(text or "")).strip()
    src_class = source_class(source_type)
    stmt_class = statement_class(txt, source_type=source_type, metric_hint=metric_hint)
    blob = " | ".join([str(metric_hint or ""), str(promise_type or ""), str(target_period_norm or ""), txt]).lower()
    if stmt_class in {"boilerplate", "scaffolding", "table_fragment"}:
        return "broad_note_only", stmt_class
    if src_class in {"weak_support", "support"} and stmt_class != "structured_numeric_bridge":
        return "broad_note_only", "weak_follow_through_link"

    result_verbs = re.compile(
        r"\b("
        r"fully operational|fully online|online and ramping|agreement executed|repaid|repayment completed|"
        r"reduced debt|reducing debt|debt reduced|principal debt reduction|reduced principal debt|reducing principal debt|"
        r"achieved|completed|expanded margin|margin expanded|"
        r"operating expenses declined|opex declined|repurchased|repurchasing|share repurchase|buyback authorization increased|"
        r"authorization increased|remaining capacity|capacity remaining"
        r")\b",
        re.I,
    )
    future_anchor = re.compile(
        r"\b(target|guidance|outlook|expect|expected|on track|plan|plans|will|by end of|by q[1-4]|"
        r"fy\s*20\d{2}|full[- ]?year|q[1-4]\s*20\d{2}|deadline|goal)\b",
        re.I,
    )
    weak_strategy = re.compile(
        r"\b(optimistic|continue to evaluate|evaluate strategic options|may|could|might|believe)\b",
        re.I,
    )
    if stmt_class == "fragmentary_text":
        strong_fragment_signal = bool(
            re.search(
                r"\b("
                r"fully operational|fully online|online and ramping|agreement executed|repaid|repayment completed|"
                r"reduced debt|reducing debt|principal debt reduction|reduced principal debt|reducing principal debt|"
                r"achieved|completed|expanded margin|margin expanded|operating expenses declined|opex declined|"
                r"repurchas\w*|buyback authorization increased|authorization increased|remaining capacity|capacity remaining"
                r")\b",
                blob,
                re.I,
            )
        )
        if not strong_fragment_signal:
            return "broad_note_only", "fragmentary_text"
    has_time = bool(PROMISE_TIME_RE.search(txt) or str(target_period_norm or "").strip())
    has_numeric = bool(NUMERIC_TOKEN_RE.search(txt))
    has_target = bool(PROMISE_TARGET_RE.search(txt))
    if result_verbs.search(blob):
        if re.search(
            r"\b("
            r"expanded margin|margin expanded|operating expenses declined|opex declined|"
            r"repurchas\w*|reduced debt|reducing debt|principal debt reduction|reduced principal debt|reducing principal debt|"
            r"repaid|repayment completed|achieved|authorization increased|remaining capacity|capacity remaining"
            r")\b",
            blob,
            re.I,
        ):
            return "result_evidence", ""
        if future_anchor.search(blob) and not re.search(r"\b(agreement executed|repaid|fully operational|completed)\b", blob, re.I):
            return "broad_note_only", "too_soft_for_tracker"
        return "later_evidence", ""
    qualified = qualify_promise_candidate(txt, source_type=source_type, metric_hint=metric_hint)
    if qualified is not None:
        return "promise_origin", ""
    if weak_strategy.search(blob):
        return "broad_note_only", "too_soft_for_tracker"
    if has_numeric and not has_time and not has_target:
        return "result_evidence", ""
    return "broad_note_only", "too_soft_for_tracker"


def source_rank(source_type: Any) -> int:
    src = str(source_type or "").strip().lower()
    if not src:
        return 0
    if is_preferred_narrative_source(src):
        return 3
    if is_renderable_narrative_source(src):
        return 2
    if src in INTERNAL_RENDERABLE_SOURCE_TYPES:
        return 2
    return 1


def progress_status_rank(status: Any) -> int:
    status_txt = str(status or "").strip().lower().replace(" ", "_")
    ranks = {
        "beat": 90,
        "completed": 88,
        "achieved": 86,
        "resolved_beat": 84,
        "resolved_pass": 82,
        "on_track": 70,
        "in_progress": 68,
        "pending": 55,
        "info": 40,
        "delayed": 25,
        "resolved_fail": 20,
        "missed": 18,
        "broken": 16,
        "not_observed": 12,
    }
    return ranks.get(status_txt, 0)


def _period_norm_end_date(period_norm: Any) -> Optional[date]:
    period_txt = str(period_norm or "").strip().upper()
    if not period_txt or period_txt in {"UNK", "TIME_ANCHOR", "PROGRAM", "OPEN", "FALLBACK"}:
        return None
    m_q = re.fullmatch(r"Q(20\d{2})Q([1-4])", period_txt)
    if m_q:
        year_num = int(m_q.group(1))
        quarter_num = int(m_q.group(2))
        month_num = quarter_num * 3
        month_end = pd.Timestamp(year=year_num, month=month_num, day=1).days_in_month
        return date(year_num, month_num, int(month_end))
    m_fy = re.fullmatch(r"FY(20\d{2})", period_txt)
    if m_fy:
        return date(int(m_fy.group(1)), 12, 31)
    parsed = parse_date(period_txt)
    if parsed is None or pd.isna(parsed):
        return None
    if isinstance(parsed, pd.Timestamp):
        return parsed.date()
    return parsed


def _safe_parse_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    value_txt = str(value).strip().lower()
    if value_txt in {"", "nat", "nan", "none"}:
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.notna(ts):
        return pd.Timestamp(ts).date()
    parsed = parse_date(value)
    if parsed is None or pd.isna(parsed):
        return None
    if isinstance(parsed, pd.Timestamp):
        if pd.isna(parsed):
            return None
        return parsed.date()
    return parsed


def derive_lifecycle_state(
    *,
    target_period_norm: Any = "",
    stated_quarter: Any = "",
    latest_evidence_quarter: Any = "",
    evaluated_through_quarter: Any = "",
    carried_to_quarter: Any = "",
    current_status: Any = "",
    superseded: bool = False,
    explicitly_closed: bool = False,
) -> str:
    if superseded:
        return "superseded"
    status_txt = str(current_status or "").strip().lower().replace("_", " ")
    if explicitly_closed or status_txt in {
        "beat",
        "completed",
        "achieved",
        "resolved beat",
        "resolved pass",
        "resolved fail",
        "missed",
        "broken",
    }:
        return "resolved"
    stated_q = _safe_parse_date(stated_quarter)
    latest_q = _safe_parse_date(latest_evidence_quarter)
    eval_q = _safe_parse_date(evaluated_through_quarter)
    carried_q = _safe_parse_date(carried_to_quarter)
    period_end = _period_norm_end_date(target_period_norm)
    if latest_q is not None and stated_q is not None and latest_q > stated_q and status_txt not in {"pending", "open"}:
        return "updated_by_later_evidence"
    if period_end is not None and eval_q is not None and eval_q < period_end:
        return "pending_period_end"
    if carried_q is not None and latest_q is not None and carried_q > latest_q:
        return "carried_forward"
    if (
        period_end is not None
        and eval_q is not None
        and eval_q > period_end
        and status_txt in {"pending", "open", "on track", "in progress", "info", "no actual available", "unknown no signal", "unclear"}
    ):
        return "expired_or_stale"
    if stated_q is not None:
        return "stated"
    return ""


def derive_status_resolution_reason(
    *,
    current_status: Any = "",
    latest_value: Any = "",
    lifecycle_state: Any = "",
) -> str:
    status_txt = str(current_status or "").strip().lower().replace("_", " ")
    lifecycle_txt = str(lifecycle_state or "").strip().lower()
    latest_txt = str(latest_value or "").strip()
    latest_num = pd.to_numeric(latest_value, errors="coerce")
    if status_txt in {
        "completed",
        "achieved",
        "beat",
        "resolved beat",
        "resolved pass",
        "resolved fail",
        "missed",
        "broken",
    }:
        if pd.notna(latest_num) or re.search(r"\b(fully operational|agreement executed|online and ramping|completed|repaid)\b", latest_txt, re.I):
            return "actual_over_text_progress"
        return "completed_over_in_progress"
    if lifecycle_txt == "pending_period_end":
        return "pending_until_period_end"
    return ""


def infer_target_period_norm(
    *,
    period_norm: Any = "",
    deadline: Any = None,
    target_year: Any = None,
    quarter: Any = None,
    text: Any = "",
) -> str:
    explicit = str(period_norm or "").strip().upper()
    if explicit and explicit not in {"UNK", "TIME_ANCHOR"}:
        return explicit
    qd = parse_date(deadline)
    if qd is not None:
        if is_quarter_end(qd):
            qn = ((int(qd.month) - 1) // 3) + 1
            return f"Q{int(qd.year)}Q{int(qn)}"
        if qd.month == 12 and qd.day >= 28:
            return f"FY{int(qd.year)}"
    try:
        year_num = int(target_year) if target_year is not None and str(target_year).strip() else None
    except Exception:
        year_num = None
    if year_num is not None and 1990 <= year_num <= 2100:
        return f"FY{year_num}"
    txt = str(text or "")
    m_q = re.search(r"\bQ([1-4])\s*(20\d{2})\b", txt, re.I)
    if m_q:
        return f"Q{int(m_q.group(2))}Q{int(m_q.group(1))}"
    m_fy = re.search(r"\b(?:FY|full[- ]?year|fiscal year)\s*(20\d{2})\b", txt, re.I)
    if m_fy:
        return f"FY{int(m_fy.group(1))}"
    m_year = re.search(r"\b(20\d{2})\b", txt)
    if m_year and re.search(r"\b(by|through|during|in|for|annualized|run[- ]?rate)\b", txt, re.I):
        return f"FY{int(m_year.group(1))}"
    q_from = parse_date(quarter)
    if q_from is not None and is_quarter_end(q_from):
        qn = ((int(q_from.month) - 1) // 3) + 1
        return f"Q{int(q_from.year)}Q{int(qn)}"
    return ""


def _canonical_stage_token(event_type: Any, metric_family: Any, text: Any, promise_type: Any = "") -> str:
    blob = " | ".join([str(event_type or ""), str(metric_family or ""), str(promise_type or ""), str(text or "")]).lower()
    if re.search(r"\b(45z|tax credit)\b", blob, re.I):
        if re.search(r"\b(agreement executed|executed agreement|monetization agreement)\b", blob, re.I):
            return "monetization_agreement"
        if re.search(r"\b(realized|realisation|realization|ebitda|monetization|value)\b", blob, re.I):
            return "monetization_realization"
        if re.search(r"\b(qualif|qualified|qualify|all eight operating plants)\b", blob, re.I):
            return "qualification"
    if re.search(r"\b(fully operational|fully online|online and ramping|online)\b", blob, re.I):
        return "online_or_operational"
    if re.search(r"\b(commissioning|construction progressing|under construction|start-?up|startup|compression infrastructure|infrastructure delivered)\b", blob, re.I):
        return "startup_or_commissioning"
    if re.search(r"\b(readiness|ready|prepare|prepared)\b", blob, re.I):
        return "readiness"
    if re.search(r"\b(agreement executed)\b", blob, re.I):
        return "monetization_agreement"
    if re.search(r"\b(deleverag|repaid|repayment|paydown|debt reduction)\b", blob, re.I):
        return "debt_reduction"
    if re.search(r"\b(cost savings|cost reduction|annualized savings|run[- ]?rate)\b", blob, re.I):
        return "cost_savings"
    if re.search(r"\b(pb bank|bank-held leases|cash optimization|cash release|trapped capital)\b", blob, re.I):
        return "liquidity_release"
    if re.search(r"\b(guidance|target|forecast|outlook)\b", blob, re.I):
        if re.search(r"\b(york|obion|central city|wood river|sendtech|presort|facility|plant)\b", blob, re.I):
            return "facility_target"
        return "program_target"
    if str(promise_type or "").strip().lower() in {"clean_milestone", "milestone"}:
        return "startup_or_commissioning"
    if str(promise_type or "").strip().lower() in {"hard_target", "guidance_range"}:
        return "program_target"
    return ""


def build_canonical_subject_key(
    *,
    entity_scope: Any,
    metric_family: Any,
    target_period_norm: Any = "",
    scope_token: Any = "",
    program_token: Any = "",
    stage_token: Any = "",
) -> str:
    parts = [
        _event_slug(metric_family),
        _event_slug(entity_scope),
        _event_slug(target_period_norm or "open"),
        _event_slug(scope_token),
        _event_slug(program_token),
        _event_slug(stage_token),
    ]
    while parts and parts[-1] == "na":
        parts.pop()
    return "|".join(parts) or "general|company_total|open"


def build_promise_lifecycle_key(
    canonical_subject_key: Any,
    *,
    stage_token: Any = "",
    promise_type: Any = "",
) -> str:
    base = str(canonical_subject_key or "").strip()
    stage = _event_slug(stage_token or promise_type or "base")
    if not base:
        return stage
    return f"{base}|{stage}"


def _change_state(text: Any) -> str:
    txt = str(text or "").lower()
    if re.search(r"\b(raised|raising|increased|higher|tightened)\b", txt, re.I):
        return "raised"
    if re.search(r"\b(lowered|reduced|cut|weaker)\b", txt, re.I):
        return "lowered"
    if re.search(r"\b(reaffirm|reiterat|maintain(?:ed)? guidance)\b", txt, re.I):
        return "reaffirmed"
    if re.search(r"\b(improv|ahead of plan|beat|better)\b", txt, re.I):
        return "improved"
    if re.search(r"\b(worsen|behind plan|miss|pressure|headwind)\b", txt, re.I):
        return "worsened"
    return "new"


def qualify_renderable_note(
    text: Any,
    *,
    source_type: Any,
    metric_hint: Any = "",
    theme_hint: Any = "",
    base_score: float = 0.0,
    quietly_removed: bool = False,
) -> Optional[RenderableNote]:
    drop_reason = renderable_note_drop_reason(text, source_type=source_type)
    if drop_reason:
        return None
    summary = _first_clean_sentence(text)
    if not summary:
        return None
    is_clean_bridge = _is_clean_numeric_bridge(text) or _is_clean_numeric_bridge(summary)
    is_short_phrase = _is_short_investor_phrase(summary) or _is_short_investor_phrase(text)
    if looks_like_fragmentary_text(summary) and not (is_clean_bridge or is_short_phrase):
        return None
    preferred_source = is_preferred_narrative_source(source_type)
    narrative_source = is_renderable_narrative_source(source_type)
    score = float(base_score or 0.0)
    if preferred_source:
        score += 8.0
    change_state = _change_state(summary)
    if change_state in {"raised", "lowered", "reaffirmed", "improved", "worsened"}:
        score += 6.0
    investor_bucket = _note_bucket(metric_hint, theme_hint, summary, quietly_removed=quietly_removed)
    if investor_bucket in {
        "Guidance / outlook",
        "Cash / liquidity / leverage",
        "Better / worse vs prior",
        "Operational drivers",
        "Hidden but important",
    }:
        score += 6.0
    if quietly_removed or investor_bucket == "Hidden but important":
        score += 4.0
    if is_clean_bridge:
        score += 5.0
    fragment_penalty = 0.0
    if len(summary) > 220:
        fragment_penalty += 10.0
    if len(NUMERIC_TOKEN_RE.findall(summary)) >= 4:
        fragment_penalty += 10.0
    score = max(0.0, min(100.0, score - fragment_penalty))
    return RenderableNote(
        summary=summary,
        bucket=investor_bucket,
        display_score=score,
        change_state=change_state,
        source_type=str(source_type or ""),
        narrative_source=narrative_source,
        preferred_source=preferred_source,
        candidate_type="investor_note_candidate",
        route_reason="quarter_notes",
        quality_drop_reason="",
    )


def _event_metric_family(metric_hint: Any, text: Any) -> str:
    blob = " | ".join([str(metric_hint or ""), str(text or "")]).lower()
    checks = [
        ("risk_management", r"\brisk management\b"),
        ("revenue", r"\b(revenue|sales|volume)\b"),
        ("adj_ebit", r"\b(adjusted ebit|adj\.?\s*ebit|ebit)\b"),
        ("eps", r"\b(eps|earnings per share)\b"),
        ("fcf", r"\b(fcf|free cash flow|cash flow)\b"),
        ("cost_savings", r"\b(cost savings?|cost reduction|annualized savings|run-rate savings)\b"),
        ("liquidity", r"\b(pb bank|liquidity|bank-held leases|cash optimization|cash release|trapped capital)\b"),
        ("debt", r"\b(deleverag|debt|leverage|repay|repayment|paydown)\b"),
        ("utilization", r"\butilization|stated capacity|operating rate\b"),
        ("regulatory_credit", r"\b45z|tax credit|monetization\b"),
        ("segment_ops", r"\b(sendtech|presort|segment|pricing|mix)\b"),
        ("milestone", r"\b(fully operational|online and ramping|agreement executed|commissioning|construction progressing|online)\b"),
    ]
    for family, pat in checks:
        if re.search(pat, blob, re.I):
            return family
    return "general"


def _event_entity_scope(metric_hint: Any, text: Any) -> str:
    blob = " | ".join([str(metric_hint or ""), str(text or "")]).lower()
    checks = [
        ("pb_bank", r"\bpb bank|bank-held leases|receivables purchase\b"),
        ("sendtech", r"\bsendtech\b"),
        ("presort", r"\bpresort\b"),
        ("york", r"\byork\b"),
        ("central_city_wood_river", r"\bcentral city\b.*\bwood river\b|\bwood river\b.*\bcentral city\b"),
        ("obion", r"\bobion\b"),
        ("advantage_nebraska", r"\badvantage nebraska\b"),
        ("company_total", r"\bcompany|corporate|full year|fy\s*20\d{2}\b"),
    ]
    for scope, pat in checks:
        if re.search(pat, blob, re.I):
            return scope
    return "company_total"


def _event_type(metric_hint: Any, theme_hint: Any, text: Any, bucket_hint: Any = "") -> str:
    blob = " | ".join([str(metric_hint or ""), str(theme_hint or ""), str(bucket_hint or ""), str(text or "")]).lower()
    checks = [
        ("guidance", r"\b(guidance|outlook|target|forecast|reaffirm|raised target|updated target|tracking midpoint)\b"),
        ("cost_savings", r"\b(cost savings?|cost reduction|annualized savings|run-rate savings)\b"),
        ("liquidity_release", r"\b(pb bank|bank-held leases|cash optimization|cash release|trapped capital)\b"),
        ("deleveraging", r"\b(deleverag|debt reduction|repay|repayment|paydown)\b"),
        ("regulatory_credit", r"\b45z|tax credit|monetization\b"),
        ("segment_driver", r"\b(sendtech|presort|pricing|mix|segment)\b"),
        ("margin_improvement", r"\b(margin|adjusted ebit|ebit improvement|profitabilit)\b"),
        ("fcf_improvement", r"\b(fcf|free cash flow)\b"),
        ("milestone", r"\b(fully operational|online and ramping|agreement executed|commissioning|construction progressing|online)\b"),
        ("operational_driver", r"\b(utilization|risk management|volume|operations?)\b"),
    ]
    for event_type, pat in checks:
        if re.search(pat, blob, re.I):
            return event_type
    return "general_note"


def _event_slug(value: Any) -> str:
    txt = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")
    return txt or "na"


def build_evidence_event(
    text: Any,
    *,
    source_type: Any,
    metric_hint: Any = "",
    theme_hint: Any = "",
    base_score: float = 0.0,
    quietly_removed: bool = False,
    period_norm: Any = "",
    source_doc: Any = "",
    display_text_hint: Any = "",
) -> Optional[EvidenceEvent]:
    note = qualify_renderable_note(
        text,
        source_type=source_type,
        metric_hint=metric_hint,
        theme_hint=theme_hint,
        base_score=base_score,
        quietly_removed=quietly_removed,
    )
    if note is None:
        return None
    summary = note.summary
    display_hint = str(display_text_hint or "").strip()
    if display_hint and not renderable_note_drop_reason(display_hint, source_type=source_type):
        summary = _first_clean_sentence(display_hint)
    combined = " | ".join([str(metric_hint or ""), str(theme_hint or ""), summary, str(text or "")])
    metric_family = _event_metric_family(metric_hint, combined)
    entity_scope = _event_entity_scope(metric_hint, combined)
    event_type = _event_type(metric_hint, theme_hint, combined, note.bucket)
    direction = _change_state(summary)
    period_key = str(period_norm or "").strip().upper()
    stage_token = _canonical_stage_token(event_type, metric_family, combined)
    source_class_txt = source_class(source_type)
    statement_class_txt = statement_class(summary, source_type=source_type, metric_hint=metric_hint)
    topic_family = _topic_family_from_bucket(note.bucket)
    parent_subject_key = build_parent_subject_key(
        entity_scope=entity_scope,
        metric_family=metric_family,
        program_token=entity_scope,
        topic_family=topic_family,
    )
    canonical_subject_key = build_canonical_subject_key(
        entity_scope=entity_scope,
        metric_family=metric_family,
        target_period_norm=period_key,
        stage_token=stage_token,
    )
    lifecycle_key = build_promise_lifecycle_key(canonical_subject_key, stage_token=stage_token, promise_type=event_type)
    lifecycle_subject_key = build_lifecycle_subject_key(
        parent_subject_key=parent_subject_key,
        canonical_subject_key=canonical_subject_key,
        stage_token=stage_token,
        target_period_norm=period_key,
    )
    event_key = "|".join([
        _event_slug(event_type),
        _event_slug(metric_family),
        _event_slug(entity_scope),
        _event_slug(period_key or "none"),
        _event_slug(direction or "new"),
    ])
    return EvidenceEvent(
        event_key=event_key,
        event_type=event_type,
        metric_family=metric_family,
        entity_scope=entity_scope,
        period_norm=period_key,
        direction=direction,
        summary=summary,
        bucket=note.bucket,
        display_score=float(note.display_score),
        source_type=str(source_type or ""),
        source_doc=str(source_doc or ""),
        narrative_source=bool(note.narrative_source),
        preferred_source=bool(note.preferred_source),
        candidate_type="investor_note_candidate",
        source_class=source_class_txt,
        statement_class=statement_class_txt,
        parent_subject_key=parent_subject_key,
        canonical_subject_key=canonical_subject_key,
        lifecycle_key=lifecycle_key,
        lifecycle_subject_key=lifecycle_subject_key,
        evidence_role=evidence_role("investor_note_candidate", route_reason="quarter_notes", promise_type=event_type),
        topic_family=topic_family,
        confidence_score=float(note.display_score),
        route_reason="quarter_notes",
        routing_reason="broad_investor_note",
        lifecycle_state="stated",
        quality_drop_reason=str(note.quality_drop_reason or ""),
    )


def merge_evidence_events(
    events: List[EvidenceEvent],
    *,
    hard_cap: int = 12,
    quietly_removed_cap: int = 3,
) -> List[EventRenderCandidate]:
    best_by_key: Dict[str, EvidenceEvent] = {}
    for ev in events:
        existing = best_by_key.get(ev.event_key)
        if existing is None:
            best_by_key[ev.event_key] = ev
            continue
        current_rank = (float(ev.display_score), int(ev.preferred_source), int(ev.narrative_source), -len(str(ev.summary or "")))
        existing_rank = (float(existing.display_score), int(existing.preferred_source), int(existing.narrative_source), -len(str(existing.summary or "")))
        if current_rank > existing_rank:
            best_by_key[ev.event_key] = ev
    ordered = sorted(
        best_by_key.values(),
        key=lambda ev: (
            1 if ev.bucket == "Quietly removed" else 0,
            -float(ev.display_score),
            -int(ev.preferred_source),
            -int(ev.narrative_source),
            len(str(ev.summary or "")),
        ),
    )
    out: List[EventRenderCandidate] = []
    quietly_removed_count = 0
    for ev in ordered:
        if ev.bucket == "Quietly removed":
            if quietly_removed_count >= max(0, int(quietly_removed_cap)):
                continue
            quietly_removed_count += 1
        out.append(
            EventRenderCandidate(
                event_key=ev.event_key,
                event_type=ev.event_type,
                metric_family=ev.metric_family,
                entity_scope=ev.entity_scope,
                period_norm=ev.period_norm,
                direction=ev.direction,
                summary=ev.summary,
                bucket=ev.bucket,
                display_score=float(ev.display_score),
                candidate_type="investor_note_candidate",
                source_class=str(ev.source_class or ""),
                statement_class=str(ev.statement_class or ""),
                parent_subject_key=str(ev.parent_subject_key or ""),
                canonical_subject_key=str(ev.canonical_subject_key or ""),
                lifecycle_key=str(ev.lifecycle_key or ""),
                lifecycle_subject_key=str(ev.lifecycle_subject_key or ""),
                evidence_role=str(ev.evidence_role or "note"),
                route_reason=str(ev.route_reason or ev.routing_reason or "quarter_notes"),
                merge_reason=str(ev.merge_reason or ""),
                collapse_reason=str(ev.collapse_reason or ""),
                conflict_resolution_reason=str(ev.conflict_resolution_reason or ""),
                lifecycle_state=str(ev.lifecycle_state or ""),
                status_resolution_reason=str(ev.status_resolution_reason or ""),
            )
        )
        if len(out) >= max(1, int(hard_cap)):
            break
    return out


def build_follow_through_signal(
    text: Any,
    *,
    source_type: Any,
    metric_hint: Any = "",
    theme_hint: Any = "",
    base_score: float = 0.0,
    period_norm: Any = "",
    source_doc: Any = "",
    display_text_hint: Any = "",
    quarter_end: Any = "",
) -> Optional[FollowThroughSignal]:
    event = build_evidence_event(
        text,
        source_type=source_type,
        metric_hint=metric_hint,
        theme_hint=theme_hint,
        base_score=base_score,
        period_norm=period_norm,
        source_doc=source_doc,
        display_text_hint=display_text_hint,
    )
    if event is None:
        return None
    return FollowThroughSignal(
        event_key=event.event_key,
        event_type=event.event_type,
        metric_family=event.metric_family,
        entity_scope=event.entity_scope,
        period_norm=event.period_norm,
        direction=event.direction,
        summary=event.summary,
        display_score=event.display_score,
        source_type=event.source_type,
        source_doc=event.source_doc,
        candidate_type="follow_through_event",
        source_class=str(event.source_class or ""),
        statement_class=str(event.statement_class or ""),
        parent_subject_key=str(event.parent_subject_key or ""),
        canonical_subject_key=event.canonical_subject_key,
        lifecycle_key=event.lifecycle_key,
        lifecycle_subject_key=str(event.lifecycle_subject_key or ""),
        evidence_role=evidence_role("follow_through_event", route_reason="promise_progress", promise_type=event.event_type),
        topic_family=event.topic_family,
        confidence_score=event.confidence_score,
        route_reason="promise_progress",
        routing_reason="follow_through_update",
        lifecycle_state="updated_by_later_evidence",
        quarter_end=str(quarter_end or ""),
    )


def route_to_investor_note_candidate(
    text: Any,
    *,
    quarter: Any,
    source_type: Any,
    source_doc: Any = "",
    metric_hint: Any = "",
    theme_hint: Any = "",
    base_score: float = 0.0,
    period_norm: Any = "",
    display_text_hint: Any = "",
    quietly_removed: bool = False,
) -> Optional[InvestorNoteCandidate]:
    event = build_evidence_event(
        text,
        source_type=source_type,
        metric_hint=metric_hint,
        theme_hint=theme_hint,
        base_score=base_score,
        quietly_removed=quietly_removed,
        period_norm=period_norm,
        source_doc=source_doc,
        display_text_hint=display_text_hint,
    )
    if event is None:
        return None
    return InvestorNoteCandidate(
        quarter=str(quarter or ""),
        source_type=str(source_type or ""),
        source_doc=str(source_doc or ""),
        source_rank=source_rank(source_type),
        raw_text=str(text or ""),
        statement_summary=str(event.summary or ""),
        candidate_type="investor_note_candidate",
        source_class=str(event.source_class or ""),
        statement_class=str(event.statement_class or ""),
        metric_family=str(event.metric_family or ""),
        entity_scope=str(event.entity_scope or ""),
        time_anchor=str(event.period_norm or quarter or ""),
        target_period_norm=str(event.period_norm or ""),
        promise_type=str(event.event_type or ""),
        drop_reason=str(event.drop_reason or ""),
        quality_drop_reason=str(event.quality_drop_reason or ""),
        route_reason=str(event.route_reason or event.routing_reason or "quarter_notes"),
        routing_reason=str(event.routing_reason or "broad_investor_note"),
        parent_subject_key=str(event.parent_subject_key or ""),
        canonical_subject_key=str(event.canonical_subject_key or ""),
        lifecycle_subject_key=str(event.lifecycle_subject_key or ""),
        evidence_role=str(event.evidence_role or "note"),
        lifecycle_state=str(event.lifecycle_state or "stated"),
        topic_family=str(event.topic_family or ""),
        confidence_score=float(event.confidence_score),
        event_key=str(event.event_key or ""),
        lifecycle_key=str(event.lifecycle_key or ""),
        narrative_source=bool(event.narrative_source),
        preferred_source=bool(event.preferred_source),
    )


def route_to_measurable_promise_candidate(
    text: Any,
    *,
    quarter: Any,
    source_type: Any,
    metric_hint: Any = "",
    source_doc: Any = "",
    target_period_norm: Any = "",
    promise_type_hint: Any = "",
    base_score: float = 0.0,
) -> Optional[MeasurablePromiseCandidate]:
    qualified = qualify_promise_candidate(text, source_type=source_type, metric_hint=metric_hint)
    if qualified is None:
        return None
    period_key = infer_target_period_norm(
        period_norm=target_period_norm,
        quarter=quarter,
        text=text,
    )
    event = build_evidence_event(
        text,
        source_type=source_type,
        metric_hint=metric_hint,
        theme_hint="",
        base_score=base_score,
        period_norm=period_key,
        source_doc=source_doc,
        display_text_hint=qualified.summary,
    )
    combined_blob = " | ".join([str(metric_hint or ""), str(text or "")])
    metric_family = event.metric_family if event is not None else _event_metric_family(metric_hint, combined_blob)
    entity_scope = event.entity_scope if event is not None else _event_entity_scope(metric_hint, combined_blob)
    stage_token = _canonical_stage_token(
        event.event_type if event is not None else promise_type_hint or qualified.scope,
        metric_family,
        combined_blob,
        promise_type_hint or qualified.scope,
    )
    canonical_subject_key = build_canonical_subject_key(
        entity_scope=entity_scope,
        metric_family=metric_family,
        target_period_norm=period_key,
        stage_token=stage_token,
    )
    lifecycle_key = build_promise_lifecycle_key(
        canonical_subject_key,
        stage_token=stage_token,
        promise_type=promise_type_hint or qualified.scope,
    )
    routing_reason = "explicit_milestone" if qualified.scope == "clean_milestone" else "measurable_target"
    topic_family = _topic_family_from_bucket(event.bucket if event is not None else _note_bucket(metric_hint, "", text))
    source_class_txt = source_class(source_type)
    statement_class_txt = statement_class(qualified.summary or text, source_type=source_type, metric_hint=metric_hint)
    parent_subject_key = build_parent_subject_key(
        entity_scope=entity_scope,
        metric_family=metric_family,
        program_token=entity_scope,
        topic_family=topic_family,
    )
    return MeasurablePromiseCandidate(
        quarter=str(quarter or ""),
        source_type=str(source_type or ""),
        source_doc=str(source_doc or ""),
        source_rank=source_rank(source_type),
        raw_text=str(text or ""),
        statement_summary=str(qualified.summary or ""),
        candidate_type="measurable_promise_candidate",
        source_class=source_class_txt,
        statement_class=statement_class_txt,
        metric_family=str(metric_family or ""),
        entity_scope=str(entity_scope or ""),
        time_anchor=str(period_key or quarter or ""),
        target_period_norm=str(period_key or ""),
        promise_type=str(promise_type_hint or qualified.scope or ""),
        drop_reason=str(qualified.drop_reason or ""),
        quality_drop_reason=str(qualified.quality_drop_reason or ""),
        route_reason="promise_tracker",
        routing_reason=routing_reason,
        parent_subject_key=parent_subject_key,
        canonical_subject_key=canonical_subject_key,
        lifecycle_subject_key=build_lifecycle_subject_key(
            parent_subject_key=parent_subject_key,
            canonical_subject_key=canonical_subject_key,
            stage_token=stage_token,
            target_period_norm=period_key,
        ),
        evidence_role=evidence_role("measurable_promise_candidate", route_reason="promise_tracker", promise_type=promise_type_hint or qualified.scope),
        lifecycle_state="stated",
        topic_family=topic_family,
        confidence_score=float(event.display_score if event is not None else base_score or 0.0),
        candidate_scope=str(qualified.scope or ""),
        event_key=str(event.event_key if event is not None else ""),
        lifecycle_key=lifecycle_key,
        narrative_source=bool(qualified.narrative_source),
        preferred_source=bool(qualified.preferred_source),
    )


def build_follow_through_event(
    text: Any,
    *,
    quarter: Any,
    source_type: Any,
    metric_hint: Any = "",
    source_doc: Any = "",
    period_norm: Any = "",
    promise_type_hint: Any = "",
    base_score: float = 0.0,
    display_text_hint: Any = "",
) -> Optional[FollowThroughEvent]:
    signal = build_follow_through_signal(
        text,
        source_type=source_type,
        metric_hint=metric_hint,
        base_score=base_score,
        period_norm=period_norm,
        source_doc=source_doc,
        display_text_hint=display_text_hint,
        quarter_end=quarter,
    )
    if signal is None:
        return None
    return FollowThroughEvent(
        quarter=str(quarter or ""),
        source_type=str(source_type or ""),
        source_doc=str(source_doc or ""),
        source_rank=source_rank(source_type),
        raw_text=str(text or ""),
        statement_summary=str(signal.summary or ""),
        candidate_type="follow_through_event",
        source_class=str(signal.source_class or ""),
        statement_class=str(signal.statement_class or ""),
        metric_family=str(signal.metric_family or ""),
        entity_scope=str(signal.entity_scope or ""),
        time_anchor=str(signal.period_norm or quarter or ""),
        target_period_norm=str(signal.period_norm or ""),
        promise_type=str(promise_type_hint or signal.event_type or ""),
        drop_reason=str(signal.drop_reason or ""),
        quality_drop_reason="",
        route_reason="promise_progress",
        routing_reason=str(signal.routing_reason or "follow_through_update"),
        parent_subject_key=str(signal.parent_subject_key or ""),
        canonical_subject_key=str(signal.canonical_subject_key or ""),
        lifecycle_subject_key=str(signal.lifecycle_subject_key or ""),
        evidence_role=str(signal.evidence_role or "later_evidence"),
        lifecycle_state=str(signal.lifecycle_state or "updated_by_later_evidence"),
        topic_family=str(signal.topic_family or ""),
        confidence_score=float(signal.confidence_score),
        event_key=str(signal.event_key or ""),
        lifecycle_key=str(signal.lifecycle_key or ""),
        narrative_source=bool(is_renderable_narrative_source(source_type)),
        preferred_source=bool(is_preferred_narrative_source(source_type)),
    )


def merge_follow_through_signals(
    signals: List[FollowThroughSignal],
    *,
    hard_cap: int = 8,
) -> List[FollowThroughSignal]:
    best_by_key: Dict[str, FollowThroughSignal] = {}
    for signal in signals:
        existing = best_by_key.get(signal.event_key)
        if existing is None:
            best_by_key[signal.event_key] = signal
            continue
        current_rank = (
            float(signal.display_score),
            int(is_preferred_narrative_source(signal.source_type)),
            int(is_renderable_narrative_source(signal.source_type)),
            -len(str(signal.summary or "")),
        )
        existing_rank = (
            float(existing.display_score),
            int(is_preferred_narrative_source(existing.source_type)),
            int(is_renderable_narrative_source(existing.source_type)),
            -len(str(existing.summary or "")),
        )
        if current_rank > existing_rank:
            best_by_key[signal.event_key] = signal
    ordered = sorted(
        best_by_key.values(),
        key=lambda signal: (
            -float(signal.display_score),
            -int(is_preferred_narrative_source(signal.source_type)),
            -int(is_renderable_narrative_source(signal.source_type)),
            len(str(signal.summary or "")),
        ),
    )
    return ordered[: max(1, int(hard_cap))]


def merge_same_subject_events(
    rows: List[Any],
    *,
    hard_cap: int = 12,
) -> List[Any]:
    counts_by_key: Dict[str, int] = {}
    best_by_key: Dict[str, Any] = {}
    for row in rows:
        subject_key = str(
            getattr(row, "lifecycle_key", "")
            or getattr(row, "canonical_subject_key", "")
            or getattr(row, "event_key", "")
            or ""
        ).strip()
        if not subject_key:
            subject_key = str(getattr(row, "event_key", "") or "").strip()
        if not subject_key:
            continue
        counts_by_key[subject_key] = counts_by_key.get(subject_key, 0) + 1
        existing = best_by_key.get(subject_key)
        if existing is None:
            best_by_key[subject_key] = row
            continue
        cur_rank = (
            progress_status_rank(getattr(row, "lifecycle_state", "") or ""),
            float(getattr(row, "confidence_score", 0.0) or 0.0),
            int(getattr(row, "source_rank", 0) or 0),
            -len(str(getattr(row, "statement_summary", "") or "")),
            str(getattr(row, "event_key", "") or ""),
        )
        existing_rank = (
            progress_status_rank(getattr(existing, "lifecycle_state", "") or ""),
            float(getattr(existing, "confidence_score", 0.0) or 0.0),
            int(getattr(existing, "source_rank", 0) or 0),
            -len(str(getattr(existing, "statement_summary", "") or "")),
            str(getattr(existing, "event_key", "") or ""),
        )
        if cur_rank > existing_rank:
            best_by_key[subject_key] = row
    ordered = sorted(
        best_by_key.values(),
        key=lambda row: (
            -float(getattr(row, "confidence_score", 0.0) or 0.0),
            -int(getattr(row, "source_rank", 0) or 0),
            str(getattr(row, "canonical_subject_key", "") or getattr(row, "event_key", "")),
        ),
    )
    out: List[Any] = []
    for row in ordered[: max(1, int(hard_cap))]:
        subject_key = str(
            getattr(row, "lifecycle_key", "")
            or getattr(row, "canonical_subject_key", "")
            or getattr(row, "event_key", "")
            or ""
        ).strip()
        merge_reason = "canonical_subject_match" if counts_by_key.get(subject_key, 0) > 1 else ""
        out.append(
            replace(
                row,
                merge_reason=merge_reason,
                collapse_reason="same_subject_same_block" if merge_reason else str(getattr(row, "collapse_reason", "") or ""),
                conflict_resolution_reason="higher_confidence" if merge_reason else str(getattr(row, "conflict_resolution_reason", "") or ""),
            )
        )
    return out


def pick_best_subject_row_for_quarter(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not rows:
        return None
    best = sorted(
        rows,
        key=lambda row: (
            -progress_status_rank(row.get("status")),
            -progress_status_rank(row.get("lifecycle_state")),
            -int(row.get("_source_rank") or row.get("source_rank") or 0),
            -float(row.get("_score") or row.get("confidence_score") or 0.0),
            -int(row.get("_clean_target_bonus") or 0),
            int(row.get("_fragment_penalty") or 0),
            str(row.get("canonical_subject_key") or row.get("metric_ref") or row.get("metric") or ""),
        ),
    )[0]
    if len(rows) > 1:
        best["merge_reason"] = str(best.get("merge_reason") or "duplicate_weaker_row")
        best["collapse_reason"] = str(best.get("collapse_reason") or "same_subject_same_block")
        best["conflict_resolution_reason"] = str(best.get("conflict_resolution_reason") or "status_precedence")
    return best


def resolve_follow_through_for_subject(
    subject_key: Any,
    events: List[FollowThroughEvent],
) -> Optional[FollowThroughEvent]:
    subject_txt = str(subject_key or "").strip()
    if not subject_txt:
        return None
    same_subject = [
        ev
        for ev in events
        if str(ev.canonical_subject_key or ev.lifecycle_key or ev.event_key or "").strip() == subject_txt
    ]
    if not same_subject:
        return None
    best = sorted(
        same_subject,
        key=lambda ev: (
            -float(ev.confidence_score or 0.0),
            -int(ev.source_rank or 0),
            len(str(ev.statement_summary or "")),
        ),
    )[0]
    merge_reason = "same_subject_later_evidence" if len(same_subject) > 1 else ""
    lifecycle_state = "updated_by_later_evidence" if len(same_subject) > 1 else str(best.lifecycle_state or "")
    return replace(best, merge_reason=merge_reason, lifecycle_state=lifecycle_state)


def qualify_promise_candidate(
    text: Any,
    *,
    source_type: Any,
    metric_hint: Any = "",
) -> Optional[QualifiedPromiseCandidate]:
    txt = str(text or "").strip()
    drop_reason = promise_candidate_drop_reason(
        txt,
        source_type=source_type,
        metric_hint=metric_hint,
    )
    if drop_reason:
        return None
    summary = _first_clean_sentence(txt, max_len=240)
    if not summary:
        return None
    preferred_source = is_preferred_narrative_source(source_type)
    narrative_source = is_renderable_narrative_source(source_type)
    blob = " | ".join([str(metric_hint or ""), txt])
    has_metric = bool(PROMISE_METRIC_RE.search(blob))
    metric_hint_low = str(metric_hint or "").strip().lower()
    has_target_language = bool(PROMISE_TARGET_RE.search(txt))
    has_time = bool(PROMISE_TIME_RE.search(txt))
    has_numeric = bool(NUMERIC_TOKEN_RE.search(txt))
    has_bridge_amount = bool(re.search(r"\$\s*\d|\b\d+(?:\.\d+)?\s*(?:million|billion|m|bn|%|x|bps)\b", txt, re.I))
    short_phrase_milestone = bool((has_metric or "strategic milestone" in metric_hint_low) and _is_short_investor_phrase(txt) and MILESTONE_ACTION_RE.search(txt))
    if has_metric and has_target_language and has_time and has_numeric:
        return QualifiedPromiseCandidate(
            scope="hard_target",
            summary=summary,
            source_type=str(source_type or ""),
            narrative_source=narrative_source,
            preferred_source=preferred_source,
            quality_drop_reason="",
        )
    if MILESTONE_ACTION_RE.search(txt) and has_time:
        return QualifiedPromiseCandidate(
            scope="clean_milestone",
            summary=summary,
            source_type=str(source_type or ""),
            narrative_source=narrative_source,
            preferred_source=preferred_source,
            quality_drop_reason="",
        )
    if has_metric and _is_clean_numeric_bridge(txt) and has_bridge_amount and (has_numeric or has_target_language):
        return QualifiedPromiseCandidate(
            scope="hard_target",
            summary=summary,
            source_type=str(source_type or ""),
            narrative_source=narrative_source,
            preferred_source=preferred_source,
            quality_drop_reason="",
        )
    if short_phrase_milestone:
        return QualifiedPromiseCandidate(
            scope="clean_milestone",
            summary=summary,
            source_type=str(source_type or ""),
            narrative_source=narrative_source,
            preferred_source=preferred_source,
            quality_drop_reason="",
        )
    return None


def rank_filing_doc(
    doc_name: str,
    is_primary: bool = False,
    *,
    penalize_admin_docs: bool = False,
) -> int:
    name = str(doc_name or "").lower()
    if not name:
        return -1000
    score = 0
    if is_primary:
        score += 30
    if re.search(r"ex-?99|99\.1|99\.2|exhibit99", name, re.I):
        score += 120
    if re.search(r"earnings|press|release|shareholder|letter|presentation|slides|deck", name, re.I):
        score += 80
    if re.search(r"guidance|outlook|target|supplement", name, re.I):
        score += 50
    if name.endswith((".htm", ".html", ".xhtml", ".txt", ".pdf")):
        score += 15
    if re.search(r"xbrl|schema|instance|cal\.xml|def\.xml|lab\.xml|pre\.xml|\.xsd$|\.xml$", name, re.I):
        score -= 120
    if penalize_admin_docs and re.search(r"cover|index|signature", name, re.I):
        score -= 20
    return score


def pick_filing_docs(
    primary_doc: Optional[str],
    index_items: List[Dict[str, Any]],
    *,
    max_docs: int = 8,
    include_pattern: Optional[Pattern[str]] = None,
    penalize_admin_docs: bool = False,
) -> List[str]:
    seen: set[str] = set()
    ranked: List[Tuple[int, str]] = []
    include_re = include_pattern or DEFAULT_DOC_INCLUDE_RE

    def _push(doc_name: str, is_primary: bool = False) -> None:
        nm = str(doc_name or "").strip()
        if not nm:
            return
        key = nm.lower()
        if key in seen:
            return
        seen.add(key)
        ranked.append(
            (
                rank_filing_doc(
                    nm,
                    is_primary=is_primary,
                    penalize_admin_docs=penalize_admin_docs,
                ),
                nm,
            )
        )

    if primary_doc:
        _push(str(primary_doc), is_primary=True)
    for item in index_items:
        name = str(item.get("name") or "")
        if not name or not include_re.search(name):
            continue
        _push(name, is_primary=False)

    ranked.sort(key=lambda x: (-x[0], x[1].lower()))
    picked = [nm for score, nm in ranked if score > -50]
    return picked[:max_docs]


def history_quarter_ends(hist: pd.DataFrame, *, max_quarters: Optional[int] = None) -> List[date]:
    if hist is None or hist.empty or "quarter" not in hist.columns:
        return []
    quarters = sorted(pd.to_datetime(hist["quarter"], errors="coerce").dropna().unique())
    if max_quarters is not None and len(quarters) > max_quarters:
        quarters = quarters[-max_quarters:]
    return [pd.Timestamp(q).date() for q in quarters]
