"""Guidance classification, text normalization, and scoring heuristics."""
from __future__ import annotations

import datetime as dt
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


FORWARD_NOTES_LABEL = "Forward-looking notes"


GUIDANCE_LEXICON: Dict[str, Any] = {
    "intent_verbs": [
        "guidance",
        "outlook",
        "financial outlook",
        "forecast",
        "target",
        "targets",
        "range",
        "in the range",
        "between",
        "we expect",
        "we expect to",
        "expects",
        "anticipated",
        "we anticipate",
        "we project",
        "we believe",
        "we estimate",
        "we plan",
        "we intend",
        "we aim",
        "we are on track",
        "we continue to expect",
        "we now expect",
        "we previously expected",
        "we continue",
        "we remain",
        "assume",
        "assuming",
        "management expects",
        "full-year",
        "for the year",
        "for fiscal",
        "fy",
        "next quarter",
        "next fiscal year",
    ],
    "status_verbs": {
        "maintained": [
            "reaffirm",
            "reaffirmed",
            "maintain",
            "maintained",
            "unchanged",
            "no change",
            "consistent with",
            "in line with",
            "on track",
        ],
        "raised": [
            "raise",
            "raised",
            "increase",
            "increased",
            "higher",
            "upward",
            "improve",
            "improved",
            "stronger than expected",
        ],
        "lowered": [
            "lower",
            "lowered",
            "decrease",
            "decreased",
            "reduced",
            "downward",
            "weaker than expected",
            "pressured",
        ],
        "withdrawn": [
            "withdraw",
            "withdrew",
            "suspend",
            "suspended",
            "no longer providing",
            "not providing guidance",
            "do not provide guidance",
            "not issuing guidance",
            "unable to forecast",
            "uncertainty prevents",
            "we are not updating guidance",
        ],
    },
    "metric_terms": {
        "Revenue": ["revenue", "net revenue", "sales", "net sales", "total revenue", "top line", "turnover"],
        "Gross profit / GM": ["gross profit", "gross margin", "gm", "gross margin %"],
        "Operating income / margin": ["operating income", "operating profit", "operating margin", "op margin", "income from operations"],
        "Adj EBITDA": ["ebitda", "adjusted ebitda", "adj ebitda", "adj. ebitda", "segment ebitda", "adjusted segment ebitda"],
        "Adj EBIT": ["ebit", "adjusted ebit", "adj ebit", "adjusted operating income", "adjusted income from operations"],
        "Adj EPS": [
            "eps",
            "adjusted eps",
            "adj eps",
            "diluted eps",
            "adjusted diluted eps",
            "adjusted diluted earnings per share",
            "earnings per share",
            "adjusted earnings per share",
        ],
        "Net income": ["net income", "net earnings", "profit", "loss", "net loss"],
        "FCF": ["free cash flow", "fcf", "free-cash-flow", "free cash flow excluding"],
        "Operating cash flow": ["operating cash flow", "cash flow from operations", "cfo"],
        "Capex": ["capex", "capital expenditures", "capital spending", "investments", "capital investment"],
        "Cost savings": ["cost savings", "savings", "run-rate savings", "annualized savings", "cost reduction", "expense reduction", "productivity savings"],
        "Restructuring charges": ["restructuring", "restructuring charges", "restructuring cost", "transformation costs", "one-time", "special items"],
        "Net debt / leverage": ["leverage", "net leverage", "debt to ebitda", "debt/ebitda", "net debt", "gross debt", "deleveraging"],
        "Interest / tax rate": ["interest expense", "net interest", "effective tax rate", "tax rate", "etr"],
        "Volume / pricing": ["volume", "volumes", "pricing", "price/mix", "mix", "units", "shipments", "utilization"],
        "ARR / SaaS": ["arr", "annual recurring revenue", "mrr", "recurring revenue", "subscription revenue", "bookings", "billings", "net retention", "nrr", "gross retention", "churn"],
        "Stores / capacity / production": ["store openings", "store closures", "capacity", "production", "output", "throughput"],
        "Regulatory credits": ["credit", "tax credit", "45z", "lcfs", "rin", "renewable identification number"],
    },
    "period_terms": [
        "full year",
        "full-year",
        "for the year",
        "fiscal year",
        "fy",
        "next quarter",
        "q1",
        "q2",
        "q3",
        "q4",
        "first quarter",
        "second quarter",
        "third quarter",
        "fourth quarter",
        "next fiscal year",
        "next year",
    ],
    "anti_signals": [
        "forward-looking statements",
        "private securities litigation reform act",
        "pslra",
        "cautionary statement",
        "no obligation to update",
        "undue reliance",
        "safe harbor",
        "subject to risks",
        "risk factors",
        "uncertainties",
        "securities act",
        "registration statement",
        "exempt from registration",
        "section 3(a)(9)",
        "no commission",
        "offering",
        "prospectus",
        "rule 144",
        "the number of shares to be sold is dependent on the satisfaction of certain conditions",
        "performance obligations",
        "transaction price allocated",
        "remaining performance obligations",
        "recognized as follows",
        "revenue recognition",
        "asc 606",
        "costs are amortized in a manner consistent with the timing of the related revenue",
        "contract performance period",
        "renewal commission is not commensurate",
        "accounting pronouncements",
        "accounting pronouncements not yet adopted",
        "fasb",
        "asu",
        "we are currently assessing the impact",
        "impact this standard will have",
        "unbilled",
        "contract assets",
        "contract liabilities",
        "base salary",
        "target bonus",
        "incentive plan",
        "employment agreement",
        "restricted stock units",
        "performance stock units",
        "psus",
        "lti award",
        "grant date value",
        "equity award",
        "compensation committee",
        "table of contents",
        "signatures",
        "exhibit index",
        "index",
        "indenture",
        "convertible notes",
        "covenant",
        "amendment",
        "waiver",
        "revolving credit facility",
        "asset allocation",
        "organizational review",
        "provides the following guidance for",
        "not anticipated to be material",
        "not expected to be material",
        "involuntary restructuring initiative",
        "the 2025 plan",
        "culture of continual improvement",
        "operational outputs and speed of execution",
        "do not foresee a near-term repeat of one-time cost reductions",
        "expected credit losses",
        "reasonable and supportable forecast",
        "balance sheet date",
    ],
    "anchor_headings": [
        "guidance",
        "outlook",
        "financial outlook",
        "2025 outlook",
        "full-year outlook",
        "2026 outlook",
        "targets",
        "updated guidance",
        "reaffirmed guidance",
        "earnings guidance",
    ],
    "doc_type_priority": {
        "ex99": 100,
        "earnings_release": 95,
        "slides": 90,
        "ceo_letter": 80,
        "10q_mda": 70,
        "10k_mda": 60,
        "other": 10,
    },
}


GUIDANCE_UI_METRIC_PRIORITY: List[str] = [
    "Revenue",
    "Adj EBITDA",
    "Adj EBIT",
    "Adj EPS",
    "FCF",
    "Capex",
    "Cost savings",
    "Restructuring charges",
    "Net debt / leverage",
    FORWARD_NOTES_LABEL,
]


HARD_EXCLUDE_TERMS = {
    "forward-looking statements",
    "private securities litigation reform act",
    "pslra",
    "safe harbor",
    "securities act",
    "registration statement",
    "exempt from registration",
    "section 3(a)(9)",
    "rule 144",
    "involuntary restructuring initiative",
    "operational outputs and speed of execution",
    "performance stock units",
    "lti award",
    "grant date value",
    "target bonus",
    "base salary",
}

TOC_TERMS = {"table of contents", "signatures", "exhibit index", "index"}

ANTI_HEADING_TERMS = {"forward-looking statements", "safe harbor", "risk factors"}

MDA_HEADING_TERMS = {
    "management discussion",
    "management's discussion",
    "mda",
    "item 2",
    "item 7",
    "results of operations",
    "liquidity and capital resources",
}


RANGE_PATTERNS: Sequence[re.Pattern[str]] = (
    re.compile(
        r"\bbetween\s+\$?\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*(billion|million|bn|m|%|x|bps)?\s+and\s+\$?\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*(billion|million|bn|m|%|x|bps)?\b",
        re.I,
    ),
    re.compile(
        r"\$?\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*(billion|million|bn|m|%|x|bps)?\s*(?:to|through|[\-\u2013\u2014])\s*\$?\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*(billion|million|bn|m|%|x|bps)?",
        re.I,
    ),
)

POINT_PATTERN = re.compile(
    r"\b(?:about|approximately|around|roughly|~)?\s*\$?\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*(billion|million|bn|m|%|x|bps)?\b",
    re.I,
)
PERCENT_PATTERN = re.compile(r"\b([+-]?[0-9]+(?:\.[0-9]+)?)\s*%\b", re.I)
BPS_PATTERN = re.compile(r"\b([+-]?[0-9]+(?:\.[0-9]+)?)\s*bps\b", re.I)
QUALITATIVE_RANGE_PATTERN = re.compile(
    r"\b(high single-digit|mid single-digit|low single-digit|high double-digit|mid double-digit|low double-digit|mid-teens|high teens|low teens|low-to-mid|mid-to-high)\b",
    re.I,
)


def normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def normalize_dedup_text(value: Any) -> str:
    txt = normalize_text(value).lower()
    txt = txt.replace("\u2013", "-").replace("\u2014", "-")
    return txt


def truncate_clean(text: Any, max_chars: int = 220) -> str:
    normalized = normalize_text(text)
    if len(normalized) <= max_chars:
        return normalized
    cut = normalized[: max(1, max_chars - 3)]
    split_idx = max(cut.rfind(" "), cut.rfind("."), cut.rfind(","), cut.rfind(";"), cut.rfind(":"))
    if split_idx >= max(12, int(max_chars * 0.75)):
        cut = cut[:split_idx]
    return f"{cut.rstrip(' ,;:.')}..."


def split_sentences(text: Any) -> List[str]:
    normalized = normalize_text(text)
    if not normalized:
        return []
    parts = re.split(r"(?<=[.!?;])\s+|\n+", normalized)
    out: List[str] = []
    for part in parts:
        p = normalize_text(part)
        if p:
            out.append(p)
    return out


def _contains_any(text_lc: str, terms: Iterable[str]) -> bool:
    return any(str(t).lower() in text_lc for t in terms)


def classify_doc_type(source_type: str, form: str, doc_name: str, section_name: str = "") -> str:
    st = str(source_type or "").lower()
    fm = str(form or "").lower()
    info = f"{doc_name or ''} {section_name or ''}".lower()
    if "ex99" in st or "ex-99" in st:
        return "ex99"
    if (
        "earnings_release" in st
        or "earnings release" in st
        or "press release" in st
        or "earnings_release" in info
        or "earnings release" in info
        or "press release" in info
    ):
        return "earnings_release"
    if "slides" in st or "presentation" in st or any(k in info for k in ("slides", "presentation", "deck")):
        return "slides"
    if "ceo" in st or "shareholder letter" in st or "letter to shareholders" in info or "ceo letter" in info:
        return "ceo_letter"
    if fm == "8-k" and any(k in info for k in ("ex99", "ex-99", "exhibit 99", "press release", "earnings release", "news release")):
        return "ex99"
    if fm == "10-q":
        return "10q_mda"
    if fm == "10-k":
        return "10k_mda"
    return "other"


def doc_type_priority(doc_type: str) -> int:
    return int(GUIDANCE_LEXICON["doc_type_priority"].get(str(doc_type or "").lower(), 10))


def is_preferred_section(form: str, heading: str, text: str = "") -> bool:
    form_l = str(form or "").lower()
    heading_l = normalize_text(heading).lower()
    text_l = normalize_text(text).lower()
    if any(k in heading_l for k in ANTI_HEADING_TERMS):
        return False
    if form_l not in {"10-q", "10-k"}:
        return True
    if not heading_l:
        return True
    if "risk factors" in heading_l:
        return False
    if any(k in heading_l for k in GUIDANCE_LEXICON["anchor_headings"]):
        return True
    if any(k in heading_l for k in MDA_HEADING_TERMS):
        return True
    if any(k in text_l for k in GUIDANCE_LEXICON["anchor_headings"]):
        return True
    return False


def classify_status(text: str) -> str:
    t = normalize_text(text).lower()
    for label, verbs in GUIDANCE_LEXICON["status_verbs"].items():
        if _contains_any(t, verbs):
            if label == "raised":
                return "Raised"
            if label == "lowered":
                return "Lowered"
            if label == "maintained":
                return "Maintained"
            if label == "withdrawn":
                return "Withdrawn"
    return "Unknown"


def normalize_period(text: str, quarter_end: Optional[dt.date]) -> Tuple[str, str]:
    t = normalize_text(text).lower()
    qd = quarter_end or dt.date.today()
    current_year = int(qd.year)
    years = [int(y) for y in re.findall(r"\b(20\d{2})\b", t)]
    years += [int(y) for y in re.findall(r"\bfy\s*[-/]?\s*(20\d{2})\b", t)]

    def _normalize_year_token(token: str, fallback: int) -> int:
        yy = int(token)
        if yy < 100:
            return 2000 + yy
        return yy

    m_q = re.search(r"\bq([1-4])(?:\s*[- ]?\s*(20\d{2}|\d{2}))?\b", t, re.I)
    if m_q:
        q_num = int(m_q.group(1))
        q_year = _normalize_year_token(m_q.group(2), current_year) if m_q.group(2) else current_year
        return f"Q{q_year}Q{q_num}", f"Q{q_year}Q{q_num}"

    m_fy = re.search(r"\bfy\s*[-/]?\s*(20\d{2}|\d{2})\b", t, re.I)
    if m_fy:
        fy_year = _normalize_year_token(m_fy.group(1), current_year)
        return f"FY {fy_year}", f"FY{fy_year}"

    if any(k in t for k in ("next quarter",)):
        q_num = ((qd.month - 1) // 3) + 1
        next_q = 1 if q_num == 4 else q_num + 1
        next_y = int(qd.year) + (1 if q_num == 4 else 0)
        return "Next Q", f"Q{next_y}Q{next_q}"

    if "next fiscal year" in t or "next year" in t:
        return "Next FY", f"FY{current_year + 1}"

    if years:
        y = max(years)
        if any(k in t for k in ("q1", "q2", "q3", "q4", "quarter")):
            q_word = None
            for idx, token in enumerate(("q1", "q2", "q3", "q4"), start=1):
                if token in t:
                    q_word = idx
                    break
            if q_word is not None:
                return f"Q{y}Q{q_word}", f"Q{y}Q{q_word}"
        return f"FY {y}", f"FY{y}"

    if any(k in t for k in ("full year", "full-year", "for the year", "fiscal year", "fy")):
        return f"FY {current_year}", f"FY{current_year}"

    return "Unknown", "UNK"


def _metric_hits(text: str) -> List[Tuple[str, int, int, str]]:
    t = normalize_text(text)
    tl = t.lower()
    hits: List[Tuple[str, int, int, str]] = []
    for metric, terms in GUIDANCE_LEXICON["metric_terms"].items():
        for term in terms:
            term_l = str(term).lower()
            for m in re.finditer(rf"\b{re.escape(term_l)}\b", tl):
                hits.append((metric, m.start(), m.end(), term_l))
    return hits


def classify_metric(text: str, match_span: Optional[Tuple[int, int]] = None) -> str:
    t = normalize_text(text)
    tl = t.lower()
    if not t:
        return "Other"
    if "cost savings" in tl:
        return "Cost savings"

    eps_context = bool(
        re.search(r"\b(?:adjusted\s+)?(?:diluted\s+)?eps\b", tl)
        or re.search(r"\b(?:adjusted\s+)?(?:diluted\s+)?earnings\s+per\s+share\b", tl)
    )
    eps_anchor = bool(
        re.search(r"\bper\s+share\b", tl)
        or re.search(r"\bdiluted\s+shares?\b", tl)
        or re.search(r"\bdiluted\s+eps\b", tl)
        or re.search(r"\bearnings\s+per\s+share\b", tl)
    )
    if "ebitda" in tl:
        return "Adj EBITDA"
    if re.search(r"\bebit\b", tl) and "ebitda" not in tl:
        return "Adj EBIT"
    if eps_context and eps_anchor:
        return "Adj EPS"

    hits = _metric_hits(t)
    if not hits:
        return "Other"
    if not (eps_context and eps_anchor):
        hits = [h for h in hits if h[0] != "Adj EPS"]
    if not hits:
        return "Other"

    metric_priority = {m: i for i, m in enumerate(GUIDANCE_UI_METRIC_PRIORITY + list(GUIDANCE_LEXICON["metric_terms"].keys()))}
    if match_span is None:
        best = min(hits, key=lambda h: (metric_priority.get(h[0], 999), h[1], -len(h[3])))
        return best[0]

    center = (int(match_span[0]) + int(match_span[1])) // 2
    near_hits = [h for h in hits if abs(((h[1] + h[2]) // 2) - center) <= 80]
    target_hits = near_hits if near_hits else hits
    best = min(
        target_hits,
        key=lambda h: (abs(((h[1] + h[2]) // 2) - center), metric_priority.get(h[0], 999), -len(h[3])),
    )
    return best[0]


def _to_num(raw: Any) -> Optional[float]:
    try:
        txt = str(raw).replace(",", "").replace("$", "").strip()
        if txt == "":
            return None
        return float(txt)
    except Exception:
        return None


def scale_value(metric: str, value: float, unit: str) -> Tuple[float, str]:
    u = str(unit or "").strip().lower()
    v = float(value)
    if u == "%":
        return v, "%"
    if u == "bps":
        return v, "bps"
    if u == "x":
        return v, "x"
    if metric in {"Adj EPS"}:
        return v, "$"
    if metric in {"Revenue", "Adj EBITDA", "Adj EBIT", "FCF", "Operating cash flow", "Capex", "Cost savings", "Restructuring charges", "Net debt / leverage", "Net income"}:
        if u in {"billion", "bn"}:
            return v * 1e9, "$m"
        if u in {"million", "m"}:
            return v * 1e6, "$m"
        if abs(v) <= 5000:
            return v * 1e6, "$m"
        return v, "$m"
    return v, (u or "")


def extract_numeric_patterns(text: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    t = normalize_text(text)
    tl = t.lower()

    occupied: List[Tuple[int, int]] = []

    def _overlaps(span: Tuple[int, int]) -> bool:
        return any(not (span[1] <= s or span[0] >= e) for s, e in occupied)

    for pat in RANGE_PATTERNS:
        for m in pat.finditer(t):
            span = (int(m.start()), int(m.end()))
            if _overlaps(span):
                continue
            lo_raw = _to_num(m.group(1))
            hi_raw = _to_num(m.group(3))
            if lo_raw is None or hi_raw is None:
                continue
            metric = classify_metric(t, span)
            unit_1 = str(m.group(2) or "")
            unit_2 = str(m.group(4) or unit_1)
            raw_range = str(m.group(0) or "")
            has_explicit_hint = ("$" in raw_range) or bool(unit_1.strip()) or bool(unit_2.strip()) or ("between" in raw_range.lower())
            if not has_explicit_hint:
                if metric != "Adj EPS":
                    continue
                if max(abs(float(lo_raw)), abs(float(hi_raw))) > 25:
                    continue
            lo, u_lo = scale_value(metric, lo_raw, unit_1)
            hi, u_hi = scale_value(metric, hi_raw, unit_2)
            lo, hi = (lo, hi) if lo <= hi else (hi, lo)
            out.append(
                {
                    "kind": "range",
                    "metric_canon": metric,
                    "value_low": float(lo),
                    "value_high": float(hi),
                    "value_mid": (float(lo) + float(hi)) / 2.0,
                    "value_point": None,
                    "unit": u_lo or u_hi,
                    "span": span,
                    "qualitative_range_text": None,
                    "raw_text": raw_range,
                }
            )
            occupied.append(span)

    for m in BPS_PATTERN.finditer(t):
        span = (int(m.start()), int(m.end()))
        if _overlaps(span):
            continue
        vv = _to_num(m.group(1))
        if vv is None:
            continue
        metric = classify_metric(t, span)
        out.append(
            {
                "kind": "point",
                "metric_canon": metric,
                "value_low": None,
                "value_high": None,
                "value_mid": None,
                "value_point": float(vv),
                "unit": "bps",
                "span": span,
                "qualitative_range_text": None,
                "raw_text": str(m.group(0) or ""),
            }
        )
        occupied.append(span)

    for m in PERCENT_PATTERN.finditer(t):
        span = (int(m.start()), int(m.end()))
        if _overlaps(span):
            continue
        vv = _to_num(m.group(1))
        if vv is None:
            continue
        metric = classify_metric(t, span)
        out.append(
            {
                "kind": "point",
                "metric_canon": metric,
                "value_low": None,
                "value_high": None,
                "value_mid": None,
                "value_point": float(vv),
                "unit": "%",
                "span": span,
                "qualitative_range_text": None,
                "raw_text": str(m.group(0) or ""),
            }
        )
        occupied.append(span)

    for m in POINT_PATTERN.finditer(t):
        span = (int(m.start()), int(m.end()))
        if _overlaps(span):
            continue
        vv = _to_num(m.group(1))
        if vv is None:
            continue
        raw_point = str(m.group(0) or "")
        unit_raw = str(m.group(2) or "")
        has_explicit_hint = ("$" in raw_point) or bool(unit_raw.strip()) or bool(re.search(r"\b(about|approximately|around|roughly|~)\b", raw_point, re.I))
        if (m.group(2) in (None, "")) and 1900 <= abs(float(vv)) <= 2100:
            continue
        metric = classify_metric(t, span)
        if not has_explicit_hint:
            if metric != "Adj EPS":
                continue
            if abs(float(vv)) > 25:
                continue
        val, unit = scale_value(metric, vv, str(m.group(2) or ""))
        out.append(
            {
                "kind": "point",
                "metric_canon": metric,
                "value_low": None,
                "value_high": None,
                "value_mid": None,
                "value_point": float(val),
                "unit": unit,
                "span": span,
                "qualitative_range_text": None,
                "raw_text": raw_point,
            }
        )
        occupied.append(span)

    for m in QUALITATIVE_RANGE_PATTERN.finditer(tl):
        span = (int(m.start()), int(m.end()))
        if _overlaps(span):
            continue
        out.append(
            {
                "kind": "qualitative_range",
                "metric_canon": classify_metric(t, span),
                "value_low": None,
                "value_high": None,
                "value_mid": None,
                "value_point": None,
                "unit": "",
                "span": span,
                "qualitative_range_text": m.group(1),
                "raw_text": str(m.group(0) or ""),
            }
        )
        occupied.append(span)

    return out


def score_chunk(
    text: str,
    heading: str = "",
    source_type: str = "",
    form: str = "",
    doc_name: str = "",
    category: str = "",
) -> Dict[str, Any]:
    normalized = normalize_text(text)
    text_lc = normalized.lower()
    heading_lc = normalize_text(heading).lower()
    category_lc = normalize_text(category).lower()

    score = 0.0
    reasons: List[str] = []
    hard_exclude = False

    intent_hits = [w for w in GUIDANCE_LEXICON["intent_verbs"] if w in text_lc]
    status_hits = [w for group in GUIDANCE_LEXICON["status_verbs"].values() for w in group if w in text_lc]
    anti_hits = [w for w in GUIDANCE_LEXICON["anti_signals"] if w in text_lc]
    period_hits = [w for w in GUIDANCE_LEXICON["period_terms"] if w in text_lc]

    numeric_hits = extract_numeric_patterns(normalized)
    metric_hits = _metric_hits(normalized)
    metric_names = sorted({m for m, _, _, _ in metric_hits})

    if intent_hits:
        score += 30
        reasons.append("+intent")
    if status_hits:
        score += 20
        reasons.append("+status")
    if numeric_hits:
        score += 25
        reasons.append("+numeric")
    if metric_hits:
        score += 20
        reasons.append("+metric")
    if period_hits:
        score += 10
        reasons.append("+period")

    if anti_hits:
        score -= 60
        reasons.append("-anti_signal")
    if any(k in text_lc for k in HARD_EXCLUDE_TERMS):
        hard_exclude = True
        reasons.append("hard_exclude")

    if any(k in text_lc for k in TOC_TERMS):
        score -= 30
        reasons.append("-toc")

    metric_density = len(metric_hits)
    if len(normalized) > 600 and (metric_density + (1 if numeric_hits else 0)) <= 1:
        score -= 20
        reasons.append("-boilerplate_density")

    if _contains_any(heading_lc, GUIDANCE_LEXICON["anchor_headings"]):
        score += 8
        reasons.append("+anchor_heading")
    if any(k in heading_lc for k in ANTI_HEADING_TERMS):
        score -= 40
        reasons.append("-anti_heading")
        if any(k in heading_lc for k in ("safe harbor", "forward-looking statements")):
            hard_exclude = True
            reasons.append("hard_exclude_heading")

    if "guidance" in category_lc or "outlook" in category_lc:
        score += 6
        reasons.append("+category_guidance")

    proximity_bonus = 0
    if numeric_hits and metric_hits:
        for num in numeric_hits:
            span = num.get("span")
            if not span:
                continue
            center = (int(span[0]) + int(span[1])) // 2
            if any(abs(((s + e) // 2) - center) <= 80 for _, s, e, _ in metric_hits):
                proximity_bonus = 15
                break
    if proximity_bonus:
        score += proximity_bonus
        reasons.append("+proximity")

    doc_type = classify_doc_type(source_type=source_type, form=form, doc_name=doc_name, section_name=heading)
    doc_pri = doc_type_priority(doc_type)
    score += max(0, int(doc_pri / 25))

    metric_hint = classify_metric(normalized, numeric_hits[0].get("span")) if numeric_hits else classify_metric(normalized, None)
    if metric_hint == "Other" and not intent_hits and not status_hits:
        score -= 10
        reasons.append("-weak_metric")

    return {
        "score": float(score),
        "hard_exclude": bool(hard_exclude),
        "reasons": reasons,
        "doc_type_canon": doc_type,
        "doc_priority": int(doc_pri),
        "intent_hits": intent_hits,
        "status_hits": status_hits,
        "metric_hits": metric_names,
        "period_hits": period_hits,
        "anti_hits": anti_hits,
        "numeric_hits": numeric_hits,
        "metric_hint": metric_hint,
    }


def dedup_text_key(text: Any) -> str:
    return normalize_dedup_text(text)
