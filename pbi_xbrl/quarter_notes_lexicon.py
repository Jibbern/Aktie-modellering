from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Tuple

from .guidance_lexicon import (
    classify_doc_type,
    classify_metric,
    doc_type_priority,
    normalize_period,
    normalize_text,
    score_chunk,
    truncate_clean,
)


QUARTER_NOTES_PROFILE: Dict[str, Any] = {
    "category_signals": {
        "guidance / targets": [
            "guidance",
            "outlook",
            "expect",
            "forecast",
            "target",
            "reaffirm",
            "raise",
            "lower",
        ],
        "one-time items": [
            "one-time",
            "restructuring",
            "special",
            "impair",
            "charge",
            "non-recurring",
        ],
        "debt / refi / covenants": [
            "debt",
            "refinanc",
            "maturity",
            "covenant",
            "credit facility",
            "revolver",
            "liquidity",
        ],
        "strategy / segment / operations": [
            "segment",
            "pricing",
            "volume",
            "mix",
            "transition",
            "strategy",
            "operations",
            "margin",
        ],
        "management / personnel": [
            "ceo",
            "cfo",
            "president",
            "board",
            "management",
            "leadership",
        ],
    },
    "anti_signals": [
        "forward-looking statements",
        "private securities litigation reform act",
        "safe harbor",
        "no obligation to update",
        "settlement date",
        "promptly following",
        "administrative agent will promptly",
        "will be entitled to",
        "securities act",
        "registration statement",
        "exempt from registration",
        "section 3(a)(9)",
        "conversion",
        "indenture",
        "loan documents",
        "administrative agent",
        "no assurance",
        "may not",
        "offering memorandum",
        "covenant definitions",
        "notes will be",
        "base salary",
        "target bonus",
        "employment agreement",
        "restricted stock",
        "director compensation",
        "table of contents",
        "signatures",
        "exhibit index",
        "risk factors",
    ],
}


PROMISE_PROFILE: Dict[str, Any] = {
    "intent_verbs": [
        "will",
        "expect to",
        "plan to",
        "target",
        "aim",
        "committed",
        "on track to",
        "we intend",
        "we are working to",
        "we continue to",
    ],
    "anti_signals": [
        "resigned",
        "board of directors",
        "director",
        "base salary",
        "target bonus",
        "safe harbor",
        "private securities litigation reform act",
        "forward-looking statements",
        "securities act",
        "registration statement",
        "liquidity remains sufficient",
        "sufficient cash",
    ],
    "measurable_commitment_verbs": [
        "eliminate",
        "reduce",
        "increase",
        "improve",
        "deliver",
        "achieve",
        "reach",
        "lower",
        "grow",
    ],
}


TIME_ANCHOR_RE = re.compile(
    r"\b(by\s+20\d{2}|next quarter|this quarter|full year|fy\s*20\d{2}|fiscal\s*20\d{2}|q[1-4]\s*20\d{2}|by end of (?:the )?(?:year|quarter))\b",
    re.I,
)
NUMERIC_RE = re.compile(r"[$%]|\b\d{1,3}(?:,\d{3})*(?:\.\d+)?\b")
ALPHA_WORD_RE = re.compile(r"\b[a-z]{3,}\b", re.I)


def _contains_any(text_lc: str, terms: Iterable[str]) -> bool:
    return any(str(t).lower() in text_lc for t in terms)


def letters_count(text: str) -> int:
    return len(re.findall(r"[A-Za-z]", str(text or "")))


def alpha_ratio(text: str) -> float:
    txt = str(text or "")
    if txt == "":
        return 0.0
    return float(letters_count(txt)) / float(max(1, len(txt)))


def word3_count(text: str) -> int:
    return len(ALPHA_WORD_RE.findall(str(text or "")))


def is_complete_signal_text(text: str) -> bool:
    txt = normalize_text(text)
    if not txt:
        return False
    if letters_count(txt) < 25:
        return False
    if alpha_ratio(txt) < 0.35:
        return False
    if word3_count(txt) < 6:
        return False
    return True


def category_key(category: str) -> str:
    cat = normalize_text(category).lower()
    for key in QUARTER_NOTES_PROFILE["category_signals"].keys():
        if key in cat:
            return key
    if "guidance" in cat or "target" in cat:
        return "guidance / targets"
    if "debt" in cat or "covenant" in cat or "refi" in cat:
        return "debt / refi / covenants"
    if "one-time" in cat or "one time" in cat:
        return "one-time items"
    if "strategy" in cat or "segment" in cat:
        return "strategy / segment / operations"
    if "management" in cat or "personnel" in cat:
        return "management / personnel"
    return "strategy / segment / operations"


def source_priority(source_type: str, form: str, doc_name: str, section: str = "") -> int:
    doc_t = classify_doc_type(source_type=source_type, form=form, doc_name=doc_name, section_name=section)
    return int(doc_type_priority(doc_t))


def score_quarter_note_candidate(
    text: str,
    category: str,
    source_type: str = "",
    form: str = "",
    doc_name: str = "",
    section: str = "",
) -> Dict[str, Any]:
    txt = normalize_text(text)
    low = txt.lower()
    base = score_chunk(
        text=txt,
        heading=section,
        source_type=source_type,
        form=form,
        doc_name=doc_name,
        category=category,
    )
    score = float(base.get("score") or 0.0)
    reasons = list(base.get("reasons") or [])
    if bool(base.get("hard_exclude")):
        return {"score": score, "hard_exclude": True, "reasons": reasons, "drop_reason": "boilerplate"}
    anti_hits = [x for x in QUARTER_NOTES_PROFILE["anti_signals"] if x in low]
    if anti_hits:
        score -= 60
        reasons.append("-quarter_notes_anti")
    ckey = category_key(category)
    cat_hits = [x for x in QUARTER_NOTES_PROFILE["category_signals"].get(ckey, []) if x in low]
    if cat_hits:
        score += 14
        reasons.append("+category_signal")
    if not is_complete_signal_text(txt):
        score -= 20
        reasons.append("-low_alpha")
    metric_canon = classify_metric(txt, None)
    period_label, period_key = normalize_period(txt, None)
    return {
        "score": score,
        "hard_exclude": False,
        "reasons": reasons,
        "metric_canon": metric_canon,
        "period_label": period_label,
        "period_key": period_key,
        "category_key": ckey,
        "doc_priority": source_priority(source_type=source_type, form=form, doc_name=doc_name, section=section),
    }


def score_promise_candidate(
    text: str,
    source_type: str = "",
    form: str = "",
    doc_name: str = "",
    section: str = "",
) -> Dict[str, Any]:
    txt = normalize_text(text)
    low = txt.lower()
    base = score_chunk(
        text=txt,
        heading=section,
        source_type=source_type,
        form=form,
        doc_name=doc_name,
        category="guidance",
    )
    score = float(base.get("score") or 0.0)
    reasons = list(base.get("reasons") or [])
    if bool(base.get("hard_exclude")):
        return {"score": score, "hard_exclude": True, "reasons": reasons, "drop_reason": "boilerplate"}
    anti_hits = [x for x in PROMISE_PROFILE["anti_signals"] if x in low]
    if anti_hits:
        score -= 70
        reasons.append("-promise_anti")
    intent_hits = [x for x in PROMISE_PROFILE["intent_verbs"] if x in low]
    if intent_hits:
        score += 22
        reasons.append("+promise_intent")
    numeric_hit = bool(NUMERIC_RE.search(txt))
    if numeric_hit:
        score += 10
        reasons.append("+numeric_target")
    time_anchor = bool(TIME_ANCHOR_RE.search(txt))
    if time_anchor:
        score += 10
        reasons.append("+time_anchor")
    measurable = any(v in low for v in PROMISE_PROFILE["measurable_commitment_verbs"])
    if measurable:
        score += 6
        reasons.append("+measurable_commitment")
    metric_canon = classify_metric(txt, None)
    period_label, period_key = normalize_period(txt, None)
    doc_pri = source_priority(source_type=source_type, form=form, doc_name=doc_name, section=section)
    score += float(max(0, int(doc_pri / 25)))
    return {
        "score": score,
        "hard_exclude": False,
        "reasons": reasons,
        "metric_canon": metric_canon,
        "period_label": period_label,
        "period_key": period_key,
        "has_intent": bool(intent_hits),
        "has_numeric": numeric_hit,
        "has_time_anchor": time_anchor,
        "measurable": measurable,
        "doc_priority": doc_pri,
    }


def compact_snippet(text: str, max_len: int = 220) -> str:
    return truncate_clean(text, max_chars=max_len)
