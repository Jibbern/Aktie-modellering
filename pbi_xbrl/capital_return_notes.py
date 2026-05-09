"""Generic note builders for capital return and compact quarter-note text.

These helpers deliberately format notes without mutating reported actuals. They
are safe to use from workbook UI surfaces, valuation memo rows, and tests for
future tickers where the same capital-return semantics apply.
"""
from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional

NEW_PREFIX = "[NEW]"
_CAPITAL_RETURN_NOTE_TEXT_KEYS = (
    "note",
    "claim",
    "text_full",
    "_render_summary",
    "_pbi_compact_note",
)
_CAPITAL_RETURN_METRIC_KEYS = (
    "metric",
    "metric_ref",
    "metric_tag",
    "metric_canon",
    "metric_display",
    "_metric_display",
)


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\xa0", " ")).strip()


def _fmt_dividend_per_share(value: float) -> str:
    return f"${float(value):.2f}/share"


def _fmt_money_m(value: float) -> str:
    return f"${float(value) / 1_000_000.0:,.1f}m"


def _fmt_shares_m(value: float) -> str:
    return f"{float(value) / 1_000_000.0:,.1f}m shares"


def _coerce_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(str(value).replace(",", "").replace("$", "").strip())
    except Exception:
        return None


def normalize_new_prefix(text: Any, *, add: bool = False) -> str:
    """Collapse repeated leading [NEW] badges and optionally add one."""
    txt = _clean_text(text)
    if not txt:
        return ""
    had_new = bool(re.match(r"^\s*(?:\[\s*NEW\s*\]\s*)+", txt, re.I))
    body = re.sub(r"^\s*(?:\[\s*NEW\s*\]\s*)+", "", txt, flags=re.I).strip()
    if not body:
        return NEW_PREFIX if (add or had_new) else ""
    if add or had_new:
        return f"{NEW_PREFIX} {body}"
    return body


def _capital_return_text_blob(item: Dict[str, Any]) -> str:
    parts: List[str] = []
    for key in (
        "note",
        "claim",
        "text_full",
        "_render_summary",
        "_pbi_compact_note",
        "evidence_snippet",
        "comment_full_text",
        "category",
        "bucket",
        "metric",
        "metric_ref",
        "metric_tag",
        "metric_canon",
        "metric_display",
        "_metric_display",
    ):
        value = item.get(key)
        if value is not None:
            parts.append(str(value))
    return _clean_text(" | ".join(parts))


def _capital_return_note_body_blob(item: Dict[str, Any]) -> str:
    parts: List[str] = []
    for key in (
        "note",
        "claim",
        "text_full",
        "_render_summary",
        "_pbi_compact_note",
        "evidence_snippet",
        "comment_full_text",
    ):
        value = item.get(key)
        if value is not None:
            parts.append(str(value))
    return _clean_text(" | ".join(parts))


def _capital_return_visible_note_blob(item: Dict[str, Any]) -> str:
    visible = _clean_text(
        " | ".join(
            str(item.get(key) or "")
            for key in ("note", "_render_summary", "_pbi_compact_note")
            if item.get(key) is not None
        )
    )
    if visible:
        return visible
    fallback = _clean_text(str(item.get("claim") or ""))
    if fallback:
        return fallback
    return _clean_text(str(item.get("text_full") or ""))


def _looks_like_dividend_text(text: Any) -> bool:
    txt = _clean_text(text)
    return bool(
        re.search(
            r"\b(quarterly dividend|regular quarterly dividend|dividend policy|cash dividends?|dividends?|"
            r"dividend/share|per share dividend|distribution)\b",
            txt,
            re.I,
        )
    )


def _looks_like_dividend_policy_text(text: Any) -> bool:
    txt = _clean_text(text)
    return bool(
        re.search(
            r"\bquarterly dividend\s+(?:increased|set|reduced|suspended)\b|"
            r"\b(?:increased|reduced|suspended)\s+(?:the\s+)?(?:regular\s+)?quarterly dividend\b|"
            r"\bdividend\s+(?:policy|approval|declared|set|suspended)\b",
            txt,
            re.I,
        )
    )


def _looks_like_dividend_cash_text(text: Any) -> bool:
    txt = _clean_text(text)
    return bool(
        re.search(
            r"\b(cash dividends?|dividends?\s+paid|ttm\s+cash\s+dividends?|"
            r"quarterly\s+cash\s+dividends?|dividend\s+cash)\b",
            txt,
            re.I,
        )
    )


def _looks_like_buyback_text(text: Any) -> bool:
    txt = _clean_text(text)
    return bool(
        re.search(
            r"\b(share repurchase|repurchas\w*|buybacks?|bought back|remaining authorization|"
            r"repurchase authorization|authorization remaining|capacity remaining)\b",
            txt,
            re.I,
        )
    )


def _set_capital_return_metric(row: Dict[str, Any], label: str) -> None:
    for key in _CAPITAL_RETURN_METRIC_KEYS:
        if key in row:
            row[key] = label
    row.setdefault("metric_ref", label)
    row.setdefault("_metric_display", label)


def normalize_capital_return_note_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize visible capital-return note text and dividend/buyback labels.

    This is intentionally text-only. It never mutates reported shares,
    dividend cash, buyback amounts, or any valuation calculation.
    """
    row = dict(item or {})
    for key in _CAPITAL_RETURN_NOTE_TEXT_KEYS:
        if key in row and isinstance(row.get(key), str):
            row[key] = normalize_new_prefix(row.get(key))

    body_blob = _capital_return_note_body_blob(row)
    visible_blob = _capital_return_visible_note_blob(row)
    full_blob = _capital_return_text_blob(row)
    classification_blob = visible_blob or body_blob
    dividend = _looks_like_dividend_text(classification_blob)
    buyback = _looks_like_buyback_text(classification_blob)
    dividend_policy = _looks_like_dividend_policy_text(classification_blob)
    dividend_cash = _looks_like_dividend_cash_text(classification_blob)
    if not buyback and not dividend and not classification_blob:
        dividend = _looks_like_dividend_text(full_blob)
        buyback = _looks_like_buyback_text(full_blob)
        dividend_policy = _looks_like_dividend_policy_text(full_blob)
        dividend_cash = _looks_like_dividend_cash_text(full_blob)
    if not dividend and not buyback:
        return row

    row["category"] = "Capital allocation / shareholder returns"
    if "bucket" in row:
        row["bucket"] = "Capital allocation / shareholder returns"

    if dividend and not buyback:
        if dividend_policy:
            _set_capital_return_metric(row, "Dividend policy")
            return row
        if dividend_cash:
            _set_capital_return_metric(row, "Dividend cash")
            return row
        replacement = "Dividend policy"
        for key in _CAPITAL_RETURN_METRIC_KEYS:
            current = str(row.get(key) or "").strip()
            current_low = current.lower()
            if (
                key in row
                and (
                    not current
                    or "buyback" in current_low
                    or "repurchase" in current_low
                    or current_low in {"capital allocation", "capital allocation / shareholder returns"}
                    or current_low.startswith("capital allocation / buyback")
                )
            ):
                row[key] = replacement
        row.setdefault("metric_ref", replacement)
        row.setdefault("_metric_display", replacement)
        return row

    if dividend and buyback:
        replacement = "Shareholder returns"
        for key in _CAPITAL_RETURN_METRIC_KEYS:
            current = str(row.get(key) or "").strip().lower()
            if "buyback" in current or "repurchase" in current:
                row[key] = replacement
        return row

    return row


def build_dividend_note(
    *,
    current_per_share: Optional[float] = None,
    previous_per_share: Optional[float] = None,
    action: str = "",
) -> str:
    """Return the short standard dividend note used by Quarter_Notes_UI."""
    action_key = str(action or "").strip().lower()
    if action_key in {"suspend", "suspended", "pause", "paused", "stop", "stopped", "eliminate", "eliminated"}:
        return normalize_new_prefix("Quarterly dividend suspended.", add=True)
    cur = _coerce_float(current_per_share)
    prev = _coerce_float(previous_per_share)
    if cur is None:
        return ""
    if prev is not None:
        if cur > prev + 0.004:
            return normalize_new_prefix(
                f"Quarterly dividend increased to {_fmt_dividend_per_share(cur)} from {_fmt_dividend_per_share(prev)}.",
                add=True,
            )
        if cur < prev - 0.004:
            return normalize_new_prefix(
                f"Quarterly dividend reduced to {_fmt_dividend_per_share(cur)} from {_fmt_dividend_per_share(prev)}.",
                add=True,
            )
    return f"Quarterly dividend set at {_fmt_dividend_per_share(cur)}."


def build_dividend_note_from_text(
    text: Any,
    *,
    current_per_share: Optional[float] = None,
    previous_per_share: Optional[float] = None,
) -> str:
    """Extract a compact dividend policy note from narrative text.

    Cash dividends paid and TTM dividend cash belong in valuation/cash-flow rows;
    this helper only emits declared/set/increased/reduced/suspended dividend
    policy notes.
    """
    txt = _clean_text(text)
    if not txt:
        return build_dividend_note(current_per_share=current_per_share, previous_per_share=previous_per_share)
    low = txt.lower()
    if re.search(
        r"\b(?:dividend|distribution)\b[^.]{0,120}\b(?:suspended|paused|stopped|eliminated|discontinued)\b|"
        r"\b(?:suspended|paused|stopped|eliminated|discontinued)\b[^.]{0,120}\b(?:dividend|distribution)\b",
        low,
        re.I,
    ):
        return build_dividend_note(action="suspended")

    from_to = re.search(
        r"\b(?:increase(?:d|s)?|raising|raised|reduce(?:d|s)?|lower(?:ed|s)?)\b"
        r"[^.]{0,140}?\bdividend\b[^.]{0,140}?\bfrom\s+\$?\s*([0-9]+(?:\.\d+)?)"
        r"\s+(?:per\s+share\s+)?(?:to|down\s+to)\s+\$?\s*([0-9]+(?:\.\d+)?)\s+per\s+share",
        txt,
        re.I,
    )
    if from_to:
        prev = _coerce_float(from_to.group(1))
        cur = _coerce_float(from_to.group(2))
        return build_dividend_note(current_per_share=cur, previous_per_share=prev)

    increase_amount = re.search(
        r"\bapproved\b[^.]{0,140}?\$?\s*([0-9]+(?:\.\d+)?)\s+per\s+share\s+increase\b"
        r".{0,320}?\$?\s*([0-9]+(?:\.\d+)?)\s+per\s+share",
        txt,
        re.I | re.S,
    )
    if increase_amount:
        inc = _coerce_float(increase_amount.group(1))
        cur = _coerce_float(increase_amount.group(2))
        if inc is not None and cur is not None:
            return build_dividend_note(current_per_share=cur, previous_per_share=round(cur - inc, 4))

    set_match = re.search(
        r"\b(?:approved|declared|set|announced)\b[^.]{0,180}?\b(?:quarterly|regular quarterly)\b"
        r"[^.]{0,140}?\$?\s*([0-9]+(?:\.\d+)?)\s+per\s+share",
        txt,
        re.I,
    )
    if set_match:
        return build_dividend_note(
            current_per_share=_coerce_float(set_match.group(1)),
            previous_per_share=previous_per_share,
        )
    return build_dividend_note(current_per_share=current_per_share, previous_per_share=previous_per_share)


def build_buyback_note(
    *,
    shares: Optional[float] = None,
    cash: Optional[float] = None,
    average_price: Optional[float] = None,
    post_quarter: bool = False,
    through_date: str = "",
) -> str:
    """Return a compact buyback execution/context note without changing shares."""
    shares_val = _coerce_float(shares)
    cash_val = _coerce_float(cash)
    avg_val = _coerce_float(average_price)
    pieces: List[str] = []
    if shares_val is not None:
        pieces.append(_fmt_shares_m(shares_val))
    if cash_val is not None:
        pieces.append(f"for {_fmt_money_m(cash_val)}")
    if not pieces:
        return ""
    if post_quarter:
        date_suffix = f" through {_clean_text(through_date)}" if _clean_text(through_date) else ""
        if shares_val is not None and cash_val is not None:
            return f"Additional {_fmt_shares_m(shares_val)} repurchased for {_fmt_money_m(cash_val)} after quarter-end{date_suffix}."
        return f"Additional {' '.join(pieces)} after quarter-end{date_suffix}."
    avg_suffix = f" at ${float(avg_val):.2f}/share" if avg_val is not None else ""
    if shares_val is not None and cash_val is not None:
        return f"Buybacks: {_fmt_shares_m(shares_val)} repurchased for {_fmt_money_m(cash_val)} during the quarter{avg_suffix}."
    return f"Buybacks: {' '.join(pieces)} repurchased during the quarter{avg_suffix}."


def build_buyback_authorization_note(*, remaining: Optional[float] = None, as_of: str = "") -> str:
    remaining_val = _coerce_float(remaining)
    if remaining_val is None:
        return ""
    as_of_txt = _clean_text(as_of)
    return f"Remaining authorization: {_fmt_money_m(remaining_val)}" + (f" as of {as_of_txt}." if as_of_txt else ".")


def _split_long_note_text(text: str, *, max_note_chars: int) -> List[str]:
    txt = _clean_text(text)
    if not txt:
        return []
    txt = re.sub(r"^Revenue inflection watch:\s*", "", txt, flags=re.I)
    if len(txt) <= max_note_chars:
        return [txt]
    if re.search(r"\bSendTech\b", txt, re.I) and re.search(r"\bPresort\b", txt, re.I):
        first = ""
        second = ""
        m = re.search(r"^(.*?\bSendTech\b.*?\bimproved)\s+and\s+(Presort\b.*)$", txt, re.I)
        if m:
            first = _clean_text(m.group(1)).rstrip(".") + "."
            second = _clean_text(m.group(2)).rstrip(".") + "."
        if first and second:
            return [first, second]
    parts = [_clean_text(part).rstrip(".") + "." for part in re.split(r"\s*;\s*", txt) if _clean_text(part)]
    if len(parts) > 1:
        return parts
    return [txt]


def normalize_quarter_note_items(
    items: Iterable[Dict[str, Any]],
    *,
    max_note_chars: int = 140,
) -> List[Dict[str, Any]]:
    """Dedupe and split visible quarter-note items while preserving metadata."""
    out: List[Dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in items or []:
        raw_note = item.get("note", item.get("claim", item.get("text_full", "")))
        for note in _split_long_note_text(str(raw_note or ""), max_note_chars=max_note_chars):
            normalized = _clean_text(note)
            if not normalized:
                continue
            key = (
                _clean_text(item.get("quarter")).lower(),
                _clean_text(item.get("category", item.get("bucket", ""))).lower(),
                normalized.lower(),
            )
            if key in seen:
                continue
            seen.add(key)
            row = dict(item)
            row["note"] = normalized
            out.append(normalize_capital_return_note_item(row))
    return out
