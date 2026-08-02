"""Deterministic Inline XBRL extraction from immutable verified bytes."""
from __future__ import annotations

import hashlib
import re
import unicodedata
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from lxml import html as lxml_html

from pbi_xbrl.longitudinal_memory.types import canonical_decimal

from .types import DiscoveredDocument, ExtractedEvidence, LocatorError, text_sha256


METHOD_ID = "extractor:source:inline-xbrl-fact@1"
_NUMERIC_TAGS = frozenset({"ix:nonfraction", "ix:fraction"})


def _text(value: Any) -> str:
    if hasattr(value, "itertext"):
        value = " ".join(value.itertext())
    return " ".join(unicodedata.normalize("NFC", str(value or "")).split())


def _node_path(node: Any) -> str:
    parts: list[str] = []
    cursor = node
    while cursor is not None and isinstance(getattr(cursor, "tag", None), str):
        parent = cursor.getparent()
        tag = str(cursor.tag).casefold()
        if parent is None:
            parts.append(tag)
            break
        siblings = [child for child in parent if str(getattr(child, "tag", "")).casefold() == tag]
        index = siblings.index(cursor) + 1
        parts.append(f"{tag}[{index}]")
        cursor = parent
    return "/" + "/".join(reversed(parts))


def _one(root: Any, xpath: str, *, subject: str) -> Any:
    matches = root.xpath(xpath)
    if len(matches) != 1:
        raise LocatorError(f"Inline XBRL {subject} matched {len(matches)} nodes, not one.")
    return matches[0]


def _local_name(node: Any) -> str:
    """Return an XML local name from either namespaced XML or HTML colon tags."""

    tag = str(getattr(node, "tag", ""))
    return tag.rsplit("}", 1)[-1].rsplit(":", 1)[-1].casefold()


def _descendants(node: Any, local_name: str) -> list[Any]:
    expected = local_name.casefold()
    return [child for child in node.iterdescendants() if _local_name(child) == expected]


def _continuations(root: Any, fact: Any) -> tuple[str, tuple[str, ...], tuple[str, ...], str | None]:
    text_parts = [_text(fact)]
    ids: list[str] = []
    paths: list[str] = []
    seen: set[str] = set()
    next_id = fact.get("continuedat")
    while next_id:
        continuation_id = str(next_id)
        if continuation_id in seen:
            raise LocatorError("Inline XBRL continuation chain contains a cycle.")
        seen.add(continuation_id)
        continuation = _one(
            root,
            f"//*[@id={_xpath_literal(continuation_id)}]",
            subject=f"continuation {continuation_id!r}",
        )
        if str(continuation.tag).casefold() != "ix:continuation":
            raise LocatorError("Inline XBRL continuedAt endpoint is not an ix:continuation node.")
        ids.append(continuation_id)
        paths.append(_node_path(continuation))
        text_parts.append(_text(continuation))
        next_id = continuation.get("continuedat")
    joined = _text(" ".join(text_parts))
    digest = text_sha256(joined) if ids else None
    return joined, tuple(ids), tuple(paths), digest


def _xpath_literal(value: str) -> str:
    if "'" not in value:
        return f"'{value}'"
    if '"' not in value:
        return f'"{value}"'
    parts = value.split("'")
    return "concat(" + ", \"'\", ".join(f"'{part}'" for part in parts) + ")"


def _context(root: Any, context_id: str) -> dict[str, Any]:
    context = _one(
        root,
        f"//*[@id={_xpath_literal(context_id)}]",
        subject=f"context {context_id!r}",
    )
    if str(context.tag).casefold() != "xbrli:context":
        raise LocatorError("Inline XBRL contextRef does not resolve to xbrli:context.")
    identifiers = _descendants(context, "identifier")
    if len(identifiers) != 1:
        raise LocatorError("Inline XBRL context does not contain one entity identifier.")
    entity = _text(identifiers[0])
    starts = _descendants(context, "startdate")
    ends = _descendants(context, "enddate")
    instants = _descendants(context, "instant")
    if instants:
        if len(instants) != 1 or starts or ends:
            raise LocatorError("Inline XBRL context has an ambiguous period.")
        period_start = None
        period_end = None
        period_instant = _text(instants[0])
    else:
        if len(starts) != 1 or len(ends) != 1:
            raise LocatorError("Inline XBRL duration context lacks one start and end.")
        period_start = _text(starts[0])
        period_end = _text(ends[0])
        period_instant = None
    dimensions = sorted(
        (
            str(node.get("dimension")),
            _text(node),
        )
        for node in _descendants(context, "explicitmember")
    )
    if len({dimension for dimension, _member in dimensions}) != len(dimensions):
        raise LocatorError("Inline XBRL context contains a duplicate dimension axis.")
    return {
        "entity_identifier": entity,
        "period_start": period_start,
        "period_end": period_end,
        "period_instant": period_instant,
        "context_dimensions": [
            {"dimension": dimension, "member": member}
            for dimension, member in dimensions
        ],
    }


def _unit(root: Any, unit_ref: str) -> tuple[list[str], list[str]]:
    unit = _one(
        root,
        f"//*[@id={_xpath_literal(unit_ref)}]",
        subject=f"unit {unit_ref!r}",
    )
    if str(unit.tag).casefold() != "xbrli:unit":
        raise LocatorError("Inline XBRL unitRef does not resolve to xbrli:unit.")
    divide = _descendants(unit, "divide")
    if divide:
        if len(divide) != 1:
            raise LocatorError("Inline XBRL unit contains multiple divide definitions.")
        numerators = _descendants(divide[0], "unitnumerator")
        denominators = _descendants(divide[0], "unitdenominator")
        numerator = sorted(
            _text(measure)
            for group in numerators
            for measure in _descendants(group, "measure")
        )
        denominator = sorted(
            _text(measure)
            for group in denominators
            for measure in _descendants(group, "measure")
        )
    else:
        numerator = sorted(_text(node) for node in _descendants(unit, "measure"))
        denominator = []
    if not numerator:
        raise LocatorError("Inline XBRL unit has no numerator measure.")
    return numerator, denominator


def _canonical_numeric(raw_text: str, *, scale: int | None, sign: str | None, nil: bool) -> str:
    if nil:
        raise LocatorError("A nil Inline XBRL fact cannot be projected as a numerical assertion.")
    normalized = raw_text.replace("\u00a0", "").replace(",", "").strip()
    if normalized.startswith("(") and normalized.endswith(")"):
        normalized = "-" + normalized[1:-1]
    normalized = re.sub(r"[^0-9.+-]", "", normalized)
    try:
        value = Decimal(normalized)
    except InvalidOperation as exc:
        raise LocatorError(f"Inline XBRL numeric text {raw_text!r} is not a closed decimal.") from exc
    if sign == "-":
        value = -abs(value)
    elif sign not in {None, "+"}:
        raise LocatorError(f"Unsupported Inline XBRL sign {sign!r}.")
    if scale is not None:
        value *= Decimal(10) ** scale
    return canonical_decimal(value)


def replay_inline_xbrl_locator(
    document: DiscoveredDocument,
    locator: Mapping[str, Any],
) -> dict[str, Any]:
    """Replay one complete locator from the immutable document snapshot."""

    if locator.get("locator_kind") != "inline-xbrl-fact" or locator.get("extraction_method_id") != METHOD_ID:
        raise LocatorError("Inline XBRL locator kind or method is not supported.")
    if hashlib.sha256(document.verified_bytes).hexdigest() != document.content_sha256:
        raise LocatorError("Inline XBRL verified byte snapshot changed.")
    root = lxml_html.fromstring(document.verified_bytes)
    fact_id = str(locator["fact_id"])
    fact = _one(root, f"//*[@id={_xpath_literal(fact_id)}]", subject=f"fact {fact_id!r}")
    if str(fact.tag).casefold() not in _NUMERIC_TAGS:
        raise LocatorError("Inline XBRL locator resolved to a nonnumeric fact.")
    context_id = str(fact.get("contextref") or "")
    unit_ref = str(fact.get("unitref") or "")
    if not context_id or not unit_ref:
        raise LocatorError("Inline XBRL numeric fact lacks contextRef or unitRef.")
    raw_text, continuation_ids, continuation_paths, continuation_digest = _continuations(root, fact)
    context = _context(root, context_id)
    numerator, denominator = _unit(root, unit_ref)
    nil = str(fact.get("xsi:nil") or fact.get("nil") or "false").casefold() in {"true", "1"}
    scale = int(fact.get("scale")) if fact.get("scale") is not None else None
    sign = fact.get("sign")
    canonical_value = _canonical_numeric(raw_text, scale=scale, sign=sign, nil=nil)
    replayed = {
        "fact_id": fact_id,
        "concept": str(fact.get("name") or ""),
        "context_id": context_id,
        **context,
        "unit_ref": unit_ref,
        "unit_numerator_measures": numerator,
        "unit_denominator_measures": denominator,
        "decimals": fact.get("decimals"),
        "scale": scale,
        "sign": sign,
        "format": fact.get("format"),
        "nil": nil,
        "continuation_ids": list(continuation_ids),
        "continuation_paths": list(continuation_paths),
        "continuation_digest": continuation_digest,
        "raw_text": raw_text,
        "canonical_value": canonical_value,
        "dom_node_path": _node_path(fact),
    }
    expected_fields = tuple(replayed)
    for field in expected_fields:
        if locator.get(field) != replayed[field]:
            raise LocatorError(f"Inline XBRL locator field {field!r} changed for fact {fact_id!r}.")
    excerpt = raw_text
    if locator.get("excerpt") != excerpt or locator.get("excerpt_sha256") != text_sha256(excerpt):
        raise LocatorError(f"Inline XBRL excerpt changed for fact {fact_id!r}.")
    return replayed


def capture_inline_xbrl_locator(
    document: DiscoveredDocument,
    *,
    fact_id: str,
    locator_key: str,
    ordinal: int = 1,
    review_state: str = "accepted",
) -> dict[str, Any]:
    """Capture one complete locator for a reviewed fixture from verified bytes.

    Production replay never calls this helper; it exists so source-backed fixture
    construction and tests do not hand-copy context, unit or DOM diagnostics.
    """

    if hashlib.sha256(document.verified_bytes).hexdigest() != document.content_sha256:
        raise LocatorError("Inline XBRL verified byte snapshot changed.")
    root = lxml_html.fromstring(document.verified_bytes)
    fact = _one(root, f"//*[@id={_xpath_literal(fact_id)}]", subject=f"fact {fact_id!r}")
    if str(fact.tag).casefold() not in _NUMERIC_TAGS:
        raise LocatorError("Inline XBRL capture resolved to a nonnumeric fact.")
    context_id = str(fact.get("contextref") or "")
    unit_ref = str(fact.get("unitref") or "")
    if not context_id or not unit_ref:
        raise LocatorError("Inline XBRL numeric fact lacks contextRef or unitRef.")
    raw_text, continuation_ids, continuation_paths, continuation_digest = _continuations(root, fact)
    context = _context(root, context_id)
    numerator, denominator = _unit(root, unit_ref)
    nil = str(fact.get("xsi:nil") or fact.get("nil") or "false").casefold() in {"true", "1"}
    scale = int(fact.get("scale")) if fact.get("scale") is not None else None
    sign = fact.get("sign")
    canonical_value = _canonical_numeric(raw_text, scale=scale, sign=sign, nil=nil)
    return {
        "locator_kind": "inline-xbrl-fact",
        "locator_version": 1,
        "locator_key": locator_key,
        "ordinal": ordinal,
        "extraction_method_id": METHOD_ID,
        "excerpt": raw_text,
        "excerpt_sha256": text_sha256(raw_text),
        "review_state": review_state,
        "fact_id": fact_id,
        "concept": str(fact.get("name") or ""),
        "context_id": context_id,
        **context,
        "unit_ref": unit_ref,
        "unit_numerator_measures": numerator,
        "unit_denominator_measures": denominator,
        "decimals": fact.get("decimals"),
        "scale": scale,
        "sign": sign,
        "format": fact.get("format"),
        "nil": nil,
        "continuation_ids": list(continuation_ids),
        "continuation_paths": list(continuation_paths),
        "continuation_digest": continuation_digest,
        "raw_text": raw_text,
        "canonical_value": canonical_value,
        "dom_node_path": _node_path(fact),
    }


def extract_inline_xbrl_evidence(
    document: DiscoveredDocument,
    assertions: list[Mapping[str, Any]],
) -> tuple[ExtractedEvidence, ...]:
    result: list[ExtractedEvidence] = []
    seen_locator_keys: set[str] = set()
    for assertion in sorted(assertions, key=lambda row: str(row["assertion_key"])):
        locator = assertion["locator"]
        locator_key = str(locator["locator_key"])
        if locator_key in seen_locator_keys:
            raise LocatorError(f"Duplicate Inline XBRL locator key {locator_key!r}.")
        seen_locator_keys.add(locator_key)
        replayed = replay_inline_xbrl_locator(document, locator)
        result.append(
            ExtractedEvidence(
                assertion_key=str(assertion["assertion_key"]),
                document_key=document.spec.document_key,
                locator_kind="inline-xbrl-fact",
                locator_key=locator_key,
                ordinal=int(locator["ordinal"]),
                extraction_method_id=METHOD_ID,
                excerpt=str(locator["excerpt"]),
                excerpt_sha256=str(locator["excerpt_sha256"]),
                value_text=str(replayed["canonical_value"]),
                comparison_text=None,
                review_state=str(locator["review_state"]),
                diagnostics={"inline_xbrl": replayed},
            )
        )
    return tuple(result)
