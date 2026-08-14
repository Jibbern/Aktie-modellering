"""Fail-closed reconstruction of semantic Inline XBRL fact text."""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping


INLINE_XBRL_FACT_TEXT_CONTRACT_ID = "contract:inline-xbrl-fact-text@1"
INLINE_XBRL_CONTINUATION_MAX_HOPS = 1024


class InlineXbrlContinuationError(ValueError):
    """Raised when a continuedAt chain cannot be reconstructed uniquely."""

    def __init__(self, code: str, message: str, **context: Any) -> None:
        super().__init__(message)
        self.code = str(code)
        self.context: Mapping[str, Any] = MappingProxyType(dict(context))


@dataclass(frozen=True)
class InlineXbrlFactText:
    contract_id: str
    text: str
    continuation_ids: tuple[str, ...]
    continuation_nodes: tuple[Any, ...]
    fragment_count: int


def _attributes(node: Any) -> Mapping[Any, Any]:
    attrs = getattr(node, "attrs", None)
    if isinstance(attrs, Mapping):
        return attrs
    attrib = getattr(node, "attrib", None)
    if isinstance(attrib, Mapping):
        return attrib
    return {}


def _attribute(node: Any, expected: str) -> str | None:
    expected_key = str(expected).rsplit(":", 1)[-1].casefold()
    for key, value in _attributes(node).items():
        key_text = str(key).rsplit("}", 1)[-1].rsplit(":", 1)[-1].casefold()
        if key_text != expected_key:
            continue
        if isinstance(value, (list, tuple)):
            value = " ".join(str(item) for item in value)
        return str(value)
    return None


def _local_name(node: Any) -> str:
    name = getattr(node, "name", None)
    if name is None:
        name = getattr(node, "tag", "")
    return str(name).rsplit("}", 1)[-1].rsplit(":", 1)[-1].casefold()


def _raw_text(node: Any) -> str:
    if callable(getattr(node, "itertext", None)):
        value = " ".join(str(part) for part in node.itertext())
    elif callable(getattr(node, "get_text", None)):
        value = node.get_text(" ", strip=False)
    else:
        raise InlineXbrlContinuationError(
            "unsupported_node",
            "Inline XBRL text reconstruction received a node without a text API.",
            node_type=type(node).__name__,
        )
    return unicodedata.normalize("NFC", str(value or ""))


def _join_fragments(fragments: list[str]) -> str:
    normalized = [re.sub(r"\s+", " ", fragment).strip() for fragment in fragments]
    joined = ""
    for fragment in normalized:
        if not fragment:
            continue
        if not joined:
            joined = fragment
            continue
        if fragment.startswith((",", ".", ";", ":", "!", "?", "%", ")", "]", "}", "-", "\u2013", "\u2014")) or joined.endswith(
            ("(", "[", "{", "-", "\u2013", "\u2014")
        ):
            joined += fragment
        else:
            joined += " " + fragment
    return joined


def _nodes_with_id(root: Any, target_id: str) -> list[Any]:
    if callable(getattr(root, "xpath", None)):
        try:
            return list(root.xpath("//*[@id=$target_id]", target_id=target_id))
        except Exception as exc:
            raise InlineXbrlContinuationError(
                "id_resolution_failed",
                f"Inline XBRL continuation ID {target_id!r} could not be resolved.",
                continuation_id=target_id,
            ) from exc
    if callable(getattr(root, "find_all", None)):
        return list(root.find_all(id=target_id))
    raise InlineXbrlContinuationError(
        "unsupported_root",
        "Inline XBRL text reconstruction received a root without an ID lookup API.",
        root_type=type(root).__name__,
    )


def reconstruct_inline_xbrl_fact_text(
    root: Any,
    fact: Any,
    *,
    max_hops: int = INLINE_XBRL_CONTINUATION_MAX_HOPS,
) -> InlineXbrlFactText:
    """Return immediate fact text plus its explicit continuedAt chain.

    Fragments are joined in chain order with one semantic boundary space, except
    when joining punctuation makes the fragments contiguous (for example,
    ``December 31`` + ``, 2019``). Neighboring DOM text is excluded. Every
    endpoint must resolve one Inline XBRL continuation node.
    """

    if int(max_hops) <= 0:
        raise ValueError("max_hops must be positive")
    fragments = [_raw_text(fact)]
    continuation_ids: list[str] = []
    continuation_nodes: list[Any] = []
    seen: set[str] = set()
    next_raw = _attribute(fact, "continuedAt")

    while next_raw is not None:
        continuation_id = str(next_raw).strip()
        if not continuation_id:
            raise InlineXbrlContinuationError(
                "empty_target",
                "Inline XBRL continuedAt must not be empty.",
                fact_id=_attribute(fact, "id"),
            )
        if continuation_id in seen:
            raise InlineXbrlContinuationError(
                "cycle",
                "Inline XBRL continuation chain contains a cycle.",
                fact_id=_attribute(fact, "id"),
                continuation_id=continuation_id,
                visited_continuation_ids=tuple(continuation_ids),
            )
        if len(continuation_ids) >= int(max_hops):
            raise InlineXbrlContinuationError(
                "excessive_chain",
                f"Inline XBRL continuation chain exceeds {int(max_hops)} links.",
                fact_id=_attribute(fact, "id"),
                continuation_id=continuation_id,
                visited_continuation_ids=tuple(continuation_ids),
            )
        seen.add(continuation_id)
        matches = _nodes_with_id(root, continuation_id)
        if len(matches) != 1:
            raise InlineXbrlContinuationError(
                "target_cardinality",
                f"Inline XBRL continuation {continuation_id!r} matched {len(matches)} nodes, not one.",
                fact_id=_attribute(fact, "id"),
                continuation_id=continuation_id,
                matching_node_count=len(matches),
                matching_node_tags=tuple(_local_name(node) for node in matches),
            )
        continuation = matches[0]
        if _local_name(continuation) != "continuation":
            raise InlineXbrlContinuationError(
                "wrong_target_type",
                "Inline XBRL continuedAt endpoint is not an ix:continuation node.",
                fact_id=_attribute(fact, "id"),
                continuation_id=continuation_id,
                matching_node_tag=_local_name(continuation),
            )
        continuation_ids.append(continuation_id)
        continuation_nodes.append(continuation)
        fragments.append(_raw_text(continuation))
        next_raw = _attribute(continuation, "continuedAt")

    reconstructed = _join_fragments(fragments)
    return InlineXbrlFactText(
        contract_id=INLINE_XBRL_FACT_TEXT_CONTRACT_ID,
        text=reconstructed,
        continuation_ids=tuple(continuation_ids),
        continuation_nodes=tuple(continuation_nodes),
        fragment_count=len(fragments),
    )
