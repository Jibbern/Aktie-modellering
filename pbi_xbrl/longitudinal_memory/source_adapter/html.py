"""Deterministic HTML evidence extraction from semantic fingerprints."""
from __future__ import annotations

from datetime import datetime
import re
from typing import Any, Mapping
import unicodedata

from lxml import html as lxml_html

from .types import DiscoveredDocument, ExtractedEvidence, LocatorError, text_sha256


TABLE_METHOD_ID = "extractor:source:html-semantic-table@1"
TEXT_METHOD_ID = "extractor:source:html-text-node@1"
DATELINE_METHOD_ID = "extractor:source:html-dateline@1"
FISCAL_LABEL_METHOD_ID = "extractor:source:html-fiscal-label@1"

_FISCAL_YEAR = re.compile(
    r"\b(?:for\s+)?fiscal(?:\s+year)?\s+(?P<year>[0-9]{4})\b",
    flags=re.IGNORECASE,
)
_FULL_YEAR = re.compile(
    r"\b(?:(?P<prefix_year>[0-9]{4})\s+)?full[- ]year(?:\s+fiscal)?"
    r"(?:\s+(?P<year>[0-9]{4}))?\b",
    flags=re.IGNORECASE,
)
_FISCAL_QUARTER = re.compile(
    r"\b(?:(?P<word>first|second|third|fourth)\s+quarter|q(?P<number>[1-4]))"
    r"(?:\s+\(in\s+thousands\))?(?:\s+of)?"
    r"(?:\s+(?:fiscal\s+)?(?P<year>[0-9]{4}))?\b",
    flags=re.IGNORECASE,
)
_FISCAL_YTD = re.compile(
    r"\b(?:fiscal\s+)?(?:year[- ]to[- ]date|ytd)(?:\s+(?P<year>[0-9]{4}))?\b",
    flags=re.IGNORECASE,
)
_TRAILING_FOUR_QUARTERS = re.compile(
    r"\b(?:trailing\s+four\s+quarters|last\s+four\s+quarters|tfq)"
    r"(?:\s+(?P<year>[0-9]{4}))?\b",
    flags=re.IGNORECASE,
)
_QUARTERS = {"first": 1, "second": 2, "third": 3, "fourth": 4}
_CLAIM_KIND_BY_PERIOD_TYPE = {
    "unspecified_fiscal_context": "fiscal-year",
    "fiscal_quarter": "fiscal-quarter",
    "fiscal_year": "annual-period",
    "fiscal_ytd": "fiscal-ytd",
    "trailing_four_quarters": "trailing-four-quarters",
}


def _text(value: Any) -> str:
    if hasattr(value, "itertext"):
        value = " ".join(value.itertext())
    return " ".join(unicodedata.normalize("NFC", str(value or "")).split())


def derive_fiscal_label_semantics(source_text: str) -> dict[str, Any]:
    """Derive one closed fiscal meaning from normalized verified source text."""

    normalized = _text(source_text)
    if not normalized:
        raise LocatorError("Fiscal-label evidence cannot be empty.")

    years = {int(match.group("year")) for match in _FISCAL_YEAR.finditer(normalized)}
    specific_types: set[str] = set()
    quarters: set[int] = set()

    for match in _FULL_YEAR.finditer(normalized):
        specific_types.add("fiscal_year")
        if match.group("prefix_year") is not None:
            years.add(int(match.group("prefix_year")))
        if match.group("year") is not None:
            years.add(int(match.group("year")))
    for match in _FISCAL_QUARTER.finditer(normalized):
        specific_types.add("fiscal_quarter")
        word = match.group("word")
        quarters.add(_QUARTERS[word.casefold()] if word is not None else int(match.group("number")))
        if match.group("year") is not None:
            years.add(int(match.group("year")))
    for match in _FISCAL_YTD.finditer(normalized):
        specific_types.add("fiscal_ytd")
        if match.group("year") is not None:
            years.add(int(match.group("year")))
    for match in _TRAILING_FOUR_QUARTERS.finditer(normalized):
        specific_types.add("trailing_four_quarters")
        if match.group("year") is not None:
            years.add(int(match.group("year")))

    if len(years) > 1:
        raise LocatorError("Fiscal-label source text contains incompatible fiscal years.")
    if len(specific_types) > 1:
        raise LocatorError("Fiscal-label source text contains multiple incompatible period types.")
    if len(quarters) > 1:
        raise LocatorError("Fiscal-label source text contains incompatible fiscal quarters.")
    if not specific_types:
        if len(years) != 1:
            raise LocatorError("Fiscal-label source text has no closed fiscal meaning.")
        period_type = "unspecified_fiscal_context"
    else:
        period_type = next(iter(specific_types))

    if period_type == "fiscal_quarter":
        if len(quarters) != 1:
            raise LocatorError("Fiscal-quarter source text lacks one explicit quarter.")
        fiscal_quarter: int | None = next(iter(quarters))
    else:
        if quarters:
            raise LocatorError("Non-quarter source text cannot carry a fiscal quarter.")
        fiscal_quarter = None

    return {
        "claim_kind": _CLAIM_KIND_BY_PERIOD_TYPE[period_type],
        "fiscal_year": next(iter(years)) if years else None,
        "period_type": period_type,
        "fiscal_quarter": fiscal_quarter,
        "claim_specificity": (
            "generic" if period_type == "unspecified_fiscal_context" else "specific"
        ),
        "source_text": normalized,
    }


def _cell_rows(table: Any) -> list[list[Any]]:
    result: list[list[Any]] = []
    for row in table.xpath(".//tr"):
        cells = row.xpath("./th | ./td")
        if any(_text(cell) for cell in cells):
            result.append(cells)
    return result


def _rows(table: Any) -> list[list[str]]:
    return [[_text(cell) for cell in cells] for cells in _cell_rows(table)]


def _span_fingerprint(table: Any, start: int, end: int) -> str:
    rows = _cell_rows(table)
    return ";".join(
        f"r{row_index}c{cell_index}:{cell.get('colspan') or '1'}x{cell.get('rowspan') or '1'}"
        for row_index in range(start, end + 1)
        for cell_index, cell in enumerate(rows[row_index])
    )


def _verify_expected(assertion_key: str, locator: Mapping[str, Any], excerpt: str) -> None:
    if excerpt != locator.get("excerpt"):
        raise LocatorError(
            f"HTML excerpt mismatch for {assertion_key!r}: expected {locator.get('excerpt')!r}, "
            f"received {excerpt!r}."
        )
    if text_sha256(excerpt) != locator.get("excerpt_sha256"):
        raise LocatorError(f"HTML excerpt digest mismatch for {assertion_key!r}.")


def _bounded_subtext(
    excerpt: str,
    fingerprint: Any,
    *,
    assertion_key: str,
    field: str,
) -> str | None:
    if fingerprint is None:
        return None
    expected = _text(fingerprint)
    starts: list[int] = []
    cursor = 0
    while True:
        match = excerpt.casefold().find(expected.casefold(), cursor)
        if match < 0:
            break
        starts.append(match)
        cursor = match + max(1, len(expected))
    if len(starts) != 1:
        raise LocatorError(
            f"HTML {field} fingerprint for {assertion_key!r} matched {len(starts)} times, not one."
        )
    value = excerpt[starts[0] : starts[0] + len(expected)]
    if value.casefold() != expected.casefold():
        raise LocatorError(f"HTML {field} fingerprint changed for {assertion_key!r}.")
    return value


def _replay_fiscal_label_claims(
    root: Any,
    locator: Mapping[str, Any],
    assertion_key: str,
) -> tuple[dict[str, Any], ...]:
    evidence = locator.get("fiscal_label_evidence")
    if evidence is None:
        return ()
    if evidence.get("locator_kind") != "html-fiscal-labels":
        raise LocatorError(f"HTML fiscal-label locator changed for {assertion_key!r}.")
    if evidence.get("extraction_method_id") != FISCAL_LABEL_METHOD_ID:
        raise LocatorError(f"HTML fiscal-label extraction method changed for {assertion_key!r}.")

    document_text = _text(root)
    claims = list(evidence.get("claims", ()))
    claim_keys = [str(claim.get("claim_key")) for claim in claims]
    if len(set(claim_keys)) != len(claim_keys):
        raise LocatorError(f"HTML fiscal-label claim keys are duplicated for {assertion_key!r}.")

    replayed: list[dict[str, Any]] = []
    for claim in sorted(claims, key=lambda row: str(row["claim_key"])):
        fingerprint = _text(claim["text_fingerprint"])
        occurrences: list[int] = []
        start = 0
        while True:
            match = document_text.find(fingerprint, start)
            if match < 0:
                break
            occurrences.append(match)
            start = match + max(1, len(fingerprint))
        ordinal = int(claim["match_ordinal"])
        if ordinal < 1 or ordinal > len(occurrences):
            raise LocatorError(
                f"HTML fiscal-label claim {claim['claim_key']!r} failed for {assertion_key!r}."
            )
        excerpt = document_text[
            occurrences[ordinal - 1] : occurrences[ordinal - 1] + len(fingerprint)
        ]
        if excerpt != fingerprint or text_sha256(excerpt) != claim["excerpt_sha256"]:
            raise LocatorError(
                f"HTML fiscal-label evidence changed for {assertion_key!r}."
            )
        semantics = derive_fiscal_label_semantics(excerpt)
        if semantics["claim_kind"] != str(claim["claim_kind"]):
            raise LocatorError(
                f"HTML fiscal-label claim {claim['claim_key']!r} declares "
                f"{claim['claim_kind']!r} but verified source text derives "
                f"{semantics['claim_kind']!r}."
            )
        replayed.append(
            {
                "claim_key": str(claim["claim_key"]),
                "claim_kind": semantics["claim_kind"],
                "fiscal_year": semantics["fiscal_year"],
                "period_type": semantics["period_type"],
                "fiscal_quarter": semantics["fiscal_quarter"],
                "claim_specificity": semantics["claim_specificity"],
                "source_text": semantics["source_text"],
                "locator_identity": (
                    f"{locator['locator_key']}#fiscal-label:{claim['claim_key']}"
                ),
                "match_ordinal": ordinal,
                "extraction_method_id": FISCAL_LABEL_METHOD_ID,
                "digest": str(claim["excerpt_sha256"]),
            }
        )
    return tuple(replayed)


def _extract_table(
    document: DiscoveredDocument,
    assertion: Mapping[str, Any],
    root: Any,
) -> ExtractedEvidence:
    locator = assertion["locator"]
    if locator["extraction_method_id"] != TABLE_METHOD_ID:
        raise LocatorError(
            f"HTML table extraction method changed for {assertion['assertion_key']!r}."
        )
    tables = root.xpath("//table")
    fingerprints = [_text(value).casefold() for value in locator["table_fingerprints"]]
    matches: list[tuple[int, Any]] = []
    for index, table in enumerate(tables):
        table_text = _text(table).casefold()
        if all(fingerprint in table_text for fingerprint in fingerprints):
            matches.append((index, table))
    if len(matches) != 1:
        raise LocatorError(
            f"HTML locator {locator['locator_key']!r} matched {len(matches)} tables, not one."
        )
    table_index, table = matches[0]
    if table_index != locator["table_index"]:
        raise LocatorError(
            f"HTML table diagnostic changed for {assertion['assertion_key']!r}: "
            f"expected {locator['table_index']}, received {table_index}."
        )
    rows = _rows(table)
    start = int(locator["row_index"])
    end = int(locator["row_end_index"]) if locator["row_end_index"] is not None else start
    if start >= len(rows) or end >= len(rows) or end < start:
        raise LocatorError(f"HTML row position is invalid for {assertion['assertion_key']!r}.")
    selected = rows[start : end + 1]
    if _span_fingerprint(table, start, end) != locator["cell_span_fingerprint"]:
        raise LocatorError(f"HTML merged-cell/span fingerprint changed for {assertion['assertion_key']!r}.")
    selected_text = " | ".join(value for row in selected for value in row if value)
    if _text(locator["row_header_fingerprint"]).casefold() not in selected_text.casefold():
        raise LocatorError(f"HTML row fingerprint changed for {assertion['assertion_key']!r}.")
    table_text = _text(table)
    if _text(locator["column_header_fingerprint"]).casefold() not in table_text.casefold():
        raise LocatorError(f"HTML column fingerprint changed for {assertion['assertion_key']!r}.")
    through_row = " | ".join(value for row in rows[: end + 1] for value in row if value)
    if _text(locator["section_fingerprint"]).casefold() not in through_row.casefold():
        raise LocatorError(f"HTML section fingerprint changed for {assertion['assertion_key']!r}.")

    context_index = locator["context_row_index"]
    context = ""
    if context_index is not None:
        if int(context_index) >= len(rows):
            raise LocatorError(f"HTML context row is invalid for {assertion['assertion_key']!r}.")
        context = " | ".join(value for value in rows[int(context_index)] if value)

    value_text: str | None = None
    comparison_text: str | None = None
    if locator["cell_index"] is not None:
        cell_index = int(locator["cell_index"])
        if start != end or cell_index >= len(rows[start]):
            raise LocatorError(f"HTML cell position is invalid for {assertion['assertion_key']!r}.")
        value_text = rows[start][cell_index]
        parts = [value for value in (context, rows[start][0], value_text) if value]
        if locator["comparison_cell_index"] is not None:
            comparison_index = int(locator["comparison_cell_index"])
            if comparison_index >= len(rows[start]):
                raise LocatorError(f"HTML comparison cell is invalid for {assertion['assertion_key']!r}.")
            comparison_text = rows[start][comparison_index]
            parts.append(f"previous: {comparison_text}")
        excerpt = " | ".join(parts)
    else:
        value_text = selected_text
        excerpt = " | ".join(value for value in (context, selected_text) if value)

    expected_position = (
        f"table={table_index};row={start}:{end};cell="
        f"{locator['cell_index'] if locator['cell_index'] is not None else 'range'}"
    )
    if locator["exact_position"] != expected_position:
        raise LocatorError(f"HTML exact position contract is inconsistent for {assertion['assertion_key']!r}.")
    if locator["node_path"] != f"html/table[{table_index}]/row[{start}]":
        raise LocatorError(f"HTML node path changed for {assertion['assertion_key']!r}.")
    _verify_expected(str(assertion["assertion_key"]), locator, excerpt)
    fiscal_label_claims = _replay_fiscal_label_claims(
        root, locator, str(assertion["assertion_key"])
    )
    return ExtractedEvidence(
        assertion_key=str(assertion["assertion_key"]),
        document_key=document.spec.document_key,
        locator_kind="table-row",
        locator_key=str(locator["locator_key"]),
        ordinal=int(locator["ordinal"]),
        extraction_method_id=str(locator["extraction_method_id"]),
        excerpt=excerpt,
        excerpt_sha256=str(locator["excerpt_sha256"]),
        value_text=value_text,
        comparison_text=comparison_text,
        review_state=str(locator["review_state"]),
        diagnostics={
            "node_path": locator["node_path"],
            "table_index": table_index,
            "row_index": start,
            "row_end_index": end,
            "cell_index": locator["cell_index"],
            "fiscal_label_claims": fiscal_label_claims,
        },
    )


def _extract_text(
    document: DiscoveredDocument,
    assertion: Mapping[str, Any],
    root: Any,
) -> ExtractedEvidence:
    locator = assertion["locator"]
    if locator["extraction_method_id"] != TEXT_METHOD_ID:
        raise LocatorError(
            f"HTML text extraction method changed for {assertion['assertion_key']!r}."
        )
    document_text = _text(root)
    fingerprint = _text(locator["text_fingerprint"])
    for ancestor in locator["ancestor_fingerprints"]:
        if _text(ancestor).casefold() not in document_text.casefold():
            raise LocatorError(f"HTML ancestor fingerprint changed for {assertion['assertion_key']!r}.")
    occurrences: list[int] = []
    start = 0
    while True:
        match = document_text.casefold().find(fingerprint.casefold(), start)
        if match < 0:
            break
        occurrences.append(match)
        start = match + max(1, len(fingerprint))
    ordinal = int(locator["match_ordinal"])
    if ordinal < 1 or ordinal > len(occurrences):
        raise LocatorError(
            f"HTML text locator {locator['locator_key']!r} has no deterministic match {ordinal}."
        )
    if locator["node_path"] != f"html/document-text/match[{ordinal}]":
        raise LocatorError(f"HTML text node path changed for {assertion['assertion_key']!r}.")
    excerpt = document_text[occurrences[ordinal - 1] : occurrences[ordinal - 1] + len(fingerprint)]
    if excerpt.casefold() != fingerprint.casefold():
        raise LocatorError(f"HTML text case/normalization drift for {assertion['assertion_key']!r}.")
    _verify_expected(str(assertion["assertion_key"]), locator, excerpt)
    fiscal_label_claims = _replay_fiscal_label_claims(
        root, locator, str(assertion["assertion_key"])
    )
    return ExtractedEvidence(
        assertion_key=str(assertion["assertion_key"]),
        document_key=document.spec.document_key,
        locator_kind="paragraph",
        locator_key=str(locator["locator_key"]),
        ordinal=int(locator["ordinal"]),
        extraction_method_id=str(locator["extraction_method_id"]),
        excerpt=excerpt,
        excerpt_sha256=str(locator["excerpt_sha256"]),
        value_text=(
            _bounded_subtext(
                excerpt,
                locator.get("value_text_fingerprint"),
                assertion_key=str(assertion["assertion_key"]),
                field="value-text",
            )
            or excerpt
        ),
        comparison_text=_bounded_subtext(
            excerpt,
            locator.get("comparison_text_fingerprint"),
            assertion_key=str(assertion["assertion_key"]),
            field="comparison-text",
        ),
        review_state=str(locator["review_state"]),
        diagnostics={
            "node_path": locator["node_path"],
            "match_ordinal": ordinal,
            "fiscal_label_claims": fiscal_label_claims,
        },
    )


def extract_html_evidence(
    document: DiscoveredDocument,
    assertions: list[Mapping[str, Any]],
) -> tuple[ExtractedEvidence, ...]:
    root = lxml_html.fromstring(document.verified_bytes)
    result: list[ExtractedEvidence] = []
    for assertion in sorted(assertions, key=lambda row: str(row["assertion_key"])):
        kind = assertion["locator"]["locator_kind"]
        if kind == "html-table":
            result.append(_extract_table(document, assertion, root))
        elif kind == "html-text":
            result.append(_extract_text(document, assertion, root))
        else:
            raise LocatorError(f"Unsupported HTML locator kind {kind!r}.")
    return tuple(result)


def replay_html_dateline(document: DiscoveredDocument) -> str:
    locator = document.spec.publication_date_locator
    if locator is None or locator.get("locator_kind") != "html-dateline":
        raise LocatorError(f"HTML source {document.spec.document_key!r} has no dateline locator.")
    if locator.get("extraction_method_id") != DATELINE_METHOD_ID:
        raise LocatorError(f"HTML dateline method changed for {document.spec.document_key!r}.")
    root = lxml_html.fromstring(document.verified_bytes)
    document_text = _text(root)
    fingerprint = _text(locator.get("text_fingerprint"))
    occurrences: list[int] = []
    start = 0
    while True:
        match = document_text.casefold().find(fingerprint.casefold(), start)
        if match < 0:
            break
        occurrences.append(match)
        start = match + max(1, len(fingerprint))
    ordinal = int(locator.get("match_ordinal", 0))
    if ordinal < 1 or ordinal > len(occurrences):
        raise LocatorError(f"HTML dateline locator failed for {document.spec.document_key!r}.")
    excerpt = document_text[occurrences[ordinal - 1] : occurrences[ordinal - 1] + len(fingerprint)]
    if excerpt != fingerprint or text_sha256(excerpt) != locator.get("excerpt_sha256"):
        raise LocatorError(f"HTML dateline evidence changed for {document.spec.document_key!r}.")
    try:
        return datetime.strptime(excerpt, "%B %d, %Y").date().isoformat()
    except ValueError as exc:
        raise LocatorError(f"HTML dateline is not an exact publication date for {document.spec.document_key!r}.") from exc
