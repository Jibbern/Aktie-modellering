"""Text-layer-only PDF evidence extraction with deterministic table locators."""
from __future__ import annotations

import hashlib
from datetime import datetime
from io import BytesIO
from typing import Any, Mapping

import pdfplumber

from .types import DiscoveredDocument, ExtractedEvidence, LocatorError, text_sha256


METHOD_ID = "extractor:source:pdf-text-table@1"
DATELINE_METHOD_ID = "extractor:source:pdf-dateline@1"


def _text(value: Any) -> str:
    return " ".join(str(value or "").split())


def extract_pdf_evidence(
    document: DiscoveredDocument,
    assertions: list[Mapping[str, Any]],
) -> tuple[ExtractedEvidence, ...]:
    result: list[ExtractedEvidence] = []
    stream = BytesIO(document.verified_bytes)
    if hashlib.sha256(stream.getbuffer()).hexdigest() != document.content_sha256:
        raise LocatorError(f"PDF byte snapshot changed for {document.spec.document_key!r}.")
    with pdfplumber.open(stream) as pdf:
        for assertion in sorted(assertions, key=lambda row: str(row["assertion_key"])):
            locator = assertion["locator"]
            if locator["locator_kind"] != "pdf-table":
                raise LocatorError(f"Unsupported PDF locator kind {locator['locator_kind']!r}.")
            if locator["extraction_method_id"] != METHOD_ID:
                raise LocatorError(
                    f"PDF extraction method changed for {assertion['assertion_key']!r}."
                )
            page_number = int(locator["page"])
            if page_number < 1 or page_number > len(pdf.pages):
                raise LocatorError(f"PDF page is invalid for {assertion['assertion_key']!r}.")
            page = pdf.pages[page_number - 1]
            page_text = _text(page.extract_text())
            if not page_text:
                raise LocatorError(f"PDF text extraction returned empty text on page {page_number}.")
            if _text(locator["region_locator"]).casefold() not in page_text.casefold():
                raise LocatorError(f"PDF region fingerprint changed for {assertion['assertion_key']!r}.")
            if _text(locator["column_header_fingerprint"]).casefold() not in page_text.casefold():
                raise LocatorError(f"PDF column fingerprint changed for {assertion['assertion_key']!r}.")

            tables = page.extract_tables()
            fingerprints = [_text(value).casefold() for value in locator["table_fingerprints"]]
            matches: list[tuple[int, list[list[Any]]]] = []
            for index, table in enumerate(tables):
                flattened = _text(" ".join(_text(cell) for row in table for cell in row)).casefold()
                if all(fingerprint in flattened for fingerprint in fingerprints):
                    matches.append((index, table))
            if len(matches) != 1:
                raise LocatorError(
                    f"PDF locator {locator['locator_key']!r} matched {len(matches)} tables, not one."
                )
            table_index, table = matches[0]
            if table_index != locator["table_index"]:
                raise LocatorError(f"PDF table diagnostic changed for {assertion['assertion_key']!r}.")
            rows = [[_text(cell) for cell in row] for row in table]
            start = int(locator["row_index"])
            end = int(locator["row_end_index"]) if locator["row_end_index"] is not None else start
            if start >= len(rows) or end >= len(rows) or end < start:
                raise LocatorError(f"PDF row position is invalid for {assertion['assertion_key']!r}.")
            selected_text = " | ".join(value for row in rows[start : end + 1] for value in row if value)
            if _text(locator["row_header_fingerprint"]).casefold() not in selected_text.casefold():
                raise LocatorError(f"PDF row fingerprint changed for {assertion['assertion_key']!r}.")

            value_text: str | None = None
            comparison_text: str | None = None
            if locator["column_index"] is not None:
                column = int(locator["column_index"])
                if start != end or column >= len(rows[start]):
                    raise LocatorError(f"PDF column position is invalid for {assertion['assertion_key']!r}.")
                value_text = rows[start][column]
                excerpt_parts = [str(locator["region_locator"]), rows[start][0], value_text]
                if locator["comparison_column_index"] is not None:
                    comparison_column = int(locator["comparison_column_index"])
                    if comparison_column >= len(rows[start]):
                        raise LocatorError(f"PDF comparison column is invalid for {assertion['assertion_key']!r}.")
                    comparison_text = rows[start][comparison_column]
                    excerpt_parts.append(f"previous: {comparison_text}")
                excerpt = " | ".join(value for value in excerpt_parts if value)
            else:
                value_text = selected_text
                excerpt = " | ".join((str(locator["region_locator"]), selected_text))

            if excerpt != locator["excerpt"]:
                raise LocatorError(
                    f"PDF excerpt mismatch for {assertion['assertion_key']!r}: "
                    f"expected {locator['excerpt']!r}, received {excerpt!r}."
                )
            if text_sha256(excerpt) != locator["excerpt_sha256"]:
                raise LocatorError(f"PDF excerpt digest mismatch for {assertion['assertion_key']!r}.")
            result.append(
                ExtractedEvidence(
                    assertion_key=str(assertion["assertion_key"]),
                    document_key=document.spec.document_key,
                    locator_kind="page",
                    locator_key=str(locator["locator_key"]),
                    ordinal=int(locator["ordinal"]),
                    extraction_method_id=str(locator["extraction_method_id"]),
                    excerpt=excerpt,
                    excerpt_sha256=str(locator["excerpt_sha256"]),
                    value_text=value_text,
                    comparison_text=comparison_text,
                    review_state=str(locator["review_state"]),
                    diagnostics={
                        "page": page_number,
                        "table_index": table_index,
                        "row_index": start,
                        "row_end_index": end,
                        "column_index": locator["column_index"],
                    },
                )
            )
    return tuple(result)


def replay_pdf_dateline(document: DiscoveredDocument) -> str:
    locator = document.spec.publication_date_locator
    if locator is None or locator.get("locator_kind") != "pdf-dateline":
        raise LocatorError(f"PDF source {document.spec.document_key!r} has no dateline locator.")
    if locator.get("extraction_method_id") != DATELINE_METHOD_ID:
        raise LocatorError(f"PDF dateline method changed for {document.spec.document_key!r}.")
    stream = BytesIO(document.verified_bytes)
    if hashlib.sha256(stream.getbuffer()).hexdigest() != document.content_sha256:
        raise LocatorError(f"PDF byte snapshot changed for {document.spec.document_key!r}.")
    with pdfplumber.open(stream) as pdf:
        page_number = int(locator.get("page", 0))
        if page_number < 1 or page_number > len(pdf.pages):
            raise LocatorError(f"PDF dateline page is invalid for {document.spec.document_key!r}.")
        page_text = _text(pdf.pages[page_number - 1].extract_text())
    if not page_text:
        raise LocatorError(f"PDF dateline text layer is empty for {document.spec.document_key!r}.")
    fingerprint = _text(locator.get("text_fingerprint"))
    occurrences: list[int] = []
    start = 0
    while True:
        match = page_text.casefold().find(fingerprint.casefold(), start)
        if match < 0:
            break
        occurrences.append(match)
        start = match + max(1, len(fingerprint))
    ordinal = int(locator.get("match_ordinal", 0))
    if ordinal < 1 or ordinal > len(occurrences):
        raise LocatorError(f"PDF dateline locator failed for {document.spec.document_key!r}.")
    excerpt = page_text[occurrences[ordinal - 1] : occurrences[ordinal - 1] + len(fingerprint)]
    if excerpt != fingerprint or text_sha256(excerpt) != locator.get("excerpt_sha256"):
        raise LocatorError(f"PDF dateline evidence changed for {document.spec.document_key!r}.")
    try:
        return datetime.strptime(excerpt, "%B %d, %Y").date().isoformat()
    except ValueError as exc:
        raise LocatorError(f"PDF dateline is not an exact publication date for {document.spec.document_key!r}.") from exc
