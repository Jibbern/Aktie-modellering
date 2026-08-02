"""Exact line-range transcript extraction with no filename inference."""
from __future__ import annotations

from typing import Any, Mapping

from .types import DiscoveredDocument, ExtractedEvidence, LocatorError, text_sha256


METHOD_ID = "extractor:source:text-exact-lines@1"


def extract_text_evidence(
    document: DiscoveredDocument,
    assertions: list[Mapping[str, Any]],
) -> tuple[ExtractedEvidence, ...]:
    try:
        lines = document.verified_bytes.decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise LocatorError(
            f"Transcript {document.spec.document_key!r} is not strict UTF-8 text."
        ) from exc
    result: list[ExtractedEvidence] = []
    for assertion in sorted(assertions, key=lambda row: str(row["assertion_key"])):
        locator = assertion["locator"]
        if locator["locator_kind"] != "text-lines":
            raise LocatorError(f"Unsupported text locator kind {locator['locator_kind']!r}.")
        if locator["extraction_method_id"] != METHOD_ID:
            raise LocatorError(
                f"Transcript extraction method changed for {assertion['assertion_key']!r}."
            )
        start, end = int(locator["start_line"]), int(locator["end_line"])
        if end < start or start < 1 or end > len(lines):
            raise LocatorError(f"Transcript line range is invalid for {assertion['assertion_key']!r}.")
        excerpt = "\n".join(lines[start - 1 : end])
        digest = text_sha256(excerpt)
        if digest != locator["line_digest"] or digest != locator["excerpt_sha256"]:
            raise LocatorError(f"Transcript line digest changed for {assertion['assertion_key']!r}.")
        if excerpt != locator["excerpt"]:
            raise LocatorError(f"Transcript line text changed for {assertion['assertion_key']!r}.")
        speaker = locator["speaker_fingerprint"]
        if speaker is not None:
            prior = lines[max(0, start - 26) : start - 1]
            matches = [index for index, line in enumerate(prior) if str(speaker).casefold() in line.casefold()]
            if not matches:
                raise LocatorError(f"Transcript speaker diagnostic changed for {assertion['assertion_key']!r}.")
        expected_turn = (
            f"one-based line {start}; nearest reviewed speaker header"
            if speaker is not None
            else f"one-based line {start}; no speaker asserted"
        )
        if locator["turn_diagnostics"] != expected_turn:
            raise LocatorError(f"Transcript turn diagnostic changed for {assertion['assertion_key']!r}.")
        result.append(
            ExtractedEvidence(
                assertion_key=str(assertion["assertion_key"]),
                document_key=document.spec.document_key,
                locator_kind="line",
                locator_key=str(locator["locator_key"]),
                ordinal=int(locator["ordinal"]),
                extraction_method_id=str(locator["extraction_method_id"]),
                excerpt=excerpt,
                excerpt_sha256=str(locator["excerpt_sha256"]),
                value_text=excerpt,
                comparison_text=None,
                review_state=str(locator["review_state"]),
                diagnostics={
                    "start_line": start,
                    "end_line": end,
                    "speaker_fingerprint": speaker,
                    "turn_diagnostics": locator["turn_diagnostics"],
                },
            )
        )
    return tuple(result)
