"""Bounded source-native ANF debt extraction from already-local SEC filings."""
from __future__ import annotations

import hashlib
import json
import re
import warnings
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

from pbi_xbrl.new_ticker_debt_scope import (
    DEBT_PROFILE_ECONOMIC_VALIDATION_ATTR,
    DebtResolutionError,
    dispositions_to_package_section,
    normalize_debt_currency_to_millions,
    resolve_debt_facilities,
    resolve_debt_collections,
    validate_resolved_debt_facility_for_profile,
)
from pbi_xbrl.longitudinal_memory.identity import source_document_identity
from pbi_xbrl.inline_xbrl_text import (
    InlineXbrlContinuationError,
    reconstruct_inline_xbrl_fact_text,
)


ANF_DEBT_HISTORY_START = "2023-04-29"
ANF_DEBT_HISTORY_END = "2026-05-02"
ANF_EXPECTED_ABL_PERIODS = (
    "2023-04-29",
    "2023-07-29",
    "2023-10-28",
    "2024-02-03",
    "2024-05-04",
    "2024-08-03",
    "2024-11-02",
    "2025-02-01",
    "2025-05-03",
    "2025-08-02",
    "2025-11-01",
    "2026-01-31",
    "2026-05-02",
)
ANF_EXPECTED_ABL_PERIOD_COUNT = len(ANF_EXPECTED_ABL_PERIODS)
ANF_LATEST_ABL_HISTORY_LIMIT = 12
ANF_DEBT_EVIDENCE_ADAPTER_ID = "debt-source-adapter:anf-sec-abl@1"

_DOCUMENT_RE = re.compile(r"^doc_(?P<accession>[0-9]{18})_anf-(?P<period>[0-9]{8})\.htm$", re.I)
_BORROWINGS_HEADING_RE = re.compile(r"(?<![A-Z0-9])(?P<number>[0-9]{1,2})\.\s*BORROWINGS\b", re.I)
_DEBT_DISCLOSURE_CONCEPT_RE = re.compile(r"^us-gaap:DebtDisclosureTextBlock$", re.I)
_ABL_SECTION_HEADING_RE = re.compile(r"\bABL Facility\b\s+(?=On\b|The\b)", re.I)
_ABL_SECTION_END_RE = re.compile(r"\bRepresentations,?\s+warranties\s+and\s+covenants\b", re.I)
_CAPACITY_LABELS = (
    "Loan cap",
    "Less: Outstanding stand-by letters of credit",
    "Borrowing capacity",
    "Less: Minimum excess availability",
    "Borrowing capacity available",
)


def _canonical_table_label(value: Any) -> str:
    return re.sub(r"\s*\([0-9]+\)\s*$", "", _clean_text(value))


@dataclass(frozen=True)
class ANFDebtSourceExtraction:
    facilities: tuple[Mapping[str, Any], ...]
    instruments: tuple[Mapping[str, Any], ...]
    maturities: tuple[Mapping[str, Any], ...]
    credit_notes: tuple[Mapping[str, Any], ...]
    source_documents: tuple[Mapping[str, Any], ...]

    def package_section(self) -> dict[str, list[dict[str, Any]]]:
        return {
            "facilities": [dict(row) for row in self.facilities],
            "instruments": [dict(row) for row in self.instruments],
            "maturities": [dict(row) for row in self.maturities],
            "credit_notes": [dict(row) for row in self.credit_notes],
        }

    def coverage(self) -> dict[str, Any]:
        return {
            "adapter": "pbi_xbrl.anf_debt_source_adapter",
            "network_access": False,
            "history_start": ANF_DEBT_HISTORY_START,
            "source_document_count": len(self.source_documents),
            "facility_period_count": len(self.facilities),
            "instrument_period_count": len(self.instruments),
            "maturity_count": len(self.maturities),
            "credit_note_count": len(self.credit_notes),
            "source_documents": [dict(row) for row in self.source_documents],
        }


class DebtSourceFactMissing(DebtResolutionError):
    """A genuinely absent optional source fact, distinct from invalid evidence."""


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _canonical_accession(digits: str) -> str:
    if not re.fullmatch(r"[0-9]{18}", digits):
        raise DebtResolutionError(
            "anf_debt_accession_invalid",
            "ANF debt source filename has no canonical SEC accession identity.",
            raw_accession=digits,
        )
    return f"{digits[:10]}-{digits[10:12]}-{digits[12:]}"


def _parse_date_text(value: str) -> str:
    cleaned = _clean_text(value).replace("Sept.", "Sep.")
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%b. %d, %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(cleaned, fmt).date().isoformat()
        except ValueError:
            continue
    return ""


def _source_ref(path: Path, fragment: str) -> str:
    return f"sec_cache/ANF/{path.name}#{fragment}"


def _publication_date(path: Path, accession_digits: str) -> str:
    index_path = path.with_name(f"index_{accession_digits}.json")
    if not index_path.exists():
        raise DebtResolutionError(
            "anf_debt_index_missing",
            "Local SEC index metadata is required for an exact publication date.",
            source_path=str(path),
            index_path=str(index_path),
        )
    payload = json.loads(index_path.read_text(encoding="utf-8"))
    items = payload.get("directory", {}).get("item", []) if isinstance(payload, Mapping) else []
    dates = {
        str(row.get("last-modified") or "")[:10]
        for row in items
        if isinstance(row, Mapping) and re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", str(row.get("last-modified") or "")[:10])
    }
    if len(dates) != 1:
        raise DebtResolutionError(
            "anf_debt_publication_date_conflict",
            "Local SEC index metadata must resolve one exact publication date.",
            source_path=str(path),
            publication_dates=sorted(dates),
        )
    return next(iter(dates))


def _soup(raw_html: bytes) -> BeautifulSoup:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", XMLParsedAsHTMLWarning)
        return BeautifulSoup(raw_html, "lxml")


def _ix_text(soup: BeautifulSoup, concept: str) -> str:
    values = {
        _clean_text(tag.get_text(" "))
        for tag in soup.find_all(attrs={"name": re.compile(rf"^{re.escape(concept)}$", re.I)})
        if _clean_text(tag.get_text(" "))
    }
    if len(values) != 1:
        raise DebtResolutionError(
            "anf_debt_document_identity_conflict",
            "Inline-XBRL document identity must resolve uniquely.",
            concept=concept,
            values=sorted(values),
        )
    return next(iter(values))


def _context_dates(
    soup: BeautifulSoup,
    *,
    source_path: Path | str = "",
) -> dict[str, tuple[str, bool]]:
    contexts: dict[str, tuple[str, bool]] = {}
    seen_context_ids: set[str] = set()
    for context in soup.find_all(lambda tag: bool(tag.name) and tag.name.casefold().endswith(":context")):
        context_id = str(context.get("id") or "")
        if context_id in seen_context_ids:
            raise DebtResolutionError(
                "anf_debt_xbrl_context_identity_conflict",
                "Inline-XBRL context IDs must be unique source identities.",
                source_path=str(source_path),
                context_id=context_id,
            )
        if context_id:
            seen_context_ids.add(context_id)
        instant = context.find(lambda tag: bool(tag.name) and tag.name.casefold().endswith(":instant"))
        instant_date = _clean_text(instant.get_text(" ")) if instant else ""
        has_dimensions = context.find(
            lambda tag: bool(tag.name) and tag.name.casefold().endswith(":explicitmember")
        ) is not None
        if context_id and instant_date:
            contexts[context_id] = (instant_date, has_dimensions)
    return contexts


def _parse_source_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and not pd.isna(value):
        return abs(float(value))
    text = _clean_text(value)
    if not text or text.casefold() in {"nan", "\u2014", "-", "$"}:
        return None
    negative = text.startswith("(") and text.endswith(")")
    text = text.strip("()$ ").replace(",", "")
    if not re.fullmatch(r"[-+]?[0-9]+(?:\.[0-9]+)?", text):
        return None
    number = float(text)
    return abs(number) if negative else number


def _find_capacity_table(tables: Sequence[Any], *, source_path: Path) -> tuple[int, Any, str]:
    matches: list[tuple[int, Any, str]] = []
    for table_index, table in enumerate(tables):
        cells = [_clean_text(value) for value in table.to_numpy().ravel()]
        if "(in thousands)" not in cells:
            continue
        row_labels = {_canonical_table_label(table.iat[row, 0]) for row in range(table.shape[0])}
        if all(required in row_labels for required in _CAPACITY_LABELS):
            dates = sorted({_parse_date_text(cell) for cell in cells if _parse_date_text(cell)})
            if len(dates) != 1:
                raise DebtResolutionError(
                    "anf_debt_capacity_period_conflict",
                    "Borrowing-capacity table must expose one exact as-of date.",
                    source_path=str(source_path),
                    table_index=table_index,
                    dates=dates,
                )
            matches.append((table_index, table, dates[0]))
    if len(matches) != 1:
        raise DebtResolutionError(
            "anf_debt_capacity_table_conflict",
            "Expected exactly one explicit-thousands borrowing-capacity table.",
            source_path=str(source_path),
            matching_tables=[row[0] for row in matches],
        )
    return matches[0]


def _table_row(table: Any, label: str) -> int:
    rows = [
        row
        for row in range(table.shape[0])
        if _canonical_table_label(table.iat[row, 0]) == label
    ]
    if len(rows) != 1:
        raise DebtResolutionError(
            "anf_debt_source_row_conflict",
            "Borrowing-capacity source label must resolve one row.",
            source_label=label,
            matching_rows=rows,
        )
    return rows[0]


def _table_value(table: Any, *, row: int, as_of_date: str) -> float:
    date_columns = {
        column
        for table_row in range(table.shape[0])
        for column in range(table.shape[1])
        if _parse_date_text(_clean_text(table.iat[table_row, column])) == as_of_date
    }
    values = {
        parsed
        for column in date_columns
        if (parsed := _parse_source_number(table.iat[row, column])) is not None
    }
    if len(values) != 1:
        raise DebtResolutionError(
            "anf_debt_source_value_conflict",
            "Exact source row and as-of columns must resolve one numeric value.",
            source_row=row,
            as_of_date=as_of_date,
            values=sorted(values),
        )
    return next(iter(values))


def _borrowings_note(soup: BeautifulSoup, *, source_path: Path | str) -> tuple[int, str]:
    facts = soup.find_all(attrs={"name": _DEBT_DISCLOSURE_CONCEPT_RE})
    if len(facts) != 1:
        raise DebtResolutionError(
            "anf_debt_note_identity_conflict",
            "The Inline-XBRL debt disclosure concept must resolve exactly one BORROWINGS note.",
            source_path=str(source_path),
            concept="us-gaap:DebtDisclosureTextBlock",
            matching_fact_count=len(facts),
            matching_fact_ids=sorted(str(fact.get("id") or "") for fact in facts),
        )
    (fact,) = facts
    note_numbers = {
        int(match.group("number"))
        for ancestor in fact.parents
        if (match := _BORROWINGS_HEADING_RE.fullmatch(_clean_text(ancestor.get_text(" ")))) is not None
    }
    if len(note_numbers) != 1:
        raise DebtResolutionError(
            "anf_debt_note_heading_conflict",
            "The authoritative debt disclosure fact must have one exact numbered BORROWINGS heading.",
            source_path=str(source_path),
            concept="us-gaap:DebtDisclosureTextBlock",
            fact_id=str(fact.get("id") or ""),
            matching_note_numbers=sorted(note_numbers),
        )
    (note_number,) = note_numbers
    try:
        reconstructed = reconstruct_inline_xbrl_fact_text(soup, fact)
    except InlineXbrlContinuationError as exc:
        raise DebtResolutionError(
            "anf_debt_note_continuation_conflict",
            "The authoritative debt disclosure continuation chain is invalid.",
            source_path=str(source_path),
            fact_id=str(fact.get("id") or ""),
            continuation_error_code=exc.code,
            **dict(exc.context),
        ) from exc
    note = _clean_text(f"{note_number}. {reconstructed.text}")
    if "ABL Facility" not in note:
        raise DebtResolutionError(
            "anf_debt_note_scope_conflict",
            "The bounded BORROWINGS note contains no ABL Facility section.",
            source_path=str(source_path),
            fact_id=str(fact.get("id") or ""),
            note_number=note_number,
        )
    return note_number, note


def _abl_subsection(note: str, *, source_path: Path | str) -> str:
    headings = list(_ABL_SECTION_HEADING_RE.finditer(note))
    if len(headings) != 1:
        raise DebtResolutionError(
            "anf_debt_abl_section_conflict",
            "The BORROWINGS note must resolve exactly one canonical ABL Facility section heading.",
            source_path=str(source_path),
            matching_heading_count=len(headings),
            matching_heading_offsets=[match.start() for match in headings],
        )
    (heading,) = headings
    start = heading.start()
    boundaries = [match for match in _ABL_SECTION_END_RE.finditer(note) if match.start() > start]
    if len(boundaries) != 1:
        raise DebtResolutionError(
            "anf_debt_abl_section_boundary_conflict",
            "The canonical ABL Facility section must resolve one exact ending boundary.",
            source_path=str(source_path),
            section_start=start,
            matching_boundary_count=len(boundaries),
            matching_boundary_offsets=[match.start() for match in boundaries],
        )
    (boundary,) = boundaries
    return note[start : boundary.start()]


def _commitment_millions(abl_section: str) -> float:
    patterns = (
        r"provides? for (?:a )?\$\s*([0-9,.]+)\s+million senior secured asset-based revolving credit facility",
        r"revolving credit facility of up to \$\s*([0-9,.]+)\s+million",
        r"increase the aggregate commitments thereunder from \$\s*[0-9,.]+\s+million to \$\s*([0-9,.]+)\s+million",
        r"increase the aggregate commitments thereunder to \$\s*([0-9,.]+)\s+million",
    )
    for pattern in patterns:
        values = {
            float(value.replace(",", ""))
            for value in re.findall(pattern, abl_section, re.I)
        }
        if len(values) == 1:
            return next(iter(values))
        if len(values) > 1:
            raise DebtResolutionError(
                "anf_debt_commitment_conflict",
                "Current ABL commitment text resolves conflicting values.",
                values=sorted(values),
            )
    raise DebtResolutionError(
        "anf_debt_commitment_missing",
        "Bounded ABL subsection contains no exact current commitment statement.",
    )


def _facility_expiry(abl_section: str) -> str:
    patterns = (
        r"scheduled to expire on ([A-Z][a-z]+\s+[0-9]{1,2},\s+[0-9]{4})",
        r"(?:which )?matures on ([A-Z][a-z]+\s+[0-9]{1,2},\s+[0-9]{4})",
        r"extend the maturity date from [A-Z][a-z]+\s+[0-9]{1,2},\s+[0-9]{4} to ([A-Z][a-z]+\s+[0-9]{1,2},\s+[0-9]{4})",
    )
    for pattern in patterns:
        values = {_parse_date_text(value) for value in re.findall(pattern, abl_section, re.I)}
        values.discard("")
        if len(values) == 1:
            return next(iter(values))
        if len(values) > 1:
            raise DebtResolutionError(
                "anf_debt_facility_expiry_conflict",
                "ABL subsection resolves conflicting facility-expiry dates.",
                values=sorted(values),
            )
    return ""


def _exact_sentence(full_text: str, pattern: str) -> str:
    matches = {
        _clean_text(match.group(0))
        for match in re.finditer(pattern, full_text, re.I)
    }
    if len(matches) > 1:
        raise DebtResolutionError(
            "anf_debt_source_sentence_conflict",
            "Bounded source sentence pattern resolves conflicting text.",
            pattern=pattern,
            matches=sorted(matches),
        )
    return next(iter(matches), "")


def _date_specific_no_draw_sentence(full_text: str, *, as_of_date: str) -> str:
    # Windows strftime has no portable no-leading-zero flag.
    parsed = date.fromisoformat(as_of_date)
    display_date = f"{parsed.strftime('%B')} {parsed.day}, {parsed.year}"
    candidates = {
        _clean_text(match.group(0))
        for match in re.finditer(
            r"The Company did not have any borrowings outstanding under the ABL Facility[^.]*\.",
            full_text,
            re.I,
        )
        if display_date in match.group(0)
    }
    if len(candidates) > 1:
        raise DebtResolutionError(
            "anf_debt_drawn_status_conflict",
            "More than one exact date-specific no-borrowings sentence was found.",
            as_of_date=as_of_date,
            candidates=sorted(candidates),
        )
    return next(iter(candidates), "")


def _ix_amount_fact(
    soup: BeautifulSoup,
    *,
    concepts: Sequence[str],
    as_of_date: str,
    source_path: Path,
) -> tuple[float, str, str]:
    contexts = _context_dates(soup, source_path=source_path)
    concept_tokens = {concept.casefold() for concept in concepts}
    matching_facts = [
        fact
        for fact in soup.find_all(lambda tag: bool(tag.name) and tag.name.casefold().endswith(":nonfraction"))
        if str(fact.get("name") or "").casefold() in concept_tokens
    ]
    if not matching_facts:
        raise DebtSourceFactMissing(
            "anf_debt_xbrl_fact_missing",
            "The optional companion concept is absent from the filing.",
            concepts=list(concepts),
            as_of_date=as_of_date,
            source_path=str(source_path),
        )
    exact_facts = []
    available_contexts: set[tuple[str, str, bool] | tuple[str, str, None]] = set()
    for fact in matching_facts:
        context_ref = str(fact.get("contextref") or fact.get("contextRef") or "")
        context = contexts.get(context_ref)
        available_contexts.add(
            (context_ref, context[0], context[1])
            if context is not None
            else (context_ref, "", None)
        )
        if context == (as_of_date, False):
            exact_facts.append(fact)
    if not exact_facts:
        raise DebtResolutionError(
            "anf_debt_xbrl_context_conflict",
            "Companion concepts exist, but none has the exact required undimensioned instant context.",
            concepts=list(concepts),
            as_of_date=as_of_date,
            source_path=str(source_path),
            available_contexts=sorted(available_contexts, key=lambda row: (row[0], row[1], str(row[2]))),
        )

    candidates: dict[tuple[str, str, str, str], tuple[float, str]] = {}
    candidate_values: set[float] = set()
    for fact in exact_facts:
        concept = str(fact.get("name") or "")
        context_ref = str(fact.get("contextref") or fact.get("contextRef") or "")
        unit_ref = str(fact.get("unitref") or fact.get("unitRef") or "").casefold()
        scale = str(fact.get("scale") or "")
        if unit_ref != "usd":
            raise DebtResolutionError(
                "anf_debt_xbrl_unit_conflict",
                "Debt companion XBRL facts require an explicit USD unit.",
                concept=concept,
                context_ref=context_ref,
                unit_ref=unit_ref,
                as_of_date=as_of_date,
                source_path=str(source_path),
            )
        if scale != "3":
            raise DebtResolutionError(
                "anf_debt_xbrl_scale_conflict",
                "Debt companion XBRL facts require explicit USD thousands semantics.",
                concept=concept,
                context_ref=context_ref,
                unit_ref=unit_ref,
                scale=scale,
                as_of_date=as_of_date,
                source_path=str(source_path),
            )
        source_value = _parse_source_number(fact.get_text(" "))
        if source_value is None:
            raise DebtResolutionError(
                "anf_debt_xbrl_numeric_malformed",
                "An exact-context debt companion fact contains no valid numeric source value.",
                concept=concept,
                context_ref=context_ref,
                raw_text=_clean_text(fact.get_text(" ")),
                as_of_date=as_of_date,
                source_path=str(source_path),
            )
        identity = (concept.casefold(), context_ref, unit_ref, scale)
        prior = candidates.get(identity)
        if prior is not None and prior[0] != source_value:
            raise DebtResolutionError(
                "anf_debt_xbrl_fact_conflict",
                "One canonical XBRL fact identity resolves conflicting numeric values.",
                concept=concept,
                context_ref=context_ref,
                values=sorted({prior[0], source_value}),
                as_of_date=as_of_date,
                source_path=str(source_path),
            )
        candidates[identity] = (source_value, concept)
        candidate_values.add(source_value)
    if len(candidate_values) != 1:
        raise DebtResolutionError(
            "anf_debt_xbrl_fact_conflict",
            "Exact XBRL concepts and as-of context must resolve one source amount.",
            concepts=list(concepts),
            as_of_date=as_of_date,
            values=sorted(candidate_values),
            source_path=str(source_path),
        )
    if len(candidates) != 1:
        raise DebtResolutionError(
            "anf_debt_xbrl_duplicate_identity",
            "More than one canonical concept/context identity represents the same companion fact.",
            concepts=list(concepts),
            as_of_date=as_of_date,
            source_path=str(source_path),
            candidate_identities=sorted(
                f"{concept}[context={context_ref},unit={unit_ref},scale={scale}]"
                for concept, context_ref, unit_ref, scale in candidates
            ),
        )
    ((_, context_ref, _, _), (source_value, concept)) = next(iter(candidates.items()))
    row_ref = f"{concept}[context={context_ref}]"
    return source_value, "thousands", row_ref


def _amount_fact(
    *,
    source_value: float | None,
    source_scale: str,
    as_of_date: str,
    source_ref: str,
    source_row_ref: str,
    evidence_refs: Sequence[str],
    status: str = "populated",
    reason: str = "",
    evidence_classification: str = "source_backed_fact",
    derivation: str = "",
    normalized_value: float | None = None,
) -> dict[str, Any]:
    if status == "populated":
        value = (
            normalized_value
            if normalized_value is not None
            else normalize_debt_currency_to_millions(
                source_value,
                source_unit="USD",
                source_scale=source_scale,
            )
        )
    else:
        value = None
        source_value = None
    return {
        "value": value,
        "status": status,
        "currency": "USD",
        "unit": "$m",
        "source_value": source_value,
        "source_unit": "USD",
        "source_scale": source_scale,
        "as_of_date": as_of_date,
        "source_ref": source_ref,
        "source_row_ref": source_row_ref,
        "evidence_refs": list(dict.fromkeys(str(ref) for ref in evidence_refs if str(ref))),
        "evidence_classification": evidence_classification,
        "derivation": derivation,
        "reason": reason,
        "core": False,
    }


def parse_anf_debt_filing(path: Path) -> dict[str, Any]:
    """Parse one local 10-Q/10-K into source-native debt rows."""

    match = _DOCUMENT_RE.fullmatch(path.name)
    if match is None:
        raise DebtResolutionError(
            "anf_debt_source_filename_invalid",
            "ANF debt source must use the cached SEC document filename contract.",
            source_path=str(path),
        )
    raw_html = path.read_bytes()
    source_sha = _sha256_bytes(raw_html)
    soup = _soup(raw_html)
    document_type = _ix_text(soup, "dei:DocumentType")
    if document_type not in {"10-Q", "10-K"}:
        raise DebtResolutionError(
            "anf_debt_source_form_rejected",
            "Debt adapter accepts only local 10-Q and 10-K sources.",
            source_path=str(path),
            document_type=document_type,
        )
    as_of_date = _parse_date_text(_ix_text(soup, "dei:DocumentPeriodEndDate"))
    if not as_of_date or as_of_date.replace("-", "") != match.group("period"):
        raise DebtResolutionError(
            "anf_debt_source_period_conflict",
            "Filename and Inline-XBRL document period must agree exactly.",
            source_path=str(path),
            filename_period=match.group("period"),
            document_period=as_of_date,
        )
    accession_digits = match.group("accession")
    accession = _canonical_accession(accession_digits)
    publication_date = _publication_date(path, accession_digits)
    full_text = _clean_text(soup.get_text(" "))
    note_number, note = _borrowings_note(soup, source_path=path)
    abl_section = _abl_subsection(note, source_path=path)
    commitment = _commitment_millions(abl_section)
    expiry = _facility_expiry(abl_section)

    tables = pd.read_html(path)
    table_index, capacity_table, table_period = _find_capacity_table(tables, source_path=path)
    if table_period != as_of_date:
        raise DebtResolutionError(
            "anf_debt_capacity_period_conflict",
            "Borrowing-capacity table period must equal the filing period.",
            source_path=str(path),
            table_period=table_period,
            filing_period=as_of_date,
        )
    source_rows = {label: _table_row(capacity_table, label) for label in _CAPACITY_LABELS}
    source_values = {
        label: _table_value(capacity_table, row=row, as_of_date=as_of_date)
        for label, row in source_rows.items()
    }
    evidence_key = f"anf_abl_{as_of_date.replace('-', '_')}_{source_sha[:12]}"
    capacity_ref = _source_ref(path, f"borrowings_capacity_table[{table_index}]")
    note_ref = _source_ref(path, f"note[{note_number}]-borrowings")
    evidence_refs = (capacity_ref, note_ref)

    no_draw_sentence = _date_specific_no_draw_sentence(full_text, as_of_date=as_of_date)
    if no_draw_sentence:
        draw_row_ref = f"exact_sentence[{hashlib.sha256(no_draw_sentence.encode('utf-8')).hexdigest()[:16]}]"
        drawn_balance = _amount_fact(
            source_value=0.0,
            source_scale="millions",
            as_of_date=as_of_date,
            source_ref=_source_ref(path, draw_row_ref),
            source_row_ref=draw_row_ref,
            evidence_refs=(*evidence_refs, _source_ref(path, draw_row_ref)),
        )
        drawn_status = "reported_zero"
    else:
        drawn_balance = _amount_fact(
            source_value=None,
            source_scale="not_applicable",
            as_of_date=as_of_date,
            source_ref=note_ref,
            source_row_ref=f"note[{note_number}]:drawn-balance-not-reported",
            evidence_refs=evidence_refs,
            status="missing_source",
            reason="The filing does not report an exact date-specific drawn ABL balance; zero was not assumed.",
            evidence_classification="unavailable",
        )
        drawn_status = "not_reported"

    companion_specs = {
        "cash": ("us-gaap:CashAndCashEquivalentsAtCarryingValue",),
        "restricted_cash": (
            "us-gaap:RestrictedCashAndCashEquivalentsNoncurrent",
            "us-gaap:RestrictedCashEquivalentsNoncurrent",
        ),
        "lease_current": ("us-gaap:OperatingLeaseLiabilityCurrent",),
        "lease_noncurrent": ("us-gaap:OperatingLeaseLiabilityNoncurrent",),
    }
    companion: dict[str, dict[str, Any]] = {}
    for name, concepts in companion_specs.items():
        try:
            source_value, source_scale, row_ref = _ix_amount_fact(
                soup,
                concepts=concepts,
                as_of_date=as_of_date,
                source_path=path,
            )
            ref = _source_ref(path, f"xbrl:{row_ref}")
            companion[name] = _amount_fact(
                source_value=source_value,
                source_scale=source_scale,
                as_of_date=as_of_date,
                source_ref=ref,
                source_row_ref=row_ref,
                evidence_refs=(*evidence_refs, ref),
            )
        except DebtSourceFactMissing:
            companion[name] = _amount_fact(
                source_value=None,
                source_scale="not_applicable",
                as_of_date=as_of_date,
                source_ref=note_ref,
                source_row_ref=f"xbrl:{name}:not-resolved",
                evidence_refs=evidence_refs,
                status="missing_source",
                reason=f"No unique exact-date {name.replace('_', ' ')} XBRL fact was resolved.",
                evidence_classification="unavailable",
            )

    row_ref = f"table[{table_index}]:rows[{min(source_rows.values())}:{max(source_rows.values())}]"
    table_amount = lambda label: _amount_fact(
        source_value=source_values[label],
        source_scale="thousands",
        as_of_date=as_of_date,
        source_ref=capacity_ref,
        source_row_ref=f"table[{table_index}]:row[{source_rows[label]}]",
        evidence_refs=evidence_refs,
    )
    cash_value = companion["cash"].get("value")
    net_value = normalize_debt_currency_to_millions(
        source_values["Borrowing capacity available"],
        source_unit="USD",
        source_scale="thousands",
    )
    if isinstance(cash_value, (int, float)) and not isinstance(cash_value, bool):
        liquidity_ref = f"{companion['cash']['source_ref']} + {capacity_ref}"
        same_date_liquidity = _amount_fact(
            source_value=None,
            source_scale="not_applicable",
            as_of_date=as_of_date,
            source_ref=liquidity_ref,
            source_row_ref="calculation[cash_and_equivalents+net_availability]",
            evidence_refs=(*evidence_refs, *companion["cash"].get("evidence_refs", [])),
            evidence_classification="source_backed_calculation",
            derivation="cash_and_equivalents + net_availability; restricted cash excluded",
            normalized_value=round(float(cash_value) + net_value, 6),
        )
    else:
        same_date_liquidity = _amount_fact(
            source_value=None,
            source_scale="not_applicable",
            as_of_date=as_of_date,
            source_ref=capacity_ref,
            source_row_ref="calculation[cash_and_equivalents+net_availability]",
            evidence_refs=evidence_refs,
            status="missing_source",
            reason="Same-date cash is unavailable, so liquidity was not derived.",
            evidence_classification="unavailable",
        )

    facility = {
        "facility_id": "anf_abl_facility",
        "facility_name": "ABL Facility",
        "facility_type": "asset_based_revolver",
        "borrower": "Abercrombie & Fitch Management Co.",
        "currency": "USD",
        "as_of_date": as_of_date,
        "publication_date": publication_date,
        "period_role": "historical",
        "source_status": "accepted",
        "source_table_scope": "borrowings_capacity_table",
        "aggregation_role": "liquidity_capacity",
        "commitment": _amount_fact(
            source_value=commitment,
            source_scale="millions",
            as_of_date=as_of_date,
            source_ref=note_ref,
            source_row_ref=f"note[{note_number}]:abl-current-commitment",
            evidence_refs=evidence_refs,
        ),
        "loan_cap": table_amount("Loan cap"),
        "drawn_balance": drawn_balance,
        "drawn_status": drawn_status,
        "letters_of_credit": table_amount("Less: Outstanding stand-by letters of credit"),
        "gross_capacity": table_amount("Borrowing capacity"),
        "minimum_excess_availability": table_amount("Less: Minimum excess availability"),
        "net_availability": table_amount("Borrowing capacity available"),
        "cash_and_equivalents": companion["cash"],
        "restricted_cash": companion["restricted_cash"],
        "same_date_liquidity": same_date_liquidity,
        "facility_expiry_date": expiry,
        "evidence_key": evidence_key,
        "evidence_refs": list(evidence_refs),
        "source_refs": [capacity_ref, note_ref],
        "source_row_ref": row_ref,
        "source_document_sha256": source_sha,
        "reason": "Source-native ABL capacity record; availability and drawn balance retain distinct evidence states.",
    }

    lease_total_ref = f"{companion['lease_current']['source_ref']} + {companion['lease_noncurrent']['source_ref']}"
    lease_current_value = companion["lease_current"].get("value")
    lease_noncurrent_value = companion["lease_noncurrent"].get("value")
    if all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in (lease_current_value, lease_noncurrent_value)):
        lease_total = _amount_fact(
            source_value=None,
            source_scale="not_applicable",
            as_of_date=as_of_date,
            source_ref=lease_total_ref,
            source_row_ref="calculation[lease_current+lease_noncurrent]",
            evidence_refs=(
                *companion["lease_current"].get("evidence_refs", []),
                *companion["lease_noncurrent"].get("evidence_refs", []),
            ),
            evidence_classification="source_backed_calculation",
            derivation="operating_lease_liability_current + operating_lease_liability_noncurrent",
            normalized_value=round(float(lease_current_value) + float(lease_noncurrent_value), 6),
        )
    else:
        lease_total = _amount_fact(
            source_value=None,
            source_scale="not_applicable",
            as_of_date=as_of_date,
            source_ref=note_ref,
            source_row_ref="calculation[lease_current+lease_noncurrent]",
            evidence_refs=evidence_refs,
            status="missing_source",
            reason="Operating-lease current and noncurrent balances did not both resolve.",
            evidence_classification="unavailable",
        )
    instrument = {
        "instrument_id": "operating_lease_liabilities",
        "instrument_name": "Operating lease liabilities",
        "instrument_type": "operating_lease_liability",
        "issuer": "Abercrombie & Fitch Co.",
        "currency": "USD",
        "as_of_date": as_of_date,
        "publication_date": publication_date,
        "period_role": "historical",
        "source_status": "accepted",
        "source_table_scope": "consolidated_balance_sheet_xbrl",
        "aggregation_role": "excluded_from_core_debt",
        "balance": lease_total,
        "current_balance": companion["lease_current"],
        "noncurrent_balance": companion["lease_noncurrent"],
        "rate_type": "not_applicable",
        "reference_rate": "",
        "spread_bps": None,
        "effective_rate": None,
        "maturity_date": "",
        "secured_status": "not_applicable",
        "seniority": "not_applicable",
        "evidence_key": f"anf_operating_leases_{as_of_date.replace('-', '_')}_{source_sha[:12]}",
        "evidence_refs": list(
            dict.fromkeys(
                [
                    *companion["lease_current"].get("evidence_refs", []),
                    *companion["lease_noncurrent"].get("evidence_refs", []),
                ]
            )
        ),
        "source_refs": list(dict.fromkeys([companion["lease_current"]["source_ref"], companion["lease_noncurrent"]["source_ref"]])),
        "source_row_ref": "xbrl:OperatingLeaseLiabilityCurrent+OperatingLeaseLiabilityNoncurrent",
        "source_document_sha256": source_sha,
        "reason": "Operating lease liabilities are retained separately and excluded from core debt.",
    }

    credit_notes: list[dict[str, Any]] = []
    note_specs = (
        (
            "facility_draw_status",
            "anf_abl_facility",
            r"The Company did not have any borrowings outstanding under the ABL Facility[^.]*\.",
        ),
        (
            "covenant_compliance",
            "anf_abl_facility",
            r"The Company (?:was|remained) in compliance with all (?:debt )?covenants under these agreements as of [^.]+\.",
        ),
        (
            "debt_redemption",
            "senior_secured_notes",
            r"(?:A&F|The Company) redeemed all of its outstanding 8\.75\s*% Senior Secured Notes[^.]*\.",
        ),
    )
    for note_type, subject_id, pattern in note_specs:
        text = _exact_sentence(note, pattern)
        if not text:
            continue
        text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        note_row_ref = f"note[{note_number}]:sentence[{text_hash[:16]}]"
        note_source_ref = _source_ref(path, note_row_ref)
        credit_notes.append(
            {
                "note_id": f"{subject_id}_{note_type}",
                "subject_id": subject_id,
                "note_type": note_type,
                "text": text,
                "as_of_date": as_of_date,
                "publication_date": publication_date,
                "period_role": "historical",
                "source_status": "accepted",
                "source_table_scope": "borrowings_note",
                "aggregation_role": "non_additive_context",
                "evidence_key": f"anf_{note_type}_{as_of_date.replace('-', '_')}_{text_hash[:12]}",
                "evidence_refs": [note_source_ref],
                "source_refs": [note_source_ref],
                "source_row_ref": note_row_ref,
                "source_document_sha256": source_sha,
                "reason": "Exact bounded BORROWINGS-note sentence; no narrative scoring applied.",
            }
        )

    return {
        "facility": facility,
        "instrument": instrument,
        "credit_notes": credit_notes,
        "source_document": {
            "path": f"sec_cache/ANF/{path.name}",
            "sha256": source_sha,
            "size": len(raw_html),
            "accession": accession,
            "document_type": document_type,
            "as_of_date": as_of_date,
            "publication_date": publication_date,
            "capacity_table_index": table_index,
            "borrowings_note_number": note_number,
        },
    }


def _assign_period_roles(rows: list[dict[str, Any]], *, subject_fields: Sequence[str]) -> None:
    by_subject: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for row in rows:
        key = tuple(str(row.get(field) or "") for field in subject_fields)
        by_subject.setdefault(key, []).append(row)
    for subject_rows in by_subject.values():
        latest = max(str(row.get("as_of_date") or "") for row in subject_rows)
        for row in subject_rows:
            row["period_role"] = "current" if str(row.get("as_of_date") or "") == latest else "historical"


def build_anf_debt_collections(sec_cache_root: Path) -> ANFDebtSourceExtraction:
    """Build canonical source-native ANF debt collections without network access."""

    if not sec_cache_root.is_dir():
        raise DebtResolutionError(
            "anf_debt_cache_missing",
            "Local ANF SEC cache is required; network fallback is prohibited.",
            sec_cache_root=str(sec_cache_root),
        )
    candidates: list[Path] = []
    for path in sorted(sec_cache_root.glob("doc_0001018840*_anf-*.htm"), key=lambda item: item.name.casefold()):
        match = _DOCUMENT_RE.fullmatch(path.name)
        if (
            match is None
            or match.group("period") < ANF_DEBT_HISTORY_START.replace("-", "")
            or match.group("period") > ANF_DEBT_HISTORY_END.replace("-", "")
        ):
            continue
        raw = path.read_bytes()
        if b"Borrowing capacity available" in raw and b"Loan cap" in raw:
            candidates.append(path)
    parsed = [parse_anf_debt_filing(path) for path in candidates]
    facilities = [dict(row["facility"]) for row in parsed]
    instruments = [dict(row["instrument"]) for row in parsed]
    credit_notes = [dict(note) for row in parsed for note in row["credit_notes"]]
    source_documents = [dict(row["source_document"]) for row in parsed]
    actual_periods = tuple(sorted(str(row.get("as_of_date") or "") for row in facilities))
    if actual_periods != ANF_EXPECTED_ABL_PERIODS:
        raise DebtResolutionError(
            "anf_debt_history_incomplete",
            "The bounded ANF adapter must reconcile every expected recent ABL period.",
            history_start=ANF_DEBT_HISTORY_START,
            expected_period_count=ANF_EXPECTED_ABL_PERIOD_COUNT,
            expected_periods=ANF_EXPECTED_ABL_PERIODS,
            actual_period_count=len(facilities),
            actual_periods=actual_periods,
        )
    _assign_period_roles(facilities, subject_fields=("facility_id",))
    _assign_period_roles(instruments, subject_fields=("instrument_id",))
    _assign_period_roles(credit_notes, subject_fields=("subject_id", "note_type"))
    raw_section = {
        "facilities": facilities,
        "instruments": instruments,
        "maturities": [],
        "credit_notes": credit_notes,
    }
    resolved = resolve_debt_collections(raw_section)
    canonical = dispositions_to_package_section(resolved)
    return ANFDebtSourceExtraction(
        facilities=tuple(canonical["facilities"]),
        instruments=tuple(canonical["instruments"]),
        maturities=tuple(canonical["maturities"]),
        credit_notes=tuple(canonical["credit_notes"]),
        source_documents=tuple(sorted(source_documents, key=lambda row: (str(row["as_of_date"]), str(row["accession"])))),
    )


def _anf_debt_cache_with_evidence(cache_root: Path) -> Path | None:
    """Locate one local ANF cache without guessing from an unrelated ticker."""

    root = Path(cache_root).expanduser()
    candidates = (root, root / "ANF", root / "sec_cache" / "ANF")
    matches: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate.resolve()).casefold() if candidate.exists() else str(candidate.absolute()).casefold()
        if key in seen:
            continue
        seen.add(key)
        if candidate.is_dir() and any(candidate.glob("doc_0001018840*_anf-*.htm")):
            matches.append(candidate)
    if not matches:
        return None
    if len(matches) != 1:
        raise DebtResolutionError(
            "anf_debt_cache_ambiguous",
            "ANF debt evidence must resolve to one local SEC cache.",
            candidates=[str(path) for path in matches],
        )
    return matches[0]


def _legacy_source_type(source_row_ref: str) -> str:
    locator = str(source_row_ref or "").strip().lower()
    if locator.startswith("table["):
        return "table"
    if locator.startswith("xbrl:") or locator.startswith("us-gaap:"):
        return "xbrl"
    return "text"


def _legacy_amount_metadata(prefix: str, amount: Mapping[str, Any]) -> dict[str, Any]:
    return {
        f"{prefix}_source_type": _legacy_source_type(str(amount.get("source_row_ref") or "")),
        f"{prefix}_source_ref": str(amount.get("source_ref") or ""),
        f"{prefix}_source_row_ref": str(amount.get("source_row_ref") or ""),
        f"{prefix}_evidence_classification": str(amount.get("evidence_classification") or ""),
        f"{prefix}_evidence_refs": json.dumps(
            list(amount.get("evidence_refs") or ()),
            ensure_ascii=False,
            separators=(",", ":"),
        ),
    }


def _legacy_usd_value(amount: Mapping[str, Any], *, amount_key: str) -> float | None:
    if amount.get("status") != "populated":
        return None
    value = amount.get("value")
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or str(amount.get("currency") or "") != "USD"
        or str(amount.get("unit") or "") != "$m"
    ):
        raise DebtResolutionError(
            "anf_legacy_debt_amount_unit_invalid",
            "ANF legacy revolver projection requires an explicit USD-millions canonical amount.",
            amount_key=amount_key,
            value=value,
            currency=amount.get("currency"),
            unit=amount.get("unit"),
        )
    return float(value) * 1_000_000.0


def anf_debt_extraction_to_legacy_revolver_history(
    extraction: ANFDebtSourceExtraction,
) -> pd.DataFrame:
    """Bridge canonical ANF facilities into the legacy writer with full lineage."""

    documents = {str(row["as_of_date"]): dict(row) for row in extraction.source_documents}
    rows: list[dict[str, Any]] = []
    for facility in sorted(extraction.facilities, key=lambda row: str(row["as_of_date"])):
        as_of_date = str(facility["as_of_date"])
        document = documents.get(as_of_date)
        if document is None:
            raise DebtResolutionError(
                "anf_debt_document_lineage_missing",
                "Every ANF facility row requires its exact SEC document identity.",
                as_of_date=as_of_date,
            )
        amount_specs = {
            "commitment": ("revolver_commitment", "commitment"),
            "facility": ("revolver_facility_size", "loan_cap"),
            "drawn": ("revolver_drawn", "drawn_balance"),
            "lc": ("revolver_letters_of_credit", "letters_of_credit"),
            "gross_capacity": ("revolver_gross_capacity", "gross_capacity"),
            "minimum_excess_availability": (
                "revolver_minimum_excess_availability",
                "minimum_excess_availability",
            ),
            "availability": ("revolver_availability", "net_availability"),
            "cash": ("same_date_cash", "cash_and_equivalents"),
            "liquidity": ("same_date_liquidity", "same_date_liquidity"),
        }
        row: dict[str, Any] = {
            "quarter": pd.Timestamp(as_of_date),
            "facility_id": str(facility["facility_id"]),
            "facility_name": str(facility["facility_name"]),
            "facility_expiry_date": str(facility.get("facility_expiry_date") or ""),
            "publication_date": str(facility["publication_date"]),
            "debt_evidence_adapter_id": ANF_DEBT_EVIDENCE_ADAPTER_ID,
            "source_document_id": source_document_identity(
                company_id="ANF",
                publisher_id="sec",
                document_type="sec-filing",
                publication_date=str(document["publication_date"]),
                document_key=f"sec-accession-{document['accession']}",
            ),
            "source_document_accession": str(document["accession"]),
            "source_document_form": str(document["document_type"]),
            "source_document_sha256": str(facility["source_document_sha256"]),
            "source_ref": str(facility["source_ref"]),
            "source_row_ref": str(facility["source_row_ref"]),
            "evidence_key": str(facility["evidence_key"]),
            "business_key": str(facility["business_key"]),
            "drawn_status": str(facility["drawn_status"]),
            "source_type": "table",
            "note": "Source-native reviewed ANF ABL facility evidence.",
        }
        for prefix, (legacy_column, amount_key) in amount_specs.items():
            amount = dict(facility[amount_key])
            row[legacy_column] = _legacy_usd_value(amount, amount_key=amount_key)
            row.update(_legacy_amount_metadata(prefix, amount))
        commitment = row["revolver_commitment"]
        drawn = row["revolver_drawn"]
        row["revolver_utilization"] = (
            float(drawn) / float(commitment)
            if commitment not in (None, 0) and drawn is not None
            else None
        )
        row["utilization_evidence_classification"] = "source_backed_calculation"
        row["utilization_derivation"] = "drawn_balance / commitment"
        row["source_snippet"] = (
            f"{facility['facility_name']} at {as_of_date}; exact reviewed SEC evidence; "
            f"accession {document['accession']}."
        )
        rows.append(row)
    frame = pd.DataFrame(rows).sort_values("quarter", kind="stable").reset_index(drop=True)
    resolved_facilities = resolve_debt_facilities(extraction.facilities)
    current_facilities = tuple(
        facility for facility in resolved_facilities if facility.period_role == "current"
    )
    if len(current_facilities) != 1:
        raise DebtResolutionError(
            "anf_current_debt_facility_ambiguous",
            "ANF legacy projection requires exactly one validated current facility.",
            current_business_keys=[facility.business_key for facility in current_facilities],
        )
    economic_validation = validate_resolved_debt_facility_for_profile(current_facilities[0])
    if not economic_validation.passed:
        raise DebtResolutionError(
            "anf_current_debt_facility_validation_failed",
            "ANF current facility did not pass the canonical debt-profile economic contract.",
            validation=economic_validation.to_dict(),
        )
    frame.attrs[DEBT_PROFILE_ECONOMIC_VALIDATION_ATTR] = economic_validation
    return frame


def build_anf_legacy_revolver_history(cache_root: Path) -> pd.DataFrame:
    """Build legacy writer rows only when the complete local ANF evidence set exists."""

    sec_cache_root = _anf_debt_cache_with_evidence(Path(cache_root))
    if sec_cache_root is None:
        return pd.DataFrame()
    return anf_debt_extraction_to_legacy_revolver_history(
        build_anf_debt_collections(sec_cache_root)
    )
