from __future__ import annotations

import copy
import json
from collections import Counter
from pathlib import Path

import pytest

import pbi_xbrl.longitudinal_memory.source_adapter.pdf as pdf_module
import pbi_xbrl.longitudinal_memory.source_adapter.spreadsheet as spreadsheet_module
from pbi_xbrl.longitudinal_memory.source_adapter.builder import _extract
from pbi_xbrl.longitudinal_memory.source_adapter.discovery import discover_sources, load_source_set
from pbi_xbrl.longitudinal_memory.source_adapter.html import (
    derive_fiscal_label_semantics,
    extract_html_evidence,
    replay_html_dateline,
)
from pbi_xbrl.longitudinal_memory.source_adapter.pdf import (
    extract_pdf_evidence,
    replay_pdf_dateline,
)
from pbi_xbrl.longitudinal_memory.source_adapter.spreadsheet import extract_spreadsheet_evidence
from pbi_xbrl.longitudinal_memory.source_adapter.text import extract_text_evidence
from pbi_xbrl.longitudinal_memory.source_adapter.types import LocatorError


REPO = Path(__file__).resolve().parents[1]
FIXTURE = REPO / "tests" / "fixtures" / "longitudinal_memory" / "anf_source_set.v1.json"
SOURCE_ROOT = Path(r"C:\Users\Jibbe\Aktier\StockModelData")


def _context():
    source_set = load_source_set(FIXTURE)
    discovered = discover_sources(source_set, SOURCE_ROOT)
    documents = {row.spec.document_key: row for row in discovered}
    assertions = {
        row["assertion_key"]: row
        for row in json.loads(FIXTURE.read_text(encoding="utf-8"))["required_assertions"]
    }
    return source_set, documents, assertions


@pytest.mark.parametrize(
    ("source_text", "period_type", "fiscal_year", "fiscal_quarter"),
    [
        ("Fiscal 2025", "unspecified_fiscal_context", 2025, None),
        ("For fiscal 2026", "unspecified_fiscal_context", 2026, None),
        ("Full Year 2025", "fiscal_year", 2025, None),
        ("Full Year Fiscal 2025", "fiscal_year", 2025, None),
        ("First Quarter 2025", "fiscal_quarter", 2025, 1),
        ("Second Quarter 2025", "fiscal_quarter", 2025, 2),
        ("Third Quarter 2025", "fiscal_quarter", 2025, 3),
        ("Fourth Quarter 2025", "fiscal_quarter", 2025, 4),
        ("Q1 2025", "fiscal_quarter", 2025, 1),
        ("Q2 2025", "fiscal_quarter", 2025, 2),
        ("Q3 2025", "fiscal_quarter", 2025, 3),
        ("Q4 2025", "fiscal_quarter", 2025, 4),
    ],
)
def test_generic_fiscal_label_grammar_is_source_text_derived(
    source_text: str,
    period_type: str,
    fiscal_year: int,
    fiscal_quarter: int | None,
) -> None:
    claim = derive_fiscal_label_semantics(source_text)
    assert (
        claim["period_type"],
        claim["fiscal_year"],
        claim["fiscal_quarter"],
    ) == (period_type, fiscal_year, fiscal_quarter)


def test_all_real_extractors_replay_every_declared_locator() -> None:
    source_set, _documents, _assertions = _context()
    discovered = discover_sources(source_set, SOURCE_ROOT)
    extracted = _extract(source_set, discovered)
    assert len(extracted) == 51
    assert Counter(row.locator_kind for row in extracted) == {
        "table-row": 28,
        "cell": 13,
        "page": 3,
        "line": 4,
        "paragraph": 3,
    }


def test_all_embedded_datelines_replay_from_verified_snapshots() -> None:
    _source_set, documents, _assertions = _context()
    observed = {}
    for key, document in documents.items():
        if document.spec.embedded_publication_date is None:
            continue
        if document.spec.source_family == "sec-exhibit":
            observed[key] = replay_html_dateline(document)
        else:
            observed[key] = replay_pdf_dateline(document)
    assert observed == {
        key: document.spec.embedded_publication_date
        for key, document in documents.items()
        if document.spec.embedded_publication_date is not None
    }


@pytest.mark.parametrize(
    "field",
    ["table_fingerprints", "row_header_fingerprint", "column_header_fingerprint", "cell_span_fingerprint"],
)
def test_changed_html_table_header_fingerprints_fail(field: str) -> None:
    _source_set, documents, assertions = _context()
    assertion = copy.deepcopy(assertions["comp-fy2025-q4-release-apac"])
    assertion["locator"][field] = ["not-present"] if field == "table_fingerprints" else "not-present"
    with pytest.raises(LocatorError):
        extract_html_evidence(documents[assertion["document_key"]], [assertion])


def test_html_table_or_merged_cell_ambiguity_fails() -> None:
    _source_set, documents, assertions = _context()
    assertion = copy.deepcopy(assertions["comp-fy2025-q4-release-apac"])
    assertion["locator"]["table_fingerprints"] = []
    with pytest.raises(LocatorError, match="matched"):
        extract_html_evidence(documents[assertion["document_key"]], [assertion])


def test_html_exact_position_and_excerpt_digest_are_replayed() -> None:
    _source_set, documents, assertions = _context()
    assertion = assertions["guidance-fy2025-revenue-may"]
    result = extract_html_evidence(documents[assertion["document_key"]], [assertion])[0]
    assert "replaces all previous" in result.excerpt
    assert result.value_text == "Growth In The Range of 3% to 6%"
    assert result.comparison_text == "Growth In The Range of 3% to 5%"


@pytest.mark.parametrize("field", ["node_path", "extraction_method_id"])
def test_changed_html_node_path_or_method_fails(field: str) -> None:
    _source_set, documents, assertions = _context()
    assertion = copy.deepcopy(assertions["guidance-fy2025-revenue-mar"])
    assertion["locator"][field] = "changed"
    with pytest.raises(LocatorError):
        extract_html_evidence(documents[assertion["document_key"]], [assertion])


@pytest.mark.parametrize("mutation", ["line", "digest", "speaker"])
def test_changed_transcript_line_digest_or_speaker_fails(mutation: str) -> None:
    _source_set, documents, assertions = _context()
    assertion = copy.deepcopy(assertions["guidance-fy2026-revenue-transcript"])
    if mutation == "line":
        assertion["locator"]["start_line"] = 51
        assertion["locator"]["end_line"] = 51
    elif mutation == "digest":
        assertion["locator"]["line_digest"] = "0" * 64
    else:
        assertion["locator"]["speaker_fingerprint"] = "Unknown Speaker"
    with pytest.raises(LocatorError):
        extract_text_evidence(documents[assertion["document_key"]], [assertion])


@pytest.mark.parametrize("field", ["turn_diagnostics", "extraction_method_id"])
def test_changed_transcript_turn_or_method_fails(field: str) -> None:
    _source_set, documents, assertions = _context()
    assertion = copy.deepcopy(assertions["guidance-fy2026-revenue-transcript"])
    assertion["locator"][field] = "changed"
    with pytest.raises(LocatorError):
        extract_text_evidence(documents[assertion["document_key"]], [assertion])


def test_required_transcript_lines_are_exact() -> None:
    _source_set, documents, assertions = _context()
    expected = {
        "event-merchandising-erp": 30,
        "management-q4-margin-bridge": 40,
        "guidance-fy2026-revenue-transcript": 52,
        "guidance-fy2026-margin-transcript": 54,
    }
    for key, line in expected.items():
        assertion = assertions[key]
        result = extract_text_evidence(documents[assertion["document_key"]], [assertion])[0]
        assert result.diagnostics["start_line"] == line
        assert result.diagnostics["end_line"] == line


@pytest.mark.parametrize("mutation", ["page", "region", "digest"])
def test_changed_pdf_page_text_or_digest_fails(mutation: str) -> None:
    _source_set, documents, assertions = _context()
    assertion = copy.deepcopy(assertions["guidance-fy2025-revenue-jan"])
    if mutation == "page":
        assertion["locator"]["page"] = 2
    elif mutation == "region":
        assertion["locator"]["region_locator"] = "not present"
    else:
        assertion["locator"]["excerpt_sha256"] = "0" * 64
    with pytest.raises(LocatorError):
        extract_pdf_evidence(documents[assertion["document_key"]], [assertion])


def test_pdf_empty_text_layer_is_a_blocker(monkeypatch: pytest.MonkeyPatch) -> None:
    _source_set, documents, assertions = _context()
    assertion = assertions["guidance-fy2025-revenue-jan"]

    class Page:
        def extract_text(self):
            return ""

        def extract_tables(self):
            return []

    class Pdf:
        pages = [Page()]

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(pdf_module.pdfplumber, "open", lambda _path: Pdf())
    with pytest.raises(LocatorError, match="empty text"):
        extract_pdf_evidence(documents[assertion["document_key"]], [assertion])


def test_changed_pdf_extraction_method_fails() -> None:
    _source_set, documents, assertions = _context()
    assertion = copy.deepcopy(assertions["guidance-fy2025-revenue-jan"])
    assertion["locator"]["extraction_method_id"] = "changed"
    with pytest.raises(LocatorError):
        extract_pdf_evidence(documents[assertion["document_key"]], [assertion])


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("sheet_name", "Missing Sheet"),
        ("a1_range", "N11"),
        ("row_header_fingerprint", "not present"),
        ("formula", "=1"),
        ("cached_value_state", "present"),
        ("number_format", "General"),
    ],
)
def test_changed_xlsx_sheet_cell_header_formula_cache_or_format_fails(
    field: str, value: object
) -> None:
    _source_set, documents, assertions = _context()
    assertion = copy.deepcopy(assertions["comp-fy2025-q4-xlsx-apac"])
    assertion["locator"][field] = value
    with pytest.raises(LocatorError):
        extract_spreadsheet_evidence(documents[assertion["document_key"]], [assertion])


def test_xlsx_preserves_zero_negative_and_formula_state() -> None:
    _source_set, documents, assertions = _context()
    keys = ["comp-fy2025-q4-xlsx-apac", "store-closures-xlsx"]
    values = {}
    for key in keys:
        assertion = assertions[key]
        result = extract_spreadsheet_evidence(
            documents[assertion["document_key"]], [assertion]
        )[0]
        values[key] = result.value_text
        assert result.diagnostics["cell_type"] == "numeric"
        assert result.diagnostics["formula"] is None
        assert result.diagnostics["cached_value_state"] == "not-applicable"
    assert values == {
        "comp-fy2025-q4-xlsx-apac": "0",
        "store-closures-xlsx": "-22",
    }


def test_xlsx_formula_and_cached_views_use_identical_verified_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _source_set, documents, assertions = _context()
    document = documents["anf-q4-2025-history"]
    assertion = assertions["comp-fy2025-q4-xlsx-apac"]
    real_load_workbook = spreadsheet_module.load_workbook
    snapshots: list[bytes] = []

    def recording_load_workbook(stream, *args, **kwargs):
        snapshots.append(stream.getvalue())
        return real_load_workbook(stream, *args, **kwargs)

    monkeypatch.setattr(spreadsheet_module, "load_workbook", recording_load_workbook)
    extract_spreadsheet_evidence(document, [assertion])
    assert snapshots == [document.verified_bytes, document.verified_bytes]


def test_changed_xlsx_extraction_method_fails() -> None:
    _source_set, documents, assertions = _context()
    assertion = copy.deepcopy(assertions["comp-fy2025-q4-xlsx-apac"])
    assertion["locator"]["extraction_method_id"] = "changed"
    with pytest.raises(LocatorError):
        extract_spreadsheet_evidence(documents[assertion["document_key"]], [assertion])
