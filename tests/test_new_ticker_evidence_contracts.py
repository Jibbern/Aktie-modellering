from __future__ import annotations

from dataclasses import fields
from pathlib import Path

from pbi_xbrl.new_ticker_evidence import (
    EvidenceCandidate,
    EvidenceSet,
    NormalizedFieldEvidenceRef,
    SourceDocument,
    SourceSnippet,
    TableCellEvidence,
    XbrlFactEvidence,
)


ROOT = Path(__file__).resolve().parents[1]


def test_source_native_evidence_contracts_have_required_lineage_surfaces() -> None:
    expected = {
        SourceDocument: {"document_id", "ticker", "source_type", "source_url", "content_hash"},
        SourceSnippet: {"snippet_id", "document_id", "locator", "text"},
        TableCellEvidence: {"evidence_id", "document_id", "table_locator", "row_label", "column_label", "raw_value"},
        XbrlFactEvidence: {"evidence_id", "document_id", "concept", "value", "unit"},
        EvidenceCandidate: {"candidate_id", "field_family", "evidence_role", "value", "source_refs"},
        EvidenceSet: {"evidence_set_id", "field_path", "candidates", "resolution_policy"},
        NormalizedFieldEvidenceRef: {"field_path", "evidence_set_id", "selected_candidate_ids", "resolution_rule_id"},
    }

    for contract, required_fields in expected.items():
        assert required_fields <= {item.name for item in fields(contract)}
        assert getattr(contract, "__dataclass_params__").frozen is True


def test_evidence_contract_module_has_no_parser_or_workbook_behavior() -> None:
    source = (ROOT / "pbi_xbrl" / "new_ticker_evidence.py").read_text(encoding="utf-8").lower()

    assert "openpyxl" not in source
    assert "load_workbook" not in source
    assert "requests" not in source
    assert "def parse" not in source
