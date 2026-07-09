from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ARCHITECTURE_PATH = ROOT / "docs" / "workbook_block_architecture.json"
COVERAGE_PATH = ROOT / "docs" / "workbook_block_coverage_matrix.json"

REQUIRED_COVERAGE_KEYS = {
    "block_id",
    "sheet",
    "ANF",
    "PBI",
    "GPRE",
    "range_similarity",
    "style_similarity",
    "merge_similarity",
    "freeze_pane_similarity",
    "row_height_similarity",
    "populated_field_families",
    "support_sheet_dependencies",
    "ticker_specific_differences",
    "include_in_standard_template",
    "exclusion_reason",
}


def _architecture() -> dict:
    return json.loads(ARCHITECTURE_PATH.read_text(encoding="utf-8"))


def _coverage() -> dict:
    return json.loads(COVERAGE_PATH.read_text(encoding="utf-8"))


def test_coverage_matrix_has_one_row_per_architecture_block() -> None:
    block_ids = {block["block_id"] for block in _architecture()["blocks"]}
    coverage = _coverage()
    coverage_rows = coverage["coverage_rows"]
    coverage_ids = [row["block_id"] for row in coverage_rows]

    assert {"version", "source_workbooks", "coverage_rows"} <= set(coverage)
    assert len(coverage_ids) == len(set(coverage_ids))
    assert set(coverage_ids) == block_ids


def test_coverage_rows_include_cross_ticker_status_and_similarity_fields() -> None:
    for row in _coverage()["coverage_rows"]:
        assert REQUIRED_COVERAGE_KEYS <= set(row)
        for ticker in ("ANF", "PBI", "GPRE"):
            status = row[ticker]
            assert {"block_exists", "resolved_sheet", "range", "nonempty_cells", "formula_cells", "merge_count", "freeze_panes"} <= set(status)
            assert isinstance(status["block_exists"], bool)
        assert row["range_similarity"] in {"same", "similar", "different", "missing"}
        assert row["style_similarity"] in {"same", "similar", "different", "not_measured"}
        assert row["merge_similarity"] in {"same", "similar", "different", "missing"}
        assert row["freeze_pane_similarity"] in {"same", "different", "missing"}
        assert row["row_height_similarity"] in {"same", "similar", "different", "not_measured"}
        assert isinstance(row["include_in_standard_template"], bool)


def test_sector_specific_overlays_are_excluded_from_standard_template() -> None:
    coverage = _coverage()
    excluded = coverage.get("excluded_sector_overlays", [])

    assert {entry["sheet"] for entry in excluded} >= {"Economics_Overlay", "Basis_Proxy_Sandbox"}
    assert all(entry["include_in_standard_template"] is False for entry in excluded)
    assert all(entry["standardization_status"] == "sector_specific" for entry in excluded)

    for row in coverage["coverage_rows"]:
        if row["sheet"] in {"Economics_Overlay", "Basis_Proxy_Sandbox"}:
            assert row["include_in_standard_template"] is False
            assert row["exclusion_reason"]


def test_standard_template_blocks_are_not_ticker_specific() -> None:
    architecture_blocks = {block["block_id"]: block for block in _architecture()["blocks"]}

    offenders = [
        row["block_id"]
        for row in _coverage()["coverage_rows"]
        if row["include_in_standard_template"]
        and architecture_blocks[row["block_id"]]["standardization_status"] == "ticker_specific"
    ]

    assert offenders == []
