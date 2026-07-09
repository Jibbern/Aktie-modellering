from __future__ import annotations

import json
from pathlib import Path

from openpyxl import load_workbook


ROOT = Path(__file__).resolve().parents[1]


def _data_root() -> Path:
    for parent in (ROOT, *ROOT.parents):
        candidate = parent / "StockModelData"
        if candidate.exists():
            return candidate
    return ROOT.parent / "StockModelData"


DATA_ROOT = _data_root()
INVENTORY_JSON = ROOT / "docs" / "standard_template_sheet_inventory.json"
LIFECYCLE_JSON = ROOT / "docs" / "support_sheet_lifecycle_contract.json"
SHELL = ROOT / "templates" / "standard_stock_model_template.xlsx"
SOURCE_WORKBOOKS = {
    "PBI": DATA_ROOT / "outputs" / "Excel stock models" / "PBI_model.xlsx",
    "GPRE": DATA_ROOT / "outputs" / "Excel stock models" / "GPRE_model.xlsx",
    "ANF": DATA_ROOT / "outputs" / "Excel stock models" / "ANF_model.xlsx",
}
ALLOWED_CLASSIFICATIONS = {
    "standard_visible_shell_sheet",
    "required_support_shell_sheet",
    "runtime_generated_support_sheet",
    "runtime_generated_audit_sheet",
    "optional_sector_pack_sheet",
    "ticker_specific_sheet",
    "deprecated_legacy_sheet",
    "exclude_from_standard_shell",
}
REQUIRED_FIELDS = {
    "sheet_name",
    "classification",
    "present_in_standard_shell",
    "present_in_PBI",
    "present_in_GPRE",
    "present_in_ANF",
    "standard_shell_state",
    "PBI_state",
    "GPRE_state",
    "ANF_state",
    "reason",
    "visible_formula_or_binding_dependency",
    "runtime_must_create_or_fill",
}
LIFECYCLE_FIELDS = {
    "sheet_name",
    "owner",
    "lifecycle",
    "neutral_shell_required",
    "headers_required",
    "allowed_writable_zones",
    "source_of_data",
    "created_when",
    "visibility",
    "validation_rules",
}


def _source_sheet_names() -> set[str]:
    names: set[str] = set()
    for path in SOURCE_WORKBOOKS.values():
        wb = load_workbook(path, read_only=True, data_only=False)
        try:
            names.update(wb.sheetnames)
        finally:
            wb.close()
    return names


def test_sheet_inventory_covers_every_pbi_gpre_anf_sheet() -> None:
    payload = json.loads(INVENTORY_JSON.read_text(encoding="utf-8"))
    rows = {row["sheet_name"]: row for row in payload["sheets"]}

    missing = _source_sheet_names() - set(rows)
    assert missing == set()

    for row in rows.values():
        assert REQUIRED_FIELDS <= set(row)
        assert row["classification"] in ALLOWED_CLASSIFICATIONS
        assert isinstance(row["reason"], str) and row["reason"].strip()


def test_support_lifecycle_contract_covers_non_visible_inventory_outputs() -> None:
    inventory = json.loads(INVENTORY_JSON.read_text(encoding="utf-8"))
    lifecycle = json.loads(LIFECYCLE_JSON.read_text(encoding="utf-8"))
    lifecycle_rows = {row["sheet_name"]: row for row in lifecycle["support_sheets"]}

    lifecycle_required_classes = {
        "required_support_shell_sheet",
        "runtime_generated_support_sheet",
        "runtime_generated_audit_sheet",
        "optional_sector_pack_sheet",
    }
    for row in inventory["sheets"]:
        if row["classification"] not in lifecycle_required_classes:
            continue
        assert row["sheet_name"] in lifecycle_rows

    for row in lifecycle_rows.values():
        assert LIFECYCLE_FIELDS <= set(row)
        assert row["owner"] in {"frozen_shell", "value_only_runtime", "legacy_writer", "optional_sector_pack"}
        assert row["lifecycle"] in {
            "static_template",
            "runtime_output",
            "audit_output",
            "source_cache_projection",
            "optional_sector_output",
        }


def test_required_support_shell_sheets_are_present_and_runtime_sheets_are_absent() -> None:
    inventory = json.loads(INVENTORY_JSON.read_text(encoding="utf-8"))
    lifecycle = json.loads(LIFECYCLE_JSON.read_text(encoding="utf-8"))
    required_support = {
        row["sheet_name"]
        for row in inventory["sheets"]
        if row["classification"] == "required_support_shell_sheet"
    }
    runtime_outputs = {
        row["sheet_name"]
        for row in lifecycle["support_sheets"]
        if row["owner"] == "value_only_runtime" and not row["neutral_shell_required"]
    }

    wb = load_workbook(SHELL, read_only=True, data_only=False)
    try:
        shell_sheets = set(wb.sheetnames)
    finally:
        wb.close()

    assert required_support <= shell_sheets
    assert runtime_outputs.isdisjoint(shell_sheets - required_support)


def test_gpre_sector_pack_sheets_are_optional_not_standard() -> None:
    payload = json.loads(INVENTORY_JSON.read_text(encoding="utf-8"))
    rows = {row["sheet_name"]: row for row in payload["sheets"]}

    for sheet_name in ("Economics_Overlay", "Basis_Proxy_Sandbox", "economics_market_raw"):
        assert rows[sheet_name]["classification"] == "optional_sector_pack_sheet"
        assert rows[sheet_name]["present_in_standard_shell"] is False
