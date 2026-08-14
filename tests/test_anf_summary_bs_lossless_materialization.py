from __future__ import annotations

from collections import Counter
from datetime import date
from decimal import Decimal
import re
from pathlib import Path, PurePosixPath
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import pytest

from pbi_xbrl.json_schema_validation import load_json_strict
from pbi_xbrl.longitudinal_memory.summary_bs_workbook_materialization import (
    ARTIFACT_TOOL_BRIDGE_ROLE,
    CANONICAL_OOXML_HASH_CONTRACT,
    MATERIALIZER_CONTRACT,
    SummaryBSWorkbookMaterializationError,
    WorkbookCellMutation,
    build_cell_mutations,
    load_materialization_plan,
    materialize_ooxml_cell_mutations,
    materialize_summary_bs_preview,
    sha256_file,
    validate_materialization_plan,
)
from pbi_xbrl.longitudinal_memory.summary_bs_workbook_projection import (
    DILUTED_SHARES_ROW_LABEL,
    INVENTORY_SALES_SPREAD_ROW_LABEL,
    build_summary_bs_projection_plan,
)
from pbi_xbrl.longitudinal_memory.ticker_profiles.anf_summary_bs_foundation import (
    build_anf_summary_bs_products,
)


DATA_ROOT = Path(r"C:\Users\Jibbe\Aktier\StockModelData")
PROTECTED_WORKBOOK = DATA_ROOT / "outputs" / "Excel stock models" / "ANF_model.xlsx"
PROTECTED_SHA256 = "ef73bdef6b6efa1bc358622b58bfc320b609c128e76c602de9d4e5f726ab98cd"
OLD_AUDIT_ROOT = DATA_ROOT / "audit" / "summary_bs_source_native_projection_2026-08-14"
SOURCE_AUDIT_ROOT = DATA_ROOT / "audit" / "anf_summary_bs_segment_exhaustive_historical_lineage_audit_2026-08-10"
SURFACE_MAP_PATH = OLD_AUDIT_ROOT / "WORKBOOK_SURFACE_MAP.json"
EXPECTED_PLAN_DIGEST = "481fd188c95090b96f810e192c6927a5f5f910672d076a9acc2ebf2591f4a215"
MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
DOCUMENT_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PACKAGE_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
CELL_RE = re.compile(rb"<c\b[^>]*/>|<c\b[^>]*>.*?</c>", re.DOTALL)
XF_RE = re.compile(rb"<xf\b[^>]*/>|<xf\b[^>]*>.*?</xf>", re.DOTALL)
BUILTIN_NUMBER_FORMATS = {
    0: "General",
    1: "0",
    2: "0.00",
    3: "#,##0",
    4: "#,##0.00",
    9: "0%",
    10: "0.00%",
    14: "mm-dd-yy",
    49: "@",
}


def test_bridge_roles_keep_artifact_tool_non_authoritative() -> None:
    assert MATERIALIZER_CONTRACT == "summary-bs-lossless-workbook-materializer@2"
    assert CANONICAL_OOXML_HASH_CONTRACT == "ordered-uncompressed-ooxml-members-sha256@1"
    assert ARTIFACT_TOOL_BRIDGE_ROLE == "READ/INSPECTION/RENDER ONLY"


@pytest.fixture(scope="session")
def plan() -> dict:
    bundle = build_anf_summary_bs_products(DATA_ROOT, SOURCE_AUDIT_ROOT)
    artifacts = bundle["artifacts"]
    plan = build_summary_bs_projection_plan(
        summary_product=artifacts["summary_product.json"],
        summary_shadow=artifacts["summary_shadow.json"],
        bs_product=artifacts["bs_segment_product.json"],
        bs_shadow=artifacts["bs_segment_shadow.json"],
        surface_map=load_json_strict(SURFACE_MAP_PATH),
        protected_workbook_sha256=PROTECTED_SHA256,
    )
    validate_materialization_plan(plan, expected_plan_digest=EXPECTED_PLAN_DIGEST)
    return plan


@pytest.fixture(scope="session")
def mutations(plan: dict) -> tuple[WorkbookCellMutation, ...]:
    return build_cell_mutations(plan)


@pytest.fixture(scope="session")
def full_previews(tmp_path_factory: pytest.TempPathFactory, plan: dict) -> tuple[Path, Path, dict, dict]:
    root = tmp_path_factory.mktemp("summary_bs_lossless_full")
    first = root / "preview_a.xlsx"
    second = root / "preview_b.xlsx"
    first_receipt = materialize_summary_bs_preview(
        base_workbook=PROTECTED_WORKBOOK,
        output_workbook=first,
        plan=plan,
        expected_plan_digest=EXPECTED_PLAN_DIGEST,
    )
    second_receipt = materialize_summary_bs_preview(
        base_workbook=PROTECTED_WORKBOOK,
        output_workbook=second,
        plan=plan,
        expected_plan_digest=EXPECTED_PLAN_DIGEST,
    )
    return first, second, first_receipt, second_receipt


def _package(path: Path) -> dict[str, bytes]:
    with ZipFile(path) as archive:
        return {name: archive.read(name) for name in archive.namelist()}


def _resolve_part(base_part: str, target: str) -> str:
    if target.startswith("/"):
        return target.lstrip("/")
    parts: list[str] = []
    for part in (PurePosixPath(base_part).parent / target).parts:
        if part == "..":
            parts.pop()
        elif part not in {"", "."}:
            parts.append(part)
    return "/".join(parts)


def _sheet_parts(package: dict[str, bytes]) -> dict[str, str]:
    workbook = ET.fromstring(package["xl/workbook.xml"])
    relationships = ET.fromstring(package["xl/_rels/workbook.xml.rels"])
    targets = {
        node.get("Id"): node.get("Target")
        for node in relationships.findall(f"{{{PACKAGE_REL_NS}}}Relationship")
    }
    result: dict[str, str] = {}
    sheets = workbook.find(f"{{{MAIN_NS}}}sheets")
    assert sheets is not None
    for sheet in sheets:
        relationship_id = sheet.get(f"{{{DOCUMENT_REL_NS}}}id")
        result[str(sheet.get("name"))] = _resolve_part(
            "xl/workbook.xml", str(targets[relationship_id])
        )
    return result


def _cell_map(data: bytes) -> dict[str, bytes]:
    result: dict[str, bytes] = {}
    for match in CELL_RE.finditer(data):
        element = match.group(0)
        coordinate = re.search(
            rb"\s+r=['\"]([^'\"]+)['\"]", element[: element.find(b">") + 1]
        )
        assert coordinate is not None
        result[coordinate.group(1).decode("ascii")] = element
    return result


def _cell_style_id(element: bytes) -> int:
    value = re.search(rb"\s+s=['\"]([0-9]+)['\"]", element[: element.find(b">") + 1])
    return 0 if value is None else int(value.group(1))


def _cell_value(element: bytes) -> tuple[str | None, str | None, str | None]:
    root = ET.fromstring(element)
    local = lambda value: value.rsplit("}", 1)[-1]
    children = {local(child.tag): child for child in list(root)}
    value = children.get("v")
    inline = children.get("is")
    formula = children.get("f")
    return (
        None if value is None else value.text,
        None
        if inline is None
        else "".join(node.text or "" for node in inline.iter() if local(node.tag) == "t"),
        None if formula is None else formula.text or "",
    )


def _mask_cells(data: bytes, coordinates: set[str]) -> bytes:
    replacements: list[tuple[int, int, bytes]] = []
    seen: set[str] = set()
    for match in CELL_RE.finditer(data):
        element = match.group(0)
        coordinate = re.search(
            rb"\s+r=['\"]([^'\"]+)['\"]", element[: element.find(b">") + 1]
        )
        assert coordinate is not None
        ref = coordinate.group(1).decode("ascii")
        if ref in coordinates:
            replacements.append((match.start(), match.end(), f"<AUTHORIZED:{ref}>".encode("ascii")))
            seen.add(ref)
    assert seen == coordinates
    output = data
    for start, end, replacement in sorted(replacements, reverse=True):
        output = output[:start] + replacement + output[end:]
    return output


def _style_catalog(styles: bytes) -> tuple[list[ET.Element], dict[int, str]]:
    root = ET.fromstring(styles)
    custom = dict(BUILTIN_NUMBER_FORMATS)
    formats = root.find(f"{{{MAIN_NS}}}numFmts")
    if formats is not None:
        custom.update(
            {
                int(node.get("numFmtId", "0")): str(node.get("formatCode"))
                for node in list(formats)
            }
        )
    xfs = root.find(f"{{{MAIN_NS}}}cellXfs")
    assert xfs is not None
    return list(xfs), custom


def _format_for_cell(element: bytes, styles: bytes) -> str:
    xfs, formats = _style_catalog(styles)
    xf = xfs[_cell_style_id(element)]
    return formats.get(int(xf.get("numFmtId", "0")), f"builtin:{xf.get('numFmtId')}")


def _changed_parts(before: dict[str, bytes], after: dict[str, bytes]) -> list[str]:
    assert list(before) == list(after)
    return sorted(name for name in before if before[name] != after[name])


def test_binding_plan_is_immutable_and_write_types_are_closed(plan: dict, mutations) -> None:
    validate_materialization_plan(plan, expected_plan_digest=EXPECTED_PLAN_DIGEST)
    assert plan["plan_digest"] == EXPECTED_PLAN_DIGEST
    assert len(plan["bindings"]) == 452
    assert Counter(binding["write_mode"] for binding in plan["bindings"]) == {
        "SET_VALUE": 388,
        "CLEAR_CONTENTS": 2,
        "NO_WRITE": 62,
    }
    assert Counter(
        "clear" if mutation.mode == "CLEAR_CONTENTS" else mutation.value_kind
        for mutation in mutations
    ) == {"number": 374, "text": 16, "date": 1, "clear": 2}
    mutated = dict(plan)
    mutated["bindings"] = list(plan["bindings"][:-1])
    with pytest.raises(SummaryBSWorkbookMaterializationError, match="digest|452"):
        validate_materialization_plan(mutated, expected_plan_digest=EXPECTED_PLAN_DIGEST)


def test_noop_materialization_is_raw_and_part_identical(tmp_path: Path) -> None:
    output = tmp_path / "noop.xlsx"
    result = materialize_ooxml_cell_mutations(
        base_workbook=PROTECTED_WORKBOOK,
        output_workbook=output,
        mutations=(),
        expected_base_sha256=PROTECTED_SHA256,
    )
    assert output.read_bytes() == PROTECTED_WORKBOOK.read_bytes()
    assert result.changed_ooxml_parts == ()
    assert result.unchanged_ooxml_part_count == len(_package(PROTECTED_WORKBOOK)) == 144


def test_single_numeric_mutation_preserves_style_and_every_other_byte(
    tmp_path: Path, mutations
) -> None:
    mutation = next(
        item for item in mutations if item.target_sheet == "SUMMARY" and item.target_cell == "B32"
    )
    output = tmp_path / "single_numeric.xlsx"
    result = materialize_ooxml_cell_mutations(
        base_workbook=PROTECTED_WORKBOOK,
        output_workbook=output,
        mutations=(mutation,),
        expected_base_sha256=PROTECTED_SHA256,
    )
    before = _package(PROTECTED_WORKBOOK)
    after = _package(output)
    summary_part = _sheet_parts(before)["SUMMARY"]
    assert list(result.changed_ooxml_parts) == [summary_part]
    assert all(before[name] == after[name] for name in before if name != summary_part)
    assert _mask_cells(before[summary_part], {"B32"}) == _mask_cells(
        after[summary_part], {"B32"}
    )
    old = _cell_map(before[summary_part])["B32"]
    new = _cell_map(after[summary_part])["B32"]
    assert _cell_style_id(old) == _cell_style_id(new)
    assert _cell_value(new) == ("1.47", None, None)


def test_single_clear_mutation_preserves_style_and_every_other_byte(
    tmp_path: Path, mutations
) -> None:
    mutation = next(
        item for item in mutations if item.target_sheet == "SUMMARY" and item.target_cell == "B42"
    )
    output = tmp_path / "single_clear.xlsx"
    result = materialize_ooxml_cell_mutations(
        base_workbook=PROTECTED_WORKBOOK,
        output_workbook=output,
        mutations=(mutation,),
        expected_base_sha256=PROTECTED_SHA256,
    )
    before = _package(PROTECTED_WORKBOOK)
    after = _package(output)
    summary_part = _sheet_parts(before)["SUMMARY"]
    assert list(result.changed_ooxml_parts) == [summary_part]
    assert all(before[name] == after[name] for name in before if name != summary_part)
    assert _mask_cells(before[summary_part], {"B42"}) == _mask_cells(
        after[summary_part], {"B42"}
    )
    old = _cell_map(before[summary_part])["B42"]
    new = _cell_map(after[summary_part])["B42"]
    assert _cell_style_id(old) == _cell_style_id(new)
    assert _cell_value(new) == (None, None, None)


def test_full_materialization_reads_back_all_452_bindings(
    plan: dict, full_previews
) -> None:
    first, _, receipt, _ = full_previews
    base = _package(PROTECTED_WORKBOOK)
    preview = _package(first)
    parts = _sheet_parts(base)
    base_cells = {name: _cell_map(base[part]) for name, part in parts.items()}
    preview_cells = {name: _cell_map(preview[part]) for name, part in parts.items()}
    passed = 0
    for binding in plan["bindings"]:
        sheet = binding["target_sheet"]
        cell = binding["target_cell"]
        before = base_cells[sheet][cell]
        after = preview_cells[sheet][cell]
        mode = binding["write_mode"]
        if mode == "NO_WRITE":
            assert before == after
        elif mode == "CLEAR_CONTENTS":
            assert _cell_value(after) == (None, None, None)
        else:
            numeric, text, formula = _cell_value(after)
            assert formula is None
            expected = binding["write_value"]
            if expected["kind"] == "number":
                assert Decimal(str(numeric)) == Decimal(expected["canonical_decimal"])
            elif expected["kind"] == "text":
                assert text == expected["text"]
            else:
                expected_serial = str(
                    (date.fromisoformat(expected["iso_date"]) - date(1899, 12, 30)).days
                )
                assert numeric == expected_serial
            assert _format_for_cell(after, preview["xl/styles.xml"]) == binding[
                "projection_number_format_code"
            ]
        passed += 1
    assert passed == 452
    assert receipt["binding_count"] == 452
    assert receipt["mutation_count"] == 393
    assert receipt["no_write_count"] == 62
    assert receipt["presentation_mutation_count"] == 3
    assert receipt["canonical_ooxml_hash_contract"] == CANONICAL_OOXML_HASH_CONTRACT
    assert receipt["contract"] == MATERIALIZER_CONTRACT


def test_full_materialization_changes_only_authorized_cells_and_style_extensions(
    mutations, full_previews
) -> None:
    first = full_previews[0]
    before = _package(PROTECTED_WORKBOOK)
    after = _package(first)
    parts = _sheet_parts(before)
    assert _changed_parts(before, after) == sorted(
        [parts["SUMMARY"], parts["BS_Segments"], "xl/styles.xml"]
    )
    by_sheet = {
        sheet: {item.target_cell for item in mutations if item.target_sheet == sheet}
        for sheet in ("SUMMARY", "BS_Segments")
    }
    for sheet, targets in by_sheet.items():
        part = parts[sheet]
        assert _mask_cells(before[part], targets) == _mask_cells(after[part], targets)
    base_styles = before["xl/styles.xml"]
    preview_styles = after["xl/styles.xml"]
    base_xfs = XF_RE.findall(
        re.search(rb"<cellXfs\b[^>]*>(.*?)</cellXfs>", base_styles, re.DOTALL).group(1)
    )
    preview_xfs = XF_RE.findall(
        re.search(rb"<cellXfs\b[^>]*>(.*?)</cellXfs>", preview_styles, re.DOTALL).group(1)
    )
    assert preview_xfs[: len(base_xfs)] == base_xfs
    assert len(preview_xfs) - len(base_xfs) == full_previews[2]["style_variant_count"]
    assert full_previews[2]["style_variant_count"] >= 3


def test_bounded_presentation_and_percentage_point_readback(plan: dict, full_previews) -> None:
    first = full_previews[0]
    package = _package(first)
    cells = _cell_map(package[_sheet_parts(package)["BS_Segments"]])
    assert _cell_value(cells["A3"])[1] == (
        "QA: A=L+E PASS | Debt N/A | Cash PASS | Quarterly Seg PASS | Annual Seg PASS"
    )
    assert _cell_value(cells["A49"])[1] == DILUTED_SHARES_ROW_LABEL
    assert _cell_value(cells["A53"])[1] == INVENTORY_SALES_SPREAD_ROW_LABEL
    pp_bindings = [
        row
        for row in plan["bindings"]
        if row["metric_key"] == "inventory_growth_minus_sales_growth"
    ]
    assert len(pp_bindings) == 8
    for binding in pp_bindings:
        numeric, text, formula = _cell_value(cells[binding["target_cell"]])
        assert text is None and formula is None
        assert Decimal(str(numeric)) == Decimal(
            binding["canonical_value"]["value"]
        ) * 100
        assert _format_for_cell(
            cells[binding["target_cell"]], package["xl/styles.xml"]
        ) == "0.0"


def test_formula_caches_and_d195_shrink_to_fit_are_preserved(full_previews) -> None:
    first = full_previews[0]
    before = _package(PROTECTED_WORKBOOK)
    after = _package(first)
    parts = _sheet_parts(before)
    assert before[parts["Valuation"]] == after[parts["Valuation"]]
    valuation_cells = _cell_map(before[parts["Valuation"]])
    d195 = valuation_cells["D195"]
    style_id = _cell_style_id(d195)
    before_xfs, _ = _style_catalog(before["xl/styles.xml"])
    after_xfs, _ = _style_catalog(after["xl/styles.xml"])
    before_alignment = before_xfs[style_id].find(f"{{{MAIN_NS}}}alignment")
    after_alignment = after_xfs[style_id].find(f"{{{MAIN_NS}}}alignment")
    assert before_alignment is not None and before_alignment.get("shrinkToFit") == "1"
    assert ET.tostring(before_xfs[style_id]) == ET.tostring(after_xfs[style_id])
    assert after_alignment is not None and after_alignment.get("shrinkToFit") == "1"
    target_parts = {parts["SUMMARY"], parts["BS_Segments"]}
    for name in parts.values():
        if name not in target_parts:
            assert before[name] == after[name]
    for part in target_parts:
        before_formulas = {
            ref: _cell_value(cell)
            for ref, cell in _cell_map(before[part]).items()
            if _cell_value(cell)[2] is not None
        }
        after_formulas = {
            ref: _cell_value(cell)
            for ref, cell in _cell_map(after[part]).items()
            if _cell_value(cell)[2] is not None
        }
        assert before_formulas == after_formulas


def test_all_57_sheet_metadata_and_filter_database_names_are_exact(
    mutations, full_previews
) -> None:
    first = full_previews[0]
    before = _package(PROTECTED_WORKBOOK)
    after = _package(first)
    parts = _sheet_parts(before)
    assert len(parts) == 57
    target_cells = {
        "SUMMARY": {item.target_cell for item in mutations if item.target_sheet == "SUMMARY"},
        "BS_Segments": {item.target_cell for item in mutations if item.target_sheet == "BS_Segments"},
    }
    for sheet, part in parts.items():
        if sheet in target_cells:
            assert _mask_cells(before[part], target_cells[sheet]) == _mask_cells(
                after[part], target_cells[sheet]
            )
        else:
            assert before[part] == after[part]
    assert before["xl/workbook.xml"] == after["xl/workbook.xml"]
    workbook = ET.fromstring(after["xl/workbook.xml"])
    names = workbook.find(f"{{{MAIN_NS}}}definedNames")
    filter_names = [] if names is None else [
        item for item in list(names) if item.get("name") == "_xlnm._FilterDatabase"
    ]
    assert len(filter_names) == 3


def test_preview_generation_is_raw_and_canonical_deterministic(full_previews) -> None:
    first, second, first_receipt, second_receipt = full_previews
    assert first.read_bytes() == second.read_bytes()
    assert sha256_file(first) == sha256_file(second)
    assert first_receipt["canonical_ooxml_sha256"] == second_receipt["canonical_ooxml_sha256"]
    assert first_receipt["changed_ooxml_parts"] == second_receipt["changed_ooxml_parts"]


def test_materializer_refuses_overwrite_and_protected_output(plan: dict, tmp_path: Path) -> None:
    output = tmp_path / "existing.xlsx"
    output.write_bytes(b"keep")
    with pytest.raises(SummaryBSWorkbookMaterializationError, match="overwrite"):
        materialize_summary_bs_preview(
            base_workbook=PROTECTED_WORKBOOK,
            output_workbook=output,
            plan=plan,
            expected_plan_digest=EXPECTED_PLAN_DIGEST,
        )
    assert output.read_bytes() == b"keep"
    with pytest.raises(SummaryBSWorkbookMaterializationError, match="output target"):
        materialize_summary_bs_preview(
            base_workbook=PROTECTED_WORKBOOK,
            output_workbook=PROTECTED_WORKBOOK,
            plan=plan,
            expected_plan_digest=EXPECTED_PLAN_DIGEST,
        )
