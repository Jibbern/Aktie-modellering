"""Lossless workbook materialization for the accepted Summary/BS binding plan.

This module owns no source selection and performs no economic calculation.  It
applies only typed, exact-cell mutations that have already been authorized by a
validated Summary/BS projection plan.  Untouched OOXML members are copied from
the frozen workbook oracle byte-for-byte at the part level.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import shutil
import tempfile
from collections import Counter
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path, PurePosixPath
from typing import Any, Literal, Mapping, Sequence
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape
from zipfile import ZipFile

from pbi_xbrl.json_schema_validation import load_json_strict

from .serialization import canonicalize
from .summary_bs_workbook_projection import (
    PRESENTATION_MUTATION_CONTRACT,
    PROJECTION_SCHEMA,
    TARGET_WORKBOOK_LIFECYCLE,
)


MATERIALIZER_CONTRACT = "summary-bs-lossless-workbook-materializer@2"
CANONICAL_OOXML_HASH_CONTRACT = "ordered-uncompressed-ooxml-members-sha256@1"
# Artifact-tool remains useful for workbook inspection and rendering, but its
# XLSX export path is not authoritative for this protected-oracle bridge.
ARTIFACT_TOOL_BRIDGE_ROLE = "READ/INSPECTION/RENDER ONLY"

_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_DOCUMENT_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_PACKAGE_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
_REL_ID = f"{{{_DOCUMENT_REL_NS}}}id"
_CELL_RE = re.compile(r"^[A-Z]+[1-9][0-9]*$")
_CELL_ELEMENT_RE = re.compile(rb"<c\b[^>]*/>|<c\b[^>]*>.*?</c>", re.DOTALL)
_ATTRIBUTE_TEMPLATE = rb"\s+%s=(['\"])(.*?)\1"

MutationMode = Literal["SET_VALUE", "CLEAR_CONTENTS"]
ValueKind = Literal["number", "text", "date"]


class SummaryBSWorkbookMaterializationError(ValueError):
    """Raised before publication when a lossless mutation cannot be proven."""


@dataclass(frozen=True)
class WorkbookCellMutation:
    """One exact workbook mutation authorized by an immutable binding."""

    target_sheet: str
    target_cell: str
    mode: MutationMode
    value_kind: ValueKind | None
    value: str | None
    legacy_number_format_code: str | None
    projection_number_format_code: str | None
    field_id: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "field_id": self.field_id,
            "legacy_number_format_code": self.legacy_number_format_code,
            "mode": self.mode,
            "projection_number_format_code": self.projection_number_format_code,
            "target_cell": self.target_cell,
            "target_sheet": self.target_sheet,
            "value": self.value,
            "value_kind": self.value_kind,
        }


@dataclass(frozen=True)
class WorkbookMaterializationResult:
    """Deterministic receipt for one scratch-workbook materialization."""

    base_workbook_sha256: str
    output_workbook_sha256: str
    canonical_ooxml_sha256: str
    mutation_count: int
    style_variant_count: int
    changed_ooxml_parts: tuple[str, ...]
    unchanged_ooxml_part_count: int
    write_type_counts: Mapping[str, int]

    def to_dict(self) -> dict[str, Any]:
        return {
            "base_workbook_sha256": self.base_workbook_sha256,
            "canonical_ooxml_hash_contract": CANONICAL_OOXML_HASH_CONTRACT,
            "canonical_ooxml_sha256": self.canonical_ooxml_sha256,
            "changed_ooxml_parts": list(self.changed_ooxml_parts),
            "contract": MATERIALIZER_CONTRACT,
            "mutation_count": self.mutation_count,
            "output_workbook_sha256": self.output_workbook_sha256,
            "style_variant_count": self.style_variant_count,
            "unchanged_ooxml_part_count": self.unchanged_ooxml_part_count,
            "write_type_counts": dict(sorted(self.write_type_counts.items())),
        }


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path | str) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_ooxml_sha256(path: Path | str) -> str:
    """Apply ``ordered-uncompressed-ooxml-members-sha256@1``."""

    digest = hashlib.sha256()
    with ZipFile(path, "r") as archive:
        for name in sorted(archive.namelist()):
            encoded_name = name.encode("utf-8")
            payload = archive.read(name)
            digest.update(len(encoded_name).to_bytes(4, "big"))
            digest.update(encoded_name)
            digest.update(len(payload).to_bytes(8, "big"))
            digest.update(payload)
    return digest.hexdigest()


def _plan_digest(plan: Mapping[str, Any]) -> str:
    payload = dict(plan)
    payload.pop("plan_digest", None)
    canonical = json.dumps(
        canonicalize(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return _sha256_bytes(canonical)


def validate_materialization_plan(
    plan: Mapping[str, Any], *, expected_plan_digest: str | None = None
) -> None:
    """Fail closed unless ``plan`` is the complete accepted 452-field plan."""

    if plan.get("schema") != PROJECTION_SCHEMA:
        raise SummaryBSWorkbookMaterializationError("Unsupported Summary/BS projection schema.")
    if plan.get("lifecycle") != TARGET_WORKBOOK_LIFECYCLE:
        raise SummaryBSWorkbookMaterializationError("Materialization cannot change target_not_wired.")
    declared_digest = str(plan.get("plan_digest") or "")
    if not re.fullmatch(r"[0-9a-f]{64}", declared_digest):
        raise SummaryBSWorkbookMaterializationError("Projection plan lacks a canonical digest.")
    if _plan_digest(plan) != declared_digest:
        raise SummaryBSWorkbookMaterializationError("Projection plan digest does not reproduce.")
    if expected_plan_digest is not None and declared_digest != expected_plan_digest.lower():
        raise SummaryBSWorkbookMaterializationError(
            f"Projection plan digest changed: {declared_digest}."
        )
    bindings = plan.get("bindings")
    validation = plan.get("validation")
    if not isinstance(bindings, list) or len(bindings) != 452:
        raise SummaryBSWorkbookMaterializationError("Materialization requires exactly 452 bindings.")
    if not isinstance(validation, Mapping) or not validation.get("passed"):
        raise SummaryBSWorkbookMaterializationError("Projection plan did not pass its closed gate.")
    required_validation = {
        "binding_count": 452,
        "duplicate_target_owner_count": 0,
        "unbound_visible_field_count": 0,
        "available_without_lineage_count": 0,
        "formula_count": 0,
        "percentage_point_binding_count": 8,
        "presentation_mutation_count": 3,
    }
    for key, expected in required_validation.items():
        if validation.get(key) != expected:
            raise SummaryBSWorkbookMaterializationError(
                f"Projection validation {key} changed: {validation.get(key)!r}."
            )
    targets: set[tuple[str, str]] = set()
    for binding in bindings:
        if not isinstance(binding, Mapping):
            raise SummaryBSWorkbookMaterializationError("A projection binding is not an object.")
        sheet = str(binding.get("target_sheet") or "")
        cell = str(binding.get("target_cell") or "")
        if sheet not in {"SUMMARY", "BS_Segments"} or not _CELL_RE.fullmatch(cell):
            raise SummaryBSWorkbookMaterializationError(
                f"Projection target is outside the accepted surfaces: {sheet}!{cell}."
            )
        target = (sheet, cell)
        if target in targets:
            raise SummaryBSWorkbookMaterializationError(
                f"Projection target has duplicate ownership: {sheet}!{cell}."
            )
        targets.add(target)
    presentation_mutations = plan.get("presentation_mutations")
    if not isinstance(presentation_mutations, list) or len(presentation_mutations) != 3:
        raise SummaryBSWorkbookMaterializationError(
            "Materialization requires exactly three bounded presentation mutations."
        )
    for mutation in presentation_mutations:
        if not isinstance(mutation, Mapping):
            raise SummaryBSWorkbookMaterializationError(
                "A presentation mutation is not an object."
            )
        if mutation.get("contract") != PRESENTATION_MUTATION_CONTRACT:
            raise SummaryBSWorkbookMaterializationError(
                "A presentation mutation has an unsupported contract."
            )
        sheet = str(mutation.get("target_sheet") or "")
        cell = str(mutation.get("target_cell") or "")
        if sheet not in {"SUMMARY", "BS_Segments"} or not _CELL_RE.fullmatch(cell):
            raise SummaryBSWorkbookMaterializationError(
                f"Presentation target is outside the accepted surfaces: {sheet}!{cell}."
            )
        target = (sheet, cell)
        if target in targets:
            raise SummaryBSWorkbookMaterializationError(
                f"Presentation target has duplicate ownership: {sheet}!{cell}."
            )
        targets.add(target)
        if mutation.get("write_mode") != "SET_VALUE":
            raise SummaryBSWorkbookMaterializationError(
                "Presentation mutations must be exact SET_VALUE operations."
            )
        write_value = mutation.get("write_value")
        if (
            not isinstance(write_value, Mapping)
            or write_value.get("kind") != "text"
            or not str(write_value.get("text") or "")
        ):
            raise SummaryBSWorkbookMaterializationError(
                "Presentation mutations must contain non-empty typed text."
            )


def load_materialization_plan(
    path: Path | str, *, expected_plan_digest: str | None = None
) -> dict[str, Any]:
    plan = load_json_strict(path)
    if not isinstance(plan, dict):
        raise SummaryBSWorkbookMaterializationError("Projection plan must be a JSON object.")
    validate_materialization_plan(plan, expected_plan_digest=expected_plan_digest)
    return plan


def build_cell_mutations(plan: Mapping[str, Any]) -> tuple[WorkbookCellMutation, ...]:
    """Translate typed plan writes without selecting or recalculating values."""

    validate_materialization_plan(plan)
    mutations: list[WorkbookCellMutation] = []
    for binding in plan["bindings"]:
        mode = str(binding.get("write_mode") or "")
        if mode == "NO_WRITE":
            continue
        if mode == "CLEAR_CONTENTS":
            mutations.append(
                WorkbookCellMutation(
                    target_sheet=str(binding["target_sheet"]),
                    target_cell=str(binding["target_cell"]),
                    mode="CLEAR_CONTENTS",
                    value_kind=None,
                    value=None,
                    legacy_number_format_code=binding.get("legacy_number_format_code"),
                    projection_number_format_code=binding.get("projection_number_format_code"),
                    field_id=str(binding["field_id"]),
                )
            )
            continue
        if mode != "SET_VALUE":
            raise SummaryBSWorkbookMaterializationError(f"Unsupported write mode {mode!r}.")
        write_value = binding.get("write_value")
        if not isinstance(write_value, Mapping):
            raise SummaryBSWorkbookMaterializationError("SET_VALUE binding lacks typed write_value.")
        kind = str(write_value.get("kind") or "")
        if kind == "number":
            value = str(write_value.get("canonical_decimal") or "")
            _finite_decimal(value)
        elif kind == "text":
            value = str(write_value.get("text") or "")
            if not value:
                raise SummaryBSWorkbookMaterializationError("Text writes cannot be empty.")
        elif kind == "date":
            value = str(write_value.get("iso_date") or "")
            try:
                date.fromisoformat(value)
            except ValueError as exc:
                raise SummaryBSWorkbookMaterializationError(
                    f"Invalid ISO date write {value!r}."
                ) from exc
        else:
            raise SummaryBSWorkbookMaterializationError(f"Unsupported write kind {kind!r}.")
        mutations.append(
            WorkbookCellMutation(
                target_sheet=str(binding["target_sheet"]),
                target_cell=str(binding["target_cell"]),
                mode="SET_VALUE",
                value_kind=kind,  # type: ignore[arg-type]
                value=value,
                legacy_number_format_code=binding.get("legacy_number_format_code"),
                projection_number_format_code=binding.get("projection_number_format_code"),
                field_id=str(binding["field_id"]),
            )
        )
    for presentation in plan["presentation_mutations"]:
        write_value = presentation["write_value"]
        mutations.append(
            WorkbookCellMutation(
                target_sheet=str(presentation["target_sheet"]),
                target_cell=str(presentation["target_cell"]),
                mode="SET_VALUE",
                value_kind="text",
                value=str(write_value["text"]),
                legacy_number_format_code=presentation.get(
                    "legacy_number_format_code"
                ),
                projection_number_format_code=presentation.get(
                    "projection_number_format_code"
                ),
                field_id=str(presentation["presentation_id"]),
            )
        )
    return tuple(sorted(mutations, key=_mutation_sort_key))


def _column_number(column: str) -> int:
    value = 0
    for character in column:
        value = value * 26 + ord(character) - ord("A") + 1
    return value


def _mutation_sort_key(mutation: WorkbookCellMutation) -> tuple[str, int, int]:
    match = re.fullmatch(r"([A-Z]+)([1-9][0-9]*)", mutation.target_cell)
    if match is None:
        raise SummaryBSWorkbookMaterializationError(
            f"Invalid target cell {mutation.target_cell!r}."
        )
    return mutation.target_sheet, int(match.group(2)), _column_number(match.group(1))


def _finite_decimal(value: str) -> Decimal:
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise SummaryBSWorkbookMaterializationError(
            f"Workbook numeric write is not a decimal: {value!r}."
        ) from exc
    if not parsed.is_finite():
        raise SummaryBSWorkbookMaterializationError("Workbook numeric writes must be finite.")
    return parsed


def _parse_xml(data: bytes) -> ET.Element:
    try:
        return ET.fromstring(data)
    except ET.ParseError as exc:
        raise SummaryBSWorkbookMaterializationError(f"Invalid OOXML part: {exc}.") from exc


def _local_name(value: str) -> str:
    return value.rsplit("}", 1)[-1]


def _resolve_part(base_part: str, target: str) -> str:
    normalized = target.replace("\\", "/")
    if normalized.startswith("/"):
        return normalized.lstrip("/")
    base = PurePosixPath(base_part).parent
    parts: list[str] = []
    for item in (base / normalized).parts:
        if item == "..":
            if not parts:
                raise SummaryBSWorkbookMaterializationError(
                    f"Relationship target escapes package root: {target!r}."
                )
            parts.pop()
        elif item not in {"", "."}:
            parts.append(item)
    return "/".join(parts)


def _sheet_part_map(archive: ZipFile) -> dict[str, str]:
    workbook_part = "xl/workbook.xml"
    workbook_root = _parse_xml(archive.read(workbook_part))
    rels_root = _parse_xml(archive.read("xl/_rels/workbook.xml.rels"))
    rels = {
        node.get("Id"): node.get("Target")
        for node in rels_root.findall(f"{{{_PACKAGE_REL_NS}}}Relationship")
    }
    sheets = workbook_root.find(f"{{{_MAIN_NS}}}sheets")
    if sheets is None:
        raise SummaryBSWorkbookMaterializationError("Workbook contains no sheets collection.")
    result: dict[str, str] = {}
    for sheet in sheets.findall(f"{{{_MAIN_NS}}}sheet"):
        name = str(sheet.get("name") or "")
        relationship_id = sheet.get(_REL_ID)
        target = rels.get(relationship_id)
        if not name or not target:
            raise SummaryBSWorkbookMaterializationError("Worksheet relationship is incomplete.")
        if name in result:
            raise SummaryBSWorkbookMaterializationError(f"Duplicate worksheet name {name!r}.")
        result[name] = _resolve_part(workbook_part, target)
    return result


def _attribute(element: bytes, name: str) -> str | None:
    match = re.search(_ATTRIBUTE_TEMPLATE % name.encode("ascii"), element)
    return None if match is None else match.group(2).decode("utf-8")


def _set_attribute(start_tag: bytes, name: str, value: str | None) -> bytes:
    pattern = re.compile(_ATTRIBUTE_TEMPLATE % name.encode("ascii"))
    if value is None:
        return pattern.sub(b"", start_tag, count=1)
    escaped = value.replace("&", "&amp;").replace('"', "&quot;")
    replacement = f' {name}="{escaped}"'.encode("utf-8")
    if pattern.search(start_tag):
        return pattern.sub(replacement, start_tag, count=1)
    insertion = len(start_tag) - (2 if start_tag.endswith(b"/>") else 1)
    return start_tag[:insertion] + replacement + start_tag[insertion:]


def _cell_elements(data: bytes) -> dict[str, tuple[int, int, bytes]]:
    result: dict[str, tuple[int, int, bytes]] = {}
    for match in _CELL_ELEMENT_RE.finditer(data):
        element = match.group(0)
        coordinate = _attribute(element[: element.find(b">") + 1], "r")
        if coordinate is None:
            raise SummaryBSWorkbookMaterializationError("Worksheet cell lacks an r attribute.")
        if coordinate in result:
            raise SummaryBSWorkbookMaterializationError(
                f"Worksheet contains duplicate cell {coordinate}."
            )
        result[coordinate] = (match.start(), match.end(), element)
    return result


def _cell_style_id(element: bytes) -> int:
    start_tag = element[: element.find(b">") + 1]
    value = _attribute(start_tag, "s")
    return 0 if value is None else int(value)


def _cell_child_names(element: bytes) -> set[str]:
    if element.endswith(b"/>"):
        return set()
    root = _parse_xml(element)
    return {_local_name(child.tag) for child in list(root)}


def _date_serial(value: str) -> str:
    parsed = date.fromisoformat(value)
    return str((parsed - date(1899, 12, 30)).days)


def _patch_cell(
    element: bytes,
    mutation: WorkbookCellMutation,
    *,
    style_id: int,
) -> bytes:
    child_names = _cell_child_names(element)
    unsupported = child_names - {"f", "v", "is"}
    if unsupported:
        raise SummaryBSWorkbookMaterializationError(
            f"Target {mutation.target_sheet}!{mutation.target_cell} has unsupported cell children: "
            f"{sorted(unsupported)!r}."
        )
    if "f" in child_names:
        raise SummaryBSWorkbookMaterializationError(
            f"Target {mutation.target_sheet}!{mutation.target_cell} unexpectedly contains a formula."
        )
    start_end = element.find(b">")
    if start_end < 0:
        raise SummaryBSWorkbookMaterializationError("Malformed worksheet cell element.")
    start_tag = element[: start_end + 1]
    if start_tag.endswith(b"/>"):
        start_tag = start_tag[:-2] + b">"
    start_tag = _set_attribute(start_tag, "s", str(style_id))
    if mutation.mode == "CLEAR_CONTENTS":
        start_tag = _set_attribute(start_tag, "t", None)
        return start_tag[:-1] + b"/>"
    if mutation.value is None or mutation.value_kind is None:
        raise SummaryBSWorkbookMaterializationError("SET_VALUE mutation lacks typed content.")
    if mutation.value_kind == "number":
        _finite_decimal(mutation.value)
        start_tag = _set_attribute(start_tag, "t", "n")
        content = b"<v>" + mutation.value.encode("ascii") + b"</v>"
    elif mutation.value_kind == "date":
        start_tag = _set_attribute(start_tag, "t", "n")
        content = b"<v>" + _date_serial(mutation.value).encode("ascii") + b"</v>"
    elif mutation.value_kind == "text":
        start_tag = _set_attribute(start_tag, "t", "inlineStr")
        preserve = (
            mutation.value != mutation.value.strip()
            or "  " in mutation.value
            or "\n" in mutation.value
        )
        space = ' xml:space="preserve"' if preserve else ""
        content = (
            f"<is><t{space}>{escape(mutation.value)}</t></is>".encode("utf-8")
        )
    else:  # pragma: no cover - dataclass typing plus plan validation close this branch
        raise SummaryBSWorkbookMaterializationError(
            f"Unsupported mutation value kind {mutation.value_kind!r}."
        )
    return start_tag + content + b"</c>"


def _canonical_element(node: ET.Element) -> tuple[Any, ...]:
    return (
        _local_name(node.tag),
        tuple(sorted(node.attrib.items())),
        (node.text or ""),
        tuple(_canonical_element(child) for child in list(node)),
    )


def _xf_key(node: ET.Element) -> tuple[Any, ...]:
    clone = copy.deepcopy(node)
    clone.attrib.pop("numFmtId", None)
    clone.attrib.pop("applyNumberFormat", None)
    return _canonical_element(clone)


def _style_state(styles: bytes) -> tuple[list[ET.Element], list[bytes], dict[str, int]]:
    root = _parse_xml(styles)
    cell_xfs = root.find(f"{{{_MAIN_NS}}}cellXfs")
    if cell_xfs is None:
        raise SummaryBSWorkbookMaterializationError("Workbook styles lack cellXfs.")
    xfs = list(cell_xfs)
    match = re.search(rb"<cellXfs\b[^>]*>(?P<body>.*?)</cellXfs>", styles, re.DOTALL)
    if match is None:
        raise SummaryBSWorkbookMaterializationError(
            "Styles part is not compatible with bounded cellXfs extension."
        )
    raw_xfs = list(
        re.finditer(rb"<xf\b[^>]*/>|<xf\b[^>]*>.*?</xf>", match.group("body"), re.DOTALL)
    )
    raw_values = [item.group(0) for item in raw_xfs]
    if len(raw_values) != len(xfs):
        raise SummaryBSWorkbookMaterializationError(
            "Raw and parsed cellXfs counts do not agree."
        )
    number_formats: dict[str, int] = {"General": 0}
    num_fmts = root.find(f"{{{_MAIN_NS}}}numFmts")
    if num_fmts is not None:
        for node in list(num_fmts):
            code = node.get("formatCode")
            identifier = node.get("numFmtId")
            if code is not None and identifier is not None:
                if code in number_formats and number_formats[code] != int(identifier):
                    raise SummaryBSWorkbookMaterializationError(
                        f"Number format {code!r} is registered more than once."
                    )
                number_formats[code] = int(identifier)
    return xfs, raw_values, number_formats


def _xf_with_number_format(raw: bytes, number_format_id: int) -> bytes:
    start_end = raw.find(b">")
    if start_end < 0:
        raise SummaryBSWorkbookMaterializationError("Malformed cellXf style.")
    start = raw[: start_end + 1]
    remainder = raw[start_end + 1 :]
    start = _set_attribute(start, "numFmtId", str(number_format_id))
    start = _set_attribute(start, "applyNumberFormat", "1")
    return start + remainder


def _parse_raw_element(raw: bytes) -> ET.Element:
    return _parse_xml(raw)


def _resolve_style_variants(
    styles: bytes,
    requests: Sequence[tuple[int, str]],
) -> tuple[dict[tuple[int, str], int], bytes, int]:
    xfs, raw_xfs, number_formats = _style_state(styles)
    resolved: dict[tuple[int, str], int] = {}
    appended: list[bytes] = []
    all_nodes = list(xfs)
    all_raw = list(raw_xfs)
    for base_style_id, format_code in requests:
        key = (base_style_id, format_code)
        if key in resolved:
            continue
        if base_style_id < 0 or base_style_id >= len(xfs):
            raise SummaryBSWorkbookMaterializationError(
                f"Target references unknown style {base_style_id}."
            )
        number_format_id = number_formats.get(format_code)
        if number_format_id is None:
            raise SummaryBSWorkbookMaterializationError(
                f"Projection number format is not registered in the oracle: {format_code!r}."
            )
        desired_raw = _xf_with_number_format(raw_xfs[base_style_id], number_format_id)
        desired_node = _parse_raw_element(desired_raw)
        desired_key = _xf_key(desired_node)
        matches = [
            index
            for index, candidate in enumerate(all_nodes)
            if _xf_key(candidate) == desired_key
            and int(candidate.get("numFmtId", "0")) == number_format_id
        ]
        if len(matches) > 1:
            raise SummaryBSWorkbookMaterializationError(
                f"Style variant for {format_code!r} is ambiguous: {matches!r}."
            )
        if matches:
            resolved[key] = matches[0]
            continue
        resolved[key] = len(all_nodes)
        appended.append(desired_raw)
        all_raw.append(desired_raw)
        all_nodes.append(desired_node)
    if not appended:
        return resolved, styles, 0
    match = re.search(rb"<cellXfs\b(?P<attrs>[^>]*)>(?P<body>.*?)</cellXfs>", styles, re.DOTALL)
    if match is None:
        raise SummaryBSWorkbookMaterializationError("Could not locate cellXfs for extension.")
    opening = styles[match.start() : match.start("body")]
    opening = _set_attribute(opening, "count", str(len(xfs) + len(appended)))
    replacement = opening + match.group("body") + b"".join(appended) + b"</cellXfs>"
    updated = styles[: match.start()] + replacement + styles[match.end() :]
    check_xfs, _, _ = _style_state(updated)
    if len(check_xfs) != len(xfs) + len(appended):
        raise SummaryBSWorkbookMaterializationError("Extended style table failed validation.")
    return resolved, updated, len(appended)


def _patch_worksheet(
    data: bytes,
    mutations: Sequence[WorkbookCellMutation],
    style_ids: Mapping[tuple[str, str], int],
) -> bytes:
    cells = _cell_elements(data)
    replacements: list[tuple[int, int, bytes]] = []
    for mutation in mutations:
        located = cells.get(mutation.target_cell)
        if located is None:
            raise SummaryBSWorkbookMaterializationError(
                f"Target cell does not exist in the frozen shell: "
                f"{mutation.target_sheet}!{mutation.target_cell}."
            )
        start, end, element = located
        replacements.append(
            (
                start,
                end,
                _patch_cell(
                    element,
                    mutation,
                    style_id=style_ids[(mutation.target_sheet, mutation.target_cell)],
                ),
            )
        )
    output = data
    for start, end, replacement in sorted(replacements, reverse=True):
        output = output[:start] + replacement + output[end:]
    return output


def _write_package(
    *,
    base_workbook: Path,
    output_workbook: Path,
    members: Mapping[str, bytes],
) -> None:
    output_workbook.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary_name = tempfile.mkstemp(
        prefix=f".{output_workbook.stem}.",
        suffix=output_workbook.suffix,
        dir=output_workbook.parent,
    )
    os.close(handle)
    temporary = Path(temporary_name)
    try:
        with ZipFile(base_workbook, "r") as source, ZipFile(temporary, "w") as output:
            output.comment = source.comment
            for info in source.infolist():
                output.writestr(info, members[info.filename])
        os.replace(temporary, output_workbook)
    finally:
        if temporary.exists():
            temporary.unlink()


def materialize_ooxml_cell_mutations(
    *,
    base_workbook: Path | str,
    output_workbook: Path | str,
    mutations: Sequence[WorkbookCellMutation],
    expected_base_sha256: str | None = None,
) -> WorkbookMaterializationResult:
    """Copy ``base_workbook`` and apply only the supplied exact-cell mutations."""

    base = Path(base_workbook)
    output = Path(output_workbook)
    if base.resolve() == output.resolve():
        raise SummaryBSWorkbookMaterializationError("Protected workbook cannot be an output target.")
    if output.exists():
        raise SummaryBSWorkbookMaterializationError(
            f"Refusing to overwrite existing materialization: {output}."
        )
    if base.suffix.lower() != ".xlsx" or output.suffix.lower() != ".xlsx":
        raise SummaryBSWorkbookMaterializationError("Lossless materialization requires .xlsx files.")
    base_sha = sha256_file(base)
    if expected_base_sha256 is not None and base_sha != expected_base_sha256.lower():
        raise SummaryBSWorkbookMaterializationError(
            f"Frozen workbook hash changed: {base_sha}."
        )
    ordered = tuple(sorted(mutations, key=_mutation_sort_key))
    targets = [(item.target_sheet, item.target_cell) for item in ordered]
    if len(targets) != len(set(targets)):
        raise SummaryBSWorkbookMaterializationError("Cell mutations contain duplicate targets.")
    if not ordered:
        output.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(base, output)
        with ZipFile(base, "r") as archive:
            unchanged_count = len(archive.namelist())
        return WorkbookMaterializationResult(
            base_workbook_sha256=base_sha,
            output_workbook_sha256=sha256_file(output),
            canonical_ooxml_sha256=canonical_ooxml_sha256(output),
            mutation_count=0,
            style_variant_count=0,
            changed_ooxml_parts=(),
            unchanged_ooxml_part_count=unchanged_count,
            write_type_counts={},
        )

    with ZipFile(base, "r") as source:
        source_infos = source.infolist()
        members = {info.filename: source.read(info.filename) for info in source_infos}
        sheet_parts = _sheet_part_map(source)
        style_part = "xl/styles.xml"
        if style_part not in members:
            raise SummaryBSWorkbookMaterializationError("Workbook lacks xl/styles.xml.")

    by_sheet: dict[str, list[WorkbookCellMutation]] = {}
    for mutation in ordered:
        if mutation.target_sheet not in sheet_parts:
            raise SummaryBSWorkbookMaterializationError(
                f"Workbook lacks target sheet {mutation.target_sheet!r}."
            )
        by_sheet.setdefault(mutation.target_sheet, []).append(mutation)

    original_cells: dict[tuple[str, str], bytes] = {}
    for sheet_name, sheet_mutations in by_sheet.items():
        part = sheet_parts[sheet_name]
        cells = _cell_elements(members[part])
        for mutation in sheet_mutations:
            located = cells.get(mutation.target_cell)
            if located is None:
                raise SummaryBSWorkbookMaterializationError(
                    f"Target cell is absent: {sheet_name}!{mutation.target_cell}."
                )
            original_cells[(sheet_name, mutation.target_cell)] = located[2]

    style_requests: list[tuple[int, str]] = []
    for mutation in ordered:
        if (
            mutation.projection_number_format_code is not None
            and mutation.projection_number_format_code
            != mutation.legacy_number_format_code
        ):
            style_requests.append(
                (
                    _cell_style_id(original_cells[(mutation.target_sheet, mutation.target_cell)]),
                    mutation.projection_number_format_code,
                )
            )
    style_variants, updated_styles, style_variant_count = _resolve_style_variants(
        members[style_part], style_requests
    )
    style_ids: dict[tuple[str, str], int] = {}
    for mutation in ordered:
        target = (mutation.target_sheet, mutation.target_cell)
        base_style_id = _cell_style_id(original_cells[target])
        if (
            mutation.projection_number_format_code is not None
            and mutation.projection_number_format_code
            != mutation.legacy_number_format_code
        ):
            style_ids[target] = style_variants[
                (base_style_id, mutation.projection_number_format_code)
            ]
        else:
            style_ids[target] = base_style_id

    changed_parts: set[str] = set()
    for sheet_name, sheet_mutations in sorted(by_sheet.items()):
        part = sheet_parts[sheet_name]
        updated = _patch_worksheet(members[part], sheet_mutations, style_ids)
        if updated != members[part]:
            members[part] = updated
            changed_parts.add(part)
    if updated_styles != members[style_part]:
        members[style_part] = updated_styles
        changed_parts.add(style_part)

    _write_package(base_workbook=base, output_workbook=output, members=members)
    with ZipFile(base, "r") as before, ZipFile(output, "r") as after:
        if before.namelist() != after.namelist():
            raise SummaryBSWorkbookMaterializationError("OOXML member inventory changed.")
        observed_changed = {
            name for name in before.namelist() if before.read(name) != after.read(name)
        }
        if observed_changed != changed_parts:
            raise SummaryBSWorkbookMaterializationError(
                f"Unexpected OOXML part changes: {sorted(observed_changed ^ changed_parts)!r}."
            )
        unchanged_count = len(before.namelist()) - len(observed_changed)

    write_types = Counter(
        "clear" if mutation.mode == "CLEAR_CONTENTS" else str(mutation.value_kind)
        for mutation in ordered
    )
    return WorkbookMaterializationResult(
        base_workbook_sha256=base_sha,
        output_workbook_sha256=sha256_file(output),
        canonical_ooxml_sha256=canonical_ooxml_sha256(output),
        mutation_count=len(ordered),
        style_variant_count=style_variant_count,
        changed_ooxml_parts=tuple(sorted(changed_parts)),
        unchanged_ooxml_part_count=unchanged_count,
        write_type_counts=dict(sorted(write_types.items())),
    )


def materialize_summary_bs_preview(
    *,
    base_workbook: Path | str,
    output_workbook: Path | str,
    plan: Mapping[str, Any],
    expected_plan_digest: str | None = None,
) -> dict[str, Any]:
    """Materialize the immutable Summary/BS plan into a fresh scratch workbook."""

    validate_materialization_plan(plan, expected_plan_digest=expected_plan_digest)
    protected = plan.get("protected_workbook")
    if not isinstance(protected, Mapping):
        raise SummaryBSWorkbookMaterializationError("Plan lacks protected workbook identity.")
    protected_sha = str(protected.get("sha256") or "").lower()
    if not re.fullmatch(r"[0-9a-f]{64}", protected_sha):
        raise SummaryBSWorkbookMaterializationError("Plan has invalid protected workbook hash.")
    mutations = build_cell_mutations(plan)
    result = materialize_ooxml_cell_mutations(
        base_workbook=base_workbook,
        output_workbook=output_workbook,
        mutations=mutations,
        expected_base_sha256=protected_sha,
    )
    no_write_count = sum(
        1 for binding in plan["bindings"] if binding.get("write_mode") == "NO_WRITE"
    )
    receipt = result.to_dict()
    receipt.update(
        {
            "binding_count": len(plan["bindings"]),
            "binding_plan_digest": plan["plan_digest"],
            "lifecycle": plan["lifecycle"],
            "no_write_count": no_write_count,
            "output_workbook": str(Path(output_workbook)),
            "presentation_mutation_count": len(plan["presentation_mutations"]),
        }
    )
    return receipt


__all__ = [
    "ARTIFACT_TOOL_BRIDGE_ROLE",
    "CANONICAL_OOXML_HASH_CONTRACT",
    "MATERIALIZER_CONTRACT",
    "SummaryBSWorkbookMaterializationError",
    "WorkbookCellMutation",
    "WorkbookMaterializationResult",
    "build_cell_mutations",
    "canonical_ooxml_sha256",
    "load_materialization_plan",
    "materialize_ooxml_cell_mutations",
    "materialize_summary_bs_preview",
    "sha256_file",
    "validate_materialization_plan",
]
