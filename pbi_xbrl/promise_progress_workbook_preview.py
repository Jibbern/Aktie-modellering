"""Preview-only workbook bridge for :class:`PromiseProgressProduct`.

The source-native product remains the sole economic authority.  This module owns a
closed workbook binding plan and a minimal OOXML materializer for disposable ANF
preview copies; it does not participate in production workbook orchestration.
"""

from __future__ import annotations

import dataclasses
import copy
import hashlib
import io
import json
import math
import os
import re
import tempfile
from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import unquote
from xml.etree import ElementTree as ET
from zipfile import ZipFile

from PIL import ImageFont

from pbi_xbrl.longitudinal_memory.promise_progress_projection import (
    ANNUAL_BLOCK_ID,
    BLOCK_FIELD_LAYOUT,
    OPEN_BLOCK_ID,
    PRODUCT_CONTRACT_ID,
    PROGRESS_RUN_RATE_ID,
    SCORECARD_BLOCK_ID,
    SHEET_NAME,
    TIMELINE_BLOCK_ID,
    PromiseProgressProduct,
    serialize_promise_progress_product,
    serialize_shadow_matrix,
    validate_promise_progress_product,
)
from pbi_xbrl.longitudinal_memory.promise_progress_product_v2 import (
    BLOCK_ORDER as PRODUCT_V2_BLOCK_ORDER,
    CREDIBILITY_BLOCK_ID as PRODUCT_V2_CREDIBILITY_BLOCK_ID,
    OPEN_BLOCK_ID as PRODUCT_V2_OPEN_BLOCK_ID,
    PROGRESSION_BLOCK_ID as PRODUCT_V2_PROGRESSION_BLOCK_ID,
    TIMELINE_BLOCK_ID as PRODUCT_V2_TIMELINE_BLOCK_ID,
    PRODUCT_VERSION as PRODUCT_V2_GOLDEN_VERSION,
    SUCCESSOR_PRODUCT_VERSION,
    PromiseProgressProductV2,
    ProductRowV2,
    display_value as display_product_v2_value,
    promise_progress_product_v2_sha256,
)


BINDING_PLAN_SCHEMA_ID = "contract:promise-progress-workbook-binding-plan@2"
PRESENTATION_CONTRACT_ID = "contract:promise-progress-workbook-presentation@2"
WORKBOOK_TRACE_SCHEMA_ID = "trace:promise-progress-workbook-preview@2"
STRUCTURAL_VALIDATION_SCHEMA_ID = "validation:promise-progress-workbook-structure@2"
SEMANTIC_VALIDATION_SCHEMA_ID = "validation:promise-progress-workbook-semantics@2"
VISUAL_FIT_VALIDATION_SCHEMA_ID = "validation:promise-progress-workbook-visual-fit@2"
LEGACY_DIFFERENCE_SCHEMA_ID = "report:promise-progress-workbook-legacy-differences@2"
PREVIEW_MANIFEST_SCHEMA_ID = "manifest:promise-progress-workbook-preview@2"

IDENTITY_TRANSFORM_ID = "identity@1"
STORE_PROGRESS_TRANSFORM_ID = "store-progress-lines@1"
SOURCE_SUMMARY_TRANSFORM_ID = "source-summary@1"
_DISPLAY_TRANSFORM_VERSION = "1"
_PINNED_FONT_PATH = Path(r"C:\Windows\Fonts\calibri.ttf")
_PINNED_FONT_NAME = "Calibri"
_PINNED_FONT_SIZE_POINTS = 11.0
_PINNED_DPI = 96
_APPROVED_DATA_ROW_HEIGHTS = (24, 40, 56, 72)

EXPECTED_DESIGN_LOCK_MANIFEST_SHA256 = (
    "af7a77b0f7fbee36d6aa92c3c3edfb4c23fe26416f7701107c4deb07ec341ade"
)
EXPECTED_ANF_WORKBOOK_SHA256 = (
    "ef73bdef6b6efa1bc358622b58bfc320b609c128e76c602de9d4e5f726ab98cd"
)
EXPECTED_ANF_PRODUCT_SHA256 = (
    "9e9c042289c1d4e424595c12a6d495170e52a46adfea9ce007baf005fb6265b1"
)
EXPECTED_ANF_SHADOW_SHA256 = (
    "37285c198f975f77e54c17a70abcf0930c81339964fee2d7f6c51da6d64efdb9"
)

_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_DOCUMENT_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_PACKAGE_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
_XML_NS = "http://www.w3.org/XML/1998/namespace"
_CELL_RE = re.compile(r"^([A-Z]{1,3})([1-9][0-9]*)$")
_SUPPORTED_DISPLAY_FORMS = frozenset(
    {"exact", "approximate", "range", "bound", "percentage", "qualitative", "date", "missing"}
)
_STATUS_STYLE_LABEL = {
    "completed": "Completed",
    "hit": "Hit",
    "beat": "Beat",
    "on_track": "On track",
    "open": "Open",
    "mixed": "Mixed",
    "missed": "Missed",
    "basis_dependent": "Basis-dependent",
    "needs_review": "Mixed",
    "withdrawn": "Withdrawn",
}
_TIMELINE_HEADER_GROUPS = (
    ("A59", range(61, 68)),
    ("A69", range(71, 75)),
    ("A76", range(78, 84)),
    ("A84", range(86, 89)),
    ("A90", range(92, 96)),
    ("A97", range(99, 103)),
)
_STATUS_CAPACITY_RANGES = (
    "H13:H20",
    "H24:H26",
    "H30:H31",
    "H35",
    "D39:D46",
    "D48:D56",
    "G61:G67",
    "G71:G74",
    "G78:G83",
    "G86:G88",
    "G92:G95",
    "G99:G102",
)

_BLOCK_HEADER_ROWS: Mapping[str, tuple[int, ...]] = {
    SCORECARD_BLOCK_ID: (4,),
    ANNUAL_BLOCK_ID: (12, 23, 29, 34),
    OPEN_BLOCK_ID: (38, 47),
    TIMELINE_BLOCK_ID: (60, 70, 77, 85, 91, 98),
}
_BLOCK_CAPACITY_ROWS: Mapping[str, tuple[int, ...]] = {
    SCORECARD_BLOCK_ID: tuple(range(5, 10)),
    ANNUAL_BLOCK_ID: tuple(range(13, 21)) + tuple(range(24, 27)) + (30, 31, 35),
    OPEN_BLOCK_ID: tuple(range(39, 47)) + tuple(range(48, 57)),
    TIMELINE_BLOCK_ID: (
        tuple(range(61, 68))
        + tuple(range(71, 75))
        + tuple(range(78, 84))
        + tuple(range(86, 89))
        + tuple(range(92, 96))
        + tuple(range(99, 103))
    ),
}
_LAYOUT_V2: Mapping[str, tuple[tuple[str, str, str, str, str, str], ...]] = {
    SCORECARD_BLOCK_ID: (
        ("category", "A", "A", "always", "left", "top"),
        ("score", "B", "B", "never", "center", "center"),
        ("evidence", "C", "F", "always", "left", "top"),
        ("read", "G", "L", "always", "left", "top"),
    ),
    ANNUAL_BLOCK_ID: (
        ("metric", "A", "A", "always", "left", "top"),
        ("initial_guide", "B", "B", "qualitative", "preserve", "top"),
        ("q1_guide", "C", "C", "qualitative", "preserve", "top"),
        ("q2_guide", "D", "D", "qualitative", "preserve", "top"),
        ("q3_guide", "E", "E", "qualitative", "preserve", "top"),
        ("q4_guide", "F", "F", "qualitative", "preserve", "top"),
        ("actual", "G", "H", "composite", "preserve", "top"),
        ("status", "I", "I", "never", "center", "center"),
        ("notes_source", "J", "L", "always", "left", "top"),
    ),
    OPEN_BLOCK_ID: (
        ("metric", "A", "B", "always", "left", "top"),
        ("current_guide", "C", "D", "qualitative", "preserve", "top"),
        ("horizon", "E", "E", "qualitative", "left", "top"),
        ("status", "F", "F", "never", "center", "center"),
        ("notes_source", "G", "L", "always", "left", "top"),
    ),
    TIMELINE_BLOCK_ID: (
        ("metric", "A", "A", "always", "left", "top"),
        ("previous_guide", "B", "B", "qualitative", "preserve", "top"),
        ("current_guide", "C", "C", "qualitative", "preserve", "top"),
        ("change_type", "D", "D", "qualitative", "left", "top"),
        ("actual", "E", "E", "composite", "preserve", "top"),
        ("progress", "F", "F", "composite", "preserve", "top"),
        ("status", "G", "G", "never", "center", "center"),
        ("horizon", "H", "H", "qualitative", "left", "top"),
        ("stated_in", "I", "I", "never", "preserve", "center"),
        ("source_date", "J", "J", "never", "preserve", "center"),
        ("source_note", "K", "L", "always", "left", "top"),
    ),
}
_HEADER_LABELS: Mapping[str, Mapping[str, str]] = {
    SCORECARD_BLOCK_ID: {
        "category": "Category", "score": "Score", "evidence": "Evidence", "read": "Read",
    },
    ANNUAL_BLOCK_ID: {
        "metric": "Metric", "initial_guide": "Initial guide", "q1_guide": "Q1 update",
        "q2_guide": "Q2 update", "q3_guide": "Q3 update", "q4_guide": "Q4 update",
        "actual": "Actual", "status": "Status", "notes_source": "Notes/source",
    },
    OPEN_BLOCK_ID: {
        "metric": "Metric", "current_guide": "Current guide", "horizon": "Horizon",
        "status": "Status", "notes_source": "Notes/source",
    },
    TIMELINE_BLOCK_ID: {
        "metric": "Metric", "previous_guide": "Previous guide", "current_guide": "New/current guide",
        "change_type": "Change type", "actual": "Actual", "progress": "Progress / run-rate",
        "status": "Status", "horizon": "Horizon", "stated_in": "Stated in",
        "source_date": "Source date", "source_note": "Source / note",
    },
}


@dataclass(frozen=True)
class PresentationFieldLayout:
    block_id: str
    field_role: str
    start_column: str
    end_column: str
    wrap_mode: str
    horizontal_alignment: str
    vertical_alignment: str

    @property
    def span_width(self) -> int:
        return _column_number(self.end_column) - _column_number(self.start_column) + 1

    def range_for_row(self, row_number: int) -> str:
        start = f"{self.start_column}{row_number}"
        end = f"{self.end_column}{row_number}"
        return start if start == end else f"{start}:{end}"


@dataclass(frozen=True)
class PromiseProgressWorkbookPresentationContract:
    field_layouts: tuple[PresentationFieldLayout, ...]
    permitted_merges: tuple[str, ...]
    visible_columns: tuple[str, ...] = tuple("ABCDEFGHIJKL")
    visible_base_width_class: str = "U24"
    visible_base_width: int = 24
    hidden_support_columns: tuple[tuple[str, int, str], ...] = (
        ("M", 4, "blank"), ("N", 4, "blank"), ("O", 13, "row_id_only")
    )
    row_height_tiers: tuple[int, ...] = _APPROVED_DATA_ROW_HEIGHTS
    timeline_max_height: int = 56
    other_block_max_height: int = 72
    transform_ids: tuple[str, ...] = (
        IDENTITY_TRANSFORM_ID, STORE_PROGRESS_TRANSFORM_ID, SOURCE_SUMMARY_TRANSFORM_ID
    )
    font_name: str = _PINNED_FONT_NAME
    font_size_points: float = _PINNED_FONT_SIZE_POINTS
    font_dpi: int = _PINNED_DPI
    contract_id: str = PRESENTATION_CONTRACT_ID

    def layout_for(self, block_id: str, field_role: str) -> PresentationFieldLayout:
        matches = [
            row for row in self.field_layouts
            if row.block_id == block_id and row.field_role == field_role
        ]
        if len(matches) != 1:
            raise PromiseProgressWorkbookPreviewError(
                f"presentation role {block_id}/{field_role} does not resolve exactly once"
            )
        return matches[0]

    def to_dict(self) -> dict[str, Any]:
        result = _canonical(dataclasses.asdict(self))
        result["font_file"] = str(_PINNED_FONT_PATH)
        result["font_file_sha256"] = sha256_file(_PINNED_FONT_PATH)
        result["header_rows"] = {
            block_id: list(rows) for block_id, rows in _BLOCK_HEADER_ROWS.items()
        }
        result["capacity_rows"] = {
            block_id: list(rows) for block_id, rows in _BLOCK_CAPACITY_ROWS.items()
        }
        result["header_labels"] = _canonical(_HEADER_LABELS)
        result["economics_authority"] = "none-presentation-only"
        result["contract_digest"] = _sha256_bytes(canonical_json_bytes(result))
        return result


def _presentation_contract_v2(design_lock_root: Path) -> PromiseProgressWorkbookPresentationContract:
    structural = load_json_strict(design_lock_root / "promise_progress_structural_parity_contract.json")
    dynamic_rows = {
        row for rows in _BLOCK_HEADER_ROWS.values() for row in rows
    } | {
        row for rows in _BLOCK_CAPACITY_ROWS.values() for row in rows
    } | {2}
    static_merges = []
    for merged_range in structural["merged_ranges"]:
        first = _expand_range(str(merged_range))[0]
        if _cell_parts(first)[1] not in dynamic_rows:
            static_merges.append(str(merged_range))
    layouts = tuple(
        PresentationFieldLayout(block_id, *row)
        for block_id, rows in _LAYOUT_V2.items()
        for row in rows
    )
    dynamic_merges = {"A2:L2"}
    for layout in layouts:
        if layout.start_column == layout.end_column:
            continue
        for row_number in _BLOCK_HEADER_ROWS[layout.block_id] + _BLOCK_CAPACITY_ROWS[layout.block_id]:
            dynamic_merges.add(layout.range_for_row(row_number))
    permitted = tuple(
        sorted(set(static_merges) | dynamic_merges, key=lambda value: _cell_sort_key(_expand_range(value)[0]))
    )
    contract = PromiseProgressWorkbookPresentationContract(
        field_layouts=layouts,
        permitted_merges=permitted,
    )
    _validate_presentation_contract(contract)
    return contract


def _validate_presentation_contract(contract: PromiseProgressWorkbookPresentationContract) -> None:
    if contract.contract_id != PRESENTATION_CONTRACT_ID:
        raise PromiseProgressWorkbookPreviewError("unsupported workbook presentation contract")
    if contract.visible_columns != tuple("ABCDEFGHIJKL") or contract.visible_base_width != 24:
        raise PromiseProgressWorkbookPreviewError("presentation contract must use the exact U24 A:L grid")
    if contract.hidden_support_columns != (("M", 4, "blank"), ("N", 4, "blank"), ("O", 13, "row_id_only")):
        raise PromiseProgressWorkbookPreviewError("hidden support column contract changed")
    identities = [(row.block_id, row.field_role) for row in contract.field_layouts]
    expected = [(block, row[0]) for block, rows in _LAYOUT_V2.items() for row in rows]
    if identities != expected or len(identities) != len(set(identities)):
        raise PromiseProgressWorkbookPreviewError("presentation field roles are not the closed reviewed set")
    merge_children = _merged_child_map(contract.permitted_merges)
    if len(contract.permitted_merges) != len(set(contract.permitted_merges)):
        raise PromiseProgressWorkbookPreviewError("presentation contract contains duplicate merges")
    for layout in contract.field_layouts:
        for row_number in _BLOCK_HEADER_ROWS[layout.block_id] + _BLOCK_CAPACITY_ROWS[layout.block_id]:
            anchor = f"{layout.start_column}{row_number}"
            if anchor in merge_children:
                raise PromiseProgressWorkbookPreviewError("presentation merge makes a reviewed anchor unwritable")
    if contract.row_height_tiers != (24, 40, 56, 72):
        raise PromiseProgressWorkbookPreviewError("presentation row height vocabulary changed")
    if set(contract.transform_ids) != {
        IDENTITY_TRANSFORM_ID, STORE_PROGRESS_TRANSFORM_ID, SOURCE_SUMMARY_TRANSFORM_ID
    }:
        raise PromiseProgressWorkbookPreviewError("presentation transform vocabulary changed")


class PromiseProgressWorkbookPreviewError(ValueError):
    """Raised before publication when the preview bridge cannot prove safety."""


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical(value: Any) -> Any:
    if dataclasses.is_dataclass(value):
        return _canonical(dataclasses.asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _canonical(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    return value


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(_canonical(value), ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def deterministic_json_bytes(value: Any) -> bytes:
    return (json.dumps(_canonical(value), ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")


def write_deterministic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(deterministic_json_bytes(value))


def load_json_strict(path: Path) -> dict[str, Any]:
    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise PromiseProgressWorkbookPreviewError(f"duplicate JSON key {key!r} in {path}")
            result[key] = value
        return result

    value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=object_pairs)
    if not isinstance(value, dict):
        raise PromiseProgressWorkbookPreviewError(f"expected a JSON object in {path}")
    return value


def verify_design_lock(
    design_lock_root: Path,
    *,
    expected_manifest_sha256: str = EXPECTED_DESIGN_LOCK_MANIFEST_SHA256,
) -> dict[str, Any]:
    manifest_path = design_lock_root / "design_lock_artifact_manifest.json"
    actual_manifest_sha = sha256_file(manifest_path)
    if actual_manifest_sha != expected_manifest_sha256:
        raise PromiseProgressWorkbookPreviewError(
            f"design-lock manifest SHA mismatch: expected {expected_manifest_sha256}, got {actual_manifest_sha}"
        )
    manifest = load_json_strict(manifest_path)
    if manifest.get("schema_id") != "manifest:promise-progress-design-lock@1":
        raise PromiseProgressWorkbookPreviewError("unexpected Promise Progress design-lock manifest identity")
    verified: list[dict[str, Any]] = []
    for artifact in manifest.get("artifacts_excluding_manifest", ()):
        path = design_lock_root / str(artifact["name"])
        if not path.is_file():
            raise PromiseProgressWorkbookPreviewError(f"design-lock artifact is missing: {path}")
        actual_size = path.stat().st_size
        actual_sha = sha256_file(path)
        if actual_size != int(artifact["size"]) or actual_sha != artifact["sha256"]:
            raise PromiseProgressWorkbookPreviewError(f"design-lock artifact differs from its manifest: {path}")
        if path.suffix.lower() == ".json":
            load_json_strict(path)
        verified.append({"name": path.name, "size": actual_size, "sha256": actual_sha})
    return {
        "manifest_path": str(manifest_path),
        "manifest_sha256": actual_manifest_sha,
        "artifact_count": len(verified),
        "artifacts": verified,
    }


def _column_number(column: str) -> int:
    value = 0
    for char in column:
        value = value * 26 + ord(char) - 64
    return value


def _cell_parts(cell: str) -> tuple[str, int]:
    match = _CELL_RE.fullmatch(cell)
    if match is None:
        raise PromiseProgressWorkbookPreviewError(f"invalid cell address {cell!r}")
    return match.group(1), int(match.group(2))


def _cell_sort_key(cell: str) -> tuple[int, int]:
    column, row = _cell_parts(cell)
    return row, _column_number(column)


def _column_name(number: int) -> str:
    value = ""
    while number:
        number, remainder = divmod(number - 1, 26)
        value = chr(65 + remainder) + value
    return value


def _expand_range(value: str) -> tuple[str, ...]:
    if ":" not in value:
        _cell_parts(value)
        return (value,)
    start, end = value.split(":", 1)
    start_column, start_row = _cell_parts(start)
    end_column, end_row = _cell_parts(end)
    start_number = _column_number(start_column)
    end_number = _column_number(end_column)
    if start_number > end_number or start_row > end_row:
        raise PromiseProgressWorkbookPreviewError(f"reversed cell range {value!r}")
    return tuple(
        f"{_column_name(column)}{row}"
        for row in range(start_row, end_row + 1)
        for column in range(start_number, end_number + 1)
    )


def _merged_child_map(merged_ranges: Sequence[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for merged_range in merged_ranges:
        cells = _expand_range(merged_range)
        for cell in cells[1:]:
            result[cell] = cells[0]
    return result


def _writable_anchor_set(allowlist: Mapping[str, Any], merged_ranges: Sequence[str]) -> frozenset[str]:
    merged_children = _merged_child_map(merged_ranges)
    ranges = (
        tuple(allowlist["writable_value_cells"])
        + tuple(allowlist["writable_note_source_anchors"])
        + tuple(allowlist["writable_hidden_trace_cells"])
    )
    return frozenset(
        cell
        for item in ranges
        for cell in _expand_range(str(item))
        if cell not in merged_children
    )


@dataclass(frozen=True)
class WorkbookBinding:
    binding_id: str
    binding_kind: str
    product_id: str
    product_contract_id: str
    block_id: str | None
    source_row_ids: tuple[str, ...]
    source_field_ids: tuple[str, ...]
    field_role: str
    sheet_name: str
    anchor_cell: str
    display_range: str
    legacy_anchor_cell: str
    display_type: str
    value_form: str
    canonical_display_text: str
    presentation_text: str
    display_transform_id: str
    display_transform_version: str
    canonical_text_digest: str
    lineage_full_text_digest: str
    machine_value: Any
    status_code: str | None
    style_role: str | None
    wrap_text: bool
    horizontal_alignment: str
    vertical_alignment: str
    source_document_ids: tuple[str, ...]
    source_occurrence_ids: tuple[str, ...]
    review_issue_ids: tuple[str, ...]
    lineage_digests: tuple[str, ...]
    fit_measurement: Mapping[str, Any]

    @property
    def display_text(self) -> str:
        """Compatibility alias for the mechanically written presentation text."""

        return self.presentation_text

    def to_dict(self) -> dict[str, Any]:
        return _canonical(dataclasses.asdict(self))


@dataclass(frozen=True)
class PromiseProgressWorkbookBindingPlan:
    product_id: str
    product_contract_id: str
    company_id: str
    ui_as_of_date: str
    template_oracle_sha256: str
    design_lock_manifest_sha256: str
    sheet_name: str
    sheet_position_1_based: int
    sheet_part: str
    presentation_contract: PromiseProgressWorkbookPresentationContract
    bindings: tuple[WorkbookBinding, ...]
    clear_destinations: tuple[str, ...]
    row_heights: tuple[tuple[int, int], ...]
    schema_id: str = BINDING_PLAN_SCHEMA_ID

    def payload_without_digest(self) -> dict[str, Any]:
        return {
            "schema_id": self.schema_id,
            "product_id": self.product_id,
            "product_contract_id": self.product_contract_id,
            "company_id": self.company_id,
            "ui_as_of_date": self.ui_as_of_date,
            "template_oracle_sha256": self.template_oracle_sha256,
            "design_lock_manifest_sha256": self.design_lock_manifest_sha256,
            "sheet_name": self.sheet_name,
            "sheet_position_1_based": self.sheet_position_1_based,
            "sheet_part": self.sheet_part,
            "presentation_contract": self.presentation_contract.to_dict(),
            "bindings": [binding.to_dict() for binding in self.bindings],
            "clear_destinations": list(self.clear_destinations),
            "row_heights": [
                {"row": row_number, "height_points": height}
                for row_number, height in self.row_heights
            ],
        }

    @property
    def lineage_digest(self) -> str:
        return _sha256_bytes(canonical_json_bytes(self.payload_without_digest()))

    def to_dict(self) -> dict[str, Any]:
        return {**self.payload_without_digest(), "lineage_digest": self.lineage_digest}


def _stable_id_parts(value: str) -> dict[str, str]:
    parts: dict[str, str] = {}
    for atom in value.split("|")[1:]:
        if "=" not in atom:
            continue
        key, raw = atom.split("=", 1)
        parts[key] = unquote(raw)
    return parts


def _display_number(value: Any) -> str:
    number = Decimal(str(value))
    if number == number.to_integral_value():
        return str(int(number))
    return format(number.normalize(), "f")


def _store_progress_presentation(machine_value: Any) -> str:
    if not isinstance(machine_value, (list, tuple)):
        raise PromiseProgressWorkbookPreviewError("store-progress-lines@1 requires structured components")
    values: dict[str, Decimal] = {}
    for component in machine_value:
        if not isinstance(component, Mapping):
            raise PromiseProgressWorkbookPreviewError("store-progress-lines@1 components must be records")
        label = str(component.get("label", "")).strip().casefold()
        record_id = unquote(unquote(str(component.get("record_id", "")))).casefold()
        if label in {"openings", "closures"}:
            key = label
        elif "metric:retail:net-store-openings@1" in record_id:
            key = "net"
        else:
            raise PromiseProgressWorkbookPreviewError("store-progress-lines@1 received an unknown component")
        if key in values:
            raise PromiseProgressWorkbookPreviewError("store-progress-lines@1 received a duplicate component")
        values[key] = Decimal(str(component.get("value")))
    if set(values) != {"openings", "closures", "net"}:
        raise PromiseProgressWorkbookPreviewError("store-progress-lines@1 requires openings, closures, and net")
    return (
        f"{_display_number(values['openings'])} openings / "
        f"{_display_number(abs(values['closures']))} closures\n"
        f"Net: {_display_number(values['net'])}"
    )


_DOCUMENT_TYPE_LABELS = {
    "earnings-release": "release",
    "earnings-transcript": "transcript",
    "business-update": "business update",
    "investor-presentation": "investor presentation",
    "sec-filing": "SEC filing",
    "filed-exhibit": "filed exhibit",
}
_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")


def _typed_source_phrase(document_ids: Sequence[str], *, include_dates: bool) -> str:
    if not document_ids:
        return ""
    grouped: dict[str, list[date]] = {}
    for document_id in document_ids:
        atoms = _stable_id_parts(document_id)
        document_type = atoms.get("type")
        publication = atoms.get("pub")
        if document_type not in _DOCUMENT_TYPE_LABELS or publication is None:
            raise PromiseProgressWorkbookPreviewError(
                f"source-summary@1 cannot summarize unsupported source identity {document_id!r}"
            )
        grouped.setdefault(document_type, []).append(date.fromisoformat(publication))
    phrases = []
    for document_type, dates in sorted(grouped.items(), key=lambda pair: min(pair[1])):
        unique_dates = sorted(set(dates))
        label = _DOCUMENT_TYPE_LABELS[document_type]
        if not include_dates:
            phrases.append(label if len(unique_dates) == 1 else f"{label}s")
        elif len(unique_dates) == 1:
            item = unique_dates[0]
            phrases.append(f"{_MONTHS[item.month - 1]} {item.day} {label}")
        else:
            first, last = unique_dates[0], unique_dates[-1]
            include_year = first.year != last.year
            first_text = f"{_MONTHS[first.month - 1]} {first.day}" + (f" {first.year}" if include_year else "")
            last_text = f"{_MONTHS[last.month - 1]} {last.day}" + (f" {last.year}" if include_year else "")
            phrases.append(f"{first_text}-{last_text} {label}s ({len(unique_dates)})")
    return " + ".join(phrases)


def _source_summary_presentation(field: Any, row_fields: Sequence[Any]) -> str:
    source_phrase = _typed_source_phrase(
        tuple(field.source_document_ids),
        include_dates=field.block_id != TIMELINE_BLOCK_ID,
    )
    status_field = next((item for item in row_fields if item.field_role == "status"), None)
    progress_field = next((item for item in row_fields if item.field_role == "progress"), None)
    qualifiers: list[str] = []
    review_rules = {
        _stable_id_parts(str(issue)).get("rule") for issue in field.review_issue_ids
    }
    review_rules.discard(None)
    if review_rules:
        if review_rules == {"promise-approximate-tolerance-missing"}:
            qualifiers.append("Needs Review: tolerance not disclosed")
        else:
            raise PromiseProgressWorkbookPreviewError(
                f"source-summary@1 cannot summarize review rules {sorted(review_rules)!r}"
            )
    elif field.selection_or_calculation_method_id == "projection:promise-progress:diagnostic-coverage-gap@1":
        qualifiers.append("Needs Review: no compatible GuidanceSeries or FY Actual; legacy parity only")
    elif status_field is not None and status_field.display_value.display_text == "Needs Review":
        qualifiers.append("Needs Review: compatible Actual unavailable")
    elif status_field is not None and status_field.display_value.display_text == "Open":
        qualifiers.append("Current guidance")
    elif status_field is not None and status_field.display_value.display_text:
        qualifiers.append(status_field.display_value.display_text)
    if progress_field is not None and progress_field.selection_or_calculation_method_id == PROGRESS_RUN_RATE_ID:
        qualifiers.append("Run rate is Progress, not realized savings")
    if not source_phrase and not qualifiers:
        raise PromiseProgressWorkbookPreviewError("source-summary@1 has no structured source or qualifier")
    return " · ".join(item for item in (source_phrase, *qualifiers) if item)


@lru_cache(maxsize=1)
def _pinned_font() -> Any:
    if not _PINNED_FONT_PATH.is_file():
        raise PromiseProgressWorkbookPreviewError(f"pinned fit font is missing: {_PINNED_FONT_PATH}")
    pixel_size = round(_PINNED_FONT_SIZE_POINTS * _PINNED_DPI / 72)
    return ImageFont.truetype(str(_PINNED_FONT_PATH), pixel_size)


def _excel_width_pixels(width: float) -> int:
    """Deterministic ECMA-compatible approximation for Calibri workbook widths."""

    return math.floor(((256 * float(width) + math.floor(128 / 7)) / 256) * 7) + 5


def _text_width_pixels(value: str) -> float:
    return float(_pinned_font().getlength(value))


def _wrapped_lines(value: str, available_width_pixels: int) -> tuple[str, ...]:
    if value == "":
        return ("",)
    lines: list[str] = []
    for paragraph in value.split("\n"):
        words = paragraph.split()
        if not words:
            lines.append("")
            continue
        current = ""
        for word in words:
            candidate = word if not current else f"{current} {word}"
            if _text_width_pixels(candidate) <= available_width_pixels:
                current = candidate
                continue
            if current:
                lines.append(current)
                current = ""
            if _text_width_pixels(word) <= available_width_pixels:
                current = word
                continue
            fragment = ""
            for character in word:
                candidate_fragment = fragment + character
                if fragment and _text_width_pixels(candidate_fragment) > available_width_pixels:
                    lines.append(fragment)
                    fragment = character
                else:
                    fragment = candidate_fragment
            current = fragment
        lines.append(current)
    return tuple(lines)


def measure_presentation_text(
    text: str,
    *,
    span_width: int,
    wrap_text: bool,
    allocated_height_points: int | None = None,
    excel_widths: Sequence[float] | None = None,
) -> dict[str, Any]:
    if span_width < 1:
        raise PromiseProgressWorkbookPreviewError("layout span width must be positive")
    resolved_widths = (
        tuple(float(value) for value in excel_widths)
        if excel_widths is not None
        else (24.0,) * span_width
    )
    if len(resolved_widths) != span_width or any(value <= 0 for value in resolved_widths):
        raise PromiseProgressWorkbookPreviewError(
            "layout measurement requires one positive Excel width per spanned column"
        )
    allocated_width_pixels = sum(_excel_width_pixels(value) for value in resolved_widths) - 12
    lines = _wrapped_lines(text, allocated_width_pixels) if wrap_text else tuple(text.split("\n"))
    bbox = _pinned_font().getbbox("Ag")
    line_height_pixels = max(1, bbox[3] - bbox[1] + 3)
    required_width_pixels = max((_text_width_pixels(line) for line in lines), default=0.0)
    required_height_pixels = len(lines) * line_height_pixels + 6
    allocated_height_pixels = None if allocated_height_points is None else allocated_height_points * _PINNED_DPI / 72
    width_fits = wrap_text or required_width_pixels <= allocated_width_pixels
    height_fits = allocated_height_pixels is None or required_height_pixels <= allocated_height_pixels
    return {
        "measurement_method": "pillow-freetype-calibri11@1",
        "font_name": _PINNED_FONT_NAME,
        "font_size_points": _PINNED_FONT_SIZE_POINTS,
        "font_dpi": _PINNED_DPI,
        "font_file_sha256": sha256_file(_PINNED_FONT_PATH),
        "span_width_units": span_width,
        "excel_widths": list(resolved_widths),
        "effective_width_pixels": allocated_width_pixels,
        "wrap_text": wrap_text,
        "line_count": len(lines),
        "required_width_pixels": round(required_width_pixels, 3),
        "required_height_pixels": required_height_pixels,
        "allocated_height_points": allocated_height_points,
        "allocated_height_pixels": None if allocated_height_pixels is None else round(allocated_height_pixels, 3),
        "width_fits": width_fits,
        "height_fits": height_fits,
        "fit": width_fits and height_fits,
    }


def plan_presentation_row(
    contract: PromiseProgressWorkbookPresentationContract,
    *,
    block_id: str,
    values_by_role: Mapping[str, str],
) -> dict[str, Any]:
    """Plan one synthetic or real presentation row without company-specific logic."""

    unknown = set(values_by_role) - {
        row.field_role for row in contract.field_layouts if row.block_id == block_id
    }
    if unknown:
        raise PromiseProgressWorkbookPreviewError(f"unknown presentation roles: {sorted(unknown)!r}")
    measurements: dict[str, dict[str, Any]] = {}
    for role, text in values_by_role.items():
        layout = contract.layout_for(block_id, role)
        if layout.wrap_mode == "always":
            wrap = True
        elif layout.wrap_mode == "never":
            wrap = False
        else:
            wrap = not measure_presentation_text(
                text, span_width=layout.span_width, wrap_text=False
            )["width_fits"]
        measurements[role] = measure_presentation_text(
            text, span_width=layout.span_width, wrap_text=wrap
        )
    if any(not row["width_fits"] for row in measurements.values()):
        raise PromiseProgressWorkbookPreviewError("layout_capacity_exceeded: synthetic row width")
    required_pixels = max(
        (row["required_height_pixels"] for row in measurements.values()), default=0
    )
    maximum = contract.timeline_max_height if block_id == TIMELINE_BLOCK_ID else contract.other_block_max_height
    selected = next(
        (
            tier for tier in contract.row_height_tiers
            if tier <= maximum and tier * _PINNED_DPI / 72 >= required_pixels
        ),
        None,
    )
    if selected is None:
        raise PromiseProgressWorkbookPreviewError(
            f"layout_capacity_exceeded: synthetic row requires {required_pixels}px above {maximum}pt"
        )
    return {
        "block_id": block_id,
        "height_points": selected,
        "measurements": measurements,
        "company_specific_branch": False,
    }


def _resolve_wrap(layout: PresentationFieldLayout, field: Any, presentation_text: str) -> bool:
    if layout.wrap_mode == "always":
        return True
    if layout.wrap_mode == "never" or not presentation_text:
        return False
    if layout.wrap_mode == "qualitative":
        if field.display_value.value_form == "qualitative" or "\n" in presentation_text:
            return True
    elif layout.wrap_mode == "composite":
        if isinstance(field.display_value.machine_value, (tuple, list, Mapping)) or "\n" in presentation_text:
            return True
    else:
        raise PromiseProgressWorkbookPreviewError(f"unknown wrap mode {layout.wrap_mode!r}")
    return not measure_presentation_text(
        presentation_text, span_width=layout.span_width, wrap_text=False
    )["width_fits"]


def _field_binding(
    product: PromiseProgressProduct,
    field: Any,
    row_fields: Sequence[Any],
    status_by_id: Mapping[str, Any],
    contract: PromiseProgressWorkbookPresentationContract,
) -> WorkbookBinding:
    status_code: str | None = None
    style_role: str | None = None
    if field.field_role == "status":
        if field.status_assessment_id is None:
            if field.display_value.display_text != "Needs Review" or field.lineage_state not in {"diagnostic", "needs-review"}:
                raise PromiseProgressWorkbookPreviewError(
                    f"unassessed Status field {field.field_id} is not an explicit product-owned Needs Review diagnostic"
                )
            status_code = "needs_review"
            style_role = "status:needs_review"
        status = status_by_id.get(field.status_assessment_id) if field.status_assessment_id is not None else None
        if status is None and field.status_assessment_id is None:
            status = None
        elif status is None:
            raise PromiseProgressWorkbookPreviewError(
                f"field {field.field_id} references unknown Status assessment {field.status_assessment_id}"
            )
        if status is not None:
            status_code = status.status_code
            style_role = f"status:{status_code}"
            if field.display_value.display_text != status.visible_label:
                raise PromiseProgressWorkbookPreviewError(
                    f"field {field.field_id} does not display its source-native Status label"
                )
    layout = contract.layout_for(field.block_id, field.field_role)
    _, row_number = _cell_parts(field.anchor_cell)
    canonical_text = field.display_value.display_text
    if field.field_role in {"notes_source", "source_note"}:
        transform_id = SOURCE_SUMMARY_TRANSFORM_ID
        presentation_text = _source_summary_presentation(field, row_fields)
    elif field.field_role in {"actual", "progress"} and isinstance(field.display_value.machine_value, (tuple, list)):
        transform_id = STORE_PROGRESS_TRANSFORM_ID
        presentation_text = _store_progress_presentation(field.display_value.machine_value)
    else:
        transform_id = IDENTITY_TRANSFORM_ID
        presentation_text = canonical_text
    wrap_text = _resolve_wrap(layout, field, presentation_text)
    canonical_digest = _sha256_bytes(canonical_text.encode("utf-8"))
    full_text_digest = _sha256_bytes(canonical_json_bytes({
        "canonical_display_text": canonical_text,
        "source_document_ids": list(field.source_document_ids),
        "source_occurrence_ids": list(field.source_occurrence_ids),
        "review_issue_ids": list(field.review_issue_ids),
        "lineage_digest": field.lineage_digest,
    }))
    return WorkbookBinding(
        binding_id=f"binding:promise-progress:{field.field_id}@1",
        binding_kind="product_field",
        product_id=product.product_id,
        product_contract_id=product.product_contract_id,
        block_id=field.block_id,
        source_row_ids=(field.row_id,),
        source_field_ids=(field.field_id,),
        field_role=field.field_role,
        sheet_name=SHEET_NAME,
        anchor_cell=f"{layout.start_column}{row_number}",
        display_range=layout.range_for_row(row_number),
        legacy_anchor_cell=field.anchor_cell,
        display_type="inline_string",
        value_form=field.display_value.value_form,
        canonical_display_text=canonical_text,
        presentation_text=presentation_text,
        display_transform_id=transform_id,
        display_transform_version=_DISPLAY_TRANSFORM_VERSION,
        canonical_text_digest=canonical_digest,
        lineage_full_text_digest=full_text_digest,
        machine_value=_canonical(field.display_value.machine_value),
        status_code=status_code,
        style_role=style_role,
        wrap_text=wrap_text,
        horizontal_alignment=layout.horizontal_alignment,
        vertical_alignment=layout.vertical_alignment if wrap_text or layout.vertical_alignment == "center" else "preserve",
        source_document_ids=tuple(field.source_document_ids),
        source_occurrence_ids=tuple(field.source_occurrence_ids),
        review_issue_ids=tuple(field.review_issue_ids),
        lineage_digests=(field.lineage_digest,),
        fit_measurement=measure_presentation_text(
            presentation_text, span_width=layout.span_width, wrap_text=wrap_text
        ),
    )


def _auxiliary_binding(
    *,
    binding_id: str,
    binding_kind: str,
    product: PromiseProgressProduct,
    block_id: str | None,
    source_row_ids: tuple[str, ...],
    source_field_ids: tuple[str, ...],
    field_role: str,
    anchor_cell: str,
    display_range: str,
    presentation_text: str,
    machine_value: Any,
    lineage_digests: tuple[str, ...],
    display_type: str = "inline_string",
    value_form: str = "heading",
    wrap_text: bool = False,
) -> WorkbookBinding:
    canonical_digest = _sha256_bytes(presentation_text.encode("utf-8"))
    column_start, _ = _cell_parts(anchor_cell)
    end_cell = display_range.split(":")[-1]
    column_end, _ = _cell_parts(end_cell)
    visible = _column_number(column_start) <= 12
    span_width = _column_number(column_end) - _column_number(column_start) + 1
    fit = (
        measure_presentation_text(presentation_text, span_width=span_width, wrap_text=wrap_text)
        if visible else {"visible": False, "fit": True}
    )
    return WorkbookBinding(
        binding_id=binding_id,
        binding_kind=binding_kind,
        product_id=product.product_id,
        product_contract_id=product.product_contract_id,
        block_id=block_id,
        source_row_ids=source_row_ids,
        source_field_ids=source_field_ids,
        field_role=field_role,
        sheet_name=SHEET_NAME,
        anchor_cell=anchor_cell,
        display_range=display_range,
        legacy_anchor_cell=anchor_cell,
        display_type=display_type,
        value_form=value_form,
        canonical_display_text=presentation_text,
        presentation_text=presentation_text,
        display_transform_id=IDENTITY_TRANSFORM_ID,
        display_transform_version=_DISPLAY_TRANSFORM_VERSION,
        canonical_text_digest=canonical_digest,
        lineage_full_text_digest=_sha256_bytes(canonical_json_bytes({
            "canonical_display_text": presentation_text,
            "lineage_digests": list(lineage_digests),
        })),
        machine_value=_canonical(machine_value),
        status_code=None,
        style_role=None,
        wrap_text=wrap_text,
        horizontal_alignment="left" if visible else "preserve",
        vertical_alignment="top" if wrap_text else "preserve",
        source_document_ids=(),
        source_occurrence_ids=(),
        review_issue_ids=(),
        lineage_digests=lineage_digests,
        fit_measurement=fit,
    )


def _v2_clear_destinations() -> tuple[str, ...]:
    cells = {f"{column}2" for column in "ABCDEFGHIJKL"}
    for rows in _BLOCK_HEADER_ROWS.values():
        for row_number in rows:
            cells.update(f"{column}{row_number}" for column in "ABCDEFGHIJKL")
    for rows in _BLOCK_CAPACITY_ROWS.values():
        for row_number in rows:
            cells.update(f"{column}{row_number}" for column in "ABCDEFGHIJKL")
            cells.add(f"O{row_number}")
    for header_cell, _ in _TIMELINE_HEADER_GROUPS:
        _, row_number = _cell_parts(header_cell)
        cells.update(f"{column}{row_number}" for column in "ABCDEFGHIJKL")
    return tuple(sorted(cells, key=_cell_sort_key))


def _finalize_row_heights(
    bindings: Sequence[WorkbookBinding],
    contract: PromiseProgressWorkbookPresentationContract,
) -> tuple[tuple[WorkbookBinding, ...], tuple[tuple[int, int], ...]]:
    row_to_block = {
        row_number: block_id
        for block_id, rows in _BLOCK_CAPACITY_ROWS.items()
        for row_number in rows
    }
    height_by_row: dict[int, int] = {}
    for row_number, block_id in row_to_block.items():
        row_bindings = [
            binding for binding in bindings
            if _cell_parts(binding.anchor_cell)[1] == row_number
            and _column_number(_cell_parts(binding.anchor_cell)[0]) <= 12
        ]
        required_pixels = max(
            (int(binding.fit_measurement.get("required_height_pixels", 0)) for binding in row_bindings),
            default=0,
        )
        if any(not bool(binding.fit_measurement.get("width_fits", True)) for binding in row_bindings):
            raise PromiseProgressWorkbookPreviewError(
                f"layout_capacity_exceeded: non-wrapped width overflow on row {row_number}"
            )
        maximum = contract.timeline_max_height if block_id == TIMELINE_BLOCK_ID else contract.other_block_max_height
        selected = next(
            (
                tier for tier in contract.row_height_tiers
                if tier <= maximum and tier * _PINNED_DPI / 72 >= required_pixels
            ),
            None,
        )
        if selected is None:
            raise PromiseProgressWorkbookPreviewError(
                f"layout_capacity_exceeded: row {row_number} requires {required_pixels}px above {maximum}pt"
            )
        height_by_row[row_number] = selected
    updated: list[WorkbookBinding] = []
    for binding in bindings:
        _, row_number = _cell_parts(binding.anchor_cell)
        allocated = height_by_row.get(row_number)
        if allocated is None or binding.fit_measurement.get("visible") is False:
            updated.append(binding)
            continue
        column_start, _ = _cell_parts(binding.anchor_cell)
        column_end, _ = _cell_parts(binding.display_range.split(":")[-1])
        measurement = measure_presentation_text(
            binding.presentation_text,
            span_width=_column_number(column_end) - _column_number(column_start) + 1,
            wrap_text=binding.wrap_text,
            allocated_height_points=allocated,
        )
        if not measurement["fit"]:
            raise PromiseProgressWorkbookPreviewError(
                f"layout_capacity_exceeded: binding {binding.binding_id} does not fit row {row_number}"
            )
        updated.append(dataclasses.replace(binding, fit_measurement=measurement))
    return tuple(updated), tuple(sorted(height_by_row.items()))


def build_promise_progress_workbook_binding_plan(
    product: PromiseProgressProduct,
    *,
    design_lock_root: Path,
) -> PromiseProgressWorkbookBindingPlan:
    issues = validate_promise_progress_product(product, replay=False)
    if issues:
        raise PromiseProgressWorkbookPreviewError(
            f"source-native product validation failed before binding: {issues!r}"
        )
    if product.product_contract_id != PRODUCT_CONTRACT_ID:
        raise PromiseProgressWorkbookPreviewError("unsupported Promise Progress product contract")
    design_lock = verify_design_lock(design_lock_root)
    structural = load_json_strict(design_lock_root / "promise_progress_structural_parity_contract.json")
    contract = _presentation_contract_v2(design_lock_root)
    status_by_id = {status.status_assessment_id: status for status in product.status_assessments}
    rows_by_id = {row.row_id: row for row in product.ordered_rows}

    bindings: list[WorkbookBinding] = [
        _auxiliary_binding(
            binding_id="binding:promise-progress:product-metadata:A2@1",
            binding_kind="product_metadata",
            product=product,
            block_id=None,
            source_row_ids=(),
            source_field_ids=(),
            field_role="product_as_of",
            anchor_cell="A2",
            display_range="A2:L2",
            value_form="metadata",
            presentation_text=(
                f"Source-native Promise Progress | {product.company_id} | "
                f"As of {product.ui_as_of_date} | Preview only"
            ),
            machine_value={"company_id": product.company_id, "ui_as_of_date": product.ui_as_of_date},
            lineage_digests=(_sha256_bytes(serialize_promise_progress_product(product)),),
            wrap_text=False,
        )
    ]
    bindings.extend(
        _field_binding(
            product,
            field,
            rows_by_id[field.row_id].fields,
            status_by_id,
            contract,
        )
        for field in product.fields
    )

    for row in product.ordered_rows:
        bindings.append(
            _auxiliary_binding(
                binding_id=f"binding:promise-progress:{row.row_id}:row-trace@1",
                binding_kind="row_trace",
                product=product,
                block_id=row.block_id,
                source_row_ids=(row.row_id,),
                source_field_ids=(),
                field_role="row_id",
                anchor_cell=f"O{row.visible_sheet_row}",
                display_range=f"O{row.visible_sheet_row}",
                display_type="row_id",
                value_form="row_id",
                presentation_text=row.row_id,
                machine_value=row.row_id,
                lineage_digests=(row.lineage_digest,),
            )
        )

    rows_by_visible = {row.visible_sheet_row: row for row in product.ordered_rows}
    for header_cell, data_rows in _TIMELINE_HEADER_GROUPS:
        group_rows = tuple(rows_by_visible[row_number] for row_number in data_rows if row_number in rows_by_visible)
        if not group_rows:
            continue
        stated_in_fields = tuple(
            field
            for row in group_rows
            for field in row.fields
            if field.field_role == "stated_in"
        )
        labels = {field.display_value.display_text for field in stated_in_fields if field.display_value.display_text}
        if not labels or len(stated_in_fields) != len(group_rows):
            raise PromiseProgressWorkbookPreviewError(
                f"timeline header {header_cell} does not resolve from its closed stated_in product fields"
            )
        label = " / ".join(sorted(labels))
        bindings.append(
            _auxiliary_binding(
                binding_id=f"binding:promise-progress:timeline-group:{header_cell}@1",
                binding_kind="timeline_group_header",
                product=product,
                block_id=group_rows[0].block_id,
                source_row_ids=tuple(row.row_id for row in group_rows),
                source_field_ids=tuple(field.field_id for field in stated_in_fields),
                field_role="timeline_group_header",
                anchor_cell=header_cell,
                display_range=f"{header_cell}:L{_cell_parts(header_cell)[1]}",
                value_form="heading",
                presentation_text=f"{label} revisions",
                machine_value=label,
                lineage_digests=tuple(field.lineage_digest for field in stated_in_fields),
            )
        )

    ordered_bindings, row_heights = _finalize_row_heights(
        tuple(sorted(bindings, key=lambda binding: (_cell_sort_key(binding.anchor_cell), binding.binding_id))),
        contract,
    )
    clear_destinations = _v2_clear_destinations()
    plan = PromiseProgressWorkbookBindingPlan(
        product_id=product.product_id,
        product_contract_id=product.product_contract_id,
        company_id=product.company_id,
        ui_as_of_date=product.ui_as_of_date,
        template_oracle_sha256=product.template_oracle_sha256,
        design_lock_manifest_sha256=design_lock["manifest_sha256"],
        sheet_name=SHEET_NAME,
        sheet_position_1_based=int(structural["oracle"]["sheet_position_1_based"]),
        sheet_part=str(structural["oracle"]["sheet_part"]),
        presentation_contract=contract,
        bindings=ordered_bindings,
        clear_destinations=clear_destinations,
        row_heights=row_heights,
    )
    validate_promise_progress_workbook_binding_plan(product, plan, design_lock_root=design_lock_root)
    return plan


def validate_promise_progress_workbook_binding_plan(
    product: PromiseProgressProduct,
    plan: PromiseProgressWorkbookBindingPlan,
    *,
    design_lock_root: Path,
) -> None:
    contract = _presentation_contract_v2(design_lock_root)
    merged_children = _merged_child_map(contract.permitted_merges)
    writable = set(_v2_clear_destinations())
    expected_fields = {field.field_id: field for field in product.fields}
    expected_rows = {row.row_id: row for row in product.ordered_rows}
    row_fields = {row.row_id: row.fields for row in product.ordered_rows}
    binding_ids = [binding.binding_id for binding in plan.bindings]
    destinations = [binding.anchor_cell for binding in plan.bindings]

    if plan.schema_id != BINDING_PLAN_SCHEMA_ID:
        raise PromiseProgressWorkbookPreviewError("unsupported workbook binding plan contract")
    if plan.presentation_contract.to_dict() != contract.to_dict():
        raise PromiseProgressWorkbookPreviewError("binding plan presentation contract differs from reviewed v2")
    if (
        plan.product_id != product.product_id
        or plan.product_contract_id != product.product_contract_id
        or plan.company_id != product.company_id
        or plan.ui_as_of_date != product.ui_as_of_date
    ):
        raise PromiseProgressWorkbookPreviewError("binding plan product identity differs from the immutable product")
    if plan.template_oracle_sha256 != product.template_oracle_sha256:
        raise PromiseProgressWorkbookPreviewError("binding plan template oracle differs from the immutable product")
    if plan.sheet_name != SHEET_NAME:
        raise PromiseProgressWorkbookPreviewError("binding plan targets an unsupported sheet")
    if len(binding_ids) != len(set(binding_ids)):
        raise PromiseProgressWorkbookPreviewError("binding plan contains duplicate binding identities")
    if len(destinations) != len(set(destinations)):
        raise PromiseProgressWorkbookPreviewError("binding plan contains duplicate workbook destinations")
    if set(plan.clear_destinations) != writable or len(plan.clear_destinations) != len(writable):
        raise PromiseProgressWorkbookPreviewError("binding plan clear scope differs from PresentationContract@2")
    expected_height_rows = {
        row for rows in _BLOCK_CAPACITY_ROWS.values() for row in rows
    }
    if (
        {row for row, _ in plan.row_heights} != expected_height_rows
        or len(plan.row_heights) != len(expected_height_rows)
        or any(height not in contract.row_height_tiers for _, height in plan.row_heights)
    ):
        raise PromiseProgressWorkbookPreviewError("binding plan row heights differ from the closed tier contract")

    field_bindings = [binding for binding in plan.bindings if binding.binding_kind == "product_field"]
    field_binding_ids = [binding.source_field_ids[0] for binding in field_bindings if len(binding.source_field_ids) == 1]
    if len(field_bindings) != len(expected_fields) or set(field_binding_ids) != set(expected_fields):
        raise PromiseProgressWorkbookPreviewError("binding plan does not cover every product field exactly once")
    row_bindings = [binding for binding in plan.bindings if binding.binding_kind == "row_trace"]
    row_binding_ids = [binding.source_row_ids[0] for binding in row_bindings if len(binding.source_row_ids) == 1]
    if len(row_bindings) != len(expected_rows) or set(row_binding_ids) != set(expected_rows):
        raise PromiseProgressWorkbookPreviewError("binding plan does not cover every row trace exactly once")
    metadata = [binding for binding in plan.bindings if binding.binding_kind == "product_metadata"]
    if len(metadata) != 1 or metadata[0].anchor_cell != "A2":
        raise PromiseProgressWorkbookPreviewError("binding plan must contain the exact deterministic A2 product metadata binding")

    status_by_id = {status.status_assessment_id: status for status in product.status_assessments}
    for binding in plan.bindings:
        if binding.sheet_name != SHEET_NAME or binding.anchor_cell not in writable:
            raise PromiseProgressWorkbookPreviewError(
                f"binding {binding.binding_id} targets an unreviewed destination {binding.anchor_cell}"
            )
        if binding.anchor_cell in merged_children:
            raise PromiseProgressWorkbookPreviewError(f"binding {binding.binding_id} targets a merged child cell")
        column, _ = _cell_parts(binding.anchor_cell)
        if column in {"M", "N"}:
            raise PromiseProgressWorkbookPreviewError("M and N are reserved blank and never writable")
        if column == "O" and binding.binding_kind != "row_trace":
            raise PromiseProgressWorkbookPreviewError("column O may contain only stable source-native row IDs")
        if binding.display_type not in {"inline_string", "row_id"}:
            raise PromiseProgressWorkbookPreviewError(
                f"binding {binding.binding_id} has unsupported display type {binding.display_type!r}"
            )
        if binding.display_transform_id not in contract.transform_ids:
            raise PromiseProgressWorkbookPreviewError(
                f"binding {binding.binding_id} uses unknown display transform {binding.display_transform_id!r}"
            )
        if binding.display_transform_version != _DISPLAY_TRANSFORM_VERSION:
            raise PromiseProgressWorkbookPreviewError("unsupported display-transform version")
        if binding.canonical_text_digest != _sha256_bytes(binding.canonical_display_text.encode("utf-8")):
            raise PromiseProgressWorkbookPreviewError("binding canonical display digest is invalid")
        if binding.display_transform_id == IDENTITY_TRANSFORM_ID and binding.presentation_text != binding.canonical_display_text:
            raise PromiseProgressWorkbookPreviewError("identity@1 cannot change source-native display text")
        if not bool(binding.fit_measurement.get("fit", False)):
            raise PromiseProgressWorkbookPreviewError(
                f"layout_capacity_exceeded: binding {binding.binding_id} is not measured to fit"
            )
        if binding.binding_kind == "product_field":
            if len(binding.source_field_ids) != 1 or len(binding.source_row_ids) != 1:
                raise PromiseProgressWorkbookPreviewError("a product-field binding must have one field and one row owner")
            field = expected_fields.get(binding.source_field_ids[0])
            if field is None or field.row_id != binding.source_row_ids[0]:
                raise PromiseProgressWorkbookPreviewError("a product-field binding references an unknown row or field")
            if (
                binding.block_id != field.block_id
                or binding.field_role != field.field_role
                or binding.value_form != field.display_value.value_form
                or binding.legacy_anchor_cell != field.anchor_cell
                or binding.canonical_display_text != field.display_value.display_text
                or _canonical(binding.machine_value) != _canonical(field.display_value.machine_value)
                or binding.lineage_digests != (field.lineage_digest,)
                or binding.source_document_ids != tuple(field.source_document_ids)
                or binding.source_occurrence_ids != tuple(field.source_occurrence_ids)
                or binding.review_issue_ids != tuple(field.review_issue_ids)
            ):
                raise PromiseProgressWorkbookPreviewError(
                    f"binding {binding.binding_id} changes source-native field semantics or destination"
                )
            layout = contract.layout_for(field.block_id, field.field_role)
            _, row_number = _cell_parts(field.anchor_cell)
            if (
                binding.anchor_cell != f"{layout.start_column}{row_number}"
                or binding.display_range != layout.range_for_row(row_number)
                or binding.horizontal_alignment != layout.horizontal_alignment
            ):
                raise PromiseProgressWorkbookPreviewError(
                    f"binding {binding.binding_id} differs from its reviewed v2 layout role"
                )
            expected_presentation = field.display_value.display_text
            expected_transform = IDENTITY_TRANSFORM_ID
            if field.field_role in {"notes_source", "source_note"}:
                expected_transform = SOURCE_SUMMARY_TRANSFORM_ID
                expected_presentation = _source_summary_presentation(field, row_fields[field.row_id])
            elif field.field_role in {"actual", "progress"} and isinstance(field.display_value.machine_value, (tuple, list)):
                expected_transform = STORE_PROGRESS_TRANSFORM_ID
                expected_presentation = _store_progress_presentation(field.display_value.machine_value)
            if (
                binding.display_transform_id != expected_transform
                or binding.presentation_text != expected_presentation
            ):
                raise PromiseProgressWorkbookPreviewError(
                    f"binding {binding.binding_id} display transform differs from the typed reviewed transform"
                )
            if binding.value_form not in _SUPPORTED_DISPLAY_FORMS:
                raise PromiseProgressWorkbookPreviewError(
                    f"binding {binding.binding_id} has unsupported source-native display form {binding.value_form!r}"
                )
            if field.field_role != "status":
                if binding.status_code is not None or binding.style_role is not None:
                    raise PromiseProgressWorkbookPreviewError("a non-Status field cannot acquire a Status style role")
            else:
                if field.status_assessment_id is None:
                    if (
                        field.display_value.display_text != "Needs Review"
                        or field.lineage_state not in {"diagnostic", "needs-review"}
                        or binding.status_code != "needs_review"
                        or binding.style_role != "status:needs_review"
                    ):
                        raise PromiseProgressWorkbookPreviewError(
                            "an unassessed Status field must remain the product-owned Needs Review diagnostic"
                        )
                    continue
                status = status_by_id.get(field.status_assessment_id)
                if status is None or binding.status_code != status.status_code or binding.style_role != f"status:{status.status_code}":
                    raise PromiseProgressWorkbookPreviewError("a Status binding differs from its source-native assessment")
        elif binding.binding_kind == "row_trace":
            if len(binding.source_row_ids) != 1 or binding.source_field_ids:
                raise PromiseProgressWorkbookPreviewError("a row trace must have exactly one row owner and no field owner")
            row = expected_rows.get(binding.source_row_ids[0])
            if row is None or binding.anchor_cell != f"O{row.visible_sheet_row}" or binding.presentation_text != row.row_id:
                raise PromiseProgressWorkbookPreviewError("a row trace differs from the source-native row identity")
        elif binding.binding_kind == "timeline_group_header":
            if not binding.source_row_ids or not binding.source_field_ids or binding.anchor_cell not in {item[0] for item in _TIMELINE_HEADER_GROUPS}:
                raise PromiseProgressWorkbookPreviewError("a timeline group header lacks complete source-native ownership")
            fields = [expected_fields.get(field_id) for field_id in binding.source_field_ids]
            if any(field is None or field.field_role != "stated_in" for field in fields):
                raise PromiseProgressWorkbookPreviewError("a timeline group header may derive only from stated_in product fields")
            labels = {field.display_value.display_text for field in fields if field is not None}
            label = " / ".join(sorted(labels))
            if not labels or binding.presentation_text != f"{label} revisions":
                raise PromiseProgressWorkbookPreviewError("a timeline group header changes its product-owned period label")
        elif binding.binding_kind != "product_metadata":
            raise PromiseProgressWorkbookPreviewError(f"unknown binding kind {binding.binding_kind!r}")


def _parse_xml(data: bytes) -> ET.Element:
    for _, namespace in ET.iterparse(io.BytesIO(data), events=("start-ns",)):
        prefix, uri = namespace
        if prefix != "xml":
            ET.register_namespace(prefix or "", uri)
    return ET.fromstring(data)


def _serialize_xml(root: ET.Element) -> bytes:
    return ET.tostring(root, encoding="utf-8", xml_declaration=True, short_empty_elements=True)


def _worksheet_cell_map(root: ET.Element) -> dict[str, ET.Element]:
    return {cell.get("r", ""): cell for cell in root.findall(f".//{{{_MAIN_NS}}}c")}


def _cell_text(cell: ET.Element | None, shared_strings: Sequence[str] = ()) -> str:
    if cell is None:
        return ""
    cell_type = cell.get("t")
    value = cell.find(f"{{{_MAIN_NS}}}v")
    inline = cell.find(f"{{{_MAIN_NS}}}is")
    if cell_type == "s" and value is not None and value.text is not None:
        return shared_strings[int(value.text)]
    if cell_type == "inlineStr" and inline is not None:
        return "".join(node.text or "" for node in inline.iter(f"{{{_MAIN_NS}}}t"))
    return "" if value is None or value.text is None else value.text


def _shared_strings(archive: ZipFile) -> tuple[str, ...]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return ()
    root = _parse_xml(archive.read("xl/sharedStrings.xml"))
    return tuple(
        "".join(node.text or "" for node in item.iter(f"{{{_MAIN_NS}}}t"))
        for item in root.findall(f"{{{_MAIN_NS}}}si")
    )


def _clear_cell(cell: ET.Element) -> None:
    for child in list(cell):
        if child.tag in {
            f"{{{_MAIN_NS}}}f",
            f"{{{_MAIN_NS}}}v",
            f"{{{_MAIN_NS}}}is",
        }:
            cell.remove(child)
    cell.attrib.pop("t", None)


def _write_inline_string(cell: ET.Element, value: str) -> None:
    _clear_cell(cell)
    if value == "":
        return
    cell.set("t", "inlineStr")
    inline = ET.SubElement(cell, f"{{{_MAIN_NS}}}is")
    text = ET.SubElement(inline, f"{{{_MAIN_NS}}}t")
    if value != value.strip() or "  " in value:
        text.set(f"{{{_XML_NS}}}space", "preserve")
    text.text = value


def _write_numeric_value(cell: ET.Element, value: str) -> None:
    _clear_cell(cell)
    Decimal(value)  # fail closed before mutating the cell representation
    cell.set("t", "n")
    node = ET.SubElement(cell, f"{{{_MAIN_NS}}}v")
    node.text = value


def _get_or_create_cell(root: ET.Element, cell_ref: str) -> ET.Element:
    cells = _worksheet_cell_map(root)
    existing = cells.get(cell_ref)
    if existing is not None:
        return existing
    _, row_number = _cell_parts(cell_ref)
    sheet_data = root.find(f"{{{_MAIN_NS}}}sheetData")
    if sheet_data is None:
        raise PromiseProgressWorkbookPreviewError("target worksheet has no sheetData")
    rows = list(sheet_data.findall(f"{{{_MAIN_NS}}}row"))
    row = next((item for item in rows if int(item.get("r", "0")) == row_number), None)
    if row is None:
        row = ET.Element(f"{{{_MAIN_NS}}}row", {"r": str(row_number)})
        insert_at = next((index for index, item in enumerate(rows) if int(item.get("r", "0")) > row_number), len(rows))
        sheet_data.insert(insert_at, row)
    cell = ET.Element(f"{{{_MAIN_NS}}}c", {"r": cell_ref})
    existing_cells = list(row.findall(f"{{{_MAIN_NS}}}c"))
    insert_at = next(
        (index for index, item in enumerate(existing_cells) if _cell_sort_key(item.get("r", "A1")) > _cell_sort_key(cell_ref)),
        len(existing_cells),
    )
    row.insert(insert_at, cell)
    return cell


def _resolve_target_sheet(archive: ZipFile, sheet_name: str) -> tuple[int, str]:
    workbook = _parse_xml(archive.read("xl/workbook.xml"))
    sheets = workbook.find(f"{{{_MAIN_NS}}}sheets")
    matches = [] if sheets is None else [sheet for sheet in sheets if sheet.get("name") == sheet_name]
    if len(matches) != 1:
        raise PromiseProgressWorkbookPreviewError(f"expected exactly one worksheet named {sheet_name!r}")
    relationship_id = matches[0].get(f"{{{_DOCUMENT_REL_NS}}}id")
    relationships = _parse_xml(archive.read("xl/_rels/workbook.xml.rels"))
    targets = [
        relationship.get("Target")
        for relationship in relationships.findall(f"{{{_PACKAGE_REL_NS}}}Relationship")
        if relationship.get("Id") == relationship_id
    ]
    if len(targets) != 1 or targets[0] is None:
        raise PromiseProgressWorkbookPreviewError("target worksheet relationship does not resolve exactly once")
    target = str(targets[0]).replace("\\", "/")
    if target.startswith("/"):
        part = target.lstrip("/")
    else:
        part = "xl/" + target.lstrip("/")
    while "/../" in part:
        left, right = part.split("/../", 1)
        part = left.rsplit("/", 1)[0] + "/" + right
    position = list(sheets).index(matches[0]) + 1
    return position, part


def _style_palette(archive: ZipFile) -> dict[int, str | None]:
    root = _parse_xml(archive.read("xl/styles.xml"))
    fills_node = root.find(f"{{{_MAIN_NS}}}fills")
    cell_xfs_node = root.find(f"{{{_MAIN_NS}}}cellXfs")
    fills = [] if fills_node is None else list(fills_node)
    cell_xfs = [] if cell_xfs_node is None else list(cell_xfs_node)
    palette: dict[int, str | None] = {}
    for index, xf in enumerate(cell_xfs):
        fill_id = int(xf.get("fillId", "0"))
        color: str | None = None
        if fill_id < len(fills):
            pattern = fills[fill_id].find(f"{{{_MAIN_NS}}}patternFill")
            foreground = None if pattern is None else pattern.find(f"{{{_MAIN_NS}}}fgColor")
            if foreground is not None and foreground.get("rgb"):
                color = "#" + str(foreground.get("rgb"))[-6:].upper()
        palette[index] = color
    return palette


def _style_font_properties(archive: ZipFile) -> dict[int, dict[str, Any]]:
    root = _parse_xml(archive.read("xl/styles.xml"))
    fonts_node = root.find(f"{{{_MAIN_NS}}}fonts")
    cell_xfs_node = root.find(f"{{{_MAIN_NS}}}cellXfs")
    fonts = [] if fonts_node is None else list(fonts_node)
    cell_xfs = [] if cell_xfs_node is None else list(cell_xfs_node)
    result: dict[int, dict[str, Any]] = {}
    for index, xf in enumerate(cell_xfs):
        font_id = int(xf.get("fontId", "0"))
        font = fonts[font_id] if 0 <= font_id < len(fonts) else None
        color = None if font is None else font.find(f"{{{_MAIN_NS}}}color")
        result[index] = {
            "font_id": font_id,
            "bold": font is not None and font.find(f"{{{_MAIN_NS}}}b") is not None,
            "rgb": (
                None
                if color is None or color.get("rgb") is None
                else "#" + str(color.get("rgb"))[-6:].upper()
            ),
        }
    return result


def _resolve_status_styles(
    archive: ZipFile,
    sheet_root: ET.Element,
    status_registry: Mapping[str, Any],
    required_status_codes: Iterable[str],
) -> dict[str, int]:
    shared = _shared_strings(archive)
    cells = _worksheet_cell_map(sheet_root)
    label_styles: dict[str, set[int]] = {}
    for cell in cells.values():
        text = _cell_text(cell, shared)
        if text:
            label_styles.setdefault(text, set()).add(int(cell.get("s", "0")))
    expected_palette = {
        str(row["status_code"]): str(row["palette"]).upper()
        for row in status_registry["visible_labels"]
    }
    palette = _style_palette(archive)
    result: dict[str, int] = {}
    for status_code in sorted(set(required_status_codes)):
        source_label = _STATUS_STYLE_LABEL[status_code]
        candidates = {
            style_id
            for style_id in label_styles.get(source_label, set())
            if palette.get(style_id) == expected_palette[status_code]
        }
        if not candidates:
            candidates = {style_id for style_id, color in palette.items() if color == expected_palette[status_code]}
        if len(candidates) != 1:
            raise PromiseProgressWorkbookPreviewError(
                f"status style {status_code!r} does not resolve to exactly one reviewed style: {sorted(candidates)!r}"
            )
        result[status_code] = next(iter(candidates))
    return result


def _alignment_variant(
    styles_root: ET.Element,
    base_style_id: int,
    *,
    wrap_text: bool,
    horizontal: str,
    vertical: str,
) -> int:
    cell_xfs = styles_root.find(f"{{{_MAIN_NS}}}cellXfs")
    if cell_xfs is None:
        raise PromiseProgressWorkbookPreviewError("workbook styles have no cellXfs collection")
    existing = list(cell_xfs)
    if base_style_id < 0 or base_style_id >= len(existing):
        raise PromiseProgressWorkbookPreviewError(f"base style {base_style_id} does not exist")
    variant = copy.deepcopy(existing[base_style_id])
    alignment = variant.find(f"{{{_MAIN_NS}}}alignment")
    if alignment is None:
        alignment = ET.SubElement(variant, f"{{{_MAIN_NS}}}alignment")
    if horizontal != "preserve":
        alignment.set("horizontal", horizontal)
    if vertical != "preserve":
        alignment.set("vertical", vertical)
    if wrap_text:
        alignment.set("wrapText", "1")
    else:
        alignment.attrib.pop("wrapText", None)
    alignment.attrib.pop("shrinkToFit", None)
    variant.set("applyAlignment", "1")
    serialized = ET.tostring(variant, encoding="utf-8")
    for index, candidate in enumerate(existing):
        if ET.tostring(candidate, encoding="utf-8") == serialized:
            return index
    cell_xfs.append(variant)
    cell_xfs.set("count", str(len(existing) + 1))
    return len(existing)


def _set_reviewed_columns(sheet_root: ET.Element) -> None:
    columns = sheet_root.find(f"{{{_MAIN_NS}}}cols")
    if columns is None:
        sheet_data = sheet_root.find(f"{{{_MAIN_NS}}}sheetData")
        if sheet_data is None:
            raise PromiseProgressWorkbookPreviewError("target worksheet has no sheetData")
        columns = ET.Element(f"{{{_MAIN_NS}}}cols")
        sheet_root.insert(list(sheet_root).index(sheet_data), columns)
    for child in list(columns):
        columns.remove(child)
    for column_number in range(1, 13):
        ET.SubElement(columns, f"{{{_MAIN_NS}}}col", {
            "min": str(column_number), "max": str(column_number),
            "width": "24", "customWidth": "1",
        })
    for column_number, width in ((13, 4), (14, 4), (15, 13)):
        ET.SubElement(columns, f"{{{_MAIN_NS}}}col", {
            "min": str(column_number), "max": str(column_number),
            "width": str(width), "hidden": "1", "customWidth": "1",
        })


def _set_product_v2_columns(sheet_root: ET.Element) -> None:
    """Apply the generic MetricWide/Standard A:J grid and isolated K:O support."""

    columns = sheet_root.find(f"{{{_MAIN_NS}}}cols")
    if columns is None:
        sheet_data = sheet_root.find(f"{{{_MAIN_NS}}}sheetData")
        if sheet_data is None:
            raise PromiseProgressWorkbookPreviewError("target worksheet has no sheetData")
        columns = ET.Element(f"{{{_MAIN_NS}}}cols")
        sheet_root.insert(list(sheet_root).index(sheet_data), columns)
    for child in list(columns):
        columns.remove(child)
    for column_number in range(1, 11):
        ET.SubElement(
            columns,
            f"{{{_MAIN_NS}}}col",
            {
                "min": str(column_number),
                "max": str(column_number),
                "width": "31.5" if column_number == 1 else "22.5",
                "customWidth": "1",
            },
        )
    for column_number, width in ((11, 4), (12, 4), (13, 4), (14, 4), (15, 13)):
        ET.SubElement(
            columns,
            f"{{{_MAIN_NS}}}col",
            {
                "min": str(column_number),
                "max": str(column_number),
                "width": str(width),
                "hidden": "1",
                "customWidth": "1",
            },
        )


def _set_reviewed_merges(
    sheet_root: ET.Element,
    permitted_merges: Sequence[str],
) -> None:
    merge_cells = sheet_root.find(f"{{{_MAIN_NS}}}mergeCells")
    if merge_cells is None:
        merge_cells = ET.Element(f"{{{_MAIN_NS}}}mergeCells")
        page_margins = sheet_root.find(f"{{{_MAIN_NS}}}pageMargins")
        insertion = len(list(sheet_root)) if page_margins is None else list(sheet_root).index(page_margins)
        sheet_root.insert(insertion, merge_cells)
    for child in list(merge_cells):
        merge_cells.remove(child)
    for merged_range in permitted_merges:
        ET.SubElement(merge_cells, f"{{{_MAIN_NS}}}mergeCell", {"ref": merged_range})
    merge_cells.set("count", str(len(permitted_merges)))


def _set_row_height(sheet_root: ET.Element, row_number: int, height_points: int) -> None:
    _get_or_create_cell(sheet_root, f"A{row_number}")
    sheet_data = sheet_root.find(f"{{{_MAIN_NS}}}sheetData")
    if sheet_data is None:
        raise PromiseProgressWorkbookPreviewError("target worksheet has no sheetData")
    row = next(
        item for item in sheet_data.findall(f"{{{_MAIN_NS}}}row")
        if int(item.get("r", "0")) == row_number
    )
    row.set("ht", str(height_points))
    row.set("customHeight", "1")


def _legacy_layout_source_column(block_id: str, field_role: str) -> str:
    matches = [
        str(column)
        for role, column, _ in BLOCK_FIELD_LAYOUT[block_id]
        if role == field_role
    ]
    if len(matches) != 1:
        raise PromiseProgressWorkbookPreviewError(
            f"legacy style source {block_id}/{field_role} does not resolve exactly once"
        )
    return matches[0]


def _write_header_contract(
    sheet_root: ET.Element,
    styles_root: ET.Element,
    contract: PromiseProgressWorkbookPresentationContract,
    original_style_ids: Mapping[str, int],
) -> None:
    for block_id, header_rows in _BLOCK_HEADER_ROWS.items():
        for row_number in header_rows:
            for layout in (row for row in contract.field_layouts if row.block_id == block_id):
                anchor = f"{layout.start_column}{row_number}"
                label = _HEADER_LABELS[block_id][layout.field_role]
                _write_inline_string(_get_or_create_cell(sheet_root, anchor), label)
                source = f"{_legacy_layout_source_column(block_id, layout.field_role)}{row_number}"
                base_style = original_style_ids.get(source, 0)
                wrap = not measure_presentation_text(
                    label, span_width=layout.span_width, wrap_text=False
                )["width_fits"]
                style_id = _alignment_variant(
                    styles_root,
                    base_style,
                    wrap_text=wrap,
                    horizontal="center" if layout.field_role in {"score", "status"} else "left",
                    vertical="center",
                )
                for destination in _expand_range(layout.range_for_row(row_number)):
                    _get_or_create_cell(sheet_root, destination).set("s", str(style_id))


def _apply_capacity_role_styles(
    sheet_root: ET.Element,
    styles_root: ET.Element,
    contract: PromiseProgressWorkbookPresentationContract,
    original_style_ids: Mapping[str, int],
) -> None:
    for block_id, capacity_rows in _BLOCK_CAPACITY_ROWS.items():
        for row_number in capacity_rows:
            for layout in (row for row in contract.field_layouts if row.block_id == block_id):
                source = f"{_legacy_layout_source_column(block_id, layout.field_role)}{row_number}"
                if layout.field_role == "status":
                    # Empty capacity rows must not inherit a legacy semantic status fill.
                    base_style = original_style_ids.get(f"A{row_number}", 0)
                else:
                    base_style = original_style_ids.get(source, original_style_ids.get(f"A{row_number}", 0))
                style_id = _alignment_variant(
                    styles_root,
                    base_style,
                    wrap_text=layout.wrap_mode == "always",
                    horizontal=layout.horizontal_alignment,
                    vertical=layout.vertical_alignment if layout.wrap_mode == "always" else "preserve",
                )
                for destination in _expand_range(layout.range_for_row(row_number)):
                    _get_or_create_cell(sheet_root, destination).set("s", str(style_id))


def _binding_style_id(
    binding: WorkbookBinding,
    *,
    styles_root: ET.Element,
    original_style_ids: Mapping[str, int],
    status_styles: Mapping[str, int],
) -> int:
    if binding.status_code is not None:
        base_style = status_styles[binding.status_code]
    else:
        base_style = original_style_ids.get(binding.legacy_anchor_cell, 0)
    if _column_number(_cell_parts(binding.anchor_cell)[0]) > 12:
        return base_style
    return _alignment_variant(
        styles_root,
        base_style,
        wrap_text=binding.wrap_text,
        horizontal=binding.horizontal_alignment,
        vertical=binding.vertical_alignment,
    )


def materialize_promise_progress_preview(
    product: PromiseProgressProduct,
    plan: PromiseProgressWorkbookBindingPlan,
    *,
    legacy_workbook: Path,
    output_workbook: Path,
    design_lock_root: Path,
) -> dict[str, Any]:
    validate_promise_progress_workbook_binding_plan(product, plan, design_lock_root=design_lock_root)
    if output_workbook.exists():
        raise PromiseProgressWorkbookPreviewError(f"refusing to overwrite existing preview: {output_workbook}")
    legacy_sha = sha256_file(legacy_workbook)
    if legacy_sha != EXPECTED_ANF_WORKBOOK_SHA256 or legacy_sha != product.template_oracle_sha256:
        raise PromiseProgressWorkbookPreviewError(
            f"legacy workbook SHA differs from the frozen product oracle: {legacy_sha}"
        )
    status_registry = load_json_strict(design_lock_root / "promise_progress_status_rule_registry.json")
    output_workbook.parent.mkdir(parents=True, exist_ok=True)

    with ZipFile(legacy_workbook, "r") as source:
        position, sheet_part = _resolve_target_sheet(source, plan.sheet_name)
        if position != plan.sheet_position_1_based or sheet_part != plan.sheet_part:
            raise PromiseProgressWorkbookPreviewError(
                f"target worksheet identity changed: position={position}, part={sheet_part}"
            )
        sheet_root = _parse_xml(source.read(sheet_part))
        styles_root = _parse_xml(source.read("xl/styles.xml"))
        original_cells = _worksheet_cell_map(sheet_root)
        original_style_ids = {
            ref: int(cell.get("s", "0")) for ref, cell in original_cells.items()
        }
        status_styles = _resolve_status_styles(
            source,
            sheet_root,
            status_registry,
            (
                binding.status_code
                for binding in plan.bindings
                if binding.status_code is not None
            ),
        )
        _set_reviewed_columns(sheet_root)
        _set_reviewed_merges(sheet_root, plan.presentation_contract.permitted_merges)
        for destination in plan.clear_destinations:
            cell = _worksheet_cell_map(sheet_root).get(destination)
            if cell is not None:
                _clear_cell(cell)
        _write_header_contract(
            sheet_root,
            styles_root,
            plan.presentation_contract,
            original_style_ids,
        )
        _apply_capacity_role_styles(
            sheet_root,
            styles_root,
            plan.presentation_contract,
            original_style_ids,
        )
        for row_number, height_points in plan.row_heights:
            _set_row_height(sheet_root, row_number, height_points)
        for binding in plan.bindings:
            cell = _get_or_create_cell(sheet_root, binding.anchor_cell)
            _write_inline_string(cell, binding.presentation_text)
            style_id = _binding_style_id(
                binding,
                styles_root=styles_root,
                original_style_ids=original_style_ids,
                status_styles=status_styles,
            )
            for destination in _expand_range(binding.display_range):
                _get_or_create_cell(sheet_root, destination).set("s", str(style_id))
        for merged_range in plan.presentation_contract.permitted_merges:
            for child in _expand_range(merged_range)[1:]:
                cell = _worksheet_cell_map(sheet_root).get(child)
                if cell is not None and _cell_text(cell):
                    raise PromiseProgressWorkbookPreviewError(
                        f"merged non-anchor cell {child} is not blank after materialization"
                    )
        changed_sheet = _serialize_xml(sheet_root)
        changed_styles = _serialize_xml(styles_root)
        source_infos = source.infolist()
        source_comment = source.comment
        members = {info.filename: source.read(info.filename) for info in source_infos}
        members[sheet_part] = changed_sheet
        members["xl/styles.xml"] = changed_styles

    temporary_handle, temporary_name = tempfile.mkstemp(
        prefix=f".{output_workbook.stem}.", suffix=output_workbook.suffix, dir=output_workbook.parent
    )
    os.close(temporary_handle)
    temporary_path = Path(temporary_name)
    try:
        with ZipFile(temporary_path, "w") as output:
            output.comment = source_comment
            for info in source_infos:
                output.writestr(info, members[info.filename])
        os.replace(temporary_path, output_workbook)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()

    return {
        "legacy_workbook": str(legacy_workbook),
        "legacy_workbook_sha256": legacy_sha,
        "preview_workbook": str(output_workbook),
        "preview_workbook_sha256": sha256_file(output_workbook),
        "canonical_workbook_content_sha256": canonical_workbook_content_sha256(output_workbook),
        "target_sheet_semantic_sha256": target_sheet_semantic_sha256(output_workbook, plan),
        "binding_plan_sha256": plan.lineage_digest,
        "presentation_contract_id": plan.presentation_contract.contract_id,
        "presentation_contract_sha256": plan.presentation_contract.to_dict()["contract_digest"],
        "written_binding_count": len(plan.bindings),
        "cleared_destination_count": len(plan.clear_destinations),
        "sheet_part": sheet_part,
        "changed_ooxml_parts": [sheet_part, "xl/styles.xml"],
    }


def canonical_workbook_content_sha256(workbook_path: Path) -> str:
    digest = hashlib.sha256()
    with ZipFile(workbook_path, "r") as archive:
        for name in sorted(archive.namelist()):
            data = archive.read(name)
            digest.update(len(name.encode("utf-8")).to_bytes(4, "big"))
            digest.update(name.encode("utf-8"))
            digest.update(len(data).to_bytes(8, "big"))
            digest.update(data)
    return digest.hexdigest()


_BUILTIN_NUMBER_FORMAT_CODES: Mapping[int, str] = {
    0: "General",
    1: "0",
    2: "0.00",
    9: "0%",
    10: "0.00%",
    14: "m/d/yy",
}


def _style_number_format_map(archive: ZipFile) -> dict[int, tuple[int, str]]:
    """Resolve each cell style to the actual OOXML number-format identity."""

    root = _parse_xml(archive.read("xl/styles.xml"))
    custom = {
        int(node.get("numFmtId", "0")): str(node.get("formatCode", "General"))
        for node in root.findall(f".//{{{_MAIN_NS}}}numFmt")
    }
    cell_xfs = root.find(f"{{{_MAIN_NS}}}cellXfs")
    if cell_xfs is None:
        raise PromiseProgressWorkbookPreviewError("workbook styles have no cellXfs collection")
    result: dict[int, tuple[int, str]] = {}
    for style_id, xf in enumerate(list(cell_xfs)):
        number_format_id = int(xf.get("numFmtId", "0"))
        format_code = custom.get(
            number_format_id,
            _BUILTIN_NUMBER_FORMAT_CODES.get(number_format_id, f"builtin:{number_format_id}"),
        )
        result[style_id] = (number_format_id, format_code)
    return result


def _excel_format_literal(value: str) -> str:
    result: list[str] = []
    quoted = False
    escaped = False
    for character in value:
        if escaped:
            result.append(character)
            escaped = False
        elif character == "\\":
            escaped = True
        elif character == '"':
            quoted = not quoted
        elif quoted or character not in {"_", "*"}:
            result.append(character)
    if quoted or escaped:
        raise PromiseProgressWorkbookPreviewError(
            f"unsupported unterminated Excel number format {value!r}"
        )
    return "".join(result)


def replay_ooxml_numeric_display(stored_value: str, format_code: str) -> str:
    """Replay the closed Product@2 scalar/date formats from actual OOXML metadata."""

    if format_code == "yyyy-mm-dd":
        serial = Decimal(str(stored_value))
        if serial != serial.to_integral_value():
            raise PromiseProgressWorkbookPreviewError(
                f"date serial is not integral: {stored_value!r}"
            )
        return (date(1899, 12, 30) + timedelta(days=int(serial))).isoformat()

    match = re.search(r"0(?:\.[0#]+)?", format_code)
    if match is None:
        raise PromiseProgressWorkbookPreviewError(
            f"unsupported Product@2 Excel number format {format_code!r}"
        )
    prefix = _excel_format_literal(format_code[: match.start()])
    suffix = _excel_format_literal(format_code[match.end() :])
    pattern = match.group(0)
    decimal_pattern = pattern.split(".", 1)[1] if "." in pattern else ""
    required_places = decimal_pattern.count("0")
    total_places = len(decimal_pattern)
    number = Decimal(str(stored_value))
    if "%" in prefix + suffix:
        number *= Decimal("100")
    quantum = Decimal("1").scaleb(-total_places)
    rounded = number.quantize(quantum, rounding=ROUND_HALF_UP)
    if rounded == 0:
        rounded = abs(rounded)
    rendered = f"{rounded:.{total_places}f}"
    if total_places > required_places:
        integer, fractional = rendered.split(".", 1)
        fractional = fractional.rstrip("0")
        if len(fractional) < required_places:
            fractional += "0" * (required_places - len(fractional))
        rendered = integer if not fractional else f"{integer}.{fractional}"
    return f"{prefix}{rendered}{suffix}"


def _workbook_sheet_snapshot(workbook_path: Path, sheet_name: str) -> tuple[dict[str, Any], str]:
    with ZipFile(workbook_path, "r") as archive:
        position, part = _resolve_target_sheet(archive, sheet_name)
        root = _parse_xml(archive.read(part))
        shared = _shared_strings(archive)
        cells = _worksheet_cell_map(root)
        style_number_formats = _style_number_format_map(archive)
        snapshot = {
            "sheet_name": sheet_name,
            "position_1_based": position,
            "sheet_part": part,
            "cells": {
                ref: {
                    "value": _cell_text(cell, shared),
                    "cell_type": cell.get("t"),
                    "style_id": int(cell.get("s", "0")),
                    "number_format_id": style_number_formats.get(
                        int(cell.get("s", "0")), (0, "General")
                    )[0],
                    "number_format_code": style_number_formats.get(
                        int(cell.get("s", "0")), (0, "General")
                    )[1],
                    "formula": (
                        cell.find(f"{{{_MAIN_NS}}}f").text
                        if cell.find(f"{{{_MAIN_NS}}}f") is not None
                        else None
                    ),
                }
                for ref, cell in sorted(cells.items(), key=lambda pair: _cell_sort_key(pair[0]))
            },
        }
    return snapshot, part


def target_sheet_semantic_sha256(
    workbook_path: Path,
    plan: PromiseProgressWorkbookBindingPlan,
) -> str:
    snapshot, _ = _workbook_sheet_snapshot(workbook_path, plan.sheet_name)
    relevant = {
        "sheet_name": plan.sheet_name,
        "bindings": [
            {
                "binding_id": binding.binding_id,
                "destination": binding.anchor_cell,
                "value": snapshot["cells"].get(binding.anchor_cell, {}).get("value", ""),
                "canonical_display_text": binding.canonical_display_text,
                "display_transform_id": binding.display_transform_id,
                "display_range": binding.display_range,
                "style_id": snapshot["cells"].get(binding.anchor_cell, {}).get("style_id", 0),
                "style_role": binding.style_role,
            }
            for binding in plan.bindings
        ],
        "cleared_destinations": {
            cell: snapshot["cells"].get(cell, {}).get("value", "")
            for cell in plan.clear_destinations
            if cell not in {binding.anchor_cell for binding in plan.bindings}
        },
        "row_heights": list(plan.row_heights),
        "presentation_contract_sha256": plan.presentation_contract.to_dict()["contract_digest"],
    }
    return _sha256_bytes(canonical_json_bytes(relevant))


def _feature_counts(root: ET.Element) -> dict[str, int]:
    return {
        "formulas": len(root.findall(f".//{{{_MAIN_NS}}}f")),
        "tables": len(root.findall(f".//{{{_MAIN_NS}}}tableParts")),
        "filters": len(root.findall(f".//{{{_MAIN_NS}}}autoFilter")),
        "data_validations": len(root.findall(f".//{{{_MAIN_NS}}}dataValidation")),
        "conditional_formatting_rules": len(root.findall(f".//{{{_MAIN_NS}}}cfRule")),
        "hyperlinks": len(root.findall(f".//{{{_MAIN_NS}}}hyperlink")),
        "sheet_protection": len(root.findall(f".//{{{_MAIN_NS}}}sheetProtection")),
        "drawings": sum(1 for child in root if child.tag.endswith("drawing")),
    }


def _worksheet_structure(root: ET.Element, dynamic_cells: set[str]) -> dict[str, Any]:
    dimension = root.find(f"{{{_MAIN_NS}}}dimension")
    panes = [dict(node.attrib) for node in root.findall(f".//{{{_MAIN_NS}}}pane")]
    selections = [dict(node.attrib) for node in root.findall(f".//{{{_MAIN_NS}}}selection")]
    columns = [dict(node.attrib) for node in root.findall(f".//{{{_MAIN_NS}}}col")]
    rows = [
        {key: value for key, value in node.attrib.items() if key != "spans"}
        for node in root.findall(f".//{{{_MAIN_NS}}}row")
    ]
    merges = [node.get("ref") for node in root.findall(f".//{{{_MAIN_NS}}}mergeCell")]
    margins = root.find(f"{{{_MAIN_NS}}}pageMargins")
    outside_cells = {
        ref: {
            "attrib": dict(cell.attrib),
            "xml": ET.tostring(cell, encoding="unicode"),
        }
        for ref, cell in _worksheet_cell_map(root).items()
        if ref not in dynamic_cells
    }
    return {
        "dimension": None if dimension is None else dimension.get("ref"),
        "panes": panes,
        "selections": selections,
        "columns": columns,
        "rows": rows,
        "merged_ranges": merges,
        "margins": None if margins is None else dict(margins.attrib),
        "features": _feature_counts(root),
        "outside_cells": outside_cells,
    }


def validate_preview_structure(
    *,
    legacy_workbook: Path,
    preview_workbook: Path,
    plan: PromiseProgressWorkbookBindingPlan,
    design_lock_root: Path,
) -> dict[str, Any]:
    structural_contract = load_json_strict(design_lock_root / "promise_progress_structural_parity_contract.json")
    status_registry = load_json_strict(design_lock_root / "promise_progress_status_rule_registry.json")
    expected_palette = {
        str(row["status_code"]): str(row["palette"]).upper()
        for row in status_registry["visible_labels"]
    }
    with ZipFile(legacy_workbook, "r") as legacy, ZipFile(preview_workbook, "r") as preview:
        legacy_position, legacy_part = _resolve_target_sheet(legacy, plan.sheet_name)
        preview_position, preview_part = _resolve_target_sheet(preview, plan.sheet_name)
        legacy_members = {name: legacy.read(name) for name in legacy.namelist()}
        preview_members = {name: preview.read(name) for name in preview.namelist()}
        changed_parts = sorted(
            name for name in set(legacy_members) | set(preview_members)
            if legacy_members.get(name) != preview_members.get(name)
        )
        unexpected_part_differences = sorted(
            set(changed_parts) - {legacy_part, "xl/styles.xml"}
        )
        legacy_root = _parse_xml(legacy_members[legacy_part])
        preview_root = _parse_xml(preview_members[preview_part])
        legacy_shared = _shared_strings(legacy)
        preview_shared = _shared_strings(preview)
        legacy_cells = _worksheet_cell_map(legacy_root)
        preview_cells = _worksheet_cell_map(preview_root)
        changed_cells = sorted(
            {
                ref for ref in set(legacy_cells) | set(preview_cells)
                if (
                    _cell_text(legacy_cells.get(ref), legacy_shared),
                    None if legacy_cells.get(ref) is None else legacy_cells[ref].get("s", "0"),
                    None if legacy_cells.get(ref) is None else legacy_cells[ref].get("t"),
                ) != (
                    _cell_text(preview_cells.get(ref), preview_shared),
                    None if preview_cells.get(ref) is None else preview_cells[ref].get("s", "0"),
                    None if preview_cells.get(ref) is None else preview_cells[ref].get("t"),
                )
            },
            key=_cell_sort_key,
        )
        dynamic = set(plan.clear_destinations)
        unexpected_changed_cells = sorted(set(changed_cells) - dynamic, key=_cell_sort_key)

        legacy_styles = _parse_xml(legacy_members["xl/styles.xml"])
        preview_styles = _parse_xml(preview_members["xl/styles.xml"])
        legacy_xfs = legacy_styles.find(f"{{{_MAIN_NS}}}cellXfs")
        preview_xfs = preview_styles.find(f"{{{_MAIN_NS}}}cellXfs")
        legacy_xf_rows = [] if legacy_xfs is None else list(legacy_xfs)
        preview_xf_rows = [] if preview_xfs is None else list(preview_xfs)
        original_styles_unchanged = (
            len(preview_xf_rows) >= len(legacy_xf_rows)
            and all(
                ET.tostring(left, encoding="utf-8") == ET.tostring(right, encoding="utf-8")
                for left, right in zip(legacy_xf_rows, preview_xf_rows)
            )
        )
        new_style_ids = list(range(len(legacy_xf_rows), len(preview_xf_rows)))
        new_style_references = sorted({
            int(cell.get("s", "0")) for cell in preview_cells.values()
            if int(cell.get("s", "0")) in new_style_ids
        })

        column_contract: dict[str, dict[str, Any]] = {}
        for node in preview_root.findall(f".//{{{_MAIN_NS}}}col"):
            for number in range(int(node.get("min", "0")), int(node.get("max", "0")) + 1):
                if 1 <= number <= 15:
                    column_contract[_column_name(number)] = {
                        "width": float(node.get("width", "0")),
                        "hidden": node.get("hidden") == "1",
                    }
        expected_columns = {
            **{column: {"width": 24.0, "hidden": False} for column in "ABCDEFGHIJKL"},
            "M": {"width": 4.0, "hidden": True},
            "N": {"width": 4.0, "hidden": True},
            "O": {"width": 13.0, "hidden": True},
        }
        row_nodes = {
            int(node.get("r", "0")): node
            for node in preview_root.findall(f".//{{{_MAIN_NS}}}row")
        }
        actual_row_heights = {
            row_number: int(float(row_nodes[row_number].get("ht", "0")))
            for row_number, _ in plan.row_heights
            if row_number in row_nodes
        }
        expected_row_heights = dict(plan.row_heights)
        preview_palette = _style_palette(preview)
        status_style_results = []
        for binding in plan.bindings:
            if binding.status_code is None:
                continue
            cell = preview_cells.get(binding.anchor_cell)
            style_id = 0 if cell is None else int(cell.get("s", "0"))
            actual_color = preview_palette.get(style_id)
            status_style_results.append({
                "binding_id": binding.binding_id,
                "destination": binding.anchor_cell,
                "status_code": binding.status_code,
                "style_id": style_id,
                "expected_fill": expected_palette[binding.status_code],
                "actual_fill": actual_color,
                "pass": actual_color == expected_palette[binding.status_code],
            })

        m_values = {
            ref: _cell_text(cell, preview_shared) for ref, cell in preview_cells.items()
            if _cell_parts(ref)[0] == "M" and _cell_parts(ref)[1] <= 102 and _cell_text(cell, preview_shared)
        }
        n_values = {
            ref: _cell_text(cell, preview_shared) for ref, cell in preview_cells.items()
            if _cell_parts(ref)[0] == "N" and _cell_parts(ref)[1] <= 102 and _cell_text(cell, preview_shared)
        }
        expected_o = {
            binding.anchor_cell: binding.presentation_text
            for binding in plan.bindings if binding.binding_kind == "row_trace"
        }
        actual_o = {
            ref: _cell_text(cell, preview_shared) for ref, cell in preview_cells.items()
            if _cell_parts(ref)[0] == "O" and _cell_parts(ref)[1] <= 102 and _cell_text(cell, preview_shared)
        }
        merges = [node.get("ref") for node in preview_root.findall(f".//{{{_MAIN_NS}}}mergeCell")]
        merged_nonblank = {
            child: _cell_text(preview_cells.get(child), preview_shared)
            for merged_range in merges
            for child in _expand_range(str(merged_range))[1:]
            if _cell_text(preview_cells.get(child), preview_shared)
        }
        dimension = preview_root.find(f"{{{_MAIN_NS}}}dimension")
        panes = [dict(node.attrib) for node in preview_root.findall(f".//{{{_MAIN_NS}}}pane")]
        views = [dict(node.attrib) for node in preview_root.findall(f".//{{{_MAIN_NS}}}sheetView")]
        validations = {
            "sheet_identity": preview_position == plan.sheet_position_1_based and preview_part == plan.sheet_part,
            "approved_ooxml_parts_only": not unexpected_part_differences and set(changed_parts) == {preview_part, "xl/styles.xml"},
            "existing_styles_unchanged_append_only": original_styles_unchanged and bool(new_style_ids),
            "new_styles_target_referenced": bool(new_style_references) and set(new_style_references).issubset(set(new_style_ids)),
            "changed_cells_within_presentation_scope": not unexpected_changed_cells,
            "dimension": dimension is not None and dimension.get("ref") == structural_contract["sheet_geometry"]["dimension"] == "A1:O102",
            "reviewed_v2_merge_set": merges == list(plan.presentation_contract.permitted_merges),
            "merged_non_anchor_cells_blank": not merged_nonblank,
            "reviewed_v2_column_grid": column_contract == expected_columns,
            "reviewed_v2_row_heights": actual_row_heights == expected_row_heights,
            "freeze_pane": any(pane.get("topLeftCell") == "A2" and pane.get("state") == "frozen" for pane in panes),
            "zoom": any(view.get("zoomScale") == "112" for view in views),
            "m_blank": not m_values,
            "n_blank": not n_values,
            "o_row_ids_exact": actual_o == expected_o,
            "status_styles": all(row["pass"] for row in status_style_results),
            "feature_counts": _feature_counts(preview_root) == _feature_counts(legacy_root),
            "no_formulas": _feature_counts(preview_root)["formulas"] == 0,
        }
    result = {
        "schema_id": STRUCTURAL_VALIDATION_SCHEMA_ID,
        "legacy_workbook_sha256": sha256_file(legacy_workbook),
        "preview_workbook_sha256": sha256_file(preview_workbook),
        "sheet_name": plan.sheet_name,
        "sheet_position_1_based": preview_position,
        "sheet_part": preview_part,
        "changed_ooxml_parts": changed_parts,
        "approved_target_presentation_dependency": "xl/styles.xml append-only alignment variants",
        "unexpected_part_difference_count": len(unexpected_part_differences),
        "unexpected_part_differences": unexpected_part_differences,
        "changed_cell_count": len(changed_cells),
        "changed_cells": changed_cells,
        "unexpected_changed_cells": unexpected_changed_cells,
        "status_style_results": status_style_results,
        "m_nonblank": m_values,
        "n_nonblank": n_values,
        "o_row_ids": actual_o,
        "column_contract": column_contract,
        "row_heights": actual_row_heights,
        "merged_non_anchor_values": merged_nonblank,
        "legacy_cell_xf_count": len(legacy_xf_rows),
        "preview_cell_xf_count": len(preview_xf_rows),
        "new_style_ids": new_style_ids,
        "new_style_references": new_style_references,
        "feature_counts": _feature_counts(preview_root),
        "validations": validations,
        "passed": all(validations.values()),
    }
    result["validation_digest"] = _sha256_bytes(canonical_json_bytes(result))
    return result


def _style_alignment_map(archive: ZipFile) -> dict[int, dict[str, Any]]:
    root = _parse_xml(archive.read("xl/styles.xml"))
    cell_xfs = root.find(f"{{{_MAIN_NS}}}cellXfs")
    rows = [] if cell_xfs is None else list(cell_xfs)
    result: dict[int, dict[str, Any]] = {}
    for index, xf in enumerate(rows):
        alignment = xf.find(f"{{{_MAIN_NS}}}alignment")
        result[index] = {
            "horizontal": None if alignment is None else alignment.get("horizontal"),
            "vertical": None if alignment is None else alignment.get("vertical"),
            "wrap_text": alignment is not None and alignment.get("wrapText") == "1",
            "shrink_to_fit": alignment is not None and alignment.get("shrinkToFit") == "1",
        }
    return result


def _style_top_border_map(archive: ZipFile) -> dict[int, dict[str, str | None]]:
    root = _parse_xml(archive.read("xl/styles.xml"))
    cell_xfs = root.find(f"{{{_MAIN_NS}}}cellXfs")
    borders = root.find(f"{{{_MAIN_NS}}}borders")
    xf_rows = [] if cell_xfs is None else list(cell_xfs)
    border_rows = [] if borders is None else list(borders)
    result: dict[int, dict[str, str | None]] = {}
    for index, xf in enumerate(xf_rows):
        border_id = int(xf.get("borderId", "0"))
        border = border_rows[border_id] if 0 <= border_id < len(border_rows) else None
        top = None if border is None else border.find(f"{{{_MAIN_NS}}}top")
        color = None if top is None else top.find(f"{{{_MAIN_NS}}}color")
        result[index] = {
            "style": None if top is None else top.get("style"),
            "rgb": None if color is None else color.get("rgb"),
        }
    return result


def validate_preview_visual_fit(
    *,
    preview_workbook: Path,
    plan: PromiseProgressWorkbookBindingPlan,
) -> dict[str, Any]:
    with ZipFile(preview_workbook, "r") as archive:
        _, part = _resolve_target_sheet(archive, plan.sheet_name)
        root = _parse_xml(archive.read(part))
        shared = _shared_strings(archive)
        cells = _worksheet_cell_map(root)
        alignments = _style_alignment_map(archive)
        row_nodes = {
            int(node.get("r", "0")): node for node in root.findall(f".//{{{_MAIN_NS}}}row")
        }
        sheet_format = root.find(f"{{{_MAIN_NS}}}sheetFormatPr")
        default_height = float("15" if sheet_format is None else sheet_format.get("defaultRowHeight", "15"))
        records: list[dict[str, Any]] = []
        for binding in plan.bindings:
            column, row_number = _cell_parts(binding.anchor_cell)
            if _column_number(column) > 12:
                continue
            end_column, _ = _cell_parts(binding.display_range.split(":")[-1])
            span_width = _column_number(end_column) - _column_number(column) + 1
            cell = cells.get(binding.anchor_cell)
            style_id = 0 if cell is None else int(cell.get("s", "0"))
            alignment = alignments.get(style_id, {})
            height = float(row_nodes.get(row_number, {}).get("ht", default_height))
            measurement = measure_presentation_text(
                _cell_text(cell, shared),
                span_width=span_width,
                wrap_text=bool(alignment.get("wrap_text", False)),
                allocated_height_points=int(height),
            )
            records.append({
                "binding_id": binding.binding_id,
                "block_id": binding.block_id,
                "field_role": binding.field_role,
                "destination": binding.anchor_cell,
                "display_range": binding.display_range,
                "canonical_display_text": binding.canonical_display_text,
                "presentation_text": binding.presentation_text,
                "display_transform_id": binding.display_transform_id,
                "effective_width_pixels": measurement["effective_width_pixels"],
                "effective_height_points": height,
                "font_name": measurement["font_name"],
                "font_size_points": measurement["font_size_points"],
                "wrap_state": alignment.get("wrap_text", False),
                "required_width_pixels": measurement["required_width_pixels"],
                "required_height_pixels": measurement["required_height_pixels"],
                "allocated_height_pixels": measurement["allocated_height_pixels"],
                "fit": measurement["fit"],
                "overflow_dependency": not bool(alignment.get("wrap_text", False)) and not measurement["width_fits"],
                "shrink_to_fit": bool(alignment.get("shrink_to_fit", False)),
                "expected_wrap": binding.wrap_text,
                "style_id": style_id,
                "pass": (
                    measurement["fit"]
                    and not bool(alignment.get("shrink_to_fit", False))
                    and bool(alignment.get("wrap_text", False)) == binding.wrap_text
                ),
            })

        header_records: list[dict[str, Any]] = []
        for block_id, header_rows in _BLOCK_HEADER_ROWS.items():
            for row_number in header_rows:
                for layout in (row for row in plan.presentation_contract.field_layouts if row.block_id == block_id):
                    anchor = f"{layout.start_column}{row_number}"
                    cell = cells.get(anchor)
                    label = _cell_text(cell, shared)
                    style_id = 0 if cell is None else int(cell.get("s", "0"))
                    alignment = alignments.get(style_id, {})
                    height = float(row_nodes.get(row_number, {}).get("ht", default_height))
                    measurement = measure_presentation_text(
                        label,
                        span_width=layout.span_width,
                        wrap_text=bool(alignment.get("wrap_text", False)),
                        allocated_height_points=int(height),
                    )
                    header_records.append({
                        "block_id": block_id,
                        "row": row_number,
                        "field_role": layout.field_role,
                        "destination": anchor,
                        "display_range": layout.range_for_row(row_number),
                        "text": label,
                        "fit": measurement["fit"],
                        "overflow_dependency": not bool(alignment.get("wrap_text", False)) and not measurement["width_fits"],
                        "shrink_to_fit": bool(alignment.get("shrink_to_fit", False)),
                        "pass": measurement["fit"] and not bool(alignment.get("shrink_to_fit", False)),
                    })
    validations = {
        "all_investor_facing_bindings_fit": all(row["fit"] for row in records),
        "all_headers_fit": all(row["fit"] for row in header_records),
        "zero_overflow_dependency": not any(row["overflow_dependency"] for row in records + header_records),
        "zero_shrink_to_fit": not any(row["shrink_to_fit"] for row in records + header_records),
        "wrap_contract_exact": all(row["expected_wrap"] == row["wrap_state"] for row in records),
        "all_records_pass": all(row["pass"] for row in records + header_records),
    }
    result = {
        "schema_id": VISUAL_FIT_VALIDATION_SCHEMA_ID,
        "presentation_contract_id": plan.presentation_contract.contract_id,
        "presentation_contract_sha256": plan.presentation_contract.to_dict()["contract_digest"],
        "preview_workbook_sha256": sha256_file(preview_workbook),
        "binding_record_count": len(records),
        "header_record_count": len(header_records),
        "clipped_visible_field_count": sum(1 for row in records + header_records if not row["fit"]),
        "overflow_dependency_count": sum(1 for row in records + header_records if row["overflow_dependency"]),
        "records": records,
        "header_records": header_records,
        "validations": validations,
        "passed": all(validations.values()),
    }
    result["validation_digest"] = _sha256_bytes(canonical_json_bytes(result))
    return result


def validate_preview_semantics(
    product: PromiseProgressProduct,
    plan: PromiseProgressWorkbookBindingPlan,
    *,
    preview_workbook: Path,
) -> dict[str, Any]:
    snapshot, _ = _workbook_sheet_snapshot(preview_workbook, plan.sheet_name)
    cells = snapshot["cells"]
    results = []
    for binding in plan.bindings:
        actual = cells.get(binding.anchor_cell, {}).get("value", "")
        results.append(
            {
                "binding_id": binding.binding_id,
                "binding_kind": binding.binding_kind,
                "block_id": binding.block_id,
                "source_row_ids": list(binding.source_row_ids),
                "source_field_ids": list(binding.source_field_ids),
                "field_role": binding.field_role,
                "destination": binding.anchor_cell,
                "display_range": binding.display_range,
                "value_form": binding.value_form,
                "canonical_display_text": binding.canonical_display_text,
                "presentation_text": binding.presentation_text,
                "display_transform_id": binding.display_transform_id,
                "canonical_text_digest": binding.canonical_text_digest,
                "lineage_full_text_digest": binding.lineage_full_text_digest,
                "expected_display_value": binding.presentation_text,
                "written_display_value": actual,
                "missing_state": binding.value_form == "missing",
                "pass": actual == binding.presentation_text,
            }
        )
    field_results = [row for row in results if row["binding_kind"] == "product_field"]
    role_counts = Counter(row["field_role"] for row in field_results)
    missing = [row for row in field_results if row["missing_state"]]
    explicit_zero = [
        row
        for row in field_results
        if row["value_form"] != "missing" and row["expected_display_value"].strip() in {"0", "0.0", "0%", "$0"}
    ]
    validations = {
        "product_identity": plan.product_id == product.product_id and plan.product_contract_id == product.product_contract_id,
        "field_count": len(field_results) == len(product.fields),
        "row_trace_count": len([row for row in results if row["binding_kind"] == "row_trace"]) == len(product.ordered_rows),
        "all_presentation_values_exact": all(row["pass"] for row in results),
        "identity_transforms_exact": all(
            row["canonical_display_text"] == row["presentation_text"]
            for row in results if row["display_transform_id"] == IDENTITY_TRANSFORM_ID
        ),
        "transforms_closed": all(
            row["display_transform_id"] in plan.presentation_contract.transform_ids for row in results
        ),
        "missing_remains_blank": all(row["written_display_value"] == "" for row in missing),
        "explicit_zero_remains_zero": all(row["written_display_value"] == row["expected_display_value"] for row in explicit_zero),
        "actual_progress_distinct": "actual" in role_counts and "progress" in role_counts,
        "status_replayed": all(
            row["written_display_value"] == row["expected_display_value"]
            for row in field_results
            if row["field_role"] == "status"
        ),
        "notes_source_typed_presentation_replayed": all(
            row["written_display_value"] == row["presentation_text"]
            for row in field_results
            if row["field_role"] in {"notes_source", "source_note"}
        ),
        "canonical_text_digests_exact": all(
            row["canonical_text_digest"] == _sha256_bytes(row["canonical_display_text"].encode("utf-8"))
            for row in field_results
        ),
        "store_progress_three_facts": any(
            row["destination"] == "G17"
            and row["display_range"] == "G17:H17"
            and row["display_transform_id"] == STORE_PROGRESS_TRANSFORM_ID
            and row["written_display_value"] == "62 openings / 22 closures\nNet: 40"
            for row in field_results
        ),
    }
    result = {
        "schema_id": SEMANTIC_VALIDATION_SCHEMA_ID,
        "product_id": product.product_id,
        "product_contract_id": product.product_contract_id,
        "product_sha256": _sha256_bytes(serialize_promise_progress_product(product)),
        "shadow_sha256": _sha256_bytes(serialize_shadow_matrix(product)),
        "binding_plan_sha256": plan.lineage_digest,
        "presentation_contract_id": plan.presentation_contract.contract_id,
        "presentation_contract_sha256": plan.presentation_contract.to_dict()["contract_digest"],
        "binding_count": len(results),
        "product_field_count": len(field_results),
        "product_row_count": len(product.ordered_rows),
        "missing_field_count": len(missing),
        "explicit_zero_field_count": len(explicit_zero),
        "field_role_counts": dict(sorted(role_counts.items())),
        "results": results,
        "validations": validations,
        "passed": all(validations.values()),
    }
    result["validation_digest"] = _sha256_bytes(canonical_json_bytes(result))
    return result


def build_workbook_trace(
    product: PromiseProgressProduct,
    plan: PromiseProgressWorkbookBindingPlan,
    *,
    preview_workbook: Path,
) -> dict[str, Any]:
    snapshot, _ = _workbook_sheet_snapshot(preview_workbook, plan.sheet_name)
    cells = snapshot["cells"]
    records = [
        {
            "binding_id": binding.binding_id,
            "binding_kind": binding.binding_kind,
            "product_id": binding.product_id,
            "product_contract_id": binding.product_contract_id,
            "block_id": binding.block_id,
            "source_native_row_ids": list(binding.source_row_ids),
            "source_native_field_ids": list(binding.source_field_ids),
            "source_native_lineage_digests": list(binding.lineage_digests),
            "source_document_ids": list(binding.source_document_ids),
            "source_occurrence_ids": list(binding.source_occurrence_ids),
            "review_issue_ids": list(binding.review_issue_ids),
            "field_role": binding.field_role,
            "workbook_binding_plan_sha256": plan.lineage_digest,
            "destination": {"sheet": binding.sheet_name, "anchor_cell": binding.anchor_cell, "display_range": binding.display_range},
            "display_type": binding.display_type,
            "value_form": binding.value_form,
            "canonical_display_text": binding.canonical_display_text,
            "canonical_text_digest": binding.canonical_text_digest,
            "display_transform_id": binding.display_transform_id,
            "display_transform_version": binding.display_transform_version,
            "presentation_text": binding.presentation_text,
            "lineage_full_text_digest": binding.lineage_full_text_digest,
            "wrap_text": binding.wrap_text,
            "horizontal_alignment": binding.horizontal_alignment,
            "vertical_alignment": binding.vertical_alignment,
            "row_height_points": dict(plan.row_heights).get(_cell_parts(binding.anchor_cell)[1]),
            "fit_measurement": _canonical(binding.fit_measurement),
            "expected_display_value": binding.presentation_text,
            "written_display_value": cells.get(binding.anchor_cell, {}).get("value", ""),
            "written_style_id": cells.get(binding.anchor_cell, {}).get("style_id", 0),
            "status_code": binding.status_code,
            "style_role": binding.style_role,
        }
        for binding in plan.bindings
    ]
    result = {
        "schema_id": WORKBOOK_TRACE_SCHEMA_ID,
        "product_id": product.product_id,
        "product_contract_id": product.product_contract_id,
        "product_sha256": _sha256_bytes(serialize_promise_progress_product(product)),
        "shadow_sha256": _sha256_bytes(serialize_shadow_matrix(product)),
        "binding_plan_sha256": plan.lineage_digest,
        "presentation_contract_sha256": plan.presentation_contract.to_dict()["contract_digest"],
        "preview_workbook_sha256": sha256_file(preview_workbook),
        "record_count": len(records),
        "records": records,
    }
    result["trace_digest"] = _sha256_bytes(canonical_json_bytes(result))
    return result


def build_legacy_difference_report(
    product: PromiseProgressProduct,
    plan: PromiseProgressWorkbookBindingPlan,
    *,
    legacy_workbook: Path,
    preview_workbook: Path,
) -> dict[str, Any]:
    legacy, _ = _workbook_sheet_snapshot(legacy_workbook, plan.sheet_name)
    preview, _ = _workbook_sheet_snapshot(preview_workbook, plan.sheet_name)
    legacy_cells = legacy["cells"]
    preview_cells = preview["cells"]
    parity_by_field = {
        row["source_native_field_id"]: row
        for row in product.parity_report()["field_comparisons"]
        if row.get("source_native_field_id") is not None
    }
    bindings_by_destination = {binding.anchor_cell: binding for binding in plan.bindings}
    differences: list[dict[str, Any]] = []
    category_counts: Counter[str] = Counter()
    for destination in plan.clear_destinations:
        column, _ = _cell_parts(destination)
        if _column_number(column) > 12:
            continue
        binding = bindings_by_destination.get(destination)
        legacy_destination = destination if binding is None else binding.legacy_anchor_cell
        legacy_value = legacy_cells.get(legacy_destination, {}).get("value", "")
        preview_value = preview_cells.get(destination, {}).get("value", "")
        legacy_style = legacy_cells.get(legacy_destination, {}).get("style_id", 0)
        preview_style = preview_cells.get(destination, {}).get("style_id", 0)
        if binding is None and legacy_value == preview_value and legacy_style == preview_style:
            continue
        parity = None
        if binding is not None and binding.binding_kind == "product_field" and binding.source_field_ids:
            parity = parity_by_field.get(binding.source_field_ids[0])
        if binding is not None and preview_value != binding.presentation_text:
            category = "mapping defect"
        elif binding is not None and binding.display_transform_id != IDENTITY_TRANSFORM_ID:
            category = "normalized presentation parity"
        elif legacy_value == preview_value and legacy_style == preview_style and legacy_destination == destination:
            category = "exact parity"
        elif binding is None:
            category = "reviewed layout evolution"
        elif binding.binding_kind in {"product_metadata", "timeline_group_header"}:
            category = "normalized presentation parity"
        elif parity is None:
            category = "unresolved"
        elif parity["classification"] == "exact-match":
            category = "reviewed layout evolution" if legacy_value == binding.canonical_display_text else "mapping defect"
        elif parity["classification"] == "accepted-semantic-match":
            category = "normalized presentation parity"
        elif parity["classification"] in {"registered-authorized-exception", "source-native-only-field"}:
            if parity.get("difference_reason_code") in {
                "legacy_unreviewed_scorecard_value",
                "legacy_generic_source_note",
                "legacy_static_status",
            }:
                category = "expected legacy defect removal"
            else:
                category = "accepted source-native semantic correction"
        elif parity["classification"] == "legacy-only-field":
            category = "expected legacy defect removal"
        elif parity["classification"] in {"mapping-alignment-defect", "unregistered-difference"}:
            category = "mapping defect"
        else:
            category = "unresolved"
        category_counts[category] += 1
        differences.append(
            {
                "destination": destination,
                "legacy_destination": legacy_destination,
                "binding_id": None if binding is None else binding.binding_id,
                "source_native_field_id": None if binding is None or not binding.source_field_ids else binding.source_field_ids[0],
                "legacy_display_value": legacy_value,
                "preview_display_value": preview_value,
                "legacy_style_id": legacy_style,
                "preview_style_id": preview_style,
                "classification": category,
                "owned_parity_classification": None if parity is None else parity["classification"],
                "difference_reason_code": None if parity is None else parity.get("difference_reason_code"),
                "parity_exception_id": None if parity is None else parity.get("exception_id"),
                "parity_comparison_digest": None if parity is None else parity.get("comparison_digest"),
            }
        )
    allowed_categories = {
        "exact parity",
        "normalized presentation parity",
        "accepted source-native semantic correction",
        "expected legacy defect removal",
        "structural difference",
        "visual difference",
        "reviewed layout evolution",
        "mapping defect",
        "unresolved",
    }
    for category in allowed_categories:
        category_counts.setdefault(category, 0)
    result = {
        "schema_id": LEGACY_DIFFERENCE_SCHEMA_ID,
        "product_id": product.product_id,
        "binding_plan_sha256": plan.lineage_digest,
        "comparison_scope": f"{plan.sheet_name}!A1:L102 dynamic preview destinations",
        "difference_count": len(differences),
        "classification_counts": dict(sorted(category_counts.items())),
        "mapping_defect_count": category_counts["mapping defect"],
        "unresolved_count": category_counts["unresolved"],
        "differences": differences,
        "owned_product_parity_report_digest": product.parity_report()["lineage_digest"],
    }
    result["report_digest"] = _sha256_bytes(canonical_json_bytes(result))
    return result


def build_preview_manifest(
    *,
    output_root: Path,
    product: PromiseProgressProduct,
    plan: PromiseProgressWorkbookBindingPlan,
    legacy_workbook: Path,
    artifact_paths: Iterable[Path],
    repeated_preview_path: Path | None = None,
    design_lock_root: Path | None = None,
) -> dict[str, Any]:
    artifacts = []
    for path in sorted(set(artifact_paths), key=lambda item: item.name.casefold()):
        if not path.is_file():
            raise PromiseProgressWorkbookPreviewError(f"preview artifact is missing: {path}")
        artifacts.append(
            {
                "name": path.name,
                "relative_path": path.relative_to(output_root).as_posix(),
                "size": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    determinism: dict[str, Any] | None = None
    if repeated_preview_path is not None:
        preview_path = output_root / "ANF_Promise_Progress_source_native_preview.xlsx"
        determinism = {
            "first_raw_sha256": sha256_file(preview_path),
            "second_raw_sha256": sha256_file(repeated_preview_path),
            "raw_byte_identical": preview_path.read_bytes() == repeated_preview_path.read_bytes(),
            "first_canonical_content_sha256": canonical_workbook_content_sha256(preview_path),
            "second_canonical_content_sha256": canonical_workbook_content_sha256(repeated_preview_path),
            "canonical_content_identical": canonical_workbook_content_sha256(preview_path)
            == canonical_workbook_content_sha256(repeated_preview_path),
            "first_target_semantic_sha256": target_sheet_semantic_sha256(preview_path, plan),
            "second_target_semantic_sha256": target_sheet_semantic_sha256(repeated_preview_path, plan),
            "target_semantic_identical": target_sheet_semantic_sha256(preview_path, plan)
            == target_sheet_semantic_sha256(repeated_preview_path, plan),
            "binding_plan_sha256": plan.lineage_digest,
            "presentation_contract_sha256": plan.presentation_contract.to_dict()["contract_digest"],
        }
        first_semantic = validate_preview_semantics(product, plan, preview_workbook=preview_path)
        second_semantic = validate_preview_semantics(product, plan, preview_workbook=repeated_preview_path)
        first_visual = validate_preview_visual_fit(preview_workbook=preview_path, plan=plan)
        second_visual = validate_preview_visual_fit(preview_workbook=repeated_preview_path, plan=plan)
        first_trace = build_workbook_trace(product, plan, preview_workbook=preview_path)
        second_trace = build_workbook_trace(product, plan, preview_workbook=repeated_preview_path)
        determinism.update({
            "semantic_validation_digest": first_semantic["validation_digest"],
            "semantic_validation_identical": first_semantic["validation_digest"] == second_semantic["validation_digest"],
            "visual_fit_validation_digest": first_visual["validation_digest"],
            "visual_fit_validation_identical": first_visual["validation_digest"] == second_visual["validation_digest"],
            "workbook_trace_digest": first_trace["trace_digest"],
            "workbook_trace_identical": first_trace["trace_digest"] == second_trace["trace_digest"],
        })
        if design_lock_root is not None:
            first_structure = validate_preview_structure(
                legacy_workbook=legacy_workbook,
                preview_workbook=preview_path,
                plan=plan,
                design_lock_root=design_lock_root,
            )
            second_structure = validate_preview_structure(
                legacy_workbook=legacy_workbook,
                preview_workbook=repeated_preview_path,
                plan=plan,
                design_lock_root=design_lock_root,
            )
            determinism.update({
                "structural_validation_digest": first_structure["validation_digest"],
                "structural_validation_identical": (
                    first_structure["validation_digest"] == second_structure["validation_digest"]
                ),
            })
    result = {
        "schema_id": PREVIEW_MANIFEST_SCHEMA_ID,
        "preview_state": "disposable-source-native-shadow-preview-not-production-cutover",
        "product_id": product.product_id,
        "product_contract_id": product.product_contract_id,
        "product_sha256": _sha256_bytes(serialize_promise_progress_product(product)),
        "shadow_sha256": _sha256_bytes(serialize_shadow_matrix(product)),
        "binding_plan_sha256": plan.lineage_digest,
        "legacy_workbook": str(legacy_workbook),
        "legacy_workbook_sha256": sha256_file(legacy_workbook),
        "artifact_root": str(output_root),
        "artifact_count": len(artifacts),
        "artifacts": artifacts,
        "fresh_regeneration": determinism,
        "generated_timestamp": None,
    }
    result["manifest_digest"] = _sha256_bytes(canonical_json_bytes(result))
    return result


# ---------------------------------------------------------------------------
# PromiseProgressProduct@2 candidate bridge
# ---------------------------------------------------------------------------

PRODUCT_V2_BINDING_PLAN_SCHEMA_ID = "contract:promise-progress-workbook-binding-plan@7"
PRODUCT_V2_PRESENTATION_CONTRACT_ID = "contract:promise-progress-workbook-presentation@7"
PRODUCT_V2_WORKBOOK_TRACE_SCHEMA_ID = "trace:promise-progress-workbook-preview@7"
PRODUCT_V2_STRUCTURAL_VALIDATION_SCHEMA_ID = "validation:promise-progress-workbook-structure@7"
PRODUCT_V2_SEMANTIC_VALIDATION_SCHEMA_ID = "validation:promise-progress-workbook-semantics@7"
PRODUCT_V2_VISUAL_VALIDATION_SCHEMA_ID = "validation:promise-progress-workbook-visual@7"
SUCCESSOR_PRODUCT_V2_BINDING_PLAN_SCHEMA_ID = (
    "contract:promise-progress-workbook-binding-plan@8"
)
SUCCESSOR_PRODUCT_V2_PRESENTATION_CONTRACT_ID = (
    "contract:promise-progress-workbook-presentation@8"
)
SUCCESSOR_PRODUCT_V2_WORKBOOK_TRACE_SCHEMA_ID = (
    "trace:promise-progress-workbook-preview@8"
)
SUCCESSOR_PRODUCT_V2_STRUCTURAL_VALIDATION_SCHEMA_ID = (
    "validation:promise-progress-workbook-structure@8"
)
SUCCESSOR_PRODUCT_V2_SEMANTIC_VALIDATION_SCHEMA_ID = (
    "validation:promise-progress-workbook-semantics@8"
)
SUCCESSOR_PRODUCT_V2_VISUAL_VALIDATION_SCHEMA_ID = (
    "validation:promise-progress-workbook-visual@8"
)
PRODUCT_V2_EVENT_SOURCE_TRANSFORM_ID = "event-source-first@1"
PRODUCT_V2_REVIEW_NOTE_TRANSFORM_ID = "review-note-summary@1"
PRODUCT_V2_COMPACT_CHANGE_TRANSFORM_ID = "compact-change-label@1"

_PRODUCT_V2_COMPACT_CHANGE_LABELS = {
    "Updated — not directly comparable": "Not directly comparable",
}

_PRODUCT_V2_LAYOUTS: Mapping[str, tuple[tuple[str, str, str, str, str, str], ...]] = {
    PRODUCT_V2_OPEN_BLOCK_ID: (
        ("metric", "A", "A", "qualitative", "left", "center"),
        ("current_guide", "B", "C", "never", "left", "center"),
        ("horizon", "D", "E", "qualitative", "left", "center"),
        ("status", "F", "F", "never", "center", "center"),
    ),
    PRODUCT_V2_PROGRESSION_BLOCK_ID: (
        ("metric", "A", "A", "qualitative", "left", "center"),
        ("version_1", "B", "B", "never", "left", "center"),
        ("version_2", "C", "C", "never", "left", "center"),
        ("version_3", "D", "D", "never", "left", "center"),
        ("version_4", "E", "E", "never", "left", "center"),
        ("version_5", "F", "F", "never", "left", "center"),
        ("actual", "G", "G", "never", "left", "center"),
        ("status", "H", "H", "never", "center", "center"),
    ),
    PRODUCT_V2_TIMELINE_BLOCK_ID: (
        ("metric", "A", "A", "qualitative", "left", "center"),
        ("previous_guide", "B", "B", "never", "left", "center"),
        ("current_guide", "C", "C", "never", "left", "center"),
        ("change_type", "D", "D", "never", "left", "center"),
        ("actual", "E", "E", "never", "left", "center"),
        ("progress", "F", "F", "never", "left", "center"),
        ("status", "G", "G", "never", "center", "center"),
        ("horizon", "H", "H", "qualitative", "left", "center"),
        ("stated_in", "I", "I", "never", "left", "center"),
        ("source_date", "J", "J", "never", "left", "center"),
    ),
    PRODUCT_V2_CREDIBILITY_BLOCK_ID: (
        ("assessment_state", "A", "J", "always", "left", "top"),
    ),
}

_PRODUCT_V2_HEADERS: Mapping[str, Mapping[str, str]] = {
    PRODUCT_V2_OPEN_BLOCK_ID: {
        "metric": "Metric",
        "current_guide": "Current guide",
        "horizon": "Horizon",
        "status": "Status",
    },
    PRODUCT_V2_TIMELINE_BLOCK_ID: {
        "metric": "Metric",
        "previous_guide": "Previous guide",
        "current_guide": "New / current guide",
        "change_type": "Change type",
        "actual": "Actual",
        "progress": "Progress / run-rate",
        "status": "Status",
        "horizon": "Horizon",
        "stated_in": "Stated in",
        "source_date": "Source date",
    },
}

_PRODUCT_V2_LIFECYCLE_STYLE: Mapping[str, Mapping[str, str]] = {
    "Current": {"style_role": "lifecycle:current-information", "fill_rgb": "D9EAF7"},
    "Final": {"style_role": "lifecycle:final-neutral", "fill_rgb": "DCE6F1"},
    "Superseded": {"style_role": "lifecycle:superseded-muted", "fill_rgb": "E7E6E6"},
    "Withdrawn": {"style_role": "lifecycle:withdrawn-muted", "fill_rgb": "FCE4D6"},
    "Needs Review": {"style_role": "lifecycle:needs-review", "fill_rgb": "FFF2CC"},
}

_PRODUCT_V2_REVIEW_NOTE_LABELS = {
    "basis_distinction_requires_review": "Full-year margin Actual pending basis review",
    "definition_equivalence_unreviewed": "Capex Actual pending definition review",
    "comparable_actual_unavailable": "Full-year Actual not yet available in reviewed data",
}

_PRODUCT_V2_STYLE_SOURCE = {
    PRODUCT_V2_OPEN_BLOCK_ID: {
        "metric": "A39",
        "current_guide": "B39",
        "horizon": "C39",
        "status": "D39",
    },
    PRODUCT_V2_PROGRESSION_BLOCK_ID: {
        "metric": "A13",
        "version_1": "B13",
        "version_2": "C13",
        "version_3": "D13",
        "version_4": "E13",
        "version_5": "F13",
        "actual": "G13",
        "status": "H13",
    },
    PRODUCT_V2_TIMELINE_BLOCK_ID: {
        "metric": "A61",
        "previous_guide": "B61",
        "current_guide": "C61",
        "change_type": "D61",
        "actual": "E61",
        "progress": "F61",
        "status": "G61",
        "horizon": "H61",
        "stated_in": "I61",
        "source_date": "J61",
    },
    PRODUCT_V2_CREDIBILITY_BLOCK_ID: {"assessment_state": "C5"},
}


@dataclass(frozen=True)
class PromiseProgressWorkbookDynamicPresentationContract:
    """Presentation-only horizontal grid and vertical allocation vocabulary."""

    field_layouts: tuple[PresentationFieldLayout, ...]
    block_order: tuple[str, ...] = PRODUCT_V2_BLOCK_ORDER
    visible_columns: tuple[str, ...] = tuple("ABCDEFGHIJ")
    width_classes: tuple[tuple[str, float], ...] = (
        ("MetricWide", 31.5),
        ("Standard", 22.5),
    )
    column_width_classes: tuple[tuple[str, str], ...] = (
        ("A", "MetricWide"),
        *((column, "Standard") for column in "BCDEFGHIJ"),
    )
    hidden_support_columns: tuple[tuple[str, int, str], ...] = (
        ("K", 4, "blank"),
        ("L", 4, "blank"),
        ("M", 4, "blank"),
        ("N", 4, "blank"),
        ("O", 13, "row_id_only"),
    )
    row_height_tiers: tuple[int, ...] = _APPROVED_DATA_ROW_HEIGHTS
    timeline_max_height: int = 56
    other_block_max_height: int = 72
    spacer_height: int = 8
    transform_ids: tuple[str, ...] = (
        IDENTITY_TRANSFORM_ID,
        PRODUCT_V2_COMPACT_CHANGE_TRANSFORM_ID,
    )
    font_name: str = _PINNED_FONT_NAME
    font_size_points: float = _PINNED_FONT_SIZE_POINTS
    font_dpi: int = _PINNED_DPI
    contract_id: str = PRODUCT_V2_PRESENTATION_CONTRACT_ID

    def layout_for(self, block_id: str, field_role: str) -> PresentationFieldLayout:
        matches = [
            row
            for row in self.field_layouts
            if row.block_id == block_id and row.field_role == field_role
        ]
        if len(matches) != 1:
            raise PromiseProgressWorkbookPreviewError(
                f"Product@2 presentation role {block_id}/{field_role} does not resolve exactly once"
            )
        return matches[0]

    def width_for_column(self, column: str) -> float:
        assignments = dict(self.column_width_classes)
        classes = dict(self.width_classes)
        try:
            return float(classes[assignments[column]])
        except KeyError as exc:
            raise PromiseProgressWorkbookPreviewError(
                f"Product@2 column {column!r} lacks one reviewed width class"
            ) from exc

    def to_dict(self) -> dict[str, Any]:
        result = _canonical(dataclasses.asdict(self))
        result["font_file"] = str(_PINNED_FONT_PATH)
        result["font_file_sha256"] = sha256_file(_PINNED_FONT_PATH)
        result["header_labels"] = _canonical(_PRODUCT_V2_HEADERS)
        result["lifecycle_palette"] = _canonical(_PRODUCT_V2_LIFECYCLE_STYLE)
        result["event_start_separator"] = {
            "style": "thin",
            "rgb": "9FBAD0",
            "scope": "every visible timeline field at the first row of a disclosure_event_id",
        }
        if self.contract_id == SUCCESSOR_PRODUCT_V2_PRESENTATION_CONTRACT_ID:
            result["timeline_event_header_role"] = {
                "style_role": "TimelineEventHeader",
                "legacy_style_source": "Promise_Progress_UI!A59",
                "fill_rgb": "5B9BD5",
                "font_rgb": "FFFFFF",
                "font_bold": True,
                "span": "A:J",
                "economics_authority": "none",
            }
            result["numeric_cell_storage"] = {
                "exact_scalars": "numeric-with-closed-number-format",
                "source_dates": "excel-date-serial-with-yyyy-mm-dd",
                "ranges_approximate_composites": "intentional-inline-string",
                "ignored_error_scope": "none",
            }
        result["economics_authority"] = "none-presentation-only"
        result["vertical_allocation"] = (
            "product-owned-block-and-row-order; dynamic physical rows; one spacer between blocks"
        )
        result["contract_digest"] = _sha256_bytes(canonical_json_bytes(result))
        return result


@dataclass(frozen=True)
class ProductV2PresentationRow:
    row_number: int
    row_kind: str
    block_id: str | None
    source_row_id: str | None
    group_id: str | None
    display_label: str
    header_labels: tuple[tuple[str, str], ...] = ()
    event_start: bool = False

    def to_dict(self) -> dict[str, Any]:
        return _canonical(dataclasses.asdict(self))


@dataclass(frozen=True)
class ProductV2WorkbookBinding:
    binding_id: str
    binding_kind: str
    product_id: str
    product_version: str
    block_id: str | None
    source_row_id: str | None
    semantic_field_id: str
    field_role: str
    sheet_name: str
    anchor_cell: str
    display_range: str
    style_source_cell: str
    value_form: str
    canonical_display_text: str
    presentation_text: str
    display_transform_id: str
    machine_value: Any
    version_state: str | None
    status_code: str | None
    style_role: str | None
    horizon_period_id: str | None
    horizon_label: str
    stated_in_period_id: str | None
    stated_in_display: str
    event_id: str | None
    event_start: bool
    wrap_text: bool
    horizontal_alignment: str
    vertical_alignment: str
    current_source_document_ids: tuple[str, ...]
    predecessor_source_document_ids: tuple[str, ...]
    actual_candidate_record_ids: tuple[str, ...]
    actual_period_id: str | None
    actual_knowledge_date: str | None
    actual_source_document_ids: tuple[str, ...]
    progress_candidate_record_ids: tuple[str, ...]
    progress_period_id: str | None
    progress_knowledge_date: str | None
    progress_source_document_ids: tuple[str, ...]
    lineage_digest: str
    parity_locator: str | None
    fit_measurement: Mapping[str, Any]
    storage_kind: str | None = None
    stored_numeric_value: str | None = None
    number_format_code: str | None = None
    semantic_row_kind: str | None = None
    status_target_guidance_version_id: str | None = None
    status_actual_candidate_record_ids: tuple[str, ...] = ()
    status_actual_period_id: str | None = None
    status_actual_knowledge_date: str | None = None
    status_actual_source_document_ids: tuple[str, ...] = ()
    status_actual_basis_id: str | None = None
    status_actual_unit_id: str | None = None
    status_rule_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        result = _canonical(dataclasses.asdict(self))
        if self.storage_kind is None:
            result.pop("storage_kind", None)
            result.pop("stored_numeric_value", None)
            result.pop("number_format_code", None)
        if self.product_version == PRODUCT_V2_GOLDEN_VERSION:
            for key in (
                "semantic_row_kind",
                "status_target_guidance_version_id",
                "status_actual_candidate_record_ids",
                "status_actual_period_id",
                "status_actual_knowledge_date",
                "status_actual_source_document_ids",
                "status_actual_basis_id",
                "status_actual_unit_id",
                "status_rule_id",
            ):
                result.pop(key, None)
        return result


@dataclass(frozen=True)
class PromiseProgressWorkbookBindingPlanV2:
    product_id: str
    product_version: str
    company_id: str
    knowledge_cutoff: str
    coverage_state: str
    template_oracle_sha256: str
    design_lock_manifest_sha256: str
    sheet_name: str
    sheet_position_1_based: int
    sheet_part: str
    used_range: str
    presentation_contract: PromiseProgressWorkbookDynamicPresentationContract
    row_plan: tuple[ProductV2PresentationRow, ...]
    bindings: tuple[ProductV2WorkbookBinding, ...]
    permitted_merges: tuple[str, ...]
    row_heights: tuple[tuple[int, int], ...]
    schema_id: str = PRODUCT_V2_BINDING_PLAN_SCHEMA_ID

    def payload_without_digest(self) -> dict[str, Any]:
        return {
            "schema_id": self.schema_id,
            "product_id": self.product_id,
            "product_version": self.product_version,
            "company_id": self.company_id,
            "knowledge_cutoff": self.knowledge_cutoff,
            "coverage_state": self.coverage_state,
            "template_oracle_sha256": self.template_oracle_sha256,
            "design_lock_manifest_sha256": self.design_lock_manifest_sha256,
            "sheet_name": self.sheet_name,
            "sheet_position_1_based": self.sheet_position_1_based,
            "sheet_part": self.sheet_part,
            "used_range": self.used_range,
            "presentation_contract": self.presentation_contract.to_dict(),
            "row_plan": [row.to_dict() for row in self.row_plan],
            "bindings": [binding.to_dict() for binding in self.bindings],
            "permitted_merges": list(self.permitted_merges),
            "row_heights": [
                {"row": row_number, "height_points": height}
                for row_number, height in self.row_heights
            ],
        }

    @property
    def lineage_digest(self) -> str:
        return _sha256_bytes(canonical_json_bytes(self.payload_without_digest()))

    def to_dict(self) -> dict[str, Any]:
        return {**self.payload_without_digest(), "lineage_digest": self.lineage_digest}


def product_v2_presentation_contract(
    product_version: str = PRODUCT_V2_GOLDEN_VERSION,
) -> PromiseProgressWorkbookDynamicPresentationContract:
    layouts = tuple(
        PresentationFieldLayout(block_id, *row)
        for block_id, rows in _PRODUCT_V2_LAYOUTS.items()
        for row in rows
    )
    contract_id = (
        PRODUCT_V2_PRESENTATION_CONTRACT_ID
        if product_version == PRODUCT_V2_GOLDEN_VERSION
        else SUCCESSOR_PRODUCT_V2_PRESENTATION_CONTRACT_ID
        if product_version == SUCCESSOR_PRODUCT_VERSION
        else ""
    )
    contract = PromiseProgressWorkbookDynamicPresentationContract(
        field_layouts=layouts, contract_id=contract_id
    )
    _validate_product_v2_presentation_contract(contract)
    return contract


def _validate_product_v2_presentation_contract(
    contract: PromiseProgressWorkbookDynamicPresentationContract,
) -> None:
    if contract.contract_id not in {
        PRODUCT_V2_PRESENTATION_CONTRACT_ID,
        SUCCESSOR_PRODUCT_V2_PRESENTATION_CONTRACT_ID,
    }:
        raise PromiseProgressWorkbookPreviewError("unsupported Product@2 presentation contract")
    if contract.visible_columns != tuple("ABCDEFGHIJ"):
        raise PromiseProgressWorkbookPreviewError("Product@2 presentation must use the exact A:J grid")
    if contract.width_classes != (("MetricWide", 31.5), ("Standard", 22.5)):
        raise PromiseProgressWorkbookPreviewError("Product@2 width vocabulary changed")
    if contract.column_width_classes != (
        ("A", "MetricWide"),
        *((column, "Standard") for column in "BCDEFGHIJ"),
    ):
        raise PromiseProgressWorkbookPreviewError("Product@2 column width assignments changed")
    if contract.hidden_support_columns != (
        ("K", 4, "blank"),
        ("L", 4, "blank"),
        ("M", 4, "blank"),
        ("N", 4, "blank"),
        ("O", 13, "row_id_only"),
    ):
        raise PromiseProgressWorkbookPreviewError("Product@2 hidden support column contract changed")
    identities = [(row.block_id, row.field_role) for row in contract.field_layouts]
    expected = [(block, row[0]) for block, rows in _PRODUCT_V2_LAYOUTS.items() for row in rows]
    if identities != expected or len(identities) != len(set(identities)):
        raise PromiseProgressWorkbookPreviewError("Product@2 presentation roles are not the closed set")
    expected_coverage = {
        PRODUCT_V2_CREDIBILITY_BLOCK_ID: set("ABCDEFGHIJ"),
        PRODUCT_V2_PROGRESSION_BLOCK_ID: set("ABCDEFGH"),
        PRODUCT_V2_OPEN_BLOCK_ID: set("ABCDEF"),
        PRODUCT_V2_TIMELINE_BLOCK_ID: set("ABCDEFGHIJ"),
    }
    for block_id in PRODUCT_V2_BLOCK_ORDER:
        occupied: set[str] = set()
        for layout in (row for row in contract.field_layouts if row.block_id == block_id):
            columns = {
                _column_name(number)
                for number in range(
                    _column_number(layout.start_column),
                    _column_number(layout.end_column) + 1,
                )
            }
            if occupied & columns:
                raise PromiseProgressWorkbookPreviewError(
                    f"Product@2 presentation roles overlap in {block_id}"
                )
            occupied.update(columns)
        if occupied != expected_coverage[block_id]:
            raise PromiseProgressWorkbookPreviewError(
                f"Product@2 presentation roles do not match their compact reviewed grid in {block_id}"
            )
    if contract.row_height_tiers != (24, 40, 56, 72):
        raise PromiseProgressWorkbookPreviewError("Product@2 row-height vocabulary changed")
    if contract.transform_ids != (
        IDENTITY_TRANSFORM_ID,
        PRODUCT_V2_COMPACT_CHANGE_TRANSFORM_ID,
    ):
        raise PromiseProgressWorkbookPreviewError(
            "Product@2 presentation adapter changed its closed transform set"
        )


def _group_contiguous_product_rows(
    rows: Sequence[ProductRowV2],
) -> tuple[tuple[str, tuple[ProductRowV2, ...]], ...]:
    groups: list[tuple[str, tuple[ProductRowV2, ...]]] = []
    current_id: str | None = None
    current_rows: list[ProductRowV2] = []
    seen: set[str] = set()
    for row in rows:
        if row.group_id is None:
            raise PromiseProgressWorkbookPreviewError(
                f"Product@2 row {row.row_id} lacks a required group identity"
            )
        if row.group_id != current_id:
            if current_id is not None:
                groups.append((current_id, tuple(current_rows)))
                seen.add(current_id)
            if row.group_id in seen:
                raise PromiseProgressWorkbookPreviewError(
                    "Product@2 grouping would reorder a non-contiguous semantic group"
                )
            current_id = row.group_id
            current_rows = []
        current_rows.append(row)
    if current_id is not None:
        groups.append((current_id, tuple(current_rows)))
    return tuple(groups)


def _investor_metadata_text(product: PromiseProgressProductV2) -> str:
    cutoff = date.fromisoformat(product.knowledge_cutoff)
    cutoff_label = cutoff.strftime("%b %d, %Y").replace(" 0", " ")
    years = sorted(
        {
            int(match.group(1))
            for block in product.blocks
            for row in block.rows
            if (match := re.fullmatch(r"FY(\d{4})", row.horizon_label)) is not None
        }
    )
    if not years:
        raise PromiseProgressWorkbookPreviewError("Product@2 has no typed guidance-history coverage")
    history = f"FY{years[0]}" if len(years) == 1 else f"FY{years[0]}–FY{years[-1]}"
    segments = [f"Data through {cutoff_label}", f"Guidance history {history}"]
    if product.coverage_state != "complete_for_reviewed_scope" or any(
        row.investor_reason_code
        for block in product.blocks
        for row in block.rows
        if row.row_kind == "guidance_progression"
    ):
        segments.append("Some full-year Actual comparisons pending review")
    return " · ".join(segments)


def _progression_date_label(publication_date: str, *, target_fiscal_year: int) -> str:
    source_date = date.fromisoformat(publication_date)
    compact = source_date.strftime("%b %d").replace(" 0", " ")
    if source_date.year == target_fiscal_year:
        return compact
    return f"{compact} '{source_date.year % 100:02d}"


def _investor_reason_presentation(row: ProductRowV2) -> str:
    if row.investor_reason_code is None:
        return ""
    return _PRODUCT_V2_REVIEW_NOTE_LABELS.get(
        row.investor_reason_code, row.investor_reason_display
    )


def _product_v2_row_plan(product: PromiseProgressProductV2) -> tuple[ProductV2PresentationRow, ...]:
    rows: list[ProductV2PresentationRow] = [
        ProductV2PresentationRow(1, "product_title", None, None, None, "Promise Progress"),
        ProductV2PresentationRow(
            2,
            "product_metadata",
            None,
            None,
            None,
            _investor_metadata_text(product),
        ),
    ]
    next_row = 3
    for block_index, block in enumerate(product.blocks):
        rows.append(
            ProductV2PresentationRow(
                next_row, "block_title", block.block_id, None, None, block.title
            )
        )
        next_row += 1
        if block.block_id == PRODUCT_V2_OPEN_BLOCK_ID:
            rows.append(
                ProductV2PresentationRow(
                    next_row,
                    "table_header",
                    block.block_id,
                    None,
                    None,
                    "",
                    tuple(_PRODUCT_V2_HEADERS[block.block_id].items()),
                )
            )
            next_row += 1
            for product_row in block.rows:
                rows.append(
                    ProductV2PresentationRow(
                        next_row,
                        "product_row",
                        block.block_id,
                        product_row.row_id,
                        product_row.group_id,
                        "",
                    )
                )
                next_row += 1
        elif block.block_id == PRODUCT_V2_PROGRESSION_BLOCK_ID:
            for group_id, group_rows in _group_contiguous_product_rows(block.rows):
                horizon_labels = {row.horizon_label for row in group_rows}
                if len(horizon_labels) != 1:
                    raise PromiseProgressWorkbookPreviewError(
                        "A progression group has inconsistent horizon labels"
                    )
                rows.append(
                    ProductV2PresentationRow(
                        next_row,
                        "group_title",
                        block.block_id,
                        None,
                        group_id,
                        next(iter(horizon_labels)),
                    )
                )
                next_row += 1
                explicit_group_slots = all(
                    all(value.progression_slot is not None for value in product_row.progression_values)
                    for product_row in group_rows
                )
                dates = (
                    []
                    if explicit_group_slots
                    else sorted(
                        {
                            value.publication_date
                            for product_row in group_rows
                            for value in product_row.progression_values
                        }
                    )
                )
                if len(dates) > 5:
                    raise PromiseProgressWorkbookPreviewError(
                        f"layout_capacity_exceeded: progression group {group_id} has {len(dates)} versions"
                    )
                labels = {
                    "metric": "Metric",
                    "version_1": "Initial guide",
                    "version_2": "Q1 update",
                    "version_3": "Q2 update",
                    "version_4": "Q3 update",
                    "version_5": "Q4 update",
                    "actual": "Actual",
                    "status": "Status",
                }
                rows.append(
                    ProductV2PresentationRow(
                        next_row,
                        "table_header",
                        block.block_id,
                        None,
                        group_id,
                        "",
                        tuple((role, labels[role]) for role, *_ in _PRODUCT_V2_LAYOUTS[block.block_id]),
                    )
                )
                next_row += 1
                for product_row in group_rows:
                    rows.append(
                        ProductV2PresentationRow(
                            next_row,
                            "product_row",
                            block.block_id,
                            product_row.row_id,
                            group_id,
                            "",
                            tuple((f"date_slot_{index + 1}", value) for index, value in enumerate(dates)),
                        )
                    )
                    next_row += 1
        elif block.block_id == PRODUCT_V2_TIMELINE_BLOCK_ID:
            rows.append(
                ProductV2PresentationRow(
                    next_row,
                    "table_header",
                    block.block_id,
                    None,
                    None,
                    "",
                    tuple(_PRODUCT_V2_HEADERS[block.block_id].items()),
                )
            )
            next_row += 1
            for event_id, event_rows in _group_contiguous_product_rows(block.rows):
                contexts = {
                    (row.stated_in_period_id, row.stated_in_display) for row in event_rows
                }
                if len(contexts) != 1 or next(iter(contexts))[0] is None:
                    raise PromiseProgressWorkbookPreviewError(
                        "A disclosure event lacks one typed reporting/update context"
                    )
                _, stated_in_display = next(iter(contexts))
                rows.append(
                    ProductV2PresentationRow(
                        next_row,
                        "event_group",
                        block.block_id,
                        None,
                        event_id,
                        (
                            f"{stated_in_display} disclosures"
                            if product.product_version == SUCCESSOR_PRODUCT_VERSION
                            else f"{stated_in_display} revisions"
                        ),
                    )
                )
                next_row += 1
                for index, product_row in enumerate(event_rows):
                    rows.append(
                        ProductV2PresentationRow(
                            next_row,
                            "product_row",
                            block.block_id,
                            product_row.row_id,
                            event_id,
                            "",
                            event_start=index == 0,
                        )
                    )
                    next_row += 1
        elif block.block_id == PRODUCT_V2_CREDIBILITY_BLOCK_ID:
            for product_row in block.rows:
                rows.append(
                    ProductV2PresentationRow(
                        next_row,
                        "product_row",
                        block.block_id,
                        product_row.row_id,
                        None,
                        "",
                    )
                )
                next_row += 1
        else:
            raise PromiseProgressWorkbookPreviewError(f"unknown Product@2 block {block.block_id!r}")
        if block_index != len(product.blocks) - 1:
            rows.append(ProductV2PresentationRow(next_row, "spacer", None, None, None, ""))
            next_row += 1
    return tuple(rows)


def _product_v2_role_values(
    row: ProductRowV2,
    presentation_row: ProductV2PresentationRow,
) -> Mapping[str, tuple[str, Any, tuple[str, ...], tuple[str, ...]]]:
    empty_sources: tuple[str, ...] = ()
    current_sources = tuple(row.current_source_document_ids)
    predecessor_sources = tuple(row.predecessor_source_document_ids)
    if row.block_id == PRODUCT_V2_OPEN_BLOCK_ID:
        return {
            "metric": (row.metric_label, row.metric_id, current_sources, empty_sources),
            "current_guide": (row.current_display, row.current_value, current_sources, empty_sources),
            "horizon": (row.horizon_label, row.horizon_period_id, current_sources, empty_sources),
            "status": (
                row.status_at_update or "",
                row.status_code_at_update,
                current_sources,
                empty_sources,
            ),
        }
    if row.block_id == PRODUCT_V2_PROGRESSION_BLOCK_ID:
        version_by_date = {value.publication_date: value for value in row.progression_values}
        result: dict[str, tuple[str, Any, tuple[str, ...], tuple[str, ...]]] = {
            "metric": (row.metric_label, row.metric_id, current_sources, empty_sources),
            "actual": (row.actual_display, row.actual_value, current_sources, empty_sources),
            "status": (
                row.status_at_update or "",
                row.status_code_at_update,
                current_sources,
                empty_sources,
            ),
        }
        explicit_slots = {
            value.progression_slot: value
            for value in row.progression_values
            if value.progression_slot is not None
        }
        if explicit_slots and len(explicit_slots) != len(row.progression_values):
            raise PromiseProgressWorkbookPreviewError(
                "A progression row cannot mix explicit and inferred update slots"
            )
        if explicit_slots:
            slot_order = ("initial", "q1", "q2", "q3", "q4")
            ordered_versions = [explicit_slots.get(slot) for slot in slot_order]
        else:
            ordered_dates = [
                value
                for key, value in presentation_row.header_labels
                if key.startswith("date_slot_")
            ]
            ordered_versions = [
                version_by_date.get(ordered_dates[index])
                if index < len(ordered_dates)
                else None
                for index in range(5)
            ]
        for index in range(1, 6):
            version = ordered_versions[index - 1]
            result[f"version_{index}"] = (
                "" if version is None else version.display_text,
                None if version is None else dict(version.canonical_value),
                empty_sources if version is None else tuple(version.source_document_ids),
                empty_sources,
            )
        return result
    if row.block_id == PRODUCT_V2_TIMELINE_BLOCK_ID:
        return {
            "metric": (row.metric_label, row.metric_id, current_sources, predecessor_sources),
            "previous_guide": (
                row.previous_display,
                row.previous_value
                if row.previous_value is not None
                else row.previous_display,
                current_sources,
                predecessor_sources,
            ),
            "current_guide": (row.current_display, row.current_value, current_sources, predecessor_sources),
            "change_type": (row.change_type or "", row.comparison_reason_code, current_sources, predecessor_sources),
            "actual": (row.actual_display, row.actual_value, current_sources, predecessor_sources),
            "progress": (row.progress_display, row.progress_value, current_sources, predecessor_sources),
            "status": (
                row.status_at_update or "",
                row.status_code_at_update,
                current_sources,
                predecessor_sources,
            ),
            "horizon": (row.horizon_label, row.horizon_period_id, current_sources, predecessor_sources),
            "stated_in": (
                row.stated_in_display,
                row.stated_in_period_id,
                current_sources,
                predecessor_sources,
            ),
            "source_date": (row.event_date or "", row.event_date, current_sources, predecessor_sources),
        }
    if row.block_id == PRODUCT_V2_CREDIBILITY_BLOCK_ID:
        return {
            "assessment_state": (
                row.investor_reason_display,
                row.investor_reason_code,
                current_sources,
                predecessor_sources,
            )
        }
    raise PromiseProgressWorkbookPreviewError(f"unknown Product@2 row block {row.block_id!r}")


def _product_v2_excel_widths(start_column: str, end_column: str) -> tuple[float, ...]:
    contract = product_v2_presentation_contract()
    return tuple(
        contract.width_for_column(_column_name(number))
        for number in range(_column_number(start_column), _column_number(end_column) + 1)
    )


def _product_v2_wrap(
    layout: PresentationFieldLayout,
    text: str,
    machine_value: Any,
) -> bool:
    if layout.wrap_mode == "always":
        return True
    if layout.wrap_mode == "never" or not text:
        return False
    if layout.wrap_mode == "qualitative":
        if isinstance(machine_value, Mapping) and machine_value.get("kind") == "qualitative":
            return True
        return "\n" in text or not measure_presentation_text(
            text,
            span_width=layout.span_width,
            wrap_text=False,
            excel_widths=_product_v2_excel_widths(
                layout.start_column, layout.end_column
            ),
        )["width_fits"]
    raise PromiseProgressWorkbookPreviewError(f"unknown Product@2 wrap mode {layout.wrap_mode!r}")


_PRODUCT_V2_NUMERIC_FORMAT_BY_UNIT: Mapping[str, tuple[str, Decimal]] = {
    "unit:core:percent@1": ("0.###%", Decimal("0.01")),
    "unit:core:currency-per-share@1": ('"$"0.00', Decimal("1")),
    "unit:core:currency-million@1": ('"$"0.###"m"', Decimal("1")),
    "unit:core:shares-million@1": ('0.###"m shares"', Decimal("1")),
    "unit:core:count@1": ("0", Decimal("1")),
}


def _ooxml_decimal(value: Decimal) -> str:
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return "0" if rendered in {"", "-0"} else rendered


def _exact_scalar_number_format(
    *, unit_id: str, machine_value: Mapping[str, Any]
) -> str | None:
    """Choose a deterministic format from semantic display-precision metadata."""

    if machine_value.get("kind") != "exact":
        return None
    semantic_value = Decimal(str(machine_value["value"]))
    explicit_places = machine_value.get("display_decimals")
    if explicit_places is not None:
        places = int(explicit_places)
        if places < 0:
            raise PromiseProgressWorkbookPreviewError(
                f"negative display precision {places!r} is invalid"
            )
        decimal_pattern = "" if places == 0 else "." + ("0" * places)
    elif semantic_value == semantic_value.to_integral_value():
        decimal_pattern = ""
    else:
        places = max(3, max(0, -semantic_value.normalize().as_tuple().exponent))
        decimal_pattern = "." + ("#" * places)

    if unit_id == "unit:core:percent@1":
        return f"0{decimal_pattern}%"
    if unit_id == "unit:core:currency-per-share@1":
        return '"$"0.00'
    if unit_id == "unit:core:currency-million@1":
        return f'"$"0{decimal_pattern}"m"'
    if unit_id == "unit:core:shares-million@1":
        return f'0{decimal_pattern}"m shares"'
    if unit_id == "unit:core:count@1":
        return f"0{decimal_pattern}"
    return None


def _product_v2_storage_spec(
    *,
    product: PromiseProgressProductV2,
    product_row: ProductRowV2 | None,
    field_role: str,
    presentation_text: str,
    machine_value: Any,
) -> tuple[str | None, str | None, str | None]:
    """Return a closed successor-only OOXML storage plan.

    Exact product scalars become numeric cells only when the closed number format
    replays the exact approved display.  Ranges, approximate values, composites,
    qualitative values, and progress labels such as ``YTD:`` remain intentional
    text; no error-ignore blanket is emitted.
    """

    if product.product_version != SUCCESSOR_PRODUCT_VERSION:
        return None, None, None
    if field_role == "source_date" and isinstance(machine_value, str):
        try:
            source_day = date.fromisoformat(machine_value)
        except ValueError:
            return None, None, None
        if presentation_text != machine_value:
            return None, None, None
        serial = (source_day - date(1899, 12, 30)).days
        return "date", str(serial), "yyyy-mm-dd"
    if (
        product_row is None
        or product_row.unit_id is None
        or not isinstance(machine_value, Mapping)
        or machine_value.get("kind") != "exact"
        or field_role
        not in {
            "current_guide",
            "previous_guide",
            "version_1",
            "version_2",
            "version_3",
            "version_4",
            "version_5",
            "actual",
            "progress",
        }
    ):
        return None, None, None
    format_spec = _PRODUCT_V2_NUMERIC_FORMAT_BY_UNIT.get(product_row.unit_id)
    if format_spec is None:
        return None, None, None
    if presentation_text != display_product_v2_value(
        machine_value, unit_id=product_row.unit_id
    ):
        return None, None, None
    _, factor = format_spec
    semantic_value = Decimal(str(machine_value["value"]))
    format_code = _exact_scalar_number_format(
        unit_id=product_row.unit_id, machine_value=machine_value
    )
    if format_code is None:
        return None, None, None
    stored = semantic_value * factor
    return "numeric", _ooxml_decimal(stored), format_code


def _product_v2_binding(
    *,
    product: PromiseProgressProductV2,
    presentation_row: ProductV2PresentationRow,
    product_row: ProductRowV2 | None,
    binding_kind: str,
    field_role: str,
    anchor_cell: str,
    display_range: str,
    text: str,
    machine_value: Any,
    style_source_cell: str,
    value_form: str,
    wrap_text: bool,
    horizontal_alignment: str,
    vertical_alignment: str,
    current_sources: tuple[str, ...] = (),
    predecessor_sources: tuple[str, ...] = (),
    canonical_text: str | None = None,
    display_transform_id: str = IDENTITY_TRANSFORM_ID,
) -> ProductV2WorkbookBinding:
    version_state = None if product_row is None else product_row.version_state
    status_code = None
    style_role = None
    if field_role == "status" and product_row is not None:
        status_code = product_row.status_code_at_update
        if status_code is None and text:
            raise PromiseProgressWorkbookPreviewError(
                "A visible Product@2 outcome Status lacks a typed status code"
            )
        if status_code is not None:
            style_role = f"status:{status_code}"
    elif field_role == "assessment_state" and version_state == "Needs Review":
        status_code = "needs_review"
        style_role = "outcome-review:needs-review"
    elif (
        binding_kind == "event_group"
        and product.product_version == SUCCESSOR_PRODUCT_VERSION
    ):
        style_role = "TimelineEventHeader"
    canonical_display_text = text if canonical_text is None else canonical_text
    storage_kind, stored_numeric_value, number_format_code = _product_v2_storage_spec(
        product=product,
        product_row=product_row,
        field_role=field_role,
        presentation_text=text,
        machine_value=machine_value,
    )
    semantic_owner = (
        f"{product.product_id}|structure={binding_kind}|row={presentation_row.row_number}|role={field_role}"
        if product_row is None
        else f"{product_row.row_id}|field={field_role}"
    )
    span_start, _ = _cell_parts(anchor_cell)
    span_end, _ = _cell_parts(display_range.split(":")[-1])
    measurement = (
        {"visible": False, "fit": True}
        if _column_number(span_start) > 10
        else measure_presentation_text(
            text,
            span_width=_column_number(span_end) - _column_number(span_start) + 1,
            wrap_text=wrap_text,
            excel_widths=_product_v2_excel_widths(span_start, span_end),
        )
    )
    lineage = {
        "product_row_lineage_digest": None if product_row is None else product_row.lineage_digest,
        "semantic_field_id": semantic_owner,
        "current_source_document_ids": list(current_sources),
        "predecessor_source_document_ids": list(predecessor_sources),
        "canonical_display_text": canonical_display_text,
        "presentation_text": text,
        "display_transform_id": display_transform_id,
        "event_start": presentation_row.event_start,
        "actual_period_id": None if product_row is None else product_row.actual_period_id,
        "actual_knowledge_date": None if product_row is None else product_row.actual_knowledge_date,
        "actual_source_document_ids": [] if product_row is None else list(product_row.actual_source_document_ids),
        "progress_period_id": None if product_row is None else product_row.progress_period_id,
        "progress_knowledge_date": None if product_row is None else product_row.progress_knowledge_date,
        "progress_source_document_ids": [] if product_row is None else list(product_row.progress_source_document_ids),
    }
    if product.product_version == SUCCESSOR_PRODUCT_VERSION:
        lineage.update(
            {
                "semantic_row_kind": None if product_row is None else product_row.row_kind,
                "status_target_guidance_version_id": (
                    None
                    if product_row is None
                    else product_row.status_target_guidance_version_id
                ),
                "status_actual_candidate_record_ids": (
                    []
                    if product_row is None
                    else list(product_row.status_actual_candidate_record_ids)
                ),
                "status_actual_period_id": (
                    None if product_row is None else product_row.status_actual_period_id
                ),
                "status_actual_knowledge_date": (
                    None
                    if product_row is None
                    else product_row.status_actual_knowledge_date
                ),
                "status_actual_source_document_ids": (
                    []
                    if product_row is None
                    else list(product_row.status_actual_source_document_ids)
                ),
                "status_actual_basis_id": (
                    None if product_row is None else product_row.status_actual_basis_id
                ),
                "status_actual_unit_id": (
                    None if product_row is None else product_row.status_actual_unit_id
                ),
                "status_rule_id": None if product_row is None else product_row.status_rule_id,
                "actual_derivation_rule_id": (
                    None if product_row is None else product_row.actual_derivation_rule_id
                ),
                "actual_derivation_input_record_ids": (
                    []
                    if product_row is None
                    else list(product_row.actual_derivation_input_record_ids)
                ),
                "actual_derivation_support_record_ids": (
                    []
                    if product_row is None
                    else list(product_row.actual_derivation_support_record_ids)
                ),
                "progress_derivation_rule_id": (
                    None if product_row is None else product_row.progress_derivation_rule_id
                ),
                "progress_derivation_input_record_ids": (
                    []
                    if product_row is None
                    else list(product_row.progress_derivation_input_record_ids)
                ),
                "progress_derivation_support_record_ids": (
                    []
                    if product_row is None
                    else list(product_row.progress_derivation_support_record_ids)
                ),
            }
        )
    if storage_kind is not None:
        lineage["workbook_storage"] = {
            "storage_kind": storage_kind,
            "stored_numeric_value": stored_numeric_value,
            "number_format_code": number_format_code,
            "presentation_text": text,
        }
    return ProductV2WorkbookBinding(
        binding_id=f"binding:promise-progress-product-v2:{_sha256_bytes(semantic_owner.encode('utf-8'))[:24]}@1",
        binding_kind=binding_kind,
        product_id=product.product_id,
        product_version=product.product_version,
        block_id=presentation_row.block_id,
        source_row_id=None if product_row is None else product_row.row_id,
        semantic_field_id=semantic_owner,
        field_role=field_role,
        sheet_name=SHEET_NAME,
        anchor_cell=anchor_cell,
        display_range=display_range,
        style_source_cell=style_source_cell,
        value_form=value_form,
        canonical_display_text=canonical_display_text,
        presentation_text=text,
        display_transform_id=display_transform_id,
        machine_value=_canonical(machine_value),
        version_state=version_state,
        status_code=status_code,
        style_role=style_role,
        horizon_period_id=None if product_row is None else product_row.horizon_period_id,
        horizon_label="" if product_row is None else product_row.horizon_label,
        stated_in_period_id=None if product_row is None else product_row.stated_in_period_id,
        stated_in_display="" if product_row is None else product_row.stated_in_display,
        event_id=None if product_row is None else product_row.event_id,
        event_start=bool(presentation_row.event_start and product_row is not None),
        wrap_text=wrap_text,
        horizontal_alignment=horizontal_alignment,
        vertical_alignment=vertical_alignment,
        current_source_document_ids=current_sources,
        predecessor_source_document_ids=predecessor_sources,
        actual_candidate_record_ids=(
            () if product_row is None else tuple(product_row.actual_candidate_record_ids)
        ),
        actual_period_id=None if product_row is None else product_row.actual_period_id,
        actual_knowledge_date=None if product_row is None else product_row.actual_knowledge_date,
        actual_source_document_ids=(
            () if product_row is None else tuple(product_row.actual_source_document_ids)
        ),
        progress_candidate_record_ids=(
            () if product_row is None else tuple(product_row.progress_candidate_record_ids)
        ),
        progress_period_id=None if product_row is None else product_row.progress_period_id,
        progress_knowledge_date=None if product_row is None else product_row.progress_knowledge_date,
        progress_source_document_ids=(
            () if product_row is None else tuple(product_row.progress_source_document_ids)
        ),
        lineage_digest=_sha256_bytes(canonical_json_bytes(lineage)),
        parity_locator=None if product_row is None else product_row.parity_locator,
        fit_measurement=measurement,
        storage_kind=storage_kind,
        stored_numeric_value=stored_numeric_value,
        number_format_code=number_format_code,
        semantic_row_kind=None if product_row is None else product_row.row_kind,
        status_target_guidance_version_id=(
            None if product_row is None else product_row.status_target_guidance_version_id
        ),
        status_actual_candidate_record_ids=(
            () if product_row is None else tuple(product_row.status_actual_candidate_record_ids)
        ),
        status_actual_period_id=(
            None if product_row is None else product_row.status_actual_period_id
        ),
        status_actual_knowledge_date=(
            None if product_row is None else product_row.status_actual_knowledge_date
        ),
        status_actual_source_document_ids=(
            () if product_row is None else tuple(product_row.status_actual_source_document_ids)
        ),
        status_actual_basis_id=(
            None if product_row is None else product_row.status_actual_basis_id
        ),
        status_actual_unit_id=(
            None if product_row is None else product_row.status_actual_unit_id
        ),
        status_rule_id=None if product_row is None else product_row.status_rule_id,
    )


def _finalize_product_v2_row_heights(
    bindings: Sequence[ProductV2WorkbookBinding],
    row_plan: Sequence[ProductV2PresentationRow],
    contract: PromiseProgressWorkbookDynamicPresentationContract,
) -> tuple[tuple[ProductV2WorkbookBinding, ...], tuple[tuple[int, int], ...]]:
    height_by_row: dict[int, int] = {}
    for row in row_plan:
        if row.row_kind == "spacer":
            height_by_row[row.row_number] = contract.spacer_height
            continue
        row_bindings = [
            binding
            for binding in bindings
            if _cell_parts(binding.anchor_cell)[1] == row.row_number
            and _column_number(_cell_parts(binding.anchor_cell)[0]) <= 10
        ]
        if any(not bool(binding.fit_measurement.get("width_fits", True)) for binding in row_bindings):
            raise PromiseProgressWorkbookPreviewError(
                f"layout_capacity_exceeded: non-wrapped width overflow on row {row.row_number}"
            )
        required_pixels = max(
            (int(binding.fit_measurement.get("required_height_pixels", 0)) for binding in row_bindings),
            default=0,
        )
        if row.row_kind in {
            "product_title",
            "product_metadata",
            "block_title",
            "group_title",
            "event_group",
            "table_header",
        }:
            maximum = 40 if row.row_kind == "product_metadata" else 24
            candidates = (24, 40) if maximum == 40 else (24,)
        elif row.row_kind == "product_row":
            # Real candidate rows remain on the 24pt compact rhythm.  A single
            # closed 40pt tier is reserved for an economically meaningful
            # metric/horizon label that cannot fit the generic cross-ticker
            # widths; provenance is never visible and cannot trigger it.
            maximum = 40
            candidates = (24, 40)
        else:
            maximum = (
                contract.timeline_max_height
                if row.block_id == PRODUCT_V2_TIMELINE_BLOCK_ID
                else contract.other_block_max_height
            )
            candidates = tuple(value for value in contract.row_height_tiers if value <= maximum)
        selected = next(
            (value for value in candidates if value * _PINNED_DPI / 72 >= required_pixels),
            None,
        )
        if selected is None:
            raise PromiseProgressWorkbookPreviewError(
                f"layout_capacity_exceeded: row {row.row_number} requires {required_pixels}px above {maximum}pt"
            )
        height_by_row[row.row_number] = selected
    updated: list[ProductV2WorkbookBinding] = []
    for binding in bindings:
        column_start, row_number = _cell_parts(binding.anchor_cell)
        if _column_number(column_start) > 10:
            updated.append(binding)
            continue
        column_end, _ = _cell_parts(binding.display_range.split(":")[-1])
        measurement = measure_presentation_text(
            binding.presentation_text,
            span_width=_column_number(column_end) - _column_number(column_start) + 1,
            wrap_text=binding.wrap_text,
            allocated_height_points=height_by_row[row_number],
            excel_widths=_product_v2_excel_widths(column_start, column_end),
        )
        if not measurement["fit"]:
            raise PromiseProgressWorkbookPreviewError(
                f"layout_capacity_exceeded: binding {binding.binding_id} does not fit"
            )
        updated.append(dataclasses.replace(binding, fit_measurement=measurement))
    return tuple(updated), tuple(sorted(height_by_row.items()))


def build_promise_progress_workbook_binding_plan_v2(
    product: PromiseProgressProductV2,
    *,
    design_lock_root: Path,
) -> PromiseProgressWorkbookBindingPlanV2:
    contract = product_v2_presentation_contract(product.product_version)
    design_lock = verify_design_lock(design_lock_root)
    structural = load_json_strict(
        design_lock_root / "promise_progress_structural_parity_contract.json"
    )
    row_plan = _product_v2_row_plan(product)
    product_rows = {row.row_id: row for block in product.blocks for row in block.rows}
    bindings: list[ProductV2WorkbookBinding] = []

    for presentation_row in row_plan:
        row_number = presentation_row.row_number
        if presentation_row.row_kind == "spacer":
            continue
        if presentation_row.row_kind in {
            "product_title",
            "product_metadata",
            "block_title",
            "group_title",
            "event_group",
        }:
            style_source = {
                "product_title": "A1",
                "product_metadata": "A2",
                "block_title": "A3",
                "group_title": "A11",
                "event_group": (
                    "A59"
                    if product.product_version == SUCCESSOR_PRODUCT_VERSION
                    else "A12"
                ),
            }[presentation_row.row_kind]
            bindings.append(
                _product_v2_binding(
                    product=product,
                    presentation_row=presentation_row,
                    product_row=None,
                    binding_kind=presentation_row.row_kind,
                    field_role=presentation_row.row_kind,
                    anchor_cell=f"A{row_number}",
                    display_range=f"A{row_number}:J{row_number}",
                    text=presentation_row.display_label,
                    machine_value=presentation_row.display_label,
                    style_source_cell=style_source,
                    value_form="heading" if presentation_row.row_kind != "product_metadata" else "metadata",
                    wrap_text=presentation_row.row_kind == "product_metadata",
                    horizontal_alignment="left",
                    vertical_alignment="center" if presentation_row.row_kind != "product_metadata" else "top",
                )
            )
            continue
        if presentation_row.row_kind == "table_header":
            if presentation_row.block_id is None:
                raise PromiseProgressWorkbookPreviewError("a Product@2 header lacks block identity")
            for field_role, label in presentation_row.header_labels:
                layout = contract.layout_for(presentation_row.block_id, field_role)
                source = "A12"
                wrap = bool(label) and not measure_presentation_text(
                    label,
                    span_width=layout.span_width,
                    wrap_text=False,
                    excel_widths=_product_v2_excel_widths(
                        layout.start_column, layout.end_column
                    ),
                )["width_fits"]
                bindings.append(
                    _product_v2_binding(
                        product=product,
                        presentation_row=presentation_row,
                        product_row=None,
                        binding_kind="table_header",
                        field_role=field_role,
                        anchor_cell=f"{layout.start_column}{row_number}",
                        display_range=layout.range_for_row(row_number),
                        text=label,
                        machine_value=label,
                        style_source_cell=source,
                        value_form="heading",
                        wrap_text=wrap,
                        horizontal_alignment="center" if field_role in {"status", "source_date"} else "left",
                        vertical_alignment="center",
                    )
                )
            continue
        if presentation_row.row_kind != "product_row" or presentation_row.source_row_id is None:
            raise PromiseProgressWorkbookPreviewError(
                f"unknown Product@2 presentation row kind {presentation_row.row_kind!r}"
            )
        product_row = product_rows[presentation_row.source_row_id]
        role_values = _product_v2_role_values(product_row, presentation_row)
        expected_roles = {
            layout.field_role
            for layout in contract.field_layouts
            if layout.block_id == product_row.block_id
        }
        if set(role_values) != expected_roles:
            raise PromiseProgressWorkbookPreviewError(
                f"Product@2 row {product_row.row_id} does not expose its closed display roles"
            )
        for field_role in [
            layout.field_role
            for layout in contract.field_layouts
            if layout.block_id == product_row.block_id
        ]:
            text, machine_value, current_sources, predecessor_sources = role_values[field_role]
            canonical_text = text
            display_transform_id = IDENTITY_TRANSFORM_ID
            if field_role == "change_type" and text in _PRODUCT_V2_COMPACT_CHANGE_LABELS:
                text = _PRODUCT_V2_COMPACT_CHANGE_LABELS[text]
                display_transform_id = PRODUCT_V2_COMPACT_CHANGE_TRANSFORM_ID
            layout = contract.layout_for(product_row.block_id, field_role)
            wrap = _product_v2_wrap(layout, text, machine_value)
            style_source = _PRODUCT_V2_STYLE_SOURCE[product_row.block_id][field_role]
            source_column, source_row_number = _cell_parts(style_source)
            if presentation_row.row_number % 2 == 0:
                style_source = f"{source_column}{source_row_number + 1}"
            bindings.append(
                _product_v2_binding(
                    product=product,
                    presentation_row=presentation_row,
                    product_row=product_row,
                    binding_kind="product_field",
                    field_role=field_role,
                    anchor_cell=f"{layout.start_column}{row_number}",
                    display_range=layout.range_for_row(row_number),
                    text=text,
                    machine_value=machine_value,
                    style_source_cell=style_source,
                    value_form="missing" if text == "" else "display",
                    wrap_text=wrap,
                    horizontal_alignment=layout.horizontal_alignment,
                    vertical_alignment=layout.vertical_alignment if wrap else "center",
                    current_sources=current_sources,
                    predecessor_sources=predecessor_sources,
                    canonical_text=canonical_text,
                    display_transform_id=display_transform_id,
                )
            )
        bindings.append(
            _product_v2_binding(
                product=product,
                presentation_row=presentation_row,
                product_row=product_row,
                binding_kind="row_trace",
                field_role="row_id",
                anchor_cell=f"O{row_number}",
                display_range=f"O{row_number}",
                text=product_row.row_id,
                machine_value=product_row.row_id,
                style_source_cell="O5",
                value_form="row_id",
                wrap_text=False,
                horizontal_alignment="preserve",
                vertical_alignment="preserve",
                current_sources=tuple(product_row.current_source_document_ids),
                predecessor_sources=tuple(product_row.predecessor_source_document_ids),
            )
        )

    ordered = tuple(
        sorted(bindings, key=lambda binding: (_cell_sort_key(binding.anchor_cell), binding.binding_id))
    )
    finalized, row_heights = _finalize_product_v2_row_heights(ordered, row_plan, contract)
    merge_set = {
        binding.display_range
        for binding in finalized
        if ":" in binding.display_range
    }
    merges = tuple(sorted(merge_set, key=lambda value: _cell_sort_key(_expand_range(value)[0])))
    last_row = row_plan[-1].row_number
    plan = PromiseProgressWorkbookBindingPlanV2(
        product_id=product.product_id,
        product_version=product.product_version,
        company_id=product.company_id,
        knowledge_cutoff=product.knowledge_cutoff,
        coverage_state=product.coverage_state,
        template_oracle_sha256=EXPECTED_ANF_WORKBOOK_SHA256,
        design_lock_manifest_sha256=design_lock["manifest_sha256"],
        sheet_name=SHEET_NAME,
        sheet_position_1_based=int(structural["oracle"]["sheet_position_1_based"]),
        sheet_part=str(structural["oracle"]["sheet_part"]),
        used_range=f"A1:O{last_row}",
        presentation_contract=contract,
        row_plan=row_plan,
        bindings=finalized,
        permitted_merges=merges,
        row_heights=row_heights,
        schema_id=(
            PRODUCT_V2_BINDING_PLAN_SCHEMA_ID
            if product.product_version == PRODUCT_V2_GOLDEN_VERSION
            else SUCCESSOR_PRODUCT_V2_BINDING_PLAN_SCHEMA_ID
        ),
    )
    validate_promise_progress_workbook_binding_plan_v2(
        product, plan, design_lock_root=design_lock_root
    )
    return plan


def validate_promise_progress_workbook_binding_plan_v2(
    product: PromiseProgressProductV2,
    plan: PromiseProgressWorkbookBindingPlanV2,
    *,
    design_lock_root: Path,
) -> None:
    contract = product_v2_presentation_contract(product.product_version)
    verify_design_lock(design_lock_root)
    expected_schema_id = (
        PRODUCT_V2_BINDING_PLAN_SCHEMA_ID
        if product.product_version == PRODUCT_V2_GOLDEN_VERSION
        else SUCCESSOR_PRODUCT_V2_BINDING_PLAN_SCHEMA_ID
    )
    if plan.schema_id != expected_schema_id:
        raise PromiseProgressWorkbookPreviewError("unsupported Product@2 binding-plan contract")
    if plan.presentation_contract.to_dict() != contract.to_dict():
        raise PromiseProgressWorkbookPreviewError("Product@2 binding plan changed its presentation contract")
    if (
        plan.product_id != product.product_id
        or plan.product_version != product.product_version
        or plan.company_id != product.company_id
        or plan.knowledge_cutoff != product.knowledge_cutoff
        or plan.coverage_state != product.coverage_state
    ):
        raise PromiseProgressWorkbookPreviewError("Product@2 binding identity differs from the product")
    expected_row_plan = _product_v2_row_plan(product)
    if plan.row_plan != expected_row_plan:
        raise PromiseProgressWorkbookPreviewError("Product@2 physical rows do not follow product order")
    row_numbers = [row.row_number for row in plan.row_plan]
    if row_numbers != list(range(1, len(row_numbers) + 1)):
        raise PromiseProgressWorkbookPreviewError("Product@2 physical rows are not contiguous")
    if plan.used_range != f"A1:O{row_numbers[-1]}":
        raise PromiseProgressWorkbookPreviewError("Product@2 used range is not derived from its row plan")
    if len({binding.binding_id for binding in plan.bindings}) != len(plan.bindings):
        raise PromiseProgressWorkbookPreviewError("Product@2 binding identities are not unique")
    destinations = [binding.anchor_cell for binding in plan.bindings]
    if len(destinations) != len(set(destinations)):
        raise PromiseProgressWorkbookPreviewError("Product@2 binding destinations are not unique")
    merged_children = _merged_child_map(plan.permitted_merges)
    if any(binding.anchor_cell in merged_children for binding in plan.bindings):
        raise PromiseProgressWorkbookPreviewError("Product@2 binding targets a merged non-anchor")
    if len(plan.permitted_merges) != len(set(plan.permitted_merges)):
        raise PromiseProgressWorkbookPreviewError("Product@2 merge identities are not unique")
    expected_merges = {
        binding.display_range for binding in plan.bindings if ":" in binding.display_range
    }
    if set(plan.permitted_merges) != expected_merges:
        raise PromiseProgressWorkbookPreviewError("Product@2 merge contract differs from role spans")
    if {row for row, _ in plan.row_heights} != set(row_numbers):
        raise PromiseProgressWorkbookPreviewError("Product@2 does not assign every physical row a height")
    row_plan_by_id = {
        row.source_row_id: row for row in plan.row_plan if row.source_row_id is not None
    }
    product_rows = {row.row_id: row for block in product.blocks for row in block.rows}
    if set(row_plan_by_id) != set(product_rows):
        raise PromiseProgressWorkbookPreviewError("Product@2 row plan does not cover each product row")
    trace_bindings = [binding for binding in plan.bindings if binding.binding_kind == "row_trace"]
    if (
        len(trace_bindings) != len(product_rows)
        or {binding.source_row_id for binding in trace_bindings} != set(product_rows)
        or any(_cell_parts(binding.anchor_cell)[0] != "O" for binding in trace_bindings)
    ):
        raise PromiseProgressWorkbookPreviewError("Product@2 O-column row trace is incomplete")
    visible_roles = [binding.field_role for binding in plan.bindings if binding.binding_kind == "product_field"]
    forbidden_visible_roles = {"version_state", "notes_source", "current_source", "source_note"}
    if forbidden_visible_roles & set(visible_roles):
        raise PromiseProgressWorkbookPreviewError(
            "Product@2 investor tables cannot expose lifecycle or provenance-detail columns"
        )
    expected_timeline_roles = {
        "metric",
        "previous_guide",
        "current_guide",
        "change_type",
        "actual",
        "progress",
        "status",
        "horizon",
        "stated_in",
        "source_date",
    }
    actual_timeline_roles = {
        binding.field_role
        for binding in plan.bindings
        if binding.binding_kind == "product_field"
        and binding.block_id == PRODUCT_V2_TIMELINE_BLOCK_ID
    }
    if actual_timeline_roles != expected_timeline_roles:
        raise PromiseProgressWorkbookPreviewError(
            "Product@2 timeline does not expose the closed investor role set"
        )
    timeline_headers = [
        row
        for row in plan.row_plan
        if row.row_kind == "table_header" and row.block_id == PRODUCT_V2_TIMELINE_BLOCK_ID
    ]
    if len(timeline_headers) != 1:
        raise PromiseProgressWorkbookPreviewError("Product@2 timeline must have one logical header")
    timeline_product_rows = next(
        block.rows for block in product.blocks if block.block_id == PRODUCT_V2_TIMELINE_BLOCK_ID
    )
    event_group_suffix = (
        "disclosures" if product.product_version == SUCCESSOR_PRODUCT_VERSION else "revisions"
    )
    expected_event_groups = [
        (event_id, f"{event_rows[0].stated_in_display} {event_group_suffix}")
        for event_id, event_rows in _group_contiguous_product_rows(timeline_product_rows)
    ]
    actual_event_groups = [
        (str(row.group_id), row.display_label)
        for row in plan.row_plan
        if row.row_kind == "event_group" and row.block_id == PRODUCT_V2_TIMELINE_BLOCK_ID
    ]
    if actual_event_groups != expected_event_groups:
        raise PromiseProgressWorkbookPreviewError(
            "Timeline reporting/update groups do not follow Product@2 event order"
        )
    for binding in plan.bindings:
        column, row_number = _cell_parts(binding.anchor_cell)
        if row_number > row_numbers[-1] or _column_number(column) > 15:
            raise PromiseProgressWorkbookPreviewError("Product@2 binding exceeds its dynamic used range")
        if column in {"L", "M", "N"}:
            raise PromiseProgressWorkbookPreviewError("L, M, and N are always blank")
        if column == "O" and binding.binding_kind != "row_trace":
            raise PromiseProgressWorkbookPreviewError("O may contain only product row IDs")
        if binding.display_transform_id not in contract.transform_ids:
            raise PromiseProgressWorkbookPreviewError("Product@2 binding invented a display transform")
        if (
            binding.display_transform_id == IDENTITY_TRANSFORM_ID
            and binding.presentation_text != binding.canonical_display_text
        ):
            raise PromiseProgressWorkbookPreviewError("Product@2 identity binding changed display text")
        if binding.display_transform_id == PRODUCT_V2_COMPACT_CHANGE_TRANSFORM_ID:
            if (
                binding.field_role != "change_type"
                or _PRODUCT_V2_COMPACT_CHANGE_LABELS.get(binding.canonical_display_text)
                != binding.presentation_text
            ):
                raise PromiseProgressWorkbookPreviewError(
                    "Product@2 compact change transform is not the closed reviewed mapping"
                )
        if product.product_version == PRODUCT_V2_GOLDEN_VERSION:
            if any(
                value is not None
                for value in (
                    binding.storage_kind,
                    binding.stored_numeric_value,
                    binding.number_format_code,
                )
            ):
                raise PromiseProgressWorkbookPreviewError(
                    "The immutable Product@2 2.0 plan acquired successor storage metadata"
                )
        elif binding.storage_kind is None:
            if binding.stored_numeric_value is not None or binding.number_format_code is not None:
                raise PromiseProgressWorkbookPreviewError(
                    "An intentional text binding carries partial numeric storage metadata"
                )
        elif (
            binding.storage_kind not in {"numeric", "date"}
            or binding.stored_numeric_value is None
            or binding.number_format_code is None
        ):
            raise PromiseProgressWorkbookPreviewError(
                "A successor numeric binding lacks its closed storage plan"
            )
        if not binding.fit_measurement.get("fit", False):
            raise PromiseProgressWorkbookPreviewError(
                f"layout_capacity_exceeded: Product@2 binding {binding.binding_id} does not fit"
            )
        if binding.binding_kind != "product_field":
            continue
        if binding.source_row_id is None or binding.source_row_id not in product_rows:
            raise PromiseProgressWorkbookPreviewError("Product@2 field binding lacks a product row owner")
        source_row = product_rows[binding.source_row_id]
        presentation_row = row_plan_by_id[binding.source_row_id]
        physical_row = presentation_row.row_number
        layout = contract.layout_for(source_row.block_id, binding.field_role)
        if (
            binding.block_id != source_row.block_id
            or binding.anchor_cell != f"{layout.start_column}{physical_row}"
            or binding.display_range != layout.range_for_row(physical_row)
        ):
            raise PromiseProgressWorkbookPreviewError("Product@2 field binding changed its role layout")
        if (
            binding.version_state != source_row.version_state
            or binding.horizon_period_id != source_row.horizon_period_id
            or binding.horizon_label != source_row.horizon_label
            or binding.stated_in_period_id != source_row.stated_in_period_id
            or binding.stated_in_display != source_row.stated_in_display
            or binding.event_id != source_row.event_id
            or binding.event_start != presentation_row.event_start
            or binding.actual_candidate_record_ids != source_row.actual_candidate_record_ids
            or binding.actual_period_id != source_row.actual_period_id
            or binding.actual_knowledge_date != source_row.actual_knowledge_date
            or binding.actual_source_document_ids != source_row.actual_source_document_ids
            or binding.progress_candidate_record_ids != source_row.progress_candidate_record_ids
            or binding.progress_period_id != source_row.progress_period_id
            or binding.progress_knowledge_date != source_row.progress_knowledge_date
            or binding.progress_source_document_ids != source_row.progress_source_document_ids
            or binding.semantic_row_kind != source_row.row_kind
            or binding.status_target_guidance_version_id
            != source_row.status_target_guidance_version_id
            or binding.status_actual_candidate_record_ids
            != source_row.status_actual_candidate_record_ids
            or binding.status_actual_period_id != source_row.status_actual_period_id
            or binding.status_actual_knowledge_date
            != source_row.status_actual_knowledge_date
            or binding.status_actual_source_document_ids
            != source_row.status_actual_source_document_ids
            or binding.status_actual_basis_id != source_row.status_actual_basis_id
            or binding.status_actual_unit_id != source_row.status_actual_unit_id
            or binding.status_rule_id != source_row.status_rule_id
        ):
            raise PromiseProgressWorkbookPreviewError(
                "Product@2 trace metadata differs from its semantic row"
            )
        if binding.predecessor_source_document_ids and source_row.block_id != PRODUCT_V2_TIMELINE_BLOCK_ID:
            raise PromiseProgressWorkbookPreviewError("predecessor evidence leaked outside the timeline trace")
        if binding.field_role == "status":
            expected_style_role = (
                None
                if source_row.status_code_at_update is None
                else f"status:{source_row.status_code_at_update}"
            )
            if (
                binding.status_code != source_row.status_code_at_update
                or binding.presentation_text != (source_row.status_at_update or "")
                or binding.style_role != expected_style_role
            ):
                raise PromiseProgressWorkbookPreviewError(
                    "Visible outcome Status differs from the Product@2 status assessment"
                )
    expected_order = [row.row_id for block in product.blocks for row in block.rows]
    physical_order = [
        row.source_row_id for row in plan.row_plan if row.row_kind == "product_row"
    ]
    if physical_order != expected_order:
        raise PromiseProgressWorkbookPreviewError("workbook plan reordered Product@2 rows")


def _assert_no_external_target_sheet_references(
    archive: ZipFile, *, sheet_part: str, sheet_name: str
) -> None:
    patterns = (
        f"'{sheet_name}'!".encode("utf-8"),
        f"{sheet_name}!".encode("utf-8"),
    )
    hits = []
    for name in archive.namelist():
        if name == sheet_part or not name.lower().endswith((".xml", ".rels")):
            continue
        payload = archive.read(name)
        if any(pattern in payload for pattern in patterns):
            hits.append(name)
    if hits:
        raise PromiseProgressWorkbookPreviewError(
            f"dynamic row allocation is unsafe because target-sheet references exist in {hits!r}"
        )


def _solid_fill_id(styles_root: ET.Element, rgb: str) -> int:
    fills = styles_root.find(f"{{{_MAIN_NS}}}fills")
    if fills is None:
        raise PromiseProgressWorkbookPreviewError("workbook styles have no fills collection")
    normalized = rgb.upper().removeprefix("#")
    if len(normalized) == 6:
        normalized = "FF" + normalized
    if len(normalized) != 8:
        raise PromiseProgressWorkbookPreviewError(f"invalid lifecycle fill {rgb!r}")
    for index, fill in enumerate(list(fills)):
        pattern = fill.find(f"{{{_MAIN_NS}}}patternFill")
        foreground = None if pattern is None else pattern.find(f"{{{_MAIN_NS}}}fgColor")
        if (
            pattern is not None
            and pattern.get("patternType") == "solid"
            and foreground is not None
            and str(foreground.get("rgb", "")).upper() == normalized
        ):
            return index
    fill = ET.Element(f"{{{_MAIN_NS}}}fill")
    pattern = ET.SubElement(fill, f"{{{_MAIN_NS}}}patternFill", {"patternType": "solid"})
    ET.SubElement(pattern, f"{{{_MAIN_NS}}}fgColor", {"rgb": normalized})
    ET.SubElement(pattern, f"{{{_MAIN_NS}}}bgColor", {"indexed": "64"})
    fills.append(fill)
    fills.set("count", str(len(list(fills))))
    return len(list(fills)) - 1


def _fill_variant(styles_root: ET.Element, base_style_id: int, rgb: str) -> int:
    cell_xfs = styles_root.find(f"{{{_MAIN_NS}}}cellXfs")
    if cell_xfs is None:
        raise PromiseProgressWorkbookPreviewError("workbook styles have no cellXfs collection")
    existing = list(cell_xfs)
    if base_style_id < 0 or base_style_id >= len(existing):
        raise PromiseProgressWorkbookPreviewError(f"base style {base_style_id} does not exist")
    variant = copy.deepcopy(existing[base_style_id])
    variant.set("fillId", str(_solid_fill_id(styles_root, rgb)))
    variant.set("applyFill", "1")
    serialized = ET.tostring(variant, encoding="utf-8")
    for index, candidate in enumerate(existing):
        if ET.tostring(candidate, encoding="utf-8") == serialized:
            return index
    cell_xfs.append(variant)
    cell_xfs.set("count", str(len(existing) + 1))
    return len(existing)


def _number_format_variant(
    styles_root: ET.Element, base_style_id: int, format_code: str
) -> int:
    """Append/reuse one custom numeric format without mutating legacy styles."""

    cell_xfs = styles_root.find(f"{{{_MAIN_NS}}}cellXfs")
    if cell_xfs is None:
        raise PromiseProgressWorkbookPreviewError("workbook styles have no cellXfs collection")
    xfs = list(cell_xfs)
    if base_style_id < 0 or base_style_id >= len(xfs):
        raise PromiseProgressWorkbookPreviewError(f"base style {base_style_id} does not exist")
    num_fmts = styles_root.find(f"{{{_MAIN_NS}}}numFmts")
    if num_fmts is None:
        num_fmts = ET.Element(f"{{{_MAIN_NS}}}numFmts", {"count": "0"})
        fonts = styles_root.find(f"{{{_MAIN_NS}}}fonts")
        styles_root.insert(0 if fonts is None else list(styles_root).index(fonts), num_fmts)
    existing_formats = list(num_fmts)
    matching = [
        int(node.get("numFmtId", "0"))
        for node in existing_formats
        if node.get("formatCode") == format_code
    ]
    if len(matching) > 1:
        raise PromiseProgressWorkbookPreviewError(
            f"number format {format_code!r} is duplicated in the style table"
        )
    if matching:
        format_id = matching[0]
    else:
        used_ids = {int(node.get("numFmtId", "0")) for node in existing_formats}
        format_id = max({163, *used_ids}) + 1
        num_fmts.append(
            ET.Element(
                f"{{{_MAIN_NS}}}numFmt",
                {"numFmtId": str(format_id), "formatCode": format_code},
            )
        )
        num_fmts.set("count", str(len(existing_formats) + 1))
    variant = copy.deepcopy(xfs[base_style_id])
    variant.set("numFmtId", str(format_id))
    variant.set("applyNumberFormat", "1")
    serialized = ET.tostring(variant, encoding="utf-8")
    for index, candidate in enumerate(xfs):
        if ET.tostring(candidate, encoding="utf-8") == serialized:
            return index
    cell_xfs.append(variant)
    cell_xfs.set("count", str(len(xfs) + 1))
    return len(xfs)


def _top_border_variant(
    styles_root: ET.Element, base_style_id: int, *, rgb: str = "9FBAD0"
) -> int:
    cell_xfs = styles_root.find(f"{{{_MAIN_NS}}}cellXfs")
    borders = styles_root.find(f"{{{_MAIN_NS}}}borders")
    if cell_xfs is None or borders is None:
        raise PromiseProgressWorkbookPreviewError(
            "workbook styles lack the collections required for event separators"
        )
    xfs = list(cell_xfs)
    border_rows = list(borders)
    if base_style_id < 0 or base_style_id >= len(xfs):
        raise PromiseProgressWorkbookPreviewError(f"base style {base_style_id} does not exist")
    base_border_id = int(xfs[base_style_id].get("borderId", "0"))
    if base_border_id < 0 or base_border_id >= len(border_rows):
        raise PromiseProgressWorkbookPreviewError(
            f"base style {base_style_id} references unknown border {base_border_id}"
        )
    border = copy.deepcopy(border_rows[base_border_id])
    top = border.find(f"{{{_MAIN_NS}}}top")
    if top is None:
        top = ET.Element(f"{{{_MAIN_NS}}}top")
        right = border.find(f"{{{_MAIN_NS}}}right")
        border.insert(2 if right is not None else 0, top)
    top.set("style", "thin")
    for child in list(top):
        top.remove(child)
    normalized = rgb.upper().removeprefix("#")
    if len(normalized) == 6:
        normalized = "FF" + normalized
    ET.SubElement(top, f"{{{_MAIN_NS}}}color", {"rgb": normalized})
    serialized_border = ET.tostring(border, encoding="utf-8")
    border_id = next(
        (
            index
            for index, candidate in enumerate(border_rows)
            if ET.tostring(candidate, encoding="utf-8") == serialized_border
        ),
        None,
    )
    if border_id is None:
        borders.append(border)
        borders.set("count", str(len(border_rows) + 1))
        border_id = len(border_rows)
    variant = copy.deepcopy(xfs[base_style_id])
    variant.set("borderId", str(border_id))
    variant.set("applyBorder", "1")
    serialized_xf = ET.tostring(variant, encoding="utf-8")
    for index, candidate in enumerate(xfs):
        if ET.tostring(candidate, encoding="utf-8") == serialized_xf:
            return index
    cell_xfs.append(variant)
    cell_xfs.set("count", str(len(xfs) + 1))
    return len(xfs)


def _product_v2_binding_style_id(
    binding: ProductV2WorkbookBinding,
    *,
    styles_root: ET.Element,
    original_style_ids: Mapping[str, int],
    status_styles: Mapping[str, int],
) -> int:
    base_style = (
        status_styles[binding.status_code]
        if binding.status_code is not None
        else original_style_ids.get(binding.style_source_cell, 0)
    )
    if _column_number(_cell_parts(binding.anchor_cell)[0]) > 10:
        return base_style
    if binding.field_role == "version_state" and binding.version_state is not None:
        base_style = _fill_variant(
            styles_root,
            base_style,
            str(_PRODUCT_V2_LIFECYCLE_STYLE[binding.version_state]["fill_rgb"]),
        )
    styled = _alignment_variant(
        styles_root,
        base_style,
        wrap_text=binding.wrap_text,
        horizontal=binding.horizontal_alignment,
        vertical=binding.vertical_alignment,
    )
    if binding.number_format_code is not None:
        styled = _number_format_variant(
            styles_root, styled, binding.number_format_code
        )
    if binding.event_start and binding.block_id == PRODUCT_V2_TIMELINE_BLOCK_ID:
        styled = _top_border_variant(styles_root, styled)
    return styled


def materialize_promise_progress_preview_v2(
    product: PromiseProgressProductV2,
    plan: PromiseProgressWorkbookBindingPlanV2,
    *,
    legacy_workbook: Path,
    output_workbook: Path,
    design_lock_root: Path,
) -> dict[str, Any]:
    validate_promise_progress_workbook_binding_plan_v2(
        product, plan, design_lock_root=design_lock_root
    )
    if output_workbook.exists():
        raise PromiseProgressWorkbookPreviewError(
            f"refusing to overwrite existing Product@2 preview: {output_workbook}"
        )
    legacy_sha = sha256_file(legacy_workbook)
    if legacy_sha != EXPECTED_ANF_WORKBOOK_SHA256:
        raise PromiseProgressWorkbookPreviewError(
            f"legacy workbook SHA differs from the frozen oracle: {legacy_sha}"
        )
    status_registry = load_json_strict(
        design_lock_root / "promise_progress_status_rule_registry.json"
    )
    output_workbook.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(legacy_workbook, "r") as source:
        position, sheet_part = _resolve_target_sheet(source, plan.sheet_name)
        if position != plan.sheet_position_1_based or sheet_part != plan.sheet_part:
            raise PromiseProgressWorkbookPreviewError("Product@2 target worksheet identity changed")
        _assert_no_external_target_sheet_references(
            source, sheet_part=sheet_part, sheet_name=plan.sheet_name
        )
        sheet_root = _parse_xml(source.read(sheet_part))
        styles_root = _parse_xml(source.read("xl/styles.xml"))
        original_style_ids = {
            ref: int(cell.get("s", "0"))
            for ref, cell in _worksheet_cell_map(sheet_root).items()
        }
        status_styles = _resolve_status_styles(
            source,
            sheet_root,
            status_registry,
            (
                binding.status_code
                for binding in plan.bindings
                if binding.status_code is not None
            ),
        )
        sheet_data = sheet_root.find(f"{{{_MAIN_NS}}}sheetData")
        if sheet_data is None:
            raise PromiseProgressWorkbookPreviewError("target worksheet has no sheetData")
        for row in list(sheet_data):
            sheet_data.remove(row)
        _set_product_v2_columns(sheet_root)
        _set_reviewed_merges(sheet_root, plan.permitted_merges)
        dimension = sheet_root.find(f"{{{_MAIN_NS}}}dimension")
        if dimension is None:
            dimension = ET.Element(f"{{{_MAIN_NS}}}dimension")
            sheet_root.insert(0, dimension)
        dimension.set("ref", plan.used_range)
        for row_number, height_points in plan.row_heights:
            _set_row_height(sheet_root, row_number, height_points)
        for binding in plan.bindings:
            cell = _get_or_create_cell(sheet_root, binding.anchor_cell)
            if binding.storage_kind in {"numeric", "date"}:
                if binding.stored_numeric_value is None or binding.number_format_code is None:
                    raise PromiseProgressWorkbookPreviewError(
                        "A numeric Product@2 binding lacks its closed storage metadata."
                    )
                _write_numeric_value(cell, binding.stored_numeric_value)
            else:
                _write_inline_string(cell, binding.presentation_text)
            style_id = _product_v2_binding_style_id(
                binding,
                styles_root=styles_root,
                original_style_ids=original_style_ids,
                status_styles=status_styles,
            )
            for destination in _expand_range(binding.display_range):
                _get_or_create_cell(sheet_root, destination).set("s", str(style_id))
        cells = _worksheet_cell_map(sheet_root)
        for merged_range in plan.permitted_merges:
            for child in _expand_range(merged_range)[1:]:
                if _cell_text(cells.get(child)):
                    raise PromiseProgressWorkbookPreviewError(
                        f"merged non-anchor cell {child} is not blank"
                    )
        members = {info.filename: source.read(info.filename) for info in source.infolist()}
        members[sheet_part] = _serialize_xml(sheet_root)
        members["xl/styles.xml"] = _serialize_xml(styles_root)
        source_infos = source.infolist()
        source_comment = source.comment

    handle, temporary_name = tempfile.mkstemp(
        prefix=f".{output_workbook.stem}.", suffix=output_workbook.suffix, dir=output_workbook.parent
    )
    os.close(handle)
    temporary_path = Path(temporary_name)
    try:
        with ZipFile(temporary_path, "w") as output:
            output.comment = source_comment
            for info in source_infos:
                output.writestr(info, members[info.filename])
        os.replace(temporary_path, output_workbook)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()
    return {
        "legacy_workbook_sha256": legacy_sha,
        "preview_workbook": str(output_workbook),
        "preview_workbook_sha256": sha256_file(output_workbook),
        "canonical_workbook_content_sha256": canonical_workbook_content_sha256(output_workbook),
        "target_sheet_semantic_sha256": target_sheet_semantic_sha256_v2(output_workbook, plan),
        "binding_plan_sha256": plan.lineage_digest,
        "presentation_contract_sha256": plan.presentation_contract.to_dict()["contract_digest"],
        "written_binding_count": len(plan.bindings),
        "used_range": plan.used_range,
        "changed_ooxml_parts": [sheet_part, "xl/styles.xml"],
    }


def target_sheet_semantic_sha256_v2(
    workbook_path: Path, plan: PromiseProgressWorkbookBindingPlanV2
) -> str:
    snapshot, _ = _workbook_sheet_snapshot(workbook_path, plan.sheet_name)
    payload = {
        "sheet_name": plan.sheet_name,
        "used_range": plan.used_range,
        "bindings": [
            {
                "binding_id": binding.binding_id,
                "destination": binding.anchor_cell,
                "display_range": binding.display_range,
                "value": snapshot["cells"].get(binding.anchor_cell, {}).get("value", ""),
                "style_id": snapshot["cells"].get(binding.anchor_cell, {}).get("style_id", 0),
            }
            for binding in plan.bindings
        ],
        "row_heights": list(plan.row_heights),
        "merges": list(plan.permitted_merges),
        "presentation_contract_sha256": plan.presentation_contract.to_dict()["contract_digest"],
    }
    return _sha256_bytes(canonical_json_bytes(payload))


def validate_preview_structure_v2(
    *,
    legacy_workbook: Path,
    preview_workbook: Path,
    plan: PromiseProgressWorkbookBindingPlanV2,
) -> dict[str, Any]:
    with ZipFile(legacy_workbook, "r") as legacy, ZipFile(preview_workbook, "r") as preview:
        legacy_position, legacy_part = _resolve_target_sheet(legacy, plan.sheet_name)
        preview_position, preview_part = _resolve_target_sheet(preview, plan.sheet_name)
        legacy_members = {name: legacy.read(name) for name in legacy.namelist()}
        preview_members = {name: preview.read(name) for name in preview.namelist()}
        changed_parts = sorted(
            name
            for name in set(legacy_members) | set(preview_members)
            if legacy_members.get(name) != preview_members.get(name)
        )
        unexpected_parts = sorted(set(changed_parts) - {legacy_part, "xl/styles.xml"})
        legacy_root = _parse_xml(legacy_members[legacy_part])
        preview_root = _parse_xml(preview_members[preview_part])
        shared = _shared_strings(preview)
        cells = _worksheet_cell_map(preview_root)
        style_palette = _style_palette(preview)
        style_fonts = _style_font_properties(preview)
        top_borders = _style_top_border_map(preview)
        legacy_styles = _parse_xml(legacy_members["xl/styles.xml"])
        preview_styles = _parse_xml(preview_members["xl/styles.xml"])
        legacy_xfs_node = legacy_styles.find(f"{{{_MAIN_NS}}}cellXfs")
        preview_xfs_node = preview_styles.find(f"{{{_MAIN_NS}}}cellXfs")
        legacy_xfs = [] if legacy_xfs_node is None else list(legacy_xfs_node)
        preview_xfs = [] if preview_xfs_node is None else list(preview_xfs_node)
        original_styles_unchanged = len(preview_xfs) >= len(legacy_xfs) and all(
            ET.tostring(left, encoding="utf-8") == ET.tostring(right, encoding="utf-8")
            for left, right in zip(legacy_xfs, preview_xfs)
        )
        columns: dict[str, dict[str, Any]] = {}
        for node in preview_root.findall(f".//{{{_MAIN_NS}}}col"):
            for number in range(int(node.get("min", "0")), int(node.get("max", "0")) + 1):
                if 1 <= number <= 15:
                    columns[_column_name(number)] = {
                        "width": float(node.get("width", "0")),
                        "hidden": node.get("hidden") == "1",
                    }
        expected_columns = {
            "A": {"width": 31.5, "hidden": False},
            **{column: {"width": 22.5, "hidden": False} for column in "BCDEFGHIJ"},
            "K": {"width": 4.0, "hidden": True},
            "L": {"width": 4.0, "hidden": True},
            "M": {"width": 4.0, "hidden": True},
            "N": {"width": 4.0, "hidden": True},
            "O": {"width": 13.0, "hidden": True},
        }
        row_nodes = {
            int(node.get("r", "0")): node
            for node in preview_root.findall(f".//{{{_MAIN_NS}}}row")
        }
        actual_heights = {
            number: int(float(row_nodes[number].get("ht", "0")))
            for number, _ in plan.row_heights
            if number in row_nodes
        }
        dimension = preview_root.find(f"{{{_MAIN_NS}}}dimension")
        merges = [node.get("ref") for node in preview_root.findall(f".//{{{_MAIN_NS}}}mergeCell")]
        merged_nonblank = {
            child: _cell_text(cells.get(child), shared)
            for merged_range in merges
            for child in _expand_range(str(merged_range))[1:]
            if _cell_text(cells.get(child), shared)
        }
        k_values = {
            ref: _cell_text(cell, shared)
            for ref, cell in cells.items()
            if _cell_parts(ref)[0] == "K" and _cell_text(cell, shared)
        }
        l_values = {
            ref: _cell_text(cell, shared)
            for ref, cell in cells.items()
            if _cell_parts(ref)[0] == "L" and _cell_text(cell, shared)
        }
        m_values = {
            ref: _cell_text(cell, shared)
            for ref, cell in cells.items()
            if _cell_parts(ref)[0] == "M" and _cell_text(cell, shared)
        }
        n_values = {
            ref: _cell_text(cell, shared)
            for ref, cell in cells.items()
            if _cell_parts(ref)[0] == "N" and _cell_text(cell, shared)
        }
        expected_o = {
            binding.anchor_cell: binding.presentation_text
            for binding in plan.bindings
            if binding.binding_kind == "row_trace"
        }
        actual_o = {
            ref: _cell_text(cell, shared)
            for ref, cell in cells.items()
            if _cell_parts(ref)[0] == "O" and _cell_text(cell, shared)
        }
        panes = [dict(node.attrib) for node in preview_root.findall(f".//{{{_MAIN_NS}}}pane")]
        views = [dict(node.attrib) for node in preview_root.findall(f".//{{{_MAIN_NS}}}sheetView")]
        lifecycle_bindings = [
            binding
            for binding in plan.bindings
            if binding.binding_kind == "product_field" and binding.field_role == "version_state"
        ]
        lifecycle_styles = {
            binding.binding_id: {
                "version_state": binding.version_state,
                "style_role": binding.style_role,
                "fill": style_palette.get(
                    int(cells[binding.anchor_cell].get("s", "0"))
                ),
            }
            for binding in lifecycle_bindings
        }
        event_start_bindings = [
            binding
            for binding in plan.bindings
            if binding.binding_kind == "product_field"
            and binding.block_id == PRODUCT_V2_TIMELINE_BLOCK_ID
            and binding.event_start
        ]
        event_start_borders = {
            binding.binding_id: top_borders.get(
                int(cells[binding.anchor_cell].get("s", "0")), {}
            )
            for binding in event_start_bindings
        }
        event_header_bindings = [
            binding
            for binding in plan.bindings
            if binding.binding_kind == "event_group"
        ]
        event_header_styles = {
            binding.binding_id: {
                "destination": binding.anchor_cell,
                "style_role": binding.style_role,
                "fill": style_palette.get(
                    int(cells[binding.anchor_cell].get("s", "0"))
                ),
                "font": style_fonts.get(
                    int(cells[binding.anchor_cell].get("s", "0")), {}
                ),
            }
            for binding in event_header_bindings
        }
        validations = {
            "sheet_identity": (
                legacy_position == preview_position == plan.sheet_position_1_based
                and legacy_part == preview_part == plan.sheet_part
            ),
            "approved_ooxml_parts_only": not unexpected_parts
            and set(changed_parts) == {preview_part, "xl/styles.xml"},
            "existing_styles_unchanged_append_only": original_styles_unchanged,
            "dynamic_used_range": dimension is not None and dimension.get("ref") == plan.used_range,
            "contiguous_dynamic_rows": sorted(row_nodes) == list(range(1, len(plan.row_plan) + 1)),
            "reviewed_column_grid": columns == expected_columns,
            "reviewed_row_heights": actual_heights == dict(plan.row_heights),
            "reviewed_merges": merges == list(plan.permitted_merges),
            "merged_non_anchor_cells_blank": not merged_nonblank,
            "k_blank": not k_values,
            "l_blank": not l_values,
            "m_blank": not m_values,
            "n_blank": not n_values,
            "o_row_ids_exact": actual_o == expected_o,
            "freeze_pane": any(
                pane.get("topLeftCell") == "A2" and pane.get("state") == "frozen"
                for pane in panes
            ),
            "zoom": any(view.get("zoomScale") == "112" for view in views),
            "feature_counts_unchanged": _feature_counts(preview_root) == _feature_counts(legacy_root),
            "no_formulas": _feature_counts(preview_root)["formulas"] == 0,
            "lifecycle_state_not_in_investor_cells": not lifecycle_bindings,
            "outcome_status_roles_are_typed": all(
                (
                    binding.presentation_text == ""
                    and binding.status_code is None
                    and binding.style_role is None
                )
                or (
                    binding.status_code is not None
                    and binding.style_role == f"status:{binding.status_code}"
                )
                for binding in plan.bindings
                if binding.binding_kind == "product_field" and binding.field_role == "status"
            ),
            "event_start_separators": bool(event_start_borders)
            and all(
                row.get("style") == "thin" and row.get("rgb") == "FF9FBAD0"
                for row in event_start_borders.values()
            ),
        }
        if plan.product_version == SUCCESSOR_PRODUCT_VERSION:
            validations.update(
                {
                    "timeline_event_header_role": bool(event_header_styles)
                    and all(
                        row["style_role"] == "TimelineEventHeader"
                        and row["fill"] == "#5B9BD5"
                        and row["font"].get("bold") is True
                        and row["font"].get("rgb") == "#FFFFFF"
                        for row in event_header_styles.values()
                    ),
                    "numeric_storage_is_scoped": all(
                        (
                            cell.get("t") == "n"
                            if binding.storage_kind in {"numeric", "date"}
                            else cell.get("t") in {None, "inlineStr"}
                        )
                        for binding in plan.bindings
                        if (cell := cells.get(binding.anchor_cell)) is not None
                    ),
                    "no_ignored_error_suppression": not preview_root.findall(
                        f".//{{{_MAIN_NS}}}ignoredErrors"
                    ),
                }
            )
    result = {
        "schema_id": (
            PRODUCT_V2_STRUCTURAL_VALIDATION_SCHEMA_ID
            if plan.product_version == PRODUCT_V2_GOLDEN_VERSION
            else SUCCESSOR_PRODUCT_V2_STRUCTURAL_VALIDATION_SCHEMA_ID
        ),
        "legacy_workbook_sha256": sha256_file(legacy_workbook),
        "preview_workbook_sha256": sha256_file(preview_workbook),
        "changed_ooxml_parts": changed_parts,
        "unexpected_ooxml_parts": unexpected_parts,
        "used_range": None if dimension is None else dimension.get("ref"),
        "column_contract": columns,
        "row_heights": {str(row): height for row, height in actual_heights.items()},
        "merge_count": len(merges),
        "k_nonblank": k_values,
        "l_nonblank": l_values,
        "m_nonblank": m_values,
        "n_nonblank": n_values,
        "o_row_ids": actual_o,
        "feature_counts": _feature_counts(preview_root),
        "lifecycle_styles": lifecycle_styles,
        "event_start_borders": event_start_borders,
        "validations": validations,
        "passed": all(validations.values()),
    }
    if plan.product_version == SUCCESSOR_PRODUCT_VERSION:
        result["timeline_event_header_styles"] = event_header_styles
    result["validation_digest"] = _sha256_bytes(canonical_json_bytes(result))
    return result


def validate_preview_semantics_v2(
    product: PromiseProgressProductV2,
    plan: PromiseProgressWorkbookBindingPlanV2,
    *,
    preview_workbook: Path,
) -> dict[str, Any]:
    snapshot, _ = _workbook_sheet_snapshot(preview_workbook, plan.sheet_name)
    results = []
    for binding in plan.bindings:
        cell_snapshot = snapshot["cells"].get(binding.anchor_cell, {})
        stored_value = cell_snapshot.get("value", "")
        cell_type = cell_snapshot.get("cell_type")
        actual_number_format_code = cell_snapshot.get("number_format_code")
        actual_number_format_id = cell_snapshot.get("number_format_id")
        if binding.storage_kind in {"numeric", "date"}:
            storage_pass = (
                cell_type == "n"
                and binding.stored_numeric_value is not None
                and Decimal(str(stored_value))
                == Decimal(binding.stored_numeric_value)
            )
            format_pass = (
                binding.number_format_code is not None
                and actual_number_format_code == binding.number_format_code
            )
            try:
                actual = replay_ooxml_numeric_display(
                    str(stored_value), str(actual_number_format_code)
                )
                format_replay_pass = True
            except (ArithmeticError, ValueError, PromiseProgressWorkbookPreviewError):
                actual = str(stored_value)
                format_replay_pass = False
        else:
            storage_pass = cell_type in {None, "inlineStr"}
            format_pass = True
            format_replay_pass = True
            actual = str(stored_value)
        entry = {
                "binding_id": binding.binding_id,
                "binding_kind": binding.binding_kind,
                "source_row_id": binding.source_row_id,
                "semantic_field_id": binding.semantic_field_id,
                "field_role": binding.field_role,
                "destination": binding.anchor_cell,
                "display_range": binding.display_range,
                "canonical_display_value": binding.canonical_display_text,
                "display_transform_id": binding.display_transform_id,
                "expected_display_value": binding.presentation_text,
                "written_display_value": actual,
                "version_state": binding.version_state,
                "horizon_period_id": binding.horizon_period_id,
                "horizon_label": binding.horizon_label,
                "stated_in_period_id": binding.stated_in_period_id,
                "stated_in_display": binding.stated_in_display,
                "event_id": binding.event_id,
                "event_start": binding.event_start,
                "current_source_document_ids": list(binding.current_source_document_ids),
                "predecessor_source_document_ids": list(binding.predecessor_source_document_ids),
                "actual_candidate_record_ids": list(binding.actual_candidate_record_ids),
                "actual_period_id": binding.actual_period_id,
                "actual_knowledge_date": binding.actual_knowledge_date,
                "actual_source_document_ids": list(binding.actual_source_document_ids),
                "progress_candidate_record_ids": list(binding.progress_candidate_record_ids),
                "progress_period_id": binding.progress_period_id,
                "progress_knowledge_date": binding.progress_knowledge_date,
                "progress_source_document_ids": list(binding.progress_source_document_ids),
                "semantic_row_kind": binding.semantic_row_kind,
                "status_target_guidance_version_id": (
                    binding.status_target_guidance_version_id
                ),
                "status_actual_candidate_record_ids": list(
                    binding.status_actual_candidate_record_ids
                ),
                "status_actual_period_id": binding.status_actual_period_id,
                "status_actual_knowledge_date": binding.status_actual_knowledge_date,
                "status_actual_source_document_ids": list(
                    binding.status_actual_source_document_ids
                ),
                "status_actual_basis_id": binding.status_actual_basis_id,
                "status_actual_unit_id": binding.status_actual_unit_id,
                "status_rule_id": binding.status_rule_id,
                "pass": (
                    storage_pass
                    and format_pass
                    and format_replay_pass
                    and actual == binding.presentation_text
                ),
            }
        if product.product_version == SUCCESSOR_PRODUCT_VERSION:
            entry.update(
                {
                    "stored_cell_value": stored_value,
                    "stored_cell_type": cell_type,
                    "storage_kind": binding.storage_kind,
                    "number_format_code": binding.number_format_code,
                    "planned_number_format_code": binding.number_format_code,
                    "actual_number_format_id": actual_number_format_id,
                    "actual_number_format_code": actual_number_format_code,
                    "independently_replayed_display": actual,
                    "number_format_identity_pass": format_pass,
                    "number_format_replay_pass": format_replay_pass,
                }
            )
        results.append(entry)
    product_rows = [row for block in product.blocks for row in block.rows]
    trace_rows = [row for row in results if row["binding_kind"] == "row_trace"]
    visible_text = " ".join(
        row["written_display_value"]
        for row in results
        if _column_number(_cell_parts(row["destination"])[0]) <= 10
    ).casefold()
    engine_jargon = (
        "guidanceseries",
        "canonical",
        "resolver",
        "binding",
        "occurrence",
        "legacy parity only",
        "unsupported mapping",
        "unresolved comparison",
        "source-native product@2 candidate",
        "product@2 candidate",
    )
    validations = {
        "product_identity": plan.product_id == product.product_id and plan.product_version == product.product_version,
        "all_values_exact": all(row["pass"] for row in results),
        "row_trace_count": len(trace_rows) == len(product_rows),
        "row_order_exact": [row["source_row_id"] for row in trace_rows]
        == [row.row_id for row in product_rows],
        "eligible_rows_only": all(row.eligible for row in product_rows),
        "no_empty_historical_groups": all(
            any(value.source_row_id is not None and value.group_id == row.group_id for value in plan.row_plan)
            for row in plan.row_plan
            if row.row_kind == "group_title"
        ),
        "open_rows_current_only": all(
            row.version_state == "Current"
            for row in next(
                block for block in product.blocks if block.block_id == PRODUCT_V2_OPEN_BLOCK_ID
            ).rows
        ),
        "one_credibility_empty_state": len(
            next(
                block for block in product.blocks if block.block_id == PRODUCT_V2_CREDIBILITY_BLOCK_ID
            ).rows
        )
        == 1,
        "no_visible_lifecycle_or_provenance_columns": not any(
            row["field_role"] in {"version_state", "notes_source", "current_source", "source_note"}
            for row in results
        ),
        "timeline_context_fields_visible": {
            "horizon", "stated_in", "source_date"
        }.issubset(
            {
                binding.field_role
                for binding in plan.bindings
                if binding.binding_kind == "product_field"
                and binding.block_id == PRODUCT_V2_TIMELINE_BLOCK_ID
            }
        ),
        "no_investor_engine_jargon": not any(value in visible_text for value in engine_jargon),
        "current_predecessor_sources_separate": all(
            set(row.current_source_document_ids).isdisjoint(row.predecessor_source_document_ids)
            for row in next(
                block for block in product.blocks if block.block_id == PRODUCT_V2_TIMELINE_BLOCK_ID
            ).rows
            if row.predecessor_source_document_ids
        ),
        "missing_remains_blank": all(
            row["written_display_value"] == ""
            for row, binding in zip(results, plan.bindings)
            if binding.value_form == "missing"
        ),
        "lifecycle_state_retained_in_trace": all(
            binding.version_state == row.version_state
            for row in product_rows
            for binding in plan.bindings
            if binding.source_row_id == row.row_id
        ),
        "timeline_source_lineage_retained_off_ui": all(
            binding.current_source_document_ids
            for binding in plan.bindings
            if binding.binding_kind == "product_field"
            and binding.block_id == PRODUCT_V2_TIMELINE_BLOCK_ID
        ),
        "investor_metadata_excludes_candidate_identity": all(
            "product@2" not in binding.presentation_text.casefold()
            and "candidate" not in binding.presentation_text.casefold()
            for binding in plan.bindings
            if binding.binding_kind == "product_metadata"
        ),
    }
    result = {
        "schema_id": (
            PRODUCT_V2_SEMANTIC_VALIDATION_SCHEMA_ID
            if product.product_version == PRODUCT_V2_GOLDEN_VERSION
            else SUCCESSOR_PRODUCT_V2_SEMANTIC_VALIDATION_SCHEMA_ID
        ),
        "product_id": product.product_id,
        "product_sha256": promise_progress_product_v2_sha256(product),
        "binding_plan_sha256": plan.lineage_digest,
        "binding_count": len(results),
        "product_row_count": len(product_rows),
        "results": results,
        "validations": validations,
        "passed": all(validations.values()),
    }
    result["validation_digest"] = _sha256_bytes(canonical_json_bytes(result))
    return result


def validate_preview_visual_fit_v2(
    *,
    preview_workbook: Path,
    plan: PromiseProgressWorkbookBindingPlanV2,
) -> dict[str, Any]:
    with ZipFile(preview_workbook, "r") as archive:
        _, part = _resolve_target_sheet(archive, plan.sheet_name)
        root = _parse_xml(archive.read(part))
        shared = _shared_strings(archive)
        cells = _worksheet_cell_map(root)
        alignments = _style_alignment_map(archive)
        row_nodes = {
            int(node.get("r", "0")): node
            for node in root.findall(f".//{{{_MAIN_NS}}}row")
        }
        records = []
        for binding in plan.bindings:
            column, row_number = _cell_parts(binding.anchor_cell)
            if _column_number(column) > 10:
                continue
            end_column, _ = _cell_parts(binding.display_range.split(":")[-1])
            cell = cells.get(binding.anchor_cell)
            style_id = 0 if cell is None else int(cell.get("s", "0"))
            alignment = alignments.get(style_id, {})
            height = int(float(row_nodes[row_number].get("ht", "0")))
            rendered_text = (
                binding.presentation_text
                if binding.storage_kind in {"numeric", "date"}
                else _cell_text(cell, shared)
            )
            measurement = measure_presentation_text(
                rendered_text,
                span_width=_column_number(end_column) - _column_number(column) + 1,
                wrap_text=bool(alignment.get("wrap_text", False)),
                allocated_height_points=height,
                excel_widths=_product_v2_excel_widths(column, end_column),
            )
            overflow = not bool(alignment.get("wrap_text", False)) and not measurement["width_fits"]
            records.append(
                {
                    "binding_id": binding.binding_id,
                    "destination": binding.anchor_cell,
                    "display_range": binding.display_range,
                    "field_role": binding.field_role,
                    "row_height_points": height,
                    "required_width_pixels": measurement["required_width_pixels"],
                    "required_height_pixels": measurement["required_height_pixels"],
                    "allocated_width_pixels": measurement["effective_width_pixels"],
                    "allocated_height_pixels": measurement["allocated_height_pixels"],
                    "wrap_state": bool(alignment.get("wrap_text", False)),
                    "expected_wrap": binding.wrap_text,
                    "overflow_dependency": overflow,
                    "shrink_to_fit": bool(alignment.get("shrink_to_fit", False)),
                    "fit": measurement["fit"],
                    "pass": measurement["fit"]
                    and not overflow
                    and not bool(alignment.get("shrink_to_fit", False))
                    and bool(alignment.get("wrap_text", False)) == binding.wrap_text,
                }
            )
    validations = {
        "zero_clipped_visible_fields": all(row["fit"] for row in records),
        "zero_overflow_dependency": not any(row["overflow_dependency"] for row in records),
        "zero_shrink_to_fit": not any(row["shrink_to_fit"] for row in records),
        "wrap_contract_exact": all(row["wrap_state"] == row["expected_wrap"] for row in records),
        "timeline_height_at_most_56": all(
            row["row_height_points"] <= 56
            for row in records
            if next(
                binding for binding in plan.bindings if binding.binding_id == row["binding_id"]
            ).block_id
            == PRODUCT_V2_TIMELINE_BLOCK_ID
        ),
        "all_records_pass": all(row["pass"] for row in records),
    }
    result = {
        "schema_id": (
            PRODUCT_V2_VISUAL_VALIDATION_SCHEMA_ID
            if plan.product_version == PRODUCT_V2_GOLDEN_VERSION
            else SUCCESSOR_PRODUCT_V2_VISUAL_VALIDATION_SCHEMA_ID
        ),
        "preview_workbook_sha256": sha256_file(preview_workbook),
        "binding_plan_sha256": plan.lineage_digest,
        "record_count": len(records),
        "clipped_visible_field_count": sum(1 for row in records if not row["fit"]),
        "overflow_dependency_count": sum(1 for row in records if row["overflow_dependency"]),
        "records": records,
        "validations": validations,
        "passed": all(validations.values()),
    }
    result["validation_digest"] = _sha256_bytes(canonical_json_bytes(result))
    return result


def build_workbook_trace_v2(
    product: PromiseProgressProductV2,
    plan: PromiseProgressWorkbookBindingPlanV2,
    *,
    preview_workbook: Path,
) -> dict[str, Any]:
    snapshot, _ = _workbook_sheet_snapshot(preview_workbook, plan.sheet_name)
    height_by_row = dict(plan.row_heights)
    product_rows = {
        row.row_id: row for block in product.blocks for row in block.rows
    }
    product_sha256 = promise_progress_product_v2_sha256(product)
    binding_plan_sha256 = plan.lineage_digest
    records = []
    for binding in plan.bindings:
        cell = snapshot["cells"].get(binding.anchor_cell, {})
        raw_value = cell.get("value", "")
        record = {
            **binding.to_dict(),
            "product_sha256": product_sha256,
            "binding_plan_sha256": binding_plan_sha256,
            "physical_row": _cell_parts(binding.anchor_cell)[1],
            "row_height_points": height_by_row[_cell_parts(binding.anchor_cell)[1]],
            "written_display_value": (
                binding.presentation_text
                if binding.storage_kind in {"numeric", "date"}
                else raw_value
            ),
            "written_style_id": cell.get("style_id", 0),
        }
        if product.product_version == SUCCESSOR_PRODUCT_VERSION:
            source_row = product_rows.get(str(binding.source_row_id))
            record.update(
                {
                    "stored_cell_value": raw_value,
                    "stored_cell_type": cell.get("cell_type"),
                    "product_unit_id": None if source_row is None else source_row.unit_id,
                    "actual_derivation_rule_id": (
                        None if source_row is None else source_row.actual_derivation_rule_id
                    ),
                    "actual_derivation_input_record_ids": (
                        []
                        if source_row is None
                        else list(source_row.actual_derivation_input_record_ids)
                    ),
                    "actual_derivation_support_record_ids": (
                        []
                        if source_row is None
                        else list(source_row.actual_derivation_support_record_ids)
                    ),
                    "progress_derivation_rule_id": (
                        None if source_row is None else source_row.progress_derivation_rule_id
                    ),
                    "progress_derivation_input_record_ids": (
                        []
                        if source_row is None
                        else list(source_row.progress_derivation_input_record_ids)
                    ),
                    "progress_derivation_support_record_ids": (
                        []
                        if source_row is None
                        else list(source_row.progress_derivation_support_record_ids)
                    ),
                }
            )
        records.append(record)
    result = {
        "schema_id": (
            PRODUCT_V2_WORKBOOK_TRACE_SCHEMA_ID
            if product.product_version == PRODUCT_V2_GOLDEN_VERSION
            else SUCCESSOR_PRODUCT_V2_WORKBOOK_TRACE_SCHEMA_ID
        ),
        "product_id": product.product_id,
        "product_sha256": product_sha256,
        "binding_plan_sha256": binding_plan_sha256,
        "preview_workbook_sha256": sha256_file(preview_workbook),
        "record_count": len(records),
        "records": records,
    }
    result["trace_digest"] = _sha256_bytes(canonical_json_bytes(result))
    return result


__all__ = [
    "BINDING_PLAN_SCHEMA_ID",
    "PRESENTATION_CONTRACT_ID",
    "IDENTITY_TRANSFORM_ID",
    "STORE_PROGRESS_TRANSFORM_ID",
    "SOURCE_SUMMARY_TRANSFORM_ID",
    "PRODUCT_V2_EVENT_SOURCE_TRANSFORM_ID",
    "PRODUCT_V2_REVIEW_NOTE_TRANSFORM_ID",
    "PRODUCT_V2_COMPACT_CHANGE_TRANSFORM_ID",
    "PRODUCT_V2_PRESENTATION_CONTRACT_ID",
    "EXPECTED_ANF_PRODUCT_SHA256",
    "EXPECTED_ANF_SHADOW_SHA256",
    "EXPECTED_ANF_WORKBOOK_SHA256",
    "EXPECTED_DESIGN_LOCK_MANIFEST_SHA256",
    "PromiseProgressWorkbookBindingPlan",
    "PromiseProgressWorkbookBindingPlanV2",
    "PromiseProgressWorkbookDynamicPresentationContract",
    "PromiseProgressWorkbookPresentationContract",
    "PromiseProgressWorkbookPreviewError",
    "WorkbookBinding",
    "build_legacy_difference_report",
    "build_preview_manifest",
    "build_promise_progress_workbook_binding_plan",
    "build_promise_progress_workbook_binding_plan_v2",
    "build_workbook_trace",
    "build_workbook_trace_v2",
    "canonical_json_bytes",
    "canonical_workbook_content_sha256",
    "load_json_strict",
    "materialize_promise_progress_preview",
    "materialize_promise_progress_preview_v2",
    "measure_presentation_text",
    "plan_presentation_row",
    "sha256_file",
    "target_sheet_semantic_sha256",
    "target_sheet_semantic_sha256_v2",
    "validate_preview_semantics",
    "validate_preview_semantics_v2",
    "validate_preview_structure",
    "validate_preview_structure_v2",
    "validate_preview_visual_fit",
    "validate_preview_visual_fit_v2",
    "validate_promise_progress_workbook_binding_plan_v2",
    "validate_promise_progress_workbook_binding_plan",
    "product_v2_presentation_contract",
    "verify_design_lock",
    "write_deterministic_json",
]
