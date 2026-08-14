"""Typed source-native Summary and BS_Segments workbook projection planning.

The source-native products remain the sole economic authority.  This module only
binds stable field identities to a verified workbook presentation surface and
produces an immutable, shadow-first write plan.  It deliberately performs no
workbook I/O.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence

from pbi_xbrl.json_schema_validation import load_json_strict

from .serialization import canonicalize, serialize_package


PROJECTION_SCHEMA = "summary-bs-source-native-workbook-projection@2"
TARGET_WORKBOOK_LIFECYCLE = "target_not_wired"
PRESENTATION_MUTATION_CONTRACT = "summary-bs-binding-derived-presentation@1"
PERCENTAGE_POINT_DISPLAY_CONTRACT = "decimal-fraction-to-percentage-points@1"
DILUTED_SHARES_ROW_LABEL = "Diluted weighted-average shares (m)"
INVENTORY_SALES_SPREAD_ROW_LABEL = "Inventory less sales YoY (pp)"

Disposition = Literal[
    "WRITE_SOURCE_NATIVE_VALUE",
    "WRITE_DERIVED_SOURCE_NATIVE_VALUE",
    "CLEAR_STALE_LEGACY_VALUE",
    "PRESERVE_PRESENTATION_ONLY_CELL",
    "PRESERVE_EXPLICIT_WORKBOOK_FORMULA",
    "NOT_VISIBLE_IN_CURRENT_PRODUCT",
    "NEEDS_REVIEW_NO_NUMERIC_PROJECTION",
    "UNAVAILABLE_NO_NUMERIC_PROJECTION",
]
WriteMode = Literal["SET_VALUE", "CLEAR_CONTENTS", "NO_WRITE"]
LegacyClassification = Literal[
    "CORRECT_LEGACY_VALUE",
    "INCORRECT_LEGACY_VALUE",
    "STALE_LEGACY_VALUE",
    "FORMULA_PRESENTATION_ONLY",
    "FORMULA_ECONOMIC_OWNER",
    "BLANK",
]

_CELL_RE = re.compile(r"^(?P<sheet>SUMMARY|BS_Segments)!(?P<cell>[A-Z]+[1-9][0-9]*)$")
_COORDINATE_RE = re.compile(r"^(?P<column>[A-Z]+)(?P<row>[1-9][0-9]*)$")
_SAFE_MISSING_MARKERS = frozenset({"N/A", "NA", "#N/A", "—"})


class SummaryBSWorkbookProjectionError(ValueError):
    """Raised before writes when a projection would be incomplete or ambiguous."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(canonicalize(value))).hexdigest()


def _require_mapping(value: Any, *, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise SummaryBSWorkbookProjectionError(f"{label} must be an object.")
    return value


def _require_sequence(value: Any, *, label: str) -> Sequence[Any]:
    if not isinstance(value, list):
        raise SummaryBSWorkbookProjectionError(f"{label} must be an array.")
    return value


def _parse_locator(value: Any) -> tuple[str, str]:
    match = _CELL_RE.fullmatch(str(value or ""))
    if match is None:
        raise SummaryBSWorkbookProjectionError(f"Invalid or non-visible legacy locator {value!r}.")
    return match.group("sheet"), match.group("cell")


def _coordinate_parts(coordinate: str) -> tuple[str, int]:
    match = _COORDINATE_RE.fullmatch(coordinate)
    if match is None:
        raise SummaryBSWorkbookProjectionError(f"Invalid cell coordinate {coordinate!r}.")
    return match.group("column"), int(match.group("row"))


def _field_index(product: Mapping[str, Any], *, label: str) -> dict[str, Mapping[str, Any]]:
    fields = _require_sequence(product.get("fields"), label=f"{label}.fields")
    result: dict[str, Mapping[str, Any]] = {}
    for raw_field in fields:
        field = _require_mapping(raw_field, label=f"{label} field")
        field_id = str(field.get("field_id") or "")
        if not field_id:
            raise SummaryBSWorkbookProjectionError(f"{label} contains a field without field_id.")
        if field_id in result:
            raise SummaryBSWorkbookProjectionError(f"Duplicate {label} field_id {field_id!r}.")
        result[field_id] = field
    return result


def _lineage_index(shadow: Mapping[str, Any], *, label: str) -> dict[str, Mapping[str, Any]]:
    rows = _require_sequence(shadow.get("field_lineage"), label=f"{label}.field_lineage")
    result: dict[str, Mapping[str, Any]] = {}
    for raw_row in rows:
        row = _require_mapping(raw_row, label=f"{label} lineage")
        field_id = str(row.get("field_id") or "")
        if not field_id:
            raise SummaryBSWorkbookProjectionError(f"{label} contains lineage without field_id.")
        if field_id in result:
            raise SummaryBSWorkbookProjectionError(f"Duplicate {label} lineage field_id {field_id!r}.")
        result[field_id] = row
    return result


def _decimal(value: Any, *, label: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise SummaryBSWorkbookProjectionError(f"{label} is not a canonical decimal: {value!r}.") from exc
    if not parsed.is_finite():
        raise SummaryBSWorkbookProjectionError(f"{label} must be finite.")
    return parsed


def _plain_decimal(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _date_from_excel_serial(value: Any) -> date | None:
    try:
        serial = _decimal(value, label="legacy date")
    except SummaryBSWorkbookProjectionError:
        return None
    integral = int(serial)
    if serial != integral:
        return None
    return date(1899, 12, 30) + timedelta(days=integral)


def _format_precision(format_code: str | None) -> tuple[bool, int] | None:
    if not format_code:
        return None
    first_section = format_code.split(";", 1)[0]
    cleaned = re.sub(r'"[^"]*"', "", first_section)
    cleaned = re.sub(r"\[[^\]]*\]", "", cleaned)
    percentage = "%" in cleaned
    match = re.search(r"0(?:\.([0#]+))?", cleaned)
    if match is None:
        return None
    return percentage, len(match.group(1) or "")


def _numeric_display_equal(
    canonical_value: Any,
    legacy_value: Any,
    number_format_code: str | None,
) -> bool:
    try:
        canonical = _decimal(canonical_value, label="canonical value")
        legacy = _decimal(legacy_value, label="legacy value")
    except SummaryBSWorkbookProjectionError:
        return False
    precision = _format_precision(number_format_code)
    if precision is None:
        return canonical == legacy
    percentage, decimal_places = precision
    scale = Decimal(100) if percentage else Decimal(1)
    quantum = Decimal(1).scaleb(-decimal_places)
    canonical_display = (canonical * scale).quantize(quantum, rounding=ROUND_HALF_UP)
    legacy_display = (legacy * scale).quantize(quantum, rounding=ROUND_HALF_UP)
    return canonical_display == legacy_display


def _safe_missing_marker(value: Any) -> bool:
    return isinstance(value, str) and value.strip().upper() in _SAFE_MISSING_MARKERS


def _legacy_classification(
    field: Mapping[str, Any],
    cell: Mapping[str, Any],
) -> LegacyClassification:
    if cell.get("formula") is not None:
        return "FORMULA_ECONOMIC_OWNER"
    legacy = cell.get("value")
    if legacy is None or legacy == "":
        return "BLANK"
    status = str(field.get("status"))
    if status != "available":
        return "CORRECT_LEGACY_VALUE" if _safe_missing_marker(legacy) else "STALE_LEGACY_VALUE"
    value = _require_mapping(field.get("value"), label=f"value for {field.get('field_id')}")
    kind = value.get("kind")
    if kind == "exact":
        correct = _numeric_display_equal(
            value.get("value"), legacy, cell.get("number_format_code")
        )
    elif kind == "qualitative":
        correct = str(value.get("text")) == str(legacy)
    elif kind == "date":
        expected = str(value.get("value"))
        if isinstance(legacy, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", legacy):
            observed = legacy
        else:
            parsed = _date_from_excel_serial(legacy)
            observed = None if parsed is None else parsed.isoformat()
        correct = expected == observed
    else:
        raise SummaryBSWorkbookProjectionError(
            f"Unsupported product value kind {kind!r} for {field.get('field_id')}."
        )
    return "CORRECT_LEGACY_VALUE" if correct else "INCORRECT_LEGACY_VALUE"


def _display_scale(field: Mapping[str, Any]) -> Decimal:
    return (
        Decimal(100)
        if field.get("unit_id") == "unit:core:percentage-points@1"
        else Decimal(1)
    )


def _write_value(field: Mapping[str, Any]) -> Mapping[str, Any] | None:
    value = field.get("value")
    if value is None:
        return None
    value = _require_mapping(value, label=f"value for {field.get('field_id')}")
    kind = str(value.get("kind"))
    if kind == "exact":
        parsed = _decimal(value.get("value"), label=f"value for {field.get('field_id')}")
        workbook_value = parsed * _display_scale(field)
        return {"kind": "number", "canonical_decimal": _plain_decimal(workbook_value)}
    if kind == "qualitative":
        text = str(value.get("text") or "")
        if not text:
            raise SummaryBSWorkbookProjectionError("Qualitative projection values cannot be empty.")
        return {"kind": "text", "text": text}
    if kind == "date":
        iso_date = str(value.get("value") or "")
        try:
            date.fromisoformat(iso_date)
        except ValueError as exc:
            raise SummaryBSWorkbookProjectionError(f"Invalid projected ISO date {iso_date!r}.") from exc
        return {"kind": "date", "iso_date": iso_date}
    raise SummaryBSWorkbookProjectionError(f"Unsupported projected value kind {kind!r}.")


def _display_role(field: Mapping[str, Any]) -> str:
    value = field.get("value")
    unit_id = str(field.get("unit_id") or "")
    if isinstance(value, Mapping) and value.get("kind") == "qualitative":
        return "narrative_text"
    if isinstance(value, Mapping) and value.get("kind") == "date":
        return "reporting_date"
    if unit_id == "unit:core:percentage-points@1":
        return "percentage_points"
    if "percent" in unit_id:
        return "percentage"
    if "ratio" in unit_id or "multiple" in unit_id:
        return "ratio"
    if "currency" in unit_id:
        return "currency_millions"
    if "shares" in unit_id:
        return "shares_millions"
    if unit_id.endswith("text@1"):
        return "missing_or_text_marker"
    return "typed_numeric"


def _projection_number_format(field: Mapping[str, Any], cell: Mapping[str, Any]) -> str | None:
    existing = cell.get("number_format_code")
    if field.get("status") != "available" or not isinstance(field.get("value"), Mapping):
        return existing
    if field["value"].get("kind") != "exact":
        return existing
    unit_id = str(field.get("unit_id") or "")
    if unit_id == "unit:core:percentage-points@1":
        return "0.0"
    if existing not in {None, "General"}:
        return existing
    if "percent" in unit_id:
        return "0.0%"
    if "ratio" in unit_id or "multiple" in unit_id:
        return "0.00x"
    if "currency" in unit_id:
        return "#,##0.000"
    if "shares" in unit_id:
        return "#,##0.0"
    return existing


def _axis_id(sheet: str, row: int) -> str:
    if sheet == "SUMMARY":
        return "summary_semantic_field"
    if row <= 68:
        return "bs_quarterly_periods"
    return "bs_annual_periods"


def _validate_period_axis(
    *, field: Mapping[str, Any], sheet: str, cell: str, surface_map: Mapping[str, Any]
) -> None:
    if sheet != "BS_Segments":
        return
    column, row = _coordinate_parts(cell)
    if row <= 68:
        if column not in "BCDEFGHI" or len(column) != 1:
            raise SummaryBSWorkbookProjectionError(f"Quarterly BS binding falls outside B:I: {sheet}!{cell}.")
        expected = f"20{str(field['period_id']).split('fy20', 1)[1].split('@', 1)[0].upper()}"
        header_cell = f"{column}7"
    else:
        if column not in "BCD" or len(column) != 1:
            raise SummaryBSWorkbookProjectionError(f"Annual BS binding falls outside B:D: {sheet}!{cell}.")
        period_match = re.search(r"fy(20\d{2})@", str(field["period_id"]))
        if period_match is None:
            raise SummaryBSWorkbookProjectionError(
                f"Annual BS binding has non-annual period {field['period_id']!r}."
            )
        expected = period_match.group(1)
        header_cell = f"{column}70"
    sheet_map = _require_mapping(surface_map["sheets"][sheet], label=f"surface {sheet}")
    header = _require_mapping(sheet_map["cells"].get(header_cell), label=f"header {header_cell}")
    observed = str(header.get("value"))
    if observed != expected:
        raise SummaryBSWorkbookProjectionError(
            f"Period-axis mismatch for {sheet}!{cell}: {field['period_id']!r} expects "
            f"{expected!r}, observed {observed!r} in {header_cell}."
        )


def _binding_disposition(
    field: Mapping[str, Any], legacy_classification: LegacyClassification
) -> tuple[Disposition, WriteMode]:
    status = str(field.get("status"))
    if status == "available":
        if field.get("directness") == "derived":
            return "WRITE_DERIVED_SOURCE_NATIVE_VALUE", "SET_VALUE"
        return "WRITE_SOURCE_NATIVE_VALUE", "SET_VALUE"
    stale = legacy_classification in {
        "STALE_LEGACY_VALUE",
        "FORMULA_ECONOMIC_OWNER",
        "INCORRECT_LEGACY_VALUE",
    }
    if stale:
        return "CLEAR_STALE_LEGACY_VALUE", "CLEAR_CONTENTS"
    if status == "needs_review":
        return "NEEDS_REVIEW_NO_NUMERIC_PROJECTION", "NO_WRITE"
    if status == "unavailable":
        return "UNAVAILABLE_NO_NUMERIC_PROJECTION", "NO_WRITE"
    return "NOT_VISIBLE_IN_CURRENT_PRODUCT", "NO_WRITE"


@dataclass(frozen=True)
class WorkbookFieldBinding:
    product_surface: str
    product_id: str
    product_sha256: str
    field_id: str
    metric_key: str
    metric_id: str
    period_id: str
    definition_id: str
    basis_id: str
    unit_id: str
    status: str
    value_state: str
    directness: str
    target_sheet: str
    target_block_id: str
    target_cell: str
    period_axis_id: str
    row_label: Any
    legacy_row_label: Any
    disposition: Disposition
    write_mode: WriteMode
    write_value: Mapping[str, Any] | None
    canonical_value: Mapping[str, Any] | None
    display_scale: str
    display_transform_contract: str
    display_role: str
    legacy_number_format_code: str | None
    projection_number_format_code: str | None
    legacy_value: Any
    legacy_formula: str | None
    legacy_classification: LegacyClassification
    canonical_fact_id: str | None
    derivation_id: str | None
    lineage_present: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "basis_id": self.basis_id,
            "canonical_fact_id": self.canonical_fact_id,
            "canonical_value": (
                dict(self.canonical_value) if self.canonical_value is not None else None
            ),
            "definition_id": self.definition_id,
            "derivation_id": self.derivation_id,
            "directness": self.directness,
            "display_role": self.display_role,
            "display_scale": self.display_scale,
            "display_transform_contract": self.display_transform_contract,
            "disposition": self.disposition,
            "field_id": self.field_id,
            "legacy_classification": self.legacy_classification,
            "legacy_formula": self.legacy_formula,
            "legacy_row_label": self.legacy_row_label,
            "legacy_value": self.legacy_value,
            "lineage_present": self.lineage_present,
            "metric_id": self.metric_id,
            "metric_key": self.metric_key,
            "legacy_number_format_code": self.legacy_number_format_code,
            "period_axis_id": self.period_axis_id,
            "period_id": self.period_id,
            "product_id": self.product_id,
            "product_sha256": self.product_sha256,
            "product_surface": self.product_surface,
            "projection_number_format_code": self.projection_number_format_code,
            "row_label": self.row_label,
            "status": self.status,
            "target_block_id": self.target_block_id,
            "target_cell": self.target_cell,
            "target_sheet": self.target_sheet,
            "unit_id": self.unit_id,
            "value_state": self.value_state,
            "write_mode": self.write_mode,
            "write_value": dict(self.write_value) if self.write_value is not None else None,
        }


def _projected_row_label(field: Mapping[str, Any], legacy_row_label: Any) -> Any:
    metric_key = str(field.get("metric_key") or "")
    if metric_key == "diluted_weighted_average_shares":
        return DILUTED_SHARES_ROW_LABEL
    if metric_key == "inventory_growth_minus_sales_growth":
        if field.get("unit_id") != "unit:core:percentage-points@1":
            raise SummaryBSWorkbookProjectionError(
                "Inventory/sales spread label requires a percentage-point unit."
            )
        return INVENTORY_SALES_SPREAD_ROW_LABEL
    return legacy_row_label


def _display_transform_contract(field: Mapping[str, Any]) -> str:
    return (
        PERCENTAGE_POINT_DISPLAY_CONTRACT
        if _display_scale(field) == 100
        else "identity-workbook-value@1"
    )


def _build_bindings_for_surface(
    *,
    product_surface: str,
    product: Mapping[str, Any],
    shadow: Mapping[str, Any],
    surface_map: Mapping[str, Any],
) -> list[WorkbookFieldBinding]:
    fields = _field_index(product, label=f"{product_surface} product")
    lineage = _lineage_index(shadow, label=f"{product_surface} shadow")
    if set(fields) != set(lineage):
        missing_lineage = sorted(set(fields) - set(lineage))
        orphan_lineage = sorted(set(lineage) - set(fields))
        raise SummaryBSWorkbookProjectionError(
            f"{product_surface} product/shadow field mismatch; missing={missing_lineage[:3]!r}, "
            f"orphan={orphan_lineage[:3]!r}."
        )
    product_sha256 = str(shadow.get("product_sha256") or "")
    if not re.fullmatch(r"[0-9a-f]{64}", product_sha256):
        raise SummaryBSWorkbookProjectionError(f"{product_surface} shadow lacks product_sha256.")
    computed_product_sha256 = hashlib.sha256(serialize_package(product)).hexdigest()
    if computed_product_sha256 != product_sha256:
        raise SummaryBSWorkbookProjectionError(
            f"{product_surface} shadow product hash does not match the supplied product."
        )
    product_id = str(product.get("product_id") or "")
    if shadow.get("product_id") != product_id:
        raise SummaryBSWorkbookProjectionError(f"{product_surface} product/shadow identity mismatch.")
    result: list[WorkbookFieldBinding] = []
    for field_id in sorted(fields):
        field = fields[field_id]
        lineage_row = lineage[field_id]
        sheet, cell = _parse_locator(lineage_row.get("legacy_locator"))
        expected_sheet = "SUMMARY" if product_surface == "Summary" else "BS_Segments"
        if sheet != expected_sheet:
            raise SummaryBSWorkbookProjectionError(
                f"{product_surface} field {field_id} unexpectedly targets {sheet}!{cell}."
            )
        sheet_map = _require_mapping(surface_map.get("sheets", {}).get(sheet), label=f"surface {sheet}")
        cell_map = _require_mapping(sheet_map.get("cells", {}).get(cell), label=f"surface cell {sheet}!{cell}")
        _validate_period_axis(field=field, sheet=sheet, cell=cell, surface_map=surface_map)
        _, row = _coordinate_parts(cell)
        rows = {
            int(item["row"]): item
            for item in _require_sequence(sheet_map.get("rows"), label=f"surface {sheet}.rows")
        }
        row_map = _require_mapping(rows.get(row), label=f"surface row {sheet}!{row}")
        if row_map.get("label") in {None, ""}:
            raise SummaryBSWorkbookProjectionError(f"Target row lacks a verified label: {sheet}!{cell}.")
        legacy_row_label = row_map["label"]
        legacy_classification = _legacy_classification(field, cell_map)
        disposition, write_mode = _binding_disposition(field, legacy_classification)
        lineage_present = bool(
            lineage_row.get("canonical_fact_id")
            and (
                lineage_row.get("source_document_ids")
                or lineage_row.get("derivation_input_fact_ids")
            )
        )
        if field.get("status") == "available" and not lineage_present:
            raise SummaryBSWorkbookProjectionError(f"Available field lacks typed lineage: {field_id}.")
        result.append(
            WorkbookFieldBinding(
                product_surface=product_surface,
                product_id=product_id,
                product_sha256=product_sha256,
                field_id=field_id,
                metric_key=str(field["metric_key"]),
                metric_id=str(field["metric_id"]),
                period_id=str(field["period_id"]),
                definition_id=str(field["definition_id"]),
                basis_id=str(field["basis_id"]),
                unit_id=str(field["unit_id"]),
                status=str(field["status"]),
                value_state=str(field["value_state"]),
                directness=str(field["directness"]),
                target_sheet=sheet,
                target_block_id=str(cell_map["block_id"]),
                target_cell=cell,
                period_axis_id=_axis_id(sheet, row),
                row_label=_projected_row_label(field, legacy_row_label),
                legacy_row_label=legacy_row_label,
                disposition=disposition,
                write_mode=write_mode,
                write_value=_write_value(field) if write_mode == "SET_VALUE" else None,
                canonical_value=(
                    dict(field["value"])
                    if isinstance(field.get("value"), Mapping)
                    else None
                ),
                display_scale=_plain_decimal(_display_scale(field)),
                display_transform_contract=_display_transform_contract(field),
                display_role=_display_role(field),
                legacy_number_format_code=cell_map.get("number_format_code"),
                projection_number_format_code=_projection_number_format(field, cell_map),
                legacy_value=cell_map.get("value"),
                legacy_formula=cell_map.get("formula"),
                legacy_classification=legacy_classification,
                canonical_fact_id=field.get("canonical_fact_id"),
                derivation_id=field.get("derivation_id"),
                lineage_present=lineage_present,
            )
        )
    return result


def _build_presentation_mutations(
    *,
    bindings: Sequence[WorkbookFieldBinding],
    surface_map: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Derive non-economic workbook text from accepted binding semantics."""

    quarterly_segments = [
        binding
        for binding in bindings
        if binding.product_surface == "BS_Segments"
        and _coordinate_parts(binding.target_cell)[1] in {61, 62, 63, 65, 66, 67}
    ]
    expected_quarterly_targets = {
        f"{column}{row}"
        for row in (61, 62, 63, 65, 66, 67)
        for column in "BCDEFGHI"
    }
    observed_quarterly_targets = {binding.target_cell for binding in quarterly_segments}
    if observed_quarterly_targets != expected_quarterly_targets:
        raise SummaryBSWorkbookProjectionError(
            "Quarterly-segment presentation status is not backed by exactly 48 bindings."
        )
    quarterly_available = sum(
        binding.status == "available" and binding.write_mode == "SET_VALUE"
        for binding in quarterly_segments
    )
    if quarterly_available == len(quarterly_segments):
        quarterly_status = "PASS"
    elif quarterly_available == 0:
        quarterly_status = "N/A"
    else:
        quarterly_status = f"{quarterly_available}/{len(quarterly_segments)}"

    bs_cells = _require_mapping(
        _require_mapping(surface_map["sheets"]["BS_Segments"], label="BS surface").get(
            "cells"
        ),
        label="BS surface cells",
    )

    def cell_mutation(
        *,
        presentation_id: str,
        cell: str,
        text: str,
        derivation: Mapping[str, Any],
    ) -> dict[str, Any]:
        legacy = _require_mapping(bs_cells.get(cell), label=f"BS presentation cell {cell}")
        if legacy.get("formula") is not None:
            raise SummaryBSWorkbookProjectionError(
                f"Presentation target BS_Segments!{cell} unexpectedly contains a formula."
            )
        return {
            "contract": PRESENTATION_MUTATION_CONTRACT,
            "derivation": dict(derivation),
            "legacy_number_format_code": legacy.get("number_format_code"),
            "legacy_value": legacy.get("value"),
            "presentation_id": presentation_id,
            "projection_number_format_code": legacy.get("number_format_code"),
            "target_cell": cell,
            "target_sheet": "BS_Segments",
            "write_mode": "SET_VALUE",
            "write_value": {"kind": "text", "text": text},
        }

    status_legacy = str(bs_cells["A3"].get("value") or "")
    expected_token = "Quarterly Seg N/A"
    if expected_token not in status_legacy:
        raise SummaryBSWorkbookProjectionError(
            f"Historical BS status text changed: {status_legacy!r}."
        )
    status_text = status_legacy.replace(
        expected_token, f"Quarterly Seg {quarterly_status}", 1
    )

    diluted = [
        binding
        for binding in bindings
        if binding.metric_key == "diluted_weighted_average_shares"
    ]
    spread = [
        binding
        for binding in bindings
        if binding.metric_key == "inventory_growth_minus_sales_growth"
    ]
    if len(diluted) != 8 or {binding.row_label for binding in diluted} != {
        DILUTED_SHARES_ROW_LABEL
    }:
        raise SummaryBSWorkbookProjectionError(
            "Diluted-share presentation requires eight consistently labeled bindings."
        )
    if len(spread) != 8 or {binding.row_label for binding in spread} != {
        INVENTORY_SALES_SPREAD_ROW_LABEL
    }:
        raise SummaryBSWorkbookProjectionError(
            "Percentage-point presentation requires eight consistently labeled bindings."
        )
    if any(
        binding.unit_id != "unit:core:percentage-points@1"
        or binding.display_scale != "100"
        or binding.projection_number_format_code != "0.0"
        for binding in spread
    ):
        raise SummaryBSWorkbookProjectionError(
            "Percentage-point bindings lack the accepted display-scale contract."
        )

    return sorted(
        [
            cell_mutation(
                presentation_id="bs-quarterly-segment-availability-status",
                cell="A3",
                text=status_text,
                derivation={
                    "available_binding_count": quarterly_available,
                    "binding_count": len(quarterly_segments),
                    "rule": "all-quarterly-segment-bindings-available",
                },
            ),
            cell_mutation(
                presentation_id="bs-diluted-weighted-average-shares-row-label",
                cell="A49",
                text=DILUTED_SHARES_ROW_LABEL,
                derivation={
                    "binding_count": len(diluted),
                    "metric_key": "diluted_weighted_average_shares",
                },
            ),
            cell_mutation(
                presentation_id="bs-inventory-sales-percentage-point-row-label",
                cell="A53",
                text=INVENTORY_SALES_SPREAD_ROW_LABEL,
                derivation={
                    "binding_count": len(spread),
                    "metric_key": "inventory_growth_minus_sales_growth",
                    "unit_id": "unit:core:percentage-points@1",
                },
            ),
        ],
        key=lambda item: (item["target_sheet"], item["target_cell"]),
    )


def build_summary_bs_projection_plan(
    *,
    summary_product: Mapping[str, Any],
    summary_shadow: Mapping[str, Any],
    bs_product: Mapping[str, Any],
    bs_shadow: Mapping[str, Any],
    surface_map: Mapping[str, Any],
    protected_workbook_sha256: str,
) -> dict[str, Any]:
    """Build and fail-closed validate the 452-field shadow-first binding plan."""

    if surface_map.get("schema") != "summary-bs-workbook-surface-map@1":
        raise SummaryBSWorkbookProjectionError("Unsupported workbook surface-map schema.")
    workbook = _require_mapping(surface_map.get("workbook"), label="surface-map workbook")
    observed_workbook_sha256 = str(workbook.get("sha256") or "").lower()
    if observed_workbook_sha256 != protected_workbook_sha256.lower():
        raise SummaryBSWorkbookProjectionError(
            "Surface map is not anchored to the protected workbook hash."
        )
    bindings = _build_bindings_for_surface(
        product_surface="Summary",
        product=summary_product,
        shadow=summary_shadow,
        surface_map=surface_map,
    ) + _build_bindings_for_surface(
        product_surface="BS_Segments",
        product=bs_product,
        shadow=bs_shadow,
        surface_map=surface_map,
    )
    expected_surface_counts = {"Summary": 35, "BS_Segments": 417}
    observed_surface_counts = {
        surface: sum(1 for binding in bindings if binding.product_surface == surface)
        for surface in expected_surface_counts
    }
    if observed_surface_counts != expected_surface_counts:
        raise SummaryBSWorkbookProjectionError(
            f"Projection surface counts changed: {observed_surface_counts!r}."
        )
    if len(bindings) != 452:
        raise SummaryBSWorkbookProjectionError(
            f"Projection must close exactly 452 fields; observed {len(bindings)}."
        )
    target_owners: dict[tuple[str, str], str] = {}
    duplicates: list[dict[str, str]] = []
    for binding in bindings:
        key = (binding.target_sheet, binding.target_cell)
        if key in target_owners:
            duplicates.append(
                {
                    "target": f"{binding.target_sheet}!{binding.target_cell}",
                    "first_field_id": target_owners[key],
                    "second_field_id": binding.field_id,
                }
            )
        target_owners[key] = binding.field_id
    if duplicates:
        raise SummaryBSWorkbookProjectionError(f"Duplicate target owners: {duplicates[:3]!r}.")

    presentation_mutations = _build_presentation_mutations(
        bindings=bindings,
        surface_map=surface_map,
    )
    for mutation in presentation_mutations:
        key = (str(mutation["target_sheet"]), str(mutation["target_cell"]))
        if key in target_owners:
            raise SummaryBSWorkbookProjectionError(
                f"Presentation target conflicts with economic binding: {key!r}."
            )
        target_owners[key] = str(mutation["presentation_id"])

    formula_inventory: list[dict[str, Any]] = []
    for sheet_name, raw_sheet in sorted(surface_map["sheets"].items()):
        sheet = _require_mapping(raw_sheet, label=f"surface {sheet_name}")
        for raw_formula in _require_sequence(
            sheet.get("formula_inventory"), label=f"surface {sheet_name}.formula_inventory"
        ):
            formula = dict(_require_mapping(raw_formula, label="formula inventory row"))
            key = (sheet_name, str(formula["cell"]))
            if key in target_owners:
                formula["classification"] = "FORMULA_ECONOMIC_OWNER"
                formula["action"] = "REPLACE_OR_CLEAR_FROM_SOURCE_NATIVE_PRODUCT"
                formula["field_id"] = target_owners[key]
            else:
                formula["classification"] = "FORMULA_PRESENTATION_ONLY"
                formula["action"] = "PRESERVE_EXPLICIT_WORKBOOK_FORMULA"
                formula["field_id"] = None
            formula["sheet"] = sheet_name
            formula_inventory.append(formula)

    binding_dicts = [binding.to_dict() for binding in sorted(bindings, key=lambda row: row.field_id)]
    disposition_counts: dict[str, int] = {}
    status_counts: dict[str, int] = {}
    legacy_classification_counts: dict[str, int] = {}
    for binding in binding_dicts:
        disposition_counts[binding["disposition"]] = disposition_counts.get(binding["disposition"], 0) + 1
        status_counts[binding["status"]] = status_counts.get(binding["status"], 0) + 1
        classification = binding["legacy_classification"]
        legacy_classification_counts[classification] = legacy_classification_counts.get(classification, 0) + 1
    expected_status_counts = {"available": 388, "needs_review": 26, "unavailable": 38}
    if status_counts != expected_status_counts:
        raise SummaryBSWorkbookProjectionError(
            f"Projection status counts changed: {status_counts!r}."
        )

    plan: dict[str, Any] = {
        "schema": PROJECTION_SCHEMA,
        "lifecycle": TARGET_WORKBOOK_LIFECYCLE,
        "economic_authority": "accepted_integrated_source_native_summary_bs_products",
        "protected_workbook": {
            "path": workbook.get("path"),
            "sha256": observed_workbook_sha256,
            "surface_digest": surface_map.get("surface_digest"),
        },
        "products": {
            "Summary": {
                "product_id": summary_product.get("product_id"),
                "product_sha256": summary_shadow.get("product_sha256"),
                "field_count": len(summary_product["fields"]),
            },
            "BS_Segments": {
                "product_id": bs_product.get("product_id"),
                "product_sha256": bs_shadow.get("product_sha256"),
                "field_count": len(bs_product["fields"]),
            },
        },
        "bindings": binding_dicts,
        "presentation_contract": {
            "contract": PRESENTATION_MUTATION_CONTRACT,
            "economic_field_count_change": 0,
            "mutation_count": len(presentation_mutations),
            "source": "validated-binding-semantics-and-frozen-shell-labels",
        },
        "presentation_mutations": presentation_mutations,
        "formula_ownership": sorted(
            formula_inventory, key=lambda row: (str(row["sheet"]), str(row["cell"]))
        ),
        "validation": {
            "binding_count": len(binding_dicts),
            "unclassified_field_count": 0,
            "duplicate_target_owner_count": 0,
            "unbound_visible_field_count": 0,
            "legacy_economic_survivor_count": 0,
            "available_without_lineage_count": sum(
                1
                for binding in binding_dicts
                if binding["status"] == "available" and not binding["lineage_present"]
            ),
            "disposition_counts": dict(sorted(disposition_counts.items())),
            "status_counts": dict(sorted(status_counts.items())),
            "legacy_classification_counts": dict(sorted(legacy_classification_counts.items())),
            "formula_count": len(formula_inventory),
            "formula_economic_owner_count": sum(
                1 for row in formula_inventory if row["classification"] == "FORMULA_ECONOMIC_OWNER"
            ),
            "formula_presentation_only_count": sum(
                1 for row in formula_inventory if row["classification"] == "FORMULA_PRESENTATION_ONLY"
            ),
            "presentation_mutation_count": len(presentation_mutations),
            "percentage_point_binding_count": sum(
                1
                for binding in binding_dicts
                if binding["display_transform_contract"]
                == PERCENTAGE_POINT_DISPLAY_CONTRACT
            ),
            "passed": True,
        },
    }
    if plan["validation"]["available_without_lineage_count"]:
        raise SummaryBSWorkbookProjectionError("Projection contains available fields without lineage.")
    if plan["validation"]["presentation_mutation_count"] != 3:
        raise SummaryBSWorkbookProjectionError(
            "Projection must contain exactly three bounded presentation mutations."
        )
    if plan["validation"]["percentage_point_binding_count"] != 8:
        raise SummaryBSWorkbookProjectionError(
            "Projection must contain exactly eight percentage-point display bindings."
        )
    plan["plan_digest"] = _digest(plan)
    return plan


def build_summary_bs_projection_plan_from_paths(
    *,
    summary_product_path: Path | str,
    summary_shadow_path: Path | str,
    bs_product_path: Path | str,
    bs_shadow_path: Path | str,
    surface_map_path: Path | str,
    protected_workbook_sha256: str,
) -> dict[str, Any]:
    return build_summary_bs_projection_plan(
        summary_product=load_json_strict(summary_product_path),
        summary_shadow=load_json_strict(summary_shadow_path),
        bs_product=load_json_strict(bs_product_path),
        bs_shadow=load_json_strict(bs_shadow_path),
        surface_map=load_json_strict(surface_map_path),
        protected_workbook_sha256=protected_workbook_sha256,
    )


def write_summary_bs_projection_plan(plan: Mapping[str, Any], output_path: Path | str) -> Path:
    output = Path(output_path)
    serialize_package(plan, output)
    loaded = load_json_strict(output)
    if loaded != canonicalize(plan):
        raise SummaryBSWorkbookProjectionError("Serialized projection plan failed strict round-trip.")
    return output


__all__ = [
    "DILUTED_SHARES_ROW_LABEL",
    "INVENTORY_SALES_SPREAD_ROW_LABEL",
    "PERCENTAGE_POINT_DISPLAY_CONTRACT",
    "PRESENTATION_MUTATION_CONTRACT",
    "PROJECTION_SCHEMA",
    "TARGET_WORKBOOK_LIFECYCLE",
    "SummaryBSWorkbookProjectionError",
    "WorkbookFieldBinding",
    "build_summary_bs_projection_plan",
    "build_summary_bs_projection_plan_from_paths",
    "write_summary_bs_projection_plan",
]
