"""Value-only filler for the frozen standard new-ticker workbook shell.

This module intentionally does not import or call the legacy production
workbook writers.  It copies the neutral shell and writes only mapped values
from a normalized company-data package into declared writable binding targets.
"""
from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from openpyxl import load_workbook
from openpyxl.cell.cell import MergedCell
from openpyxl.utils import column_index_from_string, range_boundaries

from pbi_xbrl.normalized_company_data_validation import (
    NormalizedDataIssue,
    build_mapping_gap_report,
    validate_normalized_company_data,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TEMPLATE = ROOT / "templates" / "standard_stock_model_template.xlsx"
DEFAULT_MANIFEST = ROOT / "docs" / "standard_template_shell_manifest.json"
DEFAULT_BINDING_MAP = ROOT / "docs" / "workbook_binding_map.json"


class NewTickerValueFillerError(RuntimeError):
    """Base error for value-only filler failures."""


class BindingContractError(NewTickerValueFillerError):
    """Raised when binding map entries violate the frozen shell contract."""


class NormalizedDataValidationError(NewTickerValueFillerError):
    """Raised when pre-render normalized data validation finds blocking issues."""

    def __init__(self, issues: Sequence[NormalizedDataIssue]) -> None:
        self.issues = list(issues)
        rule_ids = ", ".join(issue.rule_id for issue in self.issues[:8])
        super().__init__(f"Blocking normalized-data validation issues: {rule_ids}")


@dataclass(frozen=True)
class FillResult:
    ticker: str
    output_path: Path
    written_cell_count: int
    validation_issue_count: int
    mapping_gap_count: int
    manual_review_count: int


def fill_standard_template_from_package(
    package_path: Path | str,
    *,
    output_path: Path | str,
    ticker_override: str | None = None,
    template_path: Path | str = DEFAULT_TEMPLATE,
    manifest_path: Path | str = DEFAULT_MANIFEST,
    binding_map_path: Path | str = DEFAULT_BINDING_MAP,
    promotion_requested: bool = False,
) -> FillResult:
    """Fill the frozen standard shell from a normalized package.

    The function is value-only by construction: it validates that every binding
    target is inside a manifest writable zone, copies the shell, and then writes
    only cell values into those targets.  It does not mutate styles, formulas,
    dimensions, merges, hidden helper structure, or sheet order except for the
    documented ticker-token sheet rename.
    """

    package = _load_json(Path(package_path))
    manifest = _load_json(Path(manifest_path))
    binding_payload = _load_json(Path(binding_map_path))
    bindings = list(binding_payload.get("bindings") or [])
    ticker = _ticker(package, ticker_override)

    _validate_binding_contract(manifest, bindings)

    issues = validate_normalized_company_data(
        package,
        binding_map=bindings,
        promotion_requested=promotion_requested,
    )
    blocking = [issue for issue in issues if issue.severity.upper() in {"P0", "P1"}]
    if blocking:
        raise NormalizedDataValidationError(blocking)

    out_path = Path(output_path)
    if out_path.suffix.lower() != ".xlsx":
        raise NewTickerValueFillerError("Output path must be a macro-free .xlsx file.")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(Path(template_path), out_path)

    mapping_gaps = _combined_mapping_gaps(package, bindings, ticker=ticker)
    manual_review_flags = _combined_manual_review_flags(package, mapping_gaps)

    wb = load_workbook(out_path, data_only=False, read_only=False)
    written = 0
    try:
        _resolve_ticker_sheet(wb, ticker)
        row_schema_bindings = [
            binding
            for binding in bindings
            if bool(binding.get("writable"))
            and str(binding.get("value_shape") or "") in {"table_rows", "validation_rows"}
            and _row_schema(binding)
        ]
        for binding in bindings:
            if not bool(binding.get("writable")):
                continue
            if str(binding.get("source_policy") or "") == "validation-output":
                continue
            if _superseded_by_row_schema_binding(binding, row_schema_bindings):
                continue
            values = _values_for_binding(package, binding)
            if not values:
                continue
            written += _write_values_for_binding(wb, binding, values, ticker=ticker)

        written += _write_validation_rows(
            wb,
            bindings,
            normalized_validation_issues=[issue.to_dict() for issue in issues],
            mapping_gaps=mapping_gaps,
            manual_review_flags=manual_review_flags,
            ticker=ticker,
        )
        wb.save(out_path)
    finally:
        wb.close()

    return FillResult(
        ticker=ticker,
        output_path=out_path,
        written_cell_count=written,
        validation_issue_count=len(issues),
        mapping_gap_count=len(mapping_gaps),
        manual_review_count=len(manual_review_flags),
    )


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _ticker(package: Mapping[str, Any], override: str | None = None) -> str:
    raw = override
    if raw is None:
        meta = package.get("ticker_metadata") if isinstance(package, Mapping) else {}
        ticker_field = meta.get("ticker") if isinstance(meta, Mapping) else ""
        raw = ticker_field.get("value") if isinstance(ticker_field, Mapping) else ticker_field
    ticker = str(raw or "").strip().upper()
    if not ticker:
        raise NewTickerValueFillerError("Ticker is required in the package or --ticker override.")
    if any(ch in ticker for ch in "[]:*?/\\"):
        raise NewTickerValueFillerError(f"Ticker contains characters that are invalid in Excel sheet names: {ticker!r}")
    return ticker


def _resolve_sheet_name(sheet_name: str, ticker: str) -> str:
    return sheet_name.replace("{ticker}", ticker)


def _resolve_ticker_sheet(wb: Any, ticker: str) -> None:
    token_sheet = "{ticker}_Investment_Case"
    resolved = f"{ticker}_Investment_Case"
    if token_sheet not in wb.sheetnames:
        if resolved in wb.sheetnames:
            return
        raise NewTickerValueFillerError("Tokenized investment-case sheet is missing from the frozen shell.")
    wb[token_sheet].title = resolved
    _replace_defined_name_sheet_token(wb, token_sheet, resolved)


def _replace_defined_name_sheet_token(wb: Any, token_sheet: str, resolved_sheet: str) -> None:
    quoted_token = f"'{token_sheet}'"
    quoted_resolved = f"'{resolved_sheet}'"
    for name in list(wb.defined_names):
        defined_name = wb.defined_names[name]
        attr_text = getattr(defined_name, "attr_text", None)
        if not isinstance(attr_text, str) or token_sheet not in attr_text:
            continue
        defined_name.attr_text = attr_text.replace(quoted_token, quoted_resolved).replace(token_sheet, resolved_sheet)


def _validate_binding_contract(manifest: Mapping[str, Any], bindings: Sequence[Mapping[str, Any]]) -> None:
    sheets = {str(sheet["sheet"]): sheet for sheet in manifest.get("sheets", [])}
    for binding in bindings:
        if not bool(binding.get("writable")):
            continue
        sheet_name = str(binding.get("sheet") or "")
        sheet = sheets.get(sheet_name)
        if sheet is None:
            raise BindingContractError(f"Binding references a sheet outside the shell manifest: {sheet_name}")
        target = str(binding.get("target") or "")
        shell_zone = str(binding.get("shell_zone") or "")
        target_range = _parse_range(target)
        writable_zone = next((zone for zone in sheet.get("writable_zones", []) if zone.get("zone_id") == shell_zone), None)
        if writable_zone is None:
            raise BindingContractError(f"Binding {binding.get('binding_id')} references missing shell_zone {shell_zone!r}.")
        if not _contains(_parse_range(str(writable_zone["target"])), target_range):
            raise BindingContractError(
                f"Binding {binding.get('binding_id')} target {target} is outside writable shell zone {shell_zone}."
            )
        for zone in sheet.get("non_writable_zones", []):
            if _overlaps(target_range, _parse_range(str(zone["target"]))):
                raise BindingContractError(
                    f"Binding {binding.get('binding_id')} target {target} overlaps non-writable zone {zone.get('zone_id')}."
                )


def _superseded_by_row_schema_binding(
    binding: Mapping[str, Any],
    row_schema_bindings: Sequence[Mapping[str, Any]],
) -> bool:
    if _row_schema(binding):
        return False
    if str(binding.get("value_shape") or "") not in {"table_rows", "validation_rows"}:
        return False
    target = str(binding.get("target") or "")
    sheet = str(binding.get("sheet") or "")
    if not target or not sheet:
        return False
    target_range = _parse_range(target)
    for schema_binding in row_schema_bindings:
        if str(schema_binding.get("sheet") or "") != sheet:
            continue
        if _overlaps(target_range, _parse_range(str(schema_binding.get("target") or ""))):
            return True
    return False


def _parse_range(range_ref: str) -> tuple[int, int, int, int]:
    try:
        min_col, min_row, max_col, max_row = range_boundaries(range_ref)
    except Exception as exc:  # pragma: no cover - openpyxl supplies the exact parse detail
        raise BindingContractError(f"Invalid A1 range {range_ref!r}: {exc}") from exc
    if min_col > max_col or min_row > max_row:
        raise BindingContractError(f"Invalid reversed A1 range {range_ref!r}.")
    return min_col, min_row, max_col, max_row


def _contains(outer: tuple[int, int, int, int], inner: tuple[int, int, int, int]) -> bool:
    outer_left, outer_top, outer_right, outer_bottom = outer
    inner_left, inner_top, inner_right, inner_bottom = inner
    return (
        outer_left <= inner_left
        and inner_right <= outer_right
        and outer_top <= inner_top
        and inner_bottom <= outer_bottom
    )


def _overlaps(first: tuple[int, int, int, int], second: tuple[int, int, int, int]) -> bool:
    f_left, f_top, f_right, f_bottom = first
    s_left, s_top, s_right, s_bottom = second
    return not (f_right < s_left or s_right < f_left or f_bottom < s_top or s_bottom < f_top)


def _combined_mapping_gaps(
    package: Mapping[str, Any],
    bindings: Sequence[Mapping[str, Any]],
    *,
    ticker: str,
) -> list[dict[str, Any]]:
    explicit = [dict(item) for item in package.get("mapping_gaps", []) if isinstance(item, Mapping)]
    generated = [
        dict(item)
        for item in build_mapping_gap_report(package, bindings, ticker=ticker).get("gaps", [])
        if str(item.get("source_policy") or "") != "validation-output"
    ]
    by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    for item in [*explicit, *generated]:
        key = (
            str(item.get("sheet") or ""),
            str(item.get("target") or ""),
            str(item.get("normalized_field") or ""),
        )
        by_key[key] = dict(item)
    return list(by_key.values())


def _combined_manual_review_flags(package: Mapping[str, Any], mapping_gaps: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    flags = [dict(item) for item in package.get("manual_review_flags", []) if isinstance(item, Mapping)]
    for gap in mapping_gaps:
        if str(gap.get("source_policy") or "") == "validation-output":
            continue
        flags.append(
            {
                "severity": "P2",
                "rule_id": "mapping_gap_manual_review",
                "field": str(gap.get("normalized_field") or ""),
                "message": str(gap.get("reason") or "Required or documented mapping gap needs review before promotion."),
                "source_ref": "",
                "suggested_action": str(gap.get("suggested_action") or gap.get("missing_source_behavior") or "Resolve source or mapping gap."),
                "sheet": str(gap.get("sheet") or ""),
                "section": str(gap.get("section") or ""),
                "binding_id": str(gap.get("binding_id") or ""),
                "target": str(gap.get("target") or ""),
                "status": "manual_review_required",
            }
        )
    return flags


def _values_for_binding(package: Mapping[str, Any], binding: Mapping[str, Any]) -> list[Any]:
    normalized_field = str(binding.get("normalized_field") or "").strip()
    value_shape = str(binding.get("value_shape") or "").strip()
    if not normalized_field:
        return []
    if value_shape in {"quarterly_series", "annual_series"}:
        values = _series_values(package, normalized_field)
    elif value_shape in {"table_rows", "validation_rows"} and _row_schema(binding):
        values = _row_schema_values(package, binding)
    elif value_shape in {"table_rows", "validation_rows"}:
        values = _table_values(package, normalized_field)
    else:
        values = [_field_value(_path_get(package, normalized_field))]
    return [value for value in values if value not in (None, "")]


def _series_values(package: Mapping[str, Any], normalized_field: str) -> list[Any]:
    parsed = _indexed_collection_path(normalized_field)
    if parsed is None:
        return [_field_value(_path_get(package, normalized_field))]
    collection_path, field_path = parsed
    collection = _path_get(package, collection_path)
    if not isinstance(collection, list):
        return []
    return [_field_value(_path_get(item, field_path)) for item in collection]


def _table_values(package: Mapping[str, Any], normalized_field: str) -> list[Any]:
    if normalized_field in {"mapping_gaps", "manual_review_flags"}:
        raw = _path_get(package, normalized_field)
        if not isinstance(raw, list):
            return []
        return [_compact_row_text(item) for item in raw if isinstance(item, Mapping)]
    parsed = _indexed_collection_path(normalized_field)
    if parsed is None:
        value = _path_get(package, normalized_field)
        if isinstance(value, list):
            return [_compact_row_text(item) if isinstance(item, Mapping) else item for item in value]
        return [_field_value(value)]
    collection_path, field_path = parsed
    collection = _path_get(package, collection_path)
    if not isinstance(collection, list):
        return []
    return [_field_value(_path_get(item, field_path)) for item in collection]


def _row_schema(binding: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    schema = binding.get("row_schema")
    if not isinstance(schema, list):
        return []
    return [column for column in schema if isinstance(column, Mapping)]


def _row_schema_values(package: Mapping[str, Any], binding: Mapping[str, Any]) -> list[dict[str, Any]]:
    collection_path = str(binding.get("row_source") or "")
    if not collection_path:
        parsed = _indexed_collection_path(str(binding.get("normalized_field") or ""))
        collection_path = parsed[0] if parsed else str(binding.get("normalized_field") or "")
    collection = _path_get(package, collection_path)
    if not isinstance(collection, list):
        return []
    return _row_schema_values_from_items(collection, _row_schema(binding))


def _row_schema_values_from_items(
    items: Sequence[Mapping[str, Any]],
    row_schema: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, Mapping):
            continue
        out: dict[str, Any] = {}
        for column in row_schema:
            column_id = str(column.get("column_id") or "")
            source_field = str(column.get("source_field") or column_id)
            if not column_id:
                continue
            value = _field_value(_path_get(item, source_field))
            if value in (None, ""):
                continue
            out[column_id] = value
        if out:
            rows.append(out)
    return rows


def _indexed_collection_path(path: str) -> tuple[str, str] | None:
    parts = path.split(".")
    for idx, part in enumerate(parts):
        if part == "0" and idx > 0 and idx < len(parts) - 1:
            return ".".join(parts[:idx]), ".".join(parts[idx + 1 :])
    return None


def _path_get(obj: Any, dotted_path: str) -> Any:
    current = obj
    for part in dotted_path.split("."):
        if isinstance(current, Mapping):
            if part not in current:
                return None
            current = current[part]
            continue
        if isinstance(current, list):
            try:
                current = current[int(part)]
            except (ValueError, IndexError):
                return None
            continue
        return None
    return current


def _field_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        if str(value.get("status") or "") != "populated":
            return None
        return value.get("value")
    return value


def _compact_row_text(item: Mapping[str, Any]) -> str:
    for key in ("message", "reason", "normalized_field", "field", "metric", "note"):
        value = item.get(key)
        if value not in (None, ""):
            return str(value)
    return json.dumps(dict(item), sort_keys=True)


def _write_values_for_binding(wb: Any, binding: Mapping[str, Any], values: Sequence[Any], *, ticker: str) -> int:
    sheet_name = _resolve_sheet_name(str(binding["sheet"]), ticker)
    ws = wb[sheet_name]
    target = str(binding["target"])
    value_shape = str(binding.get("value_shape") or "")
    cells = list(_iter_target_cells(ws, target))
    if not cells:
        return 0
    if value_shape in {"table_rows", "validation_rows"} and _row_schema(binding):
        return _write_row_schema_values(
            wb,
            binding,
            [value for value in values if isinstance(value, Mapping)],
            ticker=ticker,
        )
    if value_shape in {"scalar", "text_block"}:
        cells[0].value = values[0]
        return 1
    count = 0
    for cell, value in zip(cells, values):
        cell.value = value
        count += 1
    return count


def _write_row_schema_values(
    wb: Any,
    binding: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    *,
    ticker: str,
) -> int:
    if not rows:
        return 0
    ws = wb[_resolve_sheet_name(str(binding["sheet"]), ticker)]
    min_col, min_row, max_col, max_row = range_boundaries(str(binding["target"]))
    row_schema = _row_schema(binding)
    count = 0
    row_idx = min_row
    for row_values in rows:
        if row_idx > max_row:
            break
        row_written = 0
        while row_idx <= max_row and row_written == 0:
            row_written = _write_row_schema_row(
                ws,
                binding,
                row_schema,
                row_values,
                row_idx=row_idx,
                min_col=min_col,
                max_col=max_col,
            )
            row_idx += 1
        count += row_written
    return count


def _write_row_schema_row(
    ws: Any,
    binding: Mapping[str, Any],
    row_schema: Sequence[Mapping[str, Any]],
    row_values: Mapping[str, Any],
    *,
    row_idx: int,
    min_col: int,
    max_col: int,
) -> int:
    merged_values: dict[str, list[tuple[str, Any]]] = {}
    merged_cells: dict[str, Any] = {}
    for column in row_schema:
        column_id = str(column.get("column_id") or "")
        target_column = str(column.get("target_column") or "")
        if not column_id or not target_column:
            continue
        col_idx = column_index_from_string(target_column)
        if col_idx < min_col or col_idx > max_col:
            raise BindingContractError(
                f"Binding {binding.get('binding_id')} row_schema target column {target_column} is outside {binding.get('target')}."
            )
        value = row_values.get(column_id)
        if value in (None, ""):
            continue
        cell = _writable_cell_for_target(ws, row_idx, col_idx, min_col, max_col)
        if cell is None:
            continue
        merged_values.setdefault(cell.coordinate, []).append((column_id, value))
        merged_cells[cell.coordinate] = cell
    count = 0
    for coord, parts in merged_values.items():
        cell = merged_cells[coord]
        if len(parts) == 1:
            cell.value = parts[0][1]
        else:
            cell.value = " | ".join(f"{column_id}: {value}" for column_id, value in parts)
        count += 1
    return count


def _writable_cell_for_target(ws: Any, row_idx: int, col_idx: int, min_col: int, max_col: int) -> Any | None:
    cell = ws.cell(row_idx, col_idx)
    if not isinstance(cell, MergedCell):
        return cell
    for merged_range in ws.merged_cells.ranges:
        if (
            merged_range.min_row <= row_idx <= merged_range.max_row
            and merged_range.min_col <= col_idx <= merged_range.max_col
        ):
            if merged_range.min_col < min_col or merged_range.min_col > max_col:
                return None
            return ws.cell(merged_range.min_row, merged_range.min_col)
    return None


def _iter_target_cells(ws: Any, target: str) -> Iterable[Any]:
    min_col, min_row, max_col, max_row = range_boundaries(target)
    for row in ws.iter_rows(min_row=min_row, max_row=max_row, min_col=min_col, max_col=max_col):
        for cell in row:
            if isinstance(cell, MergedCell):
                continue
            yield cell


def _write_validation_rows(
    wb: Any,
    bindings: Sequence[Mapping[str, Any]],
    *,
    normalized_validation_issues: Sequence[Mapping[str, Any]],
    mapping_gaps: Sequence[Mapping[str, Any]],
    manual_review_flags: Sequence[Mapping[str, Any]],
    ticker: str,
) -> int:
    written = 0
    validation_targets = {str(binding["binding_id"]): binding for binding in bindings if str(binding.get("source_policy")) == "validation-output"}
    if "qa_log_validation_rows" in validation_targets:
        written += _write_structured_rows(
            wb,
            validation_targets["qa_log_validation_rows"],
            [*normalized_validation_issues, *manual_review_flags],
            ticker=ticker,
        )
    if "needs_review_validation_rows" in validation_targets:
        written += _write_structured_rows(wb, validation_targets["needs_review_validation_rows"], manual_review_flags, ticker=ticker)
    if "qa_checks_mapping_gap_rows" in validation_targets:
        written += _write_structured_rows(wb, validation_targets["qa_checks_mapping_gap_rows"], _gap_issue_dicts(mapping_gaps), ticker=ticker)
    return written


def _write_structured_rows(
    wb: Any,
    binding: Mapping[str, Any],
    items: Sequence[Mapping[str, Any]],
    *,
    ticker: str,
) -> int:
    if not items:
        return 0
    schema = _row_schema(binding)
    if schema:
        return _write_row_schema_values(wb, binding, _row_schema_values_from_items(items, schema), ticker=ticker)
    return _write_rows(wb, binding, _issue_rows(items), ticker=ticker)


def _issue_rows(items: Sequence[Mapping[str, Any]]) -> list[list[Any]]:
    rows: list[list[Any]] = []
    for item in items:
        rows.append(
            [
                item.get("severity", ""),
                item.get("rule_id", ""),
                item.get("field", ""),
                item.get("message", ""),
                item.get("source_ref", ""),
                item.get("suggested_action", ""),
                item.get("sheet", ""),
                item.get("section", ""),
                item.get("binding_id", ""),
                item.get("target", ""),
                item.get("status", ""),
            ]
        )
    return rows


def _gap_issue_dicts(items: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in items:
        rows.append(
            {
                "severity": "P2",
                "rule_id": "mapping_gap",
                "normalized_field": item.get("normalized_field", ""),
                "reason": item.get("reason", "") or item.get("missing_source_behavior", "") or "Mapped field is not populated.",
                "source_ref": item.get("source_ref", ""),
                "suggested_action": item.get("suggested_action", "") or item.get("promotion_requirement", ""),
                "sheet": item.get("sheet", ""),
                "section": item.get("section", ""),
                "binding_id": item.get("binding_id", ""),
                "target": item.get("target", ""),
                "status": "missing_mapping",
            }
        )
    return rows


def _gap_rows(items: Sequence[Mapping[str, Any]]) -> list[list[Any]]:
    rows: list[list[Any]] = []
    for item in items:
        rows.append(
            [
                "P2",
                "mapping_gap",
                item.get("normalized_field", ""),
                item.get("reason", "") or item.get("missing_source_behavior", "") or "Mapped field is not populated.",
                item.get("source_ref", ""),
                item.get("suggested_action", "") or item.get("promotion_requirement", ""),
                item.get("sheet", ""),
                item.get("section", ""),
                item.get("binding_id", ""),
                item.get("target", ""),
                "missing_mapping",
            ]
        )
    return rows


def _write_rows(wb: Any, binding: Mapping[str, Any], rows: Sequence[Sequence[Any]], *, ticker: str) -> int:
    if not rows:
        return 0
    ws = wb[_resolve_sheet_name(str(binding["sheet"]), ticker)]
    min_col, min_row, max_col, max_row = range_boundaries(str(binding["target"]))
    count = 0
    for row_offset, row_values in enumerate(rows):
        row_idx = min_row + row_offset
        if row_idx > max_row:
            break
        for col_offset, value in enumerate(row_values):
            col_idx = min_col + col_offset
            if col_idx > max_col:
                break
            ws.cell(row_idx, col_idx).value = value
            count += 1
    return count
