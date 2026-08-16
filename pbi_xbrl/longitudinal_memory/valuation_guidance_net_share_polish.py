"""Bounded Guidance / Capital Return polish for the accepted ANF Valuation preview.

This module is intentionally narrow.  It removes only the visible Valuation
Operating Drivers consumer, compresses the two retained Guidance tables, and
adds a historical net-share-reduction percentage to the Capital Return
presentation.  Economics remain code-owned and are written as source-native
literals; the workbook does not become a second calculation engine.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, timedelta
from decimal import Decimal
from html import escape
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Mapping, Sequence
from zipfile import ZipFile

from pbi_xbrl.new_ticker_capital_return import validate_capital_return_records
from pbi_xbrl.longitudinal_memory.summary_bs_workbook_materialization import (
    CANONICAL_OOXML_HASH_CONTRACT,
    _cell_elements,
    _set_attribute,
    _sheet_part_map,
    canonical_ooxml_sha256,
    sha256_file,
)
from pbi_xbrl.longitudinal_memory.valuation_final_investor_polish import (
    COMMENTS_PART,
    COMMENTS_VML_PART,
    MARKET_FORMULAS,
    NORMAL_VALUATION_ROW_HEIGHT,
    VALUATION_PART,
    _blank_cell,
    _formula_text,
    _inline_cell,
    _readdress_row,
    _row_elements,
    _spacer_row,
)
from pbi_xbrl.longitudinal_memory.valuation_final_layout_cleanup import (
    LINEAGE_SUPPORT_SHEET,
    _COMMENT_RE,
    _VML_SHAPE_RE,
    _attributes,
    _column_name,
    _comment_reference,
    _inline_text,
    _vml_coordinate,
    _write_package_with_addition,
)


POLISH_CONTRACT = "valuation-guidance-net-share-polish@1"
NET_SHARE_PERCENTAGE_CONTRACT = "historical-net-share-reduction-percentage@1"
SEMANTIC_SNAPSHOT_CONTRACT = "valuation-guidance-net-share-polish-semantic-snapshot@1"
EXPECTED_BASE_WORKBOOK_SHA256 = (
    "6d35977d99a485ce12dcc29b315b9c4b2d76cc16a0a2a52733d2f5a979c657c2"
)
EXPECTED_BASE_SEMANTIC_SHA256 = (
    "2c572baeddc0891dbebe73e2648b42f2bce786f645565e3b2319f196f38a9c84"
)
EXPECTED_BASE_CANONICAL_OOXML_SHA256 = (
    "dfe5ea405b8ac74bf7c1413d3ac1fdca5a3e103a22ff7a09ca625737572c8cee"
)

VALUATION_SHEET = "Valuation"
OPERATING_DRIVERS_RANGE = "O37:AC46"
GUIDANCE_BLOCKS = ("O7:AC25", "O27:AC35")
GUIDANCE_VALUE_RANGE_BEFORE = "S:Z"
GUIDANCE_VALUE_RANGE_AFTER = "S:V"
GUIDANCE_TREND_RANGE_BEFORE = "AA:AC"
GUIDANCE_TREND_RANGE_AFTER = "W:AC"
GUIDANCE_ROWS = tuple(range(8, 26)) + tuple(range(28, 36))
GUIDANCE_COMMENT_MOVES = {
    **{f"AA{row}": f"W{row}" for row in range(9, 26)},
    **{f"AA{row}": f"W{row}" for row in range(29, 36)},
}
OPERATING_DRIVER_COMMENT_REMOVALS = (
    "O37",
    *(f"AA{row}" for row in range(39, 47)),
)

NET_SHARE_PERCENTAGE_METRIC_ID = "net_share_reduction_percentage"
NET_SHARE_PERCENTAGE_LABEL = "Net shares retired / (issued) (%)"
NET_SHARE_PERCENTAGE_FORMAT = "0.0%"
NET_SHARE_PERCENTAGE_DEFINITION = (
    "net_share_reduction / compatible beginning-period point-in-time shares outstanding"
)
SEMANTIC_TREND_DEFERRED = "SEMANTIC_TREND_CONTRACT_DEFERRED"

FINAL_VALUATION_DIMENSION = "A1:AI178"
FINAL_VISIBLE_PRODUCT_ROW = 178
BASE_VISIBLE_PRODUCT_ROW = 175
BASE_PLACEHOLDER_END_ROW = 178
SUMMARY_PERCENTAGE_ROW = 155
ANNUAL_PERCENTAGE_ROW = 176
SUMMARY_SPACER_ROW = 156
ANNUAL_SPACER_ROW = 177

# Presentation-only row movement.  Economic identities remain semantic and are
# remapped in the hidden binding support records.
CAPITAL_RETURN_ROW_MAP = {
    148: 148,
    149: 149,
    150: 150,
    151: 151,
    152: 152,
    153: 153,
    154: 154,
    155: 156,
    156: 157,
    157: 158,
    158: 159,
    159: 160,
    160: 161,
    161: 162,
    162: 163,
    163: 164,
    164: 165,
    165: 166,
    166: 167,
    167: 168,
    168: 169,
    169: 170,
    170: 171,
    171: 172,
    172: 173,
    173: 174,
    174: 175,
    175: 178,
}
_CAPITAL_ROW_TEMPLATE_MAP = {
    **{row: row for row in range(145, 155)},
    155: 156,
    156: 157,
    157: 158,
    158: 159,
    159: 160,
    160: 161,
    161: 162,
    162: 163,
    163: 164,
    164: 165,
    165: 166,
    166: 167,
    167: 168,
    168: 169,
    169: 170,
    170: 171,
    171: 172,
    172: 173,
    173: 174,
    174: 175,
    175: 178,
}
_BASE_SPACER_ROWS = {152, 155, 163, 172}

_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_MERGE_CONTAINER_RE = re.compile(rb"<mergeCells\b[^>]*>(?P<body>.*?)</mergeCells>", re.DOTALL)
_MERGE_RE = re.compile(rb"<mergeCell\b[^>]*/>")
_DIMENSION_RE = re.compile(rb"<dimension\b[^>]*/>")
_COLUMN_DEFINITION_RE = re.compile(rb"<col\b[^>]*/>")
_TARGET_RE = re.compile(r"Valuation!([A-Z]+)([1-9][0-9]*)")
_CELL_REFERENCE_RE = re.compile(r"([A-Z]+)([1-9][0-9]*)")
_ANCHOR_RE = re.compile(
    rb"(<[A-Za-z_][A-Za-z0-9_.-]*:Anchor>)([^<]+)(</[A-Za-z_][A-Za-z0-9_.-]*:Anchor>)"
)
_COLUMN_RE = re.compile(
    rb"(<[A-Za-z_][A-Za-z0-9_.-]*:Column>)([0-9]+)(</[A-Za-z_][A-Za-z0-9_.-]*:Column>)"
)


class ValuationGuidanceNetSharePolishError(ValueError):
    """Fail-closed bounded-polish contract violation."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _number_text(value: float | int | Decimal) -> str:
    parsed = Decimal(str(value))
    if not parsed.is_finite():
        raise ValuationGuidanceNetSharePolishError("Workbook values must be finite.")
    return format(parsed, "f")


def _column_number(column: str) -> int:
    result = 0
    for character in column:
        result = result * 26 + ord(character) - 64
    return result


def _coordinate_parts(coordinate: str) -> tuple[str, int]:
    match = _CELL_REFERENCE_RE.fullmatch(coordinate)
    if match is None:
        raise ValuationGuidanceNetSharePolishError(f"Invalid cell {coordinate!r}.")
    return match.group(1), int(match.group(2))


def _guidance_fit_inventory(data: bytes) -> dict[str, Any]:
    widths: dict[int, float] = {}
    for match in _COLUMN_DEFINITION_RE.finditer(data):
        attrs = _attributes(match.group(0))
        minimum = int(attrs.get("min", "0"))
        maximum = int(attrs.get("max", "0"))
        width = float(attrs.get("width", "0"))
        for column in range(minimum, maximum + 1):
            widths[column] = width
    cells = _cell_elements(data)
    value_lengths = [len(_inline_text(cells[f"S{row}"][2])) for row in GUIDANCE_ROWS]
    trend_lengths = [len(_inline_text(cells[f"AA{row}"][2])) for row in GUIDANCE_ROWS]
    value_capacity = sum(widths.get(column, 0.0) for column in range(19, 23))  # S:V
    trend_capacity = sum(widths.get(column, 0.0) for column in range(23, 30))  # W:AC
    if not value_capacity or not trend_capacity:
        raise ValuationGuidanceNetSharePolishError("Guidance column-width contract is missing.")
    if max(value_lengths) > value_capacity or max(trend_lengths) > trend_capacity:
        raise ValuationGuidanceNetSharePolishError(
            "Guidance compression would clip accepted text; widen the presentation contract."
        )
    return {
        "guidance_capacity_width_units": value_capacity,
        "guidance_max_text_length": max(value_lengths),
        "trend_capacity_width_units": trend_capacity,
        "trend_max_text_length": max(trend_lengths),
    }


def _range_intersects(reference: str, target: str) -> bool:
    def bounds(value: str) -> tuple[int, int, int, int]:
        left, _, right = value.partition(":")
        right = right or left
        l_col, l_row = _coordinate_parts(left)
        r_col, r_row = _coordinate_parts(right)
        return _column_number(l_col), l_row, _column_number(r_col), r_row

    l_min_c, l_min_r, l_max_c, l_max_r = bounds(reference)
    r_min_c, r_min_r, r_max_c, r_max_r = bounds(target)
    return not (
        l_max_c < r_min_c
        or r_max_c < l_min_c
        or l_max_r < r_min_r
        or r_max_r < l_min_r
    )


def _rebuild_row(raw: bytes, cells: Mapping[str, bytes]) -> bytes:
    start_end = raw.find(b">")
    close = raw.rfind(b"</row>")
    if start_end < 0 or close < 0:
        raise ValuationGuidanceNetSharePolishError("Malformed worksheet row.")
    located = _cell_elements(raw)
    body = raw[start_end + 1 : close]
    residue = body
    for start, end, _cell in sorted(located.values(), reverse=True):
        local_start = start - (start_end + 1)
        local_end = end - (start_end + 1)
        residue = residue[:local_start] + residue[local_end:]
    if residue.strip():
        raise ValuationGuidanceNetSharePolishError("Row contains unsupported non-cell payload.")

    def key(item: tuple[str, bytes]) -> tuple[int, int]:
        column, row = _coordinate_parts(item[0])
        return row, _column_number(column)

    return raw[: start_end + 1] + b"".join(value for _, value in sorted(cells.items(), key=key)) + b"</row>"


def _move_cell(raw: bytes, *, old: str, new: str) -> bytes:
    end = raw.find(b">")
    if end < 0:
        raise ValuationGuidanceNetSharePolishError("Malformed workbook cell.")
    start = _set_attribute(raw[: end + 1], "r", new)
    result = start + raw[end + 1 :]
    if old.encode("ascii") in result:
        raise ValuationGuidanceNetSharePolishError("Moved cell retained its old address.")
    return result


def _numeric_cell(coordinate: str, value: float, style_id: int) -> bytes:
    return (
        f'<c r="{coordinate}" s="{style_id}" t="n"><v>{_number_text(value)}</v></c>'
    ).encode("utf-8")


def _style_id(raw_cell: bytes) -> int:
    return int(_attributes(raw_cell[: raw_cell.find(b">") + 1]).get("s", "0"))


@dataclass(frozen=True)
class _FiscalPeriodIdentity:
    fiscal_year: int
    fiscal_quarter: int | None
    period_type: str

    @property
    def opening_share_period(self) -> str:
        if self.period_type == "annual":
            return f"{self.fiscal_year - 1}-Q4"
        if self.fiscal_quarter is None:
            raise ValuationGuidanceNetSharePolishError(
                "Quarter-based period lacks a fiscal-quarter identity."
            )
        ending_index = self.fiscal_year * 4 + self.fiscal_quarter - 1
        offset = 4 if self.period_type == "ttm" else 1
        prior_year, prior_zero = divmod(ending_index - offset, 4)
        return f"{prior_year}-Q{prior_zero + 1}"


def _capital_period_identity(record: Mapping[str, Any]) -> _FiscalPeriodIdentity:
    period_type = str(record.get("period_type") or "")
    fiscal_period = str(record.get("fiscal_period") or "")
    patterns = {
        "quarter": re.fullmatch(r"([0-9]{4})-Q([1-4])", fiscal_period),
        "ttm": re.fullmatch(r"TTM through ([0-9]{4})-Q([1-4])", fiscal_period),
        "annual": re.fullmatch(r"([0-9]{4})-FY", fiscal_period),
    }
    match = patterns.get(period_type)
    if match is None:
        raise ValuationGuidanceNetSharePolishError(
            "Capital Return record has inconsistent typed fiscal-period metadata: "
            f"{period_type!r} / {fiscal_period!r}."
        )
    try:
        period_start = date.fromisoformat(str(record.get("period_start") or ""))
        period_end = date.fromisoformat(str(record.get("period_end") or ""))
    except ValueError as exc:
        raise ValuationGuidanceNetSharePolishError(
            f"Capital Return period dates are invalid for {fiscal_period}."
        ) from exc
    if period_start > period_end or record.get("duration_or_instant") != "duration":
        raise ValuationGuidanceNetSharePolishError(
            f"Net-share flow lacks compatible duration metadata for {fiscal_period}."
        )
    return _FiscalPeriodIdentity(
        fiscal_year=int(match.group(1)),
        fiscal_quarter=None if period_type == "annual" else int(match.group(2)),
        period_type=period_type,
    )


def _bs_share_index(
    product: Mapping[str, Any], shadow: Mapping[str, Any]
) -> dict[str, dict[str, Any]]:
    fields = product.get("fields")
    lineage = shadow.get("field_lineage")
    if not isinstance(fields, list) or not isinstance(lineage, list):
        raise ValuationGuidanceNetSharePolishError("Summary/BS product or shadow is malformed.")
    lineage_by_field = {
        str(row.get("field_id") or ""): row
        for row in lineage
        if isinstance(row, Mapping)
    }
    company_id = str(product.get("company_id") or "").casefold()
    if not company_id:
        raise ValuationGuidanceNetSharePolishError("Summary/BS product lacks company identity.")
    result: dict[str, dict[str, Any]] = {}
    for field in fields:
        if not isinstance(field, Mapping) or field.get("metric_key") != "shares_outstanding":
            continue
        if field.get("metric_id") != "metric:financial:shares-outstanding@1":
            continue
        if field.get("semantic_role") != "point_in_time":
            continue
        if field.get("temporal_role") != "point_in_time_reporting_date":
            continue
        if field.get("unit_id") != "unit:core:shares-millions@1":
            continue
        period_id = str(field.get("period_id") or "")
        match = re.fullmatch(
            r"period:([a-z0-9][a-z0-9-]*):fy([0-9]{4})-q([1-4])@([1-9][0-9]*)",
            period_id.casefold(),
        )
        if match is None or match.group(1) != company_id:
            continue
        period = f"{match.group(2)}-Q{match.group(3)}"
        field_id = str(field.get("field_id") or "")
        shadow_row = lineage_by_field.get(field_id)
        if shadow_row is None:
            raise ValuationGuidanceNetSharePolishError(
                f"Summary/BS shares field lacks lineage: {field_id}."
            )
        raw_value = field.get("value")
        value = raw_value.get("value") if isinstance(raw_value, Mapping) else None
        if field.get("status") != "available" or value is None:
            continue
        result[period] = {
            "audit_field_id": str(shadow_row.get("audit_field_id") or ""),
            "canonical_fact_id": str(field.get("canonical_fact_id") or ""),
            "field_id": field_id,
            "legacy_locator": str(shadow_row.get("legacy_locator") or ""),
            "period": period,
            "source_document_ids": list(shadow_row.get("source_document_ids") or []),
            "source_paths": list(shadow_row.get("source_paths") or []),
            "source_sha256s": list(shadow_row.get("source_sha256s") or []),
            "value": float(value),
        }
    if not result:
        raise ValuationGuidanceNetSharePolishError("Summary/BS share-count universe is empty.")
    return result


def _capital_record_index(package: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    section = package.get("capital_returns")
    raw = section.get("records") if isinstance(section, Mapping) else None
    if not isinstance(raw, list):
        raise ValuationGuidanceNetSharePolishError("Capital Return records are unavailable.")
    records = validate_capital_return_records(raw)
    return {str(row["record_id"]): row for row in records}


def _read_support_records(data: bytes) -> tuple[dict[str, Any], ...]:
    cells = _cell_elements(data)
    records: list[dict[str, Any]] = []
    for row in range(1, 29):
        cell = cells.get(f"A{row}")
        if cell is None:
            raise ValuationGuidanceNetSharePolishError(f"Missing lineage record A{row}.")
        records.append(json.loads(_inline_text(cell[2])))
    if len(records) != 28:
        raise ValuationGuidanceNetSharePolishError("Lineage record universe changed from 28.")
    return tuple(records)


def _binding_inventory(records: Sequence[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], str]:
    bindings: list[dict[str, Any]] = []
    for record in records:
        values = record.get("bindings")
        if not isinstance(values, list):
            raise ValuationGuidanceNetSharePolishError("Lineage record lost bindings.")
        bindings.extend(dict(value) for value in values)
    return bindings, _digest(bindings)


def derive_net_share_percentage_records(
    *,
    support_records: Sequence[Mapping[str, Any]],
    package: Mapping[str, Any],
    balance_sheet_product: Mapping[str, Any],
    balance_sheet_shadow: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build Summary and annual-history derived percentage support records."""

    capital_by_id = _capital_record_index(package)
    shares_by_period = _bs_share_index(balance_sheet_product, balance_sheet_shadow)
    numerator_records = {
        (str(row.get("section") or ""), str(row.get("metric_id") or "")): row
        for row in support_records
    }

    outputs: list[dict[str, Any]] = []
    section_contracts = (
        ("capital_return_summary", SUMMARY_PERCENTAGE_ROW),
        ("annual_capital_return_history", ANNUAL_PERCENTAGE_ROW),
    )
    for section, target_row in section_contracts:
        numerator_support = numerator_records.get((section, "net_share_reduction"))
        if numerator_support is None:
            raise ValuationGuidanceNetSharePolishError(
                f"Missing net-share numerator support for {section}."
            )
        source_bindings = numerator_support.get("bindings")
        if not isinstance(source_bindings, list):
            raise ValuationGuidanceNetSharePolishError("Net-share support is malformed.")
        bindings: list[dict[str, Any]] = []
        for numerator in source_bindings:
            period = str(numerator.get("period") or "")
            numerator_id = str(numerator.get("source_identity") or "")
            capital_record = capital_by_id.get(numerator_id)
            if capital_record is None or capital_record.get("metric_id") != "net_share_reduction":
                raise ValuationGuidanceNetSharePolishError(
                    f"Capital Return numerator identity is invalid for {period}."
                )
            period_identity = _capital_period_identity(capital_record)
            if capital_record.get("fiscal_period") != period:
                raise ValuationGuidanceNetSharePolishError(
                    f"Capital Return numerator period mismatch for {period}."
                )
            if capital_record.get("unit") != "m shares":
                raise ValuationGuidanceNetSharePolishError("Net-share numerator unit changed.")
            if numerator.get("value") != capital_record.get("value"):
                raise ValuationGuidanceNetSharePolishError(
                    f"Workbook numerator no longer matches source-native Capital Return for {period}."
                )

            component_ids = [str(value) for value in capital_record.get("component_record_ids") or []]
            beginning_candidates = [
                capital_by_id[value]
                for value in component_ids
                if value in capital_by_id
                and capital_by_id[value].get("metric_id") == "beginning_period_end_shares"
            ]
            if len(beginning_candidates) != 1:
                raise ValuationGuidanceNetSharePolishError(
                    f"Net-share numerator {period} lacks one opening-share support record."
                )
            beginning = beginning_candidates[0]
            if (
                beginning.get("aggregation_role") != "point_in_time"
                or beginning.get("duration_or_instant") != "instant"
                or beginning.get("unit") != "m shares"
            ):
                raise ValuationGuidanceNetSharePolishError(
                    f"Opening-share support is not point-in-time shares for {period}."
                )
            try:
                beginning_date = date.fromisoformat(str(beginning.get("period_end") or ""))
                flow_start = date.fromisoformat(str(capital_record.get("period_start") or ""))
            except ValueError as exc:
                raise ValuationGuidanceNetSharePolishError(
                    f"Opening-share period dates are invalid for {period}."
                ) from exc
            if beginning_date + timedelta(days=1) != flow_start:
                raise ValuationGuidanceNetSharePolishError(
                    f"Opening-share support does not immediately precede {period}."
                )
            denominator_period = period_identity.opening_share_period
            denominator = shares_by_period.get(denominator_period)
            numerator_value = numerator.get("value")
            target_match = _TARGET_RE.fullmatch(str(numerator.get("target_cell") or ""))
            if target_match is None:
                raise ValuationGuidanceNetSharePolishError("Numerator binding target is invalid.")
            target_cell = f"Valuation!{target_match.group(1)}{target_row}"
            value: float | None = None
            status = "unavailable"
            source_classification = "unavailable"
            reason = "No compatible accepted Summary/BS beginning-period share fact."
            denominator_identity = ""
            denominator_fact_id = ""
            denominator_audit_id = ""
            denominator_value: float | None = None
            source_ref = str(numerator.get("source_ref") or "")
            component_source_identities = [numerator_id]
            if denominator is not None:
                denominator_value = float(denominator["value"])
                if denominator_value <= 0:
                    raise ValuationGuidanceNetSharePolishError(
                        f"Opening-share denominator is non-positive for {period}."
                    )
                beginning_value = beginning.get("value")
                if not isinstance(beginning_value, (int, float)):
                    raise ValuationGuidanceNetSharePolishError(
                        f"Capital Return opening shares are unavailable for {period}."
                    )
                if abs(float(beginning_value) - denominator_value) > 1e-9:
                    raise ValuationGuidanceNetSharePolishError(
                        "Genuine economic ownership conflict: Capital Return and Summary/BS "
                        f"opening shares disagree for {period}."
                    )
                if not isinstance(numerator_value, (int, float)):
                    raise ValuationGuidanceNetSharePolishError(
                        f"Available net-share numerator is non-numeric for {period}."
                    )
                value = float(numerator_value) / denominator_value
                status = "available"
                source_classification = "derived_exact"
                reason = ""
                denominator_identity = str(denominator["field_id"])
                denominator_fact_id = str(denominator["canonical_fact_id"])
                denominator_audit_id = str(denominator["audit_field_id"])
                component_source_identities.append(denominator_identity)
                source_ref = " + ".join(
                    value
                    for value in (source_ref, str(denominator["legacy_locator"]))
                    if value
                )

            derived_identity = (
                f"capital_return_{NET_SHARE_PERCENTAGE_METRIC_ID}_"
                f"{re.sub(r'[^a-z0-9]+', '_', period.lower()).strip('_')}_"
                f"{period_identity.period_type}"
            )
            binding = {
                "aggregation_role": "non_additive_ratio",
                "component_source_identities": component_source_identities,
                "definition": NET_SHARE_PERCENTAGE_DEFINITION,
                "denominator_audit_field_id": denominator_audit_id,
                "denominator_canonical_fact_id": denominator_fact_id,
                "denominator_field_id": denominator_identity,
                "denominator_period": denominator_period,
                "denominator_value": denominator_value,
                "derivation_rule": "net_share_reduction_m / beginning_period_end_shares_m",
                "display_period": str(numerator.get("display_period") or ""),
                "label": NET_SHARE_PERCENTAGE_LABEL,
                "metric_id": NET_SHARE_PERCENTAGE_METRIC_ID,
                "number_format": NET_SHARE_PERCENTAGE_FORMAT,
                "numerator_field_id": numerator_id,
                "numerator_value": numerator_value,
                "owner": f"capital_return.{NET_SHARE_PERCENTAGE_METRIC_ID}",
                "period": period,
                "period_compatibility": (
                    f"{denominator_period} point-in-time state immediately precedes {period} flow window"
                ),
                "reason": reason,
                "section": section,
                "source_classification": source_classification,
                "source_identity": derived_identity,
                "source_period": period,
                "source_ref": source_ref,
                "status": status,
                "target_cell": target_cell,
                "unit": "%",
                "value": value,
            }
            bindings.append(binding)
        support = {
            "bindings": bindings,
            "contract": NET_SHARE_PERCENTAGE_CONTRACT,
            "metric_id": NET_SHARE_PERCENTAGE_METRIC_ID,
            "section": section,
        }
        support["support_digest"] = _digest(support)
        outputs.append(support)
    return outputs[0], outputs[1]


def _remap_support_records(
    records: Sequence[Mapping[str, Any]],
    additions: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], ...]:
    result: list[dict[str, Any]] = []
    remapped_count = 0
    for raw_record in records:
        record = json.loads(json.dumps(raw_record))
        for binding in record["bindings"]:
            target = str(binding.get("target_cell") or "")
            match = _TARGET_RE.fullmatch(target)
            if match is None:
                raise ValuationGuidanceNetSharePolishError("Capital binding target is malformed.")
            old_row = int(match.group(2))
            if old_row in CAPITAL_RETURN_ROW_MAP:
                binding["target_cell"] = (
                    f"Valuation!{match.group(1)}{CAPITAL_RETURN_ROW_MAP[old_row]}"
                )
                remapped_count += 1
        record["support_digest"] = _digest(
            {key: value for key, value in record.items() if key != "support_digest"}
        )
        result.append(record)
    if remapped_count != 108:
        raise ValuationGuidanceNetSharePolishError(
            f"Expected 108 Capital Return target remaps, found {remapped_count}."
        )
    result.extend(json.loads(json.dumps(value)) for value in additions)
    return tuple(result)


def _support_sheet_xml(records: Sequence[Mapping[str, Any]]) -> bytes:
    if len(records) != 30:
        raise ValuationGuidanceNetSharePolishError("Lineage support must contain 30 records.")
    rows = []
    for row, record in enumerate(records, start=1):
        payload = escape(_canonical_bytes(record).decode("utf-8"), quote=False)
        rows.append(
            f'<row r="{row}"><c r="A{row}" t="inlineStr"><is><t>{payload}</t></is></c></row>'
        )
    return (
        f'<worksheet xmlns="{_MAIN_NS}"><sheetPr><outlinePr summaryBelow="1" '
        f'summaryRight="1"/></sheetPr><dimension ref="A1:A30"/><sheetViews><sheetView '
        f'workbookViewId="0"/></sheetViews><sheetFormatPr baseColWidth="8" '
        f'defaultRowHeight="15"/><sheetData>{"".join(rows)}</sheetData></worksheet>'
    ).encode("utf-8")


def _patch_guidance_and_operating_drivers(data: bytes) -> bytes:
    rows = _row_elements(data)
    output_rows: dict[int, bytes] = {}
    for row in GUIDANCE_ROWS:
        raw = rows[row][2]
        cells = {coordinate: value[2] for coordinate, value in _cell_elements(raw).items()}
        old = f"AA{row}"
        new = f"W{row}"
        if old not in cells:
            raise ValuationGuidanceNetSharePolishError(f"Guidance trend anchor {old} is missing.")
        moved = _move_cell(cells[old], old=old, new=new)
        for coordinate in list(cells):
            column, _ = _coordinate_parts(coordinate)
            if 23 <= _column_number(column) <= 29:  # W:AC
                cells.pop(coordinate)
        cells[new] = moved
        output_rows[row] = _rebuild_row(raw, cells)

    for row in range(37, 47):
        raw = rows[row][2]
        cells = {coordinate: value[2] for coordinate, value in _cell_elements(raw).items()}
        removed = []
        for coordinate in list(cells):
            column, _ = _coordinate_parts(coordinate)
            if 15 <= _column_number(column) <= 29:  # O:AC
                removed.append(coordinate)
                cells.pop(coordinate)
        if not removed:
            raise ValuationGuidanceNetSharePolishError(
                f"Operating Drivers row {row} has no removable cells."
            )
        output_rows[row] = _rebuild_row(raw, cells)

    result = data
    for row in sorted(output_rows, reverse=True):
        start, end, _ = _row_elements(result)[row]
        result = result[:start] + output_rows[row] + result[end:]
    return result


def _percentage_row(
    template: bytes,
    *,
    old_row: int,
    new_row: int,
    bindings: Sequence[Mapping[str, Any]],
) -> bytes:
    result = _readdress_row(template, old_row=old_row, new_row=new_row)
    cells = {coordinate: value[2] for coordinate, value in _cell_elements(result).items()}
    label_coordinate = f"A{new_row}"
    if label_coordinate not in cells:
        raise ValuationGuidanceNetSharePolishError("Percentage-row label template is missing.")
    cells[label_coordinate] = _inline_cell(
        label_coordinate,
        NET_SHARE_PERCENTAGE_LABEL,
        _style_id(cells[label_coordinate]),
    )
    expected_targets = {str(binding["target_cell"]).split("!", 1)[1] for binding in bindings}
    for binding in bindings:
        coordinate = str(binding["target_cell"]).split("!", 1)[1]
        if coordinate not in cells:
            raise ValuationGuidanceNetSharePolishError(
                f"Percentage-row value template {coordinate} is missing."
            )
        style = _style_id(cells[coordinate])
        value = binding.get("value")
        cells[coordinate] = (
            _numeric_cell(coordinate, float(value), style)
            if isinstance(value, (int, float)) and not isinstance(value, bool)
            else _blank_cell(coordinate, style)
        )
    template_value_coordinates = {
        coordinate
        for coordinate in cells
        if _coordinate_parts(coordinate)[0] in {"B", "C", "D"}
    }
    if not expected_targets <= template_value_coordinates:
        raise ValuationGuidanceNetSharePolishError("Percentage-row targets changed.")
    return _rebuild_row(result, cells)


def _patch_capital_rows(
    data: bytes,
    *,
    summary_record: Mapping[str, Any],
    annual_record: Mapping[str, Any],
) -> bytes:
    rows = _row_elements(data)
    if not all(row in rows for row in range(145, BASE_PLACEHOLDER_END_ROW + 1)):
        raise ValuationGuidanceNetSharePolishError("Accepted Capital Return row surface is incomplete.")
    for row in range(BASE_VISIBLE_PRODUCT_ROW + 1, BASE_PLACEHOLDER_END_ROW + 1):
        raw = rows[row][2]
        attrs = _attributes(raw[: raw.find(b">") + 1])
        if _cell_elements(raw) or float(attrs.get("ht", "nan")) != NORMAL_VALUATION_ROW_HEIGHT:
            raise ValuationGuidanceNetSharePolishError(
                f"Valuation placeholder row {row} is not safely replaceable."
            )

    output: dict[int, bytes] = {}
    for old_row, new_row in _CAPITAL_ROW_TEMPLATE_MAP.items():
        if old_row in _BASE_SPACER_ROWS:
            output[new_row] = _spacer_row(new_row)
        else:
            output[new_row] = _readdress_row(rows[old_row][2], old_row=old_row, new_row=new_row)
    output[SUMMARY_PERCENTAGE_ROW] = _percentage_row(
        rows[156][2],
        old_row=156,
        new_row=SUMMARY_PERCENTAGE_ROW,
        bindings=summary_record["bindings"],
    )
    output[ANNUAL_PERCENTAGE_ROW] = _percentage_row(
        rows[175][2],
        old_row=175,
        new_row=ANNUAL_PERCENTAGE_ROW,
        bindings=annual_record["bindings"],
    )
    output[ANNUAL_SPACER_ROW] = _spacer_row(ANNUAL_SPACER_ROW)
    if set(output) != set(range(145, FINAL_VISIBLE_PRODUCT_ROW + 1)):
        missing = sorted(set(range(145, FINAL_VISIBLE_PRODUCT_ROW + 1)) - set(output))
        raise ValuationGuidanceNetSharePolishError(f"Capital row plan is incomplete: {missing}.")
    replacement = b"".join(output[row] for row in range(145, FINAL_VISIBLE_PRODUCT_ROW + 1))
    return data[: rows[145][0]] + replacement + data[rows[BASE_PLACEHOLDER_END_ROW][1] :]


def _patch_merges(data: bytes) -> bytes:
    container = _MERGE_CONTAINER_RE.search(data)
    if container is None:
        raise ValuationGuidanceNetSharePolishError("Valuation merge metadata is missing.")
    guidance_map = {
        **{f"S{row}:Z{row}": f"S{row}:V{row}" for row in GUIDANCE_ROWS},
        **{f"AA{row}:AC{row}": f"W{row}:AC{row}" for row in GUIDANCE_ROWS},
    }
    capital_map = {
        "A158:M158": "A159:M159",
        "A167:M167": "A168:M168",
    }
    retained: list[bytes] = []
    guidance_found: set[str] = set()
    capital_found: set[str] = set()
    operating_removed: list[str] = []
    for match in _MERGE_RE.finditer(container.group("body")):
        raw = match.group(0)
        reference = _attributes(raw).get("ref", "")
        if _range_intersects(reference, OPERATING_DRIVERS_RANGE):
            operating_removed.append(reference)
            continue
        if reference in guidance_map:
            raw = _set_attribute(raw, "ref", guidance_map[reference])
            guidance_found.add(reference)
        elif reference in capital_map:
            raw = _set_attribute(raw, "ref", capital_map[reference])
            capital_found.add(reference)
        retained.append(raw)
    if guidance_found != set(guidance_map):
        raise ValuationGuidanceNetSharePolishError("Guidance merge plan drifted.")
    if capital_found != set(capital_map):
        raise ValuationGuidanceNetSharePolishError("Capital section merge plan drifted.")
    if len(operating_removed) != 37:
        raise ValuationGuidanceNetSharePolishError(
            f"Expected 37 Operating Drivers merges, found {len(operating_removed)}."
        )
    start_end = container.group(0).find(b">")
    start = _set_attribute(container.group(0)[: start_end + 1], "count", str(len(retained)))
    replacement = start + b"".join(retained) + b"</mergeCells>"
    return data[: container.start()] + replacement + data[container.end() :]


def _patch_dimension(data: bytes) -> bytes:
    match = _DIMENSION_RE.search(data)
    if match is None:
        raise ValuationGuidanceNetSharePolishError("Valuation dimension is missing.")
    current = _attributes(match.group(0)).get("ref")
    if current != "A1:AI175":
        raise ValuationGuidanceNetSharePolishError(f"Valuation dimension changed: {current}.")
    replacement = _set_attribute(match.group(0), "ref", FINAL_VALUATION_DIMENSION)
    return data[: match.start()] + replacement + data[match.end() :]


def _patch_valuation(
    data: bytes,
    *,
    summary_record: Mapping[str, Any],
    annual_record: Mapping[str, Any],
) -> bytes:
    result = _patch_guidance_and_operating_drivers(data)
    result = _patch_capital_rows(
        result,
        summary_record=summary_record,
        annual_record=annual_record,
    )
    result = _patch_merges(result)
    return _patch_dimension(result)


def _patch_comments(data: bytes) -> bytes:
    removals = set(OPERATING_DRIVER_COMMENT_REMOVALS)
    moves = dict(GUIDANCE_COMMENT_MOVES)
    removed: set[str] = set()
    moved: set[str] = set()
    output: list[bytes] = []
    cursor = 0
    for match in _COMMENT_RE.finditer(data):
        raw = match.group(0)
        reference = _comment_reference(raw)
        output.append(data[cursor : match.start()])
        cursor = match.end()
        if reference in removals:
            removed.add(reference)
            continue
        if reference in moves:
            end = raw.find(b">")
            raw = _set_attribute(raw[: end + 1], "ref", moves[reference]) + raw[end + 1 :]
            moved.add(reference)
        output.append(raw)
    output.append(data[cursor:])
    if removed != removals or moved != set(moves):
        raise ValuationGuidanceNetSharePolishError("Comment move/removal contract drifted.")
    return b"".join(output)


def _patch_vml(data: bytes) -> bytes:
    removals = set(OPERATING_DRIVER_COMMENT_REMOVALS)
    moves = dict(GUIDANCE_COMMENT_MOVES)
    removed: set[str] = set()
    moved: set[str] = set()
    output: list[bytes] = []
    cursor = 0
    for match in _VML_SHAPE_RE.finditer(data):
        raw = match.group(0)
        coordinate = _vml_coordinate(raw)
        output.append(data[cursor : match.start()])
        cursor = match.end()
        if coordinate in removals:
            removed.add(coordinate)
            continue
        if coordinate in moves:
            old_column, _ = _coordinate_parts(coordinate)
            new_column, _ = _coordinate_parts(moves[coordinate])
            old_index = _column_number(old_column) - 1
            new_index = _column_number(new_column) - 1
            shift = new_index - old_index
            column_match = _COLUMN_RE.search(raw)
            if column_match is None or int(column_match.group(2)) != old_index:
                raise ValuationGuidanceNetSharePolishError(
                    f"VML column ownership changed for {coordinate}."
                )
            raw = (
                raw[: column_match.start()]
                + column_match.group(1)
                + str(new_index).encode("ascii")
                + column_match.group(3)
                + raw[column_match.end() :]
            )
            anchor = _ANCHOR_RE.search(raw)
            if anchor is not None:
                values = [int(value.strip()) for value in anchor.group(2).split(b",")]
                if len(values) != 8:
                    raise ValuationGuidanceNetSharePolishError("VML anchor shape changed.")
                values[0] += shift
                values[2] += shift
                replacement = b", ".join(str(value).encode("ascii") for value in values)
                raw = raw[: anchor.start()] + anchor.group(1) + replacement + anchor.group(3) + raw[anchor.end() :]
            moved.add(coordinate)
        output.append(raw)
    output.append(data[cursor:])
    if removed != removals or moved != set(moves):
        raise ValuationGuidanceNetSharePolishError("VML move/removal contract drifted.")
    return b"".join(output)


@dataclass(frozen=True)
class ValuationGuidanceNetSharePolishPlan:
    contract: str
    base_workbook_sha256: str
    source_package_sha256: str
    balance_sheet_product_sha256: str
    balance_sheet_shadow_sha256: str
    operating_drivers_range: str
    guidance_blocks: tuple[str, ...]
    guidance_value_range_before: str
    guidance_value_range_after: str
    guidance_trend_range_before: str
    guidance_trend_range_after: str
    guidance_fit_inventory: Mapping[str, Any]
    guidance_comment_moves: Mapping[str, str]
    operating_driver_comment_removals: tuple[str, ...]
    capital_return_row_map: Mapping[int, int]
    old_binding_count: int
    old_available_binding_count: int
    new_binding_count: int
    new_available_binding_count: int
    new_unavailable_binding_count: int
    added_metric_instance_count: int
    prior_binding_plan_digest: str
    binding_plan_digest: str
    support_records: tuple[dict[str, Any], ...]
    net_share_percentage_records: tuple[dict[str, Any], ...]
    plan_digest: str

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["capital_return_row_map"] = {
            str(key): value for key, value in sorted(self.capital_return_row_map.items())
        }
        return result


@dataclass(frozen=True)
class ValuationGuidanceNetSharePolishResult:
    contract: str
    plan_digest: str
    base_workbook_sha256: str
    output_workbook_sha256: str
    canonical_ooxml_contract: str
    canonical_ooxml_sha256: str
    changed_ooxml_parts: tuple[str, ...]
    unchanged_ooxml_part_count: int
    valuation_dimension: str
    binding_count: int
    available_binding_count: int
    unavailable_binding_count: int
    binding_plan_digest: str
    visible_valuation_formula_count: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_valuation_guidance_net_share_polish_plan(
    *,
    base_workbook: Path | str,
    source_package: Mapping[str, Any],
    source_package_path: Path | str,
    balance_sheet_product: Mapping[str, Any],
    balance_sheet_product_path: Path | str,
    balance_sheet_shadow: Mapping[str, Any],
    balance_sheet_shadow_path: Path | str,
) -> ValuationGuidanceNetSharePolishPlan:
    base = Path(base_workbook)
    base_hash = sha256_file(base)
    if base_hash != EXPECTED_BASE_WORKBOOK_SHA256:
        raise ValuationGuidanceNetSharePolishError(
            f"Accepted header-polish preview changed: {base_hash}."
        )
    if canonical_ooxml_sha256(base) != EXPECTED_BASE_CANONICAL_OOXML_SHA256:
        raise ValuationGuidanceNetSharePolishError("Accepted canonical OOXML identity changed.")
    with ZipFile(base, "r") as archive:
        members = {info.filename: archive.read(info.filename) for info in archive.infolist()}
        sheet_parts = _sheet_part_map(archive)
    if sheet_parts.get(VALUATION_SHEET) != VALUATION_PART:
        raise ValuationGuidanceNetSharePolishError("Valuation sheet ownership changed.")
    support_part = sheet_parts.get(LINEAGE_SUPPORT_SHEET)
    if support_part is None:
        raise ValuationGuidanceNetSharePolishError("Capital lineage support sheet is missing.")
    valuation = members[VALUATION_PART]
    cells = _cell_elements(valuation)
    guidance_fit = _guidance_fit_inventory(valuation)
    if _inline_text(cells["O37"][2]) != "Operating Drivers":
        raise ValuationGuidanceNetSharePolishError("Operating Drivers baseline changed.")
    if _inline_text(cells["O7"][2])[:8] != "Guidance" or _inline_text(cells["O27"][2])[:8] != "Guidance":
        raise ValuationGuidanceNetSharePolishError("Guidance blocks are missing.")
    formulas = [
        _formula_text(value[2])
        for value in cells.values()
        if _formula_text(value[2]) is not None
    ]
    if len(formulas) != len(MARKET_FORMULAS):
        raise ValuationGuidanceNetSharePolishError("Valuation formula inventory changed.")
    comment_refs = set(_comment_reference(match.group(0)) for match in _COMMENT_RE.finditer(members[COMMENTS_PART]))
    expected_comments = set(GUIDANCE_COMMENT_MOVES) | set(OPERATING_DRIVER_COMMENT_REMOVALS)
    if not expected_comments <= comment_refs:
        raise ValuationGuidanceNetSharePolishError("Guidance/Operating Drivers comment baseline changed.")

    prior_records = _read_support_records(members[support_part])
    old_bindings, old_digest = _binding_inventory(prior_records)
    if len(old_bindings) != 140 or sum(row.get("status") == "available" for row in old_bindings) != 110:
        raise ValuationGuidanceNetSharePolishError("Accepted 140/110 binding baseline changed.")
    summary_percentage, annual_percentage = derive_net_share_percentage_records(
        support_records=prior_records,
        package=source_package,
        balance_sheet_product=balance_sheet_product,
        balance_sheet_shadow=balance_sheet_shadow,
    )
    support_records = _remap_support_records(
        prior_records,
        (summary_percentage, annual_percentage),
    )
    bindings, binding_digest = _binding_inventory(support_records)
    available = sum(row.get("status") == "available" for row in bindings)
    unavailable = len(bindings) - available
    if len(bindings) != 145 or available != 114 or unavailable != 31:
        raise ValuationGuidanceNetSharePolishError(
            f"Expanded binding universe changed: {len(bindings)}/{available}/{unavailable}."
        )
    if any(
        row.get("metric_id") == NET_SHARE_PERCENTAGE_METRIC_ID
        for row in support_records
        if row.get("section") == "quarterly_capital_return_history"
    ):
        raise ValuationGuidanceNetSharePolishError("Quarterly History gained the percentage metric.")
    payload = {
        "base_workbook_sha256": base_hash,
        "binding_plan_digest": binding_digest,
        "capital_return_row_map": {
            str(key): value for key, value in sorted(CAPITAL_RETURN_ROW_MAP.items())
        },
        "contract": POLISH_CONTRACT,
        "guidance_comment_moves": GUIDANCE_COMMENT_MOVES,
        "guidance_fit_inventory": guidance_fit,
        "guidance_value_range_after": GUIDANCE_VALUE_RANGE_AFTER,
        "guidance_trend_range_after": GUIDANCE_TREND_RANGE_AFTER,
        "net_share_percentage_records": (summary_percentage, annual_percentage),
        "operating_driver_comment_removals": OPERATING_DRIVER_COMMENT_REMOVALS,
        "source_package_sha256": sha256_file(Path(source_package_path)),
        "balance_sheet_product_sha256": sha256_file(Path(balance_sheet_product_path)),
        "balance_sheet_shadow_sha256": sha256_file(Path(balance_sheet_shadow_path)),
    }
    return ValuationGuidanceNetSharePolishPlan(
        contract=POLISH_CONTRACT,
        base_workbook_sha256=base_hash,
        source_package_sha256=payload["source_package_sha256"],
        balance_sheet_product_sha256=payload["balance_sheet_product_sha256"],
        balance_sheet_shadow_sha256=payload["balance_sheet_shadow_sha256"],
        operating_drivers_range=OPERATING_DRIVERS_RANGE,
        guidance_blocks=GUIDANCE_BLOCKS,
        guidance_value_range_before=GUIDANCE_VALUE_RANGE_BEFORE,
        guidance_value_range_after=GUIDANCE_VALUE_RANGE_AFTER,
        guidance_trend_range_before=GUIDANCE_TREND_RANGE_BEFORE,
        guidance_trend_range_after=GUIDANCE_TREND_RANGE_AFTER,
        guidance_fit_inventory=guidance_fit,
        guidance_comment_moves=GUIDANCE_COMMENT_MOVES,
        operating_driver_comment_removals=OPERATING_DRIVER_COMMENT_REMOVALS,
        capital_return_row_map=CAPITAL_RETURN_ROW_MAP,
        old_binding_count=140,
        old_available_binding_count=110,
        new_binding_count=145,
        new_available_binding_count=114,
        new_unavailable_binding_count=31,
        added_metric_instance_count=5,
        prior_binding_plan_digest=old_digest,
        binding_plan_digest=binding_digest,
        support_records=support_records,
        net_share_percentage_records=(summary_percentage, annual_percentage),
        plan_digest=_digest(payload),
    )


def materialize_valuation_guidance_net_share_polish(
    *,
    plan: ValuationGuidanceNetSharePolishPlan,
    base_workbook: Path | str,
    output_workbook: Path | str,
) -> ValuationGuidanceNetSharePolishResult:
    if plan.contract != POLISH_CONTRACT:
        raise ValuationGuidanceNetSharePolishError("Polish contract changed.")
    base = Path(base_workbook)
    output = Path(output_workbook)
    if base.resolve() == output.resolve():
        raise ValuationGuidanceNetSharePolishError("Accepted preview cannot be overwritten.")
    if output.exists():
        raise ValuationGuidanceNetSharePolishError(f"Refusing to overwrite {output}.")
    if sha256_file(base) != plan.base_workbook_sha256:
        raise ValuationGuidanceNetSharePolishError("Base workbook changed after planning.")
    with ZipFile(base, "r") as archive:
        original_names = tuple(info.filename for info in archive.infolist())
        members = {info.filename: archive.read(info.filename) for info in archive.infolist()}
        sheet_parts = _sheet_part_map(archive)
    support_part = sheet_parts[LINEAGE_SUPPORT_SHEET]
    summary_record, annual_record = plan.net_share_percentage_records
    members[VALUATION_PART] = _patch_valuation(
        members[VALUATION_PART],
        summary_record=summary_record,
        annual_record=annual_record,
    )
    members[COMMENTS_PART] = _patch_comments(members[COMMENTS_PART])
    members[COMMENTS_VML_PART] = _patch_vml(members[COMMENTS_VML_PART])
    members[support_part] = _support_sheet_xml(plan.support_records)
    _write_package_with_addition(
        base_workbook=base,
        output_workbook=output,
        members=members,
    )
    expected_changed = {VALUATION_PART, COMMENTS_PART, COMMENTS_VML_PART, support_part}
    with ZipFile(base, "r") as before, ZipFile(output, "r") as after:
        if set(before.namelist()) != set(after.namelist()):
            raise ValuationGuidanceNetSharePolishError("OOXML member inventory changed.")
        changed = tuple(
            sorted(name for name in before.namelist() if before.read(name) != after.read(name))
        )
    if set(changed) != expected_changed:
        raise ValuationGuidanceNetSharePolishError(
            f"Unexpected changed OOXML parts: {sorted(set(changed) ^ expected_changed)}."
        )
    output_bindings, output_digest = _binding_inventory(plan.support_records)
    formula_count = sum(
        _formula_text(value[2]) is not None
        for value in _cell_elements(members[VALUATION_PART]).values()
    )
    if formula_count != len(MARKET_FORMULAS):
        raise ValuationGuidanceNetSharePolishError("Valuation formula inventory changed.")
    return ValuationGuidanceNetSharePolishResult(
        contract=POLISH_CONTRACT,
        plan_digest=plan.plan_digest,
        base_workbook_sha256=plan.base_workbook_sha256,
        output_workbook_sha256=sha256_file(output),
        canonical_ooxml_contract=CANONICAL_OOXML_HASH_CONTRACT,
        canonical_ooxml_sha256=canonical_ooxml_sha256(output),
        changed_ooxml_parts=changed,
        unchanged_ooxml_part_count=len(original_names) - len(changed),
        valuation_dimension=FINAL_VALUATION_DIMENSION,
        binding_count=len(output_bindings),
        available_binding_count=sum(row.get("status") == "available" for row in output_bindings),
        unavailable_binding_count=sum(row.get("status") != "available" for row in output_bindings),
        binding_plan_digest=output_digest,
        visible_valuation_formula_count=formula_count,
    )


__all__ = [
    "ANNUAL_PERCENTAGE_ROW",
    "ANNUAL_SPACER_ROW",
    "CAPITAL_RETURN_ROW_MAP",
    "EXPECTED_BASE_CANONICAL_OOXML_SHA256",
    "EXPECTED_BASE_SEMANTIC_SHA256",
    "EXPECTED_BASE_WORKBOOK_SHA256",
    "FINAL_VALUATION_DIMENSION",
    "GUIDANCE_BLOCKS",
    "GUIDANCE_COMMENT_MOVES",
    "GUIDANCE_TREND_RANGE_AFTER",
    "GUIDANCE_TREND_RANGE_BEFORE",
    "GUIDANCE_VALUE_RANGE_AFTER",
    "GUIDANCE_VALUE_RANGE_BEFORE",
    "NET_SHARE_PERCENTAGE_CONTRACT",
    "NET_SHARE_PERCENTAGE_DEFINITION",
    "NET_SHARE_PERCENTAGE_FORMAT",
    "NET_SHARE_PERCENTAGE_LABEL",
    "NET_SHARE_PERCENTAGE_METRIC_ID",
    "OPERATING_DRIVERS_RANGE",
    "OPERATING_DRIVER_COMMENT_REMOVALS",
    "POLISH_CONTRACT",
    "SEMANTIC_SNAPSHOT_CONTRACT",
    "SEMANTIC_TREND_DEFERRED",
    "SUMMARY_PERCENTAGE_ROW",
    "SUMMARY_SPACER_ROW",
    "ValuationGuidanceNetSharePolishError",
    "ValuationGuidanceNetSharePolishPlan",
    "ValuationGuidanceNetSharePolishResult",
    "build_valuation_guidance_net_share_polish_plan",
    "derive_net_share_percentage_records",
    "materialize_valuation_guidance_net_share_polish",
]
