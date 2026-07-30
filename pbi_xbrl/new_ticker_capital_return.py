"""Ticker-neutral Capital Return identities, derivations, and workbook projection."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import hashlib
import json
import re
from typing import Any, Iterable, Mapping, Sequence


SOURCE_CLASSIFICATIONS = frozenset(
    {
        "source_native_numeric",
        "source_native_text",
        "derived_exact",
        "derived_model_output",
        "derived_estimate",
        "period_incompatible",
        "unavailable",
    }
)
PERIOD_TYPES = frozenset(
    {
        "quarter",
        "year_to_date",
        "annual",
        "ttm",
        "point_in_time",
        "guidance",
    }
)
AGGREGATION_ROLES = frozenset(
    {
        "additive_flow",
        "point_in_time",
        "weighted_average",
        "non_additive_ratio",
        "derived_relationship",
        "text_state",
    }
)


class CapitalReturnResolutionError(ValueError):
    """Raised when Capital Return identities cannot be resolved deterministically."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _slug(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", str(value or "").lower()).strip("_")
    if not normalized or not normalized[0].isalpha():
        normalized = f"record_{normalized}"
    return normalized


def _round(value: float | int | None, places: int = 6) -> float | None:
    if value is None:
        return None
    return round(float(value), places)


def make_capital_return_record(
    *,
    metric_id: str,
    semantic_role: str,
    fiscal_period: str,
    period_type: str,
    period_start: str,
    period_end: str,
    duration_or_instant: str,
    publication_date: str,
    source_document: str,
    source_document_sha256: str,
    source_section: str,
    unit: str,
    currency: str,
    scale: str,
    source_classification: str,
    aggregation_role: str,
    evidence_ref: str,
    value: float | int | None = None,
    text_value: str = "",
    derivation_identity: str = "",
    component_record_ids: Sequence[str] = (),
    supersedes_record_ids: Sequence[str] = (),
    status: str | None = None,
    reason: str = "",
    source_alias: str = "",
) -> dict[str, Any]:
    """Create one immutable normalized Capital Return record."""

    metric_id = _slug(metric_id)
    semantic_role = _slug(semantic_role)
    if period_type not in PERIOD_TYPES:
        raise CapitalReturnResolutionError(f"Unsupported Capital Return period type {period_type!r}.")
    if source_classification not in SOURCE_CLASSIFICATIONS:
        raise CapitalReturnResolutionError(
            f"Unsupported Capital Return source classification {source_classification!r}."
        )
    if aggregation_role not in AGGREGATION_ROLES:
        raise CapitalReturnResolutionError(
            f"Unsupported Capital Return aggregation role {aggregation_role!r}."
        )
    if duration_or_instant not in {"duration", "instant", "not_applicable"}:
        raise CapitalReturnResolutionError(
            f"Unsupported duration/instant identity {duration_or_instant!r}."
        )
    if source_classification in {"source_native_numeric", "derived_exact"} and value is None:
        raise CapitalReturnResolutionError(
            f"{source_classification} record {metric_id!r} must contain a numeric value."
        )
    if value is not None and isinstance(value, bool):
        raise CapitalReturnResolutionError(f"Boolean value is invalid for {metric_id!r}.")
    resolved_status = status or ("populated" if value is not None or text_value else "missing_source")
    record_id = _slug(f"capital_return_{metric_id}_{fiscal_period}_{period_type}")
    record = {
        "record_id": record_id,
        "metric_id": metric_id,
        "semantic_role": semantic_role,
        "fiscal_period": str(fiscal_period),
        "period_type": period_type,
        "period_start": str(period_start),
        "period_end": str(period_end),
        "duration_or_instant": duration_or_instant,
        "publication_date": str(publication_date),
        "source_document": str(source_document),
        "source_document_sha256": str(source_document_sha256),
        "source_section": str(source_section),
        "unit": str(unit),
        "currency": str(currency),
        "scale": str(scale),
        "value": _round(value),
        "text_value": str(text_value),
        "status": resolved_status,
        "source_classification": source_classification,
        "aggregation_role": aggregation_role,
        "derivation_identity": str(derivation_identity),
        "component_record_ids": list(dict.fromkeys(str(item) for item in component_record_ids)),
        "evidence_ref": str(evidence_ref),
        "supersedes_record_ids": list(
            dict.fromkeys(str(item) for item in supersedes_record_ids)
        ),
        "reason": str(reason),
        "source_alias": str(source_alias),
    }
    if not record["evidence_ref"]:
        raise CapitalReturnResolutionError(f"Capital Return record {record_id!r} has no evidence.")
    if not re.fullmatch(r"[0-9a-f]{64}", record["source_document_sha256"]):
        raise CapitalReturnResolutionError(
            f"Capital Return record {record_id!r} has an invalid source-document digest."
        )
    return record


def make_unavailable_record(
    *,
    metric_id: str,
    semantic_role: str,
    fiscal_period: str,
    period_type: str,
    period_start: str,
    period_end: str,
    duration_or_instant: str,
    publication_date: str,
    source_document: str,
    source_document_sha256: str,
    source_section: str,
    unit: str,
    currency: str,
    scale: str,
    aggregation_role: str,
    evidence_ref: str,
    reason: str,
    source_alias: str,
) -> dict[str, Any]:
    return make_capital_return_record(
        metric_id=metric_id,
        semantic_role=semantic_role,
        fiscal_period=fiscal_period,
        period_type=period_type,
        period_start=period_start,
        period_end=period_end,
        duration_or_instant=duration_or_instant,
        publication_date=publication_date,
        source_document=source_document,
        source_document_sha256=source_document_sha256,
        source_section=source_section,
        unit=unit,
        currency=currency,
        scale=scale,
        source_classification="unavailable",
        aggregation_role=aggregation_role,
        evidence_ref=evidence_ref,
        value=None,
        status="missing_source",
        reason=reason,
        source_alias=source_alias,
    )


def derive_cash_per_program_share(
    cash_record: Mapping[str, Any],
    share_record: Mapping[str, Any],
) -> dict[str, Any]:
    _require_compatible_periods(cash_record, share_record)
    _require_classification(cash_record, {"source_native_numeric", "derived_exact"})
    _require_classification(share_record, {"source_native_numeric", "derived_exact"})
    if str(cash_record.get("metric_id")) != "repurchase_cash_program":
        raise CapitalReturnResolutionError("Cash/program-share numerator has the wrong identity.")
    if str(share_record.get("metric_id")) != "accounting_program_shares_repurchased":
        raise CapitalReturnResolutionError("Cash/program-share denominator has the wrong identity.")
    _require_units(cash_record, unit="$m", currency="USD")
    _require_units(share_record, unit="m shares", currency="not_applicable")
    cash = _numeric(cash_record)
    shares = _numeric(share_record)
    if shares <= 0:
        raise CapitalReturnResolutionError("Cash/program-share denominator must be positive.")
    return _derived_record(
        metric_id="cash_per_program_share",
        semantic_role="derived_program_execution_measure",
        value=cash / shares,
        unit="$/share",
        currency="USD",
        scale="per_share",
        aggregation_role="derived_relationship",
        derivation_identity="repurchase_cash_program / accounting_program_shares_repurchased",
        components=(cash_record, share_record),
    )


def derive_net_share_reduction(
    repurchase_record: Mapping[str, Any],
    issuance_record: Mapping[str, Any],
    *,
    beginning_shares_record: Mapping[str, Any],
    ending_shares_record: Mapping[str, Any],
    tolerance: float = 0.0015,
) -> dict[str, Any]:
    _require_compatible_periods(repurchase_record, issuance_record)
    _require_classification(repurchase_record, {"source_native_numeric", "derived_exact"})
    _require_classification(issuance_record, {"source_native_numeric", "derived_exact"})
    if str(repurchase_record.get("metric_id")) != "accounting_program_shares_repurchased":
        raise CapitalReturnResolutionError("Net share reduction requires accounting program shares.")
    if str(issuance_record.get("metric_id")) != "share_issuance_sbc":
        raise CapitalReturnResolutionError("Net share reduction requires same-period issuance/SBC.")
    if str(beginning_shares_record.get("metric_id")) != "beginning_period_end_shares":
        raise CapitalReturnResolutionError("Share roll-forward beginning identity is invalid.")
    if str(ending_shares_record.get("metric_id")) != "ending_period_end_shares":
        raise CapitalReturnResolutionError("Share roll-forward ending identity is invalid.")
    for record in (
        repurchase_record,
        issuance_record,
        beginning_shares_record,
        ending_shares_record,
    ):
        _require_classification(record, {"source_native_numeric", "derived_exact"})
        _require_units(record, unit="m shares", currency="not_applicable")
    period_start = date.fromisoformat(str(repurchase_record.get("period_start") or ""))
    period_end = date.fromisoformat(str(repurchase_record.get("period_end") or ""))
    beginning_date = date.fromisoformat(str(beginning_shares_record.get("period_end") or ""))
    ending_date = date.fromisoformat(str(ending_shares_record.get("period_end") or ""))
    if beginning_date + timedelta(days=1) != period_start or ending_date != period_end:
        raise CapitalReturnResolutionError(
            "Share roll-forward snapshots do not bound the exact flow period."
        )
    reduction = _numeric(repurchase_record) - _numeric(issuance_record)
    roll_forward = _numeric(beginning_shares_record) - _numeric(ending_shares_record)
    if abs(reduction - roll_forward) > tolerance:
        raise CapitalReturnResolutionError(
            "Accounting repurchases and issuance do not reconcile to period-end shares."
        )
    return _derived_record(
        metric_id="net_share_reduction",
        semantic_role="exact_period_end_share_roll_forward",
        value=reduction,
        unit="m shares",
        currency="not_applicable",
        scale="millions",
        aggregation_role="derived_relationship",
        derivation_identity=(
            "accounting_program_shares_repurchased - share_issuance_sbc; "
            "reconciled to beginning_period_end_shares - ending_period_end_shares"
        ),
        components=(
            repurchase_record,
            issuance_record,
        ),
        lineage_components=(
            repurchase_record,
            issuance_record,
            beginning_shares_record,
            ending_shares_record,
        ),
    )


def derive_total_capital_return(
    buyback_cash_record: Mapping[str, Any],
    dividend_paid_record: Mapping[str, Any],
) -> dict[str, Any]:
    _require_compatible_periods(buyback_cash_record, dividend_paid_record)
    _require_classification(buyback_cash_record, {"source_native_numeric", "derived_exact"})
    _require_classification(dividend_paid_record, {"source_native_numeric", "derived_exact"})
    if str(buyback_cash_record.get("metric_id")) != "repurchase_cash_program":
        raise CapitalReturnResolutionError("Total capital return requires program repurchase cash.")
    if str(dividend_paid_record.get("metric_id")) != "dividends_paid":
        raise CapitalReturnResolutionError("Total capital return requires paid dividends.")
    _require_units(buyback_cash_record, unit="$m", currency="USD")
    _require_units(dividend_paid_record, unit="$m", currency="USD")
    return _derived_record(
        metric_id="total_capital_return",
        semantic_role="cash_returned_to_shareholders",
        value=_numeric(buyback_cash_record) + _numeric(dividend_paid_record),
        unit="$m",
        currency="USD",
        scale="millions",
        aggregation_role="additive_flow",
        derivation_identity="repurchase_cash_program + dividends_paid",
        components=(buyback_cash_record, dividend_paid_record),
    )


def derive_fcf_coverage(
    numerator_record: Mapping[str, Any],
    free_cash_flow_record: Mapping[str, Any],
    *,
    metric_id: str,
) -> dict[str, Any]:
    _require_compatible_periods(numerator_record, free_cash_flow_record)
    _require_classification(numerator_record, {"source_native_numeric", "derived_exact"})
    _require_classification(free_cash_flow_record, {"source_native_numeric", "derived_exact"})
    _require_units(numerator_record, unit="$m", currency="USD")
    _require_units(free_cash_flow_record, unit="$m", currency="USD")
    fcf = _numeric(free_cash_flow_record)
    if fcf <= 0:
        raise CapitalReturnResolutionError(
            "FCF coverage is unavailable when compatible free cash flow is zero or negative."
        )
    return _derived_record(
        metric_id=metric_id,
        semantic_role="capital_return_fcf_coverage",
        value=_numeric(numerator_record) / fcf,
        unit="%",
        currency="not_applicable",
        scale="ratio",
        aggregation_role="non_additive_ratio",
        derivation_identity=f"{numerator_record['metric_id']} / free_cash_flow",
        components=(numerator_record, free_cash_flow_record),
    )


def _numeric(record: Mapping[str, Any]) -> float:
    value = record.get("value")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CapitalReturnResolutionError(
            f"Capital Return record {record.get('record_id')!r} is not numeric."
        )
    return float(value)


def _require_classification(
    record: Mapping[str, Any],
    allowed: set[str],
) -> None:
    classification = str(record.get("source_classification") or "")
    if classification not in allowed:
        raise CapitalReturnResolutionError(
            f"Record {record.get('record_id')!r} has incompatible classification "
            f"{classification!r}."
        )


def _require_compatible_periods(*records: Mapping[str, Any]) -> None:
    identities = {
        (
            str(record.get("fiscal_period") or ""),
            str(record.get("period_start") or ""),
            str(record.get("period_end") or ""),
            str(record.get("period_type") or ""),
        )
        for record in records
    }
    if len(identities) != 1:
        raise CapitalReturnResolutionError(
            f"Capital Return derivation has period-incompatible operands: {sorted(identities)!r}."
        )


def _require_units(
    record: Mapping[str, Any],
    *,
    unit: str,
    currency: str,
) -> None:
    actual = (str(record.get("unit") or ""), str(record.get("currency") or ""))
    expected = (unit, currency)
    if actual != expected:
        raise CapitalReturnResolutionError(
            f"Record {record.get('record_id')!r} has incompatible unit/currency "
            f"{actual!r}; expected {expected!r}."
        )


def _derived_record(
    *,
    metric_id: str,
    semantic_role: str,
    value: float,
    unit: str,
    currency: str,
    scale: str,
    aggregation_role: str,
    derivation_identity: str,
    components: Sequence[Mapping[str, Any]],
    lineage_components: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    first = components[0]
    _require_compatible_periods(*components)
    lineage = tuple(lineage_components or components)
    source_documents = sorted({str(record.get("source_document") or "") for record in lineage})
    source_hashes = sorted(
        {str(record.get("source_document_sha256") or "") for record in lineage}
    )
    evidence = sorted({str(record.get("evidence_ref") or "") for record in lineage})
    if len(source_hashes) == 1:
        source_hash = source_hashes[0]
    else:
        source_hash = _digest(source_hashes)
    return make_capital_return_record(
        metric_id=metric_id,
        semantic_role=semantic_role,
        fiscal_period=str(first.get("fiscal_period") or ""),
        period_type=str(first.get("period_type") or ""),
        period_start=str(first.get("period_start") or ""),
        period_end=str(first.get("period_end") or ""),
        duration_or_instant="duration",
        publication_date=max(str(record.get("publication_date") or "") for record in lineage),
        source_document=" + ".join(source_documents),
        source_document_sha256=source_hash,
        source_section="exact deterministic derivation",
        unit=unit,
        currency=currency,
        scale=scale,
        source_classification="derived_exact",
        aggregation_role=aggregation_role,
        evidence_ref=" + ".join(evidence),
        value=value,
        derivation_identity=derivation_identity,
        component_record_ids=tuple(str(record.get("record_id") or "") for record in lineage),
        source_alias=str(first.get("source_alias") or ""),
    )


def validate_capital_return_records(records: Iterable[Mapping[str, Any]]) -> tuple[dict[str, Any], ...]:
    normalized: list[dict[str, Any]] = []
    identities: set[tuple[str, str, str]] = set()
    record_ids: set[str] = set()
    for raw in records:
        record = dict(raw)
        required = {
            "record_id",
            "metric_id",
            "semantic_role",
            "fiscal_period",
            "period_type",
            "period_start",
            "period_end",
            "duration_or_instant",
            "publication_date",
            "source_document",
            "source_document_sha256",
            "source_section",
            "unit",
            "currency",
            "scale",
            "value",
            "text_value",
            "status",
            "source_classification",
            "aggregation_role",
            "derivation_identity",
            "component_record_ids",
            "evidence_ref",
            "supersedes_record_ids",
            "reason",
            "source_alias",
        }
        missing = sorted(required - set(record))
        if missing:
            raise CapitalReturnResolutionError(
                f"Capital Return record is missing required fields: {missing!r}."
            )
        record_id = str(record["record_id"])
        identity = (
            str(record["metric_id"]),
            str(record["fiscal_period"]),
            str(record["period_type"]),
        )
        if record_id in record_ids:
            raise CapitalReturnResolutionError(f"Duplicate Capital Return record ID {record_id!r}.")
        if identity in identities:
            raise CapitalReturnResolutionError(
                f"Duplicate Capital Return metric/period identity {identity!r}."
            )
        record_ids.add(record_id)
        identities.add(identity)
        normalized.append(record)
    normalized.sort(
        key=lambda row: (
            str(row.get("period_end") or ""),
            str(row.get("period_type") or ""),
            str(row.get("metric_id") or ""),
        )
    )
    return tuple(normalized)


@dataclass(frozen=True)
class CapitalReturnWorkbookProjection:
    collection_state: str
    latest_quarter_label: str
    ttm_label: str
    annual_label: str
    product_rows: tuple[dict[str, Any], ...]
    support_rows: tuple[dict[str, Any], ...]
    projection_digest: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "collection_state": self.collection_state,
            "latest_quarter_label": self.latest_quarter_label,
            "ttm_label": self.ttm_label,
            "annual_label": self.annual_label,
            "product_rows": [dict(row) for row in self.product_rows],
            "support_rows": [dict(row) for row in self.support_rows],
            "projection_digest": self.projection_digest,
        }


CAPITAL_RETURN_PRODUCT_ROWS = (
    ("repurchase_cash_program", "Buyback cash ($m)", "#,##0.0;-#,##0.0"),
    (
        "accounting_program_shares_repurchased",
        "Program shares repurchased (m)",
        "#,##0.000;-#,##0.000",
    ),
    ("cash_per_program_share", "Cash/program share ($/share)", "0.00;-0.00"),
    ("share_issuance_sbc", "Share issuance/SBC (m)", "#,##0.000;-#,##0.000"),
    ("net_share_reduction", "Net share reduction (m)", "#,##0.000;-#,##0.000"),
    (
        "diluted_weighted_average_shares",
        "Diluted weighted-average shares (m)",
        "#,##0.000;-#,##0.000",
    ),
    ("ending_period_end_shares", "Ending period shares (m)", "#,##0.000;-#,##0.000"),
    ("authorization_remaining", "Authorization remaining ($m)", "#,##0.0;-#,##0.0"),
    (
        "reported_average_all_purchases",
        "Reported average all purchases ($/share)",
        "0.00;-0.00",
    ),
    ("dividends_paid", "Dividends paid ($m)", "#,##0.0;-#,##0.0"),
    ("ordinary_dividend_per_share", "Dividend/share ($/share)", "0.00;-0.00"),
    ("total_capital_return", "Total capital return ($m)", "#,##0.0;-#,##0.0"),
    ("buybacks_to_fcf", "Buybacks/FCF (%)", "0.0%;-0.0%"),
    ("dividends_to_fcf", "Dividends/FCF (%)", "0.0%;-0.0%"),
    ("total_capital_return_to_fcf", "Total capital return/FCF (%)", "0.0%;-0.0%"),
)


def build_capital_return_workbook_projection(
    package: Mapping[str, Any],
) -> CapitalReturnWorkbookProjection:
    section = package.get("capital_returns")
    if not isinstance(section, Mapping):
        return _unavailable_workbook_projection(package)
    raw_records = section.get("records")
    if not isinstance(raw_records, list):
        if section.get("collection_version") is not None:
            raise CapitalReturnResolutionError(
                "Typed Capital Return section has no records collection."
            )
        return _unavailable_workbook_projection(package)
    records = validate_capital_return_records(raw_records)
    latest_quarter = _latest_period(records, "quarter")
    latest_ttm = _latest_period(records, "ttm")
    latest_annual = _latest_period(records, "annual")
    by_identity = {
        (str(row["metric_id"]), str(row["fiscal_period"]), str(row["period_type"])): row
        for row in records
    }

    product_rows: list[dict[str, Any]] = []
    support_rows: list[dict[str, Any]] = []
    for priority, (metric_id, label, number_format) in enumerate(
        CAPITAL_RETURN_PRODUCT_ROWS,
        start=1,
    ):
        period_records = (
            _record_or_none(by_identity, metric_id, latest_quarter),
            _record_or_none(by_identity, metric_id, latest_ttm),
            _record_or_none(by_identity, metric_id, latest_annual),
        )
        values = tuple(_visible_value(record) for record in period_records)
        context = _row_context(metric_id, period_records)
        source_ref = next(
            (
                str(record.get("evidence_ref") or "")
                for record in period_records
                if isinstance(record, Mapping) and str(record.get("evidence_ref") or "")
            ),
            "capital_returns:typed_unavailable",
        )
        product_rows.append(
            {
                "row_key": metric_id,
                "metric": label,
                "latest_quarter": values[0],
                "ttm": values[1],
                "latest_completed_year": values[2],
                "state_context": context,
                "number_format": number_format,
                "priority": priority,
                "source_ref": source_ref,
            }
        )
        support_rows.append(
            {
                "row_key": metric_id,
                "metric_id": metric_id,
                "semantic_role": _first_text(period_records, "semantic_role"),
                "latest_record_id": _record_text(period_records[0], "record_id"),
                "ttm_record_id": _record_text(period_records[1], "record_id"),
                "annual_record_id": _record_text(period_records[2], "record_id"),
                "latest_evidence_ref": _record_text(period_records[0], "evidence_ref"),
                "ttm_evidence_ref": _record_text(period_records[1], "evidence_ref"),
                "annual_evidence_ref": _record_text(period_records[2], "evidence_ref"),
                "latest_classification": _record_text(
                    period_records[0], "source_classification"
                ),
                "ttm_classification": _record_text(period_records[1], "source_classification"),
                "annual_classification": _record_text(
                    period_records[2], "source_classification"
                ),
                "source_ref": source_ref,
            }
        )

    payload = {
        "collection_state": "source_native",
        "latest_quarter_label": latest_quarter[0],
        "ttm_label": latest_ttm[0],
        "annual_label": latest_annual[0],
        "product_rows": product_rows,
        "support_rows": support_rows,
    }
    return CapitalReturnWorkbookProjection(
        collection_state="source_native",
        latest_quarter_label=latest_quarter[0],
        ttm_label=latest_ttm[0],
        annual_label=latest_annual[0],
        product_rows=tuple(product_rows),
        support_rows=tuple(support_rows),
        projection_digest=_digest(payload),
    )


def _unavailable_workbook_projection(
    package: Mapping[str, Any],
) -> CapitalReturnWorkbookProjection:
    latest_quarter = _latest_package_period(
        package,
        "quarterly_financials.rows",
        fallback="Latest quarter",
    )
    latest_annual = _latest_package_period(
        package,
        "annual_financials.rows",
        fallback="Latest completed year",
    )
    ttm_label = (
        f"TTM through {latest_quarter}"
        if latest_quarter != "Latest quarter"
        else "TTM"
    )
    product_rows = []
    support_rows = []
    source_ref = "capital_returns:typed_collection_unavailable"
    for priority, (metric_id, label, number_format) in enumerate(
        CAPITAL_RETURN_PRODUCT_ROWS,
        start=1,
    ):
        product_rows.append(
            {
                "row_key": metric_id,
                "metric": label,
                "latest_quarter": None,
                "ttm": None,
                "latest_completed_year": None,
                "state_context": "Unavailable: no typed source-native Capital Return collection.",
                "number_format": number_format,
                "priority": priority,
                "source_ref": source_ref,
            }
        )
        support_rows.append(
            {
                "row_key": metric_id,
                "metric_id": metric_id,
                "semantic_role": "",
                "latest_record_id": "",
                "ttm_record_id": "",
                "annual_record_id": "",
                "latest_evidence_ref": "",
                "ttm_evidence_ref": "",
                "annual_evidence_ref": "",
                "latest_classification": "unavailable",
                "ttm_classification": "unavailable",
                "annual_classification": "unavailable",
                "source_ref": source_ref,
            }
        )
    payload = {
        "collection_state": "unavailable",
        "latest_quarter_label": latest_quarter,
        "ttm_label": ttm_label,
        "annual_label": latest_annual,
        "product_rows": product_rows,
        "support_rows": support_rows,
    }
    return CapitalReturnWorkbookProjection(
        collection_state="unavailable",
        latest_quarter_label=latest_quarter,
        ttm_label=ttm_label,
        annual_label=latest_annual,
        product_rows=tuple(product_rows),
        support_rows=tuple(support_rows),
        projection_digest=_digest(payload),
    )


def _latest_package_period(
    package: Mapping[str, Any],
    path: str,
    *,
    fallback: str,
) -> str:
    current: Any = package
    for part in path.split("."):
        if not isinstance(current, Mapping):
            return fallback
        current = current.get(part)
    if not isinstance(current, list):
        return fallback
    periods = []
    for row in current:
        if not isinstance(row, Mapping):
            continue
        raw = row.get("period")
        if isinstance(raw, Mapping) and str(raw.get("status") or "") == "populated":
            raw = raw.get("value")
        period = str(raw or "").strip()
        if period:
            periods.append(period)
    return max(periods, default=fallback)


def _latest_period(
    records: Sequence[Mapping[str, Any]],
    period_type: str,
) -> tuple[str, str, str]:
    candidates = {
        (str(row.get("fiscal_period") or ""), str(row.get("period_end") or ""))
        for row in records
        if str(row.get("period_type") or "") == period_type
    }
    if not candidates:
        raise CapitalReturnResolutionError(
            f"Capital Return collection has no {period_type!r} period."
        )
    period, period_end = max(candidates, key=lambda item: (item[1], item[0]))
    if not period or not period_end:
        raise CapitalReturnResolutionError(
            f"Capital Return {period_type!r} period identity is incomplete."
        )
    date.fromisoformat(period_end)
    return period, period_end, period_type


def _record_or_none(
    by_identity: Mapping[tuple[str, str, str], Mapping[str, Any]],
    metric_id: str,
    period: tuple[str, str, str],
) -> Mapping[str, Any] | None:
    period_name, _period_end, period_type = period
    return by_identity.get((metric_id, period_name, period_type))


def _visible_value(record: Mapping[str, Any] | None) -> float | int | None:
    if not isinstance(record, Mapping):
        return None
    if str(record.get("status") or "") != "populated":
        return None
    value = record.get("value")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return value


def _record_text(record: Mapping[str, Any] | None, field: str) -> str:
    if not isinstance(record, Mapping):
        return ""
    return str(record.get(field) or "")


def _first_text(records: Sequence[Mapping[str, Any] | None], field: str) -> str:
    return next((_record_text(record, field) for record in records if _record_text(record, field)), "")


def _row_context(
    metric_id: str,
    records: Sequence[Mapping[str, Any] | None],
) -> str:
    if metric_id == "authorization_remaining":
        return "Point-in-time balance; TTM uses the terminal quarter and is not summed."
    if metric_id == "reported_average_all_purchases":
        return "Filing average includes program and employee tax-withholding purchases."
    if metric_id == "cash_per_program_share":
        return "Exact cash/program-share derivation; distinct from the filing all-purchases average."
    if metric_id in {"dividends_paid", "ordinary_dividend_per_share"}:
        if not any(_visible_value(record) is not None for record in records):
            return "Unavailable: no accepted paid-dividend fact for the selected periods."
    if metric_id in {
        "total_capital_return",
        "dividends_to_fcf",
        "total_capital_return_to_fcf",
    } and not any(_visible_value(record) is not None for record in records):
        return "Unavailable until compatible paid-dividend evidence is established."
    if metric_id.endswith("_to_fcf"):
        reasons = [
            _record_text(record, "reason")
            for record in records
            if _record_text(record, "reason")
        ]
        if reasons:
            return "Coverage is blank for zero, negative, unavailable, or period-incompatible FCF."
        return "Uses source-native cash return and exact same-period FCF."
    classifications = {
        _record_text(record, "source_classification")
        for record in records
        if _record_text(record, "source_classification")
    }
    if classifications <= {"source_native_numeric"}:
        return "Source-native SEC filing facts."
    if "derived_exact" in classifications:
        return "Source-native facts plus exact period-compatible derivation."
    return "Typed source-native Capital Return record."
