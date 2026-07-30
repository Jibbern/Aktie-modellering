"""Source-native ANF Capital Return extraction from already-local SEC documents."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Iterable, Mapping, Sequence
import warnings

from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import pandas as pd

from pbi_xbrl.new_ticker_capital_return import (
    CapitalReturnResolutionError,
    derive_cash_per_program_share,
    derive_fcf_coverage,
    derive_net_share_reduction,
    make_capital_return_record,
    make_unavailable_record,
    validate_capital_return_records,
)


ANF_CAPITAL_RETURN_START = "2024-05-04"
ANF_CAPITAL_RETURN_END = "2026-05-02"
ANF_REQUIRED_QUARTERS = (
    "2024-Q1",
    "2024-Q2",
    "2024-Q3",
    "2024-Q4",
    "2025-Q1",
    "2025-Q2",
    "2025-Q3",
    "2025-Q4",
    "2026-Q1",
)
ANF_REQUIRED_ANNUAL_PERIODS = ("2024-FY", "2025-FY")
ANF_REQUIRED_TTM_COMPONENTS = ("2025-Q2", "2025-Q3", "2025-Q4", "2026-Q1")

_FILING_RE = re.compile(
    r"^doc_(?P<accession>[0-9]{18})_anf-(?P<period>[0-9]{8})\.htm$",
    re.I,
)
_RELEASE_RE = re.compile(
    r"^doc_(?P<accession>[0-9]{18})_(?P<document>q[1-4][0-9]{4}pressrelease)\.htm$",
    re.I,
)
_NUMBER_RE = re.compile(r"[-+]?[0-9]+(?:\.[0-9]+)?")

_CONCEPTS = {
    "repurchase_cash_program": "us-gaap:PaymentsForRepurchaseOfCommonStock",
    "treasury_stock_accounting_cost": "us-gaap:TreasuryStockValueAcquiredCostMethod",
    "employee_tax_withholding_cash": (
        "us-gaap:PaymentsRelatedToTaxWithholdingForShareBasedCompensation"
    ),
    "accounting_program_shares_repurchased": "us-gaap:TreasuryStockSharesAcquired",
    "share_issuance_sbc": "us-gaap:StockIssuedDuringPeriodSharesShareBasedCompensation",
    "basic_weighted_average_shares": "us-gaap:WeightedAverageNumberOfSharesOutstandingBasic",
    "diluted_weighted_average_shares": (
        "us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding"
    ),
    "incremental_dilutive_shares": (
        "us-gaap:IncrementalCommonSharesAttributableToShareBasedPaymentArrangements"
    ),
    "shares_outstanding": "us-gaap:SharesOutstanding",
    "operating_cash_flow": "us-gaap:NetCashProvidedByUsedInOperatingActivities",
    "capital_expenditures": "us-gaap:PaymentsToAcquirePropertyPlantAndEquipment",
}

_METRIC_CONTRACTS = {
    "repurchase_cash_program": (
        "cash_paid_for_program_repurchases",
        "$m",
        "USD",
        "millions",
        "additive_flow",
    ),
    "treasury_stock_accounting_cost": (
        "treasury_stock_cost_including_disclosed_commissions_and_excise_tax",
        "$m",
        "USD",
        "millions",
        "additive_flow",
    ),
    "employee_tax_withholding_cash": (
        "cash_for_employee_tax_withholding_shares",
        "$m",
        "USD",
        "millions",
        "additive_flow",
    ),
    "accounting_program_shares_repurchased": (
        "accounting_program_shares",
        "m shares",
        "not_applicable",
        "millions",
        "additive_flow",
    ),
    "public_program_shares_repurchased": (
        "issuer_table_public_program_shares",
        "m shares",
        "not_applicable",
        "millions",
        "additive_flow",
    ),
    "total_issuer_purchases": (
        "issuer_table_all_shares_purchased",
        "m shares",
        "not_applicable",
        "millions",
        "additive_flow",
    ),
    "employee_tax_withholding_shares": (
        "issuer_table_tax_withholding_shares",
        "m shares",
        "not_applicable",
        "millions",
        "additive_flow",
    ),
    "share_issuance_sbc": (
        "share_based_issuance_and_exercise_shares",
        "m shares",
        "not_applicable",
        "millions",
        "additive_flow",
    ),
    "basic_weighted_average_shares": (
        "reported_basic_weighted_average_denominator",
        "m shares",
        "not_applicable",
        "millions",
        "weighted_average",
    ),
    "diluted_weighted_average_shares": (
        "reported_diluted_weighted_average_denominator",
        "m shares",
        "not_applicable",
        "millions",
        "weighted_average",
    ),
    "incremental_dilutive_shares": (
        "incremental_share_based_dilution",
        "m shares",
        "not_applicable",
        "millions",
        "weighted_average",
    ),
    "beginning_period_end_shares": (
        "period_start_common_shares",
        "m shares",
        "not_applicable",
        "millions",
        "point_in_time",
    ),
    "ending_period_end_shares": (
        "period_end_common_shares",
        "m shares",
        "not_applicable",
        "millions",
        "point_in_time",
    ),
    "authorization_remaining": (
        "repurchase_authorization_remaining",
        "$m",
        "USD",
        "millions",
        "point_in_time",
    ),
    "authorization_total": (
        "approved_repurchase_authorization",
        "$m",
        "USD",
        "millions",
        "point_in_time",
    ),
    "reported_average_all_purchases": (
        "issuer_table_average_all_purchases",
        "$/share",
        "USD",
        "per_share",
        "non_additive_ratio",
    ),
    "operating_cash_flow": (
        "net_cash_from_operating_activities",
        "$m",
        "USD",
        "millions",
        "additive_flow",
    ),
    "capital_expenditures": (
        "cash_capital_expenditures",
        "$m",
        "USD",
        "millions",
        "additive_flow",
    ),
    "free_cash_flow": (
        "operating_cash_flow_less_capital_expenditures",
        "$m",
        "USD",
        "millions",
        "additive_flow",
    ),
}


@dataclass(frozen=True)
class _Context:
    context_id: str
    start: str
    end: str
    instant: str
    members: tuple[str, ...]


@dataclass(frozen=True)
class _Fact:
    value: float
    context_id: str
    evidence_ref: str


@dataclass(frozen=True)
class _Filing:
    path: Path
    accession: str
    form: str
    fiscal_year: int
    fiscal_period: str
    period_start: str
    period_end: str
    publication_date: str
    sha256: str
    size: int
    soup: BeautifulSoup
    contexts: Mapping[str, _Context]

    @property
    def fiscal_period_id(self) -> str:
        return f"{self.fiscal_year}-FY" if self.fiscal_period == "FY" else (
            f"{self.fiscal_year}-{self.fiscal_period}"
        )

    @property
    def source_alias(self) -> str:
        return f"ANF {self.form} {self.period_end}"


@dataclass(frozen=True)
class ANFCapitalReturnSourceExtraction:
    records: tuple[Mapping[str, Any], ...]
    guidance: tuple[Mapping[str, Any], ...]
    period_reconciliations: tuple[Mapping[str, Any], ...]
    source_documents: tuple[Mapping[str, Any], ...]

    def package_section(self) -> dict[str, Any]:
        return {
            "collection_version": "1.0",
            "records": [dict(record) for record in self.records],
            "guidance": [dict(record) for record in self.guidance],
            "period_reconciliations": [
                dict(reconciliation) for reconciliation in self.period_reconciliations
            ],
        }

    def coverage(self) -> dict[str, Any]:
        return {
            "adapter": "pbi_xbrl.anf_capital_return_source_adapter",
            "network_access": False,
            "history_start": ANF_CAPITAL_RETURN_START,
            "history_end": ANF_CAPITAL_RETURN_END,
            "record_count": len(self.records),
            "guidance_count": len(self.guidance),
            "period_reconciliation_count": len(self.period_reconciliations),
            "source_document_count": len(self.source_documents),
            "source_documents": [dict(row) for row in self.source_documents],
        }


class CapitalReturnSourceFactMissing(CapitalReturnResolutionError):
    """A genuinely absent optional SEC fact, distinct from conflicting evidence."""


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _canonical_accession(digits: str) -> str:
    if not re.fullmatch(r"[0-9]{18}", digits):
        raise CapitalReturnResolutionError(
            "ANF Capital Return source filename has no canonical SEC accession."
        )
    return f"{digits[:10]}-{digits[10:12]}-{digits[12:]}"


def _source_ref(path: Path, fragment: str) -> str:
    return f"sec_cache/ANF/{path.name}#{fragment}"


def _parse_date_text(value: str) -> str:
    cleaned = _clean_text(value)
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(cleaned, fmt).date().isoformat()
        except ValueError:
            continue
    return ""


def _soup(raw_html: bytes) -> BeautifulSoup:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", XMLParsedAsHTMLWarning)
        return BeautifulSoup(raw_html, "lxml")


def _ix_text(soup: BeautifulSoup, concept: str) -> tuple[str, str]:
    rows = {
        (_clean_text(tag.get_text(" ")), str(tag.get("contextref") or ""))
        for tag in soup.find_all(attrs={"name": re.compile(rf"^{re.escape(concept)}$", re.I)})
        if _clean_text(tag.get_text(" "))
    }
    values = {value for value, _ in rows}
    if len(values) != 1:
        raise CapitalReturnResolutionError(
            f"Inline-XBRL identity {concept!r} did not resolve uniquely: {sorted(values)!r}."
        )
    value = next(iter(values))
    context_ids = {context_id for row_value, context_id in rows if row_value == value}
    if len(context_ids) != 1:
        raise CapitalReturnResolutionError(
            f"Inline-XBRL identity {concept!r} has ambiguous contexts: "
            f"{sorted(context_ids)!r}."
        )
    return value, next(iter(context_ids))


def _contexts(soup: BeautifulSoup) -> dict[str, _Context]:
    result: dict[str, _Context] = {}
    for tag in soup.find_all(
        lambda item: bool(item.name) and item.name.casefold().endswith(":context")
    ):
        context_id = str(tag.get("id") or "")
        if not context_id or context_id in result:
            if context_id:
                raise CapitalReturnResolutionError(
                    f"Duplicate inline-XBRL context ID {context_id!r}."
                )
            continue
        start_tag = tag.find(
            lambda item: bool(item.name) and item.name.casefold().endswith(":startdate")
        )
        end_tag = tag.find(
            lambda item: bool(item.name) and item.name.casefold().endswith(":enddate")
        )
        instant_tag = tag.find(
            lambda item: bool(item.name) and item.name.casefold().endswith(":instant")
        )
        members = tuple(
            sorted(
                {
                    _clean_text(member.get_text(" "))
                    for member in tag.find_all(
                        lambda item: bool(item.name)
                        and item.name.casefold().endswith(":explicitmember")
                    )
                    if _clean_text(member.get_text(" "))
                }
            )
        )
        result[context_id] = _Context(
            context_id=context_id,
            start=_clean_text(start_tag.get_text(" ")) if start_tag else "",
            end=_clean_text(end_tag.get_text(" ")) if end_tag else "",
            instant=_clean_text(instant_tag.get_text(" ")) if instant_tag else "",
            members=members,
        )
    return result


def _publication_dates(sec_cache_root: Path) -> dict[str, str]:
    submission_path = sec_cache_root / "0001018840" / "submissions.json"
    if not submission_path.exists():
        raise CapitalReturnResolutionError(
            "Local SEC submissions metadata is required for filing dates."
        )
    payload = json.loads(submission_path.read_text(encoding="utf-8"))
    recent = payload.get("filings", {}).get("recent", {})
    accessions = list(recent.get("accessionNumber") or [])
    filing_dates = list(recent.get("filingDate") or [])
    result = {
        str(accession): str(filing_dates[index])
        for index, accession in enumerate(accessions)
        if index < len(filing_dates) and str(accession) and str(filing_dates[index])
    }
    return result


def _index_publication_date(sec_cache_root: Path, accession_digits: str) -> str:
    index_path = sec_cache_root / f"index_{accession_digits}.json"
    if not index_path.exists():
        raise CapitalReturnResolutionError(
            f"No local SEC filing-date metadata exists for {accession_digits!r}."
        )
    payload = json.loads(index_path.read_text(encoding="utf-8"))
    items = payload.get("directory", {}).get("item", [])
    dates = {
        str(row.get("last-modified") or "")[:10]
        for row in items
        if isinstance(row, Mapping)
        and re.fullmatch(
            r"[0-9]{4}-[0-9]{2}-[0-9]{2}",
            str(row.get("last-modified") or "")[:10],
        )
    }
    if len(dates) != 1:
        raise CapitalReturnResolutionError(
            f"SEC index metadata for {accession_digits!r} has ambiguous dates."
        )
    return next(iter(dates))


def _parse_inline_number(tag: Any, *, expected_unit: str) -> float | None:
    text = _clean_text(tag.get_text(" "))
    if not text or text.casefold() in {"-", "\u2014", "nan"}:
        return None
    cleaned = text.strip("()$ ").replace(",", "")
    match = _NUMBER_RE.fullmatch(cleaned)
    if match is None:
        return None
    value = float(cleaned)
    if text.startswith("(") and text.endswith(")"):
        value = -value
    if str(tag.get("sign") or "") == "-":
        value = -abs(value)
    scale = int(str(tag.get("scale") or "0"))
    actual = value * (10**scale)
    if expected_unit in {"$m", "m shares"}:
        return actual / 1_000_000
    return actual


def _fact_candidates(
    filing: _Filing,
    concept: str,
    *,
    start: str = "",
    end: str = "",
    instant: str = "",
    member: str | None = None,
    dimensionless: bool = False,
    expected_unit: str,
) -> tuple[_Fact, ...]:
    rows: list[_Fact] = []
    for tag in filing.soup.find_all(
        attrs={"name": re.compile(rf"^{re.escape(concept)}$", re.I)}
    ):
        context_id = str(tag.get("contextref") or "")
        context = filing.contexts.get(context_id)
        if context is None:
            continue
        if start and context.start != start:
            continue
        if end and context.end != end:
            continue
        if instant and context.instant != instant:
            continue
        if dimensionless and context.members:
            continue
        if member is not None and member not in context.members:
            continue
        value = _parse_inline_number(tag, expected_unit=expected_unit)
        if value is None:
            continue
        rows.append(
            _Fact(
                value=round(value, 9),
                context_id=context_id,
                evidence_ref=_source_ref(
                    filing.path,
                    f"xbrl:{concept}:{context_id}",
                ),
            )
        )
    deduped = {
        (row.value, row.context_id, row.evidence_ref): row
        for row in rows
    }
    return tuple(
        sorted(
            deduped.values(),
            key=lambda row: (row.context_id.casefold(), row.value, row.evidence_ref),
        )
    )


def _select_fact(
    filing: _Filing,
    concept: str,
    *,
    start: str = "",
    end: str = "",
    instant: str = "",
    member: str | None = None,
    dimensionless: bool = False,
    expected_unit: str,
) -> _Fact:
    candidates = _fact_candidates(
        filing,
        concept,
        start=start,
        end=end,
        instant=instant,
        member=member,
        dimensionless=dimensionless,
        expected_unit=expected_unit,
    )
    if not candidates:
        raise CapitalReturnSourceFactMissing(
            f"No exact {concept!r} fact exists for "
            f"{start or instant!r} through {end or instant!r}."
        )
    values = {row.value for row in candidates}
    if len(values) != 1:
        raise CapitalReturnResolutionError(
            f"Conflicting {concept!r} facts exist for the selected context: "
            f"{sorted(values)!r}."
        )
    evidence_refs = sorted({row.evidence_ref for row in candidates})
    return _Fact(
        value=next(iter(values)),
        context_id="+".join(sorted({row.context_id for row in candidates})),
        evidence_ref=" + ".join(evidence_refs),
    )


def _parse_filing(
    path: Path,
    *,
    publication_dates: Mapping[str, str],
    sec_cache_root: Path,
) -> _Filing:
    match = _FILING_RE.fullmatch(path.name)
    if match is None:
        raise CapitalReturnResolutionError(
            f"Unsupported ANF Capital Return filing filename {path.name!r}."
        )
    raw = path.read_bytes()
    soup = _soup(raw)
    contexts = _contexts(soup)
    fiscal_year_text, identity_context_id = _ix_text(soup, "dei:DocumentFiscalYearFocus")
    fiscal_period, period_context_id = _ix_text(soup, "dei:DocumentFiscalPeriodFocus")
    form, form_context_id = _ix_text(soup, "dei:DocumentType")
    period_end_text, end_context_id = _ix_text(soup, "dei:DocumentPeriodEndDate")
    if len({identity_context_id, period_context_id, form_context_id, end_context_id}) != 1:
        raise CapitalReturnResolutionError(
            f"ANF filing {path.name!r} has incompatible document identity contexts."
        )
    main_context = contexts.get(identity_context_id)
    if main_context is None or not main_context.start or not main_context.end:
        raise CapitalReturnResolutionError(
            f"ANF filing {path.name!r} has no exact main duration context."
        )
    period_end = _parse_date_text(period_end_text)
    if period_end != main_context.end:
        raise CapitalReturnResolutionError(
            f"ANF filing {path.name!r} period-end identity does not match its context."
        )
    accession_digits = match.group("accession")
    accession = _canonical_accession(accession_digits)
    publication_date = publication_dates.get(accession) or _index_publication_date(
        sec_cache_root,
        accession_digits,
    )
    if form not in {"10-Q", "10-K"} or fiscal_period not in {"Q1", "Q2", "Q3", "FY"}:
        raise CapitalReturnResolutionError(
            f"ANF filing {path.name!r} has unsupported form/period {form!r}/{fiscal_period!r}."
        )
    return _Filing(
        path=path,
        accession=accession,
        form=form,
        fiscal_year=int(fiscal_year_text),
        fiscal_period=fiscal_period,
        period_start=main_context.start,
        period_end=main_context.end,
        publication_date=publication_date,
        sha256=_sha256_bytes(raw),
        size=len(raw),
        soup=soup,
        contexts=contexts,
    )


def _discover_filings(sec_cache_root: Path) -> tuple[_Filing, ...]:
    publication_dates = _publication_dates(sec_cache_root)
    candidates: list[Path] = []
    for path in sorted(
        sec_cache_root.glob("doc_0001018840*_anf-*.htm"),
        key=lambda item: item.name.casefold(),
    ):
        match = _FILING_RE.fullmatch(path.name)
        if match is None:
            continue
        period = match.group("period")
        if (
            ANF_CAPITAL_RETURN_START.replace("-", "")
            <= period
            <= ANF_CAPITAL_RETURN_END.replace("-", "")
        ):
            raw = path.read_bytes()
            if (
                b"DocumentFiscalYearFocus" not in raw
                or b"DocumentFiscalPeriodFocus" not in raw
                or b"DocumentType" not in raw
            ):
                continue
            candidates.append(path)
    filings = tuple(
        sorted(
            (
                _parse_filing(
                    path,
                    publication_dates=publication_dates,
                    sec_cache_root=sec_cache_root,
                )
                for path in candidates
            ),
            key=lambda row: (
                row.period_end,
                row.form,
                row.accession,
            ),
        )
    )
    identities = [filing.fiscal_period_id for filing in filings]
    duplicates = sorted(
        {
            identity
            for identity in identities
            if identities.count(identity) > 1
        }
    )
    if duplicates:
        raise CapitalReturnResolutionError(
            f"ANF Capital Return filing identities are ambiguous: {duplicates!r}."
        )
    expected = set(ANF_REQUIRED_QUARTERS) - {
        period for period in ANF_REQUIRED_QUARTERS if period.endswith("-Q4")
    }
    expected.update(ANF_REQUIRED_ANNUAL_PERIODS)
    if set(identities) != expected:
        raise CapitalReturnResolutionError(
            "ANF Capital Return source coverage is incomplete; "
            f"expected {sorted(expected)!r}, found {sorted(identities)!r}."
        )
    return filings


def _metric_contract(metric_id: str) -> tuple[str, str, str, str, str]:
    try:
        return _METRIC_CONTRACTS[metric_id]
    except KeyError as exc:
        raise CapitalReturnResolutionError(
            f"No Capital Return metric contract exists for {metric_id!r}."
        ) from exc


def _source_record(
    *,
    filing: _Filing,
    fact: _Fact,
    metric_id: str,
    fiscal_period: str,
    period_type: str,
    period_start: str,
    period_end: str,
    duration_or_instant: str,
    source_section: str,
) -> dict[str, Any]:
    semantic_role, unit, currency, scale, aggregation_role = _metric_contract(metric_id)
    return make_capital_return_record(
        metric_id=metric_id,
        semantic_role=semantic_role,
        fiscal_period=fiscal_period,
        period_type=period_type,
        period_start=period_start,
        period_end=period_end,
        duration_or_instant=duration_or_instant,
        publication_date=filing.publication_date,
        source_document=f"sec_cache/ANF/{filing.path.name}",
        source_document_sha256=filing.sha256,
        source_section=source_section,
        unit=unit,
        currency=currency,
        scale=scale,
        source_classification="source_native_numeric",
        aggregation_role=aggregation_role,
        evidence_ref=fact.evidence_ref,
        value=fact.value,
        source_alias=filing.source_alias,
    )


def _combined_sha(records: Sequence[Mapping[str, Any]]) -> str:
    hashes = sorted({str(record.get("source_document_sha256") or "") for record in records})
    if len(hashes) == 1:
        return hashes[0]
    return hashlib.sha256(
        json.dumps(hashes, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).hexdigest()


def _flatten_joined(
    records: Sequence[Mapping[str, Any]],
    field: str,
) -> list[str]:
    return sorted(
        {
            part.strip()
            for record in records
            for part in str(record.get(field) or "").split(" + ")
            if part.strip()
        }
    )


def _derived_record(
    *,
    metric_id: str,
    fiscal_period: str,
    period_type: str,
    period_start: str,
    period_end: str,
    duration_or_instant: str,
    value: float,
    derivation_identity: str,
    components: Sequence[Mapping[str, Any]],
    source_section: str = "exact deterministic derivation",
) -> dict[str, Any]:
    semantic_role, unit, currency, scale, aggregation_role = _metric_contract(metric_id)
    return make_capital_return_record(
        metric_id=metric_id,
        semantic_role=semantic_role,
        fiscal_period=fiscal_period,
        period_type=period_type,
        period_start=period_start,
        period_end=period_end,
        duration_or_instant=duration_or_instant,
        publication_date=max(str(row.get("publication_date") or "") for row in components),
        source_document=" + ".join(_flatten_joined(components, "source_document")),
        source_document_sha256=_combined_sha(components),
        source_section=source_section,
        unit=unit,
        currency=currency,
        scale=scale,
        source_classification="derived_exact",
        aggregation_role=aggregation_role,
        evidence_ref=" + ".join(_flatten_joined(components, "evidence_ref")),
        value=value,
        derivation_identity=derivation_identity,
        component_record_ids=tuple(str(row.get("record_id") or "") for row in components),
        source_alias="ANF SEC exact derivation",
    )


def _parse_table_number(value: Any) -> float | None:
    if isinstance(value, bool) or pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = _clean_text(value).strip("()$ ").replace(",", "")
    if not text or text in {"-", "\u2014"} or _NUMBER_RE.fullmatch(text) is None:
        return None
    return float(text)


def _unique_table_value(
    table: pd.DataFrame,
    *,
    header_row: int,
    total_row: int,
    header_predicate: Any,
) -> float:
    columns = [
        column
        for column in range(table.shape[1])
        if header_predicate(_clean_text(table.iat[header_row, column]).casefold())
    ]
    values = {
        parsed
        for column in columns
        if (parsed := _parse_table_number(table.iat[total_row, column])) is not None
    }
    if len(values) != 1:
        raise CapitalReturnResolutionError(
            "Issuer-purchases table field did not resolve to one exact value; "
            f"columns={columns!r}, values={sorted(values)!r}."
        )
    return next(iter(values))


def _issuer_purchase_table(filing: _Filing) -> dict[str, Any]:
    tables = pd.read_html(filing.path)
    matches: list[tuple[int, pd.DataFrame, int, int]] = []
    for table_index, table in enumerate(tables):
        header_row = -1
        total_row = -1
        for row_index in range(table.shape[0]):
            row_text = " ".join(
                _clean_text(value)
                for value in table.iloc[row_index].tolist()
                if _clean_text(value) and _clean_text(value).casefold() != "nan"
            )
            if (
                "Total Number of Shares Purchased" in row_text
                and "Publicly Announced" in row_text
                and "Approximate Dollar Value" in row_text
            ):
                header_row = row_index
            first_values = {
                _clean_text(value).casefold()
                for value in table.iloc[row_index, : min(3, table.shape[1])].tolist()
                if _clean_text(value)
            }
            if "total" in first_values:
                total_row = row_index
        if header_row >= 0 and total_row > header_row:
            matches.append((table_index, table, header_row, total_row))
    if len(matches) != 1:
        raise CapitalReturnResolutionError(
            f"ANF filing {filing.path.name!r} must contain one issuer-purchases table; "
            f"found {len(matches)}."
        )
    table_index, table, header_row, total_row = matches[0]
    total_purchases = _unique_table_value(
        table,
        header_row=header_row,
        total_row=total_row,
        header_predicate=lambda text: (
            text.startswith("total number of shares purchased")
            and "publicly announced" not in text
        ),
    )
    average_price = _unique_table_value(
        table,
        header_row=header_row,
        total_row=total_row,
        header_predicate=lambda text: "average price" in text,
    )
    program_purchases = _unique_table_value(
        table,
        header_row=header_row,
        total_row=total_row,
        header_predicate=lambda text: (
            "total number of shares purchased" in text
            and "publicly announced" in text
        ),
    )
    authorization_remaining = _unique_table_value(
        table,
        header_row=header_row,
        total_row=total_row,
        header_predicate=lambda text: "approximate dollar value" in text,
    )
    if total_purchases < program_purchases:
        raise CapitalReturnResolutionError(
            f"Issuer-purchases table in {filing.path.name!r} has overlapping identities."
        )
    fragment = f"issuer-purchases-table[{table_index}]:total-row[{total_row}]"
    return {
        "table_index": table_index,
        "total_row": total_row,
        "total_issuer_purchases": total_purchases / 1_000_000,
        "public_program_shares_repurchased": program_purchases / 1_000_000,
        "employee_tax_withholding_shares": (
            total_purchases - program_purchases
        )
        / 1_000_000,
        "reported_average_all_purchases": average_price,
        "authorization_remaining": authorization_remaining / 1_000_000,
        "evidence_ref": _source_ref(filing.path, fragment),
    }


def _current_quarter_dates(filing: _Filing) -> tuple[str, str]:
    candidates = {
        (context.start, context.end)
        for context in filing.contexts.values()
        if context.end == filing.period_end
        and context.start
        and not context.members
    }
    if not candidates:
        raise CapitalReturnResolutionError(
            f"ANF filing {filing.path.name!r} has no current quarter context."
        )
    start, end = max(candidates, key=lambda item: item[0])
    return start, end


def _current_duration_record(
    filing: _Filing,
    *,
    metric_id: str,
    fiscal_period: str,
    period_type: str,
    period_start: str,
    period_end: str,
    exact_quarter: bool,
) -> dict[str, Any]:
    concept = _CONCEPTS[metric_id]
    _, unit, _, _, _ = _metric_contract(metric_id)
    member = (
        "us-gaap:CommonStockMember"
        if metric_id
        in {"accounting_program_shares_repurchased", "share_issuance_sbc"}
        else None
    )
    fact = _select_fact(
        filing,
        concept,
        start=period_start,
        end=period_end,
        member=member,
        dimensionless=member is None,
        expected_unit=unit,
    )
    section = (
        "statement of shareholders' equity"
        if member is not None
        else "inline XBRL financial statements"
    )
    if not exact_quarter:
        section += " (year-to-date)"
    return _source_record(
        filing=filing,
        fact=fact,
        metric_id=metric_id,
        fiscal_period=fiscal_period,
        period_type=period_type,
        period_start=period_start,
        period_end=period_end,
        duration_or_instant="duration",
        source_section=section,
    )


def _point_share_record(
    filing: _Filing,
    *,
    metric_id: str,
    fiscal_period: str,
    period_type: str,
    instant: str,
) -> dict[str, Any]:
    fact = _select_fact(
        filing,
        _CONCEPTS["shares_outstanding"],
        instant=instant,
        member="us-gaap:CommonStockMember",
        expected_unit="m shares",
    )
    return _source_record(
        filing=filing,
        fact=fact,
        metric_id=metric_id,
        fiscal_period=fiscal_period,
        period_type=period_type,
        period_start=instant,
        period_end=instant,
        duration_or_instant="instant",
        source_section="statement of shareholders' equity",
    )


def _issuer_table_records(
    filing: _Filing,
    *,
    fiscal_period: str,
    period_type: str,
    period_start: str,
    period_end: str,
) -> list[dict[str, Any]]:
    table = _issuer_purchase_table(filing)
    records: list[dict[str, Any]] = []
    for metric_id in (
        "public_program_shares_repurchased",
        "total_issuer_purchases",
        "reported_average_all_purchases",
        "authorization_remaining",
    ):
        semantic_role, unit, currency, scale, aggregation_role = _metric_contract(metric_id)
        duration_or_instant = (
            "instant" if metric_id == "authorization_remaining" else "duration"
        )
        records.append(
            make_capital_return_record(
                metric_id=metric_id,
                semantic_role=semantic_role,
                fiscal_period=fiscal_period,
                period_type=period_type,
                period_start=period_start,
                period_end=period_end,
                duration_or_instant=duration_or_instant,
                publication_date=filing.publication_date,
                source_document=f"sec_cache/ANF/{filing.path.name}",
                source_document_sha256=filing.sha256,
                source_section=(
                    "issuer purchases table; terminal snapshot"
                    if metric_id == "authorization_remaining"
                    else "issuer purchases table"
                ),
                unit=unit,
                currency=currency,
                scale=scale,
                source_classification="source_native_numeric",
                aggregation_role=aggregation_role,
                evidence_ref=str(table["evidence_ref"]),
                value=float(table[metric_id]),
                derivation_identity=(
                    "terminal point-in-time value from issuer-purchases table"
                    if metric_id == "authorization_remaining"
                    else ""
                ),
                source_alias=filing.source_alias,
            )
        )
    total_record = next(
        row for row in records if row["metric_id"] == "total_issuer_purchases"
    )
    program_record = next(
        row
        for row in records
        if row["metric_id"] == "public_program_shares_repurchased"
    )
    records.append(
        _derived_record(
            metric_id="employee_tax_withholding_shares",
            fiscal_period=fiscal_period,
            period_type=period_type,
            period_start=period_start,
            period_end=period_end,
            duration_or_instant="duration",
            value=float(total_record["value"]) - float(program_record["value"]),
            derivation_identity=(
                "total_issuer_purchases - public_program_shares_repurchased"
            ),
            components=(total_record, program_record),
        )
    )
    return records


def _record_map(records: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for record in records:
        metric_id = str(record.get("metric_id") or "")
        if metric_id in result:
            raise CapitalReturnResolutionError(
                f"Duplicate metric {metric_id!r} in one Capital Return period."
            )
        result[metric_id] = dict(record)
    return result


def _selected_point_record(
    *,
    metric_id: str,
    fiscal_period: str,
    period_type: str,
    instant: str,
    source_record: Mapping[str, Any],
) -> dict[str, Any]:
    semantic_role, unit, currency, scale, aggregation_role = _metric_contract(metric_id)
    return make_capital_return_record(
        metric_id=metric_id,
        semantic_role=semantic_role,
        fiscal_period=fiscal_period,
        period_type=period_type,
        period_start=instant,
        period_end=instant,
        duration_or_instant="instant",
        publication_date=str(source_record.get("publication_date") or ""),
        source_document=str(source_record.get("source_document") or ""),
        source_document_sha256=str(source_record.get("source_document_sha256") or ""),
        source_section="exact point-in-time selector",
        unit=unit,
        currency=currency,
        scale=scale,
        source_classification="derived_exact",
        aggregation_role=aggregation_role,
        evidence_ref=str(source_record.get("evidence_ref") or ""),
        value=float(source_record["value"]),
        derivation_identity=f"select {source_record.get('record_id')} as {metric_id}",
        component_record_ids=(str(source_record.get("record_id") or ""),),
        source_alias=str(source_record.get("source_alias") or ""),
    )


def _append_core_derivations(records: list[dict[str, Any]]) -> None:
    by_metric = _record_map(records)
    records.append(
        derive_cash_per_program_share(
            by_metric["repurchase_cash_program"],
            by_metric["accounting_program_shares_repurchased"],
        )
    )
    records.append(
        derive_net_share_reduction(
            by_metric["accounting_program_shares_repurchased"],
            by_metric["share_issuance_sbc"],
            beginning_shares_record=by_metric["beginning_period_end_shares"],
            ending_shares_record=by_metric["ending_period_end_shares"],
            tolerance=0.003,
        )
    )
    records.append(
        _derived_record(
            metric_id="free_cash_flow",
            fiscal_period=str(by_metric["operating_cash_flow"]["fiscal_period"]),
            period_type=str(by_metric["operating_cash_flow"]["period_type"]),
            period_start=str(by_metric["operating_cash_flow"]["period_start"]),
            period_end=str(by_metric["operating_cash_flow"]["period_end"]),
            duration_or_instant="duration",
            value=(
                float(by_metric["operating_cash_flow"]["value"])
                - float(by_metric["capital_expenditures"]["value"])
            ),
            derivation_identity="operating_cash_flow - capital_expenditures",
            components=(
                by_metric["operating_cash_flow"],
                by_metric["capital_expenditures"],
            ),
        )
    )


def _unavailable_record(
    *,
    filing: _Filing,
    metric_id: str,
    semantic_role: str,
    fiscal_period: str,
    period_type: str,
    period_start: str,
    period_end: str,
    unit: str,
    currency: str,
    scale: str,
    aggregation_role: str,
    reason: str,
) -> dict[str, Any]:
    return make_unavailable_record(
        metric_id=metric_id,
        semantic_role=semantic_role,
        fiscal_period=fiscal_period,
        period_type=period_type,
        period_start=period_start,
        period_end=period_end,
        duration_or_instant="duration",
        publication_date=filing.publication_date,
        source_document=f"sec_cache/ANF/{filing.path.name}",
        source_document_sha256=filing.sha256,
        source_section="bounded SEC Capital Return evidence review",
        unit=unit,
        currency=currency,
        scale=scale,
        aggregation_role=aggregation_role,
        evidence_ref=_source_ref(
            filing.path,
            f"capital-return-unavailable:{metric_id}:{fiscal_period}",
        ),
        reason=reason,
        source_alias=filing.source_alias,
    )


def _append_dividend_and_coverage_states(
    records: list[dict[str, Any]],
    *,
    filing: _Filing,
) -> None:
    by_metric = _record_map(records)
    sample = by_metric["repurchase_cash_program"]
    period = str(sample["fiscal_period"])
    period_type = str(sample["period_type"])
    start = str(sample["period_start"])
    end = str(sample["period_end"])
    dividend_reason = (
        "No accepted source-native paid-dividend fact establishes a numeric amount "
        "for this exact period; missing is not zero."
    )
    for metric_id, semantic_role, unit, currency, scale, aggregation_role in (
        (
            "dividends_declared",
            "cash_dividends_declared",
            "$m",
            "USD",
            "millions",
            "additive_flow",
        ),
        (
            "dividends_paid",
            "cash_dividends_paid",
            "$m",
            "USD",
            "millions",
            "additive_flow",
        ),
        (
            "ordinary_dividend_per_share",
            "ordinary_cash_dividend_per_share",
            "$/share",
            "USD",
            "per_share",
            "non_additive_ratio",
        ),
        (
            "special_dividend_per_share",
            "special_cash_dividend_per_share",
            "$/share",
            "USD",
            "per_share",
            "non_additive_ratio",
        ),
        (
            "dividend_payout_ratio",
            "paid_dividend_earnings_coverage",
            "%",
            "not_applicable",
            "ratio",
            "non_additive_ratio",
        ),
    ):
        records.append(
            _unavailable_record(
                filing=filing,
                metric_id=metric_id,
                semantic_role=semantic_role,
                fiscal_period=period,
                period_type=period_type,
                period_start=start,
                period_end=end,
                unit=unit,
                currency=currency,
                scale=scale,
                aggregation_role=aggregation_role,
                reason=dividend_reason,
            )
        )
    records.append(
        _unavailable_record(
            filing=filing,
            metric_id="historical_buyback_eps_attribution",
            semantic_role="historical_eps_attribution",
            fiscal_period=period,
            period_type=period_type,
            period_start=start,
            period_end=end,
            unit="$/share",
            currency="USD",
            scale="per_share",
            aggregation_role="derived_relationship",
            reason=(
                "Exact historical EPS attribution to buybacks is unavailable because "
                "timing, earnings generation, and treasury-stock-method effects are not "
                "isolated."
            ),
        )
    )
    fcf = _record_map(records)["free_cash_flow"]
    try:
        records.append(
            derive_fcf_coverage(
                by_metric["repurchase_cash_program"],
                fcf,
                metric_id="buybacks_to_fcf",
            )
        )
    except CapitalReturnResolutionError as exc:
        records.append(
            _unavailable_record(
                filing=filing,
                metric_id="buybacks_to_fcf",
                semantic_role="capital_return_fcf_coverage",
                fiscal_period=period,
                period_type=period_type,
                period_start=start,
                period_end=end,
                unit="%",
                currency="not_applicable",
                scale="ratio",
                aggregation_role="non_additive_ratio",
                reason=str(exc),
            )
        )
    for metric_id in (
        "total_capital_return",
        "dividends_to_fcf",
        "total_capital_return_to_fcf",
    ):
        records.append(
            _unavailable_record(
                filing=filing,
                metric_id=metric_id,
                semantic_role=(
                    "cash_returned_to_shareholders"
                    if metric_id == "total_capital_return"
                    else "capital_return_fcf_coverage"
                ),
                fiscal_period=period,
                period_type=period_type,
                period_start=start,
                period_end=end,
                unit="$m" if metric_id == "total_capital_return" else "%",
                currency="USD" if metric_id == "total_capital_return" else "not_applicable",
                scale="millions" if metric_id == "total_capital_return" else "ratio",
                aggregation_role=(
                    "additive_flow"
                    if metric_id == "total_capital_return"
                    else "non_additive_ratio"
                ),
                reason=(
                    "Compatible total capital return cannot be calculated while paid "
                    "dividends are unavailable."
                ),
            )
        )


def _build_ytd_records(filing: _Filing) -> list[dict[str, Any]]:
    if filing.form != "10-Q":
        raise CapitalReturnResolutionError("YTD records require a 10-Q filing.")
    fiscal_period = f"{filing.fiscal_period_id}-YTD"
    records = [
        _current_duration_record(
            filing,
            metric_id=metric_id,
            fiscal_period=fiscal_period,
            period_type="year_to_date",
            period_start=filing.period_start,
            period_end=filing.period_end,
            exact_quarter=filing.fiscal_period == "Q1",
        )
        for metric_id in (
            "repurchase_cash_program",
            "treasury_stock_accounting_cost",
            "employee_tax_withholding_cash",
            "accounting_program_shares_repurchased",
            "share_issuance_sbc",
            "basic_weighted_average_shares",
            "diluted_weighted_average_shares",
            "incremental_dilutive_shares",
            "operating_cash_flow",
            "capital_expenditures",
        )
    ]
    records.extend(
        (
            _point_share_record(
                filing,
                metric_id="beginning_period_end_shares",
                fiscal_period=fiscal_period,
                period_type="year_to_date",
                instant=(
                    datetime.fromisoformat(filing.period_start).date()
                    - timedelta(days=1)
                ).isoformat(),
            ),
            _point_share_record(
                filing,
                metric_id="ending_period_end_shares",
                fiscal_period=fiscal_period,
                period_type="year_to_date",
                instant=filing.period_end,
            ),
        )
    )
    _append_core_derivations(records)
    return records


def _build_annual_records(filing: _Filing) -> list[dict[str, Any]]:
    if filing.form != "10-K":
        raise CapitalReturnResolutionError("Annual records require a 10-K filing.")
    records = [
        _current_duration_record(
            filing,
            metric_id=metric_id,
            fiscal_period=filing.fiscal_period_id,
            period_type="annual",
            period_start=filing.period_start,
            period_end=filing.period_end,
            exact_quarter=False,
        )
        for metric_id in (
            "repurchase_cash_program",
            "treasury_stock_accounting_cost",
            "employee_tax_withholding_cash",
            "accounting_program_shares_repurchased",
            "share_issuance_sbc",
            "basic_weighted_average_shares",
            "diluted_weighted_average_shares",
            "incremental_dilutive_shares",
            "operating_cash_flow",
            "capital_expenditures",
        )
    ]
    beginning_instant = (
        datetime.fromisoformat(filing.period_start).date() - timedelta(days=1)
    ).isoformat()
    records.extend(
        (
            _point_share_record(
                filing,
                metric_id="beginning_period_end_shares",
                fiscal_period=filing.fiscal_period_id,
                period_type="annual",
                instant=beginning_instant,
            ),
            _point_share_record(
                filing,
                metric_id="ending_period_end_shares",
                fiscal_period=filing.fiscal_period_id,
                period_type="annual",
                instant=filing.period_end,
            ),
        )
    )
    _append_core_derivations(records)
    return records


def _difference_record(
    *,
    metric_id: str,
    fiscal_period: str,
    period_type: str,
    period_start: str,
    period_end: str,
    minuend: Mapping[str, Any],
    subtrahend: Mapping[str, Any],
    derivation_identity: str,
) -> dict[str, Any]:
    return _derived_record(
        metric_id=metric_id,
        fiscal_period=fiscal_period,
        period_type=period_type,
        period_start=period_start,
        period_end=period_end,
        duration_or_instant="duration",
        value=float(minuend["value"]) - float(subtrahend["value"]),
        derivation_identity=derivation_identity,
        components=(minuend, subtrahend),
    )


def _build_quarter_records(
    *,
    filing: _Filing,
    current_ytd: Mapping[str, Mapping[str, Any]] | None,
    prior_ytd: Mapping[str, Mapping[str, Any]] | None,
    annual: Mapping[str, Mapping[str, Any]] | None,
    prior_quarter: Mapping[str, Mapping[str, Any]] | None,
) -> list[dict[str, Any]]:
    if filing.fiscal_period not in {"Q1", "Q2", "Q3", "FY"}:
        raise CapitalReturnResolutionError("Unsupported quarter construction period.")
    fiscal_period = (
        f"{filing.fiscal_year}-Q4"
        if filing.fiscal_period == "FY"
        else filing.fiscal_period_id
    )
    if filing.fiscal_period == "FY":
        if annual is None or prior_ytd is None or prior_quarter is None:
            raise CapitalReturnResolutionError(
                f"{fiscal_period} requires annual, Q3 YTD, and prior-quarter evidence."
            )
        period_start = (
            datetime.fromisoformat(str(prior_quarter["ending_period_end_shares"]["period_end"]))
            .date()
            + timedelta(days=1)
        ).isoformat()
        period_end = filing.period_end
        records = [
            _difference_record(
                metric_id=metric_id,
                fiscal_period=fiscal_period,
                period_type="quarter",
                period_start=period_start,
                period_end=period_end,
                minuend=annual[metric_id],
                subtrahend=prior_ytd[metric_id],
                derivation_identity=f"annual {filing.fiscal_year} - Q3 year-to-date",
            )
            for metric_id in (
                "repurchase_cash_program",
                "treasury_stock_accounting_cost",
                "employee_tax_withholding_cash",
                "accounting_program_shares_repurchased",
                "share_issuance_sbc",
                "operating_cash_flow",
                "capital_expenditures",
            )
        ]
        for metric_id in (
            "basic_weighted_average_shares",
            "diluted_weighted_average_shares",
            "incremental_dilutive_shares",
        ):
            records.append(
                _derived_record(
                    metric_id=metric_id,
                    fiscal_period=fiscal_period,
                    period_type="quarter",
                    period_start=period_start,
                    period_end=period_end,
                    duration_or_instant="duration",
                    value=(
                        4 * float(annual[metric_id]["value"])
                        - 3 * float(prior_ytd[metric_id]["value"])
                    ),
                    derivation_identity=(
                        "4 * annual weighted average - 3 * Q3 YTD weighted average; "
                        "four equal 13-week quarters"
                    ),
                    components=(annual[metric_id], prior_ytd[metric_id]),
                )
            )
    else:
        period_start, period_end = _current_quarter_dates(filing)
        if current_ytd is None:
            raise CapitalReturnResolutionError(
                f"{fiscal_period} has no source-native YTD record set."
            )
        direct_metrics = (
            "treasury_stock_accounting_cost",
            "employee_tax_withholding_cash",
            "accounting_program_shares_repurchased",
            "share_issuance_sbc",
            "basic_weighted_average_shares",
            "diluted_weighted_average_shares",
            "incremental_dilutive_shares",
        )
        records = [
            _current_duration_record(
                filing,
                metric_id=metric_id,
                fiscal_period=fiscal_period,
                period_type="quarter",
                period_start=period_start,
                period_end=period_end,
                exact_quarter=True,
            )
            for metric_id in direct_metrics
        ]
        if filing.fiscal_period == "Q1":
            records.extend(
                _current_duration_record(
                    filing,
                    metric_id=metric_id,
                    fiscal_period=fiscal_period,
                    period_type="quarter",
                    period_start=period_start,
                    period_end=period_end,
                    exact_quarter=True,
                )
                for metric_id in (
                    "repurchase_cash_program",
                    "operating_cash_flow",
                    "capital_expenditures",
                )
            )
        else:
            if prior_ytd is None:
                raise CapitalReturnResolutionError(
                    f"{fiscal_period} requires the preceding YTD record set."
                )
            records.extend(
                _difference_record(
                    metric_id=metric_id,
                    fiscal_period=fiscal_period,
                    period_type="quarter",
                    period_start=period_start,
                    period_end=period_end,
                    minuend=current_ytd[metric_id],
                    subtrahend=prior_ytd[metric_id],
                    derivation_identity=(
                        f"{filing.fiscal_period} YTD - prior-quarter YTD"
                    ),
                )
                for metric_id in (
                    "repurchase_cash_program",
                    "operating_cash_flow",
                    "capital_expenditures",
                )
            )
    if prior_quarter is None:
        beginning = _point_share_record(
            filing,
            metric_id="beginning_period_end_shares",
            fiscal_period=fiscal_period,
            period_type="quarter",
            instant=(
                datetime.fromisoformat(period_start).date() - timedelta(days=1)
            ).isoformat(),
        )
    else:
        prior_end = prior_quarter["ending_period_end_shares"]
        beginning = _selected_point_record(
            metric_id="beginning_period_end_shares",
            fiscal_period=fiscal_period,
            period_type="quarter",
            instant=str(prior_end["period_end"]),
            source_record=prior_end,
        )
    ending = _point_share_record(
        filing,
        metric_id="ending_period_end_shares",
        fiscal_period=fiscal_period,
        period_type="quarter",
        instant=period_end,
    )
    records.extend((beginning, ending))
    records.extend(
        _issuer_table_records(
            filing,
            fiscal_period=fiscal_period,
            period_type="quarter",
            period_start=period_start,
            period_end=period_end,
        )
    )
    _append_core_derivations(records)
    _append_dividend_and_coverage_states(records, filing=filing)
    return records


def _sum_records(
    *,
    metric_id: str,
    fiscal_period: str,
    period_type: str,
    period_start: str,
    period_end: str,
    records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return _derived_record(
        metric_id=metric_id,
        fiscal_period=fiscal_period,
        period_type=period_type,
        period_start=period_start,
        period_end=period_end,
        duration_or_instant="duration",
        value=sum(float(record["value"]) for record in records),
        derivation_identity=(
            " + ".join(str(record.get("fiscal_period") or "") for record in records)
        ),
        components=records,
    )


def _average_records(
    *,
    metric_id: str,
    fiscal_period: str,
    period_type: str,
    period_start: str,
    period_end: str,
    records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return _derived_record(
        metric_id=metric_id,
        fiscal_period=fiscal_period,
        period_type=period_type,
        period_start=period_start,
        period_end=period_end,
        duration_or_instant="duration",
        value=sum(float(record["value"]) for record in records) / len(records),
        derivation_identity="equal-weight average of four consecutive 13-week quarters",
        components=records,
    )


def _enrich_annual_records(
    annual_records: list[dict[str, Any]],
    *,
    quarter_records: Sequence[Mapping[str, Mapping[str, Any]]],
    filing: _Filing,
) -> None:
    annual_map = _record_map(annual_records)
    period = str(annual_map["repurchase_cash_program"]["fiscal_period"])
    start = str(annual_map["repurchase_cash_program"]["period_start"])
    end = str(annual_map["repurchase_cash_program"]["period_end"])
    for metric_id in (
        "public_program_shares_repurchased",
        "total_issuer_purchases",
        "employee_tax_withholding_shares",
    ):
        annual_records.append(
            _sum_records(
                metric_id=metric_id,
                fiscal_period=period,
                period_type="annual",
                period_start=start,
                period_end=end,
                records=tuple(row[metric_id] for row in quarter_records),
            )
        )
    terminal_authorization = quarter_records[-1]["authorization_remaining"]
    annual_records.append(
        make_capital_return_record(
            metric_id="authorization_remaining",
            semantic_role="repurchase_authorization_remaining",
            fiscal_period=period,
            period_type="annual",
            period_start=start,
            period_end=end,
            duration_or_instant="instant",
            publication_date=str(terminal_authorization["publication_date"]),
            source_document=str(terminal_authorization["source_document"]),
            source_document_sha256=str(
                terminal_authorization["source_document_sha256"]
            ),
            source_section="terminal quarter point-in-time selector",
            unit="$m",
            currency="USD",
            scale="millions",
            source_classification="source_native_numeric",
            aggregation_role="point_in_time",
            evidence_ref=str(terminal_authorization["evidence_ref"]),
            value=float(terminal_authorization["value"]),
            derivation_identity="terminal quarter; authorization is not summed",
            component_record_ids=(str(terminal_authorization["record_id"]),),
            source_alias=str(terminal_authorization["source_alias"]),
        )
    )
    annual_records.append(
        _unavailable_record(
            filing=filing,
            metric_id="reported_average_all_purchases",
            semantic_role="issuer_table_average_all_purchases",
            fiscal_period=period,
            period_type="annual",
            period_start=start,
            period_end=end,
            unit="$/share",
            currency="USD",
            scale="per_share",
            aggregation_role="non_additive_ratio",
            reason=(
                "Quarterly filing averages are rounded and cannot be aggregated into "
                "an exact annual all-purchases average."
            ),
        )
    )
    _append_dividend_and_coverage_states(annual_records, filing=filing)


def _build_ttm_records(
    quarter_records: Sequence[Mapping[str, Mapping[str, Any]]],
    *,
    filing: _Filing,
) -> list[dict[str, Any]]:
    actual_periods = tuple(
        str(records["repurchase_cash_program"]["fiscal_period"])
        for records in quarter_records
    )
    if actual_periods != ANF_REQUIRED_TTM_COMPONENTS:
        raise CapitalReturnResolutionError(
            f"Exact TTM requires {ANF_REQUIRED_TTM_COMPONENTS!r}; found {actual_periods!r}."
        )
    starts = [str(records["repurchase_cash_program"]["period_start"]) for records in quarter_records]
    ends = [str(records["repurchase_cash_program"]["period_end"]) for records in quarter_records]
    for index in range(1, len(starts)):
        prior_end = datetime.fromisoformat(ends[index - 1]).date()
        current_start = datetime.fromisoformat(starts[index]).date()
        if prior_end + timedelta(days=1) != current_start:
            raise CapitalReturnResolutionError(
                "Exact TTM quarter components are not consecutive."
            )
    lengths = {
        (datetime.fromisoformat(end).date() - datetime.fromisoformat(start).date()).days
        + 1
        for start, end in zip(starts, ends)
    }
    if lengths != {91}:
        raise CapitalReturnResolutionError(
            f"Weighted-average TTM requires four equal 13-week quarters; found {lengths!r}."
        )
    fiscal_period = f"TTM through {actual_periods[-1]}"
    period_start = starts[0]
    period_end = ends[-1]
    records: list[dict[str, Any]] = []
    for metric_id in (
        "repurchase_cash_program",
        "treasury_stock_accounting_cost",
        "employee_tax_withholding_cash",
        "accounting_program_shares_repurchased",
        "public_program_shares_repurchased",
        "total_issuer_purchases",
        "employee_tax_withholding_shares",
        "share_issuance_sbc",
        "operating_cash_flow",
        "capital_expenditures",
        "free_cash_flow",
    ):
        records.append(
            _sum_records(
                metric_id=metric_id,
                fiscal_period=fiscal_period,
                period_type="ttm",
                period_start=period_start,
                period_end=period_end,
                records=tuple(row[metric_id] for row in quarter_records),
            )
        )
    for metric_id in (
        "basic_weighted_average_shares",
        "diluted_weighted_average_shares",
        "incremental_dilutive_shares",
    ):
        records.append(
            _average_records(
                metric_id=metric_id,
                fiscal_period=fiscal_period,
                period_type="ttm",
                period_start=period_start,
                period_end=period_end,
                records=tuple(row[metric_id] for row in quarter_records),
            )
        )
    records.extend(
        (
            _selected_point_record(
                metric_id="beginning_period_end_shares",
                fiscal_period=fiscal_period,
                period_type="ttm",
                instant=str(quarter_records[0]["beginning_period_end_shares"]["period_end"]),
                source_record=quarter_records[0]["beginning_period_end_shares"],
            ),
            _selected_point_record(
                metric_id="ending_period_end_shares",
                fiscal_period=fiscal_period,
                period_type="ttm",
                instant=str(quarter_records[-1]["ending_period_end_shares"]["period_end"]),
                source_record=quarter_records[-1]["ending_period_end_shares"],
            ),
        )
    )
    terminal_authorization = quarter_records[-1]["authorization_remaining"]
    records.append(
        make_capital_return_record(
            metric_id="authorization_remaining",
            semantic_role="repurchase_authorization_remaining",
            fiscal_period=fiscal_period,
            period_type="ttm",
            period_start=period_start,
            period_end=period_end,
            duration_or_instant="instant",
            publication_date=str(terminal_authorization["publication_date"]),
            source_document=str(terminal_authorization["source_document"]),
            source_document_sha256=str(
                terminal_authorization["source_document_sha256"]
            ),
            source_section="terminal TTM point-in-time selector",
            unit="$m",
            currency="USD",
            scale="millions",
            source_classification="source_native_numeric",
            aggregation_role="point_in_time",
            evidence_ref=str(terminal_authorization["evidence_ref"]),
            value=float(terminal_authorization["value"]),
            derivation_identity="terminal quarter; authorization is not summed",
            component_record_ids=(str(terminal_authorization["record_id"]),),
            source_alias=str(terminal_authorization["source_alias"]),
        )
    )
    records.append(
        _unavailable_record(
            filing=filing,
            metric_id="reported_average_all_purchases",
            semantic_role="issuer_table_average_all_purchases",
            fiscal_period=fiscal_period,
            period_type="ttm",
            period_start=period_start,
            period_end=period_end,
            unit="$/share",
            currency="USD",
            scale="per_share",
            aggregation_role="non_additive_ratio",
            reason=(
                "Rounded quarterly all-purchases averages cannot form an exact TTM average."
            ),
        )
    )
    by_metric = _record_map(records)
    records.append(
        derive_cash_per_program_share(
            by_metric["repurchase_cash_program"],
            by_metric["accounting_program_shares_repurchased"],
        )
    )
    records.append(
        derive_net_share_reduction(
            by_metric["accounting_program_shares_repurchased"],
            by_metric["share_issuance_sbc"],
            beginning_shares_record=by_metric["beginning_period_end_shares"],
            ending_shares_record=by_metric["ending_period_end_shares"],
            tolerance=0.003,
        )
    )
    _append_dividend_and_coverage_states(records, filing=filing)
    return records


def _authorization_records(filing: _Filing) -> list[dict[str, Any]]:
    text = _clean_text(filing.soup.get_text(" "))
    match = re.search(
        r"On\s+(?P<date>March\s+5,\s+2025),\s+the Company announced that the "
        r"Board of Directors approved a new \$(?P<amount>[0-9.]+)\s+billion "
        r"share repurchase program.*?does not have an expiration date",
        text,
        flags=re.I,
    )
    if match is None:
        raise CapitalReturnResolutionError(
            "The current ANF authorization identity did not resolve from the local 10-K."
        )
    approval_date = _parse_date_text(match.group("date"))
    amount = float(match.group("amount")) * 1_000
    evidence_ref = _source_ref(
        filing.path,
        "issuer-purchases-table:authorization-footnote",
    )
    numeric = make_capital_return_record(
        metric_id="authorization_total",
        semantic_role="approved_repurchase_authorization",
        fiscal_period=f"authorization-{approval_date}",
        period_type="point_in_time",
        period_start=approval_date,
        period_end=approval_date,
        duration_or_instant="instant",
        publication_date=filing.publication_date,
        source_document=f"sec_cache/ANF/{filing.path.name}",
        source_document_sha256=filing.sha256,
        source_section="issuer-purchases table authorization footnote",
        unit="$m",
        currency="USD",
        scale="millions",
        source_classification="source_native_numeric",
        aggregation_role="point_in_time",
        evidence_ref=evidence_ref,
        value=amount,
        source_alias=filing.source_alias,
    )
    text_records = [
        make_capital_return_record(
            metric_id=metric_id,
            semantic_role=semantic_role,
            fiscal_period=f"authorization-{approval_date}",
            period_type="point_in_time",
            period_start=approval_date,
            period_end=approval_date,
            duration_or_instant="instant",
            publication_date=filing.publication_date,
            source_document=f"sec_cache/ANF/{filing.path.name}",
            source_document_sha256=filing.sha256,
            source_section="issuer-purchases table authorization footnote",
            unit=unit,
            currency="not_applicable",
            scale="not_applicable",
            source_classification="source_native_text",
            aggregation_role="text_state",
            evidence_ref=evidence_ref,
            text_value=text_value,
            source_alias=filing.source_alias,
        )
        for metric_id, semantic_role, unit, text_value in (
            (
                "authorization_approval_date",
                "repurchase_authorization_approval_date",
                "date",
                approval_date,
            ),
            (
                "authorization_expiration_state",
                "repurchase_authorization_expiration_state",
                "text",
                "No expiration date",
            ),
        )
    ]
    return [numeric, *text_records]


def _release_identity(
    path: Path,
    *,
    sec_cache_root: Path,
    publication_dates: Mapping[str, str],
) -> dict[str, Any]:
    match = _RELEASE_RE.fullmatch(path.name)
    if match is None:
        raise CapitalReturnResolutionError(
            f"Unsupported Capital Return release filename {path.name!r}."
        )
    accession_digits = match.group("accession")
    accession = _canonical_accession(accession_digits)
    raw = path.read_bytes()
    return {
        "path": path,
        "accession": accession,
        "publication_date": (
            publication_dates.get(accession)
            or _index_publication_date(sec_cache_root, accession_digits)
        ),
        "sha256": _sha256_bytes(raw),
        "size": len(raw),
        "source_alias": f"ANF earnings release {publication_dates.get(accession) or ''}".strip(),
    }


def _guidance_period_from_header(header: str, fiscal_year: int) -> str:
    normalized = header.casefold()
    if "previous" in normalized:
        return ""
    if "first quarter outlook" in normalized:
        return f"{fiscal_year}-Q1"
    if "second quarter outlook" in normalized:
        return f"{fiscal_year}-Q2"
    if "full year outlook" in normalized:
        return f"{fiscal_year}-FY"
    return ""


def _guidance_value(value: str, *, unit: str) -> dict[str, Any]:
    normalized = _clean_text(value)
    numbers = [float(number) for number in _NUMBER_RE.findall(normalized.replace(",", ""))]
    lower = normalized.casefold()
    if "at least" in lower and len(numbers) == 1:
        return {
            "numeric_state": "minimum",
            "low": numbers[0],
            "high": None,
            "point": None,
            "numeric_usable": True,
        }
    if ("around" in lower or "~" in normalized) and len(numbers) == 1:
        return {
            "numeric_state": "approximate_point",
            "low": None,
            "high": None,
            "point": numbers[0],
            "numeric_usable": True,
        }
    if "range" in lower and len(numbers) == 2:
        return {
            "numeric_state": "range",
            "low": numbers[0],
            "high": numbers[1],
            "point": None,
            "numeric_usable": True,
        }
    raise CapitalReturnResolutionError(
        f"Unsupported typed Capital Return guidance value {value!r} for {unit!r}."
    )


def _release_guidance_records(
    release: Mapping[str, Any],
) -> list[dict[str, Any]]:
    path = Path(release["path"])
    tables = pd.read_html(path)
    candidates: list[tuple[int, pd.DataFrame]] = []
    for table_index, table in enumerate(tables):
        text = " ".join(_clean_text(value) for value in table.to_numpy().ravel())
        if "Share repurchases" in text and "Diluted weighted average shares" in text:
            candidates.append((table_index, table))
    if len(candidates) != 1:
        raise CapitalReturnResolutionError(
            f"Release {path.name!r} must contain one Capital Return guidance table."
        )
    table_index, table = candidates[0]
    full_text = " ".join(_clean_text(value) for value in table.to_numpy().ravel())
    fiscal_year_matches = {
        int(value)
        for value in re.findall(r"fiscal\s+([0-9]{4})", full_text, flags=re.I)
    }
    if len(fiscal_year_matches) != 1:
        raise CapitalReturnResolutionError(
            f"Release {path.name!r} has ambiguous guidance fiscal-year identity."
        )
    fiscal_year = next(iter(fiscal_year_matches))
    headers: list[tuple[int, tuple[int, ...], str]] = []
    for row_index in range(table.shape[0]):
        by_header: dict[str, list[int]] = {}
        for column in range(table.shape[1]):
            text = _clean_text(table.iat[row_index, column])
            period = _guidance_period_from_header(text, fiscal_year)
            if period:
                by_header.setdefault(text, []).append(column)
        for header, columns in by_header.items():
            headers.append((row_index, tuple(columns), _guidance_period_from_header(header, fiscal_year)))
    if not headers:
        raise CapitalReturnResolutionError(
            f"Release {path.name!r} exposes no typed Capital Return guidance headers."
        )
    records: list[dict[str, Any]] = []
    for row_index in range(table.shape[0]):
        labels = {
            _clean_text(table.iat[row_index, column])
            for column in range(min(3, table.shape[1]))
            if _clean_text(table.iat[row_index, column])
            and _clean_text(table.iat[row_index, column]).casefold() != "nan"
        }
        metric_id = ""
        unit = ""
        if any("share repurchases" in label.casefold() for label in labels):
            metric_id = "repurchase_cash_program"
            unit = "$m"
        elif any("diluted weighted average shares" in label.casefold() for label in labels):
            metric_id = "diluted_weighted_average_shares"
            unit = "m shares"
        if not metric_id:
            continue
        applicable_headers: dict[tuple[int, ...], tuple[int, tuple[int, ...], str]] = {}
        for header in headers:
            if header[0] < row_index:
                previous = applicable_headers.get(header[1])
                if previous is None or header[0] > previous[0]:
                    applicable_headers[header[1]] = header
        for _, columns, applicable_period in sorted(
            applicable_headers.values(),
            key=lambda row: (row[2], row[1]),
        ):
            values = {
                _clean_text(table.iat[row_index, column])
                for column in columns
                if _clean_text(table.iat[row_index, column])
                and _clean_text(table.iat[row_index, column]).casefold() != "nan"
            }
            if not values:
                continue
            if len(values) != 1:
                raise CapitalReturnResolutionError(
                    f"Release guidance row {row_index} has conflicting values: "
                    f"{sorted(values)!r}."
                )
            company_wording = next(iter(values))
            typed = _guidance_value(company_wording, unit=unit)
            guidance_id = (
                f"capital_return_guidance_{metric_id}_{applicable_period}_"
                f"{str(release['publication_date']).replace('-', '_')}"
            )
            records.append(
                {
                    "guidance_id": guidance_id,
                    "metric_id": metric_id,
                    "applicable_period": applicable_period,
                    "period_type": "guidance",
                    "publication_date": str(release["publication_date"]),
                    "source_document": f"sec_cache/ANF/{path.name}",
                    "source_document_sha256": str(release["sha256"]),
                    "source_section": f"guidance table {table_index}",
                    "unit": unit,
                    "currency": "USD" if unit == "$m" else "not_applicable",
                    "scale": "millions",
                    "numeric_state": typed["numeric_state"],
                    "low": typed["low"],
                    "high": typed["high"],
                    "point": typed["point"],
                    "company_wording": company_wording,
                    "numeric_usable": typed["numeric_usable"],
                    "status": "accepted",
                    "supersedes_guidance_ids": [],
                    "superseded_by_guidance_id": "",
                    "evidence_ref": _source_ref(
                        path,
                        f"guidance-table[{table_index}]:row[{row_index}]:{applicable_period}",
                    ),
                    "source_alias": (
                        f"ANF earnings release {release['publication_date']}"
                    ),
                }
            )
    identities = [
        (
            str(row["metric_id"]),
            str(row["applicable_period"]),
            str(row["publication_date"]),
        )
        for row in records
    ]
    if len(identities) != len(set(identities)):
        raise CapitalReturnResolutionError(
            f"Release {path.name!r} contains duplicate Capital Return guidance identities."
        )
    return records


def _guidance_collection(sec_cache_root: Path) -> tuple[dict[str, Any], ...]:
    publication_dates = _publication_dates(sec_cache_root)
    releases = [
        _release_identity(
            path,
            sec_cache_root=sec_cache_root,
            publication_dates=publication_dates,
        )
        for path in sorted(
            sec_cache_root.glob("doc_000101884026*_q*pressrelease.htm"),
            key=lambda item: item.name.casefold(),
        )
        if _RELEASE_RE.fullmatch(path.name)
    ]
    records = [
        row
        for release in releases
        for row in _release_guidance_records(release)
    ]
    by_scope: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for record in records:
        key = (str(record["metric_id"]), str(record["applicable_period"]))
        by_scope.setdefault(key, []).append(record)
    for scope_records in by_scope.values():
        scope_records.sort(
            key=lambda row: (
                str(row["publication_date"]),
                str(row["guidance_id"]),
            )
        )
        for index, record in enumerate(scope_records):
            if index < len(scope_records) - 1:
                successor = scope_records[index + 1]
                record["status"] = "superseded"
                record["superseded_by_guidance_id"] = successor["guidance_id"]
                successor["supersedes_guidance_ids"].append(record["guidance_id"])
    records.sort(
        key=lambda row: (
            str(row["applicable_period"]),
            str(row["metric_id"]),
            str(row["publication_date"]),
        )
    )
    return tuple(records)


def _period_reconciliation(
    records: Sequence[Mapping[str, Any]],
    *,
    method: str,
    component_periods: Sequence[str] = (),
) -> dict[str, Any]:
    if not records:
        raise CapitalReturnResolutionError("A period reconciliation cannot be empty.")
    duration_rows = [
        row
        for row in records
        if str(row.get("duration_or_instant") or "") == "duration"
    ]
    identities = {
        (
            str(row.get("fiscal_period") or ""),
            str(row.get("period_type") or ""),
            str(row.get("period_start") or ""),
            str(row.get("period_end") or ""),
        )
        for row in duration_rows
    }
    if len(identities) != 1:
        raise CapitalReturnResolutionError(
            f"Period reconciliation has mixed identities: {sorted(identities)!r}."
        )
    fiscal_period, period_type, start, end = next(iter(identities))
    point_dates = {
        str(row.get("period_end") or "")
        for row in records
        if str(row.get("duration_or_instant") or "") == "instant"
    }
    lower_bound = (
        datetime.fromisoformat(start).date() - timedelta(days=1)
    ).isoformat()
    if any(point_date < lower_bound or point_date > end for point_date in point_dates):
        raise CapitalReturnResolutionError(
            f"Period reconciliation has an out-of-bounds point-in-time record: "
            f"{sorted(point_dates)!r}."
        )
    return {
        "fiscal_period": fiscal_period,
        "period_type": period_type,
        "period_start": start,
        "period_end": end,
        "method": method,
        "component_periods": list(component_periods),
        "record_count": len(records),
        "record_ids": sorted(str(row.get("record_id") or "") for row in records),
        "source_document_sha256": _combined_sha(records),
        "status": "accepted",
    }


def build_anf_capital_return_collection(
    sec_cache_root: Path,
) -> ANFCapitalReturnSourceExtraction:
    """Build the bounded source-native ANF Capital Return collection offline."""

    if not sec_cache_root.is_dir():
        raise CapitalReturnResolutionError(
            "Local ANF SEC cache is required; network fallback is prohibited."
        )
    filings = _discover_filings(sec_cache_root)
    by_period = {filing.fiscal_period_id: filing for filing in filings}

    ytd_lists: dict[str, list[dict[str, Any]]] = {}
    ytd_maps: dict[str, dict[str, dict[str, Any]]] = {}
    for period in (
        "2024-Q1",
        "2024-Q2",
        "2024-Q3",
        "2025-Q1",
        "2025-Q2",
        "2025-Q3",
        "2026-Q1",
    ):
        filing = by_period[period]
        rows = _build_ytd_records(filing)
        _append_dividend_and_coverage_states(rows, filing=filing)
        ytd_lists[period] = rows
        ytd_maps[period] = _record_map(rows)

    annual_lists = {
        period: _build_annual_records(by_period[period])
        for period in ANF_REQUIRED_ANNUAL_PERIODS
    }
    annual_maps = {
        period: _record_map(rows)
        for period, rows in annual_lists.items()
    }

    quarter_lists: dict[str, list[dict[str, Any]]] = {}
    quarter_maps: dict[str, dict[str, dict[str, Any]]] = {}
    for fiscal_year in (2024, 2025, 2026):
        prior_quarter: dict[str, dict[str, Any]] | None = None
        for quarter_number in (1, 2, 3):
            period = f"{fiscal_year}-Q{quarter_number}"
            if period not in by_period:
                continue
            prior_ytd = (
                ytd_maps.get(f"{fiscal_year}-Q{quarter_number - 1}")
                if quarter_number > 1
                else None
            )
            rows = _build_quarter_records(
                filing=by_period[period],
                current_ytd=ytd_maps[period],
                prior_ytd=prior_ytd,
                annual=None,
                prior_quarter=prior_quarter,
            )
            quarter_lists[period] = rows
            quarter_maps[period] = _record_map(rows)
            prior_quarter = quarter_maps[period]
        annual_period = f"{fiscal_year}-FY"
        if annual_period in by_period:
            q3_period = f"{fiscal_year}-Q3"
            q4_period = f"{fiscal_year}-Q4"
            rows = _build_quarter_records(
                filing=by_period[annual_period],
                current_ytd=None,
                prior_ytd=ytd_maps[q3_period],
                annual=annual_maps[annual_period],
                prior_quarter=quarter_maps[q3_period],
            )
            quarter_lists[q4_period] = rows
            quarter_maps[q4_period] = _record_map(rows)

    if tuple(sorted(quarter_lists)) != tuple(sorted(ANF_REQUIRED_QUARTERS)):
        raise CapitalReturnResolutionError(
            "ANF Capital Return quarter construction did not reproduce the required "
            f"period set: {sorted(quarter_lists)!r}."
        )

    for annual_period in ANF_REQUIRED_ANNUAL_PERIODS:
        fiscal_year = annual_period.split("-", 1)[0]
        components = tuple(
            quarter_maps[f"{fiscal_year}-Q{quarter}"]
            for quarter in (1, 2, 3, 4)
        )
        _enrich_annual_records(
            annual_lists[annual_period],
            quarter_records=components,
            filing=by_period[annual_period],
        )
        annual_maps[annual_period] = _record_map(annual_lists[annual_period])

    ttm_components = tuple(quarter_maps[period] for period in ANF_REQUIRED_TTM_COMPONENTS)
    ttm_records = _build_ttm_records(
        ttm_components,
        filing=by_period["2026-Q1"],
    )

    records: list[dict[str, Any]] = []
    reconciliations: list[dict[str, Any]] = []
    for period in sorted(ytd_lists):
        rows = ytd_lists[period]
        records.extend(rows)
        reconciliations.append(
            _period_reconciliation(
                rows,
                method=(
                    "direct current-year XBRL duration"
                    if period.endswith("-Q1")
                    else "direct current YTD XBRL duration"
                ),
            )
        )
    for period in ANF_REQUIRED_QUARTERS:
        rows = quarter_lists[period]
        records.extend(rows)
        quarter_number = int(period[-1])
        method = (
            "direct current-quarter XBRL and issuer-purchases table"
            if quarter_number == 1
            else "annual minus Q3 YTD plus issuer-purchases table"
            if quarter_number == 4
            else "current YTD minus prior YTD plus exact quarter XBRL and issuer-purchases table"
        )
        reconciliations.append(_period_reconciliation(rows, method=method))
    for period in ANF_REQUIRED_ANNUAL_PERIODS:
        rows = annual_lists[period]
        records.extend(rows)
        reconciliations.append(
            _period_reconciliation(
                rows,
                method="direct annual XBRL plus exact four-quarter issuer-table aggregation",
                component_periods=tuple(
                    f"{period[:4]}-Q{quarter}" for quarter in (1, 2, 3, 4)
                ),
            )
        )
    records.extend(ttm_records)
    reconciliations.append(
        _period_reconciliation(
            ttm_records,
            method="exact four-consecutive-quarter aggregation",
            component_periods=ANF_REQUIRED_TTM_COMPONENTS,
        )
    )
    records.extend(_authorization_records(by_period["2025-FY"]))

    normalized_records = validate_capital_return_records(records)
    guidance = _guidance_collection(sec_cache_root)
    source_documents = [
        {
            "path": f"sec_cache/ANF/{filing.path.name}",
            "sha256": filing.sha256,
            "size": filing.size,
            "accession": filing.accession,
            "document_type": filing.form,
            "fiscal_period": filing.fiscal_period_id,
            "period_start": filing.period_start,
            "period_end": filing.period_end,
            "publication_date": filing.publication_date,
        }
        for filing in filings
    ]
    guidance_documents = {
        str(row["source_document"]): {
            "path": str(row["source_document"]),
            "sha256": str(row["source_document_sha256"]),
            "size": (
                sec_cache_root / Path(str(row["source_document"])).name
            ).stat().st_size,
            "accession": "",
            "document_type": "earnings_release",
            "fiscal_period": str(row["applicable_period"]),
            "period_start": "",
            "period_end": "",
            "publication_date": str(row["publication_date"]),
        }
        for row in guidance
    }
    source_documents.extend(guidance_documents.values())
    source_documents.sort(
        key=lambda row: (
            str(row["publication_date"]),
            str(row["path"]).casefold(),
        )
    )
    return ANFCapitalReturnSourceExtraction(
        records=tuple(normalized_records),
        guidance=tuple(guidance),
        period_reconciliations=tuple(
            sorted(
                reconciliations,
                key=lambda row: (
                    str(row["period_end"]),
                    str(row["period_type"]),
                    str(row["fiscal_period"]),
                ),
            )
        ),
        source_documents=tuple(source_documents),
    )
