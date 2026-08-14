"""Valuation debt/source-backed display support helpers."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, MutableMapping, Optional, Tuple

import pandas as pd

from .debt_detail_lineage import (
    DEBT_DETAIL_LINEAGE_DISPOSITION_COLUMN,
    DEBT_DETAIL_SOURCE_LINEAGE_CONTRACT_ID,
    DebtDetailLineageDisposition,
    normalize_debt_detail_lineage_dispositions,
)
from .debt_rate_semantics import DEBT_RATE_OWNERSHIP_CONTRACT_ID, DebtRateRole
from .longitudinal_memory.identity import build_identity, canonical_company_id


DEBT_PRINCIPAL_METRIC_ID = "metric:core:debt-principal@1"


@dataclass(frozen=True)
class SourceBackedDebtProjectionLineage:
    economic_id: str
    source_document_id: str
    source_occurrence_id: str
    source_document_name: str
    source_document_sha256: str
    source_page: Optional[int]
    source_locator: str
    reporting_date: str
    metric_id: str
    value: float
    unit_id: str
    currency: str
    value_status: str
    issuer_instrument_label: str
    source_row_contract_id: str = ""
    source_table_id: str = ""
    source_context_id: str = ""
    source_member: str = ""
    source_fact_id: str = ""
    related_source_fact_ids: Tuple[str, ...] = ()
    rate_value: Optional[float] = None
    rate_display: str = ""
    rate_context_id: str = ""
    rate_fact_id: str = ""
    rate_fact_name: str = ""
    rate_role: str = ""
    rate_authority: str = ""
    rate_reporting_date: str = ""
    rate_fact_ids: Tuple[str, ...] = ()
    debt_rate_facts: Tuple[Dict[str, Any], ...] = ()
    rate_ownership_contract_id: str = ""
    derivation_rule_id: str = ""
    derivation_input_ids: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        required = (
            self.economic_id,
            self.source_document_id,
            self.source_occurrence_id,
            self.source_document_name,
            self.source_document_sha256,
            self.source_locator,
            self.reporting_date,
            self.metric_id,
            self.unit_id,
            self.currency,
            self.value_status,
            self.issuer_instrument_label,
        )
        if not all(str(value or "").strip() for value in required):
            raise ValueError("Source-backed debt projection lineage is incomplete.")
        if self.value_status not in {"direct_source", "derived"}:
            raise ValueError(f"Unsupported debt projection value status: {self.value_status!r}")
        if self.value_status == "derived" and (
            not self.derivation_rule_id or not self.derivation_input_ids
        ):
            raise ValueError("Derived debt projection lineage requires a rule and input IDs.")
        if re_search_workbook_coordinate(self.economic_id):
            raise ValueError("Workbook coordinates cannot own a debt economic identity.")

    def as_columns(self) -> Dict[str, Any]:
        return {
            "lineage_contract_id": DEBT_DETAIL_SOURCE_LINEAGE_CONTRACT_ID,
            "economic_id": self.economic_id,
            "source_document_id": self.source_document_id,
            "source_occurrence_id": self.source_occurrence_id,
            "source_document_name": self.source_document_name,
            "source_document_sha256": self.source_document_sha256,
            "source_page": self.source_page,
            "source_locator": self.source_locator,
            "reporting_date": self.reporting_date,
            "metric_id": self.metric_id,
            "unit_id": self.unit_id,
            "currency": self.currency,
            "value_status": self.value_status,
            "issuer_instrument_label": self.issuer_instrument_label,
            "source_row_contract_id": self.source_row_contract_id,
            "source_table_id": self.source_table_id,
            "source_context_id": self.source_context_id,
            "source_member": self.source_member,
            "source_fact_id": self.source_fact_id,
            "related_source_fact_ids": tuple(self.related_source_fact_ids),
            "rate_value": self.rate_value,
            "rate_display": self.rate_display,
            "rate_context_id": self.rate_context_id,
            "rate_fact_id": self.rate_fact_id,
            "rate_fact_name": self.rate_fact_name,
            "rate_role": self.rate_role,
            "rate_authority": self.rate_authority,
            "rate_reporting_date": self.rate_reporting_date,
            "rate_fact_ids": tuple(self.rate_fact_ids),
            "debt_rate_facts": tuple(dict(item) for item in self.debt_rate_facts),
            "rate_ownership_contract_id": self.rate_ownership_contract_id,
            "derivation_rule_id": self.derivation_rule_id,
            "derivation_input_ids": tuple(self.derivation_input_ids),
            DEBT_DETAIL_LINEAGE_DISPOSITION_COLUMN: DebtDetailLineageDisposition.VALID,
        }


def re_search_workbook_coordinate(value: Any) -> bool:
    """Reject coordinate-shaped canonical identities without importing a renderer."""

    import re

    return bool(re.search(r"(?:^|[|:=])(?:cell=)?\$?[A-Z]{1,3}\$?[1-9][0-9]*(?:$|[|])", str(value or "")))


def _optional_scalar_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def _tuple_text_values(value: Any) -> Tuple[str, ...]:
    if value is None:
        return ()
    try:
        if pd.isna(value):
            return ()
    except (TypeError, ValueError):
        pass
    if isinstance(value, (str, bytes)):
        return (str(value).strip(),) if str(value).strip() else ()
    try:
        return tuple(str(item).strip() for item in value if str(item).strip())
    except TypeError:
        return ()


def _tuple_mapping_values(value: Any) -> Tuple[Dict[str, Any], ...]:
    if value is None:
        return ()
    try:
        if pd.isna(value):
            return ()
    except (TypeError, ValueError):
        pass
    if isinstance(value, dict):
        return (dict(value),)
    try:
        return tuple(dict(item) for item in value if isinstance(item, dict))
    except TypeError:
        return ()


def _source_backed_debt_lineage(
    *,
    ticker: Any,
    row: Any,
    name: str,
    reporting_date: Any,
    value: float,
) -> Optional[SourceBackedDebtProjectionLineage]:
    company_id = str(ticker or "").strip().upper()
    try:
        company_id = canonical_company_id(company_id)
    except Exception:
        return None
    source_path = Path(_optional_scalar_text(row.get("doc")))
    if not source_path.is_file():
        return None
    try:
        source_sha256 = hashlib.sha256(source_path.read_bytes()).hexdigest()
    except OSError:
        return None
    declared_sha256 = _optional_scalar_text(row.get("source_document_sha256")).lower()
    if declared_sha256 and declared_sha256 != source_sha256:
        return None
    page_num = None
    raw_page = row.get("printed_page")
    if raw_page is None:
        raw_page = row.get("page")
    try:
        if raw_page is not None and str(raw_page).strip() and str(raw_page).lower() != "nan":
            page_num = int(float(raw_page))
    except (TypeError, ValueError):
        return None
    q_iso = str(reporting_date.date().isoformat() if hasattr(reporting_date, "date") else reporting_date)
    source_reporting_date = _optional_scalar_text(row.get("reporting_date"))
    if not source_reporting_date:
        source_reporting_date = _optional_scalar_text(row.get("quarter"))
    if source_reporting_date:
        try:
            source_reporting_date = str(pd.Timestamp(source_reporting_date).date().isoformat())
        except Exception:
            return None
        if source_reporting_date != q_iso:
            return None
    issuer_label = _optional_scalar_text(row.get("issuer_instrument_label")) or str(name or "").strip()
    if not issuer_label:
        return None
    source_row_contract_id = _optional_scalar_text(row.get("debt_table_row_contract_id"))
    locator = _optional_scalar_text(row.get("source_locator")) or _optional_scalar_text(
        row.get("locator")
    )
    if not locator:
        if source_row_contract_id:
            return None
        locator = (
            f"page:{page_num};instrument:{name}"
            if page_num is not None
            else f"table:as-of-column-{int(row.get('asof_col_idx') or 0) + 1};instrument:{name}"
        )
    source_document_id = build_identity(
        "debt-source-document",
        (
            ("co", company_id),
            ("type", str(row.get("source") or "source-document").strip().lower().replace("_", "-")),
            ("name", source_path.name),
            ("sha256", source_sha256),
        ),
    )
    source_occurrence_id = build_identity(
        "debt-source-occurrence",
        (("doc", source_document_id), ("locator", locator)),
    )
    economic_id = build_identity(
        "debt-instrument",
        (
            ("co", company_id),
            ("name", issuer_label),
            ("as-of", q_iso),
            ("role", "principal"),
        ),
    )
    raw_unit = str(row.get("unit") or "USD").strip().upper()
    currency = "USD" if raw_unit in {"USD", "$", "US DOLLARS"} else raw_unit
    if currency != "USD":
        return None
    raw_rate_value = pd.to_numeric(row.get("rate_value"), errors="coerce")
    rate_value = float(raw_rate_value) if pd.notna(raw_rate_value) else None
    return SourceBackedDebtProjectionLineage(
        economic_id=economic_id,
        source_document_id=source_document_id,
        source_occurrence_id=source_occurrence_id,
        source_document_name=source_path.name,
        source_document_sha256=source_sha256,
        source_page=page_num,
        source_locator=locator,
        reporting_date=q_iso,
        metric_id=DEBT_PRINCIPAL_METRIC_ID,
        value=float(value),
        unit_id="unit:core:usd@1",
        currency=currency,
        value_status="direct_source",
        issuer_instrument_label=issuer_label,
        source_row_contract_id=source_row_contract_id,
        source_table_id=_optional_scalar_text(row.get("source_table_id")),
        source_context_id=_optional_scalar_text(row.get("source_context_id")),
        source_member=_optional_scalar_text(row.get("source_member")),
        source_fact_id=_optional_scalar_text(row.get("source_fact_id")),
        related_source_fact_ids=_tuple_text_values(row.get("related_fact_ids")),
        rate_value=rate_value,
        rate_display=_optional_scalar_text(row.get("rate_display")),
        rate_context_id=_optional_scalar_text(row.get("rate_context_id")),
        rate_fact_id=_optional_scalar_text(row.get("rate_fact_id")),
        rate_fact_name=_optional_scalar_text(row.get("rate_fact_name")),
        rate_role=_optional_scalar_text(row.get("rate_role")),
        rate_authority=_optional_scalar_text(row.get("rate_authority")),
        rate_reporting_date=_optional_scalar_text(row.get("rate_reporting_date")),
        rate_fact_ids=_tuple_text_values(row.get("rate_fact_ids")),
        debt_rate_facts=_tuple_mapping_values(row.get("debt_rate_facts")),
        rate_ownership_contract_id=_optional_scalar_text(
            row.get("rate_ownership_contract_id")
        ),
    )


def _enrich_registered_financial_statement_debt_rows(
    frame: pd.DataFrame,
    *,
    reporting_date: Any,
) -> pd.DataFrame:
    """Replay the canonical parser for stale cached rows before projection.

    Pipeline bundles are transport caches, not source authority.  A registered
    inline-XBRL document therefore re-establishes period and row identity before
    any cached numeric value can enter Debt Detail.
    """

    if frame is None or frame.empty or "doc" not in frame.columns:
        return frame
    out = frame.copy(deep=True)
    q = pd.to_datetime(reporting_date, errors="coerce")
    if pd.isna(q):
        return out
    try:
        from .pipeline_orchestration import _parse_financial_statement_debt_table_html
    except Exception:
        return out
    canonical_sources: Dict[str, Tuple[bool, Dict[str, Dict[str, Any]]]] = {}
    for doc_text in sorted(
        {
            _optional_scalar_text(value)
            for value in out["doc"].tolist()
            if _optional_scalar_text(value).lower().endswith((".htm", ".html"))
        }
    ):
        source_path = Path(doc_text)
        if not source_path.is_file():
            continue
        try:
            source_prefix = source_path.read_bytes()[:200_000].lower()
        except OSError:
            continue
        has_inline_xbrl = b"ix:" in source_prefix or b"xmlns:ix" in source_prefix
        try:
            canonical_rows = _parse_financial_statement_debt_table_html(
                source_path,
                pd.Timestamp(q).date(),
            )
        except Exception:
            canonical_rows = []
        canonical_by_label = {
            " ".join(
                str(row.get("issuer_instrument_label") or row.get("tranche") or "")
                .strip()
                .lower()
                .split()
            ): row
            for row in canonical_rows
            if str(row.get("issuer_instrument_label") or row.get("tranche") or "").strip()
        }
        canonical_sources[doc_text] = (has_inline_xbrl, canonical_by_label)
    records: List[Dict[str, Any]] = []
    for record in out.to_dict("records"):
        doc_text = _optional_scalar_text(record.get("doc"))
        source_entry = canonical_sources.get(doc_text)
        if source_entry is None:
            records.append(record)
            continue
        has_inline_xbrl, canonical_by_label = source_entry
        raw_label = _optional_scalar_text(record.get("issuer_instrument_label"))
        if not raw_label:
            raw_label = _optional_scalar_text(record.get("tranche"))
        canonical = canonical_by_label.get(" ".join(raw_label.lower().split()))
        if canonical is None:
            if has_inline_xbrl:
                record["debt_table_row_contract_id"] = (
                    "contract:financial-statement-debt-table-row@1"
                )
            records.append(record)
            continue
        record.update(canonical)
        record["amount_num"] = canonical.get("amount")
        records.append(record)
    return pd.DataFrame(records, index=out.index)


@dataclass(frozen=True)
class ValuationDebtSupportDeps:
    runtime: MutableMapping[str, Any]


def source_backed_debt_tranches_from_slides(
    deps: ValuationDebtSupportDeps,
    slides_debt: Any,
    latest_quarter: Any,
    ticker: Any,
) -> Any:
    """Return a deduped, source-backed tranche display table for debt detail.

    This is intentionally a display fallback for cases where the stricter tranche
    tie-out guardrail suppresses Debt_Tranches_Latest.  It does not override the
    carrying debt basis; it gives the user the current source schedule plus a
    reconciliation row.
    """
    runtime = deps.runtime
    pd = runtime["pd"]
    re = runtime["re"]

    if slides_debt is None or getattr(slides_debt, "empty", True):
        return pd.DataFrame()
    q = pd.to_datetime(latest_quarter, errors="coerce")
    if pd.isna(q):
        return pd.DataFrame()
    df = slides_debt.copy()
    if "quarter" not in df.columns:
        return pd.DataFrame()
    df["quarter"] = pd.to_datetime(df["quarter"], errors="coerce")
    df = df[df["quarter"].dt.normalize().eq(pd.Timestamp(q).normalize())]
    if df.empty:
        return pd.DataFrame()
    df = _enrich_registered_financial_statement_debt_rows(
        df,
        reporting_date=pd.Timestamp(q),
    )
    if "is_table_total" in df.columns:
        df = df[~df["is_table_total"].fillna(False).astype(bool)]
    if "amount" not in df.columns or "tranche" not in df.columns:
        return pd.DataFrame()
    df["amount_num"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df[df["amount_num"].notna() & (df["amount_num"] >= 0)]
    if df.empty:
        return pd.DataFrame()
    # Prefer the parsed statement table over PDF/text fragments when both are
    # available for the same quarter.  The PDF extractor often emits footnote
    # rows such as "2.25% ... 1,897 --" or generic Tallgrass rows that are
    # useful evidence but must not be added to the tranche principal schedule.
    if "doc" in df.columns:
        doc_txt = df["doc"].astype(str).str.lower()
        html_mask = doc_txt.str.endswith((".htm", ".html"))
        if "asof_match_found" in df.columns:
            asof_mask = df["asof_match_found"].fillna(False).astype(bool)
        else:
            asof_mask = pd.Series(True, index=df.index)
        preferred = df[html_mask & asof_mask]
        if not preferred.empty:
            df = preferred
        else:
            matched = df[asof_mask]
            if not matched.empty:
                df = matched

    def _clean_tranche_name(v: Any) -> str:
        txt = str(v or "").strip()
        txt = re.sub(r"\s+\$\s*[0-9,]+(?:\s+\$\s*[0-9,]+)*\s*$", "", txt)
        txt = re.sub(r"\s+[0-9]{1,3}(?:,[0-9]{3})+(?:\s+[0-9]{1,3}(?:,[0-9]{3})+)*\s*$", "", txt)
        txt = re.sub(r"\s+", " ", txt).strip()
        return txt

    def _dedup_key(row: Any) -> Tuple[str, Optional[int], int]:
        name = _clean_tranche_name(row.get("tranche"))
        name_norm = re.sub(r"\s*\(\d+\)\s*", "", name.lower())
        name_norm = re.sub(r"[^a-z0-9.%]+", " ", name_norm).strip()
        mat = pd.to_numeric(row.get("maturity_year"), errors="coerce")
        mat_key = int(mat) if pd.notna(mat) else None
        amt_key = int(round(float(row.get("amount_num") or 0.0), -3))
        return name_norm, mat_key, amt_key

    def _priority(row: Any) -> Tuple[int, int, int]:
        doc = str(row.get("doc") or "").lower()
        source = str(row.get("source") or "").lower()
        asof = bool(row.get("asof_match_found")) if "asof_match_found" in row.index else False
        htmlish = doc.endswith((".htm", ".html"))
        return (1 if asof else 0, 1 if htmlish else 0, 1 if source == "financial_statement" else 0)

    rows: Dict[Tuple[str, Optional[int], int], Any] = {}
    for _, row in df.iterrows():
        key = _dedup_key(row)
        if not key[0]:
            continue
        prev = rows.get(key)
        if prev is None or _priority(row) > _priority(prev):
            rows[key] = row

    out_rows: List[Dict[str, Any]] = []
    latest_year = int(pd.Timestamp(q).year)
    for _, row in sorted(
        rows.items(),
        key=lambda kv: (
            9999 if kv[1].get("maturity_year") is None or pd.isna(pd.to_numeric(kv[1].get("maturity_year"), errors="coerce")) else int(pd.to_numeric(kv[1].get("maturity_year"), errors="coerce")),
            str(kv[1].get("tranche") or ""),
        ),
    ):
        name = _clean_tranche_name(row.get("tranche"))
        issuer_label = _optional_scalar_text(row.get("issuer_instrument_label")) or name
        mat = pd.to_numeric(row.get("maturity_year"), errors="coerce")
        mat_year = int(mat) if pd.notna(mat) else None
        coupon = None
        rate_value = pd.to_numeric(row.get("rate_value"), errors="coerce")
        rate_fact_name = _optional_scalar_text(row.get("rate_fact_name")).lower()
        rate_role = _optional_scalar_text(row.get("rate_role")).lower()
        rate_contract = _optional_scalar_text(row.get("rate_ownership_contract_id"))
        rate_type = None
        spread_pct = None
        if pd.notna(rate_value):
            if rate_role == DebtRateRole.SPREAD_MARGIN.value:
                rate_type = "floating"
                spread_pct = float(rate_value)
            elif rate_role == DebtRateRole.COUPON_STATED_RATE.value:
                rate_type = "fixed"
                coupon = float(rate_value)
            elif not rate_contract:
                # Transition-only compatibility for pre-contract cached rows. New
                # source rows must provide an explicit semantic role.
                if rate_fact_name.endswith("debtinstrumentbasisspreadonvariablerate1"):
                    rate_type = "floating"
                    spread_pct = float(rate_value)
                elif rate_fact_name.endswith(
                    "debtinstrumentinterestratestatedpercentage"
                ):
                    rate_type = "fixed"
                    coupon = float(rate_value)
            elif rate_contract != DEBT_RATE_OWNERSHIP_CONTRACT_ID:
                raise ValueError(
                    f"Unsupported debt-rate ownership contract: {rate_contract!r}"
                )
        elif not _optional_scalar_text(row.get("debt_table_row_contract_id")):
            m_coupon = re.search(r"\b([0-9]+(?:\.[0-9]+)?)\s*%", name)
            if m_coupon:
                try:
                    coupon = float(m_coupon.group(1)) / 100.0
                    rate_type = "fixed"
                except Exception:
                    coupon = None
        near_term = bool(mat_year is not None and mat_year <= latest_year + 1)
        lineage = _source_backed_debt_lineage(
            ticker=ticker,
            row=row,
            name=issuer_label,
            reporting_date=pd.Timestamp(q),
            value=float(row.get("amount_num")),
        )
        projected = {
                "tranche_name": issuer_label,
                "amount_principal": float(row.get("amount_num")),
                "amount_carrying": None,
                "maturity_display": str(mat_year) if mat_year is not None else "",
                "maturity_year": mat_year,
                "rate_type": rate_type,
                "coupon_pct": coupon,
                "spread_pct": spread_pct,
                "near_term": near_term,
                "source_kind": "Slides_Debt_Profile",
                "source_basis": (
                    "source-backed principal; near-term = within 24 months of latest quarter end; "
                    "year-based conservative classification when exact maturity date is unavailable"
                ),
                "qa_status": "WARN",
                "review_note": "Fallback source-backed debt schedule shown because tranche tie-out guardrail suppressed Debt_Tranches_Latest.",
            }
        if lineage is None:
            projected[DEBT_DETAIL_LINEAGE_DISPOSITION_COLUMN] = (
                DebtDetailLineageDisposition.INVALID
            )
        else:
            projected.update(lineage.as_columns())
        out_rows.append(projected)
    return normalize_debt_detail_lineage_dispositions(pd.DataFrame(out_rows))
