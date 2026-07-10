"""Build the read-only ANF legacy-adapter normalized-data shadow package.

This is a migration fixture, not the generic source-native package path for new
tickers. It reads saved ANF workbook/support-sheet artifacts and source folders,
then emits normalized JSON and coverage reports. It does not call production
workbook writers and it never creates an ANF workbook.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

from openpyxl import load_workbook
from openpyxl.utils import range_boundaries

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pbi_xbrl.normalized_company_data_validation import (  # noqa: E402
    build_mapping_gap_report,
    build_normalized_text_quality_audit,
    classify_normalized_text_quality,
    validate_normalized_company_data,
)


REQUIRED_SECTIONS = [
    "ticker_metadata",
    "company_profile",
    "quarterly_financials",
    "annual_financials",
    "debt_liquidity",
    "capital_returns",
    "normalized_guidance",
    "segments",
    "operating_drivers",
    "quarter_notes",
    "investment_case",
    "valuation_outputs",
    "source_coverage",
    "mapping_gaps",
    "manual_review_flags",
]

# Guardrail for callers and tests: generic ticker onboarding must start from
# source evidence candidates, never from a legacy workbook adapter.
LEGACY_WORKBOOK_ADAPTER_FIXTURE = True
GENERIC_SOURCE_NATIVE_BUILDER = False

SOURCE_FAMILIES = {
    "annual_reports": "annual reports",
    "earnings_release": "earnings releases",
    "earnings_presentation": "presentations",
    "earnings_transcripts": "transcripts",
    "press_release": "press releases",
    "financial_statement": "financial statement files",
    "conferences": "conference files",
}

_NORMALIZED_UNITS = {
    "$", "$m", "$bn", "USD", "USDm", "USDbn", "%", "bps", "pp", "x",
    "$/share", "m shares", "shares", "count", "days", "quarters", "pts",
    "ratio", "stores", "visits", "m visits", "units",
}


def _default_data_root() -> Path:
    for ancestor in [REPO_ROOT, *REPO_ROOT.parents]:
        candidate = ancestor / "StockModelData"
        if candidate.exists():
            return candidate
    return REPO_ROOT.parent / "StockModelData"


def _default_workbook_path(data_root: Path) -> Path:
    return data_root / "outputs" / "Excel stock models" / "ANF_model.xlsx"


def _default_output_dir(data_root: Path) -> Path:
    return data_root / "outputs" / "stress_tests" / "ANF_new_ticker_engine"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _field(
    value: Any,
    *,
    status: str = "populated",
    source_ref: str = "",
    core: bool = False,
    reason: str = "",
    unit: str = "",
    period: str = "",
    confidence: str = "legacy_artifact_backed",
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "value": value,
        "status": status,
        "source_ref": source_ref,
        "core": bool(core),
    }
    if reason:
        out["reason"] = reason
    if unit:
        normalized_unit = unit if unit in _NORMALIZED_UNITS else "units"
        out["unit"] = normalized_unit
        if normalized_unit != unit:
            out["legacy_unit"] = unit
            out["unit_normalization_status"] = "manual_review_required"
    if period:
        out["period"] = period
    if confidence:
        out["confidence"] = confidence
    return out


def _missing(reason: str, *, source_ref: str = "", core: bool = False) -> dict[str, Any]:
    return _field(None, status="missing_source", source_ref=source_ref, core=core, reason=reason, confidence="")


def _not_applicable(reason: str, *, source_ref: str = "", core: bool = False) -> dict[str, Any]:
    return _field(None, status="not_applicable", source_ref=source_ref, core=core, reason=reason, confidence="")


def _is_present(value: Any) -> bool:
    return value not in (None, "")


def _to_iso(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value or "")


def _normalize_period(value: Any, *, period_type: str = "quarterly") -> str:
    raw = _to_iso(value).strip()
    if re.fullmatch(r"\d{4}-(?:Q[1-4]|FY)", raw):
        return raw
    if re.fullmatch(r"FY\d{4}", raw):
        return f"{raw[2:]}-FY"
    match = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", raw)
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        if period_type == "annual":
            return f"{year}-FY"
        quarter = (month - 1) // 3 + 1
        return f"{year}-Q{quarter}"
    return raw


def _publication_date_from_source(source: Any, fallback: Any = None) -> str:
    text = str(source or "")
    matches = re.findall(r"(?:19|20)\d{2}-\d{2}-\d{2}", text)
    if matches:
        return matches[-1]
    return _to_iso(fallback)


def _read_legacy_valuation_series(workbook_path: Path, row_number: int) -> dict[str, float]:
    """Read an ANF legacy display row as migration evidence only."""

    wb = load_workbook(workbook_path, read_only=True, data_only=True)
    try:
        ws = wb["Valuation"]
        values: dict[str, float] = {}
        for column in range(2, 14):
            period = str(ws.cell(6, column).value or "")
            value = ws.cell(row_number, column).value
            if period and isinstance(value, (int, float)):
                values[period] = float(value)
        return values
    finally:
        wb.close()


def _segment_dimension(member: str) -> str:
    lower = member.strip().lower()
    if lower in {"americas", "emea", "apac", "united states", "international"}:
        return "geography"
    if lower in {"abercrombie", "hollister", "gilly hicks"}:
        return "brand"
    if lower in {"total company", "company total", "total"}:
        return "total_company"
    return "reported_segment"


def _driver_type(group: str, driver: str) -> str:
    text = f"{group} {driver}".lower()
    for token, driver_type in (
        ("demand", "demand"),
        ("price", "pricing"),
        ("volume", "volume"),
        ("margin", "margin"),
        ("cost", "cost"),
        ("capital", "capital_allocation"),
        ("liquidity", "liquidity"),
        ("strategy", "strategy"),
    ):
        if token in text:
            return driver_type
    return "operational"


def _evidence_key(*parts: Any) -> str:
    canonical = "|".join(re.sub(r"\s+", " ", str(part or "")).strip() for part in parts)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def _limit_legacy_adapter_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    limit: int,
    collection_path: str,
    workbook_path: Path,
    truncations: list[dict[str, Any]],
    review_flags: list[dict[str, Any]],
) -> list[Mapping[str, Any]]:
    materialized = list(rows)
    if len(materialized) <= limit:
        return materialized
    dropped = len(materialized) - limit
    source_ref = f"{workbook_path.name}!legacy_adapter_selection"
    record = {
        "collection": collection_path,
        "selection": "tail",
        "input_rows": len(materialized),
        "retained_rows": limit,
        "dropped_rows": dropped,
        "reason": "Legacy migration fixture capacity limit; source-native builders must not copy this policy.",
        "source_ref": source_ref,
    }
    truncations.append(record)
    review_flags.append(
        {
            "severity": "P2",
            "rule_id": "legacy_adapter_truncation",
            "field": collection_path,
            "message": f"Legacy adapter retained the latest {limit} of {len(materialized)} rows and explicitly recorded {dropped} dropped rows.",
            "source_ref": source_ref,
            "suggested_action": "Use source-native evidence selection and an explicit planner overflow policy before onboarding a new ticker.",
            "adapter_metadata": record,
        }
    )
    return materialized[-limit:]


def _dedupe_legacy_adapter_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    collection_path: str,
    workbook_path: Path,
    deduplications: list[dict[str, Any]],
    review_flags: list[dict[str, Any]],
) -> list[Mapping[str, Any]]:
    retained: list[Mapping[str, Any]] = []
    seen: set[str] = set()
    for source_index, row in enumerate(rows):
        evidence_key = str(row.get("evidence_key") or "")
        if not evidence_key or evidence_key not in seen:
            retained.append(row)
            if evidence_key:
                seen.add(evidence_key)
            continue
        source_ref = f"{workbook_path.name}!legacy_adapter_selection"
        record = {
            "collection": collection_path,
            "source_index": source_index,
            "evidence_key": evidence_key,
            "reason": "Exact duplicate evidence key in the legacy workbook projection.",
            "source_ref": source_ref,
        }
        deduplications.append(record)
        review_flags.append(
            {
                "severity": "P2",
                "rule_id": "legacy_adapter_exact_duplicate",
                "field": f"{collection_path}.{source_index}",
                "message": f"Legacy adapter omitted an exact duplicate evidence row with key {evidence_key}.",
                "source_ref": source_ref,
                "suggested_action": "Confirm source-native evidence identity and deduplication policy before promotion.",
                "adapter_metadata": record,
            }
        )
    return retained


def _to_millions(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if abs(numeric) >= 100_000:
        return round(numeric / 1_000_000, 3)
    return round(numeric, 3)


def _clean_text(value: Any, *, limit: int = 420) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def _text_quality_action(classification: str) -> str:
    actions = {
        "boilerplate_or_legal": "Keep as source/audit coverage only; replace visible text with source-backed quarter commentary.",
        "compensation_or_governance_noise": "Keep governance/compensation snippets out of visible quarter notes.",
        "accounting_policy_or_definition": "Do not use formula/accounting definitions as operating-driver reads.",
        "release_header_or_source_title": "Strip source headers from visible segment notes.",
        "fragmented_sentence": "Review parser boundaries and rebuild a complete source-backed sentence.",
        "too_long_unstructured": "Condense or demote the snippet before visible rendering.",
        "missing_context": "Keep out of visible UI until enough context is normalized.",
    }
    return actions.get(classification, "Review this text before visible rendering.")


def _record_text_quality_demotion(
    demotions: list[dict[str, Any]],
    *,
    section: str,
    field: str,
    source_ref: str,
    classification: str,
    text: str,
) -> None:
    demotions.append(
        {
            "severity": "P2",
            "rule_id": "text_quality_demoted",
            "field": field,
            "message": f"Demoted non-visible-ready {section} text: {classification}.",
            "source_ref": source_ref,
            "suggested_action": _text_quality_action(classification),
            "section": section,
            "classification": classification,
            "original_excerpt": _clean_text(text, limit=220),
        }
    )


def _visible_text_or_blank(
    text: str,
    *,
    field: str,
    section: str,
    source_ref: str,
    demotions: list[dict[str, Any]],
) -> str:
    if not text:
        return ""
    classification = classify_normalized_text_quality(text, field=field, visible_ui=True)
    if classification == "clean_visible_ui":
        return text
    _record_text_quality_demotion(
        demotions,
        section=section,
        field=field,
        source_ref=source_ref,
        classification=classification,
        text=text,
    )
    return ""


def _visible_text_is_clean(
    text: str,
    *,
    field: str,
    section: str,
    source_ref: str,
    demotions: list[dict[str, Any]],
) -> bool:
    if not text:
        _record_text_quality_demotion(
            demotions,
            section=section,
            field=field,
            source_ref=source_ref,
            classification="missing_context",
            text="",
        )
        return False
    classification = classify_normalized_text_quality(text, field=field, visible_ui=True)
    if classification == "clean_visible_ui":
        return True
    _record_text_quality_demotion(
        demotions,
        section=section,
        field=field,
        source_ref=source_ref,
        classification=classification,
        text=text,
    )
    return False


def _text_quality_demotion_summary(demotions: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_section: dict[str, int] = defaultdict(int)
    by_classification: dict[str, int] = defaultdict(int)
    for row in demotions:
        by_section[str(row.get("section") or "unknown")] += 1
        by_classification[str(row.get("classification") or "unknown")] += 1
    return {
        "total_demoted": len(demotions),
        "by_section": dict(sorted(by_section.items())),
        "by_classification": dict(sorted(by_classification.items())),
    }


def _read_sheet_rows(workbook_path: Path, sheet_name: str) -> list[dict[str, Any]]:
    wb = load_workbook(workbook_path, read_only=True, data_only=True)
    try:
        if sheet_name not in wb.sheetnames:
            return []
        ws = wb[sheet_name]
        headers = [str(value or "").strip() for value in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
        rows: list[dict[str, Any]] = []
        for row_idx, values in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
            row = {headers[col_idx]: value for col_idx, value in enumerate(values) if col_idx < len(headers) and headers[col_idx]}
            if any(_is_present(value) for value in row.values()):
                row["_row_number"] = row_idx
                rows.append(row)
        return rows
    finally:
        wb.close()


def _read_cell(workbook_path: Path, sheet_name: str, cell_ref: str) -> Any:
    wb = load_workbook(workbook_path, read_only=True, data_only=True)
    try:
        return wb[sheet_name][cell_ref].value
    finally:
        wb.close()


def _build_legacy_visible_operating_drivers(workbook_path: Path, period: str) -> list[dict[str, Any]]:
    wb = load_workbook(workbook_path, read_only=True, data_only=True)
    try:
        ws = wb["Operating_Drivers"]
        items: list[dict[str, Any]] = []
        for priority, row_number in enumerate(range(6, 10), start=1):
            topic = _clean_text(ws.cell(row_number, 1).value, limit=120)
            current_read = _clean_text(ws.cell(row_number, 2).value, limit=260)
            why = _clean_text(ws.cell(row_number, 8).value, limit=260)
            source_ref = f"{workbook_path.name}!Operating_Drivers!row:{row_number}"
            if not topic or not current_read or not why:
                continue
            items.append(
                {
                    "topic": _field(topic, source_ref=source_ref, core=True),
                    "driver": _field(topic, source_ref=source_ref, core=True),
                    "current_read": _field(current_read, source_ref=source_ref, core=True),
                    "metric_value": _missing("The curated legacy watchlist row is qualitative.", source_ref=source_ref),
                    "source": source_ref,
                    "why_it_matters": _field(why, source_ref=source_ref, core=True),
                    "quality": "legacy_curated_visible_ui",
                    "period": period,
                    "driver_type": _driver_type(topic, current_read),
                    "evidence_key": _evidence_key(source_ref, topic, current_read, why),
                    "display_role": "current_watchlist",
                    "display_priority": priority,
                }
            )
        return items
    finally:
        wb.close()


def _build_legacy_visible_quarter_notes(workbook_path: Path, period: str) -> list[dict[str, Any]]:
    wb = load_workbook(workbook_path, read_only=True, data_only=True)
    try:
        ws = wb["Quarter_Notes_UI"]
        items: list[dict[str, Any]] = []
        for priority, row_number in enumerate(range(10, 16), start=1):
            theme = _clean_text(ws.cell(row_number, 1).value, limit=120)
            commentary = _clean_text(ws.cell(row_number, 3).value, limit=300)
            implication = _clean_text(ws.cell(row_number, 8).value, limit=300)
            if row_number == 14:
                implication = _clean_text(wb["Operating_Drivers"].cell(7, 8).value, limit=300)
            elif row_number == 15:
                implication = _clean_text(wb["Operating_Drivers"].cell(6, 8).value, limit=300)
            visible_source = _clean_text(ws.cell(row_number, 13).value, limit=220)
            source_ref = f"{workbook_path.name}!Quarter_Notes_UI!row:{row_number}"
            if not theme or not commentary or not implication:
                continue
            items.append(
                {
                    "theme": _field(theme, source_ref=source_ref),
                    "quarter": _field(period, source_ref=source_ref),
                    "metric": _field(theme, source_ref=source_ref),
                    "note": _field(commentary, source_ref=source_ref, core=True),
                    "commentary": _field(commentary, source_ref=source_ref, core=True),
                    "model_implication": _field(implication, source_ref=source_ref, core=True),
                    "valuation_implication": _field(implication, source_ref=source_ref),
                    "source": visible_source or source_ref,
                    "confidence": "legacy_curated_visible_ui",
                    "evidence_key": _evidence_key(source_ref, period, theme, commentary),
                    "display_role": "current_note",
                    "display_priority": priority,
                }
            )
        return items
    finally:
        wb.close()


def _source_ref(sheet: str, row: Mapping[str, Any] | None = None, *, workbook_path: Path) -> str:
    if row and row.get("_row_number"):
        return f"{workbook_path.name}!{sheet}!row:{row['_row_number']}"
    return f"{workbook_path.name}!{sheet}"


def _source_file_candidates(data_root: Path) -> list[Path]:
    roots = [
        data_root / "tickers" / "ANF",
        data_root / "sec_cache" / "ANF",
        data_root / "tickers" / "sec_cache" / "ANF",
    ]
    suffixes = {".txt", ".htm", ".html", ".json", ".csv", ".xlsx", ".pdf", ".md"}
    paths: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if path.is_file() and path.suffix.lower() in suffixes:
                paths.append(path)
    return sorted(paths, key=lambda item: str(item).lower())


def _source_coverage(data_root: Path, workbook_path: Path) -> dict[str, Any]:
    files = _source_file_candidates(data_root)
    family_counts: dict[str, int] = {}
    for family in SOURCE_FAMILIES:
        family_root = data_root / "tickers" / "ANF" / family
        family_counts[family] = len([path for path in files if family_root in path.parents])
    return {
        "sources": [
            {
                "path": str(path),
                "kind": path.suffix.lower().lstrip(".") or "file",
                "family": _classify_source_family(path),
                "status": "available",
            }
            for path in files
        ],
        "source_roots": {
            "ticker_root": str(data_root / "tickers" / "ANF"),
            "sec_cache": str(data_root / "sec_cache" / "ANF"),
            "ticker_sec_cache": str(data_root / "tickers" / "sec_cache" / "ANF"),
            "legacy_workbook": str(workbook_path),
        },
        "family_counts": family_counts,
        "legacy_artifacts": [
            "ANF_model.xlsx",
            "ANF_model.xlsx!History_Q",
            "ANF_model.xlsx!Guidance_Normalized",
            "ANF_model.xlsx!Promise_Progress",
            "ANF_model.xlsx!Quarter_Notes",
            "ANF_model.xlsx!Slides_Segments",
            "ANF_model.xlsx!Leverage_Liquidity",
            "ANF_model.xlsx!ANF_Investment_Case_Data",
        ],
    }


def _classify_source_family(path: Path) -> str:
    lower_parts = {part.lower() for part in path.parts}
    for family in SOURCE_FAMILIES:
        if family.lower() in lower_parts:
            return family
    if "sec_cache" in lower_parts:
        return "sec_xbrl"
    return "other"


def _build_quarterly_financial_rows(history_rows: Sequence[Mapping[str, Any]], workbook_path: Path) -> list[dict[str, Any]]:
    populated = [row for row in history_rows if _is_present(row.get("revenue"))]
    populated.sort(key=lambda row: _to_iso(row.get("quarter")))
    out: list[dict[str, Any]] = []
    adjusted_ebitda_by_period = _read_legacy_valuation_series(workbook_path, 24)
    for row in populated:
        period = _normalize_period(row.get("fiscal_label") or row.get("quarter"), period_type="quarterly")
        source_ref = _source_ref("History_Q", row, workbook_path=workbook_path)
        cfo = _to_millions(row.get("cfo"))
        capex = _to_millions(row.get("capex"))
        fcf = round(cfo - capex, 3) if cfo is not None and capex is not None else None
        out.append(
            {
                "period": period,
                "fiscal_year": row.get("fiscal_year"),
                "fiscal_quarter": row.get("fiscal_quarter"),
                "period_end": _to_iso(row.get("quarter")),
                "revenue": _populated_number(row.get("revenue"), source_ref, "$m", period, core=True),
                "gross_profit": _populated_number(row.get("gross_profit"), source_ref, "$m", period),
                "operating_income": _populated_number(row.get("op_income"), source_ref, "$m", period, core=True),
                "base_ebitda": _populated_number(row.get("ebitda"), source_ref, "$m", period),
                "adjusted_ebitda": _field(
                    adjusted_ebitda_by_period[period],
                    source_ref=f"{workbook_path.name}!Valuation!row:24",
                    unit="$m",
                    period=period,
                )
                if period in adjusted_ebitda_by_period
                else _missing("Legacy adjusted EBITDA row has no value for this period.", source_ref=f"{workbook_path.name}!Valuation!row:24"),
                "net_income": _populated_number(row.get("net_income"), source_ref, "$m", period),
                "eps": _populated_scalar(row.get("eps_diluted"), source_ref, "$/share", period),
                "operating_cash_flow": _populated_number(row.get("cfo"), source_ref, "$m", period),
                "capital_expenditures": _populated_number(row.get("capex"), source_ref, "$m", period),
                "free_cash_flow": _field(fcf, source_ref=source_ref, core=True, unit="$m", period=period)
                if fcf is not None
                else _missing("CFO or capex is absent for this quarter.", source_ref=source_ref, core=True),
                "diluted_shares": _populated_share_count(row.get("shares_diluted"), source_ref, period, core=True),
            }
        )
    return out


def _build_annual_financial_rows(history_rows: Sequence[Mapping[str, Any]], workbook_path: Path) -> list[dict[str, Any]]:
    by_year: dict[Any, list[Mapping[str, Any]]] = defaultdict(list)
    for row in history_rows:
        year = row.get("fiscal_year")
        if _is_present(year) and _is_present(row.get("revenue")):
            by_year[year].append(row)
    annuals: list[dict[str, Any]] = []
    adjusted_ebitda_by_period = _read_legacy_valuation_series(workbook_path, 24)
    for year, rows in by_year.items():
        rows = sorted(rows, key=lambda row: _to_iso(row.get("quarter")))
        if len(rows) < 4:
            continue
        source_ref = f"{workbook_path.name}!History_Q!fiscal_year:{year}"
        period = f"{year}-FY"
        cfo = sum(float(row.get("cfo") or 0) for row in rows)
        capex = sum(float(row.get("capex") or 0) for row in rows)
        adjusted_ebitda = sum(adjusted_ebitda_by_period.get(str(row.get("fiscal_label") or ""), 0.0) for row in rows)
        annuals.append(
            {
                "period": period,
                "fiscal_year": year,
                "revenue": _field(round(sum(float(row.get("revenue") or 0) for row in rows) / 1_000_000, 3), source_ref=source_ref, core=True, unit="$m", period=period),
                "gross_profit": _field(round(sum(float(row.get("gross_profit") or 0) for row in rows) / 1_000_000, 3), source_ref=source_ref, unit="$m", period=period),
                "operating_income": _field(round(sum(float(row.get("op_income") or 0) for row in rows) / 1_000_000, 3), source_ref=source_ref, core=True, unit="$m", period=period),
                "base_ebitda": _field(round(sum(float(row.get("ebitda") or 0) for row in rows) / 1_000_000, 3), source_ref=source_ref, unit="$m", period=period),
                "adjusted_ebitda": _field(round(adjusted_ebitda, 3), source_ref=f"{workbook_path.name}!Valuation!row:24", unit="$m", period=period),
                "net_income": _field(round(sum(float(row.get("net_income") or 0) for row in rows) / 1_000_000, 3), source_ref=source_ref, unit="$m", period=period),
                "operating_cash_flow": _field(round(cfo / 1_000_000, 3), source_ref=source_ref, unit="$m", period=period),
                "capital_expenditures": _field(round(capex / 1_000_000, 3), source_ref=source_ref, unit="$m", period=period),
                "free_cash_flow": _field(round((cfo - capex) / 1_000_000, 3), source_ref=source_ref, core=True, unit="$m", period=period),
            }
        )
    annuals.sort(key=lambda row: str(row.get("fiscal_year")))
    return annuals


def _populated_number(value: Any, source_ref: str, unit: str, period: str, *, core: bool = False) -> dict[str, Any]:
    converted = _to_millions(value)
    if converted is None:
        return _missing("Legacy artifact did not contain a numeric value.", source_ref=source_ref, core=core)
    return _field(converted, source_ref=source_ref, core=core, unit=unit, period=period)


def _populated_scalar(value: Any, source_ref: str, unit: str, period: str, *, core: bool = False) -> dict[str, Any]:
    if value in (None, ""):
        return _missing("Legacy artifact did not contain a scalar value.", source_ref=source_ref, core=core)
    return _field(value, source_ref=source_ref, core=core, unit=unit, period=period)


def _populated_share_count(value: Any, source_ref: str, period: str, *, core: bool = False) -> dict[str, Any]:
    converted = _to_millions(value)
    if converted is None:
        return _missing("Legacy artifact did not contain diluted share count.", source_ref=source_ref, core=core)
    return _field(converted, source_ref=source_ref, core=core, unit="m shares", period=period)


def _build_debt_liquidity(history_rows: Sequence[Mapping[str, Any]], leverage_rows: Sequence[Mapping[str, Any]], workbook_path: Path) -> dict[str, Any]:
    history = next((row for row in reversed(history_rows) if _is_present(row.get("cash"))), {})
    leverage = next((row for row in reversed(leverage_rows) if _is_present(row.get("cash")) or _is_present(row.get("liquidity"))), {})
    source_ref = _source_ref("Leverage_Liquidity", leverage, workbook_path=workbook_path) if leverage else _source_ref("History_Q", history, workbook_path=workbook_path)
    cash = _to_millions(leverage.get("cash") if leverage else history.get("cash"))
    total_debt = _to_millions(history.get("total_debt")) if history else None
    if total_debt is None:
        total_debt = 0.0
    net_debt = _to_millions(leverage.get("corporate_net_debt")) if leverage else None
    if net_debt is None and cash is not None:
        net_debt = round(total_debt - cash, 3)
    return {
        "cash": _field(cash, source_ref=source_ref, core=True, unit="$m") if cash is not None else _missing("Cash not found in History_Q or Leverage_Liquidity.", source_ref=source_ref, core=True),
        "total_debt": _field(total_debt, source_ref=_source_ref("History_Q", history, workbook_path=workbook_path), core=True, unit="$m"),
        "net_debt": _field(net_debt, source_ref=source_ref, core=True, unit="$m") if net_debt is not None else _missing("Net debt could not be derived.", source_ref=source_ref, core=True),
        "revolver_availability": _populated_number(leverage.get("revolver_availability"), source_ref, "$m", "latest"),
        "liquidity": _populated_number(leverage.get("liquidity"), source_ref, "$m", "latest", core=True),
        "lease_liabilities": _populated_number(history.get("lease_liabilities"), _source_ref("History_Q", history, workbook_path=workbook_path), "$m", "latest"),
        "interest_expense": _populated_number(leverage.get("interest_expense_net_ttm"), source_ref, "$m", "ttm"),
        "maturity_schedule": [],
    }


def _build_capital_returns(history_rows: Sequence[Mapping[str, Any]], workbook_path: Path) -> dict[str, Any]:
    latest_rows: list[Mapping[str, Any]] = []
    for row in sorted([item for item in history_rows if _is_present(item.get("quarter"))], key=lambda item: _to_iso(item.get("quarter"))):
        latest_rows.append(row)
        if len(latest_rows) > 4:
            latest_rows.pop(0)
    source_ref = f"{workbook_path.name}!History_Q!latest_4_quarters"
    buybacks = sum(float(row.get("buybacks_cash") or 0) for row in latest_rows)
    dividends = sum(float(row.get("dividends_cash") or 0) for row in latest_rows)
    return {
        "buybacks": _field(round(buybacks / 1_000_000, 3), source_ref=source_ref, core=True, unit="$m", period="latest_4_quarters"),
        "dividends": _field(round(dividends / 1_000_000, 3), source_ref=source_ref, unit="$m", period="latest_4_quarters")
        if dividends
        else _not_applicable("ANF has no common dividend signal in the legacy artifact.", source_ref=source_ref),
        "share_issuance": _not_applicable("No equity issuance program is represented in the legacy ANF artifact.", source_ref=source_ref),
    }


def _build_valuation_inputs(
    quarterly_rows: Sequence[Mapping[str, Any]],
    debt_liquidity: Mapping[str, Any],
    workbook_path: Path,
) -> dict[str, Any]:
    latest_four = list(quarterly_rows[-4:])
    source_ref = f"{workbook_path.name}!History_Q!latest_4_quarters"

    def total(field_name: str) -> dict[str, Any]:
        values = [row.get(field_name, {}).get("value") for row in latest_four if isinstance(row.get(field_name), Mapping)]
        numeric = [float(value) for value in values if isinstance(value, (int, float))]
        if len(numeric) != len(latest_four):
            return _missing(f"TTM {field_name} requires four source-backed quarterly values.", source_ref=source_ref, core=True)
        return _field(round(sum(numeric), 3), source_ref=source_ref, core=True, unit="$m", period="TTM")

    latest = latest_four[-1] if latest_four else {}
    latest_source = str(latest.get("revenue", {}).get("source_ref") or source_ref) if isinstance(latest.get("revenue"), Mapping) else source_ref
    as_of = str(latest.get("period_end") or "")
    diluted = latest.get("diluted_shares") if isinstance(latest.get("diluted_shares"), Mapping) else _missing("Latest diluted shares unavailable.", source_ref=latest_source, core=True)
    legacy_shares = _read_cell(workbook_path, "Valuation", "D196")
    shares = _field(float(legacy_shares), source_ref=f"{workbook_path.name}!Valuation!D196", core=True, unit="m shares") if isinstance(legacy_shares, (int, float)) else diluted
    return {
        "price": _missing("Market price is intentionally not sourced by the ANF legacy adapter fixture.", source_ref=f"{workbook_path.name}!Valuation!D194"),
        "as_of_date": _field(as_of, source_ref=latest_source, core=True) if as_of else _missing("Latest period end is unavailable.", source_ref=latest_source, core=True),
        "shares_outstanding": shares,
        "diluted_shares": diluted,
        "net_debt": dict(debt_liquidity.get("net_debt") or _missing("Net debt unavailable.", source_ref=source_ref, core=True)),
        "base_ebitda_ttm": total("base_ebitda"),
        "adjusted_ebitda_ttm": total("adjusted_ebitda"),
        "revenue_ttm": total("revenue"),
        "net_income_ttm": total("net_income"),
        "operating_cash_flow_ttm": total("operating_cash_flow"),
        "free_cash_flow_ttm": total("free_cash_flow"),
        "capex_ttm": total("capital_expenditures"),
        "target_ev_adjusted_ebitda": _populated_scalar(_read_cell(workbook_path, "Valuation", "D208"), f"{workbook_path.name}!Valuation!D208", "x", "assumption"),
        "target_ev_ebitda": _populated_scalar(_read_cell(workbook_path, "Valuation", "D209"), f"{workbook_path.name}!Valuation!D209", "x", "assumption"),
        "target_ev_yield": _populated_scalar(_read_cell(workbook_path, "Valuation", "D210"), f"{workbook_path.name}!Valuation!D210", "ratio", "assumption"),
        "maintenance_capex_ratio": _populated_scalar(_read_cell(workbook_path, "Valuation", "D213"), f"{workbook_path.name}!Valuation!D213", "ratio", "assumption"),
        "recurring_cash_costs": _populated_scalar(_read_cell(workbook_path, "Valuation", "D214"), f"{workbook_path.name}!Valuation!D214", "$m", "assumption"),
        "working_capital_normalization": _populated_scalar(_read_cell(workbook_path, "Valuation", "D215"), f"{workbook_path.name}!Valuation!D215", "$m", "assumption"),
        "per_share_denominator": _field(str(_read_cell(workbook_path, "Valuation", "D216") or ""), source_ref=f"{workbook_path.name}!Valuation!D216"),
    }


def _build_guidance_items(
    guidance_rows: Sequence[Mapping[str, Any]],
    promise_rows: Sequence[Mapping[str, Any]],
    workbook_path: Path,
    demotions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    clean_rows = [row for row in guidance_rows if _is_present(row.get("metric_hint")) and _is_present(row.get("numbers"))]
    clean_rows.sort(key=lambda row: (_to_iso(row.get("quarter")), str(row.get("metric_hint")), str(row.get("period_label"))))
    progress_by_key: dict[tuple[str, str], Mapping[str, Any]] = {}
    for row in promise_rows:
        key = (str(row.get("metric_display") or row.get("metric_ref") or ""), str(row.get("target_period_label") or ""))
        if key[0] and key[1]:
            progress_by_key[key] = row
    items: list[dict[str, Any]] = []
    for row in clean_rows:
        metric = str(row.get("metric") or row.get("metric_hint") or "").strip()
        horizon = str(row.get("horizon_label") or row.get("period_label") or "").strip()
        progress = progress_by_key.get((f"{metric} guidance", horizon)) or progress_by_key.get((metric, horizon)) or {}
        source_ref = str(row.get("doc") or _source_ref("Guidance_Normalized", row, workbook_path=workbook_path))
        publication_date = _publication_date_from_source(source_ref, row.get("source_date") or row.get("quarter"))
        value_text = str(row.get("numbers") or row.get("value") or "").strip()
        legacy_stated_in = str(row.get("stated_in_label") or "").strip()
        stated_in_period = _normalize_period(publication_date, period_type="quarterly")
        source_line_raw = _clean_text(row.get("line"), limit=260)
        field_prefix = f"normalized_guidance.items.{len(items)}"
        source_line = _visible_text_or_blank(
            source_line_raw,
            field=f"{field_prefix}.source_excerpt",
            section="normalized_guidance",
            source_ref=source_ref,
            demotions=demotions,
        )
        notes_source_raw = _clean_text(progress.get("rationale") or source_line_raw, limit=220)
        notes_source = _visible_text_or_blank(
            notes_source_raw,
            field=f"{field_prefix}.notes_source",
            section="normalized_guidance",
            source_ref=_source_ref("Promise_Progress", progress, workbook_path=workbook_path) if progress else source_ref,
            demotions=demotions,
        )
        items.append(
            {
                "metric": _field(metric, source_ref=source_ref, core=True),
                "value": _field(value_text, source_ref=source_ref, core=True, unit=str(row.get("unit") or "")),
                "horizon": _field(horizon, source_ref=source_ref, core=True),
                "source_excerpt": source_line,
                "source_date": publication_date,
                "publication_date": publication_date,
                "stated_in_period": stated_in_period,
                "legacy_stated_in_label": legacy_stated_in,
                "classification": str(row.get("source_context") or "normalized_outlook"),
                "evidence_key": _evidence_key(source_ref, metric, horizon, value_text, source_line_raw),
                "initial_guide": _missing("No distinct earlier guide was normalized for this evidence row.", source_ref=source_ref),
                "q1_update": _field("", status="missing_source", source_ref=source_ref, reason="Progression update columns are not normalized from legacy artifacts yet."),
                "q2_update": _field("", status="missing_source", source_ref=source_ref, reason="Progression update columns are not normalized from legacy artifacts yet."),
                "q3_update": _field("", status="missing_source", source_ref=source_ref, reason="Progression update columns are not normalized from legacy artifacts yet."),
                "q4_update": _field("", status="missing_source", source_ref=source_ref, reason="Progression update columns are not normalized from legacy artifacts yet."),
                "actual": _field(progress.get("actual"), source_ref=_source_ref("Promise_Progress", progress, workbook_path=workbook_path)) if progress.get("actual") not in (None, "") else _field("", status="missing_source", source_ref=_source_ref("Promise_Progress", progress, workbook_path=workbook_path) if progress else source_ref, reason="No actual/result value is normalized for this guidance row yet."),
                "progress_status": str(progress.get("status") or "open"),
                "notes_source": notes_source,
                "update_stage": "initial" if publication_date[5:7] in {"02", "03"} else "update",
                "display_role": "history",
                "display_priority": 999,
            }
        )
    if items:
        latest_publication = max(str(item["publication_date"]) for item in items)
        current_year = latest_publication[:4]
        pair_priority = {
            ("Revenue", f"{current_year} year"): 1,
            ("Revenue", f"{current_year}-Q1"): 2,
            ("Operating margin", f"{current_year} year"): 3,
            ("Operating margin", f"{current_year}-Q1"): 4,
            ("Adj EPS", f"{current_year} year"): 5,
            ("Adj EPS", f"{current_year}-Q1"): 6,
            ("Real estate activity", f"{current_year} year"): 7,
        }
        candidates = [
            item
            for item in items
            if item["publication_date"] == latest_publication
            and (str(item["metric"].get("value") or ""), str(item["horizon"].get("value") or "")) in pair_priority
        ]
        candidates.sort(
            key=lambda item: (
                pair_priority[(str(item["metric"].get("value") or ""), str(item["horizon"].get("value") or ""))],
                str(item["evidence_key"]),
            )
        )
        seen_keys: set[tuple[str, str]] = set()
        selected = 0
        for item in candidates:
            key = (str(item["metric"].get("value") or ""), str(item["horizon"].get("value") or ""))
            if key in seen_keys or selected >= 7:
                item["display_role"] = "current_secondary"
                continue
            seen_keys.add(key)
            selected += 1
            item["display_role"] = "current_primary"
            item["display_priority"] = pair_priority[key]
    return items


def _build_segments(
    segment_rows: Sequence[Mapping[str, Any]],
    history_rows: Sequence[Mapping[str, Any]],
    workbook_path: Path,
    demotions: list[dict[str, Any]],
) -> dict[str, Any]:
    fiscal_periods = {
        _to_iso(row.get("quarter")): (str(row.get("fiscal_label") or ""), row.get("fiscal_year"))
        for row in history_rows
        if _is_present(row.get("quarter"))
    }
    rows = [
        row
        for row in segment_rows
        if _is_present(row.get("segment")) and _is_present(row.get("metric")) and _is_present(row.get("value"))
    ]
    rows.sort(key=lambda row: (_to_iso(row.get("quarter")), str(row.get("period_type")), str(row.get("segment")), str(row.get("metric"))))
    items: list[dict[str, Any]] = []
    for row in rows:
        metric = str(row.get("metric") or "")
        source_ref = str(row.get("source_doc") or row.get("doc") or _source_ref("Slides_Segments", row, workbook_path=workbook_path))
        value = _to_millions(row.get("value")) if str(row.get("unit") or "").lower() in {"usd", "$", "$m"} else row.get("value")
        note = _visible_text_or_blank(
            _clean_text(row.get("note") or row.get("commentary"), limit=220),
            field=f"segments.items.{len(items)}.note",
            section="segments",
            source_ref=source_ref,
            demotions=demotions,
        )
        member = str(row.get("segment") or "").strip()
        period_type = str(row.get("period_type") or "quarterly").strip().lower()
        if period_type not in {"quarterly", "annual"}:
            period_type = "quarterly"
        source_period_end = _to_iso(row.get("quarter"))
        fiscal_label, fiscal_year = fiscal_periods.get(source_period_end, ("", None))
        normalized_period = f"{fiscal_year}-FY" if period_type == "annual" and fiscal_year not in (None, "") else fiscal_label
        if not normalized_period:
            normalized_period = _normalize_period(row.get("quarter"), period_type=period_type)
        item = {
            "dimension": _segment_dimension(member),
            "member": member,
            "segment": _field(member, source_ref=source_ref, core=True),
            "metric": metric,
            "period": normalized_period,
            "period_type": period_type,
            "source": str(row.get("source") or row.get("source_type") or ""),
            "note": _field(note, source_ref=source_ref) if note else _missing("No concise source-backed segment note survived visible-text quality filtering.", source_ref=source_ref),
        }
        if metric == "revenue" and str(row.get("period_type")) == "annual":
            item["annual_revenue"] = _field(value, source_ref=source_ref, core=True, unit="$m")
            item["revenue"] = _field(value, source_ref=source_ref, unit="$m")
        elif metric == "revenue":
            item["revenue"] = _field(value, source_ref=source_ref, core=True, unit="$m")
        elif "margin" in metric:
            item["margin"] = _field(value, source_ref=source_ref, unit=str(row.get("unit") or ""))
        elif "operating" in metric:
            item["operating_income"] = _field(value, source_ref=source_ref, unit="$m")
        else:
            item["metric_value"] = _field(value, source_ref=source_ref, unit=str(row.get("unit") or ""))
        items.append(item)
    return {"items": items}


def _build_operating_drivers(
    driver_rows: Sequence[Mapping[str, Any]],
    workbook_path: Path,
    demotions: list[dict[str, Any]],
) -> dict[str, Any]:
    rows = [row for row in driver_rows if _is_present(row.get("Driver")) or _is_present(row.get("Commentary"))]
    rows.sort(key=lambda row: (_to_iso(row.get("Quarter")), str(row.get("Driver group")), str(row.get("Driver"))))
    items: list[dict[str, Any]] = []
    for row in rows:
        source_ref = str(row.get("Source") or _source_ref("operating_drivers_raw", row, workbook_path=workbook_path))
        driver = str(row.get("Driver") or row.get("Driver group") or "").strip()
        commentary = _clean_text(row.get("Commentary") or row.get("Value"), limit=260)
        field_prefix = f"operating_drivers.items.{len(items)}"
        if not _visible_text_is_clean(
            commentary,
            field=f"{field_prefix}.current_read",
            section="operating_drivers",
            source_ref=source_ref,
            demotions=demotions,
        ):
            continue
        items.append(
            {
                "topic": _field(str(row.get("Driver group") or ""), source_ref=source_ref),
                "driver": _field(driver, source_ref=source_ref, core=True),
                "current_read": _field(commentary or driver, source_ref=source_ref, core=True),
                "metric_value": _field(row.get("Value"), source_ref=source_ref, unit=str(row.get("Unit") or "")) if _is_present(row.get("Value")) else _missing("Driver row has commentary but no clean metric value.", source_ref=source_ref),
                "source": source_ref,
                "why_it_matters": _field(_clean_text(row.get("Commentary"), limit=220), source_ref=source_ref),
                "quality": str(row.get("Quality") or ""),
                "period": _normalize_period(row.get("Quarter"), period_type="quarterly"),
                "driver_type": _driver_type(str(row.get("Driver group") or ""), driver),
                "evidence_key": _evidence_key(source_ref, row.get("Driver group"), driver, commentary),
                "display_role": "history",
                "display_priority": 999,
            }
        )
    selected_topics: set[str] = set()
    priority = 0
    for item in sorted(items, key=lambda value: (str(value.get("period") or ""), str(value.get("evidence_key") or "")), reverse=True):
        topic = str(item.get("topic", {}).get("value") or "").strip().lower()
        if not topic or topic in selected_topics or priority >= 4:
            continue
        selected_topics.add(topic)
        priority += 1
        item["display_role"] = "current_watchlist"
        item["display_priority"] = priority
    return {"items": items}


def _build_quarter_notes(
    note_rows: Sequence[Mapping[str, Any]],
    latest_source_period: str,
    workbook_path: Path,
    demotions: list[dict[str, Any]],
) -> dict[str, Any]:
    rows = [row for row in note_rows if _is_present(row.get("note")) or _is_present(row.get("renderable_note"))]
    rows.sort(key=lambda row: (_to_iso(row.get("quarter")), int(row.get("rank") or 999), str(row.get("topic") or "")))
    items: list[dict[str, Any]] = []
    for row in rows:
        source_ref = str(row.get("source_doc") or row.get("doc") or row.get("evidence_doc") or _source_ref("Quarter_Notes", row, workbook_path=workbook_path))
        note = _clean_text(row.get("renderable_note") or row.get("note") or row.get("body") or row.get("claim"), limit=300)
        field_prefix = f"quarter_notes.items.{len(items)}"
        if not _visible_text_is_clean(
            note,
            field=f"{field_prefix}.note",
            section="quarter_notes",
            source_ref=source_ref,
            demotions=demotions,
        ):
            continue
        implication = _clean_text(row.get("render_summary") or row.get("render_change"), limit=220)
        if not _visible_text_is_clean(
            implication,
            field=f"{field_prefix}.model_implication",
            section="quarter_notes",
            source_ref=source_ref,
            demotions=demotions,
        ):
            continue
        quarter = _normalize_period(row.get("quarter") or row.get("quarter_end"), period_type="quarterly")
        display_role = "audit_only" if latest_source_period and quarter > latest_source_period else "history"
        if display_role == "audit_only":
            demotions.append(
                {
                    "severity": "P2",
                    "rule_id": "quarter_note_after_available_evidence",
                    "field": f"{field_prefix}.quarter",
                    "message": f"Quarter note period {quarter} is later than available financial evidence {latest_source_period}.",
                    "source_ref": source_ref,
                    "suggested_action": "Keep the row audit-only until period evidence is available.",
                }
            )
        items.append(
            {
                "theme": _field(str(row.get("topic") or row.get("tag") or ""), source_ref=source_ref),
                "quarter": _field(quarter, source_ref=source_ref),
                "metric": _field(str(row.get("metric_ref") or row.get("category") or ""), source_ref=source_ref),
                "note": _field(note, source_ref=source_ref, core=True),
                "commentary": _field(note, source_ref=source_ref),
                "model_implication": _field(implication, source_ref=source_ref),
                "valuation_implication": _field(_clean_text(row.get("render_bucket"), limit=160), source_ref=source_ref) if _clean_text(row.get("render_bucket"), limit=160) else _missing("No source-backed valuation implication was normalized.", source_ref=source_ref),
                "source": source_ref,
                "confidence": str(row.get("confidence") or ""),
                "evidence_key": _evidence_key(source_ref, row.get("quarter") or row.get("quarter_end"), row.get("topic") or row.get("tag"), row.get("metric_ref") or row.get("category"), note),
                "display_role": display_role,
                "display_priority": 999,
                "selection_rank": int(row.get("rank") or 999),
            }
        )
    valid_periods = [str(item["quarter"].get("value") or "") for item in items if item["display_role"] != "audit_only"]
    if valid_periods:
        latest_period = max(valid_periods)
        seen_themes: set[str] = set()
        priority = 0
        for item in sorted(items, key=lambda value: (int(value.get("selection_rank") or 999), str(value.get("evidence_key") or ""))):
            if str(item["quarter"].get("value") or "") != latest_period or item["display_role"] == "audit_only":
                continue
            theme = re.sub(r"[^a-z0-9]+", "", str(item["theme"].get("value") or "").lower())
            if not theme or theme in seen_themes or priority >= 6:
                continue
            seen_themes.add(theme)
            priority += 1
            item["display_role"] = "current_note"
            item["display_priority"] = priority
    return {"items": items}


def _build_investment_case(workbook_path: Path) -> dict[str, Any]:
    summary = _clean_text(_read_cell(workbook_path, "ANF_Investment_Case", "B5"), limit=700)
    driver_points = [
        _clean_text(_read_cell(workbook_path, "SUMMARY", cell), limit=180)
        for cell in ("A17", "A18", "A19", "A20", "A21")
        if _clean_text(_read_cell(workbook_path, "SUMMARY", cell), limit=180)
    ]
    key_debate = _clean_text(_read_cell(workbook_path, "ANF_Investment_Case", "B7"), limit=700)
    source_ref = f"{workbook_path.name}!ANF_Investment_Case"
    return {
        "summary": _field(summary, source_ref=source_ref, core=True),
        "key_debate": _field(key_debate, source_ref=source_ref, core=True),
        "bull_case": _field("", status="manual_review_required", source_ref=source_ref, reason="Bull/base/bear framing is not yet normalized from a source-backed legacy artifact."),
        "base_case": _field("", status="manual_review_required", source_ref=source_ref, reason="Bull/base/bear framing is not yet normalized from a source-backed legacy artifact."),
        "bear_case": _field("", status="manual_review_required", source_ref=source_ref, reason="Bull/base/bear framing is not yet normalized from a source-backed legacy artifact."),
        "scenario_drivers": _field(" ".join(driver_points), source_ref=source_ref),
        "source_evidence": [
            {"source_ref": source_ref, "section": "SUMMARY"},
            {"source_ref": f"{workbook_path.name}!ANF_Investment_Case_Data", "section": "legacy investment-case support"},
        ],
    }


def _collect_legacy_unit_reviews(value: Any, path: str = "$") -> list[dict[str, Any]]:
    reviews: list[dict[str, Any]] = []
    if isinstance(value, Mapping):
        legacy_unit = str(value.get("legacy_unit") or "")
        if legacy_unit:
            reviews.append(
                {
                    "severity": "P2",
                    "rule_id": "legacy_adapter_unit_normalization",
                    "field": path,
                    "message": f"Legacy unit {legacy_unit!r} was normalized to {value.get('unit')!r} for schema compatibility.",
                    "source_ref": str(value.get("source_ref") or ""),
                    "suggested_action": "Confirm the unit taxonomy in a source-native extractor before promotion.",
                }
            )
        for key, child in value.items():
            reviews.extend(_collect_legacy_unit_reviews(child, f"{path}.{key}"))
    elif isinstance(value, list):
        for idx, child in enumerate(value):
            reviews.extend(_collect_legacy_unit_reviews(child, f"{path}.{idx}"))
    return reviews


def build_anf_normalized_package(*, data_root: Path, workbook_path: Path) -> dict[str, Any]:
    """Build ANF shadow data from legacy workbook artifacts for comparison only."""
    history_rows = _read_sheet_rows(workbook_path, "History_Q")
    leverage_rows = _read_sheet_rows(workbook_path, "Leverage_Liquidity")
    guidance_rows = _read_sheet_rows(workbook_path, "Guidance_Normalized")
    promise_rows = _read_sheet_rows(workbook_path, "Promise_Progress")
    segment_rows = _read_sheet_rows(workbook_path, "Slides_Segments")
    driver_rows = _read_sheet_rows(workbook_path, "operating_drivers_raw")
    quarter_note_rows = _read_sheet_rows(workbook_path, "Quarter_Notes")
    text_quality_demotions: list[dict[str, Any]] = []
    adapter_review_flags: list[dict[str, Any]] = []
    adapter_truncations: list[dict[str, Any]] = []
    adapter_deduplications: list[dict[str, Any]] = []
    source_coverage = _source_coverage(data_root, workbook_path)

    quarterly_financials = _limit_legacy_adapter_rows(
        _build_quarterly_financial_rows(history_rows, workbook_path),
        limit=12,
        collection_path="quarterly_financials.rows",
        workbook_path=workbook_path,
        truncations=adapter_truncations,
        review_flags=adapter_review_flags,
    )
    annual_financials = _limit_legacy_adapter_rows(
        _build_annual_financial_rows(history_rows, workbook_path),
        limit=6,
        collection_path="annual_financials.rows",
        workbook_path=workbook_path,
        truncations=adapter_truncations,
        review_flags=adapter_review_flags,
    )
    debt_liquidity = _build_debt_liquidity(history_rows, leverage_rows, workbook_path)
    valuation_inputs = _build_valuation_inputs(quarterly_financials, debt_liquidity, workbook_path)
    guidance_candidates = _dedupe_legacy_adapter_rows(
        _build_guidance_items(guidance_rows, promise_rows, workbook_path, text_quality_demotions),
        collection_path="normalized_guidance.items",
        workbook_path=workbook_path,
        deduplications=adapter_deduplications,
        review_flags=adapter_review_flags,
    )
    guidance_items = _limit_legacy_adapter_rows(
        guidance_candidates,
        limit=30,
        collection_path="normalized_guidance.items",
        workbook_path=workbook_path,
        truncations=adapter_truncations,
        review_flags=adapter_review_flags,
    )
    segment_items = _limit_legacy_adapter_rows(
        _build_segments(segment_rows, history_rows, workbook_path, text_quality_demotions)["items"],
        limit=80,
        collection_path="segments.items",
        workbook_path=workbook_path,
        truncations=adapter_truncations,
        review_flags=adapter_review_flags,
    )
    driver_candidates = _dedupe_legacy_adapter_rows(
        _build_operating_drivers(driver_rows, workbook_path, text_quality_demotions)["items"],
        collection_path="operating_drivers.items",
        workbook_path=workbook_path,
        deduplications=adapter_deduplications,
        review_flags=adapter_review_flags,
    )
    driver_items = _limit_legacy_adapter_rows(
        driver_candidates,
        limit=30,
        collection_path="operating_drivers.items",
        workbook_path=workbook_path,
        truncations=adapter_truncations,
        review_flags=adapter_review_flags,
    )
    latest_source_period = str(quarterly_financials[-1].get("period") or "") if quarterly_financials else ""
    for item in driver_items:
        item["display_role"] = "history"
        item["display_priority"] = 999
    driver_items.extend(_build_legacy_visible_operating_drivers(workbook_path, latest_source_period))
    quarter_note_candidates = _dedupe_legacy_adapter_rows(
        _build_quarter_notes(
            quarter_note_rows,
            latest_source_period,
            workbook_path,
            text_quality_demotions,
        )["items"],
        collection_path="quarter_notes.items",
        workbook_path=workbook_path,
        deduplications=adapter_deduplications,
        review_flags=adapter_review_flags,
    )
    quarter_note_items = _limit_legacy_adapter_rows(
        quarter_note_candidates,
        limit=40,
        collection_path="quarter_notes.items",
        workbook_path=workbook_path,
        truncations=adapter_truncations,
        review_flags=adapter_review_flags,
    )
    quarter_note_items.extend(_build_legacy_visible_quarter_notes(workbook_path, latest_source_period))

    source_ref = f"{workbook_path.name}!SUMMARY"
    package = {
        "package_version": "0.2.0-anf-shadow",
        "generated_at_utc": _now(),
        "stress_test": True,
        "shadow_package": True,
        "ticker_metadata": {
            "ticker": _field("ANF", source_ref="SEC company_tickers + ANF legacy workbook", core=True),
            "exchange": _field("NYSE", source_ref=source_ref, core=True),
            "cik": _field("0001018840", source_ref="sec_cache/ANF/0001018840", core=True),
            "fiscal_year_end": _field("retail fiscal year ending around late January / early February", source_ref=source_ref, core=True),
            "reporting_currency": _field("USD", source_ref=source_ref, core=True),
        },
        "company_profile": {
            "company_name": _field("Abercrombie & Fitch Co.", source_ref=source_ref, core=True),
            "sector": _field("Consumer Discretionary", source_ref=source_ref, core=True),
            "industry": _field("Specialty apparel retail", source_ref=source_ref, core=True),
            "business_description": _field(_clean_text(_read_cell(workbook_path, "SUMMARY", "A3"), limit=600), source_ref=source_ref, core=True),
            "revenue_model": _field("Global omnichannel apparel sales through Abercrombie and Hollister brand families, stores, digital channels, and geographic regions.", source_ref=source_ref, core=True),
            "key_advantages": _field(_clean_text(_read_cell(workbook_path, "SUMMARY", "A7"), limit=500), source_ref=source_ref),
            "key_risks": _field("Fashion demand, merchandise execution, tariffs, inventory/markdown risk, ERP execution, and tougher comparable-sales laps.", source_ref=source_ref),
            "allowed_sector_terms": [
                "Abercrombie",
                "Hollister",
                "APAC",
                "EMEA",
                "Americas",
            ],
        },
        "quarterly_financials": {"rows": quarterly_financials},
        "annual_financials": {"rows": annual_financials},
        "debt_liquidity": debt_liquidity,
        "capital_returns": _build_capital_returns(history_rows, workbook_path),
        "normalized_guidance": {"items": guidance_items},
        "segments": {"items": segment_items},
        "operating_drivers": {"items": driver_items},
        "quarter_notes": {"items": quarter_note_items},
        "investment_case": _build_investment_case(workbook_path),
        "valuation_inputs": valuation_inputs,
        "valuation_outputs": {"items": []},
        "source_coverage": source_coverage,
        "mapping_gaps": [],
        "manual_review_flags": list(text_quality_demotions) + adapter_review_flags,
    }
    unit_reviews = _collect_legacy_unit_reviews(package)
    package["manual_review_flags"].extend(unit_reviews)
    package["source_coverage"]["legacy_adapter_truncations"] = adapter_truncations
    package["source_coverage"]["legacy_adapter_deduplications"] = adapter_deduplications
    package["source_coverage"]["legacy_unit_normalizations"] = unit_reviews
    package["source_coverage"]["text_quality_demotions"] = text_quality_demotions
    package["source_coverage"]["text_quality_summary"] = _text_quality_demotion_summary(text_quality_demotions)
    return package


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
            except (IndexError, ValueError):
                return None
            continue
        return None
    return current


def _indexed_collection_path(path: str) -> tuple[str, str] | None:
    parts = path.split(".")
    for idx, part in enumerate(parts):
        if part == "0" and idx > 0 and idx < len(parts) - 1:
            return ".".join(parts[:idx]), ".".join(parts[idx + 1 :])
    return None


def _field_is_populated(value: Any) -> bool:
    return isinstance(value, Mapping) and str(value.get("status") or "") == "populated" and value.get("value") not in (None, "")


def _field_source_ref(value: Any) -> str:
    if isinstance(value, Mapping):
        return str(value.get("source_ref") or "")
    return ""


def _count_values_for_binding(package: Mapping[str, Any], binding: Mapping[str, Any]) -> tuple[int, int, list[str]]:
    row_schema = binding.get("row_schema") if isinstance(binding.get("row_schema"), list) else []
    normalized_field = str(binding.get("normalized_field") or "")
    refs: set[str] = set()
    populated = 0
    available_rows = 0

    if row_schema:
        collection_path = _collection_path_for_row_schema(binding)
        collection = _path_get(package, collection_path) if collection_path else None
        if isinstance(collection, list):
            for item in collection:
                if not isinstance(item, Mapping):
                    continue
                row_has_value = False
                for column in row_schema:
                    source_field = str(column.get("source_field") or "")
                    value = _path_get(item, source_field)
                    if _field_is_populated(value) or (not isinstance(value, Mapping) and value not in (None, "")):
                        populated += 1
                        row_has_value = True
                    ref = _field_source_ref(value)
                    if ref:
                        refs.add(ref)
                if row_has_value:
                    available_rows += 1
        return populated, available_rows, sorted(refs)

    parsed = _indexed_collection_path(normalized_field)
    if parsed:
        collection_path, field_path = parsed
        collection = _path_get(package, collection_path)
        if isinstance(collection, list):
            for item in collection:
                value = _path_get(item, field_path)
                if _field_is_populated(value):
                    populated += 1
                    refs.add(_field_source_ref(value))
            return populated, populated, sorted(ref for ref in refs if ref)

    value = _path_get(package, normalized_field)
    if _field_is_populated(value):
        return 1, 1, [_field_source_ref(value)] if _field_source_ref(value) else []
    if isinstance(value, list):
        populated = len(value)
        return populated, populated, []
    return 0, 0, []


def _collection_path_for_row_schema(binding: Mapping[str, Any]) -> str:
    collection_path = str(binding.get("row_source") or "")
    if collection_path:
        return collection_path
    normalized_field = str(binding.get("normalized_field") or "")
    parsed = _indexed_collection_path(normalized_field)
    return parsed[0] if parsed else normalized_field


def _target_capacity(binding: Mapping[str, Any]) -> tuple[int, int]:
    min_col, min_row, max_col, max_row = range_boundaries(str(binding.get("target") or "A1:A1"))
    return max_row - min_row + 1, max_col - min_col + 1


def build_binding_coverage_audit(package: Mapping[str, Any], binding_map: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for binding in binding_map:
        values_available, rows_available, source_refs = _count_values_for_binding(package, binding)
        expected_rows, expected_cols = _target_capacity(binding)
        row_schema = binding.get("row_schema") if isinstance(binding.get("row_schema"), list) else []
        would_write = values_available > 0 and str(binding.get("source_policy") or "") != "validation-output"
        reason = ""
        if not would_write:
            reason = "validation output binding" if str(binding.get("source_policy") or "") == "validation-output" else "normalized field is absent or not populated"
        rows.append(
            {
                "binding_id": binding.get("binding_id", ""),
                "sheet": binding.get("sheet", ""),
                "section": binding.get("section", ""),
                "target": binding.get("target", ""),
                "normalized_field": binding.get("normalized_field", ""),
                "value_shape": binding.get("value_shape", ""),
                "required": bool(binding.get("required")),
                "row_schema_columns": [str(column.get("column_id") or "") for column in row_schema],
                "has_populated_data": values_available > 0,
                "number_of_values_available": values_available,
                "number_of_rows_available": rows_available,
                "number_of_rows_expected": expected_rows,
                "number_of_cells_expected": expected_rows * expected_cols,
                "source_ref_coverage": source_refs,
                "would_write_useful_output": would_write,
                "blank_reason": reason,
            }
        )
    return {
        "version": "0.1.0",
        "generated_at_utc": _now(),
        "ticker": "ANF",
        "bindings": rows,
        "summary": {
            "binding_count": len(rows),
            "bindings_with_populated_data": sum(1 for row in rows if row["has_populated_data"]),
            "bindings_that_would_write_useful_output": sum(1 for row in rows if row["would_write_useful_output"]),
        },
    }


def build_source_audit(package: Mapping[str, Any], *, data_root: Path, workbook_path: Path) -> dict[str, Any]:
    source_coverage = package.get("source_coverage", {})
    family_counts = source_coverage.get("family_counts", {}) if isinstance(source_coverage, Mapping) else {}
    section_sources = {
        "ticker_metadata": ["SEC company_tickers", "sec_cache/ANF/0001018840", "ANF_model.xlsx!SUMMARY"],
        "company_profile": ["ANF_model.xlsx!SUMMARY", "company profile configuration", "earnings release About section"],
        "quarterly_financials": ["ANF_model.xlsx!History_Q", "SEC/XBRL cache", "earnings release financial schedules"],
        "annual_financials": ["ANF_model.xlsx!History_Q aggregated by fiscal_year", "annual reports", "earnings release annual schedules"],
        "debt_liquidity": ["ANF_model.xlsx!Leverage_Liquidity", "ANF_model.xlsx!History_Q", "ANF_model.xlsx!Slides_Debt_Profile"],
        "capital_returns": ["ANF_model.xlsx!History_Q", "earnings release capital allocation text"],
        "normalized_guidance": ["ANF_model.xlsx!Guidance_Normalized", "ANF_model.xlsx!Promise_Progress", "earnings releases", "transcripts"],
        "segments": ["ANF_model.xlsx!Slides_Segments", "earnings release segment tables", "presentation tables"],
        "operating_drivers": ["ANF_model.xlsx!operating_drivers_raw", "transcripts", "earnings presentations"],
        "quarter_notes": ["ANF_model.xlsx!Quarter_Notes", "ANF_model.xlsx!Quarter_Notes_Evidence"],
        "investment_case": ["ANF_model.xlsx!SUMMARY", "ANF_model.xlsx!ANF_Investment_Case_Data"],
        "valuation_outputs": ["explicit normalized valuation output builder (not available in the ANF legacy adapter fixture)"],
        "source_coverage": ["StockModelData/tickers/ANF", "StockModelData/sec_cache/ANF"],
        "mapping_gaps": ["docs/workbook_binding_map.json", "normalized package"],
        "manual_review_flags": ["pre-render validation", "mapping gap report"],
    }
    sections: list[dict[str, Any]] = []
    for section in REQUIRED_SECTIONS:
        populated_count = _section_populated_count(package.get(section))
        sections.append(
            {
                "section": section,
                "available_source_candidates": section_sources[section],
                "source_backed_available": populated_count > 0 and section not in {"mapping_gaps", "manual_review_flags"},
                "profile_backed_available": section in {"ticker_metadata", "company_profile", "investment_case"},
                "legacy_workbook_derived_available": any("ANF_model.xlsx" in source for source in section_sources[section]),
                "missing_source": populated_count == 0 and section not in {"mapping_gaps", "manual_review_flags"},
                "missing_mapping": False,
                "parser_conflict": False,
                "manual_review_required": section in {"investment_case", "mapping_gaps", "manual_review_flags"},
                "populated_field_count": populated_count,
            }
        )
    return {
        "version": "0.1.0",
        "generated_at_utc": _now(),
        "ticker": "ANF",
        "data_root": str(data_root),
        "legacy_workbook": str(workbook_path),
        "source_family_counts": family_counts,
        "sections": sections,
    }


def build_anf_text_quality_audit(package: Mapping[str, Any]) -> dict[str, Any]:
    audit = build_normalized_text_quality_audit(package)
    source_coverage = package.get("source_coverage", {}) if isinstance(package, Mapping) else {}
    demotions = (
        source_coverage.get("text_quality_demotions", [])
        if isinstance(source_coverage, Mapping) and isinstance(source_coverage.get("text_quality_demotions"), list)
        else []
    )
    demotion_summary = _text_quality_demotion_summary(demotions)
    visible_clean_by_section: dict[str, int] = defaultdict(int)
    visible_non_clean_by_section: dict[str, int] = defaultdict(int)
    for row in audit["rows"]:
        if not row.get("visible_ui"):
            continue
        section = str(row.get("section") or "unknown")
        if row.get("classification") == "clean_visible_ui":
            visible_clean_by_section[section] += 1
        else:
            visible_non_clean_by_section[section] += 1
    before_after: dict[str, dict[str, int]] = {}
    for section in sorted(set(visible_clean_by_section) | set(visible_non_clean_by_section) | set(demotion_summary["by_section"])):
        demoted = int(demotion_summary["by_section"].get(section, 0))
        after_clean = int(visible_clean_by_section.get(section, 0))
        before_after[section] = {
            "candidate_rows_before_filter": after_clean + demoted + int(visible_non_clean_by_section.get(section, 0)),
            "visible_clean_after_filter": after_clean,
            "visible_non_clean_after_filter": int(visible_non_clean_by_section.get(section, 0)),
            "demoted_before_render": demoted,
        }
    return {
        **audit,
        "ticker": "ANF",
        "generated_at_utc": _now(),
        "demotion_summary": demotion_summary,
        "before_after_summary": before_after,
        "demotions": list(demotions),
    }


def _section_populated_count(obj: Any) -> int:
    count = 0
    if isinstance(obj, Mapping):
        if _field_is_populated(obj):
            return 1
        for value in obj.values():
            count += _section_populated_count(value)
    elif isinstance(obj, list):
        for value in obj:
            count += _section_populated_count(value)
    return count


def _markdown_source_audit(audit: Mapping[str, Any]) -> str:
    lines = [
        "# ANF Normalized Package Source Audit",
        "",
        "Read-only audit for the ANF shadow normalized-data package. This document does not define workbook rendering behavior.",
        "",
        f"- Generated: `{audit['generated_at_utc']}`",
        f"- Legacy workbook: `{audit['legacy_workbook']}`",
        "",
        "| Section | Classification | Source candidates | Populated fields |",
        "| --- | --- | --- | ---: |",
    ]
    for row in audit["sections"]:
        classes = [
            label
            for key, label in (
                ("source_backed_available", "source-backed available"),
                ("profile_backed_available", "profile-backed available"),
                ("legacy_workbook_derived_available", "legacy-workbook-derived available"),
                ("missing_source", "missing source"),
                ("missing_mapping", "missing mapping"),
                ("parser_conflict", "parser conflict"),
                ("manual_review_required", "manual review required"),
            )
            if row.get(key)
        ]
        lines.append(
            f"| `{row['section']}` | {', '.join(classes) or 'none'} | {'; '.join(row['available_source_candidates'])} | {row['populated_field_count']} |"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- ANF shadow data is read from saved source/workbook artifacts only.",
            "- Missing data remains a mapping gap or manual-review item; no generic filler text is introduced.",
            "- Real workbook rendering is intentionally out of scope for this pass.",
            "",
        ]
    )
    return "\n".join(lines)


def _markdown_binding_coverage(audit: Mapping[str, Any]) -> str:
    lines = [
        "# ANF Binding Coverage Audit",
        "",
        "Coverage check for how the ANF shadow normalized package maps to the current workbook binding map.",
        "",
        f"- Generated: `{audit['generated_at_utc']}`",
        f"- Bindings with populated data: `{audit['summary']['bindings_with_populated_data']}` / `{audit['summary']['binding_count']}`",
        f"- Bindings that would write useful output: `{audit['summary']['bindings_that_would_write_useful_output']}`",
        "",
        "| Binding | Sheet | Field | Values | Rows | Would write | Reason if blank |",
        "| --- | --- | --- | ---: | ---: | --- | --- |",
    ]
    for row in audit["bindings"]:
        lines.append(
            f"| `{row['binding_id']}` | `{row['sheet']}` | `{row['normalized_field']}` | {row['number_of_values_available']} | {row['number_of_rows_available']}/{row['number_of_rows_expected']} | {row['would_write_useful_output']} | {row['blank_reason']} |"
        )
    lines.extend(
        [
            "",
            "## Row Schema Observation",
            "",
            "Table-row bindings now expose row-schema columns in the JSON binding map. The ANF shadow package populates enough row-shaped data to audit whether future filler output would be useful, without creating an ANF workbook in this pass.",
            "",
        ]
    )
    return "\n".join(lines)


def _markdown_text_quality_audit(audit: Mapping[str, Any]) -> str:
    lines = [
        "# ANF Normalized Text Quality Audit",
        "",
        "Read-only text-quality audit for the ANF shadow normalized package. Non-renderable snippets are demoted to manual review/source coverage rather than copied into visible UI fields.",
        "",
        f"- Generated: `{audit['generated_at_utc']}`",
        f"- Audited text rows: `{audit['row_count']}`",
        f"- Non-clean visible rows after filtering: `{audit['non_clean_visible_count']}`",
        f"- Demoted rows before render: `{audit['demotion_summary']['total_demoted']}`",
        "",
        "## Before / After By Section",
        "",
        "| Section | Candidate rows before filter | Visible clean after filter | Visible non-clean after filter | Demoted before render |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for section, row in audit["before_after_summary"].items():
        lines.append(
            f"| `{section}` | {row['candidate_rows_before_filter']} | {row['visible_clean_after_filter']} | {row['visible_non_clean_after_filter']} | {row['demoted_before_render']} |"
        )
    lines.extend(
        [
            "",
            "## Demotions",
            "",
            "| Field | Classification | Source | Action | Excerpt |",
            "| --- | --- | --- | --- | --- |",
        ]
    )
    for row in audit["demotions"]:
        excerpt = str(row.get("original_excerpt") or "").replace("|", "\\|")
        lines.append(
            f"| `{row.get('field', '')}` | `{row.get('classification', '')}` | `{row.get('source_ref', '')}` | {row.get('suggested_action', '')} | {excerpt} |"
        )
    if len(audit["demotions"]) > 120:
        lines.append(f"| ... | ... | ... | ... | {len(audit['demotions']) - 120} additional demotions omitted from markdown; see JSON. |")
    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- Clean source-backed text may remain in visible normalized fields.",
            "- Boilerplate, legal text, governance/compensation snippets, definitions, source headers, fragments, and overlong snippets stay audit-only/manual-review until normalized explicitly.",
            "- This audit does not create or fill any workbook.",
            "",
        ]
    )
    return "\n".join(lines)


def build_anf_shadow_outputs(
    *,
    data_root: Path,
    workbook_path: Path,
    output_dir: Path,
    docs_dir: Path,
    binding_map_path: Path | None = None,
) -> dict[str, Path]:
    binding_path = binding_map_path or (REPO_ROOT / "docs" / "workbook_binding_map.json")
    binding_map = list(_load_json(binding_path).get("bindings") or [])
    package = build_anf_normalized_package(data_root=data_root, workbook_path=workbook_path)
    raw_mapping_report = build_mapping_gap_report(package, binding_map, ticker="ANF")
    mapping_gaps = [
        gap
        for gap in raw_mapping_report.get("gaps", [])
        if str(gap.get("source_policy") or "") != "validation-output"
    ]
    mapping_report = {
        **raw_mapping_report,
        "gap_count": len(mapping_gaps),
        "gaps": mapping_gaps,
        "excluded_validation_output_gap_count": len(raw_mapping_report.get("gaps", [])) - len(mapping_gaps),
    }
    validation_issues = validate_normalized_company_data(package, binding_map=binding_map, promotion_requested=False)
    validation_report = {
        "ticker": "ANF",
        "promotion_requested": False,
        "issue_count": len(validation_issues),
        "issues": [issue.to_dict() for issue in validation_issues],
        "text_quality_demotion_count": len(
            package.get("source_coverage", {}).get("text_quality_demotions", [])
        ),
    }

    package_out = dict(package)
    package_out["mapping_gaps"] = mapping_report["gaps"]
    package_out["manual_review_flags"] = list(package.get("manual_review_flags", [])) + validation_report["issues"]
    source_audit = build_source_audit(package_out, data_root=data_root, workbook_path=workbook_path)
    binding_coverage = build_binding_coverage_audit(package_out, binding_map)
    text_quality_audit = build_anf_text_quality_audit(package_out)

    paths = {
        "package": output_dir / "ANF_normalized_data_package.json",
        "mapping_gaps": output_dir / "ANF_mapping_gaps_report.json",
        "validation": output_dir / "ANF_content_validation_report.json",
        "source_audit_json": docs_dir / "anf_normalized_package_source_audit.json",
        "source_audit_md": docs_dir / "anf_normalized_package_source_audit.md",
        "binding_coverage_json": docs_dir / "anf_binding_coverage_audit.json",
        "binding_coverage_md": docs_dir / "anf_binding_coverage_audit.md",
        "text_quality_json": docs_dir / "anf_normalized_text_quality_audit.json",
        "text_quality_md": docs_dir / "anf_normalized_text_quality_audit.md",
    }
    _write_json(paths["package"], package_out)
    _write_json(paths["mapping_gaps"], mapping_report)
    _write_json(paths["validation"], validation_report)
    _write_json(paths["source_audit_json"], source_audit)
    _write_text(paths["source_audit_md"], _markdown_source_audit(source_audit))
    _write_json(paths["binding_coverage_json"], binding_coverage)
    _write_text(paths["binding_coverage_md"], _markdown_binding_coverage(binding_coverage))
    _write_json(paths["text_quality_json"], text_quality_audit)
    _write_text(paths["text_quality_md"], _markdown_text_quality_audit(text_quality_audit))
    return paths


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=_default_data_root())
    parser.add_argument("--workbook", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--docs-dir", type=Path, default=REPO_ROOT / "docs")
    parser.add_argument("--binding-map", type=Path, default=REPO_ROOT / "docs" / "workbook_binding_map.json")
    args = parser.parse_args(argv)

    data_root = args.data_root.expanduser().resolve()
    workbook_path = (args.workbook or _default_workbook_path(data_root)).expanduser().resolve()
    output_dir = (args.output_dir or _default_output_dir(data_root)).expanduser().resolve()
    docs_dir = args.docs_dir.expanduser().resolve()
    paths = build_anf_shadow_outputs(
        data_root=data_root,
        workbook_path=workbook_path,
        output_dir=output_dir,
        docs_dir=docs_dir,
        binding_map_path=args.binding_map,
    )
    for label, path in paths.items():
        print(f"{label}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
