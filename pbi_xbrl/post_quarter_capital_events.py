"""Narrow parsers for source-backed post-quarter capital-structure events."""
from __future__ import annotations

import hashlib
import html
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

from .debt_detail_lineage import (
    DEBT_DETAIL_LINEAGE_DISPOSITION_COLUMN,
    DebtDetailLineageDisposition,
    normalize_debt_detail_lineage_dispositions,
)


POST_QUARTER_EVENT_COLUMNS = (
    "ticker",
    "event_key",
    "event_type",
    "reported_quarter_anchor",
    "event_date",
    "filing_type",
    "filing_date",
    "downloaded_at",
    "accession",
    "principal_redeemed",
    "incremental_term_loan",
    "term_loan_total",
    "gross_principal_delta",
    "next_scheduled_maturity",
    "term_loan_maturity",
    "warrants_issued",
    "potential_common_shares_issuable_max",
    "exercise_price",
    "expiration_date",
    "beneficial_ownership_limitation",
    "automatic_net_debt_adjustment",
    "history_treatment",
    "valuation_treatment",
    "used_in_workbook",
    "used_surfaces",
    "source_documents",
    "source_paths",
    "source_urls",
    "source_path_exists",
    "qa_status",
)

_TEXT_EXTENSIONS = {".htm", ".html", ".txt", ".xml"}
_EVENT_IDENTITIES = {
    "PBI": {
        "accession": "000119312526281893",
        "filename_token": "d88573",
        "filing_type": "8-K package / exhibits",
        "filing_date": "2026-06-25",
    },
    "GPRE": {
        "accession": "000110465926076397",
        "filename_token": "tm2618355",
        "filing_type": "S-3ASR / warrant exhibits",
        "filing_date": "2026-06-22",
    },
}


def _empty_events() -> pd.DataFrame:
    return pd.DataFrame(columns=POST_QUARTER_EVENT_COLUMNS)


def _normalize_accession(value: Any) -> str:
    return re.sub(r"\D+", "", str(value or ""))


def _plain_text(value: str) -> str:
    text = re.sub(r"(?is)<script\b.*?</script>", " ", value)
    text = re.sub(r"(?is)<style\b.*?</style>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _read_text(path: Path) -> str:
    try:
        raw = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return ""
    return _plain_text(raw)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError:
        return ""
    return digest.hexdigest()


def _candidate_paths(
    *,
    ticker: str,
    material_roots: Sequence[Path | str],
    cache_roots: Sequence[Path | str],
) -> list[Path]:
    identity = _EVENT_IDENTITIES[ticker]
    filename_token = str(identity["filename_token"]).lower()
    candidates: list[Path] = []
    seen_hashes: set[str] = set()
    seen_paths: set[str] = set()

    for root_value in [*material_roots, *cache_roots]:
        root = Path(root_value).expanduser()
        if not root.exists():
            continue
        paths = [root] if root.is_file() else root.rglob("*")
        for path in paths:
            if not path.is_file() or path.suffix.lower() not in _TEXT_EXTENSIONS:
                continue
            name_token = path.name.lower()
            if filename_token not in name_token:
                continue
            resolved = str(path.resolve()).casefold()
            if resolved in seen_paths:
                continue
            seen_paths.add(resolved)
            content_hash = _sha256(path)
            if content_hash and content_hash in seen_hashes:
                continue
            if content_hash:
                seen_hashes.add(content_hash)
            candidates.append(path.resolve())
    return sorted(candidates, key=lambda item: str(item).casefold())


def _load_refresh_metadata(
    *,
    ticker: str,
    accession: str,
    source_refresh_logs: Sequence[Path | str],
) -> dict[str, Any]:
    accession_key = _normalize_accession(accession)
    matched_records: list[Mapping[str, Any]] = []
    downloaded_at = ""
    for log_value in source_refresh_logs:
        path = Path(log_value).expanduser()
        if not path.is_file():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, Mapping):
            continue
        records = payload.get("records")
        if not isinstance(records, list):
            continue
        current_matches = [
            record
            for record in records
            if isinstance(record, Mapping)
            and str(record.get("ticker") or "").upper() == ticker
            and _normalize_accession(record.get("accession")) == accession_key
        ]
        if not current_matches:
            continue
        matched_records.extend(current_matches)
        candidate_downloaded_at = str(payload.get("downloaded_at") or "").strip()
        if candidate_downloaded_at and candidate_downloaded_at > downloaded_at:
            downloaded_at = candidate_downloaded_at

    source_urls = sorted(
        {
            str(record.get("source_url") or "").strip()
            for record in matched_records
            if str(record.get("source_url") or "").strip()
        }
    )
    filing_dates = sorted(
        {
            str(record.get("filing_date") or "").strip()
            for record in matched_records
            if str(record.get("filing_date") or "").strip()
        }
    )
    return {
        "downloaded_at": downloaded_at,
        "filing_date": filing_dates[-1] if filing_dates else "",
        "source_urls": source_urls,
    }


def _fallback_downloaded_at(paths: Sequence[Path]) -> str:
    timestamps: list[float] = []
    for path in paths:
        try:
            timestamps.append(path.stat().st_mtime)
        except OSError:
            continue
    if not timestamps:
        return ""
    return datetime.fromtimestamp(max(timestamps), tz=timezone.utc).isoformat()


def _contains_all(text: str, terms: Iterable[str]) -> bool:
    lowered = text.lower()
    return all(term.lower() in lowered for term in terms)


def _build_common_source_fields(
    *,
    ticker: str,
    paths: Sequence[Path],
    source_refresh_logs: Sequence[Path | str],
) -> dict[str, Any]:
    identity = _EVENT_IDENTITIES[ticker]
    refresh = _load_refresh_metadata(
        ticker=ticker,
        accession=str(identity["accession"]),
        source_refresh_logs=source_refresh_logs,
    )
    source_paths = [str(path) for path in paths]
    return {
        "filing_type": identity["filing_type"],
        "filing_date": refresh["filing_date"] or identity["filing_date"],
        "downloaded_at": refresh["downloaded_at"] or _fallback_downloaded_at(paths),
        "accession": identity["accession"],
        "source_documents": " | ".join(path.name for path in paths),
        "source_paths": " | ".join(source_paths),
        "source_urls": " | ".join(refresh["source_urls"]),
        "source_path_exists": bool(source_paths) and all(path.exists() for path in paths),
    }


def _parse_pbi_event(
    paths: Sequence[Path],
    *,
    source_refresh_logs: Sequence[Path | str],
) -> dict[str, Any] | None:
    selected_paths = [
        path
        for path in paths
        if any(token in path.name.lower() for token in ("d88573d8k.htm", "d88573dex101.htm", "d88573dex991.htm"))
    ]
    text = " ".join(_read_text(path) for path in selected_paths)
    lowered = text.lower()
    required_context = (
        "347 million" in lowered
        and "150 million" in lowered
        and "302 million" in lowered
        and "term loan a" in lowered
        and ("redeem" in lowered or "redemption" in lowered)
        and "march 2029" in lowered
        and "may 18, 2031" in lowered
    )
    if not required_context:
        return None

    common = _build_common_source_fields(
        ticker="PBI",
        paths=selected_paths,
        source_refresh_logs=source_refresh_logs,
    )
    return {
        "ticker": "PBI",
        "event_key": "PBI|2026-06-23|refinancing_redemption",
        "event_type": "refinancing_redemption",
        "reported_quarter_anchor": "2026-Q1",
        "event_date": "2026-06-23",
        **common,
        "principal_redeemed": 347_000_000.0,
        "incremental_term_loan": 150_000_000.0,
        "term_loan_total": 302_000_000.0,
        "gross_principal_delta": -197_000_000.0,
        "next_scheduled_maturity": "March 2029",
        "term_loan_maturity": "2031-05-18",
        "warrants_issued": pd.NA,
        "potential_common_shares_issuable_max": pd.NA,
        "exercise_price": pd.NA,
        "expiration_date": "",
        "beneficial_ownership_limitation": pd.NA,
        "automatic_net_debt_adjustment": False,
        "history_treatment": "History_Q unchanged; Debt_Profile unchanged; Debt_Tranches_Latest unchanged",
        "valuation_treatment": "Current Debt Detail updated; no auto net-debt adjustment",
        "used_in_workbook": "Yes",
        "used_surfaces": "Valuation current Debt Detail | Investment_Case | Support/Audit",
        "qa_status": "source_backed_principal; cash_and_transaction_costs_unresolved",
    }


def _extract_warrant_amount(text: str) -> int | None:
    patterns = (
        r"number\s+of\s+warrants\s*[:\-]?\s*([0-9][0-9,]*)",
        r"number\s+of\s+warrant\s+shares\s*[:\-]?\s*([0-9][0-9,]*)",
        r"warrant\s+shares\s*[:\-]?\s*([0-9][0-9,]*)",
    )
    lowered = text.lower()
    for pattern in patterns:
        match = re.search(pattern, lowered, flags=re.IGNORECASE)
        if not match:
            continue
        try:
            return int(match.group(1).replace(",", ""))
        except ValueError:
            continue
    return None


def _parse_gpre_event(
    paths: Sequence[Path],
    *,
    source_refresh_logs: Sequence[Path | str],
) -> dict[str, Any] | None:
    selected_paths = [
        path
        for path in paths
        if (
            "s3asr" in path.name.lower()
            or "ex10-2" in path.name.lower()
            or "ex4-4" in path.name.lower()
        )
    ]
    records = [(path, _read_text(path)) for path in selected_paths]
    purchase_text = " ".join(
        text
        for path, text in records
        if "ex10-2" in path.name.lower() or _contains_all(text, ("warrants", "500,000"))
    )
    if not _contains_all(purchase_text, ("warrant", "500,000")):
        return None

    warrant_amounts: list[int] = []
    warrant_texts: list[str] = []
    for path, text in records:
        if (
            "ex4-4" not in path.name.lower()
            and "number of warrant shares" not in text.lower()
            and "number of warrants" not in text.lower()
        ):
            continue
        amount = _extract_warrant_amount(text)
        if amount is None:
            continue
        warrant_amounts.append(amount)
        warrant_texts.append(text)
    if sorted(set(warrant_amounts)) != sorted((366_240, 37_120, 10_360, 86_280)):
        return None

    prospectus_text = " ".join(
        text
        for path, text in records
        if "s3asr" in path.name.lower()
        or _contains_all(text, ("550,000", "shares", "issuable"))
    )
    max_share_match = re.search(
        r"(?:up\s+to\s+)?550,000\s+shares.{0,180}issuable\s+(?:upon|on\s+the)\s+exercise",
        prospectus_text,
        flags=re.IGNORECASE,
    )
    maximum_context = (
        "maximum number of shares" in prospectus_text.lower()
        and (
            "without regard to the beneficial ownership limitation" in prospectus_text.lower()
            or (
                "without regard to any limitations" in prospectus_text.lower()
                and "beneficial ownership limitation" in prospectus_text.lower()
            )
        )
    )
    if not max_share_match or not maximum_context:
        return None

    warrant_blob = " ".join(warrant_texts)
    exercise_match = re.search(
        r"(?:exercise\s+price\s*[:\-]?\s*|price\s+of\s*)\$\s*([0-9]+(?:\.[0-9]+)?)",
        warrant_blob,
        flags=re.IGNORECASE,
    )
    expiration_match = re.search(
        r"(?:expiration\s+date.{0,50}june\s+16,\s+2036|june\s+16,\s+2036.{0,50}expiration\s+date)",
        warrant_blob,
        flags=re.IGNORECASE,
    )
    ownership_match = re.search(
        r"beneficial\s+ownership\s+limitation[^0-9]{0,30}([0-9]+(?:\.[0-9]+)?)\s*%",
        warrant_blob,
        flags=re.IGNORECASE,
    )
    if not exercise_match or not expiration_match or not ownership_match:
        return None

    common = _build_common_source_fields(
        ticker="GPRE",
        paths=selected_paths,
        source_refresh_logs=source_refresh_logs,
    )
    return {
        "ticker": "GPRE",
        "event_key": "GPRE|2026-06-16|warrant_dilution",
        "event_type": "warrant_dilution",
        "reported_quarter_anchor": "2026-Q1",
        "event_date": "2026-06-16",
        **common,
        "principal_redeemed": pd.NA,
        "incremental_term_loan": pd.NA,
        "term_loan_total": pd.NA,
        "gross_principal_delta": pd.NA,
        "next_scheduled_maturity": "",
        "term_loan_maturity": "",
        "warrants_issued": float(sum(set(warrant_amounts))),
        "potential_common_shares_issuable_max": 550_000.0,
        "exercise_price": float(exercise_match.group(1)),
        "expiration_date": "2036-06-16",
        "beneficial_ownership_limitation": float(ownership_match.group(1)) / 100.0,
        "automatic_net_debt_adjustment": False,
        "history_treatment": "History_Q shares/EPS unchanged",
        "valuation_treatment": "Full-dilution sensitivity uses +0.550m shares",
        "used_in_workbook": "Yes",
        "used_surfaces": "Valuation full-dilution sensitivity | Investment_Case | Support/Audit",
        "qa_status": "source_backed",
    }


def build_post_quarter_capital_events(
    *,
    ticker: str,
    material_roots: Sequence[Path | str],
    cache_roots: Sequence[Path | str] = (),
    source_refresh_logs: Sequence[Path | str] = (),
) -> pd.DataFrame:
    """Return one normalized capital event for a supported ticker, if complete."""

    ticker_key = str(ticker or "").strip().upper()
    if ticker_key not in _EVENT_IDENTITIES:
        return _empty_events()

    paths = _candidate_paths(
        ticker=ticker_key,
        material_roots=material_roots,
        cache_roots=cache_roots,
    )
    if not paths:
        return _empty_events()

    if ticker_key == "PBI":
        event = _parse_pbi_event(paths, source_refresh_logs=source_refresh_logs)
    else:
        event = _parse_gpre_event(paths, source_refresh_logs=source_refresh_logs)
    if event is None:
        return _empty_events()

    frame = pd.DataFrame([{column: event.get(column, pd.NA) for column in POST_QUARTER_EVENT_COLUMNS}])
    for column in ("automatic_net_debt_adjustment", "source_path_exists"):
        frame[column] = frame[column].astype(object)
    return frame


def apply_pbi_current_debt_overlay(
    reported_tranches: pd.DataFrame,
    event: Mapping[str, Any] | pd.Series,
) -> pd.DataFrame:
    """Return a current-display copy without changing reported tranche history."""

    current = (
        reported_tranches.copy(deep=True)
        if isinstance(reported_tranches, pd.DataFrame)
        else pd.DataFrame()
    )
    if current.empty:
        return current

    ticker = str(event.get("ticker") or "").strip().upper()
    event_type = str(event.get("event_type") or "").strip()
    redeemed = pd.to_numeric(event.get("principal_redeemed"), errors="coerce")
    term_loan_total = pd.to_numeric(event.get("term_loan_total"), errors="coerce")
    if (
        ticker != "PBI"
        or event_type != "refinancing_redemption"
        or pd.isna(redeemed)
        or pd.isna(term_loan_total)
    ):
        return current

    if "tranche_name" not in current.columns:
        return current

    tranche_names = current["tranche_name"].astype(str)
    redeemed_mask = (
        tranche_names.str.contains("2027", case=False, na=False)
        & tranche_names.str.contains("notes|6\\.875", case=False, na=False, regex=True)
    )
    current = current.loc[~redeemed_mask].copy()

    current_names = current["tranche_name"].astype(str)
    term_loan_mask = current_names.str.contains(
        r"\bterm\s+loan\s+a\b",
        case=False,
        na=False,
        regex=True,
    )
    generic_term_loan_mask = current_names.str.contains(
        r"\bterm\s+loan\b",
        case=False,
        na=False,
        regex=True,
    )
    if "maturity_year" in current.columns:
        generic_term_loan_mask &= pd.to_numeric(
            current["maturity_year"],
            errors="coerce",
        ).eq(2028)
    elif "amount_principal" in current.columns:
        generic_term_loan_mask &= pd.to_numeric(
            current["amount_principal"],
            errors="coerce",
        ).between(145_000_000.0, 160_000_000.0)
    term_loan_mask |= generic_term_loan_mask
    if not term_loan_mask.any():
        new_row = {column: pd.NA for column in current.columns}
        new_row["tranche_name"] = "Term Loan A"
        current = pd.concat([current, pd.DataFrame([new_row])], ignore_index=True)
        term_loan_mask = current["tranche_name"].astype(str).str.contains(
            r"\bterm\s+loan\s+a\b",
            case=False,
            na=False,
            regex=True,
        )

    column_defaults = {
        "amount_principal": float("nan"),
        "maturity_display": None,
        "maturity_year": float("nan"),
        "near_term": False,
        "source_kind": None,
        "source_basis": None,
    }
    for column, default in column_defaults.items():
        if column not in current.columns:
            current[column] = pd.Series(
                [default] * len(current),
                index=current.index,
                dtype=object,
            )
    for column in ("maturity_display", "source_kind", "source_basis"):
        current[column] = current[column].astype(object).where(
            current[column].notna(),
            None,
        )
    current["near_term"] = current["near_term"].fillna(False).astype(object)

    current.loc[term_loan_mask, "amount_principal"] = float(term_loan_total)
    current.loc[term_loan_mask, "tranche_name"] = "Term Loan A"
    current.loc[term_loan_mask, "maturity_display"] = "May 18, 2031"
    current.loc[term_loan_mask, "maturity_year"] = 2031
    current.loc[term_loan_mask, "near_term"] = False
    current.loc[term_loan_mask, "source_kind"] = "PostQuarter_Capital_Events"
    current.loc[term_loan_mask, "source_basis"] = "current_principal_overlay"
    current.loc[
        term_loan_mask,
        DEBT_DETAIL_LINEAGE_DISPOSITION_COLUMN,
    ] = DebtDetailLineageDisposition.NOT_APPLICABLE
    return normalize_debt_detail_lineage_dispositions(current.reset_index(drop=True))
