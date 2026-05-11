"""Derivative and OCI bridge extraction for GPRE-style hedge disclosures.

The bridge is an audit/memo surface, not a production earnings adjustment. It
keeps four concepts separate:

- income-statement derivative/hedge P&L,
- OCI/AOCI movement and AOCI reclassification,
- period-end derivative assets and liabilities,
- open notional exposure by commodity/instrument.

Workbook writers and QA rely on that separation so OCI or balance-sheet
exposure never leaks into current-quarter margin or valuation math.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd


# Stable dataframe contract for the Derivative_OCI_Bridge sheet and downstream
# diagnostic builders. Add new fields here only when the writer/readback surface
# should expose them explicitly.
DERIVATIVE_OCI_BRIDGE_COLUMNS: List[str] = [
    "quarter",
    "source_period_type",
    "quarterization_status",
    "quarterization_note",
    "reported_net_income_usd",
    "reported_diluted_eps",
    "reported_adjusted_ebitda_usd",
    "derivative_unrealized_gain_loss_pnl_usd",
    "derivative_realized_gain_loss_pnl_usd",
    "non_designated_derivative_pnl_revenue_usd",
    "non_designated_derivative_pnl_cogs_usd",
    "non_designated_derivative_pnl_total_usd",
    "derivative_gain_loss_pnl_total_usd",
    "derivative_gain_loss_revenue_usd",
    "derivative_gain_loss_cogs_usd",
    "derivative_gain_loss_other_income_usd",
    "derivative_pnl_per_gal_memo",
    "derivative_pnl_tax_effect_usd",
    "derivative_pnl_after_tax_usd",
    "earnings_excluding_unrealized_derivative_pnl_usd",
    "eps_excluding_unrealized_derivative_pnl",
    "derivative_oci_current_period_usd",
    "derivative_aoci_beginning_balance_usd",
    "derivative_aoci_ending_balance_usd",
    "derivative_aoci_reclassified_to_earnings_usd",
    "derivative_aoci_tax_effect_usd",
    "derivative_aoci_net_of_tax_usd",
    "aoci_total_ending_balance_usd",
    "derivative_assets_current_usd",
    "derivative_assets_noncurrent_usd",
    "derivative_liabilities_current_usd",
    "derivative_liabilities_noncurrent_usd",
    "derivative_net_asset_liability_usd",
    "cash_flow_hedge_reclass_revenue_usd",
    "cash_flow_hedge_reclass_cogs_usd",
    "cash_flow_hedge_reclass_total_usd",
    "fair_value_hedge_inventory_adjustment_cogs_usd",
    "fair_value_hedge_derivative_futures_effect_cogs_usd",
    "fair_value_hedge_total_pnl_usd",
    "fair_value_hedge_cogs_usd",
    "cash_flow_statement_derivative_change_usd",
    "derivative_disclosure_method",
    "derivative_source_document",
    "derivative_source_section",
    "derivative_source_quote_short",
    "derivative_confidence",
    "derivative_notes",
]


# Open-position notional disclosure is intentionally kept on the company's
# source scale. For GPRE that is "in thousands"; the writer shows the Scale
# column instead of silently multiplying values.
DERIVATIVE_EXPOSURE_COLUMNS: List[str] = [
    "Quarter",
    "Commodity",
    "Instrument",
    "Accounting bucket",
    "Direction",
    "Long notional",
    "Short notional",
    "Net notional",
    "Unit",
    "Scale",
    "Likely P&L line",
    "Interpretation",
    "Source / note",
]


DERIVATIVE_FLOW_COLUMNS: Tuple[str, ...] = (
    "derivative_unrealized_gain_loss_pnl_usd",
    "derivative_realized_gain_loss_pnl_usd",
    "non_designated_derivative_pnl_revenue_usd",
    "non_designated_derivative_pnl_cogs_usd",
    "non_designated_derivative_pnl_total_usd",
    "derivative_gain_loss_pnl_total_usd",
    "derivative_gain_loss_revenue_usd",
    "derivative_gain_loss_cogs_usd",
    "derivative_gain_loss_other_income_usd",
    "derivative_pnl_per_gal_memo",
    "derivative_pnl_tax_effect_usd",
    "derivative_pnl_after_tax_usd",
    "earnings_excluding_unrealized_derivative_pnl_usd",
    "eps_excluding_unrealized_derivative_pnl",
    "derivative_oci_current_period_usd",
    "derivative_aoci_reclassified_to_earnings_usd",
    "derivative_aoci_tax_effect_usd",
    "derivative_aoci_net_of_tax_usd",
    "cash_flow_hedge_reclass_revenue_usd",
    "cash_flow_hedge_reclass_cogs_usd",
    "cash_flow_hedge_reclass_total_usd",
    "fair_value_hedge_inventory_adjustment_cogs_usd",
    "fair_value_hedge_derivative_futures_effect_cogs_usd",
    "fair_value_hedge_total_pnl_usd",
    "fair_value_hedge_cogs_usd",
    "cash_flow_statement_derivative_change_usd",
)


DERIVATIVE_QA_COLUMNS: List[str] = [
    "quarter",
    "metric",
    "severity",
    "message",
    "source",
    "issue_family",
    "recommended_action",
    "raw_metric",
]


@dataclass(frozen=True)
class DerivativeOciBridgeResult:
    """Parsed bridge tables plus QA rows from local derivative disclosures."""

    rows: pd.DataFrame
    qa_rows: pd.DataFrame
    exposure_rows: pd.DataFrame


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if isinstance(value, float) and math.isnan(value):
            return ""
    except Exception:
        pass
    return str(value).replace("\xa0", " ").strip()


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", _as_text(value)).strip().lower()


def _parse_number_cell(value: Any) -> Optional[float]:
    text = _as_text(value)
    if not text:
        return None
    text = text.replace("$", "").replace(",", "").strip()
    text = text.replace("\u2014", "-").replace("—", "-")
    if text in {"-", "--", "–"}:
        return None
    sign = 1.0
    if text.startswith("(") and text.endswith(")"):
        sign = -1.0
        text = text[1:-1].strip()
    if not re.fullmatch(r"[-+]?\d+(?:\.\d+)?", text):
        return None
    return sign * float(text)


def _row_numbers(row: Sequence[Any]) -> List[float]:
    nums: List[float] = []
    for value in row:
        parsed = _parse_number_cell(value)
        if parsed is None:
            continue
        # Header years are never data values for this bridge.
        if parsed in {2023.0, 2024.0, 2025.0, 2026.0, 2027.0}:
            continue
        if nums and nums[-1] == parsed:
            continue
        nums.append(parsed)
    return nums


def _row_text(row: Sequence[Any]) -> str:
    return " ".join(_normalize_text(v) for v in row if _normalize_text(v))


def _find_row_numbers(tables: Sequence[pd.DataFrame], include_terms: Sequence[str]) -> List[float]:
    include = tuple(str(term).lower() for term in include_terms)
    for df in tables:
        for _, row in df.iterrows():
            values = row.tolist()
            text = _row_text(values)
            if include and all(term in text for term in include):
                nums = _row_numbers(values)
                if nums:
                    return nums
    return []


def _first_value(tables: Sequence[pd.DataFrame], include_terms: Sequence[str], index: int = 0) -> Optional[float]:
    nums = _find_row_numbers(tables, include_terms)
    if len(nums) > index:
        return float(nums[index])
    return None


def _first_value_in_table(
    tables: Sequence[pd.DataFrame],
    table_terms: Sequence[str],
    row_terms: Sequence[str],
    *,
    index: int = 0,
) -> Optional[float]:
    table_include = tuple(str(term).lower() for term in table_terms if str(term or "").strip())
    row_include = tuple(str(term).lower() for term in row_terms if str(term or "").strip())
    for df in tables:
        table_blob = " ".join(_row_text(row.tolist()) for _, row in df.iterrows())
        if table_include and not all(term in table_blob for term in table_include):
            continue
        for _, row in df.iterrows():
            values = row.tolist()
            text = _row_text(values)
            if row_include and all(term in text for term in row_include):
                nums = _row_numbers(values)
                if len(nums) > index:
                    return float(nums[index])
    return None


def _usd_thousands(value: Optional[float]) -> Any:
    if value is None:
        return pd.NA
    return float(value) * 1000.0


def _sum_optional(*values: Any) -> Any:
    nums = [float(v) for v in values if pd.notna(v)]
    if not nums:
        return pd.NA
    return float(sum(nums))


def _first_text(row: Sequence[Any], cols: Sequence[int]) -> str:
    for col in cols:
        if col < 0 or col >= len(row):
            continue
        text = _as_text(row[col])
        if text:
            return text
    return ""


def _first_number(row: Sequence[Any], cols: Sequence[int]) -> Optional[float]:
    for col in cols:
        if col < 0 or col >= len(row):
            continue
        value = _parse_number_cell(row[col])
        if value is not None:
            return float(value)
    return None


def _cols_matching(df: pd.DataFrame, terms: Sequence[str], *, header_rows: int = 3) -> List[int]:
    needles = tuple(str(term or "").lower() for term in terms if str(term or "").strip())
    out: List[int] = []
    for col_idx in range(int(df.shape[1])):
        blob = " ".join(
            _normalize_text(df.iat[row_idx, col_idx])
            for row_idx in range(min(header_rows, int(df.shape[0])))
        )
        if needles and all(term in blob for term in needles):
            out.append(col_idx)
    return out


def _direction_from_net(value: Any) -> str:
    num = pd.to_numeric(value, errors="coerce")
    if pd.isna(num):
        return ""
    if float(num) > 0:
        return "Net long"
    if float(num) < 0:
        return "Net short"
    return "Flat"


def _likely_pnl_line(commodity: str, accounting_bucket: str) -> str:
    commodity_low = str(commodity or "").lower()
    bucket_low = str(accounting_bucket or "").lower()
    if "fair-value" in bucket_low or "fair value" in bucket_low:
        return "Inventory/COGS"
    if any(term in commodity_low for term in ("ethanol", "distiller", "renewable corn oil")):
        return "Revenue"
    if any(term in commodity_low for term in ("corn", "natural gas")):
        return "COGS"
    return "Unknown / not disclosed"


def _interpret_exposure(commodity: str, accounting_bucket: str, pnl_line: str) -> str:
    bucket_low = str(accounting_bucket or "").lower()
    pnl_low = str(pnl_line or "").lower()
    commodity_low = str(commodity or "").lower()
    if "fair-value" in bucket_low or "fair value" in bucket_low:
        return "Fair-value hedge: likely inventory value protection"
    if "economic" in bucket_low or "non-designated" in bucket_low:
        return "Economic hedge: MTM usually flows directly through P&L"
    if "cogs" in pnl_low:
        if "natural gas" in commodity_low:
            return "Input hedge: protects future natural gas purchases"
        return "Input hedge: protects future corn purchases"
    if "revenue" in pnl_low:
        return "Output hedge: protects future product sales"
    return "Notional disclosure only; fair value by commodity not disclosed"


def _normalize_unit(unit: str) -> str:
    unit_txt = _as_text(unit)
    if not unit_txt:
        return ""
    mapping = {
        "mmbtu": "MMBtu",
        "mmbtu": "MMBtu",
        "mmbtus": "MMBtu",
        "mmbtu": "MMBtu",
    }
    low = unit_txt.lower().replace(" ", "")
    if low in mapping:
        return mapping[low]
    return unit_txt


def _extract_exposure_rows(
    tables: Sequence[pd.DataFrame],
    *,
    quarter: date,
    source_path: Path,
) -> List[dict[str, Any]]:
    """Parse open derivative-position notional rows while preserving disclosure basis."""

    out: List[dict[str, Any]] = []
    quarter_ts = pd.Timestamp(quarter)
    source_note_base = f"{source_path.name} | derivative note open-position table"
    for df in tables:
        if df.empty:
            continue
        table_blob = " ".join(_row_text(row.tolist()) for _, row in df.iterrows())
        if not all(term in table_blob for term in ("derivative instruments", "exchange-traded", "unit of measure", "commodity")):
            continue

        exchange_cols = _cols_matching(df, ("exchange-traded",))
        exchange_cols = [
            col
            for col in exchange_cols
            if "non-exchange" not in " ".join(
                _normalize_text(df.iat[row_idx, col])
                for row_idx in range(min(3, int(df.shape[0])))
            )
        ]
        if not exchange_cols:
            exchange_cols = _cols_matching(df, ("net long",))
        long_cols = _cols_matching(df, ("non-exchange-traded", "long"))
        short_cols = _cols_matching(df, ("non-exchange-traded", "short"))
        unit_cols = _cols_matching(df, ("unit of measure",))
        commodity_cols = _cols_matching(df, ("commodity",))
        instrument_cols = _cols_matching(df, ("derivative instruments",))

        if not exchange_cols:
            exchange_cols = [6, 7, 8]
        if not long_cols:
            long_cols = [12, 13, 14]
        if not short_cols:
            short_cols = [18, 19, 20]
        if not unit_cols:
            unit_cols = [24, 25, 26]
        if not commodity_cols:
            commodity_cols = [30, 31, 32]
        if not instrument_cols:
            instrument_cols = [0, 1, 2]

        footnote_start = max(exchange_cols) + 1
        footnote_end = min(long_cols) if long_cols else footnote_start + 3
        footnote_cols = list(range(footnote_start, max(footnote_start, footnote_end)))

        for _, raw_row in df.iterrows():
            row = raw_row.tolist()
            instrument_raw = _first_text(row, instrument_cols)
            instrument_low = instrument_raw.lower()
            if "future" not in instrument_low and "forward" not in instrument_low:
                continue
            commodity = _first_text(row, commodity_cols)
            unit = _normalize_unit(_first_text(row, unit_cols))
            if not commodity or not unit:
                continue

            footnote = _first_text(row, footnote_cols)
            if "(3)" in footnote:
                accounting_bucket = "Cash-flow hedge"
            elif "(4)" in footnote:
                accounting_bucket = "Fair-value hedge"
            else:
                accounting_bucket = "Economic / non-designated"

            if "future" in instrument_low:
                instrument = "Exchange-traded futures/options"
                net_notional = _first_number(row, exchange_cols)
                if net_notional is None:
                    continue
                long_notional = float(net_notional) if float(net_notional) > 0 else pd.NA
                short_notional = float(net_notional) if float(net_notional) < 0 else pd.NA
                direction = _direction_from_net(net_notional)
                source_note = source_note_base + "; exchange-traded disclosed net long/(short), options delta-adjusted"
            else:
                instrument = "Non-exchange-traded forwards"
                long_notional = _first_number(row, long_cols)
                short_notional = _first_number(row, short_cols)
                if long_notional is None and short_notional is None:
                    continue
                # Forward rows are disclosed gross long and gross short. Net
                # notional is a display aid only; the gross source values remain
                # visible so the workbook does not imply source-level netting.
                net_notional = _sum_optional(
                    float(long_notional) if long_notional is not None else pd.NA,
                    float(short_notional) if short_notional is not None else pd.NA,
                )
                direction = _direction_from_net(net_notional)
                source_note = source_note_base + "; forwards disclosed gross long and gross short"

            pnl_line = _likely_pnl_line(commodity, accounting_bucket)
            out.append(
                {
                    "Quarter": quarter_ts,
                    "Commodity": commodity,
                    "Instrument": instrument,
                    "Accounting bucket": accounting_bucket,
                    "Direction": direction,
                    "Long notional": long_notional if long_notional is not None else pd.NA,
                    "Short notional": short_notional if short_notional is not None else pd.NA,
                    "Net notional": net_notional if pd.notna(net_notional) else pd.NA,
                    "Unit": unit,
                    "Scale": "in thousands",
                    "Likely P&L line": pnl_line,
                    "Interpretation": _interpret_exposure(commodity, accounting_bucket, pnl_line),
                    "Source / note": source_note
                    + ("; footnote (3)" if "(3)" in footnote else "; footnote (4)" if "(4)" in footnote else "; no hedge-accounting footnote"),
                }
            )
    return out


def _quarter_from_source(path: Path) -> Optional[date]:
    match = re.search(r"_(\d{4}-\d{2}-\d{2})_financial_statement", path.name)
    if match:
        try:
            return pd.Timestamp(match.group(1)).date()
        except Exception:
            return None
    return None


def _value_for_quarter(df: Optional[pd.DataFrame], quarter: date, columns: Sequence[str]) -> Any:
    if df is None or df.empty or "quarter" not in df.columns:
        return pd.NA
    work = df.copy()
    work["_q"] = pd.to_datetime(work["quarter"], errors="coerce").dt.date
    hits = work[work["_q"].eq(quarter)]
    if hits.empty:
        return pd.NA
    for col in columns:
        if col in hits.columns:
            vals = pd.to_numeric(hits[col], errors="coerce").dropna()
            if not vals.empty:
                return float(vals.iloc[-1])
    return pd.NA


def _source_paths_for_ticker(ticker: str, root: Optional[Path] = None) -> List[Path]:
    ticker_norm = str(ticker or "").upper().strip()
    if not ticker_norm:
        return []
    workspace_root = root or Path(__file__).resolve().parents[2]
    source_dir = workspace_root / ticker_norm / "financial_statement"
    if not source_dir.exists():
        return []
    paths = list(source_dir.glob(f"{ticker_norm}_Q*_10Q_*_financial_statement.htm"))
    paths.extend(source_dir.glob(f"{ticker_norm}_FY*_10K_*_financial_statement.htm"))
    recent_paths = [
        p
        for p in paths
        if (_quarter_from_source(p) or date.min) >= date(2023, 1, 1)
    ]
    return sorted(recent_paths, key=lambda p: (_quarter_from_source(p) or date.min, p.name))


def _source_period_type(path: Path) -> str:
    name = path.name.upper()
    if "_FY" in name or "_10K_" in name:
        return "annual"
    return "source_three_month"


def _is_annual_source_row(row: Dict[str, Any]) -> bool:
    return str(row.get("source_period_type") or "").strip().lower() == "annual"


def _build_qa_rows(quarter: Optional[date], source: str, has_pnl_total: bool, has_oci: bool, notes: str) -> List[dict[str, Any]]:
    quarter_ts = pd.Timestamp(quarter) if isinstance(quarter, date) else pd.NaT
    rows: List[dict[str, Any]] = []
    if has_oci:
        rows.append(
            {
                "quarter": quarter_ts,
                "metric": "Derivative & OCI Bridge",
                "severity": "info",
                "message": "Derivative OCI/AOCI movement is memo-only; do not include in net income bridge until reclassified to earnings.",
                "source": source,
                "issue_family": "derivative_oci_pnl_separation",
                "recommended_action": "keep P&L and OCI separate",
                "raw_metric": "derivative_oci_current_period_usd",
            }
        )
    if has_pnl_total:
        rows.append(
            {
                "quarter": quarter_ts,
                "metric": "Derivative P&L memo",
                "severity": "info",
                "message": "Source discloses total derivative P&L by income-statement line but not a clean unrealized P&L split; EPS excluding derivative item is left blank.",
                "source": source,
                "issue_family": "derivative_eps_tax_support",
                "recommended_action": "leave EPS normalization blank unless after-tax unrealized P&L is disclosed",
                "raw_metric": "derivative_gain_loss_pnl_total_usd",
            }
        )
    if has_pnl_total and has_oci:
        rows.append(
            {
                "quarter": quarter_ts,
                "metric": "Derivative disclosure comparability",
                "severity": "info",
                "message": "Derivative P&L, realized hedge reclassifications, and OCI/AOCI are shown as separate memo fields; production earnings are unchanged.",
                "source": source,
                "issue_family": "derivative_disclosure_method",
                "recommended_action": "compare disclosure method before treating as recurring operating earnings",
                "raw_metric": "derivative_notes",
            }
        )
    if not rows and notes:
        rows.append(
            {
                "quarter": quarter_ts,
                "metric": "Derivative & OCI Bridge",
                "severity": "info",
                "message": notes,
                "source": source,
                "issue_family": "derivative_disclosure_method",
                "recommended_action": "memo only",
                "raw_metric": "derivative_notes",
            }
        )
    return rows


def _extract_one_source(
    ticker: str,
    source_path: Path,
    *,
    hist: Optional[pd.DataFrame] = None,
    adj_metrics: Optional[pd.DataFrame] = None,
) -> Tuple[List[dict[str, Any]], List[dict[str, Any]], List[dict[str, Any]]]:
    """Extract one filing's derivative bridge rows, QA notes, and exposure rows."""

    try:
        tables = pd.read_html(source_path)
    except Exception:
        return [], [], []
    quarter = _quarter_from_source(source_path)
    if not isinstance(quarter, date):
        return [], [], []
    period_type = _source_period_type(source_path)
    quarterization_status = "annual_source" if period_type == "annual" else "source_three_month"

    futures_rev = _usd_thousands(_first_value(tables, ("exchange-traded futures", "revenues")))
    forwards_rev = _usd_thousands(_first_value(tables, ("forwards", "revenues")))
    futures_cogs = _usd_thousands(_first_value(tables, ("exchange-traded futures", "cost of goods sold")))
    forwards_cogs = _usd_thousands(_first_value(tables, ("forwards", "cost of goods sold")))
    non_designated_total = _usd_thousands(
        _first_value_in_table(
            tables,
            ("gain (loss) recognized in income on derivatives",),
            ("net gain (loss) recognized",),
        )
    )
    revenue_total = _sum_optional(futures_rev, forwards_rev)
    cogs_total = _sum_optional(futures_cogs, forwards_cogs)
    if pd.isna(non_designated_total):
        non_designated_total = _sum_optional(revenue_total, cogs_total)

    oci_current = _usd_thousands(
        _first_value(tables, ("unrealized", "derivatives arising during the period"))
    )
    if pd.isna(oci_current):
        oci_current = _usd_thousands(
            _first_value_in_table(
                tables,
                ("gain (loss) recognized in other comprehensive income", "derivatives"),
                ("commodity contracts",),
            )
        )
    aoci_reclass = _usd_thousands(_first_value(tables, ("reclassification of realized", "derivatives")))
    aoci_begin = _usd_thousands(_first_value(tables, ("accumulated other comprehensive loss",), index=1))
    aoci_end = _usd_thousands(_first_value(tables, ("accumulated other comprehensive loss",), index=0))

    derivative_assets_current = _usd_thousands(_first_value(tables, ("derivative financial instruments",), index=0))
    # Balance-sheet row search can collide with fair-value tables that use the
    # same label. Prefer the larger current-liability balance-sheet amount when
    # more than one non-asset candidate appears.
    derivative_liabilities_current = pd.NA
    candidates: List[float] = []
    for df in tables:
        for _, row in df.iterrows():
            text = _row_text(row.tolist())
            if "derivative financial instruments" in text:
                nums = _row_numbers(row.tolist())
                if nums:
                    candidates.append(float(nums[0]) * 1000.0)
    if candidates and pd.notna(derivative_assets_current):
        asset_value = float(derivative_assets_current)
        positives = [v for v in candidates if abs(float(v) - asset_value) > 1e-9]
        if positives:
            derivative_liabilities_current = max(positives, key=abs)

    cf_reclass_revenue = _usd_thousands(_first_value(tables, ("amount of gain (loss) on exchange-traded futures reclassified",)))
    cf_reclass_cogs = pd.NA
    nums = _find_row_numbers(tables, ("amount of gain (loss) on exchange-traded futures reclassified",))
    if len(nums) > 1:
        cf_reclass_cogs = _usd_thousands(nums[1])
    cf_reclass_total = _sum_optional(cf_reclass_revenue, cf_reclass_cogs)
    fair_value_inventory_cogs = _usd_thousands(_first_value(tables, ("fair-value hedged inventories",)))
    fair_value_futures_cogs = _usd_thousands(
        _first_value(tables, ("exchange-traded futures designated as hedging instruments",))
    )
    fair_value_hedge_total = _sum_optional(fair_value_inventory_cogs, fair_value_futures_cogs)
    fair_value_hedge_cogs = fair_value_hedge_total
    cash_flow_statement_derivative_change = _usd_thousands(_first_value(tables, ("change in derivative financial instruments",)))

    # Total derivative P&L is income-statement impact only. OCI movement, AOCI
    # balances, and net derivative assets/liabilities stay out of this total.
    aggregate_revenue_total = _sum_optional(revenue_total, cf_reclass_revenue)
    aggregate_cogs_total = _sum_optional(cogs_total, cf_reclass_cogs, fair_value_hedge_total)
    pnl_total = _sum_optional(non_designated_total, cf_reclass_total, fair_value_hedge_total)
    pnl_per_gal = pd.NA
    net_derivative = _sum_optional(derivative_assets_current, -float(derivative_liabilities_current) if pd.notna(derivative_liabilities_current) else pd.NA)
    has_pnl_total = pd.notna(pnl_total)
    has_oci = pd.notna(oci_current) or pd.notna(aoci_reclass) or pd.notna(aoci_end)
    source_name = str(source_path)
    notes = (
        "Memo only: P&L derivative gains/losses are separate from OCI only cash-flow hedge movements; "
        "AOCI reclassifications are tracked separately. Source does not disclose a clean unrealized P&L split, "
        "so net income/EPS excluding unrealized derivative P&L remain blank. Total P&L is summed from "
        "disclosed revenue/COGS components when no total row is available."
    )
    row = {
        "quarter": pd.Timestamp(quarter),
        "source_period_type": period_type,
        "quarterization_status": quarterization_status,
        "quarterization_note": "Source annual values; Q4 is derived separately where Q1-Q3 are available." if period_type == "annual" else "Source provides a three-month column; YTD columns are ignored for quarterly bridge values.",
        "reported_net_income_usd": _value_for_quarter(hist, quarter, ("net_income", "net_income_attributable_to_common", "net_income_attributable")),
        "reported_diluted_eps": _value_for_quarter(hist, quarter, ("eps_diluted", "diluted_eps", "eps")),
        "reported_adjusted_ebitda_usd": _value_for_quarter(adj_metrics, quarter, ("adj_ebitda", "adjusted_ebitda")),
        "derivative_unrealized_gain_loss_pnl_usd": pd.NA,
        "derivative_realized_gain_loss_pnl_usd": pd.NA,
        "non_designated_derivative_pnl_revenue_usd": revenue_total,
        "non_designated_derivative_pnl_cogs_usd": cogs_total,
        "non_designated_derivative_pnl_total_usd": non_designated_total,
        "derivative_gain_loss_pnl_total_usd": pnl_total,
        "derivative_gain_loss_revenue_usd": aggregate_revenue_total,
        "derivative_gain_loss_cogs_usd": aggregate_cogs_total,
        "derivative_gain_loss_other_income_usd": pd.NA,
        "derivative_pnl_per_gal_memo": pnl_per_gal,
        "derivative_pnl_tax_effect_usd": pd.NA,
        "derivative_pnl_after_tax_usd": pd.NA,
        "earnings_excluding_unrealized_derivative_pnl_usd": pd.NA,
        "eps_excluding_unrealized_derivative_pnl": pd.NA,
        "derivative_oci_current_period_usd": oci_current,
        "derivative_aoci_beginning_balance_usd": aoci_begin,
        "derivative_aoci_ending_balance_usd": aoci_end,
        "derivative_aoci_reclassified_to_earnings_usd": aoci_reclass,
        "derivative_aoci_tax_effect_usd": pd.NA,
        "derivative_aoci_net_of_tax_usd": pd.NA,
        "aoci_total_ending_balance_usd": aoci_end,
        "derivative_assets_current_usd": derivative_assets_current,
        "derivative_assets_noncurrent_usd": pd.NA,
        "derivative_liabilities_current_usd": derivative_liabilities_current,
        "derivative_liabilities_noncurrent_usd": pd.NA,
        "derivative_net_asset_liability_usd": net_derivative,
        "cash_flow_hedge_reclass_revenue_usd": cf_reclass_revenue,
        "cash_flow_hedge_reclass_cogs_usd": cf_reclass_cogs,
        "cash_flow_hedge_reclass_total_usd": cf_reclass_total,
        "fair_value_hedge_inventory_adjustment_cogs_usd": fair_value_inventory_cogs,
        "fair_value_hedge_derivative_futures_effect_cogs_usd": fair_value_futures_cogs,
        "fair_value_hedge_total_pnl_usd": fair_value_hedge_total,
        "fair_value_hedge_cogs_usd": fair_value_hedge_cogs,
        "cash_flow_statement_derivative_change_usd": cash_flow_statement_derivative_change,
        "derivative_disclosure_method": "10-Q derivative footnote / OCI-AOCI memo",
        "derivative_source_document": source_name,
        "derivative_source_section": "Derivative instruments, comprehensive income, stockholders' equity",
        "derivative_source_quote_short": "Derivative P&L table, OCI/AOCI table, balance-sheet derivative instruments.",
        "derivative_confidence": "high" if has_pnl_total or has_oci else "low",
        "derivative_notes": notes,
    }
    exposure_rows = _extract_exposure_rows(tables, quarter=quarter, source_path=source_path)
    return [row], _build_qa_rows(quarter, source_name, has_pnl_total, has_oci, notes), exposure_rows


def _numeric_or_none(value: Any) -> Optional[float]:
    val = pd.to_numeric(value, errors="coerce")
    if pd.isna(val):
        return None
    return float(val)


def _derive_q4_rows(rows: List[dict[str, Any]]) -> Tuple[List[dict[str, Any]], List[dict[str, Any]]]:
    """Quarterize annual 10-K flow fields by subtracting Q1-Q3 disclosures."""

    by_year: Dict[int, Dict[int, dict[str, Any]]] = {}
    annual_by_year: Dict[int, dict[str, Any]] = {}
    quarterized_rows: List[dict[str, Any]] = []
    qa_rows: List[dict[str, Any]] = []

    for row in rows:
        qd = pd.to_datetime(row.get("quarter"), errors="coerce")
        if pd.isna(qd):
            continue
        qdate = qd.date()
        if _is_annual_source_row(row):
            annual_by_year[int(qdate.year)] = dict(row)
            continue
        qn = ((int(qdate.month) - 1) // 3) + 1
        by_year.setdefault(int(qdate.year), {})[qn] = dict(row)
        row["quarterization_status"] = row.get("quarterization_status") or "source_three_month"
        row["source_period_type"] = row.get("source_period_type") or "source_three_month"
        quarterized_rows.append(dict(row))

    for year_num, annual_row in sorted(annual_by_year.items()):
        q_map = by_year.get(year_num) or {}
        if not all(q in q_map for q in (1, 2, 3)):
            continue
        q4 = dict(annual_row)
        q4["source_period_type"] = "derived_q4"
        q4["quarterization_status"] = "annual_minus_q1_q3"
        q4["quarterization_note"] = "Q4 flow values derived from annual 10-K values less Q1-Q3 10-Q three-month values; balance-sheet exposure fields use year-end 10-K values."
        for col in DERIVATIVE_FLOW_COLUMNS:
            annual_val = _numeric_or_none(annual_row.get(col))
            prior_vals = [_numeric_or_none(q_map[q].get(col)) for q in (1, 2, 3)]
            if annual_val is None or any(val is None for val in prior_vals):
                q4[col] = pd.NA
                continue
            q4[col] = float(annual_val - sum(float(val) for val in prior_vals if val is not None))
        quarterized_rows.append(q4)
        qa_rows.append(
            {
                "quarter": pd.Timestamp(date(year_num, 12, 31)),
                "metric": "Derivative & OCI Bridge",
                "severity": "info",
                "message": "Q4 derivative/OCI flow values are inferred from annual 10-K values less Q1-Q3 three-month disclosures; do not treat annual values as standalone quarter values.",
                "source": str(annual_row.get("derivative_source_document") or ""),
                "issue_family": "derivative_quarterization_inferred",
                "recommended_action": "review annual-minus-Q1-Q3 derivation when comparing quarterly bridge values",
                "raw_metric": "quarterization_status",
            }
        )
    return quarterized_rows, qa_rows


def build_derivative_oci_bridge_from_sources(
    ticker: str,
    source_paths: Optional[Iterable[Path | str]] = None,
    *,
    hist: Optional[pd.DataFrame] = None,
    adj_metrics: Optional[pd.DataFrame] = None,
    workspace_root: Optional[Path] = None,
) -> DerivativeOciBridgeResult:
    """Build historical derivative/OCI bridge data from local filing tables.

    The parser favors reported three-month 10-Q columns. Annual 10-K values are
    used only for derived Q4 flow rows when Q1-Q3 are available; balance-sheet
    exposure fields remain period-end snapshots.
    """
    paths = [Path(p) for p in source_paths] if source_paths is not None else _source_paths_for_ticker(ticker, workspace_root)
    rows: List[dict[str, Any]] = []
    qa_rows: List[dict[str, Any]] = []
    exposure_rows: List[dict[str, Any]] = []
    for path in paths:
        if not path.exists():
            continue
        parsed_rows, parsed_qa, parsed_exposure = _extract_one_source(
            str(ticker or "").upper(),
            path,
            hist=hist,
            adj_metrics=adj_metrics,
        )
        rows.extend(parsed_rows)
        qa_rows.extend(parsed_qa)
        exposure_rows.extend(parsed_exposure)
    rows, derived_qa = _derive_q4_rows(rows)
    qa_rows.extend(derived_qa)
    rows_df = pd.DataFrame(rows)
    qa_df = pd.DataFrame(qa_rows)
    exposure_df = pd.DataFrame(exposure_rows)
    if rows_df.empty:
        rows_df = pd.DataFrame(columns=DERIVATIVE_OCI_BRIDGE_COLUMNS)
    else:
        for col in DERIVATIVE_OCI_BRIDGE_COLUMNS:
            if col not in rows_df.columns:
                rows_df[col] = pd.NA
        rows_df = rows_df[DERIVATIVE_OCI_BRIDGE_COLUMNS].sort_values("quarter", ascending=False, kind="stable")
    if qa_df.empty:
        qa_df = pd.DataFrame(columns=DERIVATIVE_QA_COLUMNS)
    else:
        for col in DERIVATIVE_QA_COLUMNS:
            if col not in qa_df.columns:
                qa_df[col] = pd.NA
        qa_df = qa_df[DERIVATIVE_QA_COLUMNS]
    if exposure_df.empty:
        exposure_df = pd.DataFrame(columns=DERIVATIVE_EXPOSURE_COLUMNS)
    else:
        for col in DERIVATIVE_EXPOSURE_COLUMNS:
            if col not in exposure_df.columns:
                exposure_df[col] = pd.NA
        exposure_df = exposure_df[DERIVATIVE_EXPOSURE_COLUMNS].sort_values(
            ["Quarter", "Commodity", "Instrument"],
            ascending=[False, True, True],
            kind="stable",
        )
    return DerivativeOciBridgeResult(rows=rows_df, qa_rows=qa_df, exposure_rows=exposure_df)
