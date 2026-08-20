"""Operating_Drivers workbook-reader support helpers."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, MutableMapping, Optional

from .longitudinal_memory.calendar_rules import CALENDAR_YEAR_RULE_ID
from .longitudinal_memory.operating_driver_foundation import (
    AggregateCompleteness,
    AggregationSemantics,
    DefinitionContinuity,
    DefinitionContinuityState,
    DriverDimension,
    DriverIdentity,
    DurationAggregateRequest,
    DurationAggregateResult,
    EvidenceAvailability,
    EvidenceClassification,
    EvidenceSourceReference,
    EvidenceSourceType,
    EvidenceValueKind,
    FiscalCalendarIdentity,
    FiscalQuarterPeriod,
    OperatingDriverEvidence,
    PeriodKind,
    TrailingTwelveMonthsPeriod,
    aggregate_duration_fail_closed,
    calendar_year_fiscal_year_period,
    calendar_year_quarter_period,
    ttm_quarter_keys,
)


@dataclass(frozen=True)
class OperatingDriverWorkbookSupportDeps:
    runtime: MutableMapping[str, Any]


class OperatingDriverWorkbookSupport:
    """Read legacy workbook rows through the typed fail-closed foundation.

    The workbook remains a legacy presentation/source adapter. Economic
    completeness, period identity, and missing-versus-zero behavior are owned
    by the shared typed foundation imported above.
    """

    def __init__(self, deps: OperatingDriverWorkbookSupportDeps) -> None:
        self.deps = deps
        self.runtime = deps.runtime

    @property
    def _re(self) -> Any:
        return self.runtime.get("re", re)

    def _date_or_none(self, value: Any) -> Optional[date]:
        return self.runtime["_date_or_none"](value)

    def _company_id(self, wb: Any) -> str:
        candidates = sorted(
            {
                str(sheet_name)[: -len("_Investment_Case")].strip().upper()
                for sheet_name in wb.sheetnames
                if str(sheet_name).endswith("_Investment_Case")
                and str(sheet_name)[: -len("_Investment_Case")].strip()
            }
        )
        return candidates[0] if len(candidates) == 1 else "WORKBOOK"

    def _company_slug(self, company_id: str) -> str:
        slug = self._re.sub(
            r"[^a-z0-9]+", "-", str(company_id).lower()
        ).strip("-")
        if not slug:
            return "workbook"
        if not slug[0].isalpha():
            slug = f"company-{slug}"
        return slug

    def _metric_slug(self, metric_label: str) -> str:
        slug = self._re.sub(
            r"[^a-z0-9]+", "-", metric_label.lower()
        ).strip("-")
        return slug or "unnamed-driver"

    def _metric_location(
        self, wb: Any, metric_label: str
    ) -> tuple[Any, int, int] | None:
        if "Operating_Drivers" not in wb.sheetnames:
            return None
        ws = wb["Operating_Drivers"]
        metric_row: Optional[int] = None
        for row in range(1, ws.max_row + 1):
            if (
                str(ws.cell(row, 1).value or "").strip().lower()
                == metric_label.strip().lower()
            ):
                metric_row = row
                break
        if metric_row is None:
            return None
        quarter_row: Optional[int] = None
        for row in range(max(1, metric_row - 25), metric_row + 1):
            if str(ws.cell(row, 1).value or "").strip().lower() == "quarter":
                quarter_row = row
                break
        if quarter_row is None:
            return None
        return ws, metric_row, quarter_row

    def _parse_fiscal_quarter_label(self, value: Any) -> tuple[int, int] | None:
        match = self._re.fullmatch(
            r"\s*(\d{4})-Q([1-4])\s*",
            str(value or ""),
            flags=self._re.I,
        )
        if match is None:
            return None
        return int(match.group(1)), int(match.group(2))

    def _latest_reconciled_calendar_quarter(
        self, wb: Any
    ) -> tuple[int, int] | None:
        """Resolve the latest History_Q fiscal identity without guessing dates."""

        if "History_Q" not in wb.sheetnames:
            return None
        ws = wb["History_Q"]
        headers = {
            str(ws.cell(1, column).value or "").strip().lower(): column
            for column in range(1, ws.max_column + 1)
        }
        quarter_date_column = headers.get("quarter")
        fiscal_year_column = headers.get("fiscal_year")
        fiscal_quarter_column = headers.get("fiscal_quarter")
        fiscal_label_column = headers.get("fiscal_label")
        if quarter_date_column is None:
            return None

        candidates: list[tuple[int, int, date]] = []
        for row in range(2, ws.max_row + 1):
            quarter_date = self._date_or_none(
                ws.cell(row, quarter_date_column).value
            )
            if quarter_date is None:
                continue
            fiscal_key: tuple[int, int] | None = None
            if fiscal_year_column is not None and fiscal_quarter_column is not None:
                try:
                    fiscal_key = (
                        int(ws.cell(row, fiscal_year_column).value),
                        int(ws.cell(row, fiscal_quarter_column).value),
                    )
                except (TypeError, ValueError):
                    fiscal_key = None
            if fiscal_key is None and fiscal_label_column is not None:
                fiscal_key = self._parse_fiscal_quarter_label(
                    ws.cell(row, fiscal_label_column).value
                )
            if fiscal_key is None:
                return None
            fiscal_year, fiscal_quarter = fiscal_key
            month = fiscal_quarter * 3
            expected_end = (
                date(fiscal_year, month + 1, 1) - date.resolution
                if fiscal_quarter < 4
                else date(fiscal_year, 12, 31)
            )
            if quarter_date != expected_end:
                # This legacy adapter lacks source-backed starts for a non-calendar
                # fiscal calendar. The shared foundation supports those calendars,
                # but this adapter must fail closed rather than guess.
                return None
            candidates.append((fiscal_year, fiscal_quarter, quarter_date))
        if not candidates:
            return None
        fiscal_year, fiscal_quarter, _ = max(
            candidates, key=lambda item: (item[0], item[1], item[2])
        )
        return fiscal_year, fiscal_quarter

    def _typed_context(
        self,
        wb: Any,
        metric_label: str,
    ) -> tuple[
        Any,
        int,
        int,
        str,
        str,
        FiscalCalendarIdentity,
        DriverIdentity,
        DefinitionContinuity,
        dict[tuple[int, int], int],
    ] | None:
        location = self._metric_location(wb, metric_label)
        if location is None:
            return None
        ws, metric_row, quarter_row = location
        company_id = self._company_id(wb)
        company_slug = self._company_slug(company_id)
        metric_slug = self._metric_slug(metric_label)
        calendar = FiscalCalendarIdentity(
            calendar_id=f"calendar:{company_slug}:calendar-year@1",
            company_id=company_id,
            calendar_rule_id=CALENDAR_YEAR_RULE_ID,
            week_pattern="calendar",
        )
        driver = DriverIdentity(
            driver_id=f"driver:legacy-operating-drivers:{metric_slug}@1",
            company_id=company_id,
            ticker=company_id,
            driver_family="legacy-workbook-duration-driver",
            canonical_label=metric_label.strip(),
            display_label=metric_label.strip(),
            unit_id="unit:usd-million@1",
            scale="1000000",
            sign_convention="positive-is-reported-contribution",
            dimensions=(
                DriverDimension(
                    dimension_id="dimension:scope:company@1",
                    member_id="member:company:total@1",
                    label="Total company",
                ),
            ),
            period_kind=PeriodKind.FISCAL_QUARTER,
            source_owner="owner:operating-drivers:legacy-active@1",
            definition_id=f"definition:operating-drivers:{metric_slug}@1",
            definition_version=1,
            aggregation_semantics=AggregationSemantics.SUMMABLE,
        )
        continuity = DefinitionContinuity(
            state=DefinitionContinuityState.SAME_SERIES,
            from_definition_id=driver.definition_id,
            from_definition_version=driver.definition_version,
            to_definition_id=driver.definition_id,
            to_definition_version=driver.definition_version,
            reason="Legacy workbook adapter exposes one unchanged labelled series.",
        )
        columns_by_period: dict[tuple[int, int], int] = {}
        for column in range(2, ws.max_column + 1):
            fiscal_key = self._parse_fiscal_quarter_label(
                ws.cell(quarter_row, column).value
            )
            if fiscal_key is None:
                continue
            if fiscal_key in columns_by_period:
                return None
            columns_by_period[fiscal_key] = column
        return (
            ws,
            metric_row,
            quarter_row,
            company_id,
            company_slug,
            calendar,
            driver,
            continuity,
            columns_by_period,
        )

    def _period(
        self,
        *,
        company_id: str,
        company_slug: str,
        calendar: FiscalCalendarIdentity,
        fiscal_year: int,
        fiscal_quarter: int,
    ) -> FiscalQuarterPeriod:
        return calendar_year_quarter_period(
            company_id=company_id,
            calendar=calendar,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            period_id=(
                f"period:{company_slug}:fy{fiscal_year}-q{fiscal_quarter}@1"
            ),
        )

    def _observations(
        self,
        *,
        ws: Any,
        metric_row: int,
        metric_label: str,
        company_id: str,
        company_slug: str,
        driver: DriverIdentity,
        continuity: DefinitionContinuity,
        columns_by_period: dict[tuple[int, int], int],
        required_periods: tuple[FiscalQuarterPeriod, ...],
    ) -> tuple[OperatingDriverEvidence, ...]:
        observations: list[OperatingDriverEvidence] = []
        for period in required_periods:
            column = columns_by_period.get(
                (period.fiscal_year, period.fiscal_quarter)
            )
            if column is None:
                continue
            raw_value = ws.cell(metric_row, column).value
            source_location = (
                f"Operating_Drivers!{ws.cell(metric_row, column).coordinate}"
            )
            evidence_id = (
                f"evidence:{company_slug}:{self._metric_slug(metric_label)}:"
                f"fy{period.fiscal_year}-q{period.fiscal_quarter}@1"
            )
            source = EvidenceSourceReference(
                source_document_id=f"legacy-workbook:{company_id}",
                source_type=EvidenceSourceType.LEGACY_WORKBOOK,
                source_location=source_location,
                publication_date=None,
                knowledge_date=None,
            )
            if raw_value in (None, ""):
                observations.append(
                    OperatingDriverEvidence(
                        evidence_id=evidence_id,
                        driver=driver,
                        period=period,
                        source=source,
                        value_kind=EvidenceValueKind.NUMERIC,
                        raw_value=None,
                        normalized_value=None,
                        source_unit_id=driver.unit_id,
                        classification=EvidenceClassification.ACTUAL,
                        availability=EvidenceAvailability.UNAVAILABLE,
                        unavailable_reason="MISSING_SOURCE_VALUE",
                        continuity=continuity,
                    )
                )
                continue
            try:
                numeric_value = Decimal(str(raw_value))
            except Exception:
                observations.append(
                    OperatingDriverEvidence(
                        evidence_id=evidence_id,
                        driver=driver,
                        period=period,
                        source=source,
                        value_kind=EvidenceValueKind.NUMERIC,
                        raw_value=None,
                        normalized_value=None,
                        source_unit_id=driver.unit_id,
                        classification=EvidenceClassification.ACTUAL,
                        availability=EvidenceAvailability.NEEDS_REVIEW,
                        unavailable_reason="NON_NUMERIC_SOURCE_VALUE",
                        continuity=continuity,
                    )
                )
                continue
            value = str(numeric_value)
            observations.append(
                OperatingDriverEvidence(
                    evidence_id=evidence_id,
                    driver=driver,
                    period=period,
                    source=source,
                    value_kind=EvidenceValueKind.NUMERIC,
                    raw_value=value,
                    normalized_value=value,
                    source_unit_id=driver.unit_id,
                    classification=EvidenceClassification.ACTUAL,
                    availability=EvidenceAvailability.AVAILABLE,
                    unavailable_reason=None,
                    continuity=continuity,
                )
            )
        return tuple(observations)

    def operating_driver_ttm_result_from_workbook(
        self, wb: Any, metric_label: str
    ) -> DurationAggregateResult | None:
        context = self._typed_context(wb, metric_label)
        latest = self._latest_reconciled_calendar_quarter(wb)
        if context is None or latest is None:
            return None
        (
            ws,
            metric_row,
            _quarter_row,
            company_id,
            company_slug,
            calendar,
            driver,
            continuity,
            columns_by_period,
        ) = context
        required_periods = tuple(
            self._period(
                company_id=company_id,
                company_slug=company_slug,
                calendar=calendar,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
            )
            for fiscal_year, fiscal_quarter in ttm_quarter_keys(*latest)
        )
        ttm_period = TrailingTwelveMonthsPeriod(
            period_id=(
                f"period:{company_slug}:ttm-fy{latest[0]}-q{latest[1]}@1"
            ),
            company_id=company_id,
            ending_quarter=required_periods[-1],
            constituent_quarters=required_periods,
        )
        request = DurationAggregateRequest(
            request_id=(
                f"aggregate:{company_slug}:{self._metric_slug(metric_label)}:ttm"
            ),
            driver=driver,
            requested_period=ttm_period,
            required_constituent_quarters=required_periods,
        )
        observations = self._observations(
            ws=ws,
            metric_row=metric_row,
            metric_label=metric_label,
            company_id=company_id,
            company_slug=company_slug,
            driver=driver,
            continuity=continuity,
            columns_by_period=columns_by_period,
            required_periods=required_periods,
        )
        return aggregate_duration_fail_closed(request, observations)

    def operating_driver_latest_full_year_result_from_workbook(
        self, wb: Any, metric_label: str
    ) -> DurationAggregateResult | None:
        context = self._typed_context(wb, metric_label)
        latest = self._latest_reconciled_calendar_quarter(wb)
        if context is None or latest is None:
            return None
        (
            ws,
            metric_row,
            _quarter_row,
            company_id,
            company_slug,
            calendar,
            driver,
            continuity,
            columns_by_period,
        ) = context
        latest_year, latest_quarter = latest
        fiscal_year = latest_year if latest_quarter == 4 else latest_year - 1
        required_periods = tuple(
            self._period(
                company_id=company_id,
                company_slug=company_slug,
                calendar=calendar,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
            )
            for fiscal_quarter in range(1, 5)
        )
        fiscal_year_period = calendar_year_fiscal_year_period(
            company_id=company_id,
            calendar=calendar,
            fiscal_year=fiscal_year,
            period_id=f"period:{company_slug}:fy{fiscal_year}@1",
        )
        request = DurationAggregateRequest(
            request_id=(
                f"aggregate:{company_slug}:{self._metric_slug(metric_label)}:"
                f"fy{fiscal_year}"
            ),
            driver=driver,
            requested_period=fiscal_year_period,
            required_constituent_quarters=required_periods,
        )
        observations = self._observations(
            ws=ws,
            metric_row=metric_row,
            metric_label=metric_label,
            company_id=company_id,
            company_slug=company_slug,
            driver=driver,
            continuity=continuity,
            columns_by_period=columns_by_period,
            required_periods=required_periods,
        )
        return aggregate_duration_fail_closed(request, observations)

    def operating_driver_ttm_sum_from_workbook(
        self, wb: Any, metric_label: str
    ) -> Optional[float]:
        """Return an exact complete four-quarter SUM, else fail closed."""

        result = self.operating_driver_ttm_result_from_workbook(wb, metric_label)
        if (
            result is None
            or result.completeness is not AggregateCompleteness.COMPLETE
            or result.value is None
        ):
            return None
        return float(Decimal(result.value))

    def operating_driver_latest_full_year_sum_from_workbook(
        self, wb: Any, metric_label: str
    ) -> Optional[float]:
        """Return an exact complete Q1-Q4 fiscal-year SUM, else fail closed."""

        result = self.operating_driver_latest_full_year_result_from_workbook(
            wb, metric_label
        )
        if (
            result is None
            or result.completeness is not AggregateCompleteness.COMPLETE
            or result.value is None
        ):
            return None
        return float(Decimal(result.value))
