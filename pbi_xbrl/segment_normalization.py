"""Ticker-neutral segment source semantics and canonical identities."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping


SEGMENT_PERIOD_TYPE_ALIASES = {
    "quarter": "quarterly",
    "quarterly": "quarterly",
    "fiscal_quarter": "quarterly",
    "annual": "annual",
    "fiscal_year": "annual",
    "full_year": "annual",
}
SEGMENT_SOURCE_SCOPE_ALIASES = {
    "quarter": "quarterly",
    "quarterly": "quarterly",
    "fourth_quarter": "quarterly",
    "annual": "annual",
    "fiscal_year": "annual",
    "full_year": "annual",
}
SEGMENT_SOURCE_SCALES = {"ones", "thousands", "millions", "not_applicable"}
SEGMENT_AGGREGATION_ROLES = {"dimension_member", "reported_total"}

_QUARTER_RE = re.compile(r"^\d{4}-Q[1-4]$")
_ANNUAL_RE = re.compile(r"^\d{4}-FY$")
_TOTAL_COMPANY_ALIASES = {"total", "company total", "total company"}


class SegmentNormalizationError(ValueError):
    """Raised when source-table semantics cannot produce one exact segment fact."""

    def __init__(
        self,
        message: str,
        *,
        raw_pair: tuple[str, str] | None = None,
        canonical_pair: tuple[str, str] | None = None,
        source_row_ref: str = "",
        business_key: str = "",
    ) -> None:
        self.raw_pair = raw_pair
        self.canonical_pair = canonical_pair
        self.source_row_ref = source_row_ref
        self.business_key = business_key
        super().__init__(message)


def _token(value: Any) -> str:
    return re.sub(r"[\s_-]+", " ", str(value or "").strip().lower())


def canonical_segment_period_type(value: Any) -> str:
    token = _token(value).replace(" ", "_")
    try:
        return SEGMENT_PERIOD_TYPE_ALIASES[token]
    except KeyError as exc:
        raise SegmentNormalizationError(f"Unsupported segment period type {value!r}.") from exc


def canonical_segment_source_scope(value: Any) -> str:
    token = _token(value).replace(" ", "_")
    try:
        return SEGMENT_SOURCE_SCOPE_ALIASES[token]
    except KeyError as exc:
        raise SegmentNormalizationError(f"Unsupported segment source-table scope {value!r}.") from exc


def canonical_segment_dimension_member(dimension: Any, member: Any) -> tuple[str, str]:
    raw_pair = (str(dimension or ""), str(member or ""))
    dimension_token = _token(dimension).replace(" ", "_")
    member_token = _token(member)
    dimension_is_total = dimension_token == "total_company"
    member_is_total = member_token in _TOTAL_COMPANY_ALIASES
    canonical_pair = (dimension_token, "total_company" if member_is_total else member_token)
    if member_is_total and not dimension_is_total:
        raise SegmentNormalizationError(
            f"Invalid segment pair: raw_pair={raw_pair!r}, canonical_pair={canonical_pair!r}; "
            "a Total Company member alias requires dimension 'total_company'.",
            raw_pair=raw_pair,
            canonical_pair=canonical_pair,
        )
    if dimension_is_total and not member_is_total:
        raise SegmentNormalizationError(
            f"Invalid segment pair: raw_pair={raw_pair!r}, canonical_pair={canonical_pair!r}; "
            "dimension 'total_company' requires a Total Company member alias.",
            raw_pair=raw_pair,
            canonical_pair=canonical_pair,
        )
    return dimension_token, "total_company" if dimension_is_total else member_token


def canonical_segment_display_member(dimension: Any, member: Any) -> str:
    """Return the stable workbook label while retaining non-total source labels."""

    canonical_pair = canonical_segment_dimension_member(dimension, member)
    if canonical_pair == ("total_company", "total_company"):
        return "Total Company"
    return str(member or "").strip()


def canonical_segment_member(dimension: Any, member: Any) -> str:
    return canonical_segment_dimension_member(dimension, member)[1]


def segment_aggregation_role(dimension: Any, member: Any) -> str:
    canonical_dimension, _ = canonical_segment_dimension_member(dimension, member)
    return "reported_total" if canonical_dimension == "total_company" else "dimension_member"


def canonical_segment_business_identity(item: Mapping[str, Any]) -> tuple[str, str, str, str, str]:
    period_type = canonical_segment_period_type(item.get("period_type"))
    dimension, member = canonical_segment_dimension_member(item.get("dimension"), item.get("member"))
    return (
        period_type,
        str(item.get("period") or "").strip(),
        dimension,
        member,
        _token(item.get("metric")).replace(" ", "_"),
    )


def normalize_segment_currency_to_millions(
    value: Any,
    *,
    source_unit: Any,
    source_scale: Any,
) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise SegmentNormalizationError(f"Segment currency value must be numeric, received {value!r}.")
    unit = str(source_unit or "").strip().lower()
    scale = _token(source_scale).replace(" ", "_")
    if scale not in SEGMENT_SOURCE_SCALES - {"not_applicable"}:
        raise SegmentNormalizationError(f"Unsupported segment source scale {source_scale!r}.")
    if unit in {"$m", "usdm"}:
        if scale != "millions":
            raise SegmentNormalizationError(
                f"A source value declared in $m must use source_scale='millions', received {source_scale!r}."
            )
        multiplier = 1.0
    elif unit in {"$", "usd"}:
        multiplier = {"ones": 0.000001, "thousands": 0.001, "millions": 1.0}[scale]
    else:
        raise SegmentNormalizationError(f"Unsupported segment currency unit {source_unit!r}.")
    return round(float(value) * multiplier, 3)


@dataclass(frozen=True)
class SegmentSourceFact:
    metric: str
    value: float
    source_unit: str
    source_scale: str
    period_type: str
    period: str
    dimension: str
    member: str
    source_table_scope: str
    source_table_id: str
    source_row_ref: str
    source_ref: str

    def __post_init__(self) -> None:
        period_type = canonical_segment_period_type(self.period_type)
        scope = canonical_segment_source_scope(self.source_table_scope)
        if period_type != scope:
            raise SegmentNormalizationError(
                f"Segment source scope {scope!r} is incompatible with period type {period_type!r} "
                f"for {self.source_row_ref}."
            )
        period_re = _QUARTER_RE if period_type == "quarterly" else _ANNUAL_RE
        if not period_re.fullmatch(str(self.period or "")):
            raise SegmentNormalizationError(
                f"Segment period {self.period!r} is incompatible with period type {period_type!r} "
                f"for {self.source_row_ref}."
            )
        if _token(self.source_scale).replace(" ", "_") not in SEGMENT_SOURCE_SCALES:
            raise SegmentNormalizationError(f"Unsupported segment source scale {self.source_scale!r}.")
        for field_name in ("metric", "dimension", "member", "source_table_id", "source_row_ref", "source_ref"):
            if not str(getattr(self, field_name) or "").strip():
                raise SegmentNormalizationError(f"Segment source fact requires {field_name}.")
        try:
            canonical_segment_dimension_member(self.dimension, self.member)
        except SegmentNormalizationError as exc:
            raw_pair = (self.dimension, self.member)
            canonical_pair = exc.canonical_pair
            business_key = "|".join(
                (
                    period_type,
                    self.period,
                    *(canonical_pair or raw_pair),
                    _token(self.metric).replace(" ", "_"),
                )
            )
            raise SegmentNormalizationError(
                f"{exc} source_row_ref={self.source_row_ref!r}, business_key={business_key!r}.",
                raw_pair=raw_pair,
                canonical_pair=canonical_pair,
                source_row_ref=self.source_row_ref,
                business_key=business_key,
            ) from exc

    @property
    def normalized_value(self) -> float:
        return normalize_segment_currency_to_millions(
            self.value,
            source_unit=self.source_unit,
            source_scale=self.source_scale,
        )

    @property
    def business_identity(self) -> tuple[str, str, str, str, str]:
        return canonical_segment_business_identity(
            {
                "period_type": self.period_type,
                "period": self.period,
                "dimension": self.dimension,
                "member": self.member,
                "metric": self.metric,
            }
        )

    def metadata(self) -> dict[str, str]:
        period_type = canonical_segment_period_type(self.period_type)
        return {
            "unit": "$m",
            "source_unit": self.source_unit,
            "source_scale": _token(self.source_scale).replace(" ", "_"),
            "source_table_scope": canonical_segment_source_scope(self.source_table_scope),
            "source_table_id": self.source_table_id,
            "source_row_ref": self.source_row_ref,
            "source_ref": self.source_ref,
            "aggregation_role": segment_aggregation_role(self.dimension, self.member),
            "period_type": period_type,
        }


def canonicalize_segment_source_facts(
    facts: Iterable[SegmentSourceFact],
) -> tuple[SegmentSourceFact, ...]:
    """Return one deterministic fact per canonical segment business identity."""

    by_identity: dict[tuple[str, str, str, str, str], SegmentSourceFact] = {}
    for fact in facts:
        identity = fact.business_identity
        prior = by_identity.get(identity)
        if prior is not None:
            raw_pair = (fact.dimension, fact.member)
            canonical_pair = identity[2:4]
            business_key = "|".join(identity)
            raise SegmentNormalizationError(
                "Duplicate canonical segment business identity "
                f"{identity!r}; first_raw_pair={(prior.dimension, prior.member)!r}, "
                f"duplicate_raw_pair={raw_pair!r}, canonical_pair={canonical_pair!r}, "
                f"first_source_row_ref={prior.source_row_ref!r}, "
                f"duplicate_source_row_ref={fact.source_row_ref!r}, business_key={business_key!r}.",
                raw_pair=raw_pair,
                canonical_pair=canonical_pair,
                source_row_ref=fact.source_row_ref,
                business_key=business_key,
            )
        by_identity[identity] = fact
    return tuple(by_identity[identity] for identity in sorted(by_identity))
