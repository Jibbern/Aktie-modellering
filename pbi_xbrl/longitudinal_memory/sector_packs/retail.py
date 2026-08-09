"""Generic retail metric, dimension and lossless value semantics."""
from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable, Mapping

from pbi_xbrl.longitudinal_memory.calendar_rules import (
    IncomparablePeriodError,
    compare_periods,
)
from pbi_xbrl.longitudinal_memory.identity import dimension_set_identity
from pbi_xbrl.longitudinal_memory.types import canonical_decimal


METRICS: Mapping[str, str] = {
    "comparable-sales": "metric:retail:comparable-sales@1",
    "store-openings": "metric:retail:store-openings@1",
    "store-closures": "metric:retail:store-closures@1",
    "ending-stores": "metric:retail:ending-stores@1",
    "net-store-openings": "metric:retail:net-store-openings@1",
    "revenue-growth": "metric:core:revenue-growth@1",
    "operating-margin": "metric:core:operating-margin@1",
}
DEFINITION_REPORTED = "definition:core:company-reported@1"
DEFINITION_GUIDANCE = "definition:core:company-guidance@1"
BASIS_REPORTED = "basis:core:reported@1"
BASIS_GUIDED = "basis:core:guided@1"
UNIT_PERCENT = "unit:core:percent@1"
UNIT_PERCENTAGE_POINT = "unit:core:percentage-point@1"
UNIT_COUNT = "unit:core:count@1"

# Product@2 extends the retail semantic vocabulary without changing the accepted
# sector-pack:retail:longitudinal@1 catalog or its serialized Product@1 inputs.
V2_METRICS: Mapping[str, str] = {
    **METRICS,
    "net-income-per-diluted-share": "metric:core:net-income-per-diluted-share@1",
    "net-income-per-diluted-share-adjusted": "metric:core:net-income-per-diluted-share@1",
    "capital-expenditures": "metric:core:capital-expenditures@1",
    "property-equipment-purchases": "metric:core:property-equipment-purchases@1",
    "share-repurchases": "metric:core:share-repurchases@1",
    "diluted-weighted-average-shares": "metric:core:diluted-weighted-average-shares@1",
    "store-closures-count": "metric:retail:store-closures-count@1",
    "store-right-sizes": "metric:retail:store-right-sizes@1",
    "store-remodels": "metric:retail:store-remodels@1",
    "store-remodels-right-sizes": "metric:retail:store-remodels-right-sizes@1",
    "operating-margin-adjusted-litigation-excluded": METRICS["operating-margin"],
}
DEFINITION_ADJUSTED_LITIGATION_EXCLUDED = (
    "definition:core:adjusted-excluding-litigation-benefit@1"
)
DEFINITION_ADJUSTED_NON_GAAP = "definition:core:adjusted-non-gaap@1"
DEFINITION_PROPERTY_EQUIPMENT_PURCHASES = (
    "definition:core:purchases-of-property-and-equipment@1"
)
BASIS_ADJUSTED_LITIGATION_EXCLUDED = "basis:core:adjusted-excluding-litigation-benefit@1"
BASIS_ADJUSTED_NON_GAAP = "basis:core:adjusted-non-gaap@1"
UNIT_CURRENCY_PER_SHARE = "unit:core:currency-per-share@1"
UNIT_CURRENCY_MILLION = "unit:core:currency-million@1"
UNIT_SHARES_MILLION = "unit:core:shares-million@1"
DIMENSIONS: Mapping[str, str] = {
    "company": "dimension:core:company@1",
    "geography": "dimension:core:geography@1",
    "brand": "dimension:core:brand@1",
}


class RetailSemanticError(ValueError):
    """Raised instead of coercing a retail assertion into the wrong value form."""


def _decimal(value: str) -> str:
    return canonical_decimal(Decimal(value.replace(",", "")))


def parse_percent_text(text: str) -> dict[str, Any]:
    normalized = " ".join(text.split())
    parenthetical = re.search(r"\(([0-9]+(?:\.[0-9]+)?)\)\s*%", normalized)
    if parenthetical:
        return {"kind": "exact", "value": canonical_decimal(-Decimal(parenthetical.group(1)))}
    direct = re.search(r"(?<![0-9])(-?[0-9]+(?:\.[0-9]+)?)\s*%", normalized)
    if not direct:
        raise RetailSemanticError(f"No exact percent is present in {text!r}.")
    return {"kind": "exact", "value": _decimal(direct.group(1))}


def parse_percent_fraction(text: str) -> dict[str, Any]:
    return {"kind": "exact", "value": canonical_decimal(Decimal(text) * Decimal("100"))}


def parse_count_text(text: str) -> dict[str, Any]:
    normalized = " ".join(text.split())
    parenthetical = re.fullmatch(r"\(([0-9]+)\)", normalized)
    if parenthetical:
        return {"kind": "exact", "value": canonical_decimal(-Decimal(parenthetical.group(1)))}
    direct = re.fullmatch(r"-?[0-9]+", normalized.replace(",", ""))
    if not direct:
        raise RetailSemanticError(f"No exact count is present in {text!r}.")
    return {"kind": "exact", "value": canonical_decimal(Decimal(direct.group(0)))}


def parse_guidance_percent(text: str) -> dict[str, Any]:
    normalized = " ".join(text.replace("–", "-").replace("—", "-").split())
    mixed_currency_percent = re.search(
        r"(?i)(?:\$\s*[+-]?[0-9]+(?:\.[0-9]+)?\s*(?:to|-)\s*"
        r"[+-]?[0-9]+(?:\.[0-9]+)?\s*%|"
        r"[+-]?[0-9]+(?:\.[0-9]+)?\s*%\s*(?:to|-)\s*\$\s*"
        r"[+-]?[0-9]+(?:\.[0-9]+)?)",
        normalized,
    )
    if mixed_currency_percent:
        raise RetailSemanticError(
            f"Mixed currency/percentage guidance is not a percentage range: {text!r}."
        )
    approximate_range = re.search(
        r"(?i)(?:(?:around|about|approximately)\s+|~\s*)"
        r"[+-]?[0-9]+(?:\.[0-9]+)?\s*(?:%\s*)?(?:to|-)\s*"
        r"[+-]?[0-9]+(?:\.[0-9]+)?\s*%",
        normalized,
    )
    if approximate_range:
        raise RetailSemanticError(
            "Approximate ranges require an explicit typed approximate-range value form; "
            "the approximation qualifier cannot be discarded."
        )
    range_match = re.search(
        r"(?i)(?<![$\w.])(?:range\s+of\s+)?"
        r"([+-]?[0-9]+(?:\.[0-9]+)?)\s*(?:%\s*)?(?:to|-)\s*"
        r"([+-]?[0-9]+(?:\.[0-9]+)?)\s*%",
        normalized,
    )
    if range_match:
        return {
            "kind": "range",
            "low": _decimal(range_match.group(1)),
            "high": _decimal(range_match.group(2)),
            "low_inclusive": True,
            "high_inclusive": True,
        }
    bound = re.search(r"(?i)at\s+least\s+(-?[0-9]+(?:\.[0-9]+)?)\s*%", normalized)
    if bound:
        return {"kind": "bound", "operator": "gte", "value": _decimal(bound.group(1))}
    approximate = re.search(
        r"(?i)(around|about|approximately|~)\s*(-?[0-9]+(?:\.[0-9]+)?)\s*%",
        normalized,
    )
    if approximate:
        qualifier = "tilde" if approximate.group(1) == "~" else approximate.group(1).lower()
        return {
            "kind": "approximate",
            "value": _decimal(approximate.group(2)),
            "qualifier": qualifier,
            "tolerance": None,
        }
    return parse_percent_text(normalized)


def parse_guidance_percent_v2(text: str) -> dict[str, Any]:
    """Parse the additional lossless percentage forms reviewed for Product@2."""

    normalized = " ".join(
        text.replace("â€“", "-").replace("â€”", "-").replace("–", "-").replace("—", "-").split()
    )
    flat_to_up = re.search(
        r"(?i)flat\s+to\s+(?:be\s+)?up\s+([0-9]+(?:\.[0-9]+)?)\s*%",
        normalized,
    )
    if flat_to_up:
        return {
            "kind": "range",
            "low": "0",
            "high": _decimal(flat_to_up.group(1)),
            "low_inclusive": True,
            "high_inclusive": True,
        }
    directional_range = re.search(
        r"(?i)\b(down|up)\b(?:\s+in)?(?:\s+the)?(?:\s+range\s+of)?\s+"
        r"([0-9]+(?:\.[0-9]+)?)\s*(?:%\s*)?(?:to|-)\s*"
        r"([0-9]+(?:\.[0-9]+)?)\s*%",
        normalized,
    )
    if directional_range:
        direction, first, second = directional_range.groups()
        low = Decimal(first)
        high = Decimal(second)
        if direction.casefold() == "down":
            low, high = -max(low, high), -min(low, high)
        else:
            low, high = min(low, high), max(low, high)
        return {
            "kind": "range",
            "low": canonical_decimal(low),
            "high": canonical_decimal(high),
            "low_inclusive": True,
            "high_inclusive": True,
        }
    down_mid_single = re.search(r"(?i)down\s+(?:in\s+the\s+)?mid-single-digits", normalized)
    if down_mid_single:
        return {
            "kind": "qualitative",
            "text": down_mid_single.group(0),
            "normalized_band": "negative-mid-single-digits",
        }
    return parse_guidance_percent(normalized)


def parse_guidance_currency_per_share(text: str) -> dict[str, Any]:
    normalized = " ".join(text.replace("â€“", "-").replace("–", "-").split())
    match = re.search(
        r"(?i)(?:range\s+of\s+)?\$?\s*([0-9]+(?:\.[0-9]+)?)\s*(?:to|-)\s*"
        r"\$?\s*([0-9]+(?:\.[0-9]+)?)",
        normalized,
    )
    if not match:
        raise RetailSemanticError(f"No per-share guidance range is present in {text!r}.")
    return {
        "kind": "range",
        "low": _decimal(match.group(1)),
        "high": _decimal(match.group(2)),
        "low_inclusive": True,
        "high_inclusive": True,
    }


def parse_guidance_currency_millions(text: str) -> dict[str, Any]:
    normalized = " ".join(
        text.replace("\u2013", "-")
        .replace("\u2014", "-")
        .replace("\u2212", "-")
        .split()
    )
    number = r"[0-9]+(?:\.[0-9]+)?"
    scale = r"millions?|m|billions?|bn|%|shares?"
    range_pattern = re.compile(
        rf"(?ix)"
        rf"(?P<approx>~|approximately|around|about)?\s*"
        rf"(?:(?:in\s+)?(?:the\s+)?range\s+of\s+)?"
        rf"(?P<low_currency>\$|\u20ac)?\s*"
        rf"(?P<low>{number})\s*"
        rf"(?P<low_scale>{scale})?\s*"
        rf"(?:to|-)\s*"
        rf"(?P<high_currency>\$|\u20ac)?\s*"
        rf"(?P<high>{number})\s*"
        rf"(?P<high_scale>{scale})?(?![A-Za-z])"
    )
    range_candidates = [
        candidate
        for candidate in range_pattern.finditer(normalized)
        if any(
            candidate.group(name) is not None
            for name in ("low_currency", "high_currency", "low_scale", "high_scale")
        )
    ]
    if len(range_candidates) > 1:
        raise RetailSemanticError(
            f"Multiple currency-range candidates are present in {text!r}; select one reviewed value."
        )
    if range_candidates:
        candidate = range_candidates[0]
        if candidate.group("approx") is not None:
            raise RetailSemanticError(
                "Approximate currency-million ranges require an explicit typed "
                "approximate-range value form; the approximation qualifier cannot be discarded."
            )
        low_currency = candidate.group("low_currency")
        high_currency = candidate.group("high_currency")
        if low_currency != "$" or high_currency not in {None, "$"}:
            raise RetailSemanticError(
                f"A USD-million guidance range must identify USD on the lower endpoint "
                f"without mixing currencies: {text!r}."
            )
        million_scales = {"m", "million", "millions"}
        low_scale = (
            candidate.group("low_scale").casefold()
            if candidate.group("low_scale") is not None
            else None
        )
        high_scale = (
            candidate.group("high_scale").casefold()
            if candidate.group("high_scale") is not None
            else None
        )
        if (
            low_scale not in million_scales | {None}
            or high_scale not in million_scales | {None}
            or (low_scale is None and high_scale is None)
        ):
            raise RetailSemanticError(
                f"Currency-million guidance range mixes or omits endpoint scales: {text!r}."
            )
        low = _decimal(candidate.group("low"))
        high = _decimal(candidate.group("high"))
        if Decimal(low) > Decimal(high):
            raise RetailSemanticError(
                f"Currency-million guidance range lower endpoint exceeds upper endpoint: {text!r}."
            )
        return {
            "kind": "range",
            "low": low,
            "high": high,
            "low_inclusive": True,
            "high_inclusive": True,
        }

    point_text = re.sub(r"(?i)\bapproximately\b", "~", normalized)
    match = re.search(
        r"(?i)(~|around|about)?\s*\$?\s*([0-9]+(?:\.[0-9]+)?)\s*m(?:illion)?",
        point_text,
    )
    if not match:
        raise RetailSemanticError(f"No currency-million guidance value is present in {text!r}.")
    value = _decimal(match.group(2))
    if match.group(1) is not None:
        qualifier = "tilde" if match.group(1) == "~" else match.group(1).casefold()
        return {"kind": "approximate", "value": value, "qualifier": qualifier, "tolerance": None}
    return {"kind": "exact", "value": value}


def parse_currency_millions(text: str) -> dict[str, Any]:
    """Parse a source-reported USD-million amount without matching share counts."""

    normalized = " ".join(text.replace("approximately", "~").split())
    match = re.search(
        r"(?i)(~|around|about)?\s*\$\s*([0-9]+(?:\.[0-9]+)?)\s*m(?:illion)?",
        normalized,
    )
    if not match:
        raise RetailSemanticError(f"No explicit currency-million amount is present in {text!r}.")
    value = _decimal(match.group(2))
    if match.group(1) is not None:
        qualifier = "tilde" if match.group(1) == "~" else match.group(1).casefold()
        return {"kind": "approximate", "value": value, "qualifier": qualifier, "tolerance": None}
    return {"kind": "exact", "value": value}


def parse_currency_per_share(text: str) -> dict[str, Any]:
    normalized = " ".join(text.split())
    match = re.search(r"(?<![0-9])\$?\s*([0-9]+(?:\.[0-9]+)?)", normalized)
    if not match:
        raise RetailSemanticError(f"No per-share currency value is present in {text!r}.")
    return {"kind": "exact", "value": _decimal(match.group(1))}


def parse_decimal_percent(text: str) -> dict[str, Any]:
    normalized = " ".join(text.split()).replace(",", "")
    parenthetical = re.fullmatch(r"\(([0-9]+(?:\.[0-9]+)?)\)", normalized)
    if parenthetical:
        return {"kind": "exact", "value": canonical_decimal(-Decimal(parenthetical.group(1)))}
    if not re.fullmatch(r"-?[0-9]+(?:\.[0-9]+)?", normalized):
        raise RetailSemanticError(f"No bare decimal percentage is present in {text!r}.")
    return {"kind": "exact", "value": canonical_decimal(Decimal(normalized))}


def parse_currency_thousands_to_millions(text: str) -> dict[str, Any]:
    """Normalize a positive purchase amount reported in thousands into USD millions."""

    normalized = " ".join(text.split())
    match = re.search(r"\(?\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*\)?", normalized)
    if not match:
        raise RetailSemanticError(f"No currency-thousands value is present in {text!r}.")
    value = Decimal(match.group(1).replace(",", "")) / Decimal("1000")
    return {"kind": "exact", "value": canonical_decimal(value)}


def parse_shares_thousands_to_millions(text: str) -> dict[str, Any]:
    """Normalize a weighted-average share count reported in thousands to millions."""

    normalized = " ".join(text.split())
    match = re.search(r"\(?\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*\)?", normalized)
    if not match:
        raise RetailSemanticError(f"No shares-thousands value is present in {text!r}.")
    value = Decimal(match.group(1).replace(",", "")) / Decimal("1000")
    return {"kind": "exact", "value": canonical_decimal(value)}


def parse_guidance_shares_millions(text: str) -> dict[str, Any]:
    normalized = " ".join(text.replace("approximately", "~").split())
    match = re.search(
        r"(?i)(~|around|about)?\s*([0-9]+(?:\.[0-9]+)?)\s*million(?:\s+shares)?",
        normalized,
    )
    if not match:
        raise RetailSemanticError(f"No shares-million guidance value is present in {text!r}.")
    value = _decimal(match.group(2))
    if match.group(1) is not None:
        qualifier = "tilde" if match.group(1) == "~" else match.group(1).casefold()
        return {"kind": "approximate", "value": value, "qualifier": qualifier, "tolerance": None}
    return {"kind": "exact", "value": value}


def _parse_guidance_count(text: str, noun_pattern: str) -> dict[str, Any]:
    normalized = " ".join(text.replace("approximately", "~").split())
    match = re.search(
        rf"(?i)(~|around|about)?\s*([0-9]+)\s+{noun_pattern}", normalized
    )
    if not match:
        raise RetailSemanticError(f"No reviewed guidance count is present in {text!r}.")
    value = canonical_decimal(match.group(2))
    qualifier = match.group(1)
    if qualifier is not None:
        return {
            "kind": "approximate",
            "value": value,
            "qualifier": "tilde" if qualifier == "~" else qualifier.casefold(),
            "tolerance": None,
        }
    return {"kind": "exact", "value": value}


def parse_guidance_store_openings(text: str) -> dict[str, Any]:
    return _parse_guidance_count(text, r"openings?\b")


def parse_guidance_store_closures(text: str) -> dict[str, Any]:
    return _parse_guidance_count(text, r"closures?\b")


def parse_guidance_store_remodels(text: str) -> dict[str, Any]:
    return _parse_guidance_count(text, r"remodels?(?:\s+and\s+right-sizes?)?\b")


def parse_reported_store_right_sizes(text: str) -> dict[str, Any]:
    return _parse_guidance_count(text, r"right[ -]?sizes?\b")


def parse_reported_store_remodels(text: str) -> dict[str, Any]:
    return _parse_guidance_count(text, r"remodels?\b")


def derive_store_remodels_right_sizes(
    right_sizes: Mapping[str, Any], remodels: Mapping[str, Any]
) -> dict[str, Any]:
    """Combine two exact, source-backed store-activity components losslessly."""

    if right_sizes.get("kind") != "exact" or remodels.get("kind") != "exact":
        raise RetailSemanticError(
            "Remodel/right-size Actual derivation requires two exact component facts."
        )
    return {
        "kind": "exact",
        "value": canonical_decimal(
            Decimal(str(right_sizes["value"])) + Decimal(str(remodels["value"]))
        ),
    }


def _as_reviewed_approximate_count(value: Mapping[str, Any]) -> dict[str, Any]:
    """Apply an explicit reviewed "all approximate" table qualifier to one count."""

    return {
        "kind": "approximate",
        "value": canonical_decimal(value["value"]),
        "qualifier": "approximately",
        "tolerance": None,
    }


def parse_approximate_guidance_store_openings(text: str) -> dict[str, Any]:
    return _as_reviewed_approximate_count(parse_guidance_store_openings(text))


def parse_approximate_guidance_store_closures(text: str) -> dict[str, Any]:
    return _as_reviewed_approximate_count(parse_guidance_store_closures(text))


def parse_approximate_guidance_store_remodels(text: str) -> dict[str, Any]:
    return _as_reviewed_approximate_count(parse_guidance_store_remodels(text))


def parse_absolute_count_text(text: str) -> dict[str, Any]:
    value = parse_count_text(text)
    return {"kind": "exact", "value": canonical_decimal(abs(Decimal(str(value["value"]))))}


def parse_store_plan_target(text: str) -> dict[str, Any]:
    normalized = " ".join(text.replace("approximately", "~").split())
    match = re.search(r"(?i)~\s*([0-9]+)\s+net\s+store\s+openings", normalized)
    if not match:
        raise RetailSemanticError("Store-plan evidence lacks an approximate net-opening target.")
    return {
        "kind": "approximate",
        "value": canonical_decimal(match.group(1)),
        "qualifier": "tilde",
        "tolerance": None,
    }


def parse_net_openings_table(text: str) -> dict[str, Any]:
    normalized = " ".join(text.split())
    sections = re.split(r"(?i)Permanently\s+closed", normalized, maxsplit=1)
    if len(sections) != 2 or re.search(r"(?i)\bNew\b", sections[0]) is None:
        raise RetailSemanticError(
            "Net openings require explicit opening and signed closure rows in one table occurrence."
        )
    opening_values = re.findall(r"\(?-?[0-9]+\)?", re.split(r"(?i)\bNew\b", sections[0], maxsplit=1)[1])
    closure_values = re.findall(r"\(?-?[0-9]+\)?", sections[1])
    if not opening_values or not closure_values:
        raise RetailSemanticError(
            "Net openings require numeric opening and signed closure row totals."
        )
    value = derive_net_openings(
        parse_count_text(opening_values[-1]),
        parse_count_text(closure_values[-1]),
    )
    return {"kind": "exact", "value": value}


def derive_net_openings(openings: Mapping[str, Any], closures: Mapping[str, Any]) -> str:
    if openings.get("kind") != "exact" or closures.get("kind") != "exact":
        raise RetailSemanticError("Net openings require exact source-backed opening and closure counts.")
    return canonical_decimal(Decimal(str(openings["value"])) + Decimal(str(closures["value"])))


@dataclass(frozen=True)
class RetailSectorPack:
    sector_pack_id: str = "sector-pack:retail:longitudinal@1"

    @property
    def metrics(self) -> Mapping[str, str]:
        return METRICS

    @property
    def change_metric_id(self) -> str:
        """Metric enabled for the bounded retail QoQ/YoY proof."""

        return METRICS["comparable-sales"]

    @property
    def total_dimension_alias(self) -> str:
        return "total company"

    @property
    def percentage_point_unit_id(self) -> str:
        return UNIT_PERCENTAGE_POINT

    @staticmethod
    def derive_net_openings(
        openings: Mapping[str, Any], closures: Mapping[str, Any]
    ) -> str:
        return derive_net_openings(openings, closures)

    def parse_value(self, parser_id: str, text: str) -> dict[str, Any]:
        parsers = {
            "parser:retail:percent-text@1": parse_percent_text,
            "parser:retail:percent-fraction@1": parse_percent_fraction,
            "parser:retail:count-text@1": parse_count_text,
            "parser:retail:guidance-percent@1": parse_guidance_percent,
            "parser:retail:store-plan@1": parse_store_plan_target,
            "parser:retail:net-openings-table@1": parse_net_openings_table,
        }
        parser = parsers.get(parser_id)
        if parser is None:
            raise RetailSemanticError(f"Unknown retail parser {parser_id!r}.")
        return parser(text)

    def metric_semantics(self, metric_key: str, *, guidance: bool = False) -> tuple[str, str, str, str]:
        metric_id = self.metrics.get(metric_key)
        if metric_id is None:
            raise RetailSemanticError(f"Unknown retail metric key {metric_key!r}.")
        unit_id = UNIT_COUNT if metric_key in {"store-openings", "store-closures", "ending-stores", "net-store-openings"} else UNIT_PERCENT
        if guidance:
            return metric_id, DEFINITION_GUIDANCE, BASIS_GUIDED, unit_id
        return metric_id, DEFINITION_REPORTED, BASIS_REPORTED, unit_id

    @staticmethod
    def assertion_mode(metric_key: str) -> str:
        return "derived" if metric_key == "net-store-openings" else "reported"

    @staticmethod
    def permitted_source_families(candidate_kind: str) -> frozenset[str]:
        policies = {
            "numerical_fact": frozenset({"sec-exhibit", "issuer-spreadsheet"}),
            "guidance": frozenset({"sec-exhibit", "issuer-pdf", "issuer-transcript"}),
            "promise_version": frozenset({"sec-exhibit", "issuer-pdf"}),
            "management_statement": frozenset({"issuer-transcript"}),
            "company_event": frozenset({"issuer-transcript"}),
            "period_evidence": frozenset({"sec-exhibit"}),
        }
        try:
            return policies[candidate_kind]
        except KeyError as exc:
            raise RetailSemanticError(
                f"No retail source-eligibility policy exists for {candidate_kind!r}."
            ) from exc

    def percentage_point_change_requests(
        self,
        periods: Iterable[Mapping[str, Any]],
        selected: Mapping[tuple[str, str, str], Mapping[str, Any]],
        *,
        total_dimension_id: str,
        calendar: Mapping[str, Any],
    ) -> tuple[tuple[str, Mapping[str, Any], Mapping[str, Any], Mapping[str, Any], Mapping[str, Any]], ...]:
        period_by_id = {str(row["period_id"]): row for row in periods}
        facts = [
            row
            for (metric_id, _period_id, dimension_id), row in selected.items()
            if metric_id == self.change_metric_id and dimension_id == total_dimension_id
        ]
        quarterly = [
            row
            for row in facts
            if period_by_id[str(row["header"]["effective_period_id"])]["period_type"] == "quarter"
        ]
        if not quarterly:
            raise RetailSemanticError("Comparable-sales change derivation requires fiscal-quarter facts.")
        later = max(
            quarterly,
            key=lambda row: int(period_by_id[str(row["header"]["effective_period_id"])]["fiscal_ordinal"]),
        )
        later_period = period_by_id[str(later["header"]["effective_period_id"])]

        def unique_period(*, ordinal: int, fiscal_quarter: int) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
            matches = [
                (row, period_by_id[str(row["header"]["effective_period_id"])])
                for row in quarterly
                if period_by_id[str(row["header"]["effective_period_id"])]["fiscal_ordinal"] == ordinal
                and period_by_id[str(row["header"]["effective_period_id"])]["fiscal_quarter"] == fiscal_quarter
            ]
            if len(matches) != 1:
                raise RetailSemanticError(
                    "Comparable-sales change derivation requires one unambiguous selected period input."
                )
            return matches[0]

        qoq_fact, qoq_period = unique_period(
            ordinal=int(later_period["fiscal_ordinal"]) - 1,
            fiscal_quarter=int(later_period["fiscal_quarter"]) - 1,
        )
        yoy_fact, yoy_period = unique_period(
            ordinal=int(later_period["fiscal_ordinal"]) - 4,
            fiscal_quarter=int(later_period["fiscal_quarter"]),
        )
        requests = (
            ("qoq-percentage-point", qoq_fact, later, qoq_period, later_period),
            ("yoy-percentage-point", yoy_fact, later, yoy_period, later_period),
        )
        try:
            for change_kind, _earlier, _later, earlier_period, current_period in requests:
                compare_periods(
                    earlier_period,
                    current_period,
                    earlier_calendar=calendar,
                    later_calendar=calendar,
                    change_kind=change_kind,
                )
        except IncomparablePeriodError as exc:
            raise RetailSemanticError(str(exc)) from exc
        return requests

    def promise_evidence_assessment(
        self, observations: Iterable[Mapping[str, Any]], eligible_states: frozenset[str]
    ) -> Mapping[str, Any]:
        def one(metric_key: str) -> Mapping[str, Any]:
            matches = [
                row
                for row in observations
                if row.get("payload", {}).get("kind") == "NumericalFact"
                and row.get("payload", {}).get("metric_id") == self.metrics[metric_key]
                and row.get("header", {}).get("review_state") in eligible_states
            ]
            if len(matches) != 1:
                raise RetailSemanticError(
                    f"Store-plan evidence requires one eligible {metric_key} fact."
                )
            return matches[0]

        openings = one("store-openings")
        closures = one("store-closures")
        net = one("net-store-openings")
        derived_net = self.derive_net_openings(
            openings["payload"]["value"],
            closures["payload"]["value"],
        )
        if net["payload"]["value"] != {"kind": "exact", "value": derived_net}:
            raise RetailSemanticError(
                "Source-table net openings do not replay opening plus signed closure counts."
            )
        return {
            "evidence_record": net,
            "candidate_records": (openings, closures, net),
            "relation_rule_id": "rule:core:promise-evidence@1",
            "review_rule_id": "promise_approximate_tolerance_missing",
            "message": (
                "The approximate store target has no source-supplied tolerance and "
                "cannot be marked achieved automatically."
            ),
            "action": "Obtain an explicit tolerance or perform reviewed promise assessment.",
        }

    def dimension_sets(
        self,
        aliases: Iterable[Mapping[str, str]],
    ) -> dict[str, tuple[str, tuple[tuple[str, str], ...]]]:
        grouped: dict[str, dict[str, str]] = {}
        for row in aliases:
            alias = " ".join(str(row["alias"]).split()).casefold()
            grouped.setdefault(alias, {})[str(row["axis"])] = str(row["member_id"])
        total_alias = grouped.get("total company")
        if total_alias is None or "company" not in total_alias:
            raise RetailSemanticError("A retail profile requires an explicit total-company member.")
        result: dict[str, tuple[str, tuple[tuple[str, str], ...]]] = {}
        total_pair = (DIMENSIONS["company"], total_alias["company"])
        for alias, axes in grouped.items():
            pairs: list[tuple[str, str]] = [total_pair]
            for axis, member_id in axes.items():
                if axis == "company":
                    continue
                if axis not in DIMENSIONS:
                    raise RetailSemanticError(f"Unknown retail dimension axis {axis!r}.")
                pairs.append((DIMENSIONS[axis], member_id))
            normalized = tuple(sorted(set(pairs)))
            result[alias] = (dimension_set_identity(normalized), normalized)
        return result

    def catalog(
        self,
        dimension_sets: Mapping[str, tuple[str, tuple[tuple[str, str], ...]]],
        aliases: Iterable[Mapping[str, str]],
        methods: Iterable[Mapping[str, Any]],
    ) -> dict[str, Any]:
        def common(key: str, identity: str, name: str, description: str) -> dict[str, Any]:
            return {
                key: identity,
                "display_name": name,
                "description": description,
                "aliases": [],
                "status": "active",
                "supersedes_id": None,
            }

        member_aliases: dict[str, list[Mapping[str, str]]] = {}
        for row in aliases:
            member_aliases.setdefault(str(row["member_id"]), []).append(row)
        members = []
        for member_id, rows in sorted(member_aliases.items()):
            ordered_aliases = sorted({str(row["alias"]) for row in rows}, key=lambda value: (len(value), value.casefold()))
            axis = str(rows[0]["axis"])
            members.append(
                {
                    "member_id": member_id,
                    "dimension_id": DIMENSIONS[axis],
                    "scope": axis,
                    "display_name": ordered_aliases[0],
                    "aliases": ordered_aliases[1:],
                    "status": "active",
                    "supersedes_id": None,
                }
            )
        return {
            "metrics": [
                common("metric_id", metric_id, key.replace("-", " ").title(), f"Versioned {key} metric.")
                for key, metric_id in sorted(self.metrics.items())
            ],
            "definitions": [
                {**common("definition_id", DEFINITION_REPORTED, "Company reported", "Company-reported definition."), "gaap_status": "operational"},
                {**common("definition_id", DEFINITION_GUIDANCE, "Company guidance", "Company guidance definition."), "gaap_status": "operational"},
            ],
            "bases": [
                {**common("basis_id", BASIS_REPORTED, "Reported", "Reported realized basis."), "realization_state": "reported"},
                {**common("basis_id", BASIS_GUIDED, "Guided", "Forward guided basis."), "realization_state": "guided"},
            ],
            "units": [
                {"unit_id": UNIT_PERCENT, "display_name": "Percent", "unit_kind": "percent", "scale": "1", "currency_behavior": "forbidden", "aliases": ["%"], "status": "active", "supersedes_id": None},
                {"unit_id": UNIT_PERCENTAGE_POINT, "display_name": "Percentage point", "unit_kind": "percentage-point", "scale": "1", "currency_behavior": "forbidden", "aliases": ["pp"], "status": "active", "supersedes_id": None},
                {"unit_id": UNIT_COUNT, "display_name": "Count", "unit_kind": "count", "scale": "1", "currency_behavior": "forbidden", "aliases": [], "status": "active", "supersedes_id": None},
            ],
            "dimensions": [
                common("dimension_id", identity, axis.title(), f"Retail {axis} axis.")
                for axis, identity in sorted(DIMENSIONS.items())
            ],
            "dimension_members": members,
            "dimension_sets": [
                {
                    "dimension_set_id": identity,
                    "members": [
                        {"dimension_id": dimension_id, "member_id": member_id}
                        for dimension_id, member_id in pairs
                    ],
                }
                for identity, pairs in sorted({value for value in dimension_sets.values()})
            ],
            "policies": [
                {"policy_id": f"policy:core:{name}@1", "assertion_type": name, "description": f"Assertion-specific {name} precedence."}
                for name in ("reported-numerical", "guidance", "management-explanation", "company-event", "model-interpretation")
            ],
            "change_rules": [
                {"rule_id": "rule:core:qoq-percentage-point@1", "change_kind": "qoq-percentage-point", "input_unit_kind": "percent", "output_unit_id": UNIT_PERCENTAGE_POINT, "description": "Adjacent-quarter percentage-point change."},
                {"rule_id": "rule:core:yoy-percentage-point@1", "change_kind": "yoy-percentage-point", "input_unit_kind": "percent", "output_unit_id": UNIT_PERCENTAGE_POINT, "description": "Same-quarter year-over-year percentage-point change."},
            ],
            "methods": sorted((dict(row) for row in methods), key=lambda row: str(row["method_id"])),
        }


RETAIL_SECTOR_PACK = RetailSectorPack()


@dataclass(frozen=True)
class RetailSectorPackV2(RetailSectorPack):
    """Versioned retail semantics used only by the Product@2 candidate source set."""

    sector_pack_id: str = "sector-pack:retail:longitudinal@2"

    @property
    def metrics(self) -> Mapping[str, str]:
        return V2_METRICS

    def parse_value(self, parser_id: str, text: str) -> dict[str, Any]:
        parsers = {
            "parser:retail:guidance-percent-v2@2": parse_guidance_percent_v2,
            "parser:retail:guidance-currency-per-share@1": parse_guidance_currency_per_share,
            "parser:retail:guidance-currency-millions@1": parse_guidance_currency_millions,
            "parser:retail:currency-millions@1": parse_currency_millions,
            "parser:retail:currency-per-share@1": parse_currency_per_share,
            "parser:retail:decimal-percent@1": parse_decimal_percent,
            "parser:retail:currency-thousands-to-millions@1": parse_currency_thousands_to_millions,
            "parser:retail:shares-thousands-to-millions@1": parse_shares_thousands_to_millions,
            "parser:retail:guidance-shares-millions@1": parse_guidance_shares_millions,
            "parser:retail:guidance-store-openings@1": parse_guidance_store_openings,
            "parser:retail:guidance-store-closures@1": parse_guidance_store_closures,
            "parser:retail:guidance-store-remodels@1": parse_guidance_store_remodels,
            "parser:retail:reported-store-right-sizes@1": parse_reported_store_right_sizes,
            "parser:retail:reported-store-remodels@1": parse_reported_store_remodels,
            "parser:retail:guidance-approximate-store-openings@1": parse_approximate_guidance_store_openings,
            "parser:retail:guidance-approximate-store-closures@1": parse_approximate_guidance_store_closures,
            "parser:retail:guidance-approximate-store-remodels@1": parse_approximate_guidance_store_remodels,
            "parser:retail:absolute-count-text@1": parse_absolute_count_text,
        }
        parser = parsers.get(parser_id)
        return parser(text) if parser is not None else super().parse_value(parser_id, text)

    def metric_semantics(
        self, metric_key: str, *, guidance: bool = False
    ) -> tuple[str, str, str, str]:
        if metric_key in METRICS:
            return super().metric_semantics(metric_key, guidance=guidance)
        if metric_key == "net-income-per-diluted-share":
            return (
                V2_METRICS[metric_key],
                DEFINITION_GUIDANCE if guidance else DEFINITION_REPORTED,
                BASIS_GUIDED if guidance else BASIS_REPORTED,
                UNIT_CURRENCY_PER_SHARE,
            )
        if metric_key == "net-income-per-diluted-share-adjusted":
            if guidance:
                raise RetailSemanticError("Adjusted EPS is not an activated guidance definition.")
            return (
                V2_METRICS[metric_key],
                DEFINITION_ADJUSTED_NON_GAAP,
                BASIS_ADJUSTED_NON_GAAP,
                UNIT_CURRENCY_PER_SHARE,
            )
        if metric_key == "capital-expenditures":
            return (
                V2_METRICS[metric_key],
                DEFINITION_GUIDANCE if guidance else DEFINITION_REPORTED,
                BASIS_GUIDED if guidance else BASIS_REPORTED,
                UNIT_CURRENCY_MILLION,
            )
        if metric_key == "property-equipment-purchases":
            if guidance:
                raise RetailSemanticError("Property/equipment purchases are not guidance capex semantics.")
            return (
                V2_METRICS[metric_key],
                DEFINITION_PROPERTY_EQUIPMENT_PURCHASES,
                BASIS_REPORTED,
                UNIT_CURRENCY_MILLION,
            )
        if metric_key == "share-repurchases":
            return (
                V2_METRICS[metric_key],
                DEFINITION_GUIDANCE if guidance else DEFINITION_REPORTED,
                BASIS_GUIDED if guidance else BASIS_REPORTED,
                UNIT_CURRENCY_MILLION,
            )
        if metric_key == "diluted-weighted-average-shares":
            return (
                V2_METRICS[metric_key],
                DEFINITION_GUIDANCE if guidance else DEFINITION_REPORTED,
                BASIS_GUIDED if guidance else BASIS_REPORTED,
                UNIT_SHARES_MILLION,
            )
        if metric_key in {
            "store-closures-count",
            "store-right-sizes",
            "store-remodels",
            "store-remodels-right-sizes",
        }:
            return (
                V2_METRICS[metric_key],
                DEFINITION_GUIDANCE if guidance else DEFINITION_REPORTED,
                BASIS_GUIDED if guidance else BASIS_REPORTED,
                UNIT_COUNT,
            )
        if metric_key == "operating-margin-adjusted-litigation-excluded":
            if guidance:
                raise RetailSemanticError("The litigation-adjusted basis is not guidance semantics.")
            return (
                V2_METRICS[metric_key],
                DEFINITION_ADJUSTED_LITIGATION_EXCLUDED,
                BASIS_ADJUSTED_LITIGATION_EXCLUDED,
                UNIT_PERCENT,
            )
        raise RetailSemanticError(f"Unknown Product@2 retail metric key {metric_key!r}.")

    @staticmethod
    def currency_for_semantics(metric_key: str) -> str | None:
        if metric_key in {
            "net-income-per-diluted-share",
            "net-income-per-diluted-share-adjusted",
            "capital-expenditures",
            "property-equipment-purchases",
            "share-repurchases",
        }:
            return "USD"
        return None

    @staticmethod
    def permitted_source_families(candidate_kind: str) -> frozenset[str]:
        if candidate_kind == "management_statement":
            return frozenset({"issuer-transcript", "sec-exhibit"})
        if candidate_kind == "numerical_fact":
            return frozenset({"sec-exhibit", "issuer-spreadsheet", "issuer-transcript"})
        return RetailSectorPack.permitted_source_families(candidate_kind)

    def catalog(
        self,
        dimension_sets: Mapping[str, tuple[str, tuple[tuple[str, str], ...]]],
        aliases: Iterable[Mapping[str, str]],
        methods: Iterable[Mapping[str, Any]],
    ) -> dict[str, Any]:
        result = super().catalog(dimension_sets, aliases, methods)

        metric_names = {
            **{metric_id: key.replace("-", " ").title() for key, metric_id in METRICS.items()},
            V2_METRICS["net-income-per-diluted-share"]: "Net income per diluted share",
            V2_METRICS["capital-expenditures"]: "Capital expenditures",
            V2_METRICS["property-equipment-purchases"]: "Purchases of property and equipment",
            V2_METRICS["share-repurchases"]: "Share repurchases",
            V2_METRICS["diluted-weighted-average-shares"]: "Diluted weighted average shares",
            V2_METRICS["store-closures-count"]: "Store closures",
            V2_METRICS["store-right-sizes"]: "Store right-sizes",
            V2_METRICS["store-remodels"]: "Store remodels",
            V2_METRICS["store-remodels-right-sizes"]: "Store remodels and right-sizes",
        }
        result["metrics"] = [
            {
                "metric_id": metric_id,
                "display_name": name,
                "description": f"Versioned {name.casefold()} metric.",
                "aliases": [],
                "status": "active",
                "supersedes_id": None,
            }
            for metric_id, name in sorted(metric_names.items())
        ]

        def common(key: str, identity: str, name: str, description: str) -> dict[str, Any]:
            return {
                key: identity,
                "display_name": name,
                "description": description,
                "aliases": [],
                "status": "active",
                "supersedes_id": None,
            }

        result["definitions"] = sorted(
            [
                *result["definitions"],
                {
                    **common(
                        "definition_id",
                        DEFINITION_ADJUSTED_LITIGATION_EXCLUDED,
                        "Adjusted excluding litigation benefit",
                        "Company-adjusted definition excluding the identified litigation benefit.",
                    ),
                    "gaap_status": "adjusted",
                },
                {
                    **common(
                        "definition_id",
                        DEFINITION_ADJUSTED_NON_GAAP,
                        "Adjusted non-GAAP",
                        "Company-adjusted non-GAAP definition.",
                    ),
                    "gaap_status": "adjusted",
                },
                {
                    **common(
                        "definition_id",
                        DEFINITION_PROPERTY_EQUIPMENT_PURCHASES,
                        "Purchases of property and equipment",
                        "Source-reported cash purchases of property and equipment.",
                    ),
                    "gaap_status": "operational",
                },
            ],
            key=lambda row: str(row["definition_id"]),
        )
        result["bases"] = sorted(
            [
                *result["bases"],
                {
                    **common(
                        "basis_id",
                        BASIS_ADJUSTED_LITIGATION_EXCLUDED,
                        "Adjusted excluding litigation benefit",
                        "Adjusted realized basis excluding the identified litigation benefit.",
                    ),
                    "realization_state": "reported",
                },
                {
                    **common(
                        "basis_id",
                        BASIS_ADJUSTED_NON_GAAP,
                        "Adjusted non-GAAP",
                        "Company-adjusted non-GAAP realized basis.",
                    ),
                    "realization_state": "reported",
                },
            ],
            key=lambda row: str(row["basis_id"]),
        )
        result["units"] = sorted(
            [
                *result["units"],
                {
                    "unit_id": UNIT_CURRENCY_PER_SHARE,
                    "display_name": "Currency per share",
                    "unit_kind": "currency",
                    "scale": "1",
                    "currency_behavior": "required",
                    "aliases": ["$/share"],
                    "status": "active",
                    "supersedes_id": None,
                },
                {
                    "unit_id": UNIT_CURRENCY_MILLION,
                    "display_name": "Currency million",
                    "unit_kind": "currency",
                    "scale": "1000000",
                    "currency_behavior": "required",
                    "aliases": ["$m"],
                    "status": "active",
                    "supersedes_id": None,
                },
                {
                    "unit_id": UNIT_SHARES_MILLION,
                    "display_name": "Shares million",
                    "unit_kind": "count",
                    "scale": "1000000",
                    "currency_behavior": "forbidden",
                    "aliases": ["million shares"],
                    "status": "active",
                    "supersedes_id": None,
                },
            ],
            key=lambda row: str(row["unit_id"]),
        )
        return result


RETAIL_SECTOR_PACK_V2 = RetailSectorPackV2()
