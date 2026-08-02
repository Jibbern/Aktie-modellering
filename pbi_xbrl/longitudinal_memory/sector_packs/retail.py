"""Generic retail metric, dimension and lossless value semantics."""
from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable, Mapping

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
    range_match = re.search(
        r"(?i)(?:range\s+of\s+)?(-?[0-9]+(?:\.[0-9]+)?)\s*%\s*(?:to|-)\s*"
        r"(-?[0-9]+(?:\.[0-9]+)?)\s*%",
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
        if any(
            bool(period["is_53_week_year"]) != bool(later_period["is_53_week_year"])
            for period in (qoq_period, yoy_period)
        ):
            raise RetailSemanticError(
                "Percentage-point comparisons cannot cross incompatible 52/53-week fiscal years."
            )
        return (
            ("qoq-percentage-point", qoq_fact, later, qoq_period, later_period),
            ("yoy-percentage-point", yoy_fact, later, yoy_period, later_period),
        )

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
