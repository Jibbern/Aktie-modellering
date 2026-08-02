"""Reusable business-services metrics, dimensions and lossless value semantics."""
from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from types import MappingProxyType
from typing import Any, Iterable, Mapping

from pbi_xbrl.longitudinal_memory.identity import dimension_set_identity
from pbi_xbrl.longitudinal_memory.types import canonical_decimal
from pbi_xbrl.longitudinal_memory.source_adapter.types import (
    MappingError,
    SemanticBinding,
    SemanticRegistry,
)


SECTOR_PACK_ID = "sector-pack:business-services:longitudinal@1"
REGISTRY_VERSION = "registry:business-services:longitudinal@1"
DIMENSION_COMPANY = "dimension:core:company@1"
DIMENSION_SEGMENT = "dimension:business-services:segment@1"
UNIT_USD_MILLIONS = "unit:core:usd-millions@1"
UNIT_PERCENT = "unit:core:percent@1"
UNIT_PERCENTAGE_POINT = "unit:core:percentage-point@1"
UNIT_BILLION_PIECES = "unit:business-services:billion-pieces@1"


def _binding(
    identity: str,
    metric: str,
    definition: str,
    basis: str,
    unit: str,
    *,
    currency: str | None = None,
    modes: tuple[str, ...] = ("numerical_fact",),
    dimensions: tuple[str, ...] = (DIMENSION_COMPANY, DIMENSION_SEGMENT),
    assertion_mode: str = "reported",
) -> SemanticBinding:
    return SemanticBinding(
        semantic_binding_id=identity,
        metric_id=metric,
        definition_id=definition,
        basis_id=basis,
        unit_id=unit,
        currency=currency,
        assertion_mode=assertion_mode,
        candidate_kinds=frozenset(modes),
        dimension_ids=dimensions,
    )


_BINDINGS = (
    _binding("binding:business-services:segment-revenue-gaap@1", "metric:core:revenue@1", "definition:core:gaap-revenue@1", "basis:core:reported@1", UNIT_USD_MILLIONS, currency="USD"),
    _binding("binding:business-services:adjusted-segment-ebit-pre-2026@1", "metric:business-services:adjusted-segment-ebit@1", "definition:business-services:adjusted-segment-ebit-pre-2026@1", "basis:core:adjusted@1", UNIT_USD_MILLIONS, currency="USD"),
    _binding("binding:business-services:adjusted-segment-ebit-ex-terminated-plan-pension@1", "metric:business-services:adjusted-segment-ebit@1", "definition:business-services:adjusted-segment-ebit-ex-terminated-plan-pension@1", "basis:core:adjusted@1", UNIT_USD_MILLIONS, currency="USD"),
    _binding("binding:business-services:adjusted-segment-margin-ex-terminated-plan-pension@1", "metric:business-services:adjusted-segment-ebit-margin@1", "definition:business-services:adjusted-segment-ebit-ex-terminated-plan-pension@1", "basis:core:derived@1", UNIT_PERCENT, assertion_mode="derived"),
    _binding("binding:business-services:reported-revenue-growth@1", "metric:core:revenue-growth@1", "definition:core:company-reported-rounded-rate@1", "basis:core:reported@1", UNIT_PERCENT),
    _binding("binding:business-services:derived-revenue-change@1", "metric:core:revenue-growth@1", "definition:core:derived-from-unrounded-revenue@1", "basis:core:derived@1", UNIT_PERCENT, assertion_mode="derived"),
    _binding("binding:business-services:pieces-processed@1", "metric:business-services:pieces-processed@1", "definition:business-services:company-reported-pieces@1", "basis:core:reported@1", UNIT_BILLION_PIECES),
    _binding("binding:business-services:volume-growth@1", "metric:business-services:volume-growth@1", "definition:core:company-reported-rounded-rate@1", "basis:core:reported@1", UNIT_PERCENT),
    _binding("binding:business-services:revenue-per-piece@1", "metric:business-services:revenue-per-piece@1", "definition:business-services:company-reported-revenue-per-piece@1", "basis:core:reported@1", UNIT_USD_MILLIONS, currency="USD"),
    _binding("binding:business-services:pricing-mix-growth@1", "metric:business-services:pricing-mix-growth@1", "definition:core:company-reported-rounded-rate@1", "basis:core:reported@1", UNIT_PERCENT),
    _binding("binding:business-services:bookings-growth@1", "metric:business-services:bookings-growth@1", "definition:business-services:company-reported-bookings@1", "basis:core:reported@1", UNIT_PERCENT),
    _binding("binding:business-services:backlog@1", "metric:business-services:backlog@1", "definition:business-services:company-reported-backlog@1", "basis:core:reported@1", UNIT_USD_MILLIONS, currency="USD"),
    _binding("binding:business-services:operating-leverage@1", "metric:business-services:operating-leverage@1", "definition:business-services:management-explained-operating-leverage@1", "basis:core:reported@1", UNIT_PERCENT),
    _binding("binding:business-services:annualized-savings-program-target@1", "metric:business-services:cost-savings@1", "definition:business-services:cost-savings-program-target@1", "basis:business-services:program-stated-annualized@1", UNIT_USD_MILLIONS, currency="USD", modes=("promise_version",), assertion_mode="stated"),
    _binding("binding:business-services:potential-annualized-savings-target@1", "metric:business-services:cost-savings@1", "definition:business-services:cost-savings-target@1", "basis:business-services:potential-annualized@1", UNIT_USD_MILLIONS, currency="USD"),
    _binding("binding:business-services:net-annualized-savings-target@1", "metric:business-services:cost-savings@1", "definition:business-services:cost-savings-target@1", "basis:business-services:net-annualized@1", UNIT_USD_MILLIONS, currency="USD"),
    _binding("binding:business-services:identified-initiated-savings@1", "metric:business-services:cost-savings@1", "definition:business-services:identified-initiated-savings@1", "basis:business-services:potential-annualized@1", UNIT_USD_MILLIONS, currency="USD"),
    _binding("binding:business-services:annualized-costs-removed@1", "metric:business-services:cost-savings@1", "definition:business-services:annualized-costs-removed@1", "basis:business-services:annualized-run-rate@1", UNIT_USD_MILLIONS, currency="USD"),
    _binding("binding:business-services:annualized-run-rate@1", "metric:business-services:cost-savings@1", "definition:business-services:annualized-run-rate@1", "basis:business-services:annualized-run-rate@1", UNIT_USD_MILLIONS, currency="USD"),
    _binding("binding:business-services:realized-period-savings@1", "metric:business-services:cost-savings@1", "definition:business-services:realized-period-savings@1", "basis:business-services:realized-period@1", UNIT_USD_MILLIONS, currency="USD"),
    _binding("binding:business-services:cumulative-savings@1", "metric:business-services:cost-savings@1", "definition:business-services:cumulative-savings@1", "basis:business-services:cumulative-realized@1", UNIT_USD_MILLIONS, currency="USD"),
    _binding("binding:business-services:gross-savings@1", "metric:business-services:cost-savings@1", "definition:business-services:gross-savings@1", "basis:business-services:annualized-run-rate@1", UNIT_USD_MILLIONS, currency="USD"),
    _binding("binding:business-services:net-savings@1", "metric:business-services:cost-savings@1", "definition:business-services:net-savings@1", "basis:business-services:annualized-run-rate@1", UNIT_USD_MILLIONS, currency="USD"),
    _binding("binding:business-services:costs-avoided@1", "metric:business-services:cost-savings@1", "definition:business-services:costs-avoided@1", "basis:business-services:prospective@1", UNIT_USD_MILLIONS, currency="USD"),
    _binding("binding:business-services:implementation-charges@1", "metric:business-services:non-recurring-implementation-charges@1", "definition:business-services:implementation-charges@1", "basis:core:reported@1", UNIT_USD_MILLIONS, currency="USD"),
    _binding("binding:business-services:revenue-guidance@1", "metric:core:revenue@1", "definition:core:company-guidance@1", "basis:core:guided@1", UNIT_USD_MILLIONS, currency="USD", modes=("guidance",), assertion_mode="guided", dimensions=(DIMENSION_COMPANY,)),
    _binding("binding:business-services:adjusted-ebit-guidance-ambiguous-pension@1", "metric:core:adjusted-ebit@1", "definition:business-services:adjusted-ebit-pension-treatment-ambiguous@1", "basis:core:guided@1", UNIT_USD_MILLIONS, currency="USD", modes=("guidance",), assertion_mode="guided", dimensions=(DIMENSION_COMPANY,)),
    _binding("binding:business-services:adjusted-ebit-guidance-ex-terminated-plan-pension@1", "metric:core:adjusted-ebit@1", "definition:business-services:adjusted-ebit-ex-terminated-plan-pension@1", "basis:core:guided@1", UNIT_USD_MILLIONS, currency="USD", modes=("guidance",), assertion_mode="guided", dimensions=(DIMENSION_COMPANY,)),
    _binding("binding:business-services:fuel-headwind@1", "metric:business-services:transport-cost-pressure@1", "definition:business-services:company-quantified-fuel-headwind@1", "basis:core:reported@1", UNIT_USD_MILLIONS, currency="USD"),
    _binding("binding:business-services:debt-reduction@1", "metric:core:debt-reduction@1", "definition:core:company-stated-debt-reduction@1", "basis:core:reported@1", UNIT_USD_MILLIONS, currency="USD"),
    _binding("binding:business-services:debt-financing@1", "metric:core:debt-financing@1", "definition:core:legal-event-amount@1", "basis:core:reported@1", UNIT_USD_MILLIONS, currency="USD"),
)
BINDINGS = MappingProxyType({row.semantic_binding_id: row for row in _BINDINGS})


def _decimal(value: str | Decimal) -> str:
    return canonical_decimal(Decimal(str(value).replace(",", "")))


def _amount_multiplier(token: str | None) -> Decimal:
    normalized = (token or "million").casefold()
    return Decimal("1000") if normalized in {"billion", "bn"} else Decimal("1")


def parse_currency_millions(text: str) -> dict[str, Any]:
    normalized = " ".join(text.replace("–", "-").replace("—", "-").split())
    if re.fullmatch(r"-?[0-9][0-9,]*(?:\.[0-9]+)?", normalized):
        raw = Decimal(normalized.replace(",", ""))
        return {"kind": "exact", "value": _decimal(raw / Decimal("1000000"))}
    match = re.search(
        r"(?i)(approximately|about|roughly|around|more than|at least)?\s*\$?\s*"
        r"(?P<value>[0-9][0-9,]*(?:\.[0-9]+)?)\s*(?P<scale>million|billion|m|bn)\b",
        normalized,
    )
    if not match:
        raise MappingError(f"No closed currency amount is present in {text!r}.")
    value = Decimal(match.group("value").replace(",", "")) * _amount_multiplier(match.group("scale"))
    qualifier = (match.group(1) or "").casefold()
    if qualifier in {"more than", "at least"}:
        return {"kind": "bound", "operator": "gte", "value": _decimal(value)}
    if qualifier:
        return {"kind": "approximate", "value": _decimal(value), "qualifier": qualifier, "tolerance": None}
    return {"kind": "exact", "value": _decimal(value)}


def parse_currency_range_millions(text: str) -> dict[str, Any]:
    normalized = " ".join(text.replace("–", "-").replace("—", "-").split())
    match = re.search(
        r"(?i)\$?\s*(?P<low>[0-9][0-9,]*(?:\.[0-9]+)?)\s*(?P<low_scale>million|billion|m|bn)?"
        r"\s*(?:to|-|(?=\$))\s*\$?\s*(?P<high>[0-9][0-9,]*(?:\.[0-9]+)?)\s*"
        r"(?P<high_scale>million|billion|m|bn)?\b",
        normalized,
    )
    if not match:
        raise MappingError(f"No closed currency range is present in {text!r}.")
    high_scale = match.group("high_scale") or match.group("low_scale") or "million"
    low_scale = match.group("low_scale") or high_scale
    return {
        "kind": "range",
        "low": _decimal(Decimal(match.group("low").replace(",", "")) * _amount_multiplier(low_scale)),
        "high": _decimal(Decimal(match.group("high").replace(",", "")) * _amount_multiplier(high_scale)),
        "low_inclusive": True,
        "high_inclusive": True,
    }


def parse_percent(text: str) -> dict[str, Any]:
    normalized = " ".join(text.split())
    match = re.search(
        r"(?<![0-9])(?P<open>\()?\s*(?P<value>[+-]?[0-9]+(?:\.[0-9]+)?)\s*%\s*(?P<close>\))?",
        normalized,
    )
    if not match:
        raise MappingError(f"No exact percentage is present in {text!r}.")
    value = Decimal(match.group("value"))
    if (match.group("open") is None) != (match.group("close") is None):
        raise MappingError(f"Percentage parentheses are incomplete in {text!r}.")
    if match.group("open") is not None or re.search(r"(?i)\b(?:decline|decrease|reduction)\b", normalized):
        value = -abs(value)
    return {"kind": "exact", "value": _decimal(value)}


def parse_billion_pieces(text: str) -> dict[str, Any]:
    match = re.search(r"(?i)([0-9]+(?:\.[0-9]+)?)\s+billion\s+(?:mail\s+)?pieces", " ".join(text.split()))
    if not match:
        raise MappingError(f"No reported billion-piece amount is present in {text!r}.")
    return {"kind": "exact", "value": _decimal(match.group(1))}


@dataclass(frozen=True)
class BusinessServicesSectorPack:
    sector_pack_id: str = SECTOR_PACK_ID
    registry_version: str = REGISTRY_VERSION

    @property
    def total_dimension_alias(self) -> str:
        return "total company"

    @property
    def percentage_point_unit_id(self) -> str:
        return UNIT_PERCENTAGE_POINT

    def semantic_binding(self, binding_id: str) -> SemanticBinding:
        try:
            return BINDINGS[binding_id]
        except KeyError as exc:
            raise MappingError(f"Unknown business-services semantic binding {binding_id!r}.") from exc

    @property
    def semantic_registry(self) -> SemanticRegistry:
        return SemanticRegistry(
            sector_pack_id=self.sector_pack_id,
            registry_version=self.registry_version,
            bindings=BINDINGS,
            dimensions={DIMENSION_COMPANY: frozenset(), DIMENSION_SEGMENT: frozenset()},
        )

    def metric_semantics(self, semantic_key: str, *, guidance: bool = False) -> tuple[str, str, str, str]:
        binding = self.semantic_binding(semantic_key)
        expected = "guidance" if guidance else None
        if expected is not None and expected not in binding.candidate_kinds:
            raise MappingError(f"Binding {semantic_key!r} is not guidance economics.")
        return binding.metric_id, binding.definition_id, binding.basis_id, binding.unit_id

    def currency_for_semantics(self, semantic_key: str) -> str | None:
        return self.semantic_binding(semantic_key).currency

    def assertion_mode(self, semantic_key: str) -> str:
        return self.semantic_binding(semantic_key).assertion_mode

    def parse_value(self, parser_id: str, text: str) -> dict[str, Any]:
        parsers = {
            "parser:business-services:currency-millions@1": parse_currency_millions,
            "parser:business-services:currency-range-millions@1": parse_currency_range_millions,
            "parser:business-services:percent@1": parse_percent,
            "parser:business-services:billion-pieces@1": parse_billion_pieces,
        }
        try:
            return parsers[parser_id](text)
        except KeyError as exc:
            raise MappingError(f"Unknown business-services parser {parser_id!r}.") from exc

    @staticmethod
    def permitted_source_families(candidate_kind: str) -> frozenset[str]:
        policies = {
            "numerical_fact": frozenset({"sec-primary", "sec-exhibit", "issuer-html", "issuer-pdf", "issuer-transcript", "reviewed-page-snapshot"}),
            "guidance": frozenset({"sec-exhibit", "issuer-html", "issuer-pdf", "issuer-transcript"}),
            "promise_version": frozenset({"reviewed-page-snapshot", "issuer-html", "issuer-pdf", "sec-exhibit"}),
            "management_statement": frozenset({"sec-exhibit", "issuer-html", "issuer-transcript"}),
            "company_event": frozenset({"sec-primary", "sec-exhibit", "issuer-html"}),
            "period_evidence": frozenset({"sec-primary", "sec-exhibit", "issuer-html", "issuer-pdf"}),
        }
        try:
            return policies[candidate_kind]
        except KeyError as exc:
            raise MappingError(f"No business-services source policy exists for {candidate_kind!r}.") from exc

    def dimension_sets(self, aliases: Iterable[Mapping[str, str]]) -> dict[str, tuple[str, tuple[tuple[str, str], ...]]]:
        grouped: dict[str, dict[str, tuple[str, str]]] = {}
        for row in aliases:
            alias = " ".join(str(row["alias"]).split()).casefold()
            axis = str(row["axis"])
            dimension_id = str(row.get("dimension_id") or ({"company": DIMENSION_COMPANY, "segment": DIMENSION_SEGMENT}.get(axis, "")))
            if not dimension_id:
                raise MappingError(f"Unknown business-services dimension axis {axis!r}.")
            prior = grouped.setdefault(alias, {}).get(axis)
            pair = (dimension_id, str(row["member_id"]))
            if prior is not None and prior != pair:
                raise MappingError(f"Alias {alias!r} maps to multiple members on one axis.")
            grouped[alias][axis] = pair
        total = grouped.get(self.total_dimension_alias)
        if total is None or "company" not in total:
            raise MappingError("Business-services profile lacks an explicit total-company member.")
        company_pair = total["company"]
        result: dict[str, tuple[str, tuple[tuple[str, str], ...]]] = {}
        for alias, axes in grouped.items():
            pairs = [company_pair]
            pairs.extend(pair for axis, pair in axes.items() if axis != "company")
            normalized = tuple(sorted(set(pairs)))
            if len({dimension for dimension, _member in normalized}) != len(normalized):
                raise MappingError(f"Dimension set for {alias!r} has a duplicate axis.")
            result[alias] = (dimension_set_identity(normalized), normalized)
        return result

    def percentage_point_change_requests(
        self,
        periods: Iterable[Mapping[str, Any]],
        selected: Mapping[tuple[str, str, str], Mapping[str, Any]],
        *,
        total_dimension_id: str,
        calendar: Mapping[str, Any],
    ) -> tuple[tuple[str, Mapping[str, Any], Mapping[str, Any], Mapping[str, Any], Mapping[str, Any]], ...]:
        del total_dimension_id, calendar
        period_by_id = {str(row["period_id"]): row for row in periods}
        eligible_metrics = {
            "metric:core:revenue-growth@1",
            "metric:business-services:adjusted-segment-ebit-margin@1",
        }
        facts = [
            row for (metric_id, _period, _dimension), row in selected.items()
            if metric_id in eligible_metrics
            and row["payload"]["definition_id"] in {
                "definition:core:company-reported-rounded-rate@1",
                "definition:business-services:adjusted-segment-ebit-ex-terminated-plan-pension@1",
            }
        ]
        by_axes: dict[tuple[str, str, str, str, str], list[Mapping[str, Any]]] = {}
        for row in facts:
            payload = row["payload"]
            axes = (
                str(payload["metric_id"]),
                str(payload["definition_id"]),
                str(payload["basis_id"]),
                str(payload["unit_id"]),
                str(row["header"]["dimension_set_id"]),
            )
            by_axes.setdefault(axes, []).append(row)
        requests: list[tuple[str, Mapping[str, Any], Mapping[str, Any], Mapping[str, Any], Mapping[str, Any]]] = []
        for axes in sorted(by_axes):
            rows = by_axes[axes]
            lookup = {
                int(period_by_id[str(row["header"]["effective_period_id"])]["fiscal_ordinal"]): row
                for row in rows
            }
            for later_ordinal in sorted(lookup):
                later = lookup[later_ordinal]
                later_period = period_by_id[str(later["header"]["effective_period_id"])]
                for kind, offset in (("qoq-percentage-point", 1), ("yoy-percentage-point", 4)):
                    earlier = lookup.get(later_ordinal - offset)
                    if earlier is None:
                        continue
                    earlier_period = period_by_id[str(earlier["header"]["effective_period_id"])]
                    if kind.startswith("qoq"):
                        expected_quarter = 1 if int(earlier_period["fiscal_quarter"]) == 4 else int(earlier_period["fiscal_quarter"]) + 1
                        if expected_quarter != int(later_period["fiscal_quarter"]):
                            continue
                    if kind.startswith("yoy") and earlier_period["fiscal_quarter"] != later_period["fiscal_quarter"]:
                        continue
                    requests.append((kind, earlier, later, earlier_period, later_period))
        return tuple(requests)

    def derived_fact_requests(
        self,
        selected: Mapping[tuple[str, str, str], Mapping[str, Any]],
    ) -> tuple[Mapping[str, Any], ...]:
        """Return transparent margin derivations from selected compatible inputs."""

        requests: list[Mapping[str, Any]] = []
        for (_metric_id, period_id, dimension_id), ebit in sorted(selected.items()):
            if ebit["payload"]["metric_id"] != "metric:business-services:adjusted-segment-ebit@1":
                continue
            if ebit["payload"]["definition_id"] != "definition:business-services:adjusted-segment-ebit-ex-terminated-plan-pension@1":
                continue
            revenue = selected.get(("metric:core:revenue@1", period_id, dimension_id))
            if revenue is None:
                continue
            numerator = Decimal(str(ebit["payload"]["value"]["value"]))
            denominator = Decimal(str(revenue["payload"]["value"]["value"]))
            if denominator == 0:
                raise MappingError("Adjusted segment margin cannot be derived from zero revenue.")
            requests.append(
                {
                    "semantic_binding_id": "binding:business-services:adjusted-segment-margin-ex-terminated-plan-pension@1",
                    "period_id": period_id,
                    "dimension_set_id": dimension_id,
                    "value": {"kind": "exact", "value": _decimal(numerator * Decimal("100") / denominator)},
                    "input_records": (revenue, ebit),
                    "rule_id": "rule:business-services:adjusted-segment-margin@1",
                }
            )
        return tuple(requests)

    def promise_evidence_assessment(self, observations: Iterable[Mapping[str, Any]], eligible_states: frozenset[str]) -> Mapping[str, Any]:
        evidence = [
            row for row in observations
            if row.get("payload", {}).get("kind") == "NumericalFact"
            and row["payload"].get("metric_id") == "metric:business-services:cost-savings@1"
            and row["payload"].get("definition_id") in {
                "definition:business-services:annualized-costs-removed@1",
                "definition:business-services:annualized-run-rate@1",
            }
            and row.get("header", {}).get("review_state") in eligible_states
        ]
        if not evidence:
            raise MappingError("Cost-savings promise requires source-backed annualized evidence.")
        evidence.sort(key=lambda row: (row["header"]["knowledge_date"], row["header"]["record_id"]))
        latest = evidence[-1]
        return {
            "evidence_record": latest,
            "candidate_records": tuple(evidence),
            "relation_rule_id": "rule:core:promise-evidence@1",
            "review_rule_id": "promise_run_rate_not_realized_savings",
            "message": "Annualized run-rate evidence is not realized-period savings and no exact promise deadline was disclosed.",
            "action": "Retain the run-rate evidence separately and perform a reviewed promise assessment.",
        }

    def catalog(
        self,
        dimension_sets: Mapping[str, tuple[str, tuple[tuple[str, str], ...]]],
        aliases: Iterable[Mapping[str, str]],
        methods: Iterable[Mapping[str, Any]],
    ) -> dict[str, Any]:
        def common(key: str, identity: str) -> dict[str, Any]:
            slug = identity.split(":")[-1].split("@")[0]
            return {key: identity, "display_name": slug.replace("-", " ").title(), "description": f"Versioned {slug} semantics.", "aliases": [], "status": "active", "supersedes_id": None}
        metrics = sorted({row.metric_id for row in BINDINGS.values()})
        definitions = sorted({row.definition_id for row in BINDINGS.values()})
        bases = sorted({row.basis_id for row in BINDINGS.values()})
        units = sorted({row.unit_id for row in BINDINGS.values()} | {UNIT_PERCENTAGE_POINT})
        member_rows: dict[str, Mapping[str, str]] = {}
        member_aliases: dict[str, list[str]] = {}
        for row in aliases:
            member_id = str(row["member_id"])
            member_rows.setdefault(member_id, row)
            member_aliases.setdefault(member_id, []).append(str(row["alias"]))
        return {
            "metrics": [common("metric_id", identity) for identity in metrics],
            "definitions": [{**common("definition_id", identity), "gaap_status": ("gaap" if "gaap" in identity else "adjusted" if "adjusted" in identity else "operational")} for identity in definitions],
            "bases": [{**common("basis_id", identity), "realization_state": ("guided" if "guided" in identity else "derived" if "derived" in identity else "reported")} for identity in bases],
            "units": [
                {
                    "unit_id": identity,
                    "display_name": identity.split(":")[-1].split("@")[0].replace("-", " ").title(),
                    "unit_kind": "currency" if identity == UNIT_USD_MILLIONS else "percentage-point" if identity == UNIT_PERCENTAGE_POINT else "percent" if identity == UNIT_PERCENT else "count",
                    "scale": "1000000" if identity == UNIT_USD_MILLIONS else "1000000000" if identity == UNIT_BILLION_PIECES else "1",
                    "currency_behavior": "required" if identity == UNIT_USD_MILLIONS else "forbidden",
                    "aliases": ["USDm"] if identity == UNIT_USD_MILLIONS else ["pp"] if identity == UNIT_PERCENTAGE_POINT else ["%"] if identity == UNIT_PERCENT else [],
                    "status": "active", "supersedes_id": None,
                }
                for identity in units
            ],
            "dimensions": [common("dimension_id", identity) for identity in (DIMENSION_COMPANY, DIMENSION_SEGMENT)],
            "dimension_members": [
                {
                    "member_id": member_id,
                    "dimension_id": str(row.get("dimension_id") or (DIMENSION_COMPANY if row["axis"] == "company" else DIMENSION_SEGMENT)),
                    "scope": str(row["axis"]),
                    "display_name": sorted(set(member_aliases[member_id]), key=lambda value: (len(value), value.casefold()))[0],
                    "aliases": sorted(set(member_aliases[member_id]))[1:],
                    "status": "active", "supersedes_id": None,
                }
                for member_id, row in sorted(member_rows.items())
            ],
            "dimension_sets": [
                {"dimension_set_id": identity, "members": [{"dimension_id": dimension_id, "member_id": member_id} for dimension_id, member_id in pairs]}
                for identity, pairs in sorted({value for value in dimension_sets.values()})
            ],
            "policies": [
                {"policy_id": f"policy:core:{name}@1", "assertion_type": name, "description": f"Assertion-specific {name} precedence."}
                for name in ("reported-numerical", "guidance", "management-explanation", "company-event", "model-interpretation")
            ],
            "change_rules": [
                {"rule_id": f"rule:core:{kind}@1", "change_kind": kind, "input_unit_kind": "percent", "output_unit_id": UNIT_PERCENTAGE_POINT, "description": f"Compatible {kind} change."}
                for kind in ("qoq-percentage-point", "yoy-percentage-point")
            ],
            "methods": sorted((dict(row) for row in methods), key=lambda row: str(row["method_id"])),
        }


BUSINESS_SERVICES_SECTOR_PACK = BusinessServicesSectorPack()
