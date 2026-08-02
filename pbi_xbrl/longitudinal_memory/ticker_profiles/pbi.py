"""Declarative PBI aliases, activation, calendar hints and reviewed links."""
from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping

from pbi_xbrl.longitudinal_memory.sector_packs.business_services import (
    BUSINESS_SERVICES_SECTOR_PACK,
)
from pbi_xbrl.longitudinal_memory.source_adapter.types import MappingError, SourceSet


PROFILE_ID = "ticker-profile:pbi:source-native@1"

_PROMISE_WORDING_RULES = (
    MappingProxyType(
        {
            "promise_subject_id": "promise-subject:business-services:cost-savings-program@1",
            "program_id": "program:pbi:2024-cost-rationalization@1",
            "target_metric_id": "metric:business-services:cost-savings@1",
            "target_definition_id": "definition:business-services:cost-savings-program-target@1",
            "target_basis_id": "basis:business-services:program-stated-annualized@1",
            "normalized_wording": (
                "Annualized savings objective under the cost rationalization program."
            ),
            "derivation_rule_id": (
                "rule:core:source-backed-normalized-promise-wording@1"
            ),
            "review_state": "reviewed",
            "source_assertions": (
                MappingProxyType(
                    {
                        "assertion_key": "promise-cost-origin",
                        "required_fragments": ("Annualized Cost Savings",),
                    }
                ),
                MappingProxyType(
                    {
                        "assertion_key": "promise-cost-july-update",
                        "required_fragments": (
                            "Cost Rationalization Program",
                            "Savings Target",
                        ),
                    }
                ),
            ),
        }
    ),
)


@dataclass(frozen=True)
class PbiTickerProfile:
    company_id: str
    cik: str
    publisher_id: str
    publisher_ids: tuple[str, ...]
    publisher_aliases: tuple[str, ...]
    member_aliases: tuple[Mapping[str, str], ...]
    activated_metric_ids: tuple[str, ...]
    activated_semantic_binding_ids: tuple[str, ...]
    sector_registry_version: str
    official_host_aliases: tuple[str, ...]
    calendar_id: str
    calendar_hint: str
    reviewed_links: tuple[Mapping[str, Any], ...]
    promise_wording_rules: tuple[Mapping[str, Any], ...]

    def member_id(self, alias: str, *, axis: str | None = None) -> str:
        normalized = " ".join(alias.split()).casefold()
        matches = {
            str(row["member_id"])
            for row in self.member_aliases
            if " ".join(str(row["alias"]).split()).casefold() == normalized
            and (axis is None or row["axis"] == axis)
        }
        if len(matches) != 1:
            raise MappingError(f"Alias {alias!r} resolves to {len(matches)} PBI members, not one.")
        return next(iter(matches))

    def dimension_id_for_alias(self, alias: str) -> str:
        normalized = " ".join(alias.split()).casefold()
        matches = {
            str(row["dimension_id"])
            for row in self.member_aliases
            if " ".join(str(row["alias"]).split()).casefold() == normalized
        }
        if len(matches) != 1:
            raise MappingError(f"Alias {alias!r} does not resolve one PBI dimension.")
        return next(iter(matches))

    def evidence_member_id(self, fingerprint: str) -> str | None:
        normalized = " ".join(fingerprint.split()).casefold()
        matches = {
            str(row["member_id"])
            for row in self.member_aliases
            if " ".join(str(row["alias"]).split()).casefold() in normalized
        }
        if len(matches) > 1:
            raise MappingError(f"Evidence fingerprint {fingerprint!r} maps to multiple PBI members.")
        return next(iter(matches), None)

    def reviewed_link(self, link_key: str) -> Mapping[str, Any]:
        matches = [row for row in self.reviewed_links if row.get("link_key") == link_key]
        if len(matches) != 1 or matches[0].get("review_state") not in {"accepted", "reviewed"}:
            raise MappingError(f"Reviewed link {link_key!r} is missing, ambiguous or not accepted.")
        return matches[0]


def load_pbi_profile(source_set: SourceSet) -> PbiTickerProfile:
    if source_set.ticker_profile_id != PROFILE_ID:
        raise MappingError(f"PBI profile module cannot load {source_set.ticker_profile_id!r}.")
    profile = source_set.profile
    if profile.get("company_id") != source_set.company_id:
        raise MappingError("PBI profile company differs from SourceSet company.")
    if str(profile.get("sector_registry_version")) != BUSINESS_SERVICES_SECTOR_PACK.registry_version:
        raise MappingError("PBI profile activates an unknown business-services registry version.")
    activated = tuple(sorted(str(value) for value in profile.get("activated_semantic_binding_ids", ())))
    if not activated:
        raise MappingError("PBI profile must activate at least one closed semantic binding.")
    bindings = [BUSINESS_SERVICES_SECTOR_PACK.semantic_binding(identity) for identity in activated]
    publisher_ids = tuple(sorted(str(value) for value in profile.get("publisher_ids", ())))
    if not publisher_ids:
        raise MappingError("PBI profile requires an explicit closed publisher set.")
    unknown_publishers = {document.publisher_id for document in source_set.documents} - set(publisher_ids)
    if unknown_publishers:
        raise MappingError(f"PBI source set uses inactive publishers {sorted(unknown_publishers)}.")
    rows: list[Mapping[str, str]] = []
    alias_index: dict[tuple[str, str], tuple[str, str]] = {}
    for raw in profile["member_aliases"]:
        row = MappingProxyType(
            {key: str(raw[key]) for key in ("axis", "dimension_id", "alias", "member_id")}
        )
        key = (row["axis"], " ".join(row["alias"].split()).casefold())
        value = (row["dimension_id"], row["member_id"])
        if key in alias_index and alias_index[key] != value:
            raise MappingError(f"Ambiguous declarative PBI member alias {row['alias']!r}.")
        alias_index[key] = value
        rows.append(row)
    return PbiTickerProfile(
        company_id=source_set.company_id,
        cik=str(profile["cik"]),
        publisher_id=str(profile["publisher_id"]),
        publisher_ids=publisher_ids,
        publisher_aliases=tuple(sorted(str(value) for value in profile["publisher_aliases"])),
        member_aliases=tuple(sorted(rows, key=lambda row: (row["axis"], row["alias"].casefold(), row["member_id"]))),
        activated_metric_ids=tuple(sorted({binding.metric_id for binding in bindings})),
        activated_semantic_binding_ids=activated,
        sector_registry_version=str(profile["sector_registry_version"]),
        official_host_aliases=tuple(sorted(str(value) for value in profile["official_host_aliases"])),
        calendar_id=str(profile["calendar_id"]),
        calendar_hint=str(profile["calendar_hint"]),
        reviewed_links=tuple(source_set.reviewed_links),
        promise_wording_rules=_PROMISE_WORDING_RULES,
    )
