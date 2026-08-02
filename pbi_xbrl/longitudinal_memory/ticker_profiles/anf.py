"""Declarative ANF aliases, activation, calendar hints and reviewed links."""
from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping

from pbi_xbrl.longitudinal_memory.source_adapter.types import MappingError, SourceSet


PROFILE_ID = "ticker-profile:anf:source-native@1"


@dataclass(frozen=True)
class AnfTickerProfile:
    company_id: str
    cik: str
    publisher_id: str
    publisher_aliases: tuple[str, ...]
    member_aliases: tuple[Mapping[str, str], ...]
    activated_metric_ids: tuple[str, ...]
    official_host_aliases: tuple[str, ...]
    calendar_id: str
    calendar_hint: str
    reviewed_links: tuple[Mapping[str, Any], ...]

    def member_id(self, alias: str, *, axis: str | None = None) -> str:
        normalized = " ".join(alias.split()).casefold()
        matches = [
            str(row["member_id"])
            for row in self.member_aliases
            if " ".join(str(row["alias"]).split()).casefold() == normalized
            and (axis is None or row["axis"] == axis)
        ]
        if len(set(matches)) != 1:
            raise MappingError(
                f"Alias {alias!r} resolves to {len(set(matches))} profile members, not one."
            )
        return matches[0]

    def reviewed_link(self, link_key: str) -> Mapping[str, Any]:
        matches = [row for row in self.reviewed_links if row.get("link_key") == link_key]
        if len(matches) != 1 or matches[0].get("review_state") not in {"accepted", "reviewed"}:
            raise MappingError(f"Reviewed link {link_key!r} is missing, ambiguous or not accepted.")
        return matches[0]

    def evidence_member_id(self, fingerprint: str) -> str | None:
        normalized = " ".join(fingerprint.split()).casefold()
        matches = {
            str(row["member_id"])
            for row in self.member_aliases
            if " ".join(str(row["alias"]).split()).casefold() in normalized
        }
        if len(matches) > 1:
            raise MappingError(
                f"Evidence fingerprint {fingerprint!r} maps to multiple profile members."
            )
        return next(iter(matches), None)


def load_anf_profile(source_set: SourceSet) -> AnfTickerProfile:
    if source_set.ticker_profile_id != PROFILE_ID:
        raise MappingError(
            f"ANF profile module cannot load {source_set.ticker_profile_id!r}."
        )
    profile = source_set.profile
    alias_index: dict[tuple[str, str], str] = {}
    rows: list[Mapping[str, str]] = []
    for raw in profile["member_aliases"]:
        row = MappingProxyType({key: str(raw[key]) for key in ("axis", "alias", "member_id")})
        key = (row["axis"], " ".join(row["alias"].split()).casefold())
        prior = alias_index.get(key)
        if prior is not None and prior != row["member_id"]:
            raise MappingError(f"Ambiguous declarative member alias {row['alias']!r}.")
        alias_index[key] = row["member_id"]
        rows.append(row)
    if profile.get("company_id") != source_set.company_id:
        raise MappingError("ANF profile company differs from SourceSet company.")
    publisher_id = str(profile["publisher_id"])
    if any(document.publisher_id != publisher_id for document in source_set.documents):
        raise MappingError("ANF source documents contain a publisher outside the profile.")
    return AnfTickerProfile(
        company_id=str(profile["company_id"]),
        cik=str(profile["cik"]),
        publisher_id=publisher_id,
        publisher_aliases=tuple(sorted(str(value) for value in profile["publisher_aliases"])),
        member_aliases=tuple(sorted(rows, key=lambda row: (row["axis"], row["alias"].casefold(), row["member_id"]))),
        activated_metric_ids=tuple(sorted(str(value) for value in profile["activated_metric_ids"])),
        official_host_aliases=tuple(sorted(str(value) for value in profile["official_host_aliases"])),
        calendar_id=str(profile["calendar_id"]),
        calendar_hint=str(profile["calendar_hint"]),
        reviewed_links=tuple(source_set.reviewed_links),
    )
