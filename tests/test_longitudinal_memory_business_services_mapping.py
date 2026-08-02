from __future__ import annotations

import dataclasses
from pathlib import Path
from types import MappingProxyType

import pytest

from pbi_xbrl.longitudinal_memory.sector_packs.business_services import (
    BUSINESS_SERVICES_SECTOR_PACK,
    parse_billion_pieces,
    parse_currency_millions,
    parse_currency_range_millions,
    parse_percent,
)
from pbi_xbrl.longitudinal_memory.source_adapter.discovery import load_source_set
from pbi_xbrl.longitudinal_memory.source_adapter.types import MappingError
from pbi_xbrl.longitudinal_memory.ticker_profiles.pbi import load_pbi_profile


REPO = Path(__file__).resolve().parents[1]
FIXTURE = REPO / "tests" / "fixtures" / "longitudinal_memory" / "pbi_source_set.v1.json"


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("$122.678 million", {"kind": "exact", "value": "122.678"}),
        ("approximately $70 million", {"kind": "approximate", "value": "70", "qualifier": "approximately", "tolerance": None}),
        ("around $6 million", {"kind": "approximate", "value": "6", "qualifier": "around", "tolerance": None}),
        ("more than $200 million", {"kind": "bound", "operator": "gte", "value": "200"}),
        ("$1.2 billion", {"kind": "exact", "value": "1200"}),
    ],
)
def test_currency_parser_preserves_value_form(text: str, expected: dict) -> None:
    assert parse_currency_millions(text) == expected


def test_removing_around_changes_the_value_form_to_exact() -> None:
    assert parse_currency_millions("around $6 million") == {
        "kind": "approximate",
        "value": "6",
        "qualifier": "around",
        "tolerance": None,
    }
    assert parse_currency_millions("$6 million") == {"kind": "exact", "value": "6"}


def test_currency_range_parser_does_not_create_midpoint() -> None:
    assert parse_currency_range_millions("$180 million to $200 million") == {
        "kind": "range",
        "low": "180",
        "high": "200",
        "low_inclusive": True,
        "high_inclusive": True,
    }


@pytest.mark.parametrize(
    ("text", "value"),
    [("decline of 8%", "-8"), ("(5%)", "-5"), ("grew 3%", "3"), ("-1%", "-1")],
)
def test_reported_percent_parser_preserves_sign(text: str, value: str) -> None:
    assert parse_percent(text) == {"kind": "exact", "value": value}


def test_billion_piece_parser_is_typed() -> None:
    assert parse_billion_pieces("Presort processed 3.3 billion pieces") == {
        "kind": "exact",
        "value": "3.3",
    }


def test_closed_business_services_registry_has_distinct_savings_definitions() -> None:
    pack = BUSINESS_SERVICES_SECTOR_PACK
    identities = {
        binding.definition_id for binding in pack.semantic_registry.bindings.values()
    }
    assert {
        "definition:business-services:cost-savings-target@1",
        "definition:business-services:identified-initiated-savings@1",
        "definition:business-services:annualized-costs-removed@1",
        "definition:business-services:annualized-run-rate@1",
        "definition:business-services:implementation-charges@1",
    } <= identities
    assert pack.semantic_binding(
        "binding:business-services:annualized-run-rate@1"
    ).definition_id != pack.semantic_binding(
        "binding:business-services:annualized-costs-removed@1"
    ).definition_id


def test_unknown_binding_and_parser_fail_closed() -> None:
    with pytest.raises(MappingError, match="Unknown business-services semantic binding"):
        BUSINESS_SERVICES_SECTOR_PACK.semantic_binding("binding:unknown@1")
    with pytest.raises(MappingError, match="Unknown business-services parser"):
        BUSINESS_SERVICES_SECTOR_PACK.parse_value("parser:unknown@1", "1")


def test_reviewed_metadata_is_not_an_economic_source_family() -> None:
    for kind in (
        "numerical_fact",
        "guidance",
        "promise_version",
        "management_statement",
        "company_event",
    ):
        assert "reviewed-metadata" not in BUSINESS_SERVICES_SECTOR_PACK.permitted_source_families(kind)


def test_pbi_profile_is_declarative_and_activates_closed_bindings() -> None:
    source_set = load_source_set(FIXTURE)
    profile = load_pbi_profile(source_set)
    assert profile.company_id == "PBI"
    assert profile.cik == "0000078814"
    assert profile.calendar_id == "calendar:pbi:calendar-year@1"
    assert profile.member_id("SendTech") == "member:pbi:segment:sendtech@1"
    assert profile.member_id("Presort") == "member:pbi:segment:presort@1"
    assert set(profile.activated_semantic_binding_ids) == set(
        source_set.profile["activated_semantic_binding_ids"]
    )


def test_ambiguous_profile_alias_fails() -> None:
    source_set = load_source_set(FIXTURE)
    profile = dict(source_set.profile)
    aliases = [dict(row) for row in profile["member_aliases"]]
    aliases.append(
        {
            "axis": "segment",
            "dimension_id": "dimension:business-services:segment@1",
            "alias": "SendTech",
            "member_id": "member:pbi:segment:presort@1",
        }
    )
    profile["member_aliases"] = aliases
    mutated = dataclasses.replace(source_set, profile=MappingProxyType(profile))
    with pytest.raises(MappingError, match="Ambiguous declarative PBI member alias"):
        load_pbi_profile(mutated)


def test_segment_dimension_sets_include_company_scope() -> None:
    source_set = load_source_set(FIXTURE)
    sets = BUSINESS_SERVICES_SECTOR_PACK.dimension_sets(source_set.profile["member_aliases"])
    sendtech = sets["sendtech"][1]
    presort = sets["presort"][1]
    assert len(sendtech) == 2
    assert len(presort) == 2
    assert sendtech != presort
