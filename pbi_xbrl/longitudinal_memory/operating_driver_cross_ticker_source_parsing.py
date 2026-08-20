"""Shared declarative source matcher for Operating Drivers evidence.

The parser is intentionally sector/ticker agnostic: callers supply explicit
rules and source metadata.  It preserves exact, approximate, and qualitative
precision and never turns missing evidence into zero.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import re
from typing import Sequence


PARSER_CONTRACT = "operating-drivers-declarative-source-matcher@1"


class OperatingDriverSourceParsingError(ValueError):
    """Raised for unsafe or ambiguous declarative extraction rules."""


@dataclass(frozen=True)
class DeclarativeExtractionRule:
    rule_id: str
    driver_id: str
    pattern: str
    unit: str
    definition_id: str
    precision: str
    value_group: str | None = "value"
    scale: Decimal = Decimal("1")


@dataclass(frozen=True)
class ParsedSourceFact:
    rule_id: str
    driver_id: str
    source_id: str
    period_label: str
    value: Decimal | None
    evidence_text: str
    precision: str
    unit: str
    definition_id: str


def extract_source_native_facts(
    text: str,
    *,
    source_id: str,
    period_label: str,
    rules: Sequence[DeclarativeExtractionRule],
) -> tuple[ParsedSourceFact, ...]:
    """Apply unique declarative matches while preserving source precision."""

    facts: list[ParsedSourceFact] = []
    for rule in rules:
        matches = list(re.finditer(rule.pattern, text, flags=re.IGNORECASE | re.DOTALL))
        if len(matches) > 1:
            raise OperatingDriverSourceParsingError(
                f"Rule {rule.rule_id!r} produced ambiguous duplicate matches."
            )
        if not matches:
            continue
        match = matches[0]
        value: Decimal | None = None
        if rule.precision == "EXACT":
            if not rule.value_group:
                raise OperatingDriverSourceParsingError("Exact rules require a numeric capture group.")
            raw = match.group(rule.value_group).replace(",", "")
            value = Decimal(raw) * rule.scale
        elif rule.value_group is not None:
            raise OperatingDriverSourceParsingError(
                "Approximate and qualitative rules may not emit exact numeric values."
            )
        facts.append(
            ParsedSourceFact(
                rule_id=rule.rule_id,
                driver_id=rule.driver_id,
                source_id=source_id,
                period_label=period_label,
                value=value,
                evidence_text=" ".join(match.group(0).split()),
                precision=rule.precision,
                unit=rule.unit,
                definition_id=rule.definition_id,
            )
        )
    return tuple(facts)


__all__ = [
    "DeclarativeExtractionRule",
    "OperatingDriverSourceParsingError",
    "PARSER_CONTRACT",
    "ParsedSourceFact",
    "extract_source_native_facts",
]
