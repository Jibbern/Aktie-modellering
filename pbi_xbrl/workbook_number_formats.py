"""Shared presentation rules for workbook number formats."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


_RED_TOKEN = re.compile(r"\[red\]", re.IGNORECASE)


@dataclass(frozen=True)
class NumberFormatNormalization:
    custom_formats_changed: int
    explicit_styles_changed: int


def neutralize_negative_number_format(number_format: str) -> str:
    """Remove Excel's red-font directive without changing numeric semantics."""

    return _RED_TOKEN.sub("", str(number_format))


def neutralize_workbook_negative_number_formats(workbook: Any) -> NumberFormatNormalization:
    """Remove red-font directives from every workbook number-format authority."""

    custom_formats = workbook._number_formats  # type: ignore[attr-defined]
    custom_changes = 0
    for index, number_format in enumerate(tuple(custom_formats)):
        normalized = neutralize_negative_number_format(number_format)
        if normalized == number_format:
            continue
        list.__setitem__(custom_formats, index, normalized)
        custom_changes += 1

    if custom_changes:
        custom_formats._dict = {  # type: ignore[attr-defined]
            number_format: index for index, number_format in enumerate(custom_formats)
        }

    explicit_changes = 0
    style_owners: list[Any] = list(workbook._named_styles)  # type: ignore[attr-defined]
    for worksheet in workbook.worksheets:
        style_owners.extend(worksheet._cells.values())
        style_owners.extend(worksheet.row_dimensions.values())
        style_owners.extend(worksheet.column_dimensions.values())

    for owner in style_owners:
        number_format = str(getattr(owner, "number_format", "General"))
        normalized = neutralize_negative_number_format(number_format)
        if normalized == number_format:
            continue
        owner.number_format = normalized
        explicit_changes += 1

    return NumberFormatNormalization(
        custom_formats_changed=custom_changes,
        explicit_styles_changed=explicit_changes,
    )
