from __future__ import annotations

from typing import Any


DEFAULT_EMPTY_SHEET_MESSAGE = "No data for current build"


def write_empty_sheet_placeholder(ws: Any, *, cell_ref: str = "A1", message: str = DEFAULT_EMPTY_SHEET_MESSAGE) -> None:
    """Write the shared workbook placeholder for empty-but-valid sheet outputs."""

    ws[cell_ref] = str(message or DEFAULT_EMPTY_SHEET_MESSAGE)
