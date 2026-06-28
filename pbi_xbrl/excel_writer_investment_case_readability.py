"""Investment Case readability/layout polish support."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, MutableMapping


@dataclass(frozen=True)
class InvestmentCaseReadabilityDeps:
    runtime: MutableMapping[str, Any]


class InvestmentCaseReadability:
    def __init__(self, deps: InvestmentCaseReadabilityDeps) -> None:
        self._runtime = deps.runtime

    def polish_investment_case_readability(self, ws: Any) -> None:
        """Apply final Investment_Case readability fixes without touching formulas."""
        Alignment = self._runtime["Alignment"]

        if ws is None or not str(getattr(ws, "title", "")).endswith("_Investment_Case"):
            return
        is_gtx_case = str(getattr(ws, "title", "") or "").strip() == "GTX_Investment_Case"

        max_row = int(getattr(ws, "max_row", 0) or 0)
        max_col = int(getattr(ws, "max_column", 0) or 0)

        def _txt(row: int, col: int) -> str:
            return str(ws.cell(row, col).value or "").strip()

        def _row_text(row: int) -> str:
            return " ".join(_txt(row, cc) for cc in range(1, min(max_col, 12) + 1) if _txt(row, cc))

        def _is_section(row: int) -> bool:
            fill = str(ws.cell(row, 1).fill.fgColor.rgb or "").upper()
            return bool(_txt(row, 1)) and fill.endswith(("5B9BD5", "6FA8DC"))

        for rr in range(1, max_row + 1):
            if _txt(rr, 1).lower() != "investment snapshot":
                continue
            for body_rr in range(rr + 1, max_row + 1):
                if not _txt(body_rr, 1) or _is_section(body_rr):
                    break
                ws.row_dimensions[body_rr].height = 24.0
            break

        note = "Uses Investment_Case manual inputs; may differ from Valuation Thesis Bridge."
        for rr in range(1, max_row + 1):
            if note.lower() in _row_text(rr).lower():
                ws.row_dimensions[rr].height = 13.5
                for cc in range(1, min(max_col, 10) + 1):
                    ws.cell(rr, cc).alignment = Alignment(horizontal="left", vertical="center", wrap_text=False)

        # Layout-aware note/read columns: only extend right when the table already
        # reserves a notes/read column near the right edge and the cells to the right
        # are empty. This keeps formulas and output columns untouched.
        note_headers = {
            "notes",
            "notes/source",
            "source / note",
            "source / confidence",
            "read",
            "source",
            "source / read",
            "investment read",
        }
        for rr in range(1, max_row + 1):
            note_col = 0
            for cc in range(1, min(max_col, 10) + 1):
                if _txt(rr, cc).lower() in note_headers:
                    note_col = cc
                    break
            if note_col < 8 or note_col >= 10:
                continue
            if any(_txt(rr, cc) for cc in range(note_col + 1, 11)):
                continue
            block_end = rr
            for body_rr in range(rr + 1, max_row + 1):
                if _is_section(body_rr):
                    break
                if not _row_text(body_rr):
                    break
                block_end = body_rr
            for body_rr in range(rr, block_end + 1):
                if any(_txt(body_rr, cc) for cc in range(note_col + 1, 11)):
                    continue
                coord = ws.cell(body_rr, note_col).coordinate
                if any(coord in merged for merged in ws.merged_cells.ranges):
                    continue
                try:
                    ws.merge_cells(start_row=body_rr, start_column=note_col, end_row=body_rr, end_column=10)
                except ValueError:
                    continue
                ws.cell(body_rr, note_col).alignment = Alignment(horizontal="left", vertical="top", wrap_text=True)

        if is_gtx_case:
            # GTX has longer scenario-read, quality-impact and "what needs to
            # happen" labels than the generic sector template.  Give those
            # columns peer-style breathing room without changing formulas or
            # source-backed content.
            for col, width in {
                "A": 50.0,
                "B": 36.0,
                "C": 34.0,
                "H": 30.0,
                "I": 28.0,
                "J": 28.0,
            }.items():
                current = float(ws.column_dimensions[col].width or 0.0)
                if current < width:
                    ws.column_dimensions[col].width = width
