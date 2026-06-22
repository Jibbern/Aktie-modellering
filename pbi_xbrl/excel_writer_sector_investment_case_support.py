"""Sector Investment Case workbook-reader support helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, MutableMapping, Optional, Set, Tuple


@dataclass(frozen=True)
class SectorInvestmentCaseSupportDeps:
    runtime: MutableMapping[str, Any]


class SectorInvestmentCaseSupport:
    def __init__(self, deps: SectorInvestmentCaseSupportDeps) -> None:
        self._runtime = deps.runtime

    def company_operating_margin_proxy_from_workbook(self, wb: Any) -> Tuple[Optional[float], str]:
        """Return the latest visible company operating-margin proxy from Valuation.

        This deliberately returns a concrete value, not a defined-name formula, so
        Investment_Case segment scenarios never depend on undefined names.
        """

        pd = self._runtime["pd"]
        math = self._runtime["math"]

        if wb is None or "Valuation" not in getattr(wb, "sheetnames", []):
            return None, ""
        ws = wb["Valuation"]
        candidates = [
            ("Operating margin %", "Company operating margin proxy"),
            ("EBIT margin %", "EBIT margin proxy"),
            ("Adj EBIT margin %", "Adjusted operating margin proxy"),
            ("Adj EBITDA margin %", "Adjusted EBITDA margin proxy"),
        ]
        for label, basis in candidates:
            for rr in range(1, int(ws.max_row or 0) + 1):
                if str(ws.cell(rr, 1).value or "").strip().lower() != label.lower():
                    continue
                for cc in range(int(ws.max_column or 1), 1, -1):
                    raw = ws.cell(rr, cc).value
                    val = pd.to_numeric(raw, errors="coerce")
                    if pd.isna(val):
                        continue
                    margin = float(val)
                    if abs(margin) > 1.5:
                        margin /= 100.0
                    if math.isfinite(margin) and -0.5 <= margin <= 0.5:
                        return margin, basis
        return None, ""

    def segment_scenario_label_aliases(self, label: Any) -> Set[str]:
        """Return normalized aliases used to match visible segment labels to BS_Segments.

        Company source sheets often use fuller names than the Investment_Case UI
        (for example, "Presort Services" vs "Presort").  Keep this matcher narrow:
        it should improve source-backed segment margin selection without turning
        into a fuzzy segment parser.
        """

        re = self._runtime["re"]

        text = str(label or "").strip().lower()

        def _norm(value: str) -> str:
            return re.sub(r"[^a-z0-9]+", "", value.lower())

        aliases = {_norm(text)}
        if "presort" in text:
            aliases.update({_norm("Presort"), _norm("Presort Services")})
        if "sendtech" in text or "send tech" in text:
            aliases.update({_norm("SendTech"), _norm("SendTech Solutions"), _norm("SendTech Services")})
        if "abercrombie" in text:
            aliases.update({_norm("Abercrombie"), _norm("Abercrombie brand")})
        if "hollister" in text:
            aliases.update({_norm("Hollister"), _norm("Hollister brand")})
        if "americas" in text:
            aliases.add(_norm("Americas"))
        if "emea" in text:
            aliases.add(_norm("EMEA"))
        if "apac" in text:
            aliases.add(_norm("APAC"))
        return {alias for alias in aliases if alias}

    def bs_segments_latest_segment_margin_from_workbook(self, wb: Any, label: Any) -> Tuple[Any, str]:
        """Return latest source-backed segment margin ref from BS_Segments if present."""

        pd = self._runtime["pd"]
        math = self._runtime["math"]
        re = self._runtime["re"]
        get_column_letter = self._runtime["get_column_letter"]

        if wb is None or "BS_Segments" not in getattr(wb, "sheetnames", []):
            return None, ""
        aliases = self.segment_scenario_label_aliases(label)
        if not aliases:
            return None, ""
        ws = wb["BS_Segments"]
        margin_sections = [
            ("segment operating margin %", "Segment operating margin"),
            ("operating margin %", "Segment operating margin"),
            ("segment ebit margin %", "Segment EBIT margin"),
            ("ebit margin %", "Segment EBIT margin"),
            ("segment adjusted ebit margin %", "Segment adjusted EBIT margin"),
            ("adjusted ebit margin %", "Segment adjusted EBIT margin"),
            ("segment adjusted ebitda margin %", "Segment adjusted EBITDA margin"),
            ("adjusted ebitda margin %", "Segment adjusted EBITDA margin"),
            ("ebitda margin %", "Segment EBITDA margin proxy"),
        ]
        section_basis: Dict[int, str] = {}
        for rr in range(1, int(ws.max_row or 0) + 1):
            row_label = str(ws.cell(rr, 1).value or "").strip().lower()
            for section_label, basis in margin_sections:
                if row_label == section_label:
                    section_basis[rr] = basis
                    break
        if not section_basis:
            return None, ""

        max_row = int(ws.max_row or 0)
        max_col = int(ws.max_column or 1)
        for section_row, basis in sorted(section_basis.items()):
            next_section = min([r for r in section_basis if r > section_row] or [max_row + 1])
            for rr in range(section_row + 1, min(next_section, section_row + 30, max_row + 1)):
                row_name = str(ws.cell(rr, 1).value or "").strip()
                row_key = re.sub(r"[^a-z0-9]+", "", row_name.lower())
                if not row_key or not any(alias in row_key or row_key in alias for alias in aliases):
                    continue
                for cc in range(max_col, 1, -1):
                    raw = ws.cell(rr, cc).value
                    val = pd.to_numeric(raw, errors="coerce")
                    if pd.isna(val):
                        continue
                    margin = float(val)
                    if abs(margin) > 1.5:
                        margin /= 100.0
                    if math.isfinite(margin) and -0.75 <= margin <= 1.0:
                        ref = f"='BS_Segments'!${get_column_letter(cc)}${rr}"
                        return ref, basis
        return None, ""
