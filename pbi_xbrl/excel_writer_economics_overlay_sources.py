from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple

import pandas as pd

from .guidance_lexicon import normalize_text as glx_normalize_text


@dataclass(frozen=True)
class EconomicsOverlaySourceSupportDeps:
    source_lines: Sequence[Mapping[str, Any]]
    economics_market_rows: Sequence[Mapping[str, Any]]
    coefficient_templates: Sequence[Any]
    as_of_market_quarter: Any
    driver_source_display: Callable[..., str]
    driver_source_note: Callable[..., str]
    parse_driver_number: Callable[..., Any]
    convert_market_price_value: Callable[..., Tuple[Any, bool]]
    economics_market_region_tags: Callable[..., Sequence[str]]
    quarter_label_short: Callable[..., str]


class EconomicsOverlaySourceSupport:
    def __init__(self, deps: EconomicsOverlaySourceSupportDeps) -> None:
        self._deps = deps
        self._coefficient_templates_by_key = {
            str(getattr(tpl, "key", "") or "").strip(): tpl
            for tpl in deps.coefficient_templates
            if str(getattr(tpl, "key", "") or "").strip()
        }
        self._coefficient_detail_cache: Dict[str, Dict[str, Any]] = {}

    def source_short(self, rec: Optional[Mapping[str, Any]]) -> str:
        if not rec:
            return ""
        qtxt = ""
        if isinstance(rec.get("quarter"), date):
            qtxt = f" ({rec['quarter'].isoformat()})"
        return f"{self._deps.driver_source_display(rec.get('source_type'), rec.get('source_doc'))}{qtxt}".strip()

    @staticmethod
    def line_has_alias(line_low: str, aliases: Tuple[str, ...]) -> bool:
        for alias in tuple(aliases or ()):
            alias_txt = str(alias or "").strip().lower()
            if alias_txt and re.search(rf"\b{re.escape(alias_txt)}\b", line_low):
                return True
        return False

    def best_line(
        self,
        aliases: Tuple[str, ...],
        *,
        extra_terms: Tuple[str, ...] = tuple(),
        exclude_terms: Tuple[str, ...] = tuple(),
        preferred_sources: Tuple[str, ...] = tuple(),
    ) -> Optional[Dict[str, Any]]:
        best = None
        best_score = -10000.0
        extra_low = tuple(str(x or "").strip().lower() for x in extra_terms if str(x or "").strip())
        exclude_low = tuple(str(x or "").strip().lower() for x in exclude_terms if str(x or "").strip())
        preferred_low = {str(x or "").strip().lower() for x in preferred_sources if str(x or "").strip()}
        for line_entry in self._deps.source_lines:
            line_txt = str(line_entry.get("line_txt") or "")
            line_low = str(line_entry.get("line_low") or "")
            if not self.line_has_alias(line_low, aliases):
                continue
            if exclude_low and any(tok in line_low for tok in exclude_low):
                continue
            if extra_low and not any(tok in line_low for tok in extra_low):
                continue
            source_rank = int(line_entry.get("source_rank") or 99)
            source_type = str(line_entry.get("source_type") or "").strip().lower()
            score = 90.0 - float(source_rank) * 6.0 - float(line_entry.get("fragment_penalty") or 0.0) * 3.0
            if bool(line_entry.get("is_complete_signal")):
                score += 4.0
            if preferred_low and source_type in preferred_low:
                score += 8.0
            qd = line_entry.get("quarter")
            if isinstance(qd, date):
                score += float(qd.strftime("%Y%m%d")) / 100000000.0
            if len(line_txt) <= 180:
                score += 2.0
            if bool(line_entry.get("has_sentence_end")):
                score += 1.0
            if score > best_score:
                best_score = score
                best = {"record": line_entry.get("record"), "line": line_txt, "quarter": qd}
        return best

    def parse_overlay_coefficient_value(self, line_txt: Any, coeff_key: str) -> Optional[float]:
        txt = glx_normalize_text(line_txt)
        low = txt.lower()

        def _pick(patterns: Tuple[str, ...]) -> Optional[float]:
            for pat in patterns:
                m = re.search(pat, txt, re.I)
                if not m:
                    continue
                val = self._deps.parse_driver_number(m.group(1))
                if val is not None:
                    return float(val)
            return None

        if coeff_key == "ethanol_yield":
            return _pick(
                (
                    r"([0-9]+(?:\.\d+)?)\s*(?:gallons?|gal)\s*(?:of ethanol)?\s*(?:per|/)\s*(?:bushel|bu)\b",
                    r"ethanol yield(?:\s+of)?\s*([0-9]+(?:\.\d+)?)\s*(?:gallons?|gal)\s*(?:per|/)\s*(?:bushel|bu)\b",
                )
            )
        if coeff_key == "renewable_corn_oil_yield":
            if any(tok in low for tok in ("incremental", "msc", "technology delivers", "premium to")):
                return None
            return _pick(
                (
                    r"([0-9]+(?:\.\d+)?)\s*(?:lbs?|pounds?)\s*(?:of renewable corn oil)?\s*(?:per|/)\s*(?:bushel|bu)\b",
                    r"renewable corn oil yield(?:\s+of)?\s*([0-9]+(?:\.\d+)?)\s*(?:lbs?|pounds?)\s*(?:per|/)\s*(?:bushel|bu)\b",
                )
            )
        if coeff_key == "distillers_yield":
            return _pick(
                (
                    r"([0-9]+(?:\.\d+)?)\s*(?:lbs?|pounds?)\s*(?:of distillers grains)?\s*(?:per|/)\s*(?:bushel|bu)\b",
                    r"distillers(?: grains)? yield(?:\s+of)?\s*([0-9]+(?:\.\d+)?)\s*(?:lbs?|pounds?)\s*(?:per|/)\s*(?:bushel|bu)\b",
                )
            )
        if coeff_key == "uhp_yield":
            return _pick(
                (
                    r"([0-9]+(?:\.\d+)?)\s*(?:lbs?|pounds?)\s*(?:of )?(?:uhp|ultra-high protein)\s*(?:per|/)\s*(?:bushel|bu)\b",
                    r"(?:uhp|ultra-high protein) yield(?:\s+of)?\s*([0-9]+(?:\.\d+)?)\s*(?:lbs?|pounds?)\s*(?:per|/)\s*(?:bushel|bu)\b",
                )
            )
        if coeff_key == "natural_gas_usage":
            btu_val = _pick((r"([0-9]{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?:btu|btus)\s*(?:per|/)\s*(?:gal|gallon)\b",))
            if btu_val is not None:
                return btu_val
            mmbtu_val = _pick((r"([0-9]+(?:\.\d+)?)\s*(?:mmbtu)\s*(?:per|/)\s*(?:gal|gallon)\b",))
            if mmbtu_val is not None:
                return float(mmbtu_val) * 1_000_000.0
            return None
        if coeff_key == "electricity_usage":
            return _pick((r"([0-9]+(?:\.\d+)?)\s*(?:kwh|kilowatt-hours?)\s*(?:per|/)\s*(?:gal|gallon)\b",))
        return None

    @staticmethod
    def market_unit_pattern(input_key: str) -> str:
        return {
            "corn_price": r"(?:bushel|bu)",
            "ethanol_price": r"(?:gal|gallon)",
            "distillers_grains_price": r"(?:lb|lbs|pound|pounds)",
            "uhp_price": r"(?:lb|lbs|pound|pounds)",
            "renewable_corn_oil_price": r"(?:lb|lbs|pound|pounds)",
            "natural_gas_price": r"(?:mmbtu)",
        }.get(input_key, r"(?:lb|lbs|pound|pounds|gal|gallon|bushel|bu|mmbtu)")

    def parse_market_input_value(self, line_txt: Any, input_key: str) -> Optional[float]:
        txt = glx_normalize_text(line_txt)
        unit_pat = self.market_unit_pattern(input_key)
        for pat in (
            rf"price of\s+\$?\s*([0-9]+(?:\.\d+)?)\s*(?:/|per)\s*{unit_pat}",
            rf"\$?\s*([0-9]+(?:\.\d+)?)\s*(?:/|per)\s*{unit_pat}",
        ):
            m = re.search(pat, txt, re.I)
            if not m:
                continue
            val = self._deps.parse_driver_number(m.group(1))
            if val is not None:
                return float(val)
        return None

    def driver_source_comment(self, rec: Optional[Mapping[str, Any]]) -> str:
        if not rec:
            return ""
        return self._deps.driver_source_note(rec.get("source_doc"), rec.get("Commentary"), rec.get("_source_note"))

    @staticmethod
    def market_quality_rank(txt: Any) -> int:
        low = str(txt or "").strip().lower()
        return 3 if low == "high" else 2 if low == "medium" else 1 if low == "low" else 0

    def pick_market_reference(
        self,
        tpl: Any,
        target_quarter: Optional[date] = None,
        *,
        exact_quarter: bool = False,
    ) -> Optional[Dict[str, Any]]:
        if not self._deps.economics_market_rows:
            return None
        series_keys = tuple(str(x or "").strip() for x in (getattr(tpl, "source_series_keys", ()) or ()) if str(x or "").strip())
        preferred_regions = tuple(str(x or "").strip().lower() for x in (getattr(tpl, "preferred_regions", ()) or ()) if str(x or "").strip())
        agg_pref = str(getattr(tpl, "aggregation_preference", "") or "quarter_avg").strip().lower()
        target_unit = str(getattr(tpl, "unit", "") or "").strip()
        quarter_cutoff = target_quarter if isinstance(target_quarter, date) else self._deps.as_of_market_quarter
        if not series_keys or not isinstance(quarter_cutoff, date):
            return None
        candidates: list[Tuple[Tuple[Any, ...], Dict[str, Any]]] = []
        for rec in self._deps.economics_market_rows:
            rec_q = rec.get("quarter")
            if not isinstance(rec_q, date):
                continue
            if exact_quarter:
                if rec_q != quarter_cutoff:
                    continue
            elif rec_q > quarter_cutoff:
                continue
            if str(rec.get("series_key") or "") not in series_keys:
                continue
            converted_val, converted = self._deps.convert_market_price_value(
                rec.get("price_value"),
                str(rec.get("unit") or ""),
                target_unit,
            )
            if converted_val is None:
                continue
            agg_level = str(rec.get("aggregation_level") or "").strip().lower()
            region_tags = self._deps.economics_market_region_tags(rec.get("region"))
            region_rank = 99
            for idx, pref in enumerate(preferred_regions):
                if pref in region_tags:
                    region_rank = idx
                    break
            try:
                series_rank = series_keys.index(str(rec.get("series_key") or ""))
            except ValueError:
                series_rank = 99
            agg_rank = 0 if agg_level == agg_pref else 1 if agg_level == "quarter_end" else 2
            score = (
                0 if exact_quarter else -int(rec_q.strftime("%Y%m%d")),
                agg_rank,
                region_rank,
                series_rank,
                -self.market_quality_rank(rec.get("quality")),
                -int(rec.get("_obs_count") or 0),
            )
            picked = dict(rec)
            picked["_converted_value"] = float(converted_val)
            picked["_converted"] = bool(converted)
            candidates.append((score, picked))
        if not candidates:
            return None
        return sorted(candidates, key=lambda item: item[0])[0][1]

    @staticmethod
    def parse_quarter_label_text(value_in: Any) -> Optional[date]:
        if isinstance(value_in, date):
            return value_in
        txt = str(value_in or "").strip()
        match = re.fullmatch(r"(\d{4})-Q([1-4])", txt)
        if not match:
            return None
        year_num = int(match.group(1))
        quarter_num = int(match.group(2))
        quarter_end_map = {
            1: date(year_num, 3, 31),
            2: date(year_num, 6, 30),
            3: date(year_num, 9, 30),
            4: date(year_num, 12, 31),
        }
        return quarter_end_map.get(quarter_num)

    def overlay_coefficient_detail(self, key_in: str) -> Dict[str, Any]:
        cache_key = str(key_in or "").strip()
        if cache_key in self._coefficient_detail_cache:
            return dict(self._coefficient_detail_cache[cache_key])
        tpl = self._coefficient_templates_by_key.get(cache_key)
        if tpl is None:
            self._coefficient_detail_cache[cache_key] = {}
            return {}
        aliases = tuple(getattr(tpl, "aliases", ()) or (str(getattr(tpl, "label", "") or ""),))
        best = self.best_line(aliases, preferred_sources=("10-K", "presentation", "earnings_release"))
        value = None
        basis = str(getattr(tpl, "default_basis", "") or "")
        source_txt = str(getattr(tpl, "default_source", "") or "")
        source_comment = ""
        if best is not None:
            parsed = self.parse_overlay_coefficient_value(best.get("line"), cache_key)
            if parsed is not None:
                value = float(parsed)
                basis = "reported"
                source_txt = self.source_short(best.get("record"))
                source_comment = self._deps.driver_source_note(best.get("record", {}).get("source_doc"), best.get("line"))
        if value is None and getattr(tpl, "default_value", None) is not None:
            value = float(getattr(tpl, "default_value"))
        if not source_txt and basis.strip().lower() == "user assumption":
            source_txt = "User assumption"
        detail = {
            "value": value,
            "basis": basis,
            "source_txt": source_txt,
            "source_comment": source_comment,
            "template": tpl,
        }
        self._coefficient_detail_cache[cache_key] = dict(detail)
        return detail

    @staticmethod
    def overlay_coefficient_basis_display(basis_in: Any) -> str:
        basis_txt = str(basis_in or "").strip()
        low = basis_txt.lower()
        return (
            "Reported"
            if low == "reported"
            else "Inferred"
            if low == "inferred"
            else "Report-aligned"
            if low == "report aligned"
            else "User-entered assumption"
            if low == "user assumption"
            else basis_txt
        )

    @staticmethod
    def overlay_coefficient_source_display(source_in: Any) -> str:
        source_txt = str(source_in or "").strip()
        low = source_txt.lower()
        return (
            "Platform baseline assumption"
            if low == "platform baseline coefficient"
            else "User-entered process assumption"
            if low == "process assumption"
            else "User-entered assumption"
            if low == "user assumption"
            else source_txt
        )

    def market_source_note(self, rec: Optional[Mapping[str, Any]]) -> str:
        if not rec:
            return ""
        qtxt = self._deps.quarter_label_short(rec.get("quarter"))
        agg_level = str(rec.get("aggregation_level") or "").strip().lower()
        agg_lbl = "avg" if agg_level == "quarter_avg" else "end" if agg_level == "quarter_end" else "obs"
        obs_count = int(rec.get("_obs_count") or 0)
        count_txt = f" | {obs_count} obs" if obs_count > 0 and agg_level != "observation" else ""
        converted_txt = " | proxied unit conversion" if bool(rec.get("_converted")) else ""
        return f"{qtxt} {agg_lbl} | {rec.get('source_type') or ''}{count_txt}{converted_txt}".strip(" |")
