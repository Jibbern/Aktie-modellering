"""GPRE commercial setup source support for workbook writer."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd


@dataclass(frozen=True)
class GpreCommercialSetupDeps:
    is_gpre_profile: bool
    ctx_ref: Any
    cache_dir: Any
    load_operating_driver_source_records_by_quarter: Callable[[], Mapping[Any, Sequence[Mapping[str, Any]]]]
    normalize_text: Callable[..., str]
    split_sentences: Callable[..., Sequence[str]]
    compact_snippet: Callable[..., str]
    ensure_terminal_period: Callable[..., str]
    data_root_from_sec_cache_path: Callable[..., Any]


class GpreCommercialSetupSupport:
    def __init__(self, deps: GpreCommercialSetupDeps) -> None:
        self.deps = deps
        self._commercial_setup_cache: Optional[List[Dict[str, Any]]] = None
        self._local_doc_text_cache: Dict[str, str] = {}

    def read_local_doc_text(self, path_in: Any) -> str:
        try:
            path_obj = Path(str(path_in or "")).expanduser().resolve()
        except Exception:
            return ""
        cache_key = str(path_obj)
        cached_txt = self._local_doc_text_cache.get(cache_key)
        if cached_txt is not None:
            return cached_txt
        txt = ""
        try:
            if path_obj.exists() and path_obj.is_file():
                txt = self.deps.normalize_text(path_obj.read_text(encoding="utf-8", errors="ignore"))
        except Exception:
            txt = ""
        self._local_doc_text_cache[cache_key] = txt
        return txt

    def _local_conference_dirs(self) -> List[Path]:
        dirs: List[Path] = []
        portable_root: Optional[Path] = None
        try:
            portable_root = self.deps.data_root_from_sec_cache_path(Path(self.deps.cache_dir)) if self.deps.cache_dir is not None else None
        except Exception:
            portable_root = None
        if portable_root is not None:
            dirs.append(portable_root / "tickers" / "GPRE" / "conferences")
        dirs.append(Path(__file__).resolve().parents[2] / "GPRE" / "conferences")
        out: List[Path] = []
        seen: set[str] = set()
        for path_obj in dirs:
            try:
                key = str(path_obj.expanduser().resolve())
            except Exception:
                key = str(path_obj)
            if key in seen:
                continue
            seen.add(key)
            out.append(path_obj)
        return out

    def _local_conference_path(self, metadata_name: str, raw_name: str) -> Path:
        fallback = self._local_conference_dirs()[0] / raw_name
        for conf_dir in self._local_conference_dirs():
            metadata_path = conf_dir / metadata_name
            if metadata_path.exists():
                return metadata_path
            raw_path = conf_dir / raw_name
            if raw_path.exists():
                fallback = raw_path
        return fallback

    def _local_conference_raw_path(self, raw_name: str) -> Path:
        fallback = self._local_conference_dirs()[0] / raw_name
        for conf_dir in self._local_conference_dirs():
            raw_path = conf_dir / raw_name
            if raw_path.exists():
                return raw_path
        return fallback

    def local_bofa_conference_path(self) -> Path:
        return self._local_conference_path(
            "BofA_America_Conference_2026_METADATA_EN.txt",
            "BofA_America_Conference_2026.txt",
        )

    def local_bofa_conference_text(self) -> str:
        return self.read_local_doc_text(self.local_bofa_conference_path())

    def local_stephens_conference_path(self) -> Path:
        return self._local_conference_path(
            "Stephens_Annual_Investment_Conference_2025_METADATA_EN.txt",
            "Stephens_Annual_Investment_Conference_2025.txt",
        )

    def local_stephens_conference_raw_path(self) -> Path:
        return self._local_conference_raw_path("Stephens_Annual_Investment_Conference_2025.txt")

    def local_stephens_conference_text(self) -> str:
        return self.read_local_doc_text(self.local_stephens_conference_path())

    def local_stephens_conference_raw_text(self) -> str:
        return self.read_local_doc_text(self.local_stephens_conference_raw_path())

    def local_bmo_conference_path(self) -> Path:
        return self._local_conference_path(
            "GPRE_BMO_Farm_to_Market_2026_METADATA_EN.txt",
            "21st Annual Global Farm to Market Conference (BMO conference) may 13, 2026.txt",
        )

    def local_bmo_conference_text(self) -> str:
        return self.read_local_doc_text(self.local_bmo_conference_path())

    def local_bofa_conference_excerpt(self, terms: Tuple[str, ...], max_len: int = 280) -> str:
        conf_txt = self.local_bofa_conference_text()
        if not conf_txt:
            return ""
        sentences = [str(s).strip() for s in self.deps.split_sentences(conf_txt) if str(s).strip()]
        if not sentences:
            return ""
        terms_low = [self.deps.normalize_text(term).lower() for term in terms if str(term or "").strip()]
        matched = [s for s in sentences if any(term in self.deps.normalize_text(s).lower() for term in terms_low)]
        if matched:
            return self.deps.ensure_terminal_period(self.deps.compact_snippet(" ".join(matched[:3]), max_len))
        return self.deps.ensure_terminal_period(self.deps.compact_snippet(conf_txt, max_len))

    def records(self) -> List[Dict[str, Any]]:
        deps = self.deps
        is_gpre_profile = deps.is_gpre_profile
        ctx_ref = deps.ctx_ref
        _load_operating_driver_source_records_by_quarter = deps.load_operating_driver_source_records_by_quarter
        glx_normalize_text = deps.normalize_text
        glx_split_sentences = deps.split_sentences
        qn_compact_snippet = deps.compact_snippet
        _ensure_terminal_period = deps.ensure_terminal_period
        _gpre_local_bofa_conference_text_shared = self.local_bofa_conference_text
        _gpre_local_bofa_conference_path_shared = self.local_bofa_conference_path
        _gpre_local_bofa_conference_excerpt_shared = self.local_bofa_conference_excerpt
        if not is_gpre_profile:
            return []
        if self._commercial_setup_cache is not None:
            return [dict(x) for x in self._commercial_setup_cache]

        records_by_quarter = _load_operating_driver_source_records_by_quarter()
        if ctx_ref is not None:
            callbacks_obj = getattr(ctx_ref, "callbacks", None)
            callback_override = getattr(callbacks_obj, "load_operating_driver_source_records_by_quarter", None)
            if callable(callback_override) and callback_override is not _load_operating_driver_source_records_by_quarter:
                try:
                    override_records = callback_override()
                except Exception:
                    override_records = None
                if isinstance(override_records, dict) and override_records:
                    records_by_quarter = override_records
        out_items: List[Dict[str, Any]] = []

        def _pick_record(qd: date, must_terms: Tuple[str, ...]) -> Optional[Dict[str, Any]]:
            def _norm_match(txt_in: Any) -> str:
                return re.sub(r"[\s\-_/]+", " ", glx_normalize_text(str(txt_in or "")).lower()).strip()

            best: Optional[Dict[str, Any]] = None
            best_score = -10000.0
            must_terms_norm = tuple(_norm_match(term) for term in must_terms if str(term or "").strip())
            for rec in records_by_quarter.get(qd, []) or []:
                blob = _norm_match(rec.get("_text_low") or rec.get("text") or "")
                if not blob or any(term not in blob for term in must_terms_norm):
                    continue
                source_type = str(rec.get("source_type") or "").strip().lower()
                score = 100.0 - float(rec.get("source_rank") or 99) * 5.0 - float(rec.get("_fragment_penalty") or 0.0) * 2.0
                if "transcript" in source_type:
                    score += 10.0
                elif "conference" in source_type:
                    score += 8.0
                elif "presentation" in source_type:
                    score += 4.0
                if bool(rec.get("_is_complete_signal")):
                    score += 2.0
                if score > best_score:
                    best_score = score
                    best = rec
            return dict(best) if best is not None else None

        def _extract_excerpt(text_in: Any, terms: Tuple[str, ...]) -> str:
            txt = glx_normalize_text(text_in)
            if not txt:
                return ""
            sentences = [str(s).strip() for s in glx_split_sentences(txt) if str(s).strip()]
            matched = [s for s in sentences if any(str(term or "").strip().lower() in s.lower() for term in terms)]
            if matched:
                return _ensure_terminal_period(qn_compact_snippet(" ".join(matched[:2]), 280))
            return _ensure_terminal_period(qn_compact_snippet(txt, 280))

        def _quarter_label(v: Any) -> str:
            t = pd.to_datetime(v, errors="coerce")
            if pd.isna(t):
                return "N/A"
            qn = ((int(t.month) - 1) // 3) + 1
            return f"Q{qn} {int(t.year)}"

        def _source_payload(source_type: str, source_doc: str, source_date_in: Any, source_quarter_in: Any) -> Dict[str, Any]:
            return {
                "source_type": source_type,
                "doc": source_doc,
                "form": source_type,
                "section": Path(str(source_doc or "")).name,
                "date": pd.to_datetime(source_date_in, errors="coerce"),
                "source_quarter_end": pd.to_datetime(source_quarter_in, errors="coerce"),
            }

        def _append_item(
            *,
            source_quarter: date,
            horizon_label: str,
            horizon_norm: str,
            setup_type: str,
            setup_display: str,
            source_type: str,
            source_doc: str,
            source_date: Any,
            source_excerpt: str,
            coverage_text: str = "",
            coverage_pct_low: Optional[float] = None,
            coverage_pct_high: Optional[float] = None,
            coverage_volume: str = "",
            locked_margin_text: str = "",
            locked_margin_low: Optional[float] = None,
            locked_margin_high: Optional[float] = None,
            setup_quality: str = "",
            openness_text: str = "",
            openness_level: str = "",
            legs_involved: str = "",
            result_effect: str = "",
            management_takeaway: str = "",
            confidence: str = "high",
            needs_manual_review: bool = False,
            guidance_metric: str = "",
            guidance_text: str = "",
            guidance_score: float = 82.0,
            commentary_text: str = "",
            commentary_priority: int = 50,
            future_latest_only: bool = False,
            show_in_guidance: bool = True,
            show_in_setup: bool = True,
            show_in_management_commentary: bool = True,
            commentary_home: str = "overlay_management",
        ) -> None:
            out_items.append(
                    {
                        "source_quarter": source_quarter,
                        "source_quarter_label": _quarter_label(source_quarter),
                    "horizon_quarter": horizon_label,
                    "horizon_period_norm": horizon_norm,
                    "source_type": source_type,
                    "setup_type": setup_type,
                    "setup_display": setup_display,
                    "coverage_text": coverage_text,
                    "coverage_pct_low": coverage_pct_low,
                    "coverage_pct_high": coverage_pct_high,
                    "coverage_volume": coverage_volume,
                    "locked_margin_text": locked_margin_text,
                    "locked_margin_low": locked_margin_low,
                    "locked_margin_high": locked_margin_high,
                    "setup_quality": setup_quality,
                    "openness_text": openness_text,
                    "openness_level": openness_level,
                    "legs_involved": legs_involved,
                    "result_effect": result_effect,
                    "management_takeaway": management_takeaway,
                    "source_excerpt": source_excerpt,
                    "source_location": source_doc,
                    "confidence": confidence,
                    "needs_manual_review": bool(needs_manual_review),
                    "guidance_metric": guidance_metric,
                    "guidance_text": _ensure_terminal_period(guidance_text) if guidance_text else "",
                    "guidance_score": float(guidance_score),
                    "commentary_text": _ensure_terminal_period(commentary_text) if commentary_text else "",
                    "commentary_priority": int(commentary_priority),
                    "_future_latest_only": bool(future_latest_only),
                    "show_in_guidance": bool(show_in_guidance),
                    "show_in_setup": bool(show_in_setup),
                    "show_in_management_commentary": bool(show_in_management_commentary),
                    "commentary_home": str(commentary_home or "overlay_management"),
                    "source_date": pd.to_datetime(source_date, errors="coerce"),
                    "source": _source_payload(source_type, source_doc, source_date, source_quarter),
                }
            )

        q1_2023_rec = _pick_record(date(2023, 3, 31), ("50 million to 100 million gallons", "$0.22 to $0.25"))
        if q1_2023_rec:
            _append_item(
                source_quarter=date(2023, 3, 31),
                horizon_label="Q4 2023 / year-end",
                horizon_norm="Q2023Q4",
                setup_type="locked_margin_setup",
                setup_display="Selective hedging",
                source_type=str(q1_2023_rec.get("source_type") or ""),
                source_doc=str(q1_2023_rec.get("source_doc") or ""),
                source_date=date(2023, 5, 4),
                source_excerpt=_extract_excerpt(q1_2023_rec.get("text"), ("50 million to 100 million", "$0.22 to $0.25", "hedged")),
                coverage_text="50m-100m gallons hedged through year-end; most production remained open",
                coverage_volume="50m-100m gallons",
                locked_margin_text="Q4 paper margin about $0.22-$0.25/gal",
                locked_margin_low=0.22,
                locked_margin_high=0.25,
                setup_quality="selective",
                openness_text="",
                legs_involved="Base crush",
                management_takeaway="Management planned to lock selectively rather than hedge everything",
                guidance_metric="Locked margin setup",
                guidance_text="Q4 paper margins were about $0.22-$0.25/gal.",
                commentary_text="50m-100m gallons were hedged through year-end, while most production remained open.",
                commentary_priority=12,
            )

        q2_2023_rec = _pick_record(date(2023, 6, 30), ("lock in over $0.20 a gallon", "took most of that away"))
        if q2_2023_rec:
            _append_item(
                source_quarter=date(2023, 6, 30),
                horizon_label="Q2 2023",
                horizon_norm="Q2023Q2",
                setup_type="hedge_realization_effect",
                setup_display="Locked margin not realized",
                source_type=str(q2_2023_rec.get("source_type") or ""),
                source_doc=str(q2_2023_rec.get("source_doc") or ""),
                source_date=date(2023, 8, 4),
                source_excerpt=_extract_excerpt(q2_2023_rec.get("text"), ("lock in over $0.20 a gallon", "took most of that away", "outages")),
                locked_margin_text="Q2 hedges locked in over $0.20/gal",
                locked_margin_low=0.20,
                setup_quality="mixed realization",
                result_effect="The prior $0.12-$0.17/gal on-paper setup tracked through May before outages and delays took most of the benefit away",
                management_takeaway="The quarter tracked at or better than the prior on-paper setup through May, but operations prevented realization",
                guidance_metric="Locked margin setup",
                guidance_text="Q2 hedges locked in over $0.20/gal.",
                commentary_text="Management said the prior $0.12-$0.17/gal on-paper setup was tracking through May before Wood River delays and outages took most of the locked Q2 benefit away.",
                commentary_priority=10,
            )

        q1_2024_rec = _pick_record(date(2024, 3, 31), ("mid-high single digits", "primarily open to the margin structure"))
        if q1_2024_rec:
            _append_item(
                source_quarter=date(2024, 3, 31),
                horizon_label="Q2 2024",
                horizon_norm="Q2024Q2",
                setup_type="coverage_openness",
                setup_display="Open Q2 setup",
                source_type=str(q1_2024_rec.get("source_type") or ""),
                source_doc=str(q1_2024_rec.get("source_doc") or ""),
                source_date=date(2024, 5, 9),
                source_excerpt=_extract_excerpt(q1_2024_rec.get("text"), ("mid-high single digits", "low teens", "primarily open")),
                coverage_text="Primarily open to the margin structure across products",
                locked_margin_text="Q2 margins ranged from the mid-high single digits to the low teens",
                setup_quality="primarily open",
                openness_text="",
                openness_level="largely_open",
                legs_involved="Across products",
                management_takeaway="Management kept the book primarily open rather than heavily locking Q2",
                guidance_metric="Coverage / openness",
                guidance_text="Q2 margins averaged from the mid-high single digits to the low teens with the book primarily open across products.",
                commentary_text="Management said the business remained primarily open to the margin structure across all products.",
                commentary_priority=9,
            )

        q3_2023_rec = _pick_record(date(2023, 9, 30), ("largely open to the expanded margins in the fourth quarter", "winter gas"))
        if q3_2023_rec:
            _append_item(
                source_quarter=date(2023, 9, 30),
                horizon_label="Q4 2023",
                horizon_norm="Q2023Q4",
                setup_type="commercial_positioning",
                setup_display="Open Q4 setup",
                source_type=str(q3_2023_rec.get("source_type") or ""),
                source_doc=str(q3_2023_rec.get("source_doc") or ""),
                source_date=date(2023, 10, 31),
                source_excerpt=_extract_excerpt(q3_2023_rec.get("text"), ("largely open", "priced some of our fourth quarter early", "veg oil", "winter gas", "physical corn")),
                coverage_text="Largely open to expanded Q4 margins",
                locked_margin_text="Some Q4 veg oil priced early above market; winter gas at or below market",
                setup_quality="open with selective coverage",
                openness_text="",
                openness_level="largely_open",
                legs_involved="Veg oil, winter gas, physical corn basis",
                management_takeaway="Some Q4 veg oil was priced early before prices fell further; physical corn basis was generally covered at or below market",
                guidance_metric="Coverage / openness",
                guidance_text="Q4 remained largely open to expanded margins.",
                commentary_text="Management said some Q4 veg oil was priced early before prices fell further, while winter gas and physical corn were generally covered at or below market.",
                commentary_priority=9,
            )
        q3_2023_util_rec = _pick_record(date(2023, 9, 30), ("production at 93.9% of capacity",))
        if q3_2023_util_rec:
            _append_item(
                source_quarter=date(2023, 9, 30),
                horizon_label="",
                horizon_norm="Q2023Q3",
                setup_type="management_commentary",
                setup_display="Utilization",
                source_type=str(q3_2023_util_rec.get("source_type") or ""),
                source_doc=str(q3_2023_util_rec.get("source_doc") or ""),
                source_date=date(2023, 10, 31),
                source_excerpt=_extract_excerpt(q3_2023_util_rec.get("text"), ("production at 93.9% of capacity",)),
                confidence="medium",
                commentary_text="Plant utilization reflected 93.9% during the quarter, returning the platform to consistent operations.",
                commentary_priority=7,
                show_in_setup=False,
                commentary_home="operating_commentary",
            )

        q2_2024_rec = _pick_record(date(2024, 6, 30), ("high 20s to high 30s per gallon", "some of the third quarter production"))
        q2_2024_open_rec = _pick_record(date(2024, 6, 30), ("significant amount open for the third quarter", "de minimis hedging"))
        if q2_2024_rec:
            excerpt_blob = " | ".join(
                [
                    _extract_excerpt(q2_2024_rec.get("text"), ("high 20s to high 30s", "some of the third quarter production")),
                    _extract_excerpt((q2_2024_open_rec or {}).get("text"), ("significant amount open", "de minimis hedging")),
                ]
            ).strip(" |")
            _append_item(
                source_quarter=date(2024, 6, 30),
                horizon_label="Q3 2024 / Q4 2024",
                horizon_norm="Q2024Q3",
                setup_type="all_in_margin_setup",
                setup_display="All-in margin setup",
                source_type=str(q2_2024_rec.get("source_type") or ""),
                source_doc=str(q2_2024_rec.get("source_doc") or ""),
                source_date=date(2024, 8, 6),
                source_excerpt=excerpt_blob,
                coverage_text="Some Q3 production hedged; significant Q3 volume still open; de minimis Q4 hedging",
                locked_margin_text="Q3 all-in margins roughly $0.20-$0.30+ per gallon",
                setup_quality="disciplined but still open",
                openness_text="",
                openness_level="partially_open",
                legs_involved="All-in margins, not just simple crush | Physical corn basis",
                management_takeaway="Physical corn was about 85% covered across the platform",
                guidance_metric="All-in margin setup",
                guidance_text="Q3 all-in margins were roughly $0.20-$0.30+ per gallon.",
                commentary_text="Some Q3 production was hedged near peak July margins, while significant volume remained open.",
                commentary_priority=8,
            )

        q4_2024_rec = _pick_record(date(2024, 12, 31), ("largely unhedged and open to the crush", "wrong choice"))
        if q4_2024_rec:
            _append_item(
                source_quarter=date(2024, 12, 31),
                horizon_label="Q4 2024",
                horizon_norm="Q2024Q4",
                setup_type="coverage_openness",
                setup_display="Open Q4 setup",
                source_type=str(q4_2024_rec.get("source_type") or ""),
                source_doc=str(q4_2024_rec.get("source_doc") or ""),
                source_date=date(2025, 2, 7),
                source_excerpt=_extract_excerpt(q4_2024_rec.get("text"), ("largely unhedged", "open to the crush", "wrong choice")),
                coverage_text="Largely unhedged / open to the crush going into Q4",
                setup_quality="too open",
                openness_text="",
                openness_level="largely_open",
                result_effect="Management later said it was the wrong choice",
                management_takeaway="The quarter highlighted the cost of staying too open",
                guidance_metric="Coverage / openness",
                guidance_text="The business was largely unhedged going into Q4.",
                commentary_text="Management later said remaining largely open to the crush going into Q4 was the wrong choice.",
                commentary_priority=7,
            )
        q4_2024_util_rec = _pick_record(date(2024, 12, 31), ("production at 92% of capacity",))
        q4_2023_util_rec = _pick_record(date(2023, 12, 31), ("plant utilization rate of 95%",))
        if q4_2024_util_rec and q4_2023_util_rec:
            _append_item(
                source_quarter=date(2024, 12, 31),
                horizon_label="",
                horizon_norm="Q2024Q4",
                setup_type="management_commentary",
                setup_display="Utilization",
                source_type=str(q4_2024_util_rec.get("source_type") or ""),
                source_doc=str(q4_2024_util_rec.get("source_doc") or ""),
                source_date=date(2025, 2, 7),
                source_excerpt=_extract_excerpt(q4_2024_util_rec.get("text"), ("production at 92% of capacity",)),
                confidence="medium",
                commentary_text="Plant utilization reflected 92% during the fourth quarter compared to the 95% run rate reported in the same period last year.",
                commentary_priority=6,
                show_in_setup=False,
                commentary_home="operating_commentary",
            )

        q1_2025_cov_rec = _pick_record(date(2025, 3, 31), ("a little more than half of our q2 crush margins",))
        q1_2025_hedge_rec = _pick_record(date(2025, 3, 31), ("lock in some of those margins", "simple crush"))
        q1_2025_open_rec = _pick_record(date(2025, 3, 31), ("maybe 50% to 70%", "there's no script"))
        if q1_2025_cov_rec:
            excerpt_parts = [
                _extract_excerpt(q1_2025_cov_rec.get("text"), ("a little more than half", "favorable levels")),
                _extract_excerpt((q1_2025_hedge_rec or {}).get("text"), ("simple crush", "hedge dco", "hedge in meal")),
                _extract_excerpt((q1_2025_open_rec or {}).get("text"), ("50% to 70%", "not a mark")),
            ]
            _append_item(
                source_quarter=date(2025, 3, 31),
                horizon_label="Q2 2025 / later quarters",
                horizon_norm="Q2025Q2",
                setup_type="coverage_openness",
                setup_display="Disciplined hedging",
                source_type=str(q1_2025_cov_rec.get("source_type") or ""),
                source_doc=str(q1_2025_cov_rec.get("source_doc") or ""),
                source_date=date(2025, 5, 8),
                source_excerpt=" | ".join([x for x in excerpt_parts if x]),
                coverage_text="Just over half of Q2 crush margins were secured; Q3 and Q4 remained more open",
                setup_quality="disciplined",
                openness_text="",
                openness_level="partially_open",
                legs_involved="Simple crush, board crush, DCO and meal",
                management_takeaway="The program was risk-driven, not fixed at 50%; 50%-70% was the rough top end",
                guidance_metric="Coverage / openness",
                guidance_text="Just over half of Q2 crush margins were secured at favorable levels.",
                commentary_text="Most hedge positioning was concentrated in Q2, with Q3 and Q4 still more open.",
                commentary_priority=6,
            )

        q2_2025_rec = _pick_record(date(2025, 6, 30), ("65% crushed", "small benefit"))
        if q2_2025_rec:
            _append_item(
                source_quarter=date(2025, 6, 30),
                horizon_label="Q3 2025",
                horizon_norm="Q2025Q3",
                setup_type="coverage_openness",
                setup_display="Q3 hedge coverage",
                source_type=str(q2_2025_rec.get("source_type") or ""),
                source_doc=str(q2_2025_rec.get("source_doc") or ""),
                source_date=date(2025, 8, 11),
                source_excerpt=_extract_excerpt(q2_2025_rec.get("text"), ("small benefit", "65% crushed", "not just that simple crush")),
                coverage_text="Q3 was about 65% crushed, moving closer to 70%",
                coverage_pct_low=65.0,
                coverage_pct_high=70.0,
                setup_quality="more aggressive",
                openness_text="",
                openness_level="mostly_covered",
                legs_involved="Corn bought, DDGs sold and corn oil pricing",
                result_effect="Q2 hedging was a small benefit",
                management_takeaway="The setup was broader than simple crush alone",
                guidance_metric="Coverage / openness",
                guidance_text="Q3 was about 65% crushed, moving closer to 70%.",
                commentary_text="Q2 hedging was a small benefit, and the Q3 setup included corn, DDGs and corn oil beyond simple crush.",
                commentary_priority=5,
            )

        q3_2025_transcript_rec = _pick_record(date(2025, 9, 30), ("75% hedged on crush in the fourth quarter", "put positions on for q1 in 2026"))
        if q3_2025_transcript_rec:
            _append_item(
                source_quarter=date(2025, 9, 30),
                horizon_label="Q4 2025 / Q1 2026",
                horizon_norm="Q2025Q4",
                setup_type="coverage_openness",
                setup_display="Forward hedge positioning",
                source_type=str(q3_2025_transcript_rec.get("source_type") or ""),
                source_doc=str(q3_2025_transcript_rec.get("source_doc") or ""),
                source_date=date(2025, 9, 30),
                source_excerpt=_extract_excerpt(q3_2025_transcript_rec.get("text"), ("75% hedged on crush", "put positions on for q1 in 2026", "disciplined hedging strategy")),
                coverage_text="Q4 crush about 75% hedged; Q1 2026 positions already on",
                setup_quality="disciplined",
                openness_text="",
                openness_level="mostly_covered",
                management_takeaway="Management said forward hedges had already been layered into Q4 and Q1 2026.",
                confidence="medium",
                guidance_metric="Risk management",
                guidance_text="Management said Q4 crush was about 75% hedged and positions had been put on for Q1 2026.",
                commentary_text="Management said Q4 crush was about 75% hedged and positions had been put on for Q1 2026.",
                commentary_priority=4,
            )
        q3_2025_active_rec = _pick_record(date(2025, 9, 30), ("active in q4 and q1 of 2026", "looking for opportunities to lock in margin"))
        if q3_2025_active_rec:
            _append_item(
                source_quarter=date(2025, 9, 30),
                horizon_label="Q4 2025 / Q1 2026",
                horizon_norm="Q2025Q4",
                setup_type="hedge_execution",
                setup_display="Active lock-in execution",
                source_type=str(q3_2025_active_rec.get("source_type") or ""),
                source_doc=str(q3_2025_active_rec.get("source_doc") or ""),
                source_date=date(2025, 9, 30),
                source_excerpt=_extract_excerpt(q3_2025_active_rec.get("text"), ("active in q4 and q1 of 2026", "looking for opportunities to lock in margin")),
                coverage_text="Management said the team stayed active in Q4 and Q1 2026 rather than following a fixed hedge script",
                setup_quality="active execution",
                openness_level="actively_managed",
                management_takeaway="Management said the team was in the market every day looking for lock-in opportunities.",
                confidence="medium",
                guidance_metric="Risk management",
                guidance_text="Management said the team stayed active in Q4 and Q1 2026 looking for margin lock-in opportunities.",
                commentary_text="Management said the team stayed active in Q4 and Q1 2026, looking daily for lock-in opportunities.",
                commentary_priority=3,
            )
        q3_2025_margin_rec = _pick_record(date(2025, 9, 30), ("overall margin structure improves significantly", "stronger corn oil values"))
        if q3_2025_margin_rec:
            _append_item(
                source_quarter=date(2025, 9, 30),
                horizon_label="",
                horizon_norm="Q2025Q3",
                setup_type="management_commentary",
                setup_display="Margin structure",
                source_type=str(q3_2025_margin_rec.get("source_type") or ""),
                source_doc=str(q3_2025_margin_rec.get("source_doc") or ""),
                source_date=date(2025, 9, 30),
                source_excerpt=_extract_excerpt(q3_2025_margin_rec.get("text"), ("margin structure", "lower input costs", "stronger corn oil values")),
                confidence="medium",
                commentary_text="Late-Q3 and early-Q4 margin structure improved on tighter ethanol supply, lower input costs and stronger corn-oil values.",
                commentary_priority=2,
                show_in_setup=False,
            )
        q3_2025_demand_rec = _pick_record(date(2025, 9, 30), ("healthy export volumes", "growing acceptance of e15"))
        if q3_2025_demand_rec:
            _append_item(
                source_quarter=date(2025, 9, 30),
                horizon_label="Q4 2025 / 2026",
                horizon_norm="Q2025Q4",
                setup_type="management_commentary",
                setup_display="Demand support",
                source_type=str(q3_2025_demand_rec.get("source_type") or ""),
                source_doc=str(q3_2025_demand_rec.get("source_doc") or ""),
                source_date=date(2025, 9, 30),
                source_excerpt=_extract_excerpt(q3_2025_demand_rec.get("text"), ("healthy export volumes", "growing acceptance of e15")),
                confidence="medium",
                commentary_text="Healthy export volumes and wider E15 acceptance were cited as demand supports into 2026.",
                commentary_priority=3,
                show_in_setup=False,
            )
        q3_2025_pressure_rec = _pick_record(date(2025, 9, 30), ("ddgs and high protein values remained under pressure",))
        if q3_2025_pressure_rec:
            _append_item(
                source_quarter=date(2025, 9, 30),
                horizon_label="",
                horizon_norm="Q2025Q3",
                setup_type="management_commentary",
                setup_display="Coproduct pressure",
                source_type=str(q3_2025_pressure_rec.get("source_type") or ""),
                source_doc=str(q3_2025_pressure_rec.get("source_doc") or ""),
                source_date=date(2025, 9, 30),
                source_excerpt=_extract_excerpt(q3_2025_pressure_rec.get("text"), ("ddgs and high protein values remained under pressure",)),
                confidence="medium",
                commentary_text="DDGS and high-protein values remained under pressure through much of the quarter.",
                commentary_priority=4,
                show_in_setup=False,
            )
        q3_2025_util_rec = _pick_record(date(2025, 9, 30), ("above 100% capacity utilization",))
        if q3_2025_util_rec:
            _append_item(
                source_quarter=date(2025, 9, 30),
                horizon_label="",
                horizon_norm="Q2025Q3",
                setup_type="management_commentary",
                setup_display="Utilization",
                source_type=str(q3_2025_util_rec.get("source_type") or ""),
                source_doc=str(q3_2025_util_rec.get("source_doc") or ""),
                source_date=date(2025, 9, 30),
                source_excerpt=_extract_excerpt(q3_2025_util_rec.get("text"), ("above 100% capacity utilization",)),
                confidence="medium",
                commentary_text="Plants ran above 100% capacity utilization during the quarter.",
                commentary_priority=4,
                show_in_setup=False,
                commentary_home="operating_commentary",
            )
        q3_2025_reliability_rec = _pick_record(date(2025, 9, 30), ("reliability-centered maintenance", "planned and unplanned downtime"))
        if q3_2025_reliability_rec:
            _append_item(
                source_quarter=date(2025, 9, 30),
                horizon_label="",
                horizon_norm="Q2025Q3",
                setup_type="management_commentary",
                setup_display="Reliability",
                source_type=str(q3_2025_reliability_rec.get("source_type") or ""),
                source_doc=str(q3_2025_reliability_rec.get("source_doc") or ""),
                source_date=date(2025, 9, 30),
                source_excerpt=_extract_excerpt(q3_2025_reliability_rec.get("text"), ("reliability-centered maintenance", "planned and unplanned downtime")),
                confidence="medium",
                commentary_text="Reliability-centered maintenance reduced planned and unplanned downtime.",
                commentary_priority=5,
                show_in_setup=False,
                commentary_home="operating_commentary",
            )

        q4_2025_transcript_rec = _pick_record(date(2025, 12, 31), ("significant portion of our q1 production margin logged in",))
        if q4_2025_transcript_rec:
            _append_item(
                source_quarter=date(2025, 12, 31),
                horizon_label="Q1 2026",
                horizon_norm="Q2026Q1",
                setup_type="coverage_openness",
                setup_display="Q1 margin positioning",
                source_type=str(q4_2025_transcript_rec.get("source_type") or ""),
                source_doc=str(q4_2025_transcript_rec.get("source_doc") or ""),
                source_date=date(2025, 12, 31),
                source_excerpt=_extract_excerpt(q4_2025_transcript_rec.get("text"), ("significant portion of our q1 production margin logged in", "disciplined risk management approach")),
                coverage_text="Significant portion of Q1 production margin already logged in",
                setup_quality="disciplined",
                openness_text="",
                openness_level="partially_open",
                management_takeaway="Management said a significant portion of Q1 production margin was already logged in.",
                confidence="medium",
                guidance_metric="Risk management",
                guidance_text="Management said a significant portion of Q1 production margin was already logged in.",
                commentary_text="Management said a significant portion of Q1 production margin was already logged in.",
                commentary_priority=4,
                show_in_guidance=False,
            )
        q4_2025_payoff_rec = _pick_record(date(2025, 12, 31), ("partially hedged, heading into q4", "positions paid off as ethanol softened later in the quarter"))
        if q4_2025_payoff_rec:
            _append_item(
                source_quarter=date(2025, 12, 31),
                horizon_label="Q4 2025",
                horizon_norm="Q2025Q4",
                setup_type="hedge_realization_effect",
                setup_display="Hedge payoff",
                source_type=str(q4_2025_payoff_rec.get("source_type") or ""),
                source_doc=str(q4_2025_payoff_rec.get("source_doc") or ""),
                source_date=date(2025, 12, 31),
                source_excerpt=_extract_excerpt(q4_2025_payoff_rec.get("text"), ("partially hedged", "positions paid off as ethanol softened")),
                coverage_text="Management said the business entered Q4 partially hedged",
                setup_quality="disciplined",
                result_effect="Those hedge positions paid off as ethanol softened later in the quarter",
                management_takeaway="Management said partial hedges helped when ethanol prices softened.",
                confidence="medium",
                guidance_metric="Risk management",
                guidance_text="Management said the business entered Q4 partially hedged and those positions paid off later in the quarter.",
                commentary_text="Management said the quarter benefited from positions that were already in place as ethanol softened later in Q4.",
                commentary_priority=3,
            )
        q4_2025_demand_rec = _pick_record(date(2025, 12, 31), ("solid domestic blending and strong export demand",))
        if q4_2025_demand_rec:
            _append_item(
                source_quarter=date(2025, 12, 31),
                horizon_label="",
                horizon_norm="Q2025Q4",
                setup_type="management_commentary",
                setup_display="Demand support",
                source_type=str(q4_2025_demand_rec.get("source_type") or ""),
                source_doc=str(q4_2025_demand_rec.get("source_doc") or ""),
                source_date=date(2025, 12, 31),
                source_excerpt=_extract_excerpt(q4_2025_demand_rec.get("text"), ("solid domestic blending and strong export demand",)),
                confidence="medium",
                commentary_text="Solid domestic blending and strong export demand supported Q4 ethanol margins.",
                commentary_priority=2,
                show_in_setup=False,
            )
        q4_2025_corn_oil_rec = _pick_record(date(2025, 12, 31), ("corn oil markets remained steady", "contribute nicely to our gross margin"))
        if q4_2025_corn_oil_rec:
            _append_item(
                source_quarter=date(2025, 12, 31),
                horizon_label="",
                horizon_norm="Q2025Q4",
                setup_type="management_commentary",
                setup_display="Corn-oil support",
                source_type=str(q4_2025_corn_oil_rec.get("source_type") or ""),
                source_doc=str(q4_2025_corn_oil_rec.get("source_doc") or ""),
                source_date=date(2025, 12, 31),
                source_excerpt=_extract_excerpt(q4_2025_corn_oil_rec.get("text"), ("corn oil markets remained steady", "contribute nicely to our gross margin")),
                confidence="medium",
                commentary_text="Corn-oil values contributed positively to gross margin during the quarter.",
                commentary_priority=3,
                show_in_setup=False,
            )
        q4_2025_protein_rec = _pick_record(date(2025, 12, 31), ("protein pricing continued to be under pressure",))
        if q4_2025_protein_rec:
            _append_item(
                source_quarter=date(2025, 12, 31),
                horizon_label="",
                horizon_norm="Q2025Q4",
                setup_type="management_commentary",
                setup_display="Protein pressure",
                source_type=str(q4_2025_protein_rec.get("source_type") or ""),
                source_doc=str(q4_2025_protein_rec.get("source_doc") or ""),
                source_date=date(2025, 12, 31),
                source_excerpt=_extract_excerpt(q4_2025_protein_rec.get("text"), ("protein pricing continued to be under pressure",)),
                confidence="medium",
                commentary_text="Protein pricing remained under pressure in Q4.",
                commentary_priority=4,
                show_in_setup=False,
            )
        q4_2025_margin_rec = _pick_record(date(2025, 12, 31), ("simple crush margins", "holding up relatively well"))
        if q4_2025_margin_rec:
            _append_item(
                source_quarter=date(2025, 12, 31),
                horizon_label="Q1 2026",
                horizon_norm="Q2026Q1",
                setup_type="management_commentary",
                setup_display="Simple crush support",
                source_type=str(q4_2025_margin_rec.get("source_type") or ""),
                source_doc=str(q4_2025_margin_rec.get("source_doc") or ""),
                source_date=date(2025, 12, 31),
                source_excerpt=_extract_excerpt(q4_2025_margin_rec.get("text"), ("simple crush margins", "holding up relatively well", "corn continues to be")),
                confidence="medium",
                commentary_text="Management said simple crush margins were holding up relatively well as low corn costs supported the setup heading into 2026.",
                commentary_priority=4,
                show_in_setup=False,
            )
        q2_2025_bridge_rec = _pick_record(date(2025, 6, 30), ("one-time sale of accumulated rins", "inventory lower of cost or net realizable value adjustment"))
        if q2_2025_bridge_rec:
            _append_item(
                source_quarter=date(2025, 6, 30),
                horizon_label="",
                horizon_norm="Q2025Q2",
                setup_type="management_commentary",
                setup_display="Reported bridge items",
                source_type=str(q2_2025_bridge_rec.get("source_type") or ""),
                source_doc=str(q2_2025_bridge_rec.get("source_doc") or ""),
                source_date=date(2025, 8, 11),
                source_excerpt=_extract_excerpt(q2_2025_bridge_rec.get("text"), ("accumulated rins", "inventory lower of cost", "net realizable value adjustment")),
                confidence="high",
                commentary_text="Reported ethanol-production margin included a $22.6m accumulated RIN sale and a $2.3m inventory NRV adjustment.",
                commentary_priority=1,
                show_in_setup=False,
            )
        q2_2024_protein_rec = _pick_record(date(2024, 6, 30), ("corn oil pricing", "ultra-high protein demand leading to profitable outlook"))
        if q2_2024_protein_rec:
            _append_item(
                source_quarter=date(2024, 6, 30),
                horizon_label="Q3 2024",
                horizon_norm="Q2024Q3",
                setup_type="management_commentary",
                setup_display="Coproduct outlook",
                source_type=str(q2_2024_protein_rec.get("source_type") or ""),
                source_doc=str(q2_2024_protein_rec.get("source_doc") or ""),
                source_date=date(2024, 8, 6),
                source_excerpt=_extract_excerpt(q2_2024_protein_rec.get("text"), ("corn oil pricing", "ultra-high protein demand", "profitable outlook")),
                confidence="high",
                commentary_text="Corn-oil pricing and Ultra-high protein demand pointed to a profitable Q3 outlook.",
                commentary_priority=4,
                show_in_setup=False,
            )
        q2_2024_yield_rec = _pick_record(date(2024, 6, 30), ("record renewable corn oil yields", "record ultra-high protein platform yields"))
        if q2_2024_yield_rec:
            _append_item(
                source_quarter=date(2024, 6, 30),
                horizon_label="",
                horizon_norm="Q2024Q2",
                setup_type="management_commentary",
                setup_display="Yield improvement",
                source_type=str(q2_2024_yield_rec.get("source_type") or ""),
                source_doc=str(q2_2024_yield_rec.get("source_doc") or ""),
                source_date=date(2024, 8, 6),
                source_excerpt=_extract_excerpt(q2_2024_yield_rec.get("text"), ("record renewable corn oil yields", "record ultra-high protein platform yields")),
                confidence="high",
                commentary_text="Record renewable corn-oil yields and record Ultra-high protein platform yields improved coproduct economics.",
                commentary_priority=5,
                show_in_setup=False,
            )
        q3_2024_yield_rec = _pick_record(date(2024, 9, 30), ("record high ethanol and ultra-high protein yields", "record protein production"))
        if q3_2024_yield_rec:
            _append_item(
                source_quarter=date(2024, 9, 30),
                horizon_label="",
                horizon_norm="Q2024Q3",
                setup_type="management_commentary",
                setup_display="Yield and output",
                source_type=str(q3_2024_yield_rec.get("source_type") or ""),
                source_doc=str(q3_2024_yield_rec.get("source_doc") or ""),
                source_date=date(2024, 10, 31),
                source_excerpt=_extract_excerpt(q3_2024_yield_rec.get("text"), ("record high ethanol and ultra-high protein yields", "record protein production", "record renewable corn oil production")),
                confidence="high",
                commentary_text="Record high ethanol and Ultra-high protein yields supported record protein output and corn-oil production.",
                commentary_priority=5,
                show_in_setup=False,
                commentary_home="operating_commentary",
            )

        conf_path = Path(__file__).resolve().parents[2] / "sec_cache" / "GPRE" / "external" / "conferences" / "2026-02-26_bofa" / "structured_statements.json"
        if conf_path.exists():
            try:
                conf_rows = json.loads(conf_path.read_text(encoding="utf-8"))
            except Exception:
                conf_rows = []
            if isinstance(conf_rows, list):
                for row in conf_rows:
                    if not isinstance(row, dict):
                        continue
                    fam = str(row.get("promise_family") or "")
                    subtopic = str(row.get("subtopic") or "")
                    topic = str(row.get("topic") or "")
                    if fam == "Commercial / margin setup" or (topic == "Ethanol margins" and subtopic == "Current quarter setup"):
                        _append_item(
                            source_quarter=date(2026, 3, 31),
                            horizon_label="Q1 2026",
                            horizon_norm="Q2026Q1",
                            setup_type="commercial_positioning",
                            setup_display="Current margin setup",
                            source_type="conference",
                            source_doc="external/conferences/2026-02-26_bofa/transcript.md",
                            source_date=date(2026, 2, 26),
                            source_excerpt=_ensure_terminal_period(glx_normalize_text(str(row.get("source_excerpt") or row.get("text") or ""))),
                            locked_margin_text="Q1 consolidated crush margins were better year over year",
                            setup_quality="constructive",
                            legs_involved="Consolidated crush, exports and DCO",
                            management_takeaway="Management linked the stronger setup to corn supply, domestic ethanol markets and DCO values",
                            guidance_metric="Commercial positioning",
                            guidance_text="Q1 consolidated crush margins were better year over year.",
                            commentary_text="Management said the Q1 position was helped by stronger domestic ethanol markets, limited inventory build and stronger DCO values.",
                            commentary_priority=5,
                            future_latest_only=True,
                            show_in_guidance=False,
                        )
                        break
                else:
                    for row in conf_rows:
                        if not isinstance(row, dict):
                            continue
                        txt = glx_normalize_text(str(row.get("text") or ""))
                        if "q1 consolidated crush margins were better year over year" not in txt.lower():
                            continue
                        _append_item(
                            source_quarter=date(2026, 3, 31),
                            horizon_label="Q1 2026",
                            horizon_norm="Q2026Q1",
                            setup_type="commercial_positioning",
                            setup_display="Current margin setup",
                            source_type="conference",
                            source_doc="external/conferences/2026-02-26_bofa/transcript.md",
                            source_date=date(2026, 2, 26),
                            source_excerpt=_ensure_terminal_period(glx_normalize_text(str(row.get("source_excerpt") or row.get("text") or ""))),
                            locked_margin_text="Q1 consolidated crush margins were better year over year",
                            setup_quality="constructive",
                            legs_involved="Consolidated crush, exports and DCO",
                            management_takeaway="Management linked the stronger setup to corn supply, domestic ethanol markets and DCO values",
                            guidance_metric="Commercial positioning",
                            guidance_text="Q1 consolidated crush margins were better year over year.",
                            commentary_text="Management said the Q1 position was helped by stronger domestic ethanol markets, limited inventory build and stronger DCO values.",
                            commentary_priority=5,
                            future_latest_only=True,
                            show_in_guidance=False,
                        )
                        break

        if not any(str(rec.get("setup_display") or "") == "Current margin setup" for rec in out_items):
            local_bofa_text = _gpre_local_bofa_conference_text_shared()
            local_bofa_path = _gpre_local_bofa_conference_path_shared()
            if local_bofa_text and re.search(r"\bpleased with our position in q1\b", local_bofa_text, re.I):
                _append_item(
                    source_quarter=date(2026, 3, 31),
                    horizon_label="Q1 2026",
                    horizon_norm="Q2026Q1",
                    setup_type="commercial_positioning",
                    setup_display="Current margin setup",
                    source_type="conference",
                    source_doc=str(local_bofa_path),
                    source_date=date(2026, 2, 26),
                    source_excerpt=_gpre_local_bofa_conference_excerpt_shared(
                        (
                            "pleased with our position in q1",
                            "stronger domestic ethanol markets",
                            "dco values have strengthened",
                        )
                    ),
                    locked_margin_text="Q1 consolidated crush margins were better year over year",
                    setup_quality="constructive",
                    legs_involved="Consolidated crush, exports and DCO",
                    management_takeaway="Management linked the stronger setup to corn supply, domestic ethanol markets and DCO values",
                    guidance_metric="Commercial positioning",
                    guidance_text="Q1 consolidated crush margins were better year over year.",
                    commentary_text="Management said the Q1 position was helped by stronger domestic ethanol markets, limited inventory build and stronger DCO values.",
                    commentary_priority=5,
                    future_latest_only=True,
                    show_in_guidance=False,
                )

        q1_2026_q2_setup_rec = _pick_record(date(2026, 3, 31), ("fairly well hedged for q2", "stronger result than in q1"))
        if q1_2026_q2_setup_rec:
            _append_item(
                source_quarter=date(2026, 3, 31),
                horizon_label="Q2 2026",
                horizon_norm="Q2026Q2",
                setup_type="commercial_positioning",
                setup_display="Q2 setup",
                source_type=str(q1_2026_q2_setup_rec.get("source_type") or "transcript"),
                source_doc=str(q1_2026_q2_setup_rec.get("source_doc") or ""),
                source_date=date(2026, 5, 8),
                source_excerpt=_extract_excerpt(
                    q1_2026_q2_setup_rec.get("text"),
                    ("fairly well hedged for q2", "stronger result than in q1", "input costs"),
                ),
                coverage_text="Management expects Q2 stronger than Q1 and fairly well hedged, especially on input costs",
                setup_quality="constructive",
                legs_involved="Input costs and commercial margin setup",
                result_effect="Forward-looking Q2 setup commentary; not formal numerical guidance",
                management_takeaway="Management expects Q2 to be stronger than Q1 and said the company is fairly well hedged for Q2, especially on input costs.",
                confidence="high",
                guidance_metric="Q2 commercial setup",
                guidance_text="Management expects Q2 to be stronger than Q1 and said the company is fairly well hedged for Q2, especially on input costs.",
                guidance_score=88.0,
                commentary_text="Management expects Q2 to be stronger than Q1 and said the company is fairly well hedged for Q2, especially on input costs.",
                commentary_priority=0,
            )

        q1_2026_45z_bridge_rec = _pick_record(date(2026, 3, 31), ("56.1 million", "45z production tax credits"))
        if q1_2026_45z_bridge_rec:
            _append_item(
                source_quarter=date(2026, 3, 31),
                horizon_label="Q1 2026 / FY2026",
                horizon_norm="FY2026",
                setup_type="management_commentary",
                setup_display="45Z bridge",
                source_type=str(q1_2026_45z_bridge_rec.get("source_type") or "earnings_release"),
                source_doc=str(q1_2026_45z_bridge_rec.get("source_doc") or ""),
                source_date=date(2026, 5, 8),
                source_excerpt=_extract_excerpt(
                    q1_2026_45z_bridge_rec.get("text"),
                    ("56.1 million", "45z production tax credits", "cost of goods sold"),
                ),
                confidence="high",
                commentary_text="Q1 reported crush was materially affected by 45Z COGS reduction; ex-45Z crush and base-business Adj EBITDA are separate bridges.",
                commentary_priority=1,
                show_in_guidance=False,
                show_in_setup=False,
            )

        q1_2026_45z_guidance_rec = _pick_record(date(2026, 3, 31), ("200 million to 225 million", "advantage nebraska"))
        if q1_2026_45z_guidance_rec:
            _append_item(
                source_quarter=date(2026, 3, 31),
                horizon_label="FY2026",
                horizon_norm="FY2026",
                setup_type="management_commentary",
                setup_display="45Z FY2026 guidance",
                source_type=str(q1_2026_45z_guidance_rec.get("source_type") or "presentation"),
                source_doc=str(q1_2026_45z_guidance_rec.get("source_doc") or ""),
                source_date=date(2026, 5, 8),
                source_excerpt=_extract_excerpt(
                    q1_2026_45z_guidance_rec.get("text"),
                    ("200 million to 225 million", "advantage nebraska", "remaining facilities"),
                ),
                confidence="high",
                commentary_text="FY2026 45Z EBITDA guidance is $200m-$225m, led by Advantage Nebraska $140m-$165m and remaining facilities about $60m.",
                commentary_priority=2,
                show_in_guidance=False,
                show_in_setup=False,
            )

        out_items.sort(
            key=lambda z: (
                pd.to_datetime(z.get("source_quarter"), errors="coerce"),
                str(z.get("horizon_period_norm") or ""),
                str(z.get("setup_display") or ""),
            )
        )
        self._commercial_setup_cache = [dict(x) for x in out_items]
        return [dict(x) for x in out_items]
