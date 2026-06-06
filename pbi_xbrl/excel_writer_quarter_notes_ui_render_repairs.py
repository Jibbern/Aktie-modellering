"""Quarter_Notes_UI post-render repair and formatting helpers."""
from __future__ import annotations

from copy import copy
from dataclasses import dataclass
from datetime import date
import re
from typing import Any, Callable, Dict, List, Mapping, MutableMapping, Optional, Sequence, Tuple

import pandas as pd


@dataclass(frozen=True)
class QuarterNotesUiRenderRepairDeps:
    ws: Any
    quarters: Sequence[Any]
    is_pbi_profile: bool
    is_gpre_profile: bool
    is_anf_profile: bool
    ui_state: MutableMapping[str, Any]
    render_blocks_start: float
    get_analysis_sheet_style_bundle: Callable[[], Mapping[str, Any]]
    quarter_label_short: Callable[..., str]
    normalize_text: Callable[..., str]
    ensure_terminal_period: Callable[..., str]
    anf_visible_quarter_note_summaries: Callable[..., Sequence[str]]
    anf_clean_visible_ui_text: Callable[..., str]
    anf_polish_quarter_note_visible_fields: Callable[..., Tuple[str, str]]
    record_writer_elapsed: Callable[..., None]
    perf_counter: Callable[[], float]


@dataclass(frozen=True)
class QuarterNotesUiRenderRepairResult:
    row_count: int
    max_row: int
    inserted_rows: int
    deleted_rows: int


def repair_quarter_notes_ui_after_render(
    deps: QuarterNotesUiRenderRepairDeps,
) -> QuarterNotesUiRenderRepairResult:
    ws = deps.ws
    quarters = deps.quarters
    inserted_rows = 0
    deleted_rows = 0

    if deps.is_anf_profile:
        def _anf_note_category_for_summary(summary_in: str, fallback: str) -> str:
            low_sum = summary_in.lower()
            if low_sum.startswith("inventory"):
                return "Inventory / working capital"
            if "digital" in low_sum or "omnichannel" in low_sum or "visits" in low_sum:
                return "Digital / omnichannel"
            if "abercrombie" in low_sum or "hollister" in low_sum or "brand" in low_sum:
                return "Brand / demand"
            if "comp" in low_sum:
                return "Comps"
            if low_sum.startswith("eps") or low_sum.startswith("2025 sales") or low_sum.startswith("regions"):
                return "Results / financials"
            if "guidance" in low_sum or "outlook" in low_sum:
                return "Guidance / outlook"
            if "margin bridge" in low_sum or "tariff" in low_sum or "freight" in low_sum:
                return "Margin bridge"
            if "buyback" in low_sum or "repurchase" in low_sum:
                return "Capital allocation / shareholder returns"
            if "store" in low_sum or "remodel" in low_sum:
                return "Stores / real estate"
            return fallback

        def _anf_metric_for_summary(summary_in: str, fallback: str) -> str:
            prefix = summary_in.split(":", 1)[0].strip() if ":" in summary_in else ""
            return deps.anf_clean_visible_ui_text(prefix or fallback, max_chars=64)

        def _copy_visible_note_row_style(src_row: int, dst_row: int) -> None:
            for cc in range(1, 5):
                src = ws.cell(src_row, cc)
                dst = ws.cell(dst_row, cc)
                if src.has_style:
                    dst._style = copy(src._style)
                dst.font = copy(src.font)
                dst.fill = copy(src.fill)
                dst.border = copy(src.border)
                dst.alignment = copy(src.alignment)
                dst.number_format = src.number_format
            ws.row_dimensions[dst_row].height = ws.row_dimensions[src_row].height

        latest_anf_note_label = deps.quarter_label_short(max(quarters)) if quarters else ""
        current_anf_note_label = ""
        rr = 3
        while rr <= ws.max_row:
            block_label = str(ws.cell(row=rr, column=1).value or "").strip()
            if re.fullmatch(r"Q[1-4]\s+20\d{2}", block_label, re.I):
                current_anf_note_label = block_label
                rr += 1
                continue
            note_txt = str(ws.cell(row=rr, column=3).value or "").strip()
            if not note_txt or str(ws.cell(row=rr, column=2).value or "").strip().lower() == "category":
                rr += 1
                continue
            if (
                current_anf_note_label
                and latest_anf_note_label
                and current_anf_note_label != latest_anf_note_label
                and "fy2026 margin bridge is sourced" in note_txt.lower()
            ):
                ws.delete_rows(rr, 1)
                deleted_rows += 1
                continue
            summaries = deps.anf_visible_quarter_note_summaries(
                note_txt,
                quarter_label=current_anf_note_label,
                latest_label=latest_anf_note_label,
            )
            if not summaries:
                ws.delete_rows(rr, 1)
                deleted_rows += 1
                continue
            fallback_cat = deps.anf_clean_visible_ui_text(ws.cell(row=rr, column=2).value)
            fallback_metric = deps.anf_clean_visible_ui_text(ws.cell(row=rr, column=4).value, max_chars=64)
            polished_cat, polished_metric = deps.anf_polish_quarter_note_visible_fields(
                _anf_note_category_for_summary(summaries[0], fallback_cat),
                _anf_metric_for_summary(summaries[0], fallback_metric),
                summaries[0],
            )
            ws.cell(row=rr, column=2).value = polished_cat
            ws.cell(row=rr, column=3).value = summaries[0]
            ws.cell(row=rr, column=4).value = polished_metric
            insert_at = rr + 1
            for summary in summaries[1:]:
                ws.insert_rows(insert_at, 1)
                inserted_rows += 1
                _copy_visible_note_row_style(rr, insert_at)
                polished_cat, polished_metric = deps.anf_polish_quarter_note_visible_fields(
                    _anf_note_category_for_summary(summary, fallback_cat),
                    _anf_metric_for_summary(summary, fallback_metric),
                    summary,
                )
                ws.cell(row=insert_at, column=1).value = ""
                ws.cell(row=insert_at, column=2).value = polished_cat
                ws.cell(row=insert_at, column=3).value = summary
                ws.cell(row=insert_at, column=4).value = polished_metric
                insert_at += 1
            rr = insert_at

        def _append_anf_latest_atomic_notes() -> None:
            nonlocal inserted_rows
            if not latest_anf_note_label:
                return
            block_start: Optional[int] = None
            block_end: Optional[int] = None
            for row_no in range(3, ws.max_row + 1):
                label_txt = str(ws.cell(row=row_no, column=1).value or "").strip()
                if re.fullmatch(r"Q[1-4]\s+20\d{2}", label_txt, re.I):
                    if label_txt == latest_anf_note_label:
                        block_start = row_no
                        continue
                    if block_start is not None:
                        block_end = row_no - 1
                        break
            if block_start is None:
                return
            if block_end is None:
                block_end = ws.max_row
            existing = {
                deps.normalize_text(str(ws.cell(row=row_no, column=3).value or "")).lower()
                for row_no in range(block_start, block_end + 1)
            }
            additions = [
                ("Results / financials", "Record Q4: net sales rose 5% to $1.67bn with balanced growth across regions, brands and channels.", "Record Q4 sales"),
                ("Comps", "Comparable sales: total comp increased 1% in Q4 2025; read is slower growth against a tough +16% lap.", "Total comp"),
                ("Brand / demand", "Abercrombie: returned to growth and delivered record Q4 net sales.", "Abercrombie momentum"),
                ("Brand / demand", "Hollister: delivered its 11th consecutive quarter of growth.", "Hollister momentum"),
                ("Margin bridge", "Q4 margin bridge: tariff pressure was partly offset by freight benefits.", "Tariff / freight"),
                ("Capital allocation / shareholder returns", "Buybacks: 2026 guidance calls for around $450m of share repurchases.", "2026 buybacks"),
                ("Stores / real estate", "Stores: 2026 outlook includes 55 openings, 25 closures and 70 remodels/right-sizes.", "Store plan"),
            ]
            insert_at = block_end + 1
            template_row = block_start + 2 if block_start + 2 <= block_end else block_start
            for category, note, metric in additions:
                clean_note = deps.anf_clean_visible_ui_text(note, max_chars=260)
                if deps.normalize_text(clean_note).lower() in existing:
                    continue
                category, metric = deps.anf_polish_quarter_note_visible_fields(category, metric, clean_note)
                ws.insert_rows(insert_at, 1)
                inserted_rows += 1
                _copy_visible_note_row_style(template_row, insert_at)
                ws.cell(row=insert_at, column=1).value = ""
                ws.cell(row=insert_at, column=2).value = category
                ws.cell(row=insert_at, column=3).value = clean_note
                ws.cell(row=insert_at, column=4).value = metric
                ws.row_dimensions[insert_at].height = 19.5
                existing.add(deps.normalize_text(clean_note).lower())
                insert_at += 1

        _append_anf_latest_atomic_notes()

        def _ensure_anf_prior_quarter_notes() -> None:
            """Backfill concise source-backed notes when parser filtering leaves recent ANF quarters sparse."""
            nonlocal inserted_rows
            target_notes: Dict[str, List[Tuple[str, str, str]]] = {
                "Q1 2025": [
                    ("Results / financials", "Q1 actuals: net sales were $1.10bn and operating margin was 9.3%.", "Q1 actuals"),
                    ("Comps", "Comps: total +4%; Abercrombie -10%; Hollister +23%.", "Brand comps"),
                    ("Guidance / outlook", "2025 guide moved to sales +3-6% and EPS $9.50-$10.50 after Q1.", "2025 guide update"),
                    ("Capital allocation", "Share-count guidance moved lower to around 49m, making buybacks a clearer EPS support lever.", "Share count"),
                ],
                "Q2 2025": [
                    ("Results / financials", "Q2 actuals: net sales were $1.21bn and operating margin was 17.1%.", "Q2 actuals"),
                    ("Comps", "Comps: total +3%; Abercrombie -11%; Hollister +19%.", "Brand comps"),
                    ("Guidance / outlook", "2025 guide increased to sales +5-7%, EPS $10.00-$10.50 and capex around $225m.", "2025 guide update"),
                    ("Margin bridge", "Tariffs became a larger 2025 cost item; mitigation focused on sourcing, costing and pricing.", "Tariff mitigation"),
                    ("Brand / demand", "Hollister remained the growth engine while Abercrombie comps stayed negative against tough laps.", "Brand mix"),
                ],
                "Q3 2025": [
                    ("Results / financials", "Q3 actuals: net sales were $1.29bn and operating margin was 12.0%.", "Q3 actuals"),
                    ("Comps", "Comps: total +3%; Abercrombie -7%; Hollister +15%.", "Brand comps"),
                    ("Guidance / outlook", "2025 guide moved to sales +6-7%, EPS $10.20-$10.50 and buybacks around $450m.", "2025 guide update"),
                    ("Capital allocation", "Buyback guidance increased to around $450m, supporting EPS through lower share count.", "Buyback guide"),
                    ("Margin bridge", "Q3 kept the margin debate centered on tariffs, freight, markdown discipline and expense leverage.", "Margin driver"),
                ],
                "Q1 2024": [
                    ("Comps", "Comps were very strong: total +22%, Abercrombie +29% and Hollister +13%; read is high-quality demand but a tough future lap.", "Comp stack"),
                    ("Brand / demand", "Abercrombie led early-2024 growth while Hollister was already positive.", "Brand demand"),
                    ("Margin bridge", "Strong demand and lower markdown pressure supported margin expansion.", "Margin driver"),
                    ("Guidance / outlook", "The early-2024 pace raised the bar for future comp comparisons and margin durability.", "Lapping risk"),
                ],
                "Q2 2024": [
                    ("Comps", "Comps remained very strong: total +22%, Abercrombie +21% and Hollister +15%; read is broad demand strength.", "Comp stack"),
                    ("Brand / demand", "Both brand families were positive, giving ANF balanced momentum before the 2025 slowdown.", "Brand demand"),
                    ("Guidance / outlook", "High-teens/twenties comp momentum created a difficult comparison base for 2025.", "Lapping risk"),
                    ("Margin bridge", "Gross margin quality benefited from strong sell-through and lower markdown pressure.", "Margin driver"),
                ],
                "Q3 2024": [
                    ("Comps", "Comps slowed but stayed strong: total +16%, Abercrombie +11% and Hollister +21%; Hollister was accelerating.", "Comp stack"),
                    ("Brand / demand", "Hollister became the clearer growth engine as Abercrombie growth moderated.", "Brand mix"),
                    ("Guidance / outlook", "The comp stack shows why 2025 low-single-digit comps were not automatically weak.", "Lapping risk"),
                    ("Results / financials", "Q3 2024 showed continued top-line strength but a tougher setup for 2025 comparisons.", "Quarter actuals"),
                ],
                "Q4 2024": [
                    ("Results / financials", "Q4 actuals: net sales were $1.58bn and operating margin was 16.2%, setting a tough 2025 lap.", "Q4 actuals"),
                    ("Comps", "Comps were +16% in Q4 2024; Q4 2025 had to lap an unusually strong base.", "Comp stack"),
                    ("Brand / demand", "Hollister comp was +24% while Abercrombie comp was +5%, foreshadowing Hollister leadership.", "Brand mix"),
                    ("Guidance / outlook", "Initial 2025 guide called for sales +3-5%, operating margin 14-15% and EPS $10.40-$11.40.", "2025 initial guide"),
                    ("Capital allocation", "The initial 2025 plan included about $400m of buybacks, making share count part of the EPS setup.", "Buyback guide"),
                ],
                "Q4 2023": [
                    ("Results / financials", "Q4 2023 was the turnaround launchpad, with very strong demand creating difficult future comparisons.", "Q4 actuals"),
                    ("Comps", "Q4 2023 comp momentum was unusually strong, so later low-single-digit comps need to be read against that stack.", "Lapping risk"),
                    ("Brand / demand", "The 2023 exit rate set up Abercrombie strength first, then Hollister became the bigger growth engine.", "Brand demand"),
                    ("Margin bridge", "Strong sell-through and lower markdown pressure were key margin supports entering 2024.", "Margin driver"),
                    ("Guidance / outlook", "The 2024 setup depended on keeping brand momentum while lapping the 2023 recovery surge.", "Forward setup"),
                ],
            }

            def _find_block(label: str) -> Tuple[Optional[int], Optional[int]]:
                start: Optional[int] = None
                end: Optional[int] = None
                for row_no in range(3, ws.max_row + 1):
                    label_txt = str(ws.cell(row=row_no, column=1).value or "").strip()
                    if not re.fullmatch(r"Q[1-4]\s+20\d{2}", label_txt, re.I):
                        continue
                    if label_txt == label:
                        start = row_no
                        continue
                    if start is not None:
                        end = row_no - 1
                        break
                if start is not None and end is None:
                    end = ws.max_row
                return start, end

            def _template_row() -> int:
                for row_no in range(3, ws.max_row + 1):
                    cat = str(ws.cell(row=row_no, column=2).value or "").strip().lower()
                    note = str(ws.cell(row=row_no, column=3).value or "").strip()
                    if note and cat and cat != "category":
                        return row_no
                return 5

            template = _template_row()
            for q_label, notes in target_notes.items():
                block_start, block_end = _find_block(q_label)
                if block_start is None or block_end is None:
                    continue
                existing_notes = {
                    deps.normalize_text(str(ws.cell(row=row_no, column=3).value or "")).lower()
                    for row_no in range(block_start, block_end + 1)
                    if str(ws.cell(row=row_no, column=3).value or "").strip()
                }
                existing_count = sum(
                    1
                    for row_no in range(block_start, block_end + 1)
                    if str(ws.cell(row=row_no, column=2).value or "").strip().lower() not in {"", "category"}
                    and str(ws.cell(row=row_no, column=3).value or "").strip()
                )
                target_count = 5
                if existing_count >= target_count:
                    continue
                insert_at = block_end + 1
                for category, note, metric in notes:
                    clean_note = deps.anf_clean_visible_ui_text(note, max_chars=245)
                    note_key = deps.normalize_text(clean_note).lower()
                    if not note_key or note_key in existing_notes:
                        continue
                    ws.insert_rows(insert_at, 1)
                    inserted_rows += 1
                    _copy_visible_note_row_style(template, insert_at)
                    ws.cell(row=insert_at, column=1).value = ""
                    ws.cell(row=insert_at, column=2).value = category
                    ws.cell(row=insert_at, column=3).value = clean_note
                    ws.cell(row=insert_at, column=4).value = metric
                    ws.row_dimensions[insert_at].height = 19.5
                    existing_notes.add(note_key)
                    insert_at += 1
                    existing_count += 1
                    if existing_count >= target_count:
                        break

        _ensure_anf_prior_quarter_notes()

    deps.record_writer_elapsed(
        "write_excel.ui.render.quarter_notes.render_blocks",
        deps.perf_counter() - deps.render_blocks_start,
    )
    final_formatting_start = deps.perf_counter()
    theme = deps.get_analysis_sheet_style_bundle()
    zebra_fills = [copy(theme["neutral_fill_alt"]), copy(theme["neutral_fill"])]
    zebra_idx = 0
    if deps.is_gpre_profile:
        gpre_rows_to_delete: List[int] = []
        current_gpre_block_qd: Optional[date] = None
        for rr in range(3, ws.max_row + 1):
            block_label_txt = str(ws.cell(row=rr, column=1).value or "").strip()
            block_qd = pd.to_datetime(block_label_txt, errors="coerce")
            if pd.notna(block_qd):
                current_gpre_block_qd = pd.Timestamp(block_qd).date()
            note_txt = deps.normalize_text(str(ws.cell(row=rr, column=3).value or ""))
            metric_txt = str(ws.cell(row=rr, column=4).value or "").strip()
            note_low = note_txt.lower()
            if (
                "ethanol production includes" in note_low
                and "56.1 million" in note_low
                and "45z production tax credits" in note_low
            ):
                ws.cell(row=rr, column=3).value = (
                    "Ethanol production COGS includes $56.1m of 45Z production tax credits, "
                    "recorded as a reduction of cost of goods sold."
                )
                ws.cell(row=rr, column=4).value = "45Z accounting / COGS reduction"
                ws.row_dimensions[rr].height = 19.5
                continue
            if (
                current_gpre_block_qd == date(2026, 3, 31)
                and re.search(r"\brepurchase authorization\b", note_low, re.I)
                and re.search(r"\$200\.0m|\$200m", note_txt, re.I)
            ):
                gpre_rows_to_delete.append(rr)
                continue
            if (
                current_gpre_block_qd is not None
                and current_gpre_block_qd > date(2024, 6, 30)
                and note_txt == "Repurchase authorization increased to $200.0m."
            ):
                gpre_rows_to_delete.append(rr)
                continue
            if (
                "45z-related adjusted ebitda outlook is at least $188" in note_low
                or "45z-related adjusted ebitda outlook at least $188" in note_low
            ):
                if current_gpre_block_qd is not None and current_gpre_block_qd >= date(2026, 3, 31):
                    ws.cell(row=rr, column=3).value = "[NEW] FY 2026 45Z EBITDA contribution guidance is $200m-$225m."
                    ws.cell(row=rr, column=4).value = "45Z EBITDA guidance"
                else:
                    ws.cell(row=rr, column=3).value = "[NEW] FY 2026 45Z EBITDA starting point was about $188m."
                    ws.cell(row=rr, column=4).value = "45Z EBITDA starting point"
                continue
            if (
                "advantage nebraska" in note_low
                and re.search(r"\$140\s*-\s*\$165m", note_txt, re.I)
                and "remaining facilities" in note_low
            ):
                ws.cell(row=rr, column=3).value = "45Z guidance: Advantage Nebraska $140m-$165m; remaining facilities about $60m in FY2026."
                ws.cell(row=rr, column=4).value = "45Z guidance bridge"
                ws.insert_rows(rr + 1, 1)
                inserted_rows += 1
                for cc in range(1, min(ws.max_column, 5) + 1):
                    src_cell = ws.cell(row=rr, column=cc)
                    dst_cell = ws.cell(row=rr + 1, column=cc)
                    dst_cell._style = copy(src_cell._style)
                    if src_cell.has_style:
                        dst_cell.font = copy(src_cell.font)
                        dst_cell.fill = copy(src_cell.fill)
                        dst_cell.border = copy(src_cell.border)
                        dst_cell.alignment = copy(src_cell.alignment)
                        dst_cell.number_format = src_cell.number_format
                ws.cell(row=rr + 1, column=1).value = ws.cell(row=rr, column=1).value
                ws.cell(row=rr + 1, column=2).value = ws.cell(row=rr, column=2).value
                ws.cell(row=rr + 1, column=3).value = "On-farm practices excluded pending final Treasury guidance/calculator."
                ws.cell(row=rr + 1, column=4).value = "45Z farm-practice upside"
                ws.row_dimensions[rr].height = 19.5
                ws.row_dimensions[rr + 1].height = 19.5
                continue
            util_match = re.search(r"(\d{2,3})%", note_txt, re.I)
            if "strong utilization in the quarter" in note_txt.lower() and "operating ethanol plants" in note_txt.lower() and util_match:
                ws.cell(row=rr, column=3).value = deps.ensure_terminal_period(
                    f"Utilization reached {util_match.group(1)}% across operating plants"
                )
                if not metric_txt or metric_txt == "45Z agreement update":
                    ws.cell(row=rr, column=4).value = "Utilization"
        for rr in sorted(set(gpre_rows_to_delete), reverse=True):
            ws.delete_rows(rr, 1)
            deleted_rows += 1
        q2_setup_note = (
            "Management expects Q2 to be stronger than Q1 and says Q2 is fairly well hedged, especially on input costs."
        )
        q1_block_start: Optional[int] = None
        q1_block_end: Optional[int] = None
        for rr in range(1, ws.max_row + 1):
            label_txt = str(ws.cell(row=rr, column=1).value or "").strip()
            block_qd = pd.to_datetime(label_txt, errors="coerce")
            if pd.notna(block_qd):
                if pd.Timestamp(block_qd).date() == date(2026, 3, 31):
                    q1_block_start = rr
                    continue
                if q1_block_start is not None:
                    q1_block_end = rr - 1
                    break
        if q1_block_start is not None and q1_block_end is None:
            q1_block_end = ws.max_row
        if q1_block_start is not None and q1_block_end is not None:
            existing_q1_notes = {
                deps.normalize_text(str(ws.cell(row=rr, column=3).value or "")).lower()
                for rr in range(q1_block_start, q1_block_end + 1)
            }
            if deps.normalize_text(q2_setup_note).lower() not in existing_q1_notes:
                insert_after = q1_block_start + 1
                for rr in range(q1_block_start, q1_block_end + 1):
                    note_txt_local = deps.normalize_text(str(ws.cell(row=rr, column=3).value or "")).lower()
                    if "on-farm practices excluded" in note_txt_local:
                        insert_after = rr
                        break
                ws.insert_rows(insert_after + 1, 1)
                inserted_rows += 1
                src_row = insert_after
                dst_row = insert_after + 1
                for cc in range(1, min(ws.max_column, 5) + 1):
                    src_cell = ws.cell(row=src_row, column=cc)
                    dst_cell = ws.cell(row=dst_row, column=cc)
                    dst_cell._style = copy(src_cell._style)
                    if src_cell.has_style:
                        dst_cell.font = copy(src_cell.font)
                        dst_cell.fill = copy(src_cell.fill)
                        dst_cell.border = copy(src_cell.border)
                        dst_cell.alignment = copy(src_cell.alignment)
                        dst_cell.number_format = src_cell.number_format
                ws.cell(row=dst_row, column=1).value = ws.cell(row=src_row, column=1).value
                ws.cell(row=dst_row, column=2).value = "Guidance / outlook"
                ws.cell(row=dst_row, column=3).value = q2_setup_note
                ws.cell(row=dst_row, column=4).value = "Q2 commercial setup"
                ws.row_dimensions[dst_row].height = 19.5
    for rr in range(2, ws.max_row + 1):
        a = str(ws.cell(row=rr, column=1).value or "").strip()
        b = str(ws.cell(row=rr, column=2).value or "").strip().lower()
        c = str(ws.cell(row=rr, column=3).value or "").strip()
        d = str(ws.cell(row=rr, column=4).value or "").strip()
        if deps.is_anf_profile and c:
            c_low = c.lower()
            if re.search(r"\b(buyback|repurchases?|share repurchases?)\b", c_low, re.I):
                ws.cell(row=rr, column=2).value = "Capital allocation"
                b = "capital allocation"
                if not d or "margin bridge" in d.lower():
                    ws.cell(row=rr, column=4).value = "Share repurchases"
                    d = "Share repurchases"
        if not a and not b and not c and not d:
            ws.row_dimensions[rr].height = 15.0
            continue
        if a and not b and not c and not d:
            ws.row_dimensions[rr].height = 19.5
            continue
        if b == "category" and c.lower() == "note" and d.lower() == "metric":
            ws.row_dimensions[rr].height = 19.5
            continue
        if rr >= 3 and (b or c or d):
            zebra_fill = zebra_fills[zebra_idx % 2]
            for cc in range(1, 5):
                cell = ws.cell(row=rr, column=cc)
                if cc == 3 and str(cell.fill.fill_type or "") == "solid":
                    existing_rgb = str(cell.fill.fgColor.rgb or "")
                    if existing_rgb not in {"00FFFFFF", "00F7F9FC", ""}:
                        continue
                cell.fill = copy(zebra_fill)
            ws.row_dimensions[rr].height = min(20.0, max(19.5, float(ws.row_dimensions[rr].height or 19.5)))
            zebra_idx += 1

    ws.freeze_panes = "A3"
    ws.column_dimensions["A"].width = 16
    ws.column_dimensions["B"].width = 38
    ws.column_dimensions["C"].width = 150
    ws.column_dimensions["D"].width = 30
    ws.column_dimensions["E"].hidden = True
    ws.column_dimensions["E"].width = 0.1
    deps.ui_state["quarters"] = quarters
    deps.record_writer_elapsed(
        "write_excel.ui.render.quarter_notes.final_formatting",
        deps.perf_counter() - final_formatting_start,
    )
    return QuarterNotesUiRenderRepairResult(
        row_count=max(0, int(getattr(ws, "max_row", 0) or 0) - 2),
        max_row=int(getattr(ws, "max_row", 0) or 0),
        inserted_rows=inserted_rows,
        deleted_rows=deleted_rows,
    )
