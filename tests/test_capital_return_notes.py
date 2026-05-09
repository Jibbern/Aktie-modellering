from __future__ import annotations

from pbi_xbrl.capital_return_notes import (
    build_buyback_note,
    build_dividend_note,
    build_dividend_note_from_text,
    normalize_capital_return_note_item,
    normalize_new_prefix,
    normalize_quarter_note_items,
)


def test_build_dividend_note_standardizes_common_actions() -> None:
    assert (
        build_dividend_note(current_per_share=0.10, previous_per_share=0.09)
        == "[NEW] Quarterly dividend increased to $0.10/share from $0.09/share."
    )
    assert build_dividend_note(current_per_share=0.09, previous_per_share=0.09) == "Quarterly dividend set at $0.09/share."
    assert (
        build_dividend_note(current_per_share=0.08, previous_per_share=0.09)
        == "[NEW] Quarterly dividend reduced to $0.08/share from $0.09/share."
    )
    assert build_dividend_note(action="suspended") == "[NEW] Quarterly dividend suspended."
    assert build_dividend_note() == ""


def test_build_dividend_note_from_text_handles_increase_amount_and_set_cases() -> None:
    pbi_style = (
        "The Board approved a $0.01 per share increase in the quarterly common stock dividend. "
        "The next quarterly dividend is $0.10 per share. Results also include discontinued operations."
    )
    assert (
        build_dividend_note_from_text(pbi_style)
        == "[NEW] Quarterly dividend increased to $0.10/share from $0.09/share."
    )
    assert (
        build_dividend_note_from_text("The Board declared a regular quarterly dividend of $0.09 per share.")
        == "Quarterly dividend set at $0.09/share."
    )
    assert (
        build_dividend_note_from_text("The Board reduced the quarterly dividend from $0.09 down to $0.07 per share.")
        == "[NEW] Quarterly dividend reduced to $0.07/share from $0.09/share."
    )
    assert build_dividend_note_from_text("The Board suspended the quarterly dividend.") == "[NEW] Quarterly dividend suspended."
    assert build_dividend_note_from_text("No capital-return policy was disclosed.") == ""


def test_normalize_new_prefix_collapses_duplicate_badges() -> None:
    body = "Quarterly dividend increased to $0.09/share from $0.08/share."
    assert normalize_new_prefix(body, add=True) == f"[NEW] {body}"
    assert normalize_new_prefix(f"[NEW] {body}", add=True) == f"[NEW] {body}"
    assert normalize_new_prefix(f"[NEW] [NEW]   {body}") == f"[NEW] {body}"
    assert normalize_new_prefix(f"  [new]   [NEW] {body}  ") == f"[NEW] {body}"


def test_build_buyback_note_keeps_quarter_and_post_quarter_context_separate() -> None:
    assert (
        build_buyback_note(shares=12_900_000, cash=135_600_000, average_price=10.51)
        == "Buybacks: 12.9m shares repurchased for $135.6m during the quarter at $10.51/share."
    )
    assert (
        build_buyback_note(shares=4_300_000, cash=50_000_000, post_quarter=True, through_date="May 1, 2026")
        == "Additional 4.3m shares repurchased for $50.0m after quarter-end through May 1, 2026."
    )


def test_normalize_capital_return_note_item_keeps_dividends_out_of_buyback_metric() -> None:
    dividend_row = normalize_capital_return_note_item(
        {
            "quarter": "2025-09-30",
            "category": "Capital allocation / shareholder returns",
            "metric_ref": "Capital allocation / buyback",
            "_metric_display": "Capital allocation / buyback",
            "note": "[NEW] [NEW] Quarterly dividend increased to $0.09/share from $0.08/share.",
        }
    )
    assert dividend_row["note"] == "[NEW] Quarterly dividend increased to $0.09/share from $0.08/share."
    assert dividend_row["category"] == "Capital allocation / shareholder returns"
    assert dividend_row["metric_ref"] == "Dividend policy"
    assert dividend_row["_metric_display"] == "Dividend policy"

    buyback_row = normalize_capital_return_note_item(
        {
            "category": "Capital allocation / shareholder returns",
            "metric_ref": "Capital allocation / buyback",
            "_metric_display": "Capital allocation / buyback",
            "note": "Repurchased 12.9m shares for $135.6m during Q1.",
        }
    )
    assert buyback_row["metric_ref"] == "Capital allocation / buyback"
    assert buyback_row["_metric_display"] == "Capital allocation / buyback"


def test_normalize_capital_return_note_item_uses_visible_note_before_long_evidence() -> None:
    row = normalize_capital_return_note_item(
        {
            "category": "Capital allocation / shareholder returns",
            "metric_ref": "Capital allocation / buyback",
            "_metric_display": "Capital allocation / buyback",
            "_render_summary": "[NEW] Remaining share repurchase capacity was $148.2m at quarter-end.",
            "evidence_snippet": (
                "As of September 30, capacity remained under the repurchase authorization. "
                "We also increased our quarterly dividend from $0.08 to $0.09 per share."
            ),
        }
    )
    assert row["_metric_display"] == "Capital allocation / buyback"
    assert row["metric_ref"] == "Capital allocation / buyback"


def test_normalize_capital_return_note_item_distinguishes_dividend_policy_from_cash() -> None:
    policy_row = normalize_capital_return_note_item(
        {
            "category": "Capital allocation / shareholder returns",
            "metric_ref": "Dividend cash",
            "_metric_display": "Dividend cash",
            "note": "[NEW] Quarterly dividend increased to $0.10/share from $0.09/share.",
        }
    )
    assert policy_row["metric_ref"] == "Dividend policy"
    assert policy_row["_metric_display"] == "Dividend policy"

    set_row = normalize_capital_return_note_item(
        {
            "metric_ref": "Dividend cash",
            "_metric_display": "Dividend cash",
            "note": "Quarterly dividend set at $0.09/share.",
        }
    )
    assert set_row["metric_ref"] == "Dividend policy"
    assert set_row["_metric_display"] == "Dividend policy"

    cash_row = normalize_capital_return_note_item(
        {
            "metric_ref": "Dividend policy",
            "_metric_display": "Dividend policy",
            "note": "Q1 cash dividends paid $13.3m / $0.09 per share | TTM cash dividends $53.4m",
        }
    )
    assert cash_row["metric_ref"] == "Dividend cash"
    assert cash_row["_metric_display"] == "Dividend cash"


def test_normalize_quarter_note_items_dedupes_and_splits_long_combo_notes() -> None:
    notes = normalize_quarter_note_items(
        [
            {
                "quarter": "2026-03-31",
                "category": "Operating drivers",
                "note": (
                    "Revenue inflection watch: SendTech bookings/subscribers improved and Presort wins outpaced "
                    "losses; management expects Presort YoY volume to turn positive by early Q3 if trends hold."
                ),
            },
            {
                "quarter": "2026-03-31",
                "category": "Operating drivers",
                "note": "SendTech bookings/subscribers improved.",
            },
        ],
        max_note_chars=110,
    )
    note_texts = [row["note"] for row in notes]
    assert note_texts == [
        "SendTech bookings/subscribers improved.",
        "Presort wins outpaced losses; management expects Presort YoY volume to turn positive by early Q3 if trends hold.",
    ]


def test_normalize_quarter_note_items_normalizes_capital_return_rows() -> None:
    notes = normalize_quarter_note_items(
        [
            {
                "quarter": "2025-09-30",
                "category": "Capital allocation / shareholder returns",
                "metric_ref": "Capital allocation / buyback",
                "_metric_display": "Capital allocation / buyback",
                "note": "[NEW] [NEW] Quarterly dividend increased to $0.09/share from $0.08/share.",
            },
            {
                "quarter": "2025-09-30",
                "category": "Capital allocation / shareholder returns",
                "metric_ref": "Capital allocation / buyback",
                "_metric_display": "Capital allocation / buyback",
                "note": "Repurchased 14.1m shares for $161.5m during the quarter.",
            },
        ],
    )
    assert notes[0]["note"] == "[NEW] Quarterly dividend increased to $0.09/share from $0.08/share."
    assert notes[0]["metric_ref"] == "Dividend policy"
    assert notes[0]["_metric_display"] == "Dividend policy"
    assert notes[1]["metric_ref"] == "Capital allocation / buyback"
