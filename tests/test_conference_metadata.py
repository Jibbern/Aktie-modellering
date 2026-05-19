from __future__ import annotations

from pathlib import Path

from pbi_xbrl.conference_metadata import metadata_source_file, parse_metadata_key_values
from pbi_xbrl.pipeline_runtime import material_dirs_signature


def test_metadata_source_file_accepts_legacy_specific_source_keys() -> None:
    values = parse_metadata_key_values(
        "\n".join(
            [
                "[METADATA]",
                "source_txt_file = Stephens_Annual_Investment_Conference_2025.txt",
                "source_pdf_file = ignored_when_txt_is_present.pdf",
            ]
        )
    )

    assert metadata_source_file(values) == "Stephens_Annual_Investment_Conference_2025.txt"


def test_metadata_source_file_falls_back_to_pdf_source_key() -> None:
    values = parse_metadata_key_values(
        "\n".join(
            [
                "[METADATA]",
                "source_pdf_file = PBI_Q1_2026_ceo_letter.pdf",
            ]
        )
    )

    assert metadata_source_file(values) == "PBI_Q1_2026_ceo_letter.pdf"


def test_material_signature_tracks_conference_and_ceo_letter_metadata(tmp_path: Path) -> None:
    base_dir = tmp_path / "TEST"
    base_dir.mkdir()
    before = material_dirs_signature(base_dir, "TEST")

    conference_dir = base_dir / "conferences"
    conference_dir.mkdir()
    (conference_dir / "TEST_Conference_METADATA_EN.txt").write_text(
        "source_txt_file = TEST_Conference.txt\nq4_hedged_pct = approximately_75\n",
        encoding="utf-8",
    )
    after_conference = material_dirs_signature(base_dir, "TEST")

    ceo_dir = base_dir / "CEO_letters"
    ceo_dir.mkdir()
    (ceo_dir / "TEST_Q1_2026_ceo_letter_METADATA_EN.txt").write_text(
        "source_pdf_file = TEST_Q1_2026_ceo_letter.pdf\nstrategic_review = on_track\n",
        encoding="utf-8",
    )
    after_ceo = material_dirs_signature(base_dir, "TEST")

    assert after_conference != before
    assert after_ceo != after_conference
