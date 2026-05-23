from __future__ import annotations

from pathlib import Path

from stock_models import _default_cache_dir_for_ticker, _material_signature


def test_portable_data_root_resolves_cache_without_code_root(tmp_path: Path) -> None:
    code_root = tmp_path / "code"
    data_root = tmp_path / "OneDriveStockData"
    code_root.mkdir()
    data_root.mkdir()

    cache_dir = _default_cache_dir_for_ticker(code_root, "GPRE", data_root=data_root)

    assert cache_dir == data_root / "sec_cache" / "GPRE"
    assert cache_dir.exists()


def test_material_signature_can_use_explicit_ticker_material_root(tmp_path: Path) -> None:
    repo_root = tmp_path / "code"
    material_root = tmp_path / "OneDriveStockData" / "GPRE"
    (material_root / "earnings_transcripts").mkdir(parents=True)
    (material_root / "earnings_transcripts" / "gpre_q1_2026.txt").write_text("45Z commentary", encoding="utf-8")

    assert _material_signature(repo_root, "GPRE", material_root) != "missing"
