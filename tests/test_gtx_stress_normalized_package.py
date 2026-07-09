from __future__ import annotations

import json
from pathlib import Path

from scripts.generate_normalized_company_data_package import main


def test_gtx_stress_command_writes_reports_without_workbook(tmp_path: Path) -> None:
    data_root = tmp_path / "StockModelData"
    sec_cache = data_root / "sec_cache" / "GTX"
    ticker_root = data_root / "tickers" / "GTX"
    sec_cache.mkdir(parents=True)
    ticker_root.mkdir(parents=True)
    (sec_cache / "sample.txt").write_text(
        "GTX earnings release. Guidance signal in filing text: safe harbor statements may differ materially.",
        encoding="utf-8",
    )
    (ticker_root / "profile.txt").write_text("Garrett Motion GTX source notes.", encoding="utf-8")
    output_dir = tmp_path / "stress"

    rc = main(
        [
            "--ticker",
            "GTX",
            "--data-root",
            str(data_root),
            "--stress-test",
            "--output-dir",
            str(output_dir),
        ]
    )

    assert rc == 0
    expected = [
        output_dir / "GTX_normalized_data_package.json",
        output_dir / "GTX_mapping_gaps_report.json",
        output_dir / "GTX_content_validation_report.json",
    ]
    for path in expected:
        assert path.exists()

    package = json.loads(expected[0].read_text(encoding="utf-8"))
    gaps = json.loads(expected[1].read_text(encoding="utf-8"))
    validation = json.loads(expected[2].read_text(encoding="utf-8"))

    assert package["ticker_metadata"]["ticker"]["value"] == "GTX"
    assert gaps["ticker"] == "GTX"
    assert {"binding_id", "shell_zone", "value_shape", "row_family"} <= set(gaps["gaps"][0])
    assert validation["ticker"] == "GTX"
    assert validation["issues"]
    assert not (output_dir / "GTX_model.xlsx").exists()
    assert not (data_root / "outputs" / "Excel stock models" / "GTX_model.xlsx").exists()
