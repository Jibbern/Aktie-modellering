from __future__ import annotations

import subprocess
from pathlib import Path

from pbi_xbrl import workbook_validation_runner


ROOT = Path(__file__).resolve().parents[1]


def test_root_default_validation_tickers_remain_canonical_three() -> None:
    assert tuple(workbook_validation_runner.TICKERS) == ("PBI", "GPRE", "ANF")


def test_architecture_pass_does_not_touch_production_workbook_writers() -> None:
    result = subprocess.run(
        ["git", "diff", "--name-only"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    changed = {line.strip().replace("\\", "/") for line in result.stdout.splitlines() if line.strip()}
    forbidden = {
        path
        for path in changed
        if path.startswith("pbi_xbrl/excel_writer")
        or path in {
            "pbi_xbrl/pipeline.py",
            "pbi_xbrl/company_profiles.py",
            "pbi_xbrl/quarter_notes.py",
            "pbi_xbrl/sec_xbrl.py",
            "pbi_xbrl/summary_overview.py",
            "stock_models.py",
        }
    }

    assert forbidden == set()
