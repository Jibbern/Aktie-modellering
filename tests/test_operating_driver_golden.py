from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
from zipfile import ZipFile

from pbi_xbrl.longitudinal_memory.operating_driver_golden import (
    GOLDEN_ACCEPTANCE_STATUS,
    GOLDEN_ID,
    GOLDEN_LIFECYCLE,
    GOLDEN_MANIFEST_PATH,
    GOLDEN_PRODUCTION_DEFAULT,
    WORKBOOK_IDS,
    fixture_bytes,
    load_json_strict,
    reproduce_registered_golden,
    verify_golden_manifest,
)
from pbi_xbrl.path_config import resolve_effective_data_root_from_ancestors


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = resolve_effective_data_root_from_ancestors(REPOSITORY_ROOT, env={}).data_root
if DATA_ROOT is None:
    raise RuntimeError("A registered StockModelData root is required for golden tests.")

PROTECTED = {
    "ANF": DATA_ROOT / "outputs" / "Excel stock models" / "ANF_model.xlsx",
    "PBI": DATA_ROOT / "outputs" / "Excel stock models" / "PBI_model.xlsx",
    "GPRE": DATA_ROOT / "outputs" / "Excel stock models" / "GPRE_model.xlsm",
}
EXPECTED = {
    "ANF": {
        "raw": "3a99f3dd098884744b71313fb9d44ad02da0fb8906a6e6567c28f290bf4dcc8e",
        "semantic": "bcbb34a65556f1325a34c1679de8a54cc72060c6923fd7694350ea3fba3ec37c",
        "canonical": "a090559f3123842ff11073e45f78e331801b79262ecd741f74190762d4b80c91",
        "package": "84f7e9bd17cf199cd3764fe809e5660bab6aaa1ded1d4f17701d26ca0b52be29",
    },
    "PBI": {
        "raw": "7e11a89d48994ed192c5f301fceb30a5218b03e1bef3806d02b9326b295c9838",
        "semantic": "ba3103fa0659f185a590d0471945cb037631b06cf82ad6956f93688a5e851671",
        "canonical": "d1986d853426a4a01ff7dc6ee0012179b1a1c03bc9c5fddbae19e83c44389e66",
        "package": "3af89eda78691eac4a1dff3688a2eaa699f1cf2a5fe49f5e19c23927e94cf26f",
    },
    "GPRE": {
        "raw": "ae3bcf6b6a30531d2f9b68e7fef19a4711d7c7c283d4886e4b06e5fa662bbcb6",
        "semantic": "381bf605fc15bce31a531200cc60ba67f9677bace41f97b209e4537ac9ab45ec",
        "canonical": "f00cbb645498122018f5f2c5f3ace4642e7a9bb6e8c361addf31e935c94de97a",
        "package": "15c4754797aa6232086feb81dac6bdca283a89fadc0e667365a55bf848a4ee45",
    },
}


def test_operating_drivers_golden_manifest_is_closed_and_green() -> None:
    receipt = verify_golden_manifest()
    manifest = receipt["manifest"]
    assert receipt["passed"] is True
    assert manifest["golden_id"] == receipt["golden_id"] == GOLDEN_ID
    assert manifest["acceptance_status"] == GOLDEN_ACCEPTANCE_STATUS
    assert manifest["lifecycle"] == receipt["lifecycle"] == GOLDEN_LIFECYCLE
    assert manifest["production_default"] is receipt["production_default"] is False
    assert GOLDEN_PRODUCTION_DEFAULT is False
    assert manifest["checkpoint"]["pre_operating_drivers_checkpoint"] == (
        "3e9c86f37996fe4eab414435c706955957b1e9df"
    )
    assert manifest["checkpoint"]["rollback_requires_protected_workbook_modification"] is False


def test_product_packages_and_cross_ticker_contract_are_frozen() -> None:
    receipt = verify_golden_manifest()
    assert {ticker: row["package_sha256"] for ticker, row in receipt["packages"].items()} == {
        ticker: row["package"] for ticker, row in EXPECTED.items()
    }
    contract = load_json_strict(
        GOLDEN_MANIFEST_PATH.parent / "operating_drivers_generic_contract.v1.json"
    )
    assert contract["architecture"] == ["SHARED_ENGINE", "SECTOR_PACK", "DECLARATIVE_TICKER_PROFILE"]
    assert contract["ticker_specific_python_economic_branch_count"] == 0
    assert contract["ticker_specific_python_ui_economic_branch_count"] == 0
    assert contract["ticker_specific_python_analytics_economic_branch_count"] == 0
    assert contract["missing_is_never_zero"] is True
    assert contract["workbook_bridge"] == "target_not_wired"
    assert contract["production_default"] is False


def test_all_registered_workbooks_replay_exactly(tmp_path: Path) -> None:
    manifest = load_json_strict(GOLDEN_MANIFEST_PATH)
    by_ticker = {row["ticker"]: row for row in manifest["workbook_goldens"]}
    for ticker in ("ANF", "PBI", "GPRE"):
        output = tmp_path / (f"{ticker}_operating_drivers_golden" + PROTECTED[ticker].suffix)
        receipt = reproduce_registered_golden(
            ticker=ticker,
            base_workbook=PROTECTED[ticker],
            output_workbook=output,
        )
        assert receipt["output_workbook_sha256"] == EXPECTED[ticker]["raw"]
        assert receipt["semantic_sha256"] == EXPECTED[ticker]["semantic"]
        assert receipt["canonical_ooxml_sha256"] == EXPECTED[ticker]["canonical"]
        assert receipt["workbook_id"] == WORKBOOK_IDS[ticker]
        assert receipt["reproduced_from_committed_fixtures"] is True
        assert by_ticker[ticker]["data_root_relative_path"].startswith("audit/")


def test_gpre_vba_and_package_inventory_are_byte_preserved(tmp_path: Path) -> None:
    output = tmp_path / "GPRE_operating_drivers_golden.xlsm"
    receipt = reproduce_registered_golden(
        ticker="GPRE", base_workbook=PROTECTED["GPRE"], output_workbook=output
    )
    expected_vba = "5a42646fe31b0593af2490af2151a65380c74d2f67e12815550abc39340e1f87"
    with ZipFile(PROTECTED["GPRE"], "r") as before, ZipFile(output, "r") as after:
        assert before.namelist() == after.namelist()
        assert hashlib.sha256(before.read("xl/vbaProject.bin")).hexdigest() == expected_vba
        assert hashlib.sha256(after.read("xl/vbaProject.bin")).hexdigest() == expected_vba
    assert receipt["vba_sha256"] == expected_vba


def test_quality_and_ownership_gates_are_zero() -> None:
    manifest = load_json_strict(GOLDEN_MANIFEST_PATH)
    acceptance = load_json_strict(
        GOLDEN_MANIFEST_PATH.parent / "operating_drivers_acceptance.v1.json"
    )
    assert manifest["ownership"]["duplicate_economic_owner_count"] == 0
    assert all(value == 0 for value in manifest["quality_gates"].values())
    assert acceptance["source_observation_delta"] == 0
    assert acceptance["analytics_delta"] == 0
    assert acceptance["semantic_delta"] == 0
    assert acceptance["ownership_delta"] == 0
    assert acceptance["repair_event_count"] == 0
    assert acceptance["gpre_vba_delta_count"] == 0
    assert acceptance["deterministic_replay"] == "PASS"


def test_generic_layers_have_no_ticker_specific_python_economic_branch() -> None:
    paths = [
        REPOSITORY_ROOT / "pbi_xbrl/longitudinal_memory/operating_driver_foundation.py",
        REPOSITORY_ROOT / "pbi_xbrl/longitudinal_memory/operating_driver_derived_analytics.py",
        REPOSITORY_ROOT / "pbi_xbrl/longitudinal_memory/operating_driver_semantic_priority.py",
        REPOSITORY_ROOT / "pbi_xbrl/longitudinal_memory/operating_driver_story_selection.py",
        REPOSITORY_ROOT / "pbi_xbrl/longitudinal_memory/operating_driver_cross_ticker_product.py",
        REPOSITORY_ROOT / "pbi_xbrl/longitudinal_memory/operating_driver_cross_ticker_workbook.py",
    ]
    pattern = re.compile(
        r"(?:ticker\s*(?:==|!=)\s*['\"](?:ANF|PBI|GPRE)['\"]|"
        r"['\"](?:ANF|PBI|GPRE)['\"]\s*(?:==|!=)\s*ticker)",
        re.I,
    )
    assert [str(path) for path in paths if pattern.search(path.read_text(encoding="utf-8"))] == []


def test_lifecycle_and_ownership_registries_route_the_golden() -> None:
    lifecycle = json.loads((REPOSITORY_ROOT / "docs/SYSTEM_LIFECYCLE_REGISTRY.json").read_text(encoding="utf-8"))
    ownership = json.loads((REPOSITORY_ROOT / "docs/OWNERSHIP_REGISTRY.json").read_text(encoding="utf-8"))
    components = {row["component_id"]: row for row in lifecycle["components"]}
    concepts = {row["concept_id"]: row for row in ownership["concepts"]}
    assert components["component:operating-drivers-source-native-product@1"]["production_status"] == "accepted_not_workbook_wired"
    assert components["component:operating-drivers-workbook-bridge@1"]["lifecycle_state"] == "target_not_wired"
    assert concepts["concept:operating-drivers-economics@1"]["canonical_owner_component_id"] == "component:operating-drivers-source-native-product@1"
    assert concepts["concept:operating-drivers-workbook-projection@1"]["canonical_owner_component_id"] == "component:operating-drivers-workbook-bridge@1"


def test_manifest_has_no_absolute_path_or_weak_identity() -> None:
    manifest = load_json_strict(GOLDEN_MANIFEST_PATH)
    text = fixture_bytes(GOLDEN_MANIFEST_PATH).decode("utf-8")
    assert "C:\\\\Users\\\\" not in text
    assert all(not Path(row["repository_path"]).is_absolute() for row in manifest["implementation_artifacts"])
    assert all(not Path(row["relative_path"]).is_absolute() for row in manifest["fixture_artifacts"])
    assert all(row["data_root_relative_path"].startswith("audit/") for row in manifest["workbook_goldens"])
