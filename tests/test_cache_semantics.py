from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path

import pandas as pd
import pytest

import pbi_xbrl.cache_semantics as cache_semantics
import pbi_xbrl.pipeline_orchestration as orchestration
import stock_models
from pbi_xbrl.cache_semantics import (
    CACHE_IDENTITY_SERIALIZATION_VERSION,
    MARKET_PROVIDER_PARSE_VERSIONS,
    CacheIdentityError,
    build_cache_identity,
    content_file_set_identity,
    file_content_sha256,
    module_content_identity,
)
from pbi_xbrl.pipeline_runtime import PipelineStageCache, dataframe_quick_signature
from pbi_xbrl.pipeline_types import PipelineConfig
from pbi_xbrl.market_data.providers import PROVIDERS


def _economic_identity(*, source: str = "source-sha", code: str = "code-sha", profile: str = "PBI") -> str:
    return build_cache_identity(
        "mutation-test-economic-cache",
        {
            "code_identity": code,
            "configuration": {"strictness": "strict"},
            "profile_identity": profile,
            "semantic_versions": {"unit_norm": "v1"},
            "source_content_identity": source,
        },
        required_fields=("code_identity", "profile_identity", "source_content_identity"),
    ).key


def test_identical_relevant_inputs_have_identical_identity() -> None:
    assert _economic_identity() == _economic_identity()


def test_source_content_mutation_changes_identity(tmp_path: Path) -> None:
    source = tmp_path / "source.htm"
    source.write_bytes(b"alpha")
    before = _economic_identity(source=file_content_sha256(source))
    source.write_bytes(b"bravo")
    after = _economic_identity(source=file_content_sha256(source))
    assert after != before


def test_source_mtime_only_change_does_not_change_identity(tmp_path: Path) -> None:
    source = tmp_path / "source.htm"
    source.write_bytes(b"stable")
    before = _economic_identity(source=file_content_sha256(source))
    stat = source.stat()
    os.utime(source, ns=(stat.st_atime_ns, stat.st_mtime_ns + 10_000_000))
    after = _economic_identity(source=file_content_sha256(source))
    assert after == before


def test_size_preserving_source_mutation_changes_identity(tmp_path: Path) -> None:
    source = tmp_path / "source.htm"
    source.write_bytes(b"alpha")
    before = file_content_sha256(source)
    source.write_bytes(b"bravo")
    assert source.stat().st_size == 5
    assert file_content_sha256(source) != before


def test_relevant_code_content_mutation_changes_identity(tmp_path: Path) -> None:
    module = tmp_path / "owner.py"
    module.write_text("VALUE = 1\n", encoding="utf-8")
    before = module_content_identity(tmp_path, ["owner.py"], contract_id="test-code")
    module.write_text("VALUE = 2\n", encoding="utf-8")
    after = module_content_identity(tmp_path, ["owner.py"], contract_id="test-code")
    assert after != before


def test_code_mtime_only_change_does_not_change_identity(tmp_path: Path) -> None:
    module = tmp_path / "owner.py"
    module.write_text("VALUE = 1\n", encoding="utf-8")
    before = module_content_identity(tmp_path, ["owner.py"], contract_id="test-code")
    stat = module.stat()
    os.utime(module, ns=(stat.st_atime_ns, stat.st_mtime_ns + 10_000_000))
    after = module_content_identity(tmp_path, ["owner.py"], contract_id="test-code")
    assert after == before


def test_relevant_semantic_version_change_changes_identity(monkeypatch) -> None:
    before = orchestration._tier3_non_gaap_stage_cache_key(
        sec_source_identity="sec-sha",
        submissions_signature="submissions-sha", mode_name="strict", max_quarters=20
    )
    monkeypatch.setattr(orchestration, "ADJUSTED_METRIC_UNIT_NORMALIZATION_VERSION", "mutation-v2")
    after = orchestration._tier3_non_gaap_stage_cache_key(
        sec_source_identity="sec-sha",
        submissions_signature="submissions-sha", mode_name="strict", max_quarters=20
    )
    assert after != before


def test_unrelated_semantic_version_does_not_change_unrelated_cache(monkeypatch) -> None:
    before = orchestration._tier3_non_gaap_stage_cache_key(
        sec_source_identity="sec-sha",
        submissions_signature="submissions-sha", mode_name="strict", max_quarters=20
    )
    monkeypatch.setattr(orchestration, "DEBT_TABLE_PERIOD_OWNERSHIP_VERSION", "unrelated-debt-v2")
    after = orchestration._tier3_non_gaap_stage_cache_key(
        sec_source_identity="sec-sha",
        submissions_signature="submissions-sha", mode_name="strict", max_quarters=20
    )
    assert after == before


def test_sec_source_content_identity_changes_sec_derived_stage_key() -> None:
    before = orchestration._tier3_non_gaap_stage_cache_key(
        sec_source_identity="sec-source-a",
        submissions_signature="submissions-sha",
        mode_name="strict",
        max_quarters=20,
    )
    after = orchestration._tier3_non_gaap_stage_cache_key(
        sec_source_identity="sec-source-b",
        submissions_signature="submissions-sha",
        mode_name="strict",
        max_quarters=20,
    )
    assert after != before


def test_profile_change_changes_bundle_identity_but_output_path_does_not(
    tmp_path: Path, monkeypatch
) -> None:
    cache_dir = tmp_path / "sec_cache"
    cache_dir.mkdir()
    cfg = PipelineConfig(cache_dir=cache_dir, repo_root=tmp_path)
    monkeypatch.setattr(stock_models, "_code_signature", lambda *_args: "code-sha")
    monkeypatch.setattr(stock_models, "_material_signature", lambda *_args: "materials-sha")
    monkeypatch.setattr(stock_models, "_sec_cache_signature", lambda *_args: "sec-sha")
    monkeypatch.setattr(
        stock_models,
        "market_input_fingerprint",
        lambda *_args, **_kwargs: {"fingerprint": "market-sha"},
    )

    args_a = argparse.Namespace(ticker="PBI", cik="", output=tmp_path / "one.xlsx")
    args_b = argparse.Namespace(ticker="PBI", cik="", output=tmp_path / "two.xlsx")
    args_c = argparse.Namespace(ticker="ANF", cik="", output=tmp_path / "one.xlsx")
    key_a = stock_models._pipeline_bundle_cache_key(args_a, cfg, tmp_path)
    key_b = stock_models._pipeline_bundle_cache_key(args_b, cfg, tmp_path)
    key_c = stock_models._pipeline_bundle_cache_key(args_c, cfg, tmp_path)
    assert key_a == key_b
    assert key_c != key_a


def test_debt_rate_semantic_version_change_invalidates_bundle_identity(
    tmp_path: Path, monkeypatch
) -> None:
    cache_dir = tmp_path / "sec_cache"
    cache_dir.mkdir()
    cfg = PipelineConfig(cache_dir=cache_dir, repo_root=tmp_path)
    args = argparse.Namespace(ticker="GPRE", cik="", output=tmp_path / "model.xlsx")
    monkeypatch.setattr(stock_models, "_code_signature", lambda *_args: "code-sha")
    monkeypatch.setattr(stock_models, "_material_signature", lambda *_args: "materials-sha")
    monkeypatch.setattr(stock_models, "_sec_cache_signature", lambda *_args: "sec-sha")
    monkeypatch.setattr(
        stock_models,
        "market_input_fingerprint",
        lambda *_args, **_kwargs: {"fingerprint": "market-sha"},
    )
    before = stock_models._pipeline_bundle_cache_key(args, cfg, tmp_path)
    monkeypatch.setattr(
        stock_models,
        "DEBT_RATE_SEMANTIC_OWNERSHIP_VERSION",
        "mutation-v2",
    )
    after = stock_models._pipeline_bundle_cache_key(args, cfg, tmp_path)
    assert after != before


def test_current_linked_worktree_code_identity_is_resolved() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    signature = stock_models._code_signature(repo_root)
    assert signature not in {"", "none", "unknown", "default"}
    assert len(signature) == 64


@pytest.mark.parametrize("weak_value", ["", "none", "unknown", "default", None])
def test_required_identity_cannot_collapse_to_weak_value(weak_value) -> None:
    with pytest.raises(CacheIdentityError, match="required cache identity field"):
        build_cache_identity(
            "weak-required-test",
            {"code_identity": weak_value},
            required_fields=("code_identity",),
        )


def test_mapping_and_set_order_do_not_change_canonical_identity() -> None:
    first = build_cache_identity(
        "ordering-test", {"mapping": {"b": 2, "a": 1}, "set": {"z", "a"}}
    ).key
    second = build_cache_identity(
        "ordering-test", {"set": {"a", "z"}, "mapping": {"a": 1, "b": 2}}
    ).key
    assert first == second


def test_machine_absolute_path_is_rejected_as_semantic_payload(tmp_path: Path) -> None:
    with pytest.raises(CacheIdentityError, match="filesystem paths are not canonical"):
        build_cache_identity("path-leak-test", {"source": tmp_path / "source.htm"})


def test_stage_cache_rejects_stale_or_tampered_metadata(tmp_path: Path) -> None:
    cache = PipelineStageCache(tmp_path, "0000000001", 8)
    cache.save("stage", "semantic-key", {"amount": 1.0})
    meta_path, data_path = cache._paths("stage")
    assert cache.load("stage", "semantic-key") == {"amount": 1.0}

    data_path.write_bytes(data_path.read_bytes() + b"tampered")
    assert cache.load("stage", "semantic-key") is None

    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta["identity_contract"] = "stale"
    meta_path.write_text(json.dumps(meta), encoding="utf-8")
    assert cache.load("stage", "semantic-key") is None


def test_source_hash_matches_verified_published_bytes(tmp_path: Path) -> None:
    source = tmp_path / "published.htm"
    payload = b"<html><body>verified</body></html>"
    source.write_bytes(payload)
    assert file_content_sha256(source) == hashlib.sha256(payload).hexdigest()


def test_local_pdf_manifest_is_content_owned_not_stat_owned(tmp_path: Path) -> None:
    source = tmp_path / "source.pdf"
    source.write_bytes(b"first-pdf")
    stat = source.stat()
    entry = {
        "pages": 1,
        "source_content_sha256": file_content_sha256(source),
        "size": stat.st_size,
        "mtime": stat.st_mtime,
    }
    assert orchestration._local_non_gaap_pdf_manifest_entry_matches_source(source, entry)

    source.write_bytes(b"other-pdf")
    assert source.stat().st_size == stat.st_size
    os.utime(source, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    assert not orchestration._local_non_gaap_pdf_manifest_entry_matches_source(source, entry)


def test_legacy_stat_only_pdf_manifest_is_rejected(tmp_path: Path) -> None:
    source = tmp_path / "source.pdf"
    source.write_bytes(b"pdf-source")
    stat = source.stat()
    legacy_entry = {"pages": 1, "size": stat.st_size, "mtime": stat.st_mtime}
    assert not orchestration._local_non_gaap_pdf_manifest_entry_matches_source(
        source, legacy_entry
    )


def test_identical_source_bytes_at_different_paths_share_content_identity(tmp_path: Path) -> None:
    left = tmp_path / "left" / "source.htm"
    right = tmp_path / "right" / "renamed.htm"
    left.parent.mkdir()
    right.parent.mkdir()
    left.write_bytes(b"same published source")
    right.write_bytes(b"same published source")
    left_identity = content_file_set_identity(
        [left], contract_id="path-independent-source", include_logical_names=False
    )
    right_identity = content_file_set_identity(
        [right], contract_id="path-independent-source", include_logical_names=False
    )
    assert left_identity == right_identity


def test_cache_miss_and_warm_hit_preserve_zero_and_missing(tmp_path: Path) -> None:
    cache = PipelineStageCache(tmp_path, "0000000001", 8)
    assert cache.load("economics", "key") is None
    expected = pd.DataFrame({"explicit_zero": [0.0], "missing": [None]})
    cache.save("economics", "key", expected)
    warm = cache.load("economics", "key")
    pd.testing.assert_frame_equal(warm, expected)
    assert warm.loc[0, "explicit_zero"] == 0.0
    assert warm.loc[0, "missing"] is None


def test_cache_identity_contract_version_is_explicit() -> None:
    identity = _economic_identity()
    assert identity.startswith(f"{CACHE_IDENTITY_SERIALIZATION_VERSION}:")


def test_market_provider_parse_versions_have_one_discoverable_owner() -> None:
    assert {
        source: provider.provider_parse_version
        for source, provider in PROVIDERS.items()
    } == dict(MARKET_PROVIDER_PARSE_VERSIONS)


def test_dataframe_identity_falls_back_to_full_actual_schema() -> None:
    first = pd.DataFrame(
        {
            "as_of": ["2026-06-30"],
            "Total_bucketed": [230.0],
            "Source": ["Debt table"],
        }
    )
    reordered = first[["Source", "Total_bucketed", "as_of"]]
    changed = first.copy()
    changed.loc[0, "Total_bucketed"] = 231.0

    requested = ["quarter", "maturity_year", "amount_total"]
    first_identity = dataframe_quick_signature(first, requested)
    assert dataframe_quick_signature(reordered, requested) == first_identity
    assert dataframe_quick_signature(changed, requested) != first_identity


def test_dataframe_identity_rejects_duplicate_columns() -> None:
    frame = pd.DataFrame([[1, 2]], columns=["amount", "amount"])
    with pytest.raises(CacheIdentityError, match="duplicate dataframe columns"):
        dataframe_quick_signature(frame, ["amount"])
