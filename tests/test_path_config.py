from __future__ import annotations

from pathlib import Path
import json
import sys

import stock_models
from pbi_xbrl.path_config import (
    StockModelPathConfig,
    clear_config_data_root,
    resolve_effective_data_root,
    resolve_effective_data_root_from_ancestors,
    write_config_data_root,
)
from pbi_xbrl.market_data.cache import resolve_market_cache_root
from pbi_xbrl import render_validation_runner, workbook_validation_runner


def test_data_root_resolver_maps_portable_layout(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    data_root = tmp_path / "StockModelData"
    paths = StockModelPathConfig(repo_root=repo_root, data_root=data_root)

    assert paths.data_root == data_root.resolve()
    assert paths.sec_cache_dir == data_root.resolve() / "sec_cache"
    assert paths.ticker_sec_cache_dir("gpre") == data_root.resolve() / "sec_cache" / "GPRE"
    assert paths.ticker_dir("gpre") == data_root.resolve() / "tickers" / "GPRE"
    assert paths.market_cache_dir == data_root.resolve() / "market_cache"
    assert paths.writer_cache_dir == data_root.resolve() / "writer_cache"
    assert paths.basis_proxy_dir == data_root.resolve() / "basis_proxy"
    assert paths.excel_output_dir == data_root.resolve() / "outputs" / "Excel stock models"
    assert paths.render_checks_dir == data_root.resolve() / "render_checks"
    assert paths.validation_reports_dir == data_root.resolve() / "validation_reports"
    assert paths.logs_dir == data_root.resolve() / "logs"


def test_legacy_resolver_preserves_existing_layout(tmp_path: Path) -> None:
    repo_root = tmp_path / "Aktier"
    paths = StockModelPathConfig(repo_root=repo_root)

    assert paths.data_root is None
    assert paths.sec_cache_dir == repo_root.resolve() / "sec_cache"
    assert paths.ticker_sec_cache_dir("PBI") == repo_root.resolve() / "sec_cache" / "PBI"
    assert paths.ticker_dir("PBI") == repo_root.resolve() / "PBI"
    assert paths.market_cache_dir == repo_root.resolve() / "sec_cache" / "market_data"
    assert paths.excel_output_dir == repo_root.resolve() / "Excel stock models"
    assert paths.render_checks_dir == repo_root.resolve() / "render_checks"


def test_data_root_runtime_dirs_created_only_when_requested(tmp_path: Path) -> None:
    paths = StockModelPathConfig(repo_root=tmp_path / "repo", data_root=tmp_path / "StockModelData")

    assert not paths.sec_cache_dir.exists()
    created = paths.ensure_runtime_dirs(ticker="anf")

    assert paths.sec_cache_dir.exists()
    assert paths.ticker_sec_cache_dir("anf").exists()
    assert paths.ticker_dir("anf").exists()
    assert paths.excel_output_dir.exists()
    assert paths.market_cache_dir.exists()
    assert paths.writer_cache_dir.exists()
    assert paths.basis_proxy_dir.exists()
    assert created["ticker_dir"] == paths.ticker_dir("ANF")


def test_stock_models_default_output_and_history_paths_respect_data_root(tmp_path: Path) -> None:
    paths = StockModelPathConfig(repo_root=tmp_path / "repo", data_root=tmp_path / "StockModelData")

    assert stock_models._default_out_path("gpre", paths=paths) == paths.excel_output_dir / "GPRE_model.xlsm"
    assert stock_models._default_step_a_out_path("gpre", paths=paths) == paths.excel_output_dir / "GPRE_step_a.xlsx"
    assert stock_models._default_history_export_path("gpre", ".csv", paths=paths) == paths.ticker_dir("GPRE") / "GPRE_model_History_Q.csv"


def test_validation_and_render_runners_resolve_data_root_defaults(tmp_path: Path) -> None:
    paths = StockModelPathConfig(repo_root=tmp_path / "repo", data_root=tmp_path / "StockModelData")

    assert workbook_validation_runner.resolve_workbook_dir(data_root=paths.data_root, workbook_dir=None) == paths.excel_output_dir
    assert workbook_validation_runner.resolve_output_dir(data_root=paths.data_root, output_dir=None) == paths.validation_reports_dir / "workbook_validation"
    assert render_validation_runner.resolve_workbook_dir(data_root=paths.data_root, workbook_dir=None) == paths.excel_output_dir
    assert render_validation_runner.resolve_output_root(data_root=paths.data_root, output_root=None) == paths.render_checks_dir


def test_market_cache_resolves_to_portable_market_cache_when_layout_exists(tmp_path: Path) -> None:
    paths = StockModelPathConfig(repo_root=tmp_path / "repo", data_root=tmp_path / "StockModelData")
    paths.ensure_runtime_dirs(ticker="gpre")

    assert resolve_market_cache_root(paths.ticker_sec_cache_dir("gpre")) == paths.market_cache_dir


def _seed_light_data_root(root: Path) -> None:
    (root / "sec_cache").mkdir(parents=True)
    (root / "tickers").mkdir()


def test_effective_data_root_priority_cli_env_config_auto_legacy(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "Aktier"
    repo_root.mkdir()
    auto_root = repo_root / "StockModelData"
    config_root = tmp_path / "ConfiguredRoot"
    env_root = tmp_path / "EnvRoot"
    cli_root = tmp_path / "CliRoot"
    for root in (auto_root, config_root, env_root, cli_root):
        _seed_light_data_root(root)
    write_config_data_root(repo_root, config_root)

    monkeypatch.setenv("STOCK_MODEL_DATA_ROOT", str(env_root))
    assert resolve_effective_data_root(repo_root, cli_data_root=cli_root).source == "CLI"

    env_res = resolve_effective_data_root(repo_root, cli_data_root="")
    assert env_res.data_root == env_root.resolve()
    assert env_res.source == "env"

    monkeypatch.delenv("STOCK_MODEL_DATA_ROOT")
    config_res = resolve_effective_data_root(repo_root)
    assert config_res.data_root == config_root.resolve()
    assert config_res.source == "config"

    clear_config_data_root(repo_root)
    auto_res = resolve_effective_data_root(repo_root)
    assert auto_res.data_root == auto_root.resolve()
    assert auto_res.source == "auto-detected"

    for child in auto_root.iterdir():
        if child.is_dir():
            child.rmdir()
    auto_root.rmdir()
    legacy_res = resolve_effective_data_root(repo_root)
    assert legacy_res.data_root is None
    assert legacy_res.source == "legacy"


def test_config_file_set_and_clear_root(tmp_path: Path) -> None:
    repo_root = tmp_path / "Aktier"
    data_root = tmp_path / "StockModelData"
    _seed_light_data_root(data_root)

    config_path = write_config_data_root(repo_root, data_root)
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    assert payload["data_root"] == str(data_root.resolve())
    assert resolve_effective_data_root(repo_root).data_root == data_root.resolve()

    clear_config_data_root(repo_root)
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    assert "data_root" not in payload


def test_registered_data_root_resolves_from_a_secondary_worktree(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    secondary_worktree = workspace / "Code.worktrees" / "secondary"
    data_root = workspace / "StockModelData"
    secondary_worktree.mkdir(parents=True)
    _seed_light_data_root(data_root)
    write_config_data_root(workspace, data_root)

    result = resolve_effective_data_root_from_ancestors(secondary_worktree, env={})

    assert result.data_root == data_root.resolve()
    assert result.source == "config"
    assert result.config_path == workspace / "stock_model_config.json"


def test_onedrive_data_root_refused_unless_allowed(tmp_path: Path) -> None:
    repo_root = tmp_path / "Aktier"
    one_drive_root = tmp_path / "OneDrive" / "StockModelData"
    _seed_light_data_root(one_drive_root)

    refused = resolve_effective_data_root(repo_root, cli_data_root=one_drive_root)
    assert refused.data_root is None
    assert refused.source == "legacy"
    assert refused.errors

    allowed = resolve_effective_data_root(repo_root, cli_data_root=one_drive_root, allow_onedrive_data_root=True)
    assert allowed.data_root == one_drive_root.resolve()
    assert allowed.source == "CLI"


def test_stock_models_print_paths_includes_effective_data_root_source(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    repo_root = tmp_path / "Aktier"
    auto_root = repo_root / "StockModelData"
    _seed_light_data_root(auto_root)
    monkeypatch.setattr(stock_models, "_project_root", lambda: repo_root)
    monkeypatch.setattr(sys, "argv", ["stock_models.py", "--ticker", "ANF", "--print-paths"])

    stock_models.main()

    payload = json.loads(capsys.readouterr().out)
    assert payload["data_root"] == str(auto_root.resolve())
    assert payload["data_root_source"] == "auto-detected"
    assert payload["ticker_dir"] == str(auto_root.resolve() / "tickers" / "ANF")
    assert payload["excel_output_dir"] == str(auto_root.resolve() / "outputs" / "Excel stock models")
