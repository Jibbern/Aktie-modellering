from __future__ import annotations

import os
from pathlib import Path

import pytest

import stock_models


def _checkout(root: Path, *, linked: bool) -> Path:
    root.mkdir(parents=True)
    git_marker = root / ".git"
    if linked:
        git_marker.write_text("gitdir: ../.git/worktrees/example\n", encoding="utf-8")
    else:
        git_marker.mkdir()
    (root / "pbi_xbrl").mkdir()
    (root / "stock_models.py").write_text("VALUE = 1\n", encoding="utf-8")
    (root / "pbi_xbrl" / "pipeline.py").write_text("PIPELINE = 1\n", encoding="utf-8")
    return root


def test_current_linked_worktree_resolves_nonempty_content_signature() -> None:
    root = stock_models._project_root()
    assert (root / ".git").is_file()
    signature = stock_models._code_signature(root)
    assert signature != "none"
    assert len(signature) == 64


def test_primary_and_linked_checkout_shapes_share_content_identity(tmp_path: Path) -> None:
    primary = _checkout(tmp_path / "primary", linked=False)
    linked = _checkout(tmp_path / "linked", linked=True)
    assert stock_models._code_signature(primary) == stock_models._code_signature(linked)


def test_nested_working_directory_does_not_change_module_owned_checkout(monkeypatch, tmp_path: Path) -> None:
    expected = stock_models._project_root()
    nested = tmp_path / "nested" / "working" / "directory"
    nested.mkdir(parents=True)
    monkeypatch.chdir(nested)
    assert stock_models._project_root() == expected
    assert stock_models._code_signature(stock_models._project_root()) == stock_models._code_signature(expected)


def test_relevant_content_mutation_changes_signature(tmp_path: Path) -> None:
    root = _checkout(tmp_path / "checkout", linked=True)
    before = stock_models._code_signature(root)
    (root / "pbi_xbrl" / "pipeline.py").write_text("PIPELINE = 2\n", encoding="utf-8")
    assert stock_models._code_signature(root) != before


def test_mtime_only_mutation_does_not_change_signature(tmp_path: Path) -> None:
    root = _checkout(tmp_path / "checkout", linked=True)
    code_path = root / "pbi_xbrl" / "pipeline.py"
    before = stock_models._code_signature(root)
    stat = code_path.stat()
    os.utime(code_path, (stat.st_atime + 10, stat.st_mtime + 10))
    assert stock_models._code_signature(root) == before


def test_stock_model_data_content_does_not_change_code_signature(tmp_path: Path) -> None:
    root = _checkout(tmp_path / "checkout", linked=True)
    before = stock_models._code_signature(root)
    data_file = root / "StockModelData" / "tickers" / "PBI" / "source.htm"
    data_file.parent.mkdir(parents=True)
    data_file.write_text("economic source content", encoding="utf-8")
    assert stock_models._code_signature(root) == before


@pytest.mark.parametrize("shape", ["missing_git", "missing_code", "not_a_checkout"])
def test_unresolved_repository_identity_fails_explicitly(tmp_path: Path, shape: str) -> None:
    root = tmp_path / shape
    root.mkdir()
    if shape != "missing_git":
        (root / ".git").mkdir()
    if shape not in {"missing_code", "not_a_checkout"}:
        (root / "stock_models.py").write_text("pass\n", encoding="utf-8")
        (root / "pbi_xbrl").mkdir()
    with pytest.raises(stock_models.RepositoryIdentityError, match="repository code root is unresolved"):
        stock_models._code_signature(root)
