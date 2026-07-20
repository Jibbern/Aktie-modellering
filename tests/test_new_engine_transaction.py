from __future__ import annotations

import hashlib
import os
from pathlib import Path
import sys

import pytest

from pbi_xbrl.new_engine_transaction import (
    NewEngineTransactionError,
    candidate_path_for,
    normalize_candidate_acl,
    publish_no_overwrite,
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_candidate_path_is_uncreated_and_in_final_directory(tmp_path: Path) -> None:
    final_path = tmp_path / "ANF_shadow_model_v8.xlsx"

    candidate = candidate_path_for(final_path)

    assert candidate.parent == final_path.parent
    assert candidate.name.startswith(".ANF_shadow_model_v8.")
    assert candidate.name.endswith(".candidate.xlsx")
    assert not candidate.exists()


def test_windows_publication_is_atomic_and_never_overwrites(tmp_path: Path) -> None:
    final_path = tmp_path / "ANF_shadow_model_v8.xlsx"
    candidate = candidate_path_for(final_path)
    candidate.write_bytes(b"candidate")

    publish_no_overwrite(candidate, final_path, platform="win32")

    assert final_path.read_bytes() == b"candidate"
    assert not candidate.exists()

    second = candidate_path_for(final_path)
    second.write_bytes(b"second")
    with pytest.raises(NewEngineTransactionError, match="already exists"):
        publish_no_overwrite(second, final_path, platform="win32")
    assert final_path.read_bytes() == b"candidate"
    assert second.read_bytes() == b"second"


def test_posix_publication_uses_no_overwrite_link_semantics(tmp_path: Path) -> None:
    final_path = tmp_path / "TEST_shadow_model_v1.xlsx"
    candidate = candidate_path_for(final_path)
    candidate.write_bytes(b"candidate")

    publish_no_overwrite(candidate, final_path, platform="linux")

    assert final_path.read_bytes() == b"candidate"
    assert not candidate.exists()


@pytest.mark.skipif(sys.platform != "win32", reason="Windows ACL validation is Windows-only")
def test_windows_acl_reset_preserves_bytes_and_inherits_parent(tmp_path: Path) -> None:
    candidate = tmp_path / ".acl-test.candidate.xlsx"
    candidate.write_bytes(b"workbook bytes")
    before = _sha256(candidate)

    result = normalize_candidate_acl(candidate, platform="win32")

    assert result["status"] == "PASS"
    assert result["inheritance_enabled"] is True
    assert result["inherited_ace_count"] > 0
    assert result["sha256_before"] == result["sha256_after"] == before
    assert os.access(candidate, os.R_OK)


def test_non_windows_acl_check_is_explicitly_not_applicable(tmp_path: Path) -> None:
    candidate = tmp_path / ".acl-test.candidate.xlsx"
    candidate.write_bytes(b"workbook bytes")

    result = normalize_candidate_acl(candidate, platform="linux")

    assert result == {
        "status": "NOT_APPLICABLE",
        "platform": "linux",
        "sha256_before": _sha256(candidate),
        "sha256_after": _sha256(candidate),
    }
