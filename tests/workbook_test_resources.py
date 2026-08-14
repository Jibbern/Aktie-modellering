from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Mapping

from pbi_xbrl.path_config import resolve_effective_data_root_from_ancestors


SUPPORTED_WORKBOOK_EXTENSIONS = (".xlsx", ".xlsm")


def semantic_ooxml_rgb(value: object) -> str:
    """Return the six-digit RGB identity for an OOXML direct color.

    Excel/openpyxl may serialize the otherwise identical direct color with either
    ``00`` or ``FF`` in the alpha byte.  Workbook style contracts in this test
    suite own the visible RGB triplet, while still rejecting every other alpha
    encoding and every malformed color token.
    """

    raw = str(value or "").strip().upper()
    if len(raw) == 8:
        alpha, raw = raw[:2], raw[2:]
        if alpha not in {"00", "FF"}:
            raise AssertionError(f"Unsupported OOXML direct-color alpha: {alpha!r}")
    if len(raw) != 6 or any(ch not in "0123456789ABCDEF" for ch in raw):
        raise AssertionError(f"Unsupported OOXML direct-color encoding: {value!r}")
    return raw


def registered_data_root(
    anchor: Path | None = None,
    *,
    env: Mapping[str, str] | None = None,
) -> Path:
    """Return the explicitly registered StockModelData root for test resources."""

    resolution = resolve_effective_data_root_from_ancestors(
        anchor or Path(__file__).resolve(),
        env={} if env is None else env,
    )
    if resolution.data_root is None:
        details = "; ".join((*resolution.errors, *resolution.warnings)) or "no registered data root"
        raise AssertionError(f"Required registered StockModelData root is unavailable: {details}")
    return resolution.data_root


def registered_workbook_dir(anchor: Path | None = None) -> Path:
    """Resolve the delivered-workbook owner without depending on a checkout path."""

    explicit = str(os.environ.get("STOCK_MODEL_WORKBOOK_DIR") or "").strip()
    workbook_dir = (
        Path(explicit).expanduser().resolve()
        if explicit
        else registered_data_root(anchor) / "outputs" / "Excel stock models"
    )
    if not workbook_dir.is_dir():
        raise AssertionError(f"Required registered workbook directory is unavailable: {workbook_dir}")
    return workbook_dir


def delivered_workbook_path(ticker: str, anchor: Path | None = None) -> Path:
    """Resolve exactly one versioned delivered workbook for ``ticker``.

    Both OOXML extensions are accepted because macro preservation is an artifact
    contract, not a filesystem-order preference.  More than one matching artifact
    is ambiguous and therefore fails closed.
    """

    ticker_id = str(ticker or "").strip().upper()
    if not ticker_id:
        raise AssertionError("Ticker is required for delivered-workbook resolution.")
    workbook_dir = registered_workbook_dir(anchor)
    candidates = tuple(
        path
        for suffix in SUPPORTED_WORKBOOK_EXTENSIONS
        if (path := workbook_dir / f"{ticker_id}_model{suffix}").is_file()
    )
    if len(candidates) != 1:
        rendered = ", ".join(str(path) for path in candidates) or "none"
        raise AssertionError(
            f"Expected one unambiguous delivered workbook for {ticker_id} under "
            f"{workbook_dir}; found {len(candidates)}: {rendered}"
        )
    return candidates[0]


def versioned_registered_artifact_path(
    relative_path: str | Path,
    expected_sha256: str,
    anchor: Path | None = None,
) -> Path:
    """Resolve a hash-bound historical artifact beneath the registered data root."""

    root = registered_data_root(anchor).resolve()
    path = (root / Path(relative_path)).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise AssertionError(f"Historical artifact escapes registered data root: {path}") from exc
    if not path.is_file():
        raise AssertionError(f"Required versioned historical artifact is unavailable: {path}")
    actual_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
    expected = str(expected_sha256 or "").strip().lower()
    if actual_sha256 != expected:
        raise AssertionError(
            f"Historical artifact identity mismatch for {path}: "
            f"expected {expected}, got {actual_sha256}"
        )
    return path


def registered_ticker_dir(ticker: str, anchor: Path | None = None) -> Path:
    ticker_id = str(ticker or "").strip().upper()
    path = registered_data_root(anchor) / "tickers" / ticker_id
    if not path.is_dir():
        raise AssertionError(f"Required registered ticker resource directory is unavailable: {path}")
    return path
