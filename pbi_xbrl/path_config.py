"""Central path resolver for legacy and portable stock-model data layouts."""
from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Dict, Mapping, Optional


CONFIG_FILENAMES = ("stock_model_config.json", ".stock_model_config.json")
DATA_ROOT_ENV_VAR = "STOCK_MODEL_DATA_ROOT"


@dataclass(frozen=True)
class StockModelPathConfig:
    """Resolve filesystem locations without changing workbook business logic.

    Without ``data_root`` the resolver mirrors the historical checkout layout under
    ``repo_root``. With ``data_root`` it maps all runtime/source/output folders under
    one portable data folder that can be copied or synced separately from the code.
    """

    repo_root: Path
    data_root: Optional[Path] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "repo_root", Path(self.repo_root).expanduser().resolve())
        if self.data_root is not None:
            object.__setattr__(self, "data_root", Path(self.data_root).expanduser().resolve())

    @property
    def portable(self) -> bool:
        return self.data_root is not None

    @property
    def sec_cache_dir(self) -> Path:
        return (self.data_root / "sec_cache") if self.data_root is not None else (self.repo_root / "sec_cache")

    def ticker_sec_cache_dir(self, ticker: str | None) -> Path:
        t = str(ticker or "").strip().upper()
        return self.sec_cache_dir / t if t else self.sec_cache_dir

    def ticker_dir(self, ticker: str | None) -> Path:
        t = str(ticker or "").strip().upper()
        if self.data_root is not None:
            return self.data_root / "tickers" / t if t else self.data_root / "tickers"
        return self.repo_root / t if t else self.repo_root

    @property
    def market_cache_dir(self) -> Path:
        if self.data_root is not None:
            return self.data_root / "market_cache"
        return self.sec_cache_dir / "market_data"

    @property
    def writer_cache_dir(self) -> Path:
        if self.data_root is not None:
            return self.data_root / "writer_cache"
        return self.repo_root / "writer_cache"

    @property
    def basis_proxy_dir(self) -> Path:
        if self.data_root is not None:
            return self.data_root / "basis_proxy"
        return self.repo_root / "GPRE" / "basis_proxy"

    @property
    def excel_output_dir(self) -> Path:
        if self.data_root is not None:
            return self.data_root / "outputs" / "Excel stock models"
        return self.repo_root / "Excel stock models"

    @property
    def render_checks_dir(self) -> Path:
        if self.data_root is not None:
            return self.data_root / "render_checks"
        return self.repo_root / "render_checks"

    @property
    def validation_reports_dir(self) -> Path:
        if self.data_root is not None:
            return self.data_root / "validation_reports"
        return self.repo_root / "Code" / "validation_reports"

    @property
    def logs_dir(self) -> Path:
        if self.data_root is not None:
            return self.data_root / "logs"
        return self.repo_root / "logs"

    def ensure_runtime_dirs(self, ticker: str | None = None) -> Dict[str, Path]:
        """Create the standard runtime folders for an explicit run.

        Validation/import code should call this only when it is about to write. The
        resolver itself is intentionally side-effect free.
        """

        dirs: Dict[str, Path] = {
            "sec_cache_dir": self.sec_cache_dir,
            "market_cache_dir": self.market_cache_dir,
            "writer_cache_dir": self.writer_cache_dir,
            "basis_proxy_dir": self.basis_proxy_dir,
            "excel_output_dir": self.excel_output_dir,
            "render_checks_dir": self.render_checks_dir,
            "validation_reports_dir": self.validation_reports_dir,
            "logs_dir": self.logs_dir,
        }
        if ticker:
            dirs["ticker_sec_cache_dir"] = self.ticker_sec_cache_dir(ticker)
            dirs["ticker_dir"] = self.ticker_dir(ticker)
        for path in dirs.values():
            path.mkdir(parents=True, exist_ok=True)
        return dirs


def resolve_stock_model_paths(repo_root: Path, data_root: str | Path | None = None) -> StockModelPathConfig:
    root = Path(data_root).expanduser().resolve() if str(data_root or "").strip() else None
    return StockModelPathConfig(repo_root=repo_root, data_root=root)


@dataclass(frozen=True)
class EffectiveDataRoot:
    data_root: Optional[Path]
    source: str
    config_path: Optional[Path] = None
    warnings: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()
    allow_onedrive_data_root: bool = False

    @property
    def portable(self) -> bool:
        return self.data_root is not None


def _repo_root(path: Path | str) -> Path:
    return Path(path).expanduser().resolve()


def config_file_path(repo_root: Path | str) -> Path:
    return _repo_root(repo_root) / CONFIG_FILENAMES[0]


def find_config_file(repo_root: Path | str) -> Optional[Path]:
    root = _repo_root(repo_root)
    for name in CONFIG_FILENAMES:
        path = root / name
        if path.exists() and path.is_file():
            return path
    return None


def read_stock_model_config(repo_root: Path | str) -> tuple[Dict[str, object], Optional[Path], Optional[str]]:
    path = find_config_file(repo_root)
    if path is None:
        return {}, None, None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {}, path, f"{type(exc).__name__}: {exc}"
    return (payload if isinstance(payload, dict) else {}), path, None


def write_config_data_root(
    repo_root: Path | str,
    data_root: Path | str,
    *,
    allow_onedrive_data_root: bool = False,
) -> Path:
    path = config_file_path(repo_root)
    payload: Dict[str, object] = {}
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(existing, dict):
                payload.update(existing)
        except Exception:
            payload = {}
    payload["data_root"] = str(Path(data_root).expanduser().resolve())
    payload["allow_onedrive_data_root"] = bool(allow_onedrive_data_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def clear_config_data_root(repo_root: Path | str) -> Path:
    path = config_file_path(repo_root)
    payload: Dict[str, object] = {}
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(existing, dict):
                payload.update(existing)
        except Exception:
            payload = {}
    payload.pop("data_root", None)
    payload.pop("allow_onedrive_data_root", None)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def _path_contains_onedrive(path: Path) -> bool:
    return any("onedrive" in str(part).lower() for part in path.parts)


def light_data_root_check(
    data_root: Path | str,
    *,
    allow_onedrive_data_root: bool = False,
    require_existing: bool = True,
) -> tuple[bool, list[str]]:
    root = Path(data_root).expanduser().resolve()
    problems: list[str] = []
    if require_existing:
        if not root.exists() or not root.is_dir():
            problems.append(f"data_root does not exist: {root}")
        elif not ((root / "sec_cache").exists() or (root / "tickers").exists()):
            problems.append(f"data_root missing sec_cache/tickers: {root}")
    if _path_contains_onedrive(root) and not allow_onedrive_data_root:
        problems.append(
            "live data_root appears to be inside OneDrive; use a local StockModelData folder "
            "or pass --allow-onedrive-data-root / set config allow_onedrive_data_root=true explicitly"
        )
    return not problems, problems


def _candidate_result(
    repo_root: Path,
    candidate: Path,
    source: str,
    *,
    allow_onedrive_data_root: bool,
    config_path: Optional[Path] = None,
    require_existing: bool = True,
) -> EffectiveDataRoot:
    ok, problems = light_data_root_check(
        candidate,
        allow_onedrive_data_root=allow_onedrive_data_root,
        require_existing=require_existing,
    )
    if ok:
        return EffectiveDataRoot(
            data_root=candidate.expanduser().resolve(),
            source=source,
            config_path=config_path,
            allow_onedrive_data_root=allow_onedrive_data_root,
        )
    return EffectiveDataRoot(
        data_root=None,
        source="legacy",
        config_path=config_path,
        warnings=tuple(problems),
        errors=tuple(problems if source == "CLI" else ()),
        allow_onedrive_data_root=allow_onedrive_data_root,
    )


def resolve_effective_data_root(
    repo_root: Path | str,
    *,
    cli_data_root: Path | str | None = None,
    env: Optional[Mapping[str, str]] = None,
    allow_onedrive_data_root: bool = False,
) -> EffectiveDataRoot:
    """Resolve the data-root source in operator priority order.

    Priority:
    CLI > STOCK_MODEL_DATA_ROOT > repo config > auto-detected StockModelData > legacy.
    Only explicit CLI roots may be missing; configured/auto roots must pass the light
    health check before they are used.
    """

    root = _repo_root(repo_root)
    env_map = os.environ if env is None else env
    cli_text = str(cli_data_root or "").strip()
    if cli_text:
        return _candidate_result(
            root,
            Path(cli_text),
            "CLI",
            allow_onedrive_data_root=allow_onedrive_data_root,
            require_existing=False,
        )

    env_text = str(env_map.get(DATA_ROOT_ENV_VAR, "") or "").strip()
    if env_text:
        env_res = _candidate_result(
            root,
            Path(env_text),
            "env",
            allow_onedrive_data_root=allow_onedrive_data_root,
            require_existing=True,
        )
        if env_res.data_root is not None:
            return env_res
        env_warnings = list(env_res.warnings)
    else:
        env_warnings = []

    payload, cfg_path, cfg_error = read_stock_model_config(root)
    cfg_warnings = list(env_warnings)
    if cfg_error:
        cfg_warnings.append(f"could not read config file {cfg_path}: {cfg_error}")
    cfg_text = str(payload.get("data_root") or "").strip()
    cfg_allow_onedrive = bool(payload.get("allow_onedrive_data_root"))
    if cfg_text:
        cfg_res = _candidate_result(
            root,
            Path(cfg_text),
            "config",
            allow_onedrive_data_root=bool(allow_onedrive_data_root or cfg_allow_onedrive),
            config_path=cfg_path,
            require_existing=True,
        )
        if cfg_res.data_root is not None:
            if cfg_warnings:
                return EffectiveDataRoot(
                    data_root=cfg_res.data_root,
                    source=cfg_res.source,
                    config_path=cfg_res.config_path,
                    warnings=tuple(cfg_warnings),
                    allow_onedrive_data_root=cfg_res.allow_onedrive_data_root,
                )
            return cfg_res
        cfg_warnings.extend(cfg_res.warnings)

    auto = root / "StockModelData"
    if auto.exists():
        auto_res = _candidate_result(
            root,
            auto,
            "auto-detected",
            allow_onedrive_data_root=allow_onedrive_data_root,
            require_existing=True,
        )
        if auto_res.data_root is not None:
            return EffectiveDataRoot(
                data_root=auto_res.data_root,
                source=auto_res.source,
                config_path=cfg_path,
                warnings=tuple(cfg_warnings),
                allow_onedrive_data_root=auto_res.allow_onedrive_data_root,
            )
        cfg_warnings.extend(auto_res.warnings)

    return EffectiveDataRoot(data_root=None, source="legacy", config_path=cfg_path, warnings=tuple(cfg_warnings))


def resolve_effective_data_root_from_ancestors(
    start: Path | str,
    *,
    cli_data_root: Path | str | None = None,
    env: Optional[Mapping[str, str]] = None,
    allow_onedrive_data_root: bool = False,
) -> EffectiveDataRoot:
    """Resolve a registered data root for a repository or any linked worktree.

    A linked worktree may be outside the primary checkout's directory.  Walking
    ancestors lets it discover the same workspace-level config without encoding a
    developer-specific path.  An explicit CLI root remains authoritative and is
    therefore evaluated once rather than falling through to ancestor configs.
    """

    root = _repo_root(start)
    if str(cli_data_root or "").strip():
        return resolve_effective_data_root(
            root,
            cli_data_root=cli_data_root,
            env=env,
            allow_onedrive_data_root=allow_onedrive_data_root,
        )

    warnings: list[str] = []
    last_config_path: Optional[Path] = None
    for candidate in (root, *root.parents):
        result = resolve_effective_data_root(
            candidate,
            env=env,
            allow_onedrive_data_root=allow_onedrive_data_root,
        )
        if result.data_root is not None:
            return result
        warnings.extend(result.warnings)
        if result.config_path is not None:
            last_config_path = result.config_path

    return EffectiveDataRoot(
        data_root=None,
        source="legacy",
        config_path=last_config_path,
        warnings=tuple(dict.fromkeys(warnings)),
        allow_onedrive_data_root=allow_onedrive_data_root,
    )


def data_root_from_sec_cache_path(cache_dir: Path | str | None) -> Optional[Path]:
    """Infer portable data root from ``<data_root>/sec_cache/<TICKER>`` paths."""

    if cache_dir is None:
        return None
    try:
        croot = Path(cache_dir).expanduser().resolve()
    except Exception:
        croot = Path(cache_dir)
    if croot.name.lower() == "sec_cache":
        return croot.parent
    if croot.parent.name.lower() == "sec_cache":
        return croot.parent.parent
    return None
