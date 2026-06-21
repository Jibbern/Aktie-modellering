"""Economics_Overlay callback adapter support."""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any, MutableMapping

from .excel_writer_economics_overlay_orchestrator import (
    EconomicsOverlayOrchestratorDeps,
    write_economics_overlay_sheet as _write_economics_overlay_sheet,
)


@dataclass(frozen=True)
class EconomicsOverlaySheetDeps:
    runtime: MutableMapping[str, Any]


class EconomicsOverlaySheetWriter:
    def __init__(self, deps: EconomicsOverlaySheetDeps) -> None:
        self._runtime = deps.runtime

    def write_economics_overlay_sheet(self, rows: Any) -> Any:
        runtime = self._runtime
        orchestrator_deps = EconomicsOverlayOrchestratorDeps(
            **{field.name: runtime[field.name] for field in fields(EconomicsOverlayOrchestratorDeps)}
        )
        return _write_economics_overlay_sheet(orchestrator_deps, rows)
