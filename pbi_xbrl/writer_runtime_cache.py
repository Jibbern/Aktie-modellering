"""Run-scoped writer caches.

These caches sit between the long-lived pipeline/stage caches and the final workbook
render. They are intentionally per-export only: safe to reuse within one workbook write,
and safe to throw away before the next export.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .operating_drivers_runtime import OperatingDriversRuntime
from .quarter_notes_runtime import QuarterNotesRuntime
from .valuation_precompute_runtime import ValuationPrecomputeRuntime


@dataclass
class WriterRuntimeCache:
    # These caches are intentionally run-scoped: they may memoize expensive source
    # analysis within one export, but they must not leak assumptions between exports.
    valuation_style_bundle_cache: Optional[Dict[str, Any]] = None
    valuation_render_bundle_cache: Optional[Dict[str, Any]] = None
    valuation_precompute_bundle_cache: Optional[Dict[str, Any]] = None
    valuation_filing_docs_by_quarter_cache: Optional[Dict[str, Any]] = None
    adj_net_leverage_text_map_cache: Optional[Dict[pd.Timestamp, float]] = None
    leverage_local_material_index_cache: Optional[List[Dict[str, Any]]] = None
    leverage_audit_doc_index_cache: Optional[List[Dict[str, Any]]] = None
    promise_progress_ui_bundle_cache: Optional[Dict[str, Any]] = None
    valuation_buyback_auth_source_bundle_cache: Optional[Dict[str, Any]] = None
    operating_drivers: OperatingDriversRuntime = field(default_factory=OperatingDriversRuntime)
    quarter_notes: QuarterNotesRuntime = field(default_factory=QuarterNotesRuntime)
    valuation_precompute: ValuationPrecomputeRuntime = field(default_factory=ValuationPrecomputeRuntime)
