"""Generic, source-native ingress for the C1 longitudinal-memory sidecar."""

from .builder import AdapterBuildResult, build_source_native_sidecar
from .discovery import discover_sources, load_source_set, verify_reviewed_model_inputs
from .types import SourceAdapterError

__all__ = [
    "AdapterBuildResult",
    "SourceAdapterError",
    "build_source_native_sidecar",
    "discover_sources",
    "load_source_set",
    "verify_reviewed_model_inputs",
]
