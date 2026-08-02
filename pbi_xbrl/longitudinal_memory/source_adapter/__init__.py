"""Generic, source-native ingress for the C1 longitudinal-memory sidecar."""

from .builder import AdapterBuildResult, build_source_native_sidecar
from .discovery import discover_sources, load_source_set, verify_reviewed_model_inputs
from .inline_xbrl import capture_inline_xbrl_locator, extract_inline_xbrl_evidence
from .reviewed_metadata import verify_reviewed_metadata_documents
from .types import SourceAdapterError

__all__ = [
    "AdapterBuildResult",
    "SourceAdapterError",
    "build_source_native_sidecar",
    "discover_sources",
    "capture_inline_xbrl_locator",
    "extract_inline_xbrl_evidence",
    "load_source_set",
    "verify_reviewed_metadata_documents",
    "verify_reviewed_model_inputs",
]
