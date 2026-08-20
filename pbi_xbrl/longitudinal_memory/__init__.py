"""Contract-first longitudinal company-memory primitives.

This package is intentionally workbook-independent and company-agnostic. It
does not replace the current normalized package until later consumer cutovers.
"""

from .changes import derive_percentage_point_change
from .identity import IDENTITY_CONTRACT_VERSION, identity_digest
from .operating_driver_foundation import (
    OPERATING_DRIVER_FOUNDATION_CONTRACT_VERSION,
    aggregate_duration_fail_closed,
    safe_qoq,
    safe_yoy,
)
from .operating_driver_derived_analytics import (
    OPERATING_DRIVER_DERIVED_ANALYTICS_CONTRACT_VERSION,
    build_derived_analytics,
)
from .operating_driver_shadow_registry import (
    OPERATING_DRIVER_SHADOW_REGISTRY_CONTRACT_VERSION,
    build_shadow_registry,
)
from .operating_driver_semantic_priority import (
    OPERATING_DRIVER_CONTEXT_SEMANTIC_PRIORITY_CONTRACT_VERSION,
    build_context_semantic_priority,
)
from .operating_driver_story_selection import (
    OPERATING_DRIVER_ORTHOGONAL_STORY_SELECTION_CONTRACT_VERSION,
    build_orthogonal_story_selection,
)
from .operating_driver_golden import (
    GOLDEN_ID as OPERATING_DRIVERS_GOLDEN_ID,
    reproduce_registered_golden as reproduce_operating_drivers_golden,
    verify_golden_manifest as verify_operating_drivers_golden,
)
from .reconciliation import resolve_observations
from .serialization import runtime_sidecar_filename, serialize_package
from .validation import validate_or_raise, validate_package

__all__ = [
    "IDENTITY_CONTRACT_VERSION",
    "OPERATING_DRIVER_FOUNDATION_CONTRACT_VERSION",
    "OPERATING_DRIVER_DERIVED_ANALYTICS_CONTRACT_VERSION",
    "OPERATING_DRIVER_CONTEXT_SEMANTIC_PRIORITY_CONTRACT_VERSION",
    "OPERATING_DRIVER_ORTHOGONAL_STORY_SELECTION_CONTRACT_VERSION",
    "OPERATING_DRIVER_SHADOW_REGISTRY_CONTRACT_VERSION",
    "OPERATING_DRIVERS_GOLDEN_ID",
    "aggregate_duration_fail_closed",
    "build_shadow_registry",
    "build_derived_analytics",
    "build_context_semantic_priority",
    "build_orthogonal_story_selection",
    "derive_percentage_point_change",
    "identity_digest",
    "resolve_observations",
    "reproduce_operating_drivers_golden",
    "runtime_sidecar_filename",
    "safe_qoq",
    "safe_yoy",
    "serialize_package",
    "validate_or_raise",
    "validate_package",
    "verify_operating_drivers_golden",
]
