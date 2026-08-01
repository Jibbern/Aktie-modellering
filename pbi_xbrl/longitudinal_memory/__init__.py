"""Contract-first longitudinal company-memory primitives.

This package is intentionally workbook-independent and company-agnostic. It
does not replace the current normalized package until later consumer cutovers.
"""

from .changes import derive_percentage_point_change
from .identity import IDENTITY_CONTRACT_VERSION, identity_digest
from .reconciliation import resolve_observations
from .serialization import runtime_sidecar_filename, serialize_package
from .validation import validate_or_raise, validate_package

__all__ = [
    "IDENTITY_CONTRACT_VERSION",
    "derive_percentage_point_change",
    "identity_digest",
    "resolve_observations",
    "runtime_sidecar_filename",
    "serialize_package",
    "validate_or_raise",
    "validate_package",
]
