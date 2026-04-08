"""Registered market-data providers used by the sync service.

The registry stays small and explicit so orchestration can rebuild manifests and
parsed outputs deterministically from the providers that are currently
supported.
"""

from .ams_3617 import AMS3617Provider
from .cme_ethanol_platts import CMEChicagoEthanolPlattsProvider
from .nwer import NWERProvider


PROVIDERS = {
    "ams_3617": AMS3617Provider(),
    "cme_ethanol_platts": CMEChicagoEthanolPlattsProvider(),
    "nwer": NWERProvider(),
}

__all__ = ["PROVIDERS", "AMS3617Provider", "CMEChicagoEthanolPlattsProvider", "NWERProvider"]
