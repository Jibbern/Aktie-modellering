"""Registered market-data providers used by the sync service.

The registry stays small and explicit so orchestration can rebuild manifests and
parsed outputs deterministically from the providers that are currently
supported.
"""

from .ams_3617 import AMS3617Provider
from .ams_3618 import AMS3618Provider
from .cme_ethanol_platts import CMEChicagoEthanolPlattsProvider
from .local_barchart_futures import (
    LocalBarchartCornFuturesProvider,
    LocalBarchartGasFuturesProvider,
)
from .local_chicago_ethanol_futures import LocalChicagoEthanolFuturesProvider
from .nwer import NWERProvider


PROVIDERS = {
    "ams_3617": AMS3617Provider(),
    "ams_3618": AMS3618Provider(),
    "local_barchart_corn_futures": LocalBarchartCornFuturesProvider(),
    "local_barchart_gas_futures": LocalBarchartGasFuturesProvider(),
    "local_chicago_ethanol_futures": LocalChicagoEthanolFuturesProvider(),
    "cme_ethanol_platts": CMEChicagoEthanolPlattsProvider(),
    "nwer": NWERProvider(),
}

__all__ = [
    "PROVIDERS",
    "AMS3617Provider",
    "AMS3618Provider",
    "LocalBarchartCornFuturesProvider",
    "LocalBarchartGasFuturesProvider",
    "LocalChicagoEthanolFuturesProvider",
    "CMEChicagoEthanolPlattsProvider",
    "NWERProvider",
]
