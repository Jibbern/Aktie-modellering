"""Declarative sector semantics consumed by source-native adapters."""

from .retail import RETAIL_SECTOR_PACK, RetailSectorPack
from .business_services import BUSINESS_SERVICES_SECTOR_PACK, BusinessServicesSectorPack

__all__ = [
    "BUSINESS_SERVICES_SECTOR_PACK",
    "BusinessServicesSectorPack",
    "RETAIL_SECTOR_PACK",
    "RetailSectorPack",
]
