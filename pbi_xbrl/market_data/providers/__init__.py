from .ams_3617 import AMS3617Provider
from .nwer import NWERProvider


PROVIDERS = {
    "ams_3617": AMS3617Provider(),
    "nwer": NWERProvider(),
}

__all__ = ["PROVIDERS", "AMS3617Provider", "NWERProvider"]
