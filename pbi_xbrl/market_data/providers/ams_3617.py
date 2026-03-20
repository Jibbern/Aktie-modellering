from __future__ import annotations

from .base import BaseMarketProvider


class AMS3617Provider(BaseMarketProvider):
    source = "ams_3617"
    provider_parse_version = "v1"
    local_patterns = ("ams_3617_pdfs/*.pdf", "ams_3617_pdfs/**/*.pdf")
