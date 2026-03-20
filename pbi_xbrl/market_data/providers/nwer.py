from __future__ import annotations

from .base import BaseMarketProvider


class NWERProvider(BaseMarketProvider):
    source = "nwer"
    provider_parse_version = "v1"
    local_patterns = ("nwer_pdfs/*.pdf", "nwer_pdfs/**/*.pdf")
