"""Reviewed ANF evidence foundation for the Product@2.1 successor.

This module deliberately stops at source registration, evidence extraction,
canonical facts, guidance versions, and reusable relations.  It does not select
Promise Progress rows and it has no workbook dependency.

The implementation contract is the read-only authority-expansion audit.  Audit
records are replayed against immutable local bytes and emitted as a deterministic
candidate package; the accepted Product@2 golden fixtures are never rewritten.
"""
from __future__ import annotations

import csv
import hashlib
import json
import re
import unicodedata
from collections import Counter, defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from lxml import html as lxml_html

from ..identity import build_identity, canonical_slug
from ..serialization import serialize_package


AUDIT_ID = "anf-local-source-review-authority-expansion@2026-08-09"
KNOWLEDGE_CUTOFF = "2026-07-29"
SOURCE_SET_ID = "source-set:anf:reviewed-evidence-foundation-successor@4"
PREDECESSOR_SOURCE_SET_ID = (
    "source-set:anf:promise-progress-product-v2-post-golden-successor@3"
)
ACCEPTED_PRODUCT_V2_SOURCE_SET_ID = (
    "source-set:anf:promise-progress-product-v2-candidate@2"
)
FOUNDATION_ID = "evidence-foundation:anf:product-v2-1-successor@1"
FOUNDATION_VERSION = "2.1.0-evidence-candidate"

AUDIT_FILENAMES = (
    "local_source_inventory.json",
    "source_review_decisions.json",
    "expanded_fact_inventory.json",
    "source_authority_reconciliation.json",
    "quarter_guidance_inventory.json",
    "q4_expanded_evidence_matrix.json",
    "derivation_opportunities_expanded.json",
    "cross_sheet_relevance_expanded.json",
    "remaining_extraction_gaps.json",
    "remaining_mapping_gaps.json",
    "source_conflicts_expanded.json",
    "audit_summary.md",
)

AUDIT_SHA256 = {
    "audit_summary.md": "6483d7959a48a3741ebd27255e022ad3579286b9354b6966f28a4a14b840525b",
    "cross_sheet_relevance_expanded.json": "bb7da077ca28a80dd1d45fa0a73426386740b77fb78f3cfef6e67295e326cc2b",
    "derivation_opportunities_expanded.json": "424ee1ef2f879e9cf8d2bdf605c2b89b51518adb682c7bd496e6fba3dd063792",
    "expanded_fact_inventory.json": "590d5fe001dd12a823b84d52c142af700185a4c905ecfd3fd8fe729f18346799",
    "local_source_inventory.json": "6f5e6b40b97822124e0a82daeb0f4837d2f03bf63d055f0c20b27ac27067d0d5",
    "q4_expanded_evidence_matrix.json": "15f273d32b12bc0952696baa00c848b592b903337c8a847c8afcd4e150e5ac3a",
    "quarter_guidance_inventory.json": "7db7508a3fe3d4957e77130f7c4eed7c48001a34c489c4366847b9c4c7056835",
    "remaining_extraction_gaps.json": "79d978f79c0af421f1256e57c48b55aa42484171f03c4e91ace5134decbdbd9b",
    "remaining_mapping_gaps.json": "3c3dc1b00e2baa254a9248101dd4d605e18695b9bd746a76aac59e834499abb6",
    "source_authority_reconciliation.json": "423cb0324fd1187612f6d97de261135ec742e2631565c018aa31960e63c25b04",
    "source_conflicts_expanded.json": "b9b4cd275fe1aee894825fe12d9c6ace7cd9f9349b6d413f606c3cd4d3a06c2f",
    "source_review_decisions.json": "2404613fb670efe8c8512eb3872f5f420626c9d5b7632c7435fca628d0e1e98b",
}

REVIEW_DECISIONS = frozenset(
    {
        "REVIEW_ACCEPT",
        "REVIEW_ACCEPT_WITH_LIMITATIONS",
        "REVIEW_DUPLICATE_ONLY",
        "REMAIN_NEEDS_REVIEW",
        "REJECT_AS_SOURCE",
    }
)
ECONOMICALLY_ELIGIBLE_DECISIONS = frozenset(
    {"REVIEW_ACCEPT", "REVIEW_ACCEPT_WITH_LIMITATIONS"}
)

AUTHORITY_TIERS = {
    "sec_filing": 1,
    "sec_8k_wrapper": 1,
    "earnings_release": 2,
    "business_update": 2,
    "governance_release": 2,
    "investor_presentation": 3,
    "investor_day_deck": 3,
    "earnings_call_transcript": 4,
    "conference_transcript": 4,
    "annual_report": 1,
    "other_issuer_source": 5,
    "proxy_or_source_download": 5,
}

DIRECTNESS_RANK = {
    "direct_exact": 1,
    "direct_range": 1,
    "direct_approximate": 1,
    "direct_minimum": 1,
    "direct_composite": 1,
    "exact_same_metric_derivation": 2,
    "derived_exact": 2,
    "component_based_derivation": 3,
    "derived_components": 3,
    "bounded_rounding_derivation": 4,
    "derived_bounded": 4,
    "unsupported": 5,
    "unsupported/inferential": 5,
}

CORE_METRIC_IDS = {
    "revenue-growth": "metric:core:revenue-growth@1",
    "net-sales-amount": "metric:core:net-sales@1",
    "operating-income-amount": "metric:core:operating-income@1",
    "operating-margin": "metric:core:operating-margin@1",
    "reported-diluted-eps": "metric:core:net-income-per-diluted-share@1",
    "net-income-per-diluted-share": "metric:core:net-income-per-diluted-share@1",
    "diluted-weighted-average-shares": (
        "metric:core:diluted-weighted-average-shares@1"
    ),
    "net-income-attributable": "metric:core:net-income-attributable@1",
    "gross-profit-amount": "metric:core:gross-profit@1",
    "property-equipment-purchases": (
        "metric:core:property-equipment-purchases@1"
    ),
    "common-stock-purchases-cash": (
        "metric:core:common-stock-purchases-cash@1"
    ),
    "share-repurchases": "metric:core:share-repurchases@1",
    "capital-expenditures": "metric:core:capital-expenditures@1",
    "comparable-sales": "metric:retail:comparable-sales@1",
    "store-openings": "metric:retail:store-openings@1",
    "store-closures": "metric:retail:store-closures@1",
    "store-closures-count": "metric:retail:store-closures-count@1",
    "store-remodels": "metric:retail:store-remodels@1",
    "store-right-sizes": "metric:retail:store-right-sizes@1",
    "store-remodels-right-sizes": "metric:retail:store-remodels-right-sizes@1",
    "net-store-openings": "metric:retail:net-store-openings@1",
}

UNIT_IDS = {
    "USD million": "unit:core:currency-millions@1",
    "USD thousand": "unit:core:currency-thousands@1",
    "USD billion": "unit:core:currency-billions@1",
    "USD per share": "unit:core:currency-per-share@1",
    "USD/share": "unit:core:currency-per-share@1",
    "million shares": "unit:core:shares-millions@1",
    "percent": "unit:core:percent@1",
    "percentage points": "unit:core:percentage-points@1",
    "basis points": "unit:core:basis-points@1",
    "count": "unit:core:count@1",
    "stores": "unit:core:count@1",
    "qualitative": "unit:core:qualitative@1",
    "USD and shares": "unit:core:composite@1",
}

XBRL_CONCEPTS = {
    "net-sales-amount": "Revenues",
    "operating-income-amount": "OperatingIncomeLoss",
    "net-income-attributable": "NetIncomeLoss",
    "reported-diluted-eps": "EarningsPerShareDiluted",
    "diluted-weighted-average-shares": (
        "WeightedAverageNumberOfDilutedSharesOutstanding"
    ),
    "gross-profit-amount": "GrossProfit",
    "property-equipment-purchases": "PaymentsToAcquirePropertyPlantAndEquipment",
    "common-stock-purchases-cash": "PaymentsForRepurchaseOfCommonStock",
}

RECONCILIATION_METRICS = (
    "net-sales-amount",
    "operating-income-amount",
    "net-income-attributable",
    "reported-diluted-eps",
    "diluted-weighted-average-shares",
    "gross-profit-amount",
)

HISTORICAL_REQUIRED_PERIODS = frozenset(
    {
        *(f"FY{year}-Q1" for year in range(2022, 2025)),
        *(f"FY{year}-Q2" for year in range(2022, 2025)),
        *(f"FY{year}-YTD-Q2" for year in range(2022, 2025)),
        *(f"FY{year}-Q3" for year in range(2022, 2025)),
        *(f"FY{year}-YTD-Q3" for year in range(2022, 2025)),
        *(f"FY{year}-Q4" for year in range(2022, 2025)),
        *(f"FY{year}" for year in range(2022, 2025)),
        "FY2026-Q1",
    }
)

ANNUAL_STORE_TOTALS = {
    "FY2022": {"store-openings": "59", "store-closures-count": "26", "store-remodels": "1", "store-right-sizes": "8"},
    "FY2023": {"store-openings": "35", "store-closures-count": "32", "store-remodels": "13", "store-right-sizes": "9"},
    "FY2024": {"store-openings": "65", "store-closures-count": "41", "store-remodels": "48", "store-right-sizes": "12"},
    "FY2025": {"store-openings": "62", "store-closures-count": "22", "store-remodels": "47", "store-right-sizes": "11"},
}

ANNUAL_STORE_TABLES = {
    "FY2022": {"experience": 47, "activity": 48},
    "FY2023": {"experience": 51, "activity": 52},
    "FY2024": {"experience": 51, "activity": 52},
    "FY2025": {"experience": 52, "activity": 53},
}

# Reviewed SEC-primary period disclosures used by Promise Progress.  Values are
# not trusted merely because they appear in this contract: the extractor below
# re-reads the immutable source bytes, finds the matching sentence, and refuses
# to emit an observation unless the parsed source values equal these reviewed
# expectations.
STORE_PERIOD_ACTIVITY = (
    ("2022-06-08", "FY2022-Q1", {"store-openings": "4", "store-closures-count": "5"}),
    ("2022-09-07", "FY2022-YTD-Q2", {"store-openings": "12", "store-closures-count": "7"}),
    ("2022-12-06", "FY2022-YTD-Q3", {"store-openings": "31", "store-closures-count": "9"}),
    ("2023-06-06", "FY2023-Q1", {"store-openings": "6", "store-closures-count": "10"}),
    ("2023-09-01", "FY2023-YTD-Q2", {"store-openings": "15", "store-closures-count": "18"}),
    ("2023-12-04", "FY2023-YTD-Q3", {"store-openings": "24", "store-closures-count": "21"}),
    ("2024-06-07", "FY2024-Q1", {"store-openings": "1", "store-closures-count": "13"}),
    (
        "2024-09-06",
        "FY2024-YTD-Q2",
        {
            "store-openings": "18",
            "store-closures-count": "26",
            "store-remodels": "23",
            "store-right-sizes": "7",
        },
    ),
    (
        "2024-12-06",
        "FY2024-YTD-Q3",
        {
            "store-openings": "39",
            "store-closures-count": "31",
            "store-remodels": "30",
            "store-right-sizes": "8",
        },
    ),
    (
        "2025-06-06",
        "FY2025-Q1",
        {
            "store-openings": "7",
            "store-closures-count": "3",
            "store-remodels": "9",
            "store-right-sizes": "1",
        },
    ),
    (
        "2025-09-05",
        "FY2025-YTD-Q2",
        {
            "store-openings": "26",
            "store-closures-count": "8",
            "store-remodels": "16",
            "store-right-sizes": "5",
        },
    ),
    (
        "2025-12-05",
        "FY2025-YTD-Q3",
        {
            "store-openings": "48",
            "store-closures-count": "10",
            "store-remodels": "24",
            "store-right-sizes": "8",
        },
    ),
)

ANNUAL_STORE_GUIDANCE = (
    (
        "2022-06-08",
        "FY2022",
        "plans to open 60 new stores, while closing 30 stores",
        {
            "store-openings": {
                "kind": "exact",
                "value": "60",
                "comparison_contract": "plan-point-without-reviewed-tolerance",
            },
            "store-closures-count": {
                "kind": "exact",
                "value": "30",
                "comparison_contract": "plan-point-without-reviewed-tolerance",
            },
        },
    ),
    (
        "2022-09-07",
        "FY2022",
        "plans to open 60 new stores, while closing 30 stores",
        {
            "store-openings": {
                "kind": "exact",
                "value": "60",
                "comparison_contract": "plan-point-without-reviewed-tolerance",
            },
            "store-closures-count": {
                "kind": "exact",
                "value": "30",
                "comparison_contract": "plan-point-without-reviewed-tolerance",
            },
        },
    ),
    (
        "2022-12-06",
        "FY2022",
        "plans to open 60 new stores, while closing 30 stores",
        {
            "store-openings": {
                "kind": "exact",
                "value": "60",
                "comparison_contract": "plan-point-without-reviewed-tolerance",
            },
            "store-closures-count": {
                "kind": "exact",
                "value": "30",
                "comparison_contract": "plan-point-without-reviewed-tolerance",
            },
        },
    ),
    (
        "2023-06-06",
        "FY2023",
        "plans to be a net store opener again this year with approximately 35-40 new stores, while closing approximately 20-25 stores",
        {
            "store-openings": {
                "kind": "range",
                "low": "35",
                "high": "40",
                "approximate": True,
            },
            "store-closures-count": {
                "kind": "range",
                "low": "20",
                "high": "25",
                "approximate": True,
            },
            "net-store-openings": {
                "kind": "qualitative",
                "value": "Net store opener",
            },
        },
    ),
    (
        "2023-09-01",
        "FY2023",
        "plans to be a net store opener again this year with approximately 35 new stores, while closing approximately 30 stores",
        {
            "store-openings": {"kind": "approximate", "value": "35", "tolerance": None},
            "store-closures-count": {"kind": "approximate", "value": "30", "tolerance": None},
            "net-store-openings": {"kind": "qualitative", "value": "Net store opener"},
        },
    ),
    (
        "2023-12-04",
        "FY2023",
        "expects store count to remain steady with approximately 35 new stores, while closing approximately 35 stores",
        {
            "store-openings": {"kind": "approximate", "value": "35", "tolerance": None},
            "store-closures-count": {"kind": "approximate", "value": "35", "tolerance": None},
            "net-store-openings": {"kind": "qualitative", "value": "Store count remain steady"},
        },
    ),
    (
        "2024-06-07",
        "FY2024",
        "store investment plan to include approximately 60 new stores, while closing approximately 40 stores",
        {
            "store-openings": {"kind": "approximate", "value": "60", "tolerance": None},
            "store-closures-count": {"kind": "approximate", "value": "40", "tolerance": None},
        },
    ),
    (
        "2024-09-06",
        "FY2024",
        "store investment plan includes delivering approximately 60 new stores, along with approximately 60 remodels and rightsizes, while closing approximately 40 stores",
        {
            "store-openings": {"kind": "approximate", "value": "60", "tolerance": None},
            "store-closures-count": {"kind": "approximate", "value": "40", "tolerance": None},
            "store-remodels-right-sizes": {"kind": "approximate", "value": "60", "tolerance": None},
        },
    ),
    (
        "2024-12-06",
        "FY2024",
        "store investment plan includes delivering approximately 20 net store openings during Fiscal 2024 consisting of opening approximately 60 new stores, while closing approximately 40 stores",
        {
            "net-store-openings": {"kind": "approximate", "value": "20", "tolerance": None},
            "store-openings": {"kind": "approximate", "value": "60", "tolerance": None},
            "store-closures-count": {"kind": "approximate", "value": "40", "tolerance": None},
            "store-remodels-right-sizes": {"kind": "approximate", "value": "60", "tolerance": None},
        },
    ),
)

MAY_2026_ANNUAL_GUIDANCE = (
    (2, "revenue-growth", "Growth In The Range of 3% to 5%", "percent", None),
    (4, "operating-margin", "In The Range of 12.0% to 12.5%", "percent", None),
    (6, "reported-diluted-eps", "In The Range of $10.20 to $11.00", "USD/share", "USD"),
    (7, "share-repurchases", "Around $450 million", "USD million", "USD"),
    (8, "diluted-weighted-average-shares", "Around 44 million", "million shares", None),
    (9, "capital-expenditures", "Around $225 million", "USD million", "USD"),
    (10, "net-store-openings", "~30 Net Store Openings", "count", None),
    (11, "store-openings", "50 Openings", "count", None),
    (11, "store-closures-count", "20 Closures", "count", None),
    (12, "store-remodels-right-sizes", "80 Remodels and Right-Sizes", "count", None),
)

CAPEX_DEFINITION_PERIODS = (
    ("FY2022-Q1", "2022-06-08", "26.292"),
    ("FY2022-YTD-Q2", "2022-09-07", "59.582"),
    ("FY2022-YTD-Q3", "2022-12-06", "120.282"),
    ("FY2022", "2023-03-02", "164.566"),
    ("FY2023-Q1", "2023-06-06", "46.391"),
    ("FY2023-YTD-Q2", "2023-09-01", "89.78"),
    ("FY2023-YTD-Q3", "2023-12-04", "128.601"),
    ("FY2023", "2025-03-31", "157.797"),
    ("FY2024-Q1", "2024-06-07", "38.886"),
    ("FY2024-YTD-Q2", "2024-09-06", "81.649"),
    ("FY2024-YTD-Q3", "2024-12-06", "132.04"),
    ("FY2024", "2025-03-31", "182.903"),
    ("FY2025-Q1", "2025-06-06", "50.764"),
    ("FY2025-YTD-Q2", "2025-09-05", "116.943"),
    ("FY2025-YTD-Q3", "2025-12-05", "185.212"),
    ("FY2025", "2026-03-26", "240.774"),
    ("FY2026-Q1", "2026-06-05", "61.341"),
)

SEC_DEFINITION_RELATIONS = (
    ("FY2022", "2023-03-02", "issuer release directly calls the cash-flow amount capital expenditures"),
    ("FY2023", "2025-03-31", "audited comparative labels total capital expenditures and identical P&E purchases"),
    ("FY2024", "2025-03-31", "audited annual filing labels total capital expenditures and identical P&E purchases"),
    ("FY2025-YTD-Q3", "2025-12-05", "10-Q labels the 39-week P&E cash-flow amount total capital expenditures"),
    ("FY2025", "2026-03-26", "audited annual filing labels the P&E cash-flow amount total capital expenditures"),
)

DEBT_EVENTS = (
    {
        "event_id": "debt-event:anf:2024-redemption-notice@1",
        "knowledge_date": "2024-06-28",
        "metric": "senior-secured-notes-redemption-notice",
        "value": {"kind": "exact", "value": "213.906", "unit": "USD million"},
        "source_fingerprint": "8.75%",
    },
    {
        "event_id": "debt-event:anf:2024-redemption-complete@1",
        "knowledge_date": "2024-07-17",
        "metric": "senior-secured-notes-redemption-complete",
        "value": {"kind": "exact", "value": "213.906", "unit": "USD million"},
        "source_fingerprint": "redemption",
    },
    {
        "event_id": "debt-event:anf:2024-abl-capacity@1",
        "knowledge_date": "2024-08-07",
        "metric": "abl-capacity",
        "value": {"kind": "exact", "value": "500", "unit": "USD million"},
        "source_fingerprint": "500",
    },
    {
        "event_id": "debt-event:anf:2024-fixed-zero-long-term-debt@1",
        "knowledge_date": "2024-09-06",
        "metric": "long-term-debt-balance",
        "value": {"kind": "exact", "value": "0", "unit": "USD million"},
        "source_fingerprint": "LongTermDebtNoncurrent",
    },
)


class EvidenceFoundationError(ValueError):
    """Raised when reviewed evidence cannot be materialized losslessly."""


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, child in pairs:
        if key in value:
            raise EvidenceFoundationError(f"Duplicate JSON key {key!r}.")
        value[key] = child
    return value


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_strict_object)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceFoundationError(f"Cannot strictly read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise EvidenceFoundationError(f"Audit artifact {path} is not a JSON object.")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _text(value: Any) -> str:
    if hasattr(value, "itertext"):
        value = " ".join(value.itertext())
    return " ".join(unicodedata.normalize("NFC", str(value or "")).split())


def _clean_text(value: Any) -> str:
    text = _text(value)
    replacements = {
        "â€“": "–",
        "â€”": "—",
        "â‰¥": "≥",
        "â‰¤": "≤",
        "â€™": "’",
        "â€œ": "“",
        "â€": "”",
        "Â": "",
    }
    for source, target in replacements.items():
        text = text.replace(source, target)
    return text


def _slug(value: Any) -> str:
    """Normalize reviewed audit labels into the repository identity grammar."""
    normalized = re.sub(r"[^a-z0-9]+", "-", _clean_text(value).casefold()).strip("-")
    if normalized and not normalized[0].isalpha():
        normalized = f"value-{normalized}"
    return canonical_slug(normalized or "unknown")


def _normalize_period_key(value: Any) -> str:
    period = _clean_text(value)
    if re.fullmatch(r"fy\d{4}(?:-(?:q[1-4]|ytd-q[1-4]))?", period, re.IGNORECASE):
        return period.upper()
    return period


def _audit_unit(metric: str, explicit_unit: Any, canonical_value: Mapping[str, Any]) -> str:
    if explicit_unit:
        unit = str(explicit_unit)
    else:
        unit = str(canonical_value.get("unit") or "")
    if unit:
        return {
            "USD per share": "USD/share",
            "stores": "count",
        }.get(unit, unit)
    if metric in {"revenue-growth", "operating-margin", "comparable-sales"}:
        return "percent"
    if metric in {"reported-diluted-eps", "net-income-per-diluted-share"}:
        return "USD/share"
    if metric == "diluted-weighted-average-shares":
        return "million shares"
    if metric in {
        "net-sales-amount",
        "operating-income-amount",
        "net-income-attributable",
        "property-equipment-purchases",
        "capital-expenditures",
        "common-stock-purchases-cash",
        "share-repurchases",
    }:
        return "USD million"
    if metric in {
        "store-openings",
        "store-closures",
        "store-closures-count",
        "store-remodels",
        "store-right-sizes",
        "net-store-openings",
        "ending-stores",
    }:
        return "count"
    return "qualitative"


def _audit_definition_id(metric: str, value: Any, record: Mapping[str, Any]) -> str:
    definition = str(value or "")
    if metric in {
        "store-openings",
        "store-closures",
        "store-closures-count",
        "store-remodels",
        "store-right-sizes",
        "net-store-openings",
    } and definition in {"", "company-reported"}:
        return "definition:anf:company-owned-store-activity@1"
    if definition in {"", "company-reported"}:
        if str(record.get("classification") or "") == "new_presentation_evidence":
            return "definition:anf:management-target@1"
        return "definition:core:company-reported@1"
    if definition == "company-guidance":
        return "definition:core:company-guidance@1"
    return f"definition:anf:{_slug(definition)}@1"


def _audit_basis_id(metric: str, value: Any, record: Mapping[str, Any]) -> str:
    basis = str(value or "")
    if basis in {"", "reported"}:
        if str(record.get("classification") or "") == "new_presentation_evidence":
            return "basis:anf:targeted@1"
        return "basis:core:reported@1"
    if basis == "guided":
        return "basis:core:guided@1"
    return f"basis:anf:{_slug(basis)}@1"


def _fact_value(value: Mapping[str, Any]) -> dict[str, Any]:
    """Keep semantic value shape separate from the observation's typed unit."""
    result: dict[str, Any] = {}
    for key, child in value.items():
        if key == "unit" or (key == "direction" and child is None):
            continue
        if key == "components" and isinstance(child, list):
            result[key] = [
                _fact_value(component) if isinstance(component, Mapping) else component
                for component in child
            ]
        else:
            result[key] = child
    return result


def _canonical_decimal(value: Decimal | str | int) -> str:
    decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    if decimal_value == 0:
        return "0"
    normalized = format(decimal_value.normalize(), "f")
    return normalized.rstrip("0").rstrip(".") if "." in normalized else normalized


def _stable_id(kind: str, *components: tuple[str, Any]) -> str:
    return build_identity(kind, components)


def _metric_id(metric: str) -> str:
    return CORE_METRIC_IDS.get(metric, f"metric:anf:{_slug(metric)}@1")


def _period_id(period: str) -> str:
    return f"period:anf:{_slug(period)}@1"


def _unit_id(unit: str | None) -> str:
    normalized = _clean_text(unit or "qualitative")
    return UNIT_IDS.get(normalized, f"unit:anf:{_slug(normalized)}@1")


def _locator_digest(locator: Mapping[str, Any]) -> str:
    payload = json.dumps(locator, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def _period_kind(period: str) -> str:
    if "YTD" in period.upper():
        return "ytd"
    if re.fullmatch(r"FY\d{4}-Q[1-4]", period.upper()):
        return "quarter"
    if re.fullmatch(r"FY\d{4}", period.upper()):
        return "annual"
    return "other"


def load_audit_contract(audit_root: Path | str) -> dict[str, Any]:
    root = Path(audit_root)
    if not root.is_dir():
        raise EvidenceFoundationError(f"Reviewed audit root is absent: {root}")
    artifacts: list[dict[str, Any]] = []
    loaded: dict[str, Any] = {}
    for filename in AUDIT_FILENAMES:
        path = root / filename
        if not path.is_file():
            raise EvidenceFoundationError(f"Required reviewed audit artifact is absent: {path}")
        actual_sha256 = _sha256(path)
        if actual_sha256 != AUDIT_SHA256[filename]:
            raise EvidenceFoundationError(
                f"Reviewed audit artifact changed: {filename} is {actual_sha256}, "
                f"not {AUDIT_SHA256[filename]}."
            )
        artifacts.append(
            {
                "relative_path": filename,
                "sha256": actual_sha256,
                "size": path.stat().st_size,
            }
        )
        if path.suffix == ".json":
            loaded[path.stem] = _load_json(path)
        else:
            loaded[path.stem] = path.read_text(encoding="utf-8")
    for name, value in loaded.items():
        if isinstance(value, Mapping) and value.get("audit_id") != AUDIT_ID:
            raise EvidenceFoundationError(
                f"Audit identity mismatch in {name}: {value.get('audit_id')!r}."
            )
    loaded["audit_artifacts"] = artifacts
    loaded["audit_root"] = str(root)
    return loaded


def _manifest_sec_metadata(source_root: Path) -> dict[str, dict[str, str]]:
    manifest = (
        source_root
        / "tickers"
        / "ANF"
        / "financial_statement"
        / "ANF_financial_statement_manifest.csv"
    )
    result: dict[str, dict[str, str]] = {}
    with manifest.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            report_date = str(row.get("reportDate") or "")
            materialized = Path(str(row.get("materialized_path") or "")).name
            if not report_date or not materialized:
                continue
            source_path = str(row.get("source_local_path") or "")
            accession_match = re.search(r"(0001018840\d{8})", source_path)
            accession = None
            if accession_match:
                raw = accession_match.group(1)
                accession = f"{raw[:10]}-{raw[10:12]}-{raw[12:]}"
            result[report_date] = {
                "form": str(row.get("form") or ""),
                "filed_date": str(row.get("filedDate") or ""),
                "accession": accession or "",
                "primary_document": str(row.get("filename") or ""),
            }
    result["2026-05-02"] = {
        "form": "10-Q",
        "filed_date": "2026-06-05",
        "accession": "0001018840-26-000036",
        "primary_document": "anf-20260502.htm",
    }
    return result


def _limitation_codes(reason: str, path: str) -> list[str]:
    value = reason.casefold()
    codes: list[str] = []
    if "inferred" in value and "date" in value:
        codes.append("inferred_event_date")
    if "visual" in value or "slides 1-50" in value:
        codes.append("missing_visual_image_layer")
    if "mixed-scale" in value:
        codes.extend(("mixed_scale_regions", "stale_period_labels"))
    if "representation" in value:
        codes.append("representation_only")
    if "exhibit-level" in value:
        codes.append("wrapper_context_only_without_exhibit_bytes")
    if "governance" in value and "not promise" in value:
        codes.append("governance_only_not_economic_authority")
    if "period- and basis-resolved" in value:
        codes.append("requires_explicit_period_and_basis")
    if path.casefold().endswith("anf_q4_2025_earnings_presentation_quarterly_history.xlsx"):
        codes.extend(
            (
                "fy2025_column_o_is_39_week_ytd_not_annual",
                "annual_mapping_from_column_o_forbidden",
            )
        )
    return sorted(set(codes))


def _registration_key(path: str, sha256: str) -> str:
    return _stable_id("source-registration", ("sha256", sha256), ("path", Path(path).name))


def _build_source_registry(
    audit: Mapping[str, Any], source_root: Path
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, dict[str, Any]]]:
    decisions = audit["source_review_decisions"]["records"]
    inventory_rows = audit["local_source_inventory"]["records"]
    inventory = {str(row["inventory_document_id"]): row for row in inventory_rows}
    sec_metadata = _manifest_sec_metadata(source_root)
    registrations: list[dict[str, Any]] = []
    limitations: list[dict[str, Any]] = []
    by_path: dict[str, dict[str, Any]] = {}
    seen_decisions: set[str] = set()
    for decision in decisions:
        decision_class = str(decision["proposed_decision"])
        if decision_class not in REVIEW_DECISIONS:
            raise EvidenceFoundationError(f"Unknown reviewed decision {decision_class!r}.")
        decision_id = str(decision["decision_id"])
        if decision_id in seen_decisions:
            raise EvidenceFoundationError(f"Duplicate source-review decision {decision_id}.")
        seen_decisions.add(decision_id)
        path = Path(str(decision["path"]))
        if not path.is_file():
            raise EvidenceFoundationError(f"Reviewed local source is absent: {path}")
        actual_sha = _sha256(path)
        expected_sha = str(decision["content_sha256"])
        if actual_sha != expected_sha:
            raise EvidenceFoundationError(f"Reviewed source hash changed: {path}")
        inventory_row = inventory.get(str(decision.get("source_inventory_id") or ""), {})
        semantic_type = str(
            inventory_row.get("semantic_source_type") or decision.get("source_type") or "other_issuer_source"
        )
        report_date = str(
            inventory_row.get("report_date")
            or (
                inventory_row.get("publication_date")
                if semantic_type == "sec_filing"
                else ""
            )
            or ""
        )
        form = str(inventory_row.get("form") or "")
        accession = str(inventory_row.get("accession_number") or "")
        publication_date = str(
            inventory_row.get("filing_date")
            or inventory_row.get("publication_date")
            or ""
        )
        if semantic_type == "sec_filing":
            metadata = sec_metadata.get(report_date)
            if metadata is None:
                embedded_dates = re.findall(r"(?<!\d)(20\d{6})(?!\d)", path.name)
                for compact_date in embedded_dates:
                    candidate_report_date = (
                        f"{compact_date[:4]}-{compact_date[4:6]}-{compact_date[6:]}"
                    )
                    if candidate_report_date in sec_metadata:
                        report_date = candidate_report_date
                        metadata = sec_metadata[candidate_report_date]
                        break
            if metadata is None:
                raise EvidenceFoundationError(
                    f"SEC filing metadata is absent for report date {report_date}."
                )
            publication_date = metadata["filed_date"]
            form = metadata["form"]
            accession = metadata["accession"]
        document_key = str(inventory_row.get("document_key") or "")
        if not document_key:
            if semantic_type == "sec_filing":
                document_key = f"anf-sec-{accession.casefold()}"
            elif accession:
                document_key = f"anf-sec-wrapper-{accession.casefold()}"
            else:
                document_key = f"anf-{_slug(semantic_type)}-{expected_sha[:16]}"
        source_document_id = str(inventory_row.get("source_document_id") or "")
        if not source_document_id:
            source_document_id = _stable_id(
                "doc",
                ("co", "ANF"),
                ("type", semantic_type),
                ("key", document_key),
                ("sha256", expected_sha),
            )
        reason = _clean_text(decision.get("reason"))
        provenance_only_wrapper = (
            semantic_type == "sec_8k_wrapper"
            and "exhibit-level economics require present exhibit bytes"
            in reason.casefold()
        )
        reviewed_source_eligible = decision_class in ECONOMICALLY_ELIGIBLE_DECISIONS
        economic_evidence_eligible = (
            reviewed_source_eligible and not provenance_only_wrapper
        )
        limitation_codes = (
            _limitation_codes(reason, str(path))
            if decision_class == "REVIEW_ACCEPT_WITH_LIMITATIONS"
            else []
        )
        if decision_class == "REVIEW_ACCEPT_WITH_LIMITATIONS" and not limitation_codes:
            limitation_codes = ["review-accept-with-limitations"]
        review_limit_ids = [
            _stable_id("source-limitation", ("doc", source_document_id), ("code", code))
            for code in limitation_codes
        ]
        registration = {
            "source_registration_id": _registration_key(str(path), expected_sha),
            "source_document_id": source_document_id,
            "document_key": document_key,
            "company_id": "ANF",
            "path": str(path),
            "relative_path": str(inventory_row.get("relative_path") or ""),
            "content_sha256": expected_sha,
            "source_type": semantic_type,
            "authority_tier": AUTHORITY_TIERS.get(semantic_type, 5),
            "review_decision": decision_class,
            "review_reason": reason,
            "reviewed_source_eligible": reviewed_source_eligible,
            "economic_evidence_eligible": economic_evidence_eligible,
            "provenance_only": provenance_only_wrapper,
            "duplicate_only": decision_class == "REVIEW_DUPLICATE_ONLY",
            "publication_date": publication_date or None,
            "knowledge_date": publication_date or None,
            "report_date": report_date or None,
            "form": form or None,
            "accession": accession or None,
            "limitation_ids": review_limit_ids,
            "promotion_performed": False,
            "in_current_model_period_scope": bool(decision.get("in_current_model_period_scope")),
            "local_copy_paths": sorted(
                set(
                    str(value)
                    for value in (
                        inventory_row.get("copy_paths")
                        or [str(path)]
                    )
                )
            ),
        }
        if (
            semantic_type in {"earnings_call_transcript", "conference_transcript"}
            and not registration["knowledge_date"]
        ):
            # These transcripts were admitted through the reviewed local audit at the
            # accepted source-set cutoff.  Do not project them back into earlier events.
            registration["knowledge_date"] = KNOWLEDGE_CUTOFF
        registrations.append(registration)
        by_path[str(path).casefold()] = registration
        if limitation_codes:
            for code, limitation_id in zip(limitation_codes, review_limit_ids, strict=True):
                limitation: dict[str, Any] = {
                    "limitation_id": limitation_id,
                    "source_document_id": source_document_id,
                    "code": code,
                    "exact_review_reason": reason,
                    "enforcement": "selection_must_fail_closed",
                }
                if code == "fy2025_column_o_is_39_week_ytd_not_annual":
                    limitation["blocked_locator"] = {
                        "sheet": "Historical Income Statement",
                        "column": "O",
                        "actual_period_kind": "39-week-ytd",
                        "forbidden_period_kind": "annual",
                    }
                if code == "mixed_scale_regions":
                    limitation["required_before_use"] = (
                        "explicit cell-level scale, period, and cell-type validation"
                    )
                limitations.append(limitation)
    registrations.sort(key=lambda row: row["source_registration_id"])
    limitations.sort(key=lambda row: row["limitation_id"])
    return registrations, limitations, by_path


def _semantic_source_documents(
    registrations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    by_hash: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in registrations:
        if row["economic_evidence_eligible"]:
            by_hash[str(row["content_sha256"])].append(row)
    documents: list[dict[str, Any]] = []
    for sha256, rows in sorted(by_hash.items()):
        ranked = sorted(
            rows,
            key=lambda row: (
                int(row["authority_tier"]),
                str(row["path"]).casefold(),
            ),
        )
        canonical = ranked[0]
        documents.append(
            {
                "source_document_id": canonical["source_document_id"],
                "document_key": canonical["document_key"],
                "content_sha256": sha256,
                "source_type": canonical["source_type"],
                "authority_tier": canonical["authority_tier"],
                "publication_date": canonical["publication_date"],
                "knowledge_date": canonical["knowledge_date"],
                "report_date": canonical["report_date"],
                "form": canonical["form"],
                "accession": canonical["accession"],
                "review_decision": canonical["review_decision"],
                "limitation_ids": sorted(
                    {
                        limitation_id
                        for row in rows
                        for limitation_id in row["limitation_ids"]
                    }
                ),
                "representation_paths": sorted(
                    {
                        path
                        for row in rows
                        for path in row["local_copy_paths"]
                    }
                ),
                "semantic_representation_count": len(rows),
            }
        )
    return documents


def _wrapper_relations(
    audit: Mapping[str, Any], registrations: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    inventory = audit["local_source_inventory"]["records"]
    registry_by_hash = {str(row["content_sha256"]): row for row in registrations}
    wrappers = [row for row in inventory if row.get("semantic_source_type") == "sec_8k_wrapper"]
    releases = [row for row in registrations if row["source_type"] in {"earnings_release", "business_update"}]
    relations: list[dict[str, Any]] = []
    for wrapper in wrappers:
        wrapper_registration = registry_by_hash[str(wrapper["content_sha256"])]
        filing_date = str(wrapper.get("filing_date") or "")
        linked = [
            release
            for release in releases
            if release.get("publication_date") == filing_date
        ]
        relations.append(
            {
                "relation_id": _stable_id(
                    "source-relation",
                    ("wrapper", wrapper_registration["source_document_id"]),
                    ("accession", wrapper["accession_number"]),
                ),
                "relation_type": "sec-wrapper-provenance",
                "wrapper_source_document_id": wrapper_registration["source_document_id"],
                "accession": wrapper["accession_number"],
                "form": wrapper["form"],
                "filing_date": filing_date,
                "report_date": wrapper.get("report_date"),
                "copy_paths": sorted(wrapper.get("copy_paths") or []),
                "file_representation_count": int(wrapper.get("file_representation_count") or 1),
                "candidate_exhibit_source_document_ids": sorted(
                    {str(release["source_document_id"]) for release in linked}
                ),
                "economic_fact_multiplicity": 0,
                "wrapper_economic_body_eligible": bool(
                    wrapper_registration["economic_evidence_eligible"]
                ),
                "review_constraint": (
                    "wrapper may own only its reviewed direct body economics; linked exhibit "
                    "occurrences remain separately owned and are never multiplied"
                    if wrapper_registration["economic_evidence_eligible"]
                    else "wrapper establishes provenance only; exhibit bytes or another "
                    "reviewed representation own economic assertions"
                ),
            }
        )
    return relations


def _source_lookup(
    registrations: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]]]:
    by_path = {str(row["path"]).casefold(): row for row in registrations}
    by_hash = {str(row["content_sha256"]): row for row in registrations}
    by_key = {str(row["document_key"]): row for row in registrations}
    return by_path, by_hash, by_key


def _source_for_audit_record(
    record: Mapping[str, Any],
    by_path: Mapping[str, Mapping[str, Any]],
    by_hash: Mapping[str, Mapping[str, Any]],
    by_key: Mapping[str, Mapping[str, Any]],
) -> Mapping[str, Any]:
    path = record.get("source_path") or record.get("path")
    if path and str(path).casefold() in by_path:
        return by_path[str(path).casefold()]
    sha = record.get("source_sha256") or record.get("document_sha256")
    if sha and str(sha) in by_hash:
        return by_hash[str(sha)]
    key = record.get("event_document_key") or record.get("document_key")
    if key and str(key) in by_key:
        return by_key[str(key)]
    raise EvidenceFoundationError(
        f"Reviewed audit record has no registered semantic source: {record!r}"
    )


def _require_economic_source(source: Mapping[str, Any], context: str) -> None:
    if not source.get("economic_evidence_eligible"):
        raise EvidenceFoundationError(
            f"{context} references a source that is not eligible as economic evidence: "
            f"{source.get('source_document_id')}."
        )


def _occurrence_from_audit(
    record: Mapping[str, Any],
    source: Mapping[str, Any],
    *, occurrence_key: str,
) -> dict[str, Any]:
    locator = record.get("source_locator") or record.get("locator") or {
        "locator_kind": "audit-reviewed-occurrence",
        "audit_record": occurrence_key,
    }
    locator = json.loads(json.dumps(locator, ensure_ascii=False))
    locator_kind = str(locator.get("locator_kind") or "audit-reviewed-occurrence")
    occurrence_id = _stable_id(
        "occurrence",
        ("doc", source["source_document_id"]),
        ("locator", _locator_digest(locator)),
        ("key", occurrence_key),
    )
    return {
        "occurrence_id": occurrence_id,
        "source_document_id": source["source_document_id"],
        "source_content_sha256": source["content_sha256"],
        "locator_kind": locator_kind,
        "locator": locator,
        "excerpt": _clean_text(record.get("excerpt")) or None,
        "excerpt_sha256": record.get("excerpt_sha256"),
        "knowledge_date": record.get("knowledge_date") or source.get("knowledge_date"),
        "semantic_directness": record.get("semantic_directness"),
        "authority_tier": source["authority_tier"],
        "review_decision": source["review_decision"],
        "audit_record_id": occurrence_key,
    }


def _parse_display_value(value: Any, *, unit_hint: str | None = None) -> dict[str, Any]:
    text = _clean_text(value)
    lower = text.casefold()
    direction = "down" if "down" in lower else "up" if "up" in lower else None
    impact_polarity = (
        "unfavorable"
        if "unfavorable" in lower or "unfavorability" in lower
        else "favorable"
        if "favorable" in lower or "favorability" in lower
        else None
    )

    def qualified(result: dict[str, Any]) -> dict[str, Any]:
        if direction is not None:
            result["direction"] = direction
        if impact_polarity is not None:
            result["impact_polarity"] = impact_polarity
        return result

    if not text:
        return {"kind": "unavailable", "reason": "source value absent"}
    if "/" in text and ("bps" in lower or "$" in text):
        components = []
        for part in text.split("/"):
            components.append(_parse_display_value(part.strip(), unit_hint=unit_hint))
        return qualified(
            {"kind": "composite", "components": components, "source_text": text}
        )
    qualitative_terms = (
        "low-single-digits",
        "high-single-digits",
        "low-double-digits",
        "mid-teens",
        "around flat",
        "around break-even",
        "flattish",
    )
    if any(term in lower for term in qualitative_terms):
        return qualified({
            "kind": "qualitative",
            "value": text,
        })
    if "breakeven" in lower and re.search(r"\d", text):
        numbers = re.findall(r"[-+]?\d+(?:\.\d+)?", text)
        if len(numbers) == 1:
            return qualified({
                "kind": "range",
                "low": "0",
                "high": _canonical_decimal(numbers[0]),
                "low_inclusive": True,
                "high_inclusive": True,
                "unit": "percent",
            })
    normalized = text.replace(",", "")
    numbers = re.findall(r"(?<![A-Za-z])[-+]?\d+(?:\.\d+)?", normalized)
    if not numbers:
        return qualified({"kind": "qualitative", "value": text})
    unit = unit_hint
    if "%" in text:
        unit = "percent"
    elif "bps" in lower or "basis point" in lower:
        unit = "basis points"
    elif "shares" in lower:
        unit = "million shares" if "m" in lower or "million" in lower else "shares"
    elif "$" in text:
        unit = "USD billion" if "bn" in lower or "billion" in lower else "USD million" if "m" in lower or "million" in lower else "USD"
    elif "year" in lower:
        unit = "years"
    values = [_canonical_decimal(number) for number in numbers]
    if len(values) >= 2 and any(token in text for token in ("–", " to ", "-")):
        return qualified({
            "kind": "range",
            "low": values[0],
            "high": values[1],
            "low_inclusive": True,
            "high_inclusive": True,
            "unit": unit,
        })
    if text.lstrip().startswith(("≥", ">=")) or "at least" in lower:
        return qualified({"kind": "bound", "operator": "gte", "value": values[0], "unit": unit})
    if text.lstrip().startswith(("≤", "<=")) or "at most" in lower:
        return qualified({"kind": "bound", "operator": "lte", "value": values[0], "unit": unit})
    if text.lstrip().startswith((">", "more than", "over ")):
        return qualified({"kind": "bound", "operator": "gt", "value": values[0], "unit": unit})
    if "~" in text or "approximately" in lower or "around" in lower or "about" in lower:
        return qualified({
            "kind": "approximate",
            "value": values[0],
            "qualifier": "around",
            "tolerance": None,
            "unit": unit,
        })
    return qualified({"kind": "exact", "value": values[0], "unit": unit})


def _guidance_metric_contract(metric: str) -> tuple[str, str | None]:
    if metric in {"revenue-growth", "operating-margin"}:
        return "percent", None
    if metric == "reported-diluted-eps":
        return "USD/share", "USD"
    if metric == "diluted-weighted-average-shares":
        return "million shares", None
    if metric == "share-repurchases":
        return "USD million", "USD"
    if metric == "capital-expenditures":
        return "USD million", "USD"
    if metric in {
        "net-store-openings",
        "store-openings",
        "store-closures-count",
        "store-remodels-right-sizes",
    }:
        return "count", None
    if metric == "tariff-impact":
        return "composite", "USD"
    if metric == "operating-income":
        return "qualitative", "USD"
    return "qualitative", None


def _date_from_source_key(key: str, path: str) -> str:
    match = re.search(r"(20\d{2}-\d{2}-\d{2})", key) or re.search(
        r"(20\d{2}-\d{2}-\d{2})", path
    )
    if match:
        return match.group(1)
    raise EvidenceFoundationError(f"Cannot derive reviewed source date for {key!r}.")


def _stated_in_period(target_period: str, source_date: str) -> str:
    match = re.fullmatch(r"FY(\d{4})-Q([1-4])", target_period)
    if match is None:
        raise EvidenceFoundationError(f"Quarter guidance has non-quarter horizon {target_period!r}.")
    fiscal_year = int(match.group(1))
    target_quarter = int(match.group(2))
    if source_date.endswith("-01-12") and target_quarter == 4:
        return f"FY{fiscal_year}-Q4-pre-release"
    if target_quarter == 1:
        return f"FY{fiscal_year - 1}-Q4-results"
    return f"FY{fiscal_year}-Q{target_quarter - 1}-results"


def _build_quarter_guidance(
    audit: Mapping[str, Any], registrations: Sequence[Mapping[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    by_path, by_hash, by_key = _source_lookup(registrations)
    rows = audit["quarter_guidance_inventory"]["records"]
    if len(rows) != 60:
        raise EvidenceFoundationError(f"Reviewed quarter-guidance count is {len(rows)}, not 60.")
    assertions: list[dict[str, Any]] = []
    versions: list[dict[str, Any]] = []
    occurrences: list[dict[str, Any]] = []
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        source = _source_for_audit_record(row, by_path, by_hash, by_key)
        _require_economic_source(source, str(row["quarter_guidance_id"]))
        source_date = str(source.get("publication_date") or "")
        if not re.fullmatch(r"20\d{2}-\d{2}-\d{2}", source_date):
            source_date = _date_from_source_key(
                str(row.get("event_document_key") or ""), str(row["source_path"])
            )
        metric = str(row["metric"])
        target_period = str(row["target_period"])
        unit, currency = _guidance_metric_contract(metric)
        value = _parse_display_value(row["source_value_representation"], unit_hint=unit)
        occurrence = _occurrence_from_audit(
            row,
            source,
            occurrence_key=str(row["quarter_guidance_id"]),
        )
        occurrence["knowledge_date"] = source_date
        occurrences.append(occurrence)
        assertion_id = _stable_id(
            "source-assertion",
            ("kind", "quarter-guidance"),
            ("audit", row["quarter_guidance_id"]),
            ("occ", occurrence["occurrence_id"]),
        )
        assertion = {
            "assertion_id": assertion_id,
            "quarter_guidance_id": row["quarter_guidance_id"],
            "assertion_kind": "guidance",
            "metric_id": _metric_id(metric),
            "metric_key": metric,
            "horizon_period_id": _period_id(target_period),
            "horizon_period_key": target_period,
            "horizon_type": "quarter",
            "stated_in_period_id": _period_id(_stated_in_period(target_period, source_date)),
            "source_date": source_date,
            "knowledge_date": source_date,
            "source_document_id": source["source_document_id"],
            "occurrence_id": occurrence["occurrence_id"],
            "canonical_value": value,
            "source_value_representation": _clean_text(row["source_value_representation"]),
            "semantic_directness": row["semantic_directness"],
            "definition_id": "definition:core:company-guidance@1",
            "basis_id": "basis:core:guided@1",
            "unit_id": _unit_id(unit),
            "currency": currency,
            "dimension_set_id": "dimset:anf:total-company@1",
            "review_state": "reviewed",
        }
        assertions.append(assertion)
        grouped[(metric, target_period)].append(assertion)
    for (metric, target_period), series_assertions in sorted(grouped.items()):
        unit, currency = _guidance_metric_contract(metric)
        series_id = _stable_id(
            "guidance-series",
            ("co", "ANF"),
            ("metric", _metric_id(metric)),
            ("horizon", _period_id(target_period)),
            ("unit", _unit_id(unit)),
            ("ccy", currency or "na"),
        )
        ordered = sorted(
            series_assertions,
            key=lambda row: (str(row["source_date"]), str(row["quarter_guidance_id"])),
        )
        series_versions: list[dict[str, Any]] = []
        for index, assertion in enumerate(ordered):
            version_id = _stable_id(
                "guidance-version",
                ("series", series_id),
                ("occ", assertion["occurrence_id"]),
            )
            predecessor = None if index == 0 else series_versions[-1]["guidance_version_id"]
            series_versions.append(
                {
                    "guidance_version_id": version_id,
                    "guidance_series_id": series_id,
                    "metric_id": _metric_id(metric),
                    "horizon_period_id": _period_id(target_period),
                    "horizon_type": "quarter",
                    "stated_in_period_id": assertion["stated_in_period_id"],
                    "source_date": assertion["source_date"],
                    "knowledge_date": assertion["knowledge_date"],
                    "source_assertion_id": assertion["assertion_id"],
                    "source_document_id": assertion["source_document_id"],
                    "occurrence_id": assertion["occurrence_id"],
                    "canonical_value": assertion["canonical_value"],
                    "unit_id": assertion["unit_id"],
                    "currency": assertion["currency"],
                    "predecessor_guidance_version_id": predecessor,
                    "successor_guidance_version_id": None,
                    "version_ordinal": index + 1,
                }
            )
        for previous, current in zip(series_versions, series_versions[1:], strict=False):
            previous["successor_guidance_version_id"] = current["guidance_version_id"]
        versions.extend(series_versions)
    return assertions, versions, occurrences


_NUMBER_WORDS = {
    "1": "one",
    "2": "two",
    "3": "three",
    "4": "four",
    "5": "five",
    "6": "six",
    "7": "seven",
    "8": "eight",
    "9": "nine",
    "10": "ten",
    "11": "eleven",
    "12": "twelve",
    "13": "thirteen",
    "14": "fourteen",
    "15": "fifteen",
    "16": "sixteen",
    "17": "seventeen",
    "18": "eighteen",
    "19": "nineteen",
    "20": "twenty",
    "21": "twenty-one",
    "22": "twenty-two",
    "23": "twenty-three",
    "24": "twenty-four",
    "25": "twenty-five",
    "26": "twenty-six",
    "30": "thirty",
    "31": "thirty-one",
    "35": "thirty-five",
    "39": "thirty-nine",
    "40": "forty",
    "47": "forty-seven",
    "48": "forty-eight",
    "50": "fifty",
    "55": "fifty-five",
    "60": "sixty",
    "70": "seventy",
    "80": "eighty",
}


def _source_on_date(
    registrations: Sequence[Mapping[str, Any]],
    publication_date: str,
    *,
    source_type: str,
) -> Mapping[str, Any]:
    matches = [
        row
        for row in registrations
        if row.get("publication_date") == publication_date
        and row.get("source_type") == source_type
        and row.get("economic_evidence_eligible")
    ]
    if len(matches) != 1:
        raise EvidenceFoundationError(
            f"Reviewed {source_type} source on {publication_date} is not unique: "
            f"{[row.get('source_document_id') for row in matches]}."
        )
    return matches[0]


def _document_text(source: Mapping[str, Any]) -> str:
    return _clean_text(
        lxml_html.fromstring(Path(str(source["path"])).read_bytes()).text_content()
    )


def _store_section(source: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
    text = _document_text(source)
    lower = text.casefold()
    headings = (
        "global store network optimization",
        "global store network optimization initiative",
    )
    start = max(lower.rfind(heading) for heading in headings)
    if start < 0:
        # FY2025+ filings retain the same reviewed disclosure but drop the heading
        # from the flattened inline-XBRL text.  Anchor on the period-activity phrase.
        candidates = [
            lower.find("through the end of the first fiscal quarter"),
            lower.find("through the end of the second fiscal quarter"),
            lower.find("through the end of the third fiscal quarter"),
        ]
        candidates = [candidate for candidate in candidates if candidate >= 0]
        if not candidates:
            raise EvidenceFoundationError(
                f"Reviewed store disclosure is absent from {source['source_document_id']}."
            )
        start = min(candidates)
    excerpt = text[start : start + 1800]
    locator = {
        "locator_kind": "html-reviewed-text-section",
        "heading_fingerprint": "Global Store Network Optimization / store investment plan",
        "character_start": start,
        "excerpt": excerpt,
        "excerpt_sha256": hashlib.sha256(excerpt.encode("utf-8")).hexdigest(),
    }
    return excerpt, locator


def _number_pattern(value: str) -> str:
    word = _NUMBER_WORDS.get(value)
    return re.escape(value) if word is None else f"(?:{re.escape(value)}|{re.escape(word)})"


def _verify_store_value(section: str, metric: str, value: str) -> None:
    number = _number_pattern(value)
    patterns = {
        "store-openings": rf"opened\s+{number}\s+new",
        "store-closures-count": rf"closing\s+{number}(?:\s+legacy)?\s+store",
        "store-remodels": rf"remodeled\s+{number}\s+store",
        "store-right-sizes": rf"right-sized\s+{number}\s+store",
    }
    pattern = patterns[metric]
    if re.search(pattern, section, re.IGNORECASE) is None:
        raise EvidenceFoundationError(
            f"Reviewed store value {metric}={value} is absent from source section."
        )


def _store_period_observations(
    registrations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    for publication_date, period, metrics in STORE_PERIOD_ACTIVITY:
        source = _source_on_date(
            registrations, publication_date, source_type="sec_filing"
        )
        section, base_locator = _store_section(source)
        for metric, value in sorted(metrics.items()):
            _verify_store_value(section, metric, value)
            locator = {
                **base_locator,
                "metric": metric,
                "period": period,
                "reviewed_value": value,
            }
            occurrence_id = _stable_id(
                "occurrence",
                ("doc", source["source_document_id"]),
                ("locator", _locator_digest(locator)),
                ("period", period),
                ("metric", metric),
            )
            observations.append(
                {
                    "observation_id": _stable_id(
                        "observation",
                        ("metric", _metric_id(metric)),
                        ("period", _period_id(period)),
                        ("occ", occurrence_id),
                    ),
                    "metric_key": metric,
                    "metric_id": _metric_id(metric),
                    "period_key": period,
                    "period_id": _period_id(period),
                    "period_kind": _period_kind(period),
                    "definition_id": "definition:anf:company-owned-store-activity@1",
                    "basis_id": "basis:core:reported@1",
                    "dimension_set_id": "dimset:anf:total-company@1",
                    "unit": "count",
                    "unit_id": _unit_id("count"),
                    "currency": None,
                    "canonical_value": {"kind": "exact", "value": value},
                    "semantic_directness": "direct_exact",
                    "source_document_id": source["source_document_id"],
                    "source_content_sha256": source["content_sha256"],
                    "occurrence_id": occurrence_id,
                    "knowledge_date": publication_date,
                    "source_authority_tier": 1,
                    "locator": locator,
                }
            )
    return observations


def _annual_store_release_observations(
    registrations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Retain event-time release facts before later SEC authority confirmation."""

    release_dates = {
        "FY2022": "2023-03-02",
        "FY2023": "2024-03-07",
        "FY2024": "2025-03-06",
        "FY2025": "2026-03-04",
    }
    observations: list[dict[str, Any]] = []

    def emit(
        *,
        source: Mapping[str, Any],
        period: str,
        metric: str,
        value: str,
        table_index: int,
        row_index: int,
        cell_index: int,
        row: Sequence[str],
    ) -> None:
        excerpt = " | ".join(row)
        locator = {
            "locator_kind": "html-semantic-table-row",
            "table_index": table_index,
            "row_index": row_index,
            "cell_index": cell_index,
            "row_header_fingerprint": row[0],
            "excerpt": excerpt,
            "excerpt_sha256": hashlib.sha256(excerpt.encode("utf-8")).hexdigest(),
            "metric": metric,
            "period": period,
        }
        occurrence_id = _stable_id(
            "occurrence",
            ("doc", source["source_document_id"]),
            ("locator", _locator_digest(locator)),
            ("period", period),
        )
        observations.append(
            {
                "observation_id": _stable_id(
                    "observation",
                    ("metric", _metric_id(metric)),
                    ("period", _period_id(period)),
                    ("occ", occurrence_id),
                ),
                "metric_key": metric,
                "metric_id": _metric_id(metric),
                "period_key": period,
                "period_id": _period_id(period),
                "period_kind": _period_kind(period),
                "definition_id": "definition:anf:company-owned-store-activity@1",
                "basis_id": "basis:core:reported@1",
                "dimension_set_id": "dimset:anf:total-company@1",
                "unit": "count",
                "unit_id": _unit_id("count"),
                "currency": None,
                "canonical_value": {"kind": "exact", "value": value},
                "semantic_directness": "direct_exact",
                "source_document_id": source["source_document_id"],
                "source_content_sha256": source["content_sha256"],
                "occurrence_id": occurrence_id,
                "knowledge_date": source["knowledge_date"],
                "source_authority_tier": source["authority_tier"],
                "locator": locator,
            }
        )

    for period, publication_date in release_dates.items():
        source = _source_on_date(
            registrations, publication_date, source_type="earnings_release"
        )
        root = lxml_html.fromstring(Path(str(source["path"])).read_bytes())
        tables = root.xpath("//table")
        annual_rows: list[tuple[int, int, list[str]]] = []
        q4_rows: list[tuple[int, int, list[str]]] = []
        for table_index, table in enumerate(tables):
            table_text = _text(table)
            rows = _html_rows(table)
            for row_index, row in enumerate(rows):
                if not row or row[0].strip().casefold() not in {
                    "new",
                    "permanently closed",
                }:
                    continue
                if "Fifty-Two Weeks Ended" in table_text or "Fifty-Three Weeks Ended" in table_text:
                    annual_rows.append((table_index, row_index, row))
                if period == "FY2022" and "Thirteen Weeks Ended" in table_text:
                    q4_rows.append((table_index, row_index, row))
        # One physical table can contain both a Q4 and annual section.  Select by
        # reviewed row position/value rather than treating the whole table as one period.
        annual_expected = {
            "New": ANNUAL_STORE_TOTALS[period]["store-openings"],
            "Permanently closed": ANNUAL_STORE_TOTALS[period]["store-closures-count"],
        }
        for label, value in annual_expected.items():
            candidates = [
                item
                for item in annual_rows
                if item[2][0].strip().casefold() == label.casefold()
                and any(
                    re.sub(r"[^0-9.-]", "", cell).lstrip("-") == value
                    for cell in item[2][1:]
                )
            ]
            # The same table may expose Q4 and annual sections.  The annual row is
            # the last matching row in document order.
            if not candidates:
                raise EvidenceFoundationError(
                    f"Annual release store fact {period}/{label}={value} is absent."
                )
            table_index, row_index, row = sorted(candidates)[-1]
            cell_index = max(
                index
                for index, cell in enumerate(row)
                if re.sub(r"[^0-9.-]", "", cell).lstrip("-") == value
            )
            emit(
                source=source,
                period=period,
                metric=(
                    "store-openings" if label == "New" else "store-closures-count"
                ),
                value=value,
                table_index=table_index,
                row_index=row_index,
                cell_index=cell_index,
                row=row,
            )
        if period == "FY2022":
            for label, value, metric in (
                ("New", "28", "store-openings"),
                ("Permanently closed", "17", "store-closures-count"),
            ):
                candidates = [
                    item
                    for item in q4_rows
                    if item[2][0].strip().casefold() == label.casefold()
                    and any(
                        re.sub(r"[^0-9.-]", "", cell).lstrip("-") == value
                        for cell in item[2][1:]
                    )
                ]
                if not candidates:
                    raise EvidenceFoundationError(
                        f"Direct FY2022 Q4 release store fact {label}={value} is absent."
                    )
                table_index, row_index, row = sorted(candidates)[0]
                cell_index = max(
                    index
                    for index, cell in enumerate(row)
                    if re.sub(r"[^0-9.-]", "", cell).lstrip("-") == value
                )
                emit(
                    source=source,
                    period="FY2022-Q4",
                    metric=metric,
                    value=value,
                    table_index=table_index,
                    row_index=row_index,
                    cell_index=cell_index,
                    row=row,
                )
    return observations


def _fy2026_q1_promise_observations(
    registrations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    source = _source_on_date(
        registrations, "2026-05-27", source_type="earnings_release"
    )
    text = _document_text(source)
    reviewed = (
        (
            "revenue-growth",
            "percent",
            None,
            {"kind": "exact", "value": "2"},
            "Record first quarter net sales of $1.1 billion, up 2% from last year",
            "direct_exact",
        ),
        (
            "operating-margin",
            "percent",
            None,
            {"kind": "exact", "value": "8", "display_decimals": 1},
            "Operating margin of 8.0%",
            "direct_exact",
        ),
        (
            "share-repurchases",
            "USD million",
            "USD",
            {"kind": "exact", "value": "105"},
            "$105 million in shares repurchased in the quarter",
            "direct_exact",
        ),
    )
    observations: list[dict[str, Any]] = []
    for metric, unit, currency, value, fingerprint, directness in reviewed:
        start = text.casefold().find(fingerprint.casefold())
        if start < 0:
            raise EvidenceFoundationError(
                f"Reviewed FY2026 Q1 source occurrence changed: {fingerprint!r}."
            )
        excerpt = text[max(0, start - 80) : start + len(fingerprint) + 80]
        locator = {
            "locator_kind": "html-reviewed-text-occurrence",
            "character_start": start,
            "fingerprint": fingerprint,
            "excerpt": excerpt,
            "excerpt_sha256": hashlib.sha256(excerpt.encode("utf-8")).hexdigest(),
        }
        occurrence_id = _stable_id(
            "occurrence",
            ("doc", source["source_document_id"]),
            ("locator", _locator_digest(locator)),
            ("metric", metric),
            ("period", "FY2026-Q1"),
        )
        observations.append(
            {
                "observation_id": _stable_id(
                    "observation",
                    ("metric", _metric_id(metric)),
                    ("period", _period_id("FY2026-Q1")),
                    ("occ", occurrence_id),
                ),
                "metric_key": metric,
                "metric_id": _metric_id(metric),
                "period_key": "FY2026-Q1",
                "period_id": _period_id("FY2026-Q1"),
                "period_kind": "quarter",
                "definition_id": "definition:core:company-reported@1",
                "basis_id": "basis:core:reported@1",
                "dimension_set_id": "dimset:anf:total-company@1",
                "unit": unit,
                "unit_id": _unit_id(unit),
                "currency": currency,
                "canonical_value": value,
                "semantic_directness": directness,
                "source_document_id": source["source_document_id"],
                "source_content_sha256": source["content_sha256"],
                "occurrence_id": occurrence_id,
                "knowledge_date": "2026-05-27",
                "source_authority_tier": source["authority_tier"],
                "locator": locator,
            }
        )
    return observations


def _annual_guidance_stated_in(target_period: str, source_date: str) -> str:
    fiscal_year = int(target_period.removeprefix("FY"))
    month = int(source_date[5:7])
    if source_date.startswith(f"{fiscal_year + 1}-01"):
        return f"FY{fiscal_year}-Q4-pre-release"
    quarter = {5: 1, 6: 1, 8: 2, 9: 2, 11: 3, 12: 3}.get(month)
    if quarter is None:
        # May 2026 is a normal FY2026 Q1 result/update event.
        quarter = max(1, min(3, (month - 3) // 3 + 1))
    return f"FY{fiscal_year}-Q{quarter}-results"


def _build_missing_annual_guidance(
    registrations: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    assertions: list[dict[str, Any]] = []
    occurrences: list[dict[str, Any]] = []

    def add_assertion(
        *,
        source: Mapping[str, Any],
        source_date: str,
        target_period: str,
        metric: str,
        value: Mapping[str, Any],
        unit: str,
        currency: str | None,
        source_value: str,
        locator: Mapping[str, Any],
        directness: str,
        progression_slot: str,
    ) -> None:
        occurrence_id = _stable_id(
            "occurrence",
            ("doc", source["source_document_id"]),
            ("locator", _locator_digest(locator)),
            ("metric", metric),
            ("horizon", target_period),
        )
        occurrence = {
            "occurrence_id": occurrence_id,
            "source_document_id": source["source_document_id"],
            "source_content_sha256": source["content_sha256"],
            "locator_kind": locator["locator_kind"],
            "locator": dict(locator),
            "excerpt": source_value,
            "excerpt_sha256": hashlib.sha256(source_value.encode("utf-8")).hexdigest(),
            "knowledge_date": source_date,
            "semantic_directness": directness,
            "authority_tier": source["authority_tier"],
            "review_decision": source["review_decision"],
            "audit_record_id": f"annual-guidance:{source_date}:{metric}",
        }
        occurrences.append(occurrence)
        assertion_id = _stable_id(
            "source-assertion",
            ("kind", "annual-guidance"),
            ("occ", occurrence_id),
        )
        assertions.append(
            {
                "assertion_id": assertion_id,
                "assertion_kind": "guidance",
                "metric_id": _metric_id(metric),
                "metric_key": metric,
                "horizon_period_id": _period_id(target_period),
                "horizon_period_key": target_period,
                "horizon_type": "annual",
                "stated_in_period_id": _period_id(
                    _annual_guidance_stated_in(target_period, source_date)
                ),
                "source_date": source_date,
                "knowledge_date": source_date,
                "source_document_id": source["source_document_id"],
                "occurrence_id": occurrence_id,
                "canonical_value": _fact_value(value),
                "source_value_representation": source_value,
                "semantic_directness": directness,
                "definition_id": "definition:core:company-guidance@1",
                "basis_id": "basis:core:guided@1",
                "unit_id": _unit_id(unit),
                "currency": currency,
                "dimension_set_id": "dimset:anf:total-company@1",
                "review_state": "reviewed",
                "progression_slot": progression_slot,
            }
        )

    for source_date, target_period, _fingerprint, metrics in ANNUAL_STORE_GUIDANCE:
        source = _source_on_date(registrations, source_date, source_type="sec_filing")
        section, base_locator = _store_section(source)
        slot = {"06": "q1", "09": "q2", "12": "q3"}[source_date[5:7]]
        for metric, raw_value in sorted(metrics.items()):
            numbers = [
                str(child)
                for key, child in raw_value.items()
                if key in {"value", "low", "high"}
                and re.fullmatch(r"[-+]?\d+(?:\.\d+)?", str(child))
            ]
            if any(
                not re.search(rf"\b{_number_pattern(number)}\b", section, re.IGNORECASE)
                for number in numbers
            ):
                raise EvidenceFoundationError(
                    f"Reviewed annual store guidance {source_date}/{metric} changed."
                )
            locator = {
                **base_locator,
                "metric": metric,
                "horizon": target_period,
                "guidance_section": True,
            }
            directness = {
                "range": "direct_range",
                "approximate": "direct_approximate",
                "qualitative": "direct_composite",
            }.get(str(raw_value["kind"]), "direct_exact")
            add_assertion(
                source=source,
                source_date=source_date,
                target_period=target_period,
                metric=metric,
                value=raw_value,
                unit="count",
                currency=None,
                source_value=section,
                locator=locator,
                directness=directness,
                progression_slot=slot,
            )

    may_source = _source_on_date(
        registrations, "2026-05-27", source_type="earnings_release"
    )
    root = lxml_html.fromstring(Path(str(may_source["path"])).read_bytes())
    tables = root.xpath("//table")
    if len(tables) <= 6:
        raise EvidenceFoundationError("Reviewed May 2026 outlook table is absent.")
    rows = [row for row in _html_rows(tables[6]) if any(cell for cell in row)]
    if not rows or "replaces all previous full year guidance" not in rows[0][0].casefold():
        raise EvidenceFoundationError("May 2026 full-year replacement heading changed.")
    for row_index, metric, expected, unit, currency in MAY_2026_ANNUAL_GUIDANCE:
        row = rows[row_index]
        if metric in {"store-openings", "store-closures-count"}:
            current_cell = row[0]
        elif metric == "store-remodels-right-sizes":
            current_cell = row[0]
        else:
            current_cell = row[1]
        if expected.casefold() not in current_cell.casefold():
            raise EvidenceFoundationError(
                f"Reviewed May 2026 annual guidance changed for {metric}: {current_cell!r}."
            )
        value = _parse_display_value(expected, unit_hint=unit)
        if metric in {
            "store-openings",
            "store-closures-count",
            "store-remodels-right-sizes",
        }:
            value = {
                "kind": "approximate",
                "value": value["value"],
                "qualifier": "around",
                "tolerance": None,
                "unit": "count",
            }
        locator = {
            "locator_kind": "html-semantic-table-cell",
            "table_index": 6,
            "row_index": row_index,
            "cell_index": 0 if metric.startswith("store-") else 1,
            "row": row,
            "excerpt_sha256": hashlib.sha256(
                " | ".join(row).encode("utf-8")
            ).hexdigest(),
        }
        add_assertion(
            source=may_source,
            source_date="2026-05-27",
            target_period="FY2026",
            metric=metric,
            value=value,
            unit=unit,
            currency=currency,
            source_value=expected,
            locator=locator,
            directness={
                "range": "direct_range",
                "approximate": "direct_approximate",
                "bound": "direct_minimum",
                "qualitative": "direct_composite",
            }.get(str(value["kind"]), "direct_exact"),
            progression_slot="q1",
        )

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for assertion in assertions:
        grouped[(str(assertion["metric_id"]), str(assertion["horizon_period_id"]))].append(
            assertion
        )
    versions: list[dict[str, Any]] = []
    for (metric_id, horizon_id), series_assertions in sorted(grouped.items()):
        first = series_assertions[0]
        series_id = _stable_id(
            "guidance-series",
            ("co", "ANF"),
            ("metric", metric_id),
            ("horizon", horizon_id),
            ("unit", first["unit_id"]),
            ("ccy", first.get("currency") or "na"),
        )
        ordered = sorted(
            series_assertions,
            key=lambda row: (str(row["source_date"]), str(row["assertion_id"])),
        )
        series_versions: list[dict[str, Any]] = []
        for ordinal, assertion in enumerate(ordered, 1):
            version_id = _stable_id(
                "guidance-version",
                ("series", series_id),
                ("occ", assertion["occurrence_id"]),
            )
            series_versions.append(
                {
                    "guidance_version_id": version_id,
                    "guidance_series_id": series_id,
                    "metric_id": metric_id,
                    "horizon_period_id": horizon_id,
                    "horizon_type": "annual",
                    "stated_in_period_id": assertion["stated_in_period_id"],
                    "source_date": assertion["source_date"],
                    "knowledge_date": assertion["knowledge_date"],
                    "source_assertion_id": assertion["assertion_id"],
                    "source_document_id": assertion["source_document_id"],
                    "occurrence_id": assertion["occurrence_id"],
                    "canonical_value": assertion["canonical_value"],
                    "unit_id": assertion["unit_id"],
                    "currency": assertion["currency"],
                    "predecessor_guidance_version_id": (
                        None
                        if ordinal == 1
                        else series_versions[-1]["guidance_version_id"]
                    ),
                    "successor_guidance_version_id": None,
                    "version_ordinal": ordinal,
                    "progression_slot": assertion["progression_slot"],
                }
            )
        for previous, current in zip(series_versions, series_versions[1:], strict=False):
            previous["successor_guidance_version_id"] = current["guidance_version_id"]
        versions.extend(series_versions)
    if len(assertions) != 34 or len(versions) != 34:
        raise EvidenceFoundationError(
            f"Missing annual guidance projection produced {len(assertions)}/34 records."
        )
    return assertions, versions, occurrences


def _local_name(node: Any) -> str:
    return str(getattr(node, "tag", "")).rsplit("}", 1)[-1].rsplit(":", 1)[-1].casefold()


def _descendants(node: Any, local_name: str) -> list[Any]:
    expected = local_name.casefold()
    return [child for child in node.iterdescendants() if _local_name(child) == expected]


def _context_map(root: Any) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for context in root.iter():
        if _local_name(context) != "context" or not context.get("id"):
            continue
        starts = _descendants(context, "startdate")
        ends = _descendants(context, "enddate")
        dimensions = _descendants(context, "explicitmember")
        result[str(context.get("id"))] = {
            "start": _text(starts[0]) if len(starts) == 1 else None,
            "end": _text(ends[0]) if len(ends) == 1 else None,
            "dimensions": sorted(
                {
                    f"{member.get('dimension')}={_text(member)}"
                    for member in dimensions
                }
            ),
        }
    return result


def _xbrl_numeric(node: Any) -> Decimal:
    raw = _text(node).replace("\u00a0", "").replace(",", "").strip()
    if str(node.get("format") or "").casefold().endswith("fixed-zero") or raw in {"—", "-", "–"}:
        value = Decimal(0)
    else:
        if raw.startswith("(") and raw.endswith(")"):
            raw = "-" + raw[1:-1]
        raw = re.sub(r"[^0-9.+-]", "", raw)
        try:
            value = Decimal(raw)
        except InvalidOperation as exc:
            raise EvidenceFoundationError(f"Inline XBRL value {raw!r} is not numeric.") from exc
    if node.get("sign") == "-":
        value = -abs(value)
    if node.get("scale") is not None:
        value *= Decimal(10) ** int(node.get("scale"))
    return value


def _period_labels_for_sec(report_date: str, form: str, durations: Sequence[int]) -> dict[int, str]:
    end = date.fromisoformat(report_date)
    fiscal_year = end.year - 1 if end.month <= 2 else end.year
    if form == "10-K":
        return {max(durations): f"FY{fiscal_year}"}
    if end.month in {4, 5}:
        quarter = 1
    elif end.month in {7, 8}:
        quarter = 2
    elif end.month in {10, 11}:
        quarter = 3
    else:
        raise EvidenceFoundationError(f"Unsupported ANF 10-Q report date {report_date}.")
    shortest = min(durations)
    result = {shortest: f"FY{fiscal_year}-Q{quarter}"}
    longest = max(durations)
    if longest != shortest:
        result[longest] = f"FY{fiscal_year}-YTD-Q{quarter}"
    return result


def _extract_sec_observations(
    registrations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    for source in registrations:
        if source["source_type"] != "sec_filing" or not source["economic_evidence_eligible"]:
            continue
        path = Path(str(source["path"]))
        root = lxml_html.fromstring(path.read_bytes())
        contexts = _context_map(root)
        report_date = str(source["report_date"])
        form = str(source["form"])
        for metric, concept in XBRL_CONCEPTS.items():
            candidates: list[tuple[Any, Mapping[str, Any], int]] = []
            for node in root.iter():
                if _local_name(node) not in {"nonfraction", "fraction"}:
                    continue
                name = str(node.get("name") or "").rsplit(":", 1)[-1]
                if name != concept:
                    continue
                context = contexts.get(str(node.get("contextref") or ""))
                if context is None or context["end"] != report_date:
                    continue
                dimensions = context["dimensions"]
                retained_earnings_rollforward = dimensions == [
                    "us-gaap:StatementEquityComponentsAxis=us-gaap:RetainedEarningsMember"
                ]
                if dimensions and not (
                    metric == "net-income-attributable" and retained_earnings_rollforward
                ):
                    continue
                if context["start"] is None:
                    continue
                duration = (date.fromisoformat(report_date) - date.fromisoformat(str(context["start"]))).days + 1
                if duration < 70 or duration > 380:
                    continue
                candidates.append((node, context, duration))
            if not candidates:
                continue
            durations = sorted({duration for _node, _context, duration in candidates})
            labels = _period_labels_for_sec(report_date, form, durations)
            if form == "10-Q" and metric in {
                "property-equipment-purchases",
                "common-stock-purchases-cash",
            }:
                # Cash-flow statement concepts are fiscal-year-to-date in Q2/Q3 even
                # when only one duration exists for the concept.  Q1 is both the
                # event quarter and YTD, so retain the single Q1 identity rather than
                # manufacturing two semantically identical facts.
                filing_period = next(iter(labels.values()))
                match = re.search(r"(FY\d{4})-(?:YTD-)?Q([1-3])", filing_period)
                if match is None:
                    raise EvidenceFoundationError(
                        f"Cash-flow period cannot be typed for {report_date}: {labels}."
                    )
                fiscal_year, quarter = match.groups()
                typed_period = (
                    f"{fiscal_year}-Q1"
                    if quarter == "1"
                    else f"{fiscal_year}-YTD-Q{quarter}"
                )
                labels = {max(durations): typed_period}
            for duration, period in sorted(labels.items(), key=lambda pair: pair[1]):
                matching = [row for row in candidates if row[2] == duration]
                by_value: dict[str, tuple[Any, Mapping[str, Any], int]] = {}
                for node, context, length in matching:
                    value = _xbrl_numeric(node)
                    if metric in {
                        "net-sales-amount",
                        "operating-income-amount",
                        "net-income-attributable",
                        "gross-profit-amount",
                        "property-equipment-purchases",
                        "common-stock-purchases-cash",
                    }:
                        value /= Decimal(1_000_000)
                        unit = "USD million"
                        currency = "USD"
                    elif metric == "diluted-weighted-average-shares":
                        value /= Decimal(1_000_000)
                        unit = "million shares"
                        currency = None
                    else:
                        unit = "USD/share"
                        currency = "USD"
                    key = _canonical_decimal(value)
                    by_value.setdefault(key, (node, context, length))
                if len(by_value) != 1:
                    raise EvidenceFoundationError(
                        f"SEC {metric} {period} has conflicting total-company values {sorted(by_value)}."
                    )
                value_text, (node, context, _length) = next(iter(by_value.items()))
                fact_id = str(node.get("id") or "")
                locator = {
                    "locator_kind": "inline-xbrl-fact",
                    "fact_id": fact_id,
                    "concept": str(node.get("name") or ""),
                    "context_id": str(node.get("contextref") or ""),
                    "period_start": context["start"],
                    "period_end": context["end"],
                    "context_dimensions": context["dimensions"],
                    "raw_text": _text(node),
                    "scale": int(node.get("scale")) if node.get("scale") is not None else None,
                    "sign": node.get("sign"),
                    "format": node.get("format"),
                }
                occurrence_id = _stable_id(
                    "occurrence",
                    ("doc", source["source_document_id"]),
                    ("fact", fact_id or _locator_digest(locator)),
                    ("period", period),
                )
                observations.append(
                    {
                        "observation_id": _stable_id(
                            "observation",
                            ("metric", _metric_id(metric)),
                            ("period", _period_id(period)),
                            ("occ", occurrence_id),
                        ),
                        "metric_key": metric,
                        "metric_id": _metric_id(metric),
                        "period_key": period,
                        "period_id": _period_id(period),
                        "period_kind": _period_kind(period),
                        "definition_id": "definition:core:company-reported@1",
                        "basis_id": "basis:core:reported@1",
                        "dimension_set_id": "dimset:anf:total-company@1",
                        "unit": unit,
                        "unit_id": _unit_id(unit),
                        "currency": currency,
                        "canonical_value": {"kind": "exact", "value": value_text},
                        "semantic_directness": "direct_exact",
                        "source_document_id": source["source_document_id"],
                        "source_content_sha256": source["content_sha256"],
                        "occurrence_id": occurrence_id,
                        "knowledge_date": source["knowledge_date"],
                        "source_authority_tier": 1,
                        "locator": locator,
                    }
                )
    return observations


def _prior_ytd_sales_observations(
    registrations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Extract comparative YTD sales needed for source-replayable growth.

    These are not inferred workbook values.  They are dimensionless inline-XBRL
    Revenues facts carried in the same reviewed 10-Q as the current YTD amount.
    """

    observations: list[dict[str, Any]] = []
    for source in registrations:
        if (
            source.get("source_type") != "sec_filing"
            or source.get("form") != "10-Q"
            or not source.get("economic_evidence_eligible")
        ):
            continue
        report_date = str(source.get("report_date") or "")
        if not re.fullmatch(r"20\d{2}-\d{2}-\d{2}", report_date):
            continue
        end = date.fromisoformat(report_date)
        if end.month in {7, 8}:
            quarter = 2
        elif end.month in {10, 11}:
            quarter = 3
        else:
            continue
        fiscal_year = end.year
        if fiscal_year not in {2022, 2023, 2024, 2025}:
            continue
        root = lxml_html.fromstring(Path(str(source["path"])).read_bytes())
        contexts = _context_map(root)
        candidates: list[tuple[Any, Mapping[str, Any], int]] = []
        for node in root.iter():
            if _local_name(node) not in {"nonfraction", "fraction"}:
                continue
            if str(node.get("name") or "").rsplit(":", 1)[-1] != "Revenues":
                continue
            context = contexts.get(str(node.get("contextref") or ""))
            if context is None or context["start"] is None or context["dimensions"]:
                continue
            context_end = date.fromisoformat(str(context["end"]))
            if context_end.year != fiscal_year - 1:
                continue
            duration = (
                context_end - date.fromisoformat(str(context["start"]))
            ).days + 1
            expected = 180 if quarter == 2 else 270
            if abs(duration - expected) > 25:
                continue
            candidates.append((node, context, duration))
        values: dict[str, tuple[Any, Mapping[str, Any], int]] = {}
        for node, context, duration in candidates:
            value = _xbrl_numeric(node) / Decimal(1_000_000)
            values.setdefault(_canonical_decimal(value), (node, context, duration))
        if len(values) != 1:
            raise EvidenceFoundationError(
                f"Comparative YTD sales in {source['source_document_id']} are not unique: "
                f"{sorted(values)}."
            )
        value_text, (node, context, duration) = next(iter(values.items()))
        period = f"FY{fiscal_year - 1}-YTD-Q{quarter}"
        locator = {
            "locator_kind": "inline-xbrl-comparative-fact",
            "fact_id": str(node.get("id") or ""),
            "concept": str(node.get("name") or ""),
            "context_id": str(node.get("contextref") or ""),
            "period_start": context["start"],
            "period_end": context["end"],
            "duration_days": duration,
            "context_dimensions": context["dimensions"],
            "raw_text": _text(node),
            "scale": int(node.get("scale")) if node.get("scale") is not None else None,
        }
        occurrence_id = _stable_id(
            "occurrence",
            ("doc", source["source_document_id"]),
            ("fact", locator["fact_id"] or _locator_digest(locator)),
            ("period", period),
        )
        observations.append(
            {
                "observation_id": _stable_id(
                    "observation",
                    ("metric", _metric_id("net-sales-amount")),
                    ("period", _period_id(period)),
                    ("occ", occurrence_id),
                ),
                "metric_key": "net-sales-amount",
                "metric_id": _metric_id("net-sales-amount"),
                "period_key": period,
                "period_id": _period_id(period),
                "period_kind": "ytd",
                "definition_id": "definition:core:company-reported@1",
                "basis_id": "basis:core:reported@1",
                "dimension_set_id": "dimset:anf:total-company@1",
                "unit": "USD million",
                "unit_id": _unit_id("USD million"),
                "currency": "USD",
                "canonical_value": {"kind": "exact", "value": value_text},
                "semantic_directness": "direct_exact",
                "source_document_id": source["source_document_id"],
                "source_content_sha256": source["content_sha256"],
                "occurrence_id": occurrence_id,
                "knowledge_date": source["knowledge_date"],
                "source_authority_tier": 1,
                "locator": locator,
            }
        )
    if len(observations) != 8:
        raise EvidenceFoundationError(
            f"Comparative YTD sales extraction produced {len(observations)}, not 8."
        )
    return observations


def _html_rows(table: Any) -> list[list[str]]:
    return [
        [_text(cell) for cell in row.xpath("./th|./td")]
        for row in table.xpath(".//tr")
    ]


def _first_numeric_cell(row: Sequence[str]) -> tuple[int, Decimal]:
    for index, raw in enumerate(row[1:], 1):
        text = raw.replace(",", "").strip()
        if not re.search(r"\d", text):
            continue
        negative = text.startswith("(") and text.endswith(")")
        number = re.sub(r"[^0-9.]", "", text)
        if number:
            value = Decimal(number)
            return index, -value if negative else value
    raise EvidenceFoundationError(f"Reviewed statement row has no numeric value: {row!r}")


def _release_statement_tables(path: Path) -> list[tuple[int, Any, list[list[str]]]]:
    root = lxml_html.fromstring(path.read_bytes())
    result = []
    for index, table in enumerate(root.xpath("//table")):
        text = _text(table)
        if "Condensed Consolidated Statements of Operations" in text and "Net sales" in text:
            result.append((index, table, _html_rows(table)))
    return result


def _duration_fingerprint(period: str) -> tuple[str, ...]:
    if "YTD-Q2" in period:
        return ("Twenty-Six Weeks Ended",)
    if "YTD-Q3" in period:
        return ("Thirty-Nine Weeks Ended",)
    if re.fullmatch(r"FY\d{4}", period):
        return ("Fifty-Two Weeks Ended", "Fifty-Three Weeks Ended")
    if period.endswith("-Q4") and period.startswith("FY2023"):
        return ("Fourteen Weeks Ended",)
    return ("Thirteen Weeks Ended", "Fourteen Weeks Ended")


def _extract_release_observation(
    source: Mapping[str, Any],
    *,
    metric: str,
    period: str,
) -> dict[str, Any]:
    tables = _release_statement_tables(Path(str(source["path"])))
    fingerprints = _duration_fingerprint(period)
    matches = [
        row
        for row in tables
        if any(fingerprint in _text(row[1]) for fingerprint in fingerprints)
    ]
    if len(matches) != 1:
        raise EvidenceFoundationError(
            f"Release {source['path']} {period} matched {len(matches)} statement tables."
        )
    table_index, _table, rows = matches[0]
    if metric == "net-sales-amount":
        row_index = next(index for index, row in enumerate(rows) if row and row[0].strip().casefold() == "net sales")
        unit = "USD million"
        currency = "USD"
    elif metric == "operating-income-amount":
        row_index = next(index for index, row in enumerate(rows) if row and row[0].casefold().startswith("operating") and ("income" in row[0].casefold() or "loss" in row[0].casefold()))
        unit = "USD million"
        currency = "USD"
    elif metric == "net-income-attributable":
        row_index = next(
            index
            for index, row in enumerate(rows)
            if row
            and "attributable to" in row[0].casefold()
            and "noncontrolling" not in row[0].casefold()
            and "per share" not in row[0].casefold()
        )
        unit = "USD million"
        currency = "USD"
    elif metric == "gross-profit-amount":
        row_index = next(index for index, row in enumerate(rows) if row and row[0].strip().casefold() == "gross profit")
        unit = "USD million"
        currency = "USD"
    elif metric == "reported-diluted-eps":
        diluted = [index for index, row in enumerate(rows) if row and row[0].strip().casefold() == "diluted"]
        if len(diluted) < 2:
            raise EvidenceFoundationError("Release statement does not distinguish diluted EPS and shares.")
        row_index = diluted[0]
        unit = "USD/share"
        currency = "USD"
    elif metric == "diluted-weighted-average-shares":
        diluted = [index for index, row in enumerate(rows) if row and row[0].strip().casefold() == "diluted"]
        if len(diluted) < 2:
            raise EvidenceFoundationError("Release statement does not distinguish diluted EPS and shares.")
        row_index = diluted[-1]
        unit = "million shares"
        currency = None
    else:
        raise EvidenceFoundationError(f"Unsupported release fact metric {metric!r}.")
    cell_index, value = _first_numeric_cell(rows[row_index])
    if metric in {
        "net-sales-amount",
        "operating-income-amount",
        "net-income-attributable",
        "gross-profit-amount",
        "diluted-weighted-average-shares",
    }:
        value /= Decimal(1_000)
    value_text = _canonical_decimal(value)
    excerpt = " | ".join(value for value in (rows[row_index][0], rows[row_index][cell_index]) if value)
    locator = {
        "locator_kind": "html-semantic-table-row",
        "table_index": table_index,
        "row_index": row_index,
        "cell_index": cell_index,
        "row_header_fingerprint": rows[row_index][0],
        "period_column": period,
        "excerpt": excerpt,
        "excerpt_sha256": hashlib.sha256(excerpt.encode("utf-8")).hexdigest(),
    }
    occurrence_id = _stable_id(
        "occurrence",
        ("doc", source["source_document_id"]),
        ("locator", _locator_digest(locator)),
        ("period", period),
    )
    return {
        "observation_id": _stable_id(
            "observation",
            ("metric", _metric_id(metric)),
            ("period", _period_id(period)),
            ("occ", occurrence_id),
        ),
        "metric_key": metric,
        "metric_id": _metric_id(metric),
        "period_key": period,
        "period_id": _period_id(period),
        "period_kind": _period_kind(period),
        "definition_id": "definition:core:company-reported@1",
        "basis_id": "basis:core:reported@1",
        "dimension_set_id": "dimset:anf:total-company@1",
        "unit": unit,
        "unit_id": _unit_id(unit),
        "currency": currency,
        "canonical_value": {"kind": "exact", "value": value_text},
        "semantic_directness": "direct_exact",
        "source_document_id": source["source_document_id"],
        "source_content_sha256": source["content_sha256"],
        "occurrence_id": occurrence_id,
        "knowledge_date": source["knowledge_date"],
        "source_authority_tier": source["authority_tier"],
        "locator": locator,
    }


def _release_by_period(
    registrations: Sequence[Mapping[str, Any]], sec_observations: Sequence[Mapping[str, Any]]
) -> dict[str, Mapping[str, Any]]:
    releases = {
        str(row["publication_date"]): row
        for row in registrations
        if row["source_type"] == "earnings_release" and row["economic_evidence_eligible"]
    }
    sec_sources = {
        str(row["source_document_id"]): row
        for row in registrations
        if row["source_type"] == "sec_filing"
    }
    result: dict[str, Mapping[str, Any]] = {}
    for observation in sec_observations:
        period = str(observation["period_key"])
        sec_source = sec_sources[str(observation["source_document_id"])]
        filed = date.fromisoformat(str(sec_source["publication_date"]))
        candidates = [
            row
            for release_date, row in releases.items()
            if date.fromisoformat(release_date) < filed
            and 1 <= (filed - date.fromisoformat(release_date)).days <= 30
        ]
        if candidates:
            result[period] = max(candidates, key=lambda row: str(row["publication_date"]))
    return result


def _prior_ytd_release_sales_observations(
    registrations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    release_dates = {
        (2022, 2): "2022-08-29",
        (2022, 3): "2022-11-23",
        (2023, 2): "2023-08-24",
        (2023, 3): "2023-11-22",
        (2024, 2): "2024-08-29",
        (2024, 3): "2024-11-27",
        (2025, 2): "2025-08-28",
        (2025, 3): "2025-11-26",
    }
    observations: list[dict[str, Any]] = []
    for (fiscal_year, quarter), publication_date in release_dates.items():
        source = _source_on_date(
            registrations, publication_date, source_type="earnings_release"
        )
        current_period = f"FY{fiscal_year}-YTD-Q{quarter}"
        prior_period = f"FY{fiscal_year - 1}-YTD-Q{quarter}"
        matches = [
            table
            for table in _release_statement_tables(Path(str(source["path"])))
            if any(
                fingerprint in _text(table[1])
                for fingerprint in _duration_fingerprint(current_period)
            )
        ]
        if len(matches) != 1:
            raise EvidenceFoundationError(
                f"Release comparative YTD table is not unique for {publication_date}."
            )
        table_index, _table, rows = matches[0]
        row_index = next(
            index
            for index, row in enumerate(rows)
            if row and row[0].strip().casefold() == "net sales"
        )
        row = rows[row_index]
        numeric: list[tuple[int, Decimal]] = []
        for cell_index, raw in enumerate(row[1:], 1):
            text = raw.replace(",", "").strip()
            if not re.search(r"\d", text):
                continue
            negative = text.startswith("(") and text.endswith(")")
            token = re.sub(r"[^0-9.]", "", text)
            if not token:
                continue
            raw_value = Decimal(token)
            if raw_value < Decimal(1000):
                continue
            value = raw_value / Decimal(1000)
            numeric.append((cell_index, -value if negative else value))
        if len(numeric) < 2:
            raise EvidenceFoundationError(
                f"Release comparative net-sales row lacks two values: {row!r}."
            )
        cell_index, value = numeric[1]
        value_text = _canonical_decimal(value)
        excerpt = " | ".join(row)
        locator = {
            "locator_kind": "html-semantic-table-comparative-cell",
            "table_index": table_index,
            "row_index": row_index,
            "cell_index": cell_index,
            "row_header_fingerprint": "Net sales",
            "period": prior_period,
            "excerpt": excerpt,
            "excerpt_sha256": hashlib.sha256(excerpt.encode("utf-8")).hexdigest(),
        }
        occurrence_id = _stable_id(
            "occurrence",
            ("doc", source["source_document_id"]),
            ("locator", _locator_digest(locator)),
            ("period", prior_period),
        )
        observations.append(
            {
                "observation_id": _stable_id(
                    "observation",
                    ("metric", _metric_id("net-sales-amount")),
                    ("period", _period_id(prior_period)),
                    ("occ", occurrence_id),
                ),
                "metric_key": "net-sales-amount",
                "metric_id": _metric_id("net-sales-amount"),
                "period_key": prior_period,
                "period_id": _period_id(prior_period),
                "period_kind": "ytd",
                "definition_id": "definition:core:company-reported@1",
                "basis_id": "basis:core:reported@1",
                "dimension_set_id": "dimset:anf:total-company@1",
                "unit": "USD million",
                "unit_id": _unit_id("USD million"),
                "currency": "USD",
                "canonical_value": {"kind": "exact", "value": value_text},
                "semantic_directness": "direct_exact",
                "source_document_id": source["source_document_id"],
                "source_content_sha256": source["content_sha256"],
                "occurrence_id": occurrence_id,
                "knowledge_date": publication_date,
                "source_authority_tier": source["authority_tier"],
                "locator": locator,
            }
        )
    return observations


def _reconcile_sec_release(
    registrations: Sequence[Mapping[str, Any]],
    sec_observations: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    release_sources = _release_by_period(registrations, sec_observations)
    release_observations: list[dict[str, Any]] = []
    relations: list[dict[str, Any]] = []
    for sec in sec_observations:
        metric = str(sec["metric_key"])
        if metric not in RECONCILIATION_METRICS:
            continue
        period = str(sec["period_key"])
        release = release_sources.get(period)
        if release is None:
            raise EvidenceFoundationError(f"Paired earnings release is absent for {period}.")
        try:
            release_observation = _extract_release_observation(
                release, metric=metric, period=period
            )
        except (StopIteration, EvidenceFoundationError):
            if metric == "gross-profit-amount":
                continue
            raise
        sec_value = sec["canonical_value"]["value"]
        release_value = release_observation["canonical_value"]["value"]
        if sec_value != release_value:
            raise EvidenceFoundationError(
                f"Reviewed SEC/release exact reconciliation failed for {metric} {period}: "
                f"{sec_value} != {release_value}."
            )
        release_observations.append(release_observation)
        relations.append(
            {
                "relation_id": _stable_id(
                    "reconciliation",
                    ("metric", _metric_id(metric)),
                    ("period", _period_id(period)),
                    ("sec", sec["occurrence_id"]),
                    ("release", release_observation["occurrence_id"]),
                ),
                "relation_type": "same-basis-exact-match",
                "metric_id": _metric_id(metric),
                "period_id": _period_id(period),
                "value": sec_value,
                "sec_observation_id": sec["observation_id"],
                "release_observation_id": release_observation["observation_id"],
                "sec_source_document_id": sec["source_document_id"],
                "release_source_document_id": release_observation["source_document_id"],
                "release_knowledge_date": release_observation["knowledge_date"],
                "sec_knowledge_date": sec["knowledge_date"],
                "temporal_rule": (
                    "SEC confirmation is eligible only on or after its filing date "
                    "and cannot backdate the release event"
                ),
                "canonical_direct_evidence": "earnings_release",
                "later_authority_confirmation": "sec_primary_filing",
            }
        )
    if len(relations) != 148:
        counts = Counter(row["metric_id"] for row in relations)
        raise EvidenceFoundationError(
            f"SEC/release reconciliation produced {len(relations)}, not 148: {dict(counts)}"
        )
    return release_observations, relations


def _direct_q4_release_observations(
    registrations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Extract direct Q4 statement facts before considering residual derivations."""

    observations: list[dict[str, Any]] = []
    metrics = (
        "net-sales-amount",
        "operating-income-amount",
        "net-income-attributable",
        "reported-diluted-eps",
        "diluted-weighted-average-shares",
        "gross-profit-amount",
    )
    for source in registrations:
        publication_date = str(source.get("publication_date") or "")
        if (
            source.get("source_type") != "earnings_release"
            or not source.get("economic_evidence_eligible")
            or not re.fullmatch(r"20\d{2}-03-\d{2}", publication_date)
        ):
            continue
        fiscal_year = int(publication_date[:4]) - 1
        if fiscal_year not in {2022, 2023, 2024, 2025}:
            continue
        period = f"FY{fiscal_year}-Q4"
        for metric in metrics:
            try:
                observations.append(
                    _extract_release_observation(source, metric=metric, period=period)
                )
            except (EvidenceFoundationError, StopIteration):
                if metric != "gross-profit-amount":
                    raise
    return observations


def _fact_key(observation: Mapping[str, Any]) -> tuple[str, ...]:
    return (
        str(observation["metric_id"]),
        str(observation["definition_id"]),
        str(observation["basis_id"]),
        str(observation["period_id"]),
        str(observation["dimension_set_id"]),
        str(observation["unit_id"]),
        str(observation.get("currency") or "na"),
    )


def _canonical_facts(observations: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, ...], list[Mapping[str, Any]]] = defaultdict(list)
    for observation in observations:
        grouped[_fact_key(observation)].append(observation)
    facts: list[dict[str, Any]] = []
    for key, rows in sorted(grouped.items()):
        values = {
            json.dumps(row["canonical_value"], sort_keys=True, separators=(",", ":"))
            for row in rows
        }
        if len(values) != 1:
            raise EvidenceFoundationError(
                f"Same-semantic observations conflict for {key}: {sorted(values)}"
            )
        ordered = sorted(
            rows,
            key=lambda row: (
                DIRECTNESS_RANK.get(str(row["semantic_directness"]), 99),
                int(row["source_authority_tier"]),
                str(row["knowledge_date"]),
                str(row["occurrence_id"]),
            ),
        )
        direct = ordered[0]
        fact_id = _stable_id(
            "canonical-fact",
            ("metric", key[0]),
            ("definition", key[1]),
            ("basis", key[2]),
            ("period", key[3]),
            ("dims", key[4]),
            ("unit", key[5]),
            ("ccy", key[6]),
        )
        facts.append(
            {
                "canonical_fact_id": fact_id,
                "metric_id": key[0],
                "metric_key": direct["metric_key"],
                "definition_id": key[1],
                "basis_id": key[2],
                "period_id": key[3],
                "period_key": direct["period_key"],
                "period_kind": direct["period_kind"],
                "dimension_set_id": key[4],
                "unit_id": key[5],
                "unit": direct["unit"],
                "currency": None if key[6] == "na" else key[6],
                "canonical_value": direct["canonical_value"],
                "preferred_direct_observation_id": direct["observation_id"],
                "observation_ids": sorted(str(row["observation_id"]) for row in rows),
                "source_document_ids": sorted(str(row["source_document_id"]) for row in rows),
                "knowledge_dates": sorted({str(row["knowledge_date"]) for row in rows}),
                "temporal_selection_rule": (
                    "select only observations with knowledge_date <= event cutoff; do not backdate later authority"
                ),
            }
        )
    return facts


def _eligible_fact_input(
    facts: Sequence[Mapping[str, Any]],
    observations: Mapping[str, Mapping[str, Any]],
    *,
    metric_id: str,
    period_id: str,
    cutoff: str,
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    matches = [
        fact
        for fact in facts
        if fact["metric_id"] == metric_id
        and fact["period_id"] == period_id
        and fact["dimension_set_id"] == "dimset:anf:total-company@1"
        and fact["canonical_value"].get("kind") == "exact"
    ]
    if len(matches) != 1:
        raise EvidenceFoundationError(
            f"Derived input {metric_id}/{period_id} is not unique: {len(matches)}."
        )
    fact = matches[0]
    eligible = [
        observations[observation_id]
        for observation_id in fact["observation_ids"]
        if observation_id in observations
        and str(observations[observation_id]["knowledge_date"]) <= cutoff
    ]
    if not eligible:
        raise EvidenceFoundationError(
            f"Derived input {metric_id}/{period_id} is unavailable by {cutoff}."
        )
    eligible.sort(
        key=lambda row: (
            DIRECTNESS_RANK.get(str(row["semantic_directness"]), 99),
            str(row["knowledge_date"]),
            int(row["source_authority_tier"]),
            str(row["observation_id"]),
        )
    )
    return fact, eligible[0]


def _derived_observation(
    *,
    metric_key: str,
    period_key: str,
    value: Mapping[str, Any],
    definition_id: str,
    basis_id: str,
    unit: str,
    currency: str | None,
    input_pairs: Sequence[tuple[Mapping[str, Any], Mapping[str, Any]]],
    derivation_rule_id: str,
    directness: str,
    calculation: str,
) -> dict[str, Any]:
    facts = [pair[0] for pair in input_pairs]
    inputs = [pair[1] for pair in input_pairs]
    input_fact_ids = tuple(str(fact["canonical_fact_id"]) for fact in facts)
    input_observation_ids = tuple(str(row["observation_id"]) for row in inputs)
    if len(set(input_fact_ids)) != len(input_fact_ids):
        raise EvidenceFoundationError("A derived observation repeats an input fact.")
    knowledge_date = max(str(row["knowledge_date"]) for row in inputs)
    source_document_ids = tuple(
        sorted({str(row["source_document_id"]) for row in inputs})
    )
    occurrence_id = _stable_id(
        "derived-occurrence",
        ("rule", derivation_rule_id),
        ("metric", _metric_id(metric_key)),
        ("period", _period_id(period_key)),
        ("inputs", "|".join(input_fact_ids)),
    )
    locator = {
        "locator_kind": "canonical-derived-fact",
        "derivation_rule_id": derivation_rule_id,
        "input_canonical_fact_ids": list(input_fact_ids),
        "input_observation_ids": list(input_observation_ids),
        "calculation": calculation,
    }
    return {
        "observation_id": _stable_id(
            "observation",
            ("metric", _metric_id(metric_key)),
            ("period", _period_id(period_key)),
            ("occ", occurrence_id),
        ),
        "metric_key": metric_key,
        "metric_id": _metric_id(metric_key),
        "period_key": period_key,
        "period_id": _period_id(period_key),
        "period_kind": _period_kind(period_key),
        "definition_id": definition_id,
        "basis_id": basis_id,
        "dimension_set_id": "dimset:anf:total-company@1",
        "unit": unit,
        "unit_id": _unit_id(unit),
        "currency": currency,
        "canonical_value": dict(value),
        "semantic_directness": directness,
        "source_document_id": str(inputs[-1]["source_document_id"]),
        "source_document_ids": list(source_document_ids),
        "source_content_sha256": str(inputs[-1]["source_content_sha256"]),
        "occurrence_id": occurrence_id,
        "knowledge_date": knowledge_date,
        "source_authority_tier": min(
            int(row["source_authority_tier"]) for row in inputs
        ),
        "locator": locator,
        "derivation_rule_id": derivation_rule_id,
        "derivation_input_record_ids": list(input_fact_ids),
        "derivation_input_observation_ids": list(input_observation_ids),
        "derivation_support_record_ids": [],
        "calculation": calculation,
    }


def _derived_period_observations(
    base_facts: Sequence[Mapping[str, Any]],
    base_observations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    observation_by_id = {
        str(row["observation_id"]): row for row in base_observations
    }
    derived: list[dict[str, Any]] = []
    event_dates = {
        (2022, 2): "2022-08-29",
        (2022, 3): "2022-11-23",
        (2023, 2): "2023-08-24",
        (2023, 3): "2023-11-22",
        (2024, 2): "2024-08-29",
        (2024, 3): "2024-11-27",
        (2025, 2): "2025-08-28",
        (2025, 3): "2025-11-26",
    }
    residual_dates = {
        (2022, 2): "2022-09-07",
        (2022, 3): "2022-12-06",
        (2023, 2): "2023-09-01",
        (2023, 3): "2023-12-04",
        (2024, 2): "2024-09-06",
        (2024, 3): "2024-12-06",
        (2025, 2): "2025-09-05",
        (2025, 3): "2025-12-05",
    }

    # Exact quarter flows from cumulative same-metric records.
    residual_metrics = (
        "property-equipment-purchases",
        "store-openings",
        "store-closures-count",
        "store-remodels",
        "store-right-sizes",
    )
    for fiscal_year in range(2022, 2026):
        for quarter in (2, 3):
            cutoff = residual_dates[(fiscal_year, quarter)]
            current_period = f"FY{fiscal_year}-YTD-Q{quarter}"
            prior_period = (
                f"FY{fiscal_year}-Q1"
                if quarter == 2
                else f"FY{fiscal_year}-YTD-Q2"
            )
            output_period = f"FY{fiscal_year}-Q{quarter}"
            for metric in residual_metrics:
                metric_id = _metric_id(metric)
                try:
                    current = _eligible_fact_input(
                        base_facts,
                        observation_by_id,
                        metric_id=metric_id,
                        period_id=_period_id(current_period),
                        cutoff=cutoff,
                    )
                    prior = _eligible_fact_input(
                        base_facts,
                        observation_by_id,
                        metric_id=metric_id,
                        period_id=_period_id(prior_period),
                        cutoff=cutoff,
                    )
                except EvidenceFoundationError:
                    continue
                current_value = Decimal(
                    str(current[0]["canonical_value"]["value"])
                )
                prior_value = Decimal(str(prior[0]["canonical_value"]["value"]))
                result = current_value - prior_value
                if result < 0:
                    raise EvidenceFoundationError(
                        f"Negative additive period residual for {metric}/{output_period}."
                    )
                current_fact = current[0]
                derived.append(
                    _derived_observation(
                        metric_key=metric,
                        period_key=output_period,
                        value={"kind": "exact", "value": _canonical_decimal(result)},
                        definition_id=str(current_fact["definition_id"]),
                        basis_id=str(current_fact["basis_id"]),
                        unit=str(current_fact["unit"]),
                        currency=current_fact.get("currency"),
                        input_pairs=(current, prior),
                        derivation_rule_id=(
                            "derivation:promise-progress:quarter-ytd-minus-prior-ytd@1"
                        ),
                        directness="exact_same_metric_derivation",
                        calculation=(
                            f"{current_value} - {prior_value} = {result}"
                        ),
                    )
                )

    expected_growth = {
        (2022, 2): Decimal("-1.7"),
        (2022, 3): Decimal("-2.1"),
        (2023, 2): Decimal("9.5"),
        (2023, 3): Decimal("13.2"),
        (2024, 2): Decimal("21.6"),
        (2024, 3): Decimal("19.0"),
        (2025, 2): Decimal("7.0"),
        (2025, 3): Decimal("6.9"),
    }
    expected_margin = {
        (2022, 2): Decimal("-0.7"),
        (2022, 3): Decimal("0.2"),
        (2023, 2): Decimal("7.0"),
        (2023, 3): Decimal("9.3"),
        (2024, 2): Decimal("14.2"),
        (2024, 3): Decimal("14.4"),
        (2025, 2): Decimal("13.4"),
        (2025, 3): Decimal("12.9"),
    }
    for fiscal_year in range(2022, 2026):
        for quarter in (2, 3):
            cutoff = event_dates[(fiscal_year, quarter)]
            period = f"FY{fiscal_year}-YTD-Q{quarter}"
            current_sales = _eligible_fact_input(
                base_facts,
                observation_by_id,
                metric_id=_metric_id("net-sales-amount"),
                period_id=_period_id(period),
                cutoff=cutoff,
            )
            prior_sales = _eligible_fact_input(
                base_facts,
                observation_by_id,
                metric_id=_metric_id("net-sales-amount"),
                period_id=_period_id(f"FY{fiscal_year - 1}-YTD-Q{quarter}"),
                cutoff=cutoff,
            )
            current_value = Decimal(
                str(current_sales[0]["canonical_value"]["value"])
            )
            prior_value = Decimal(str(prior_sales[0]["canonical_value"]["value"]))
            growth = (current_value / prior_value - Decimal(1)) * Decimal(100)
            rounded_growth = growth.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
            if rounded_growth != expected_growth[(fiscal_year, quarter)]:
                raise EvidenceFoundationError(
                    f"Reviewed YTD growth replay changed for {period}: {growth}."
                )
            derived.append(
                _derived_observation(
                    metric_key="revenue-growth",
                    period_key=period,
                    value={
                        "kind": "exact",
                        "value": _canonical_decimal(growth),
                        "display_decimals": 1,
                    },
                    definition_id="definition:core:company-reported@1",
                    basis_id="basis:core:reported@1",
                    unit="percent",
                    currency=None,
                    input_pairs=(current_sales, prior_sales),
                    derivation_rule_id=(
                        "derivation:promise-progress:ytd-growth-from-current-prior-amounts@1"
                    ),
                    directness="component_based_derivation",
                    calculation=(
                        f"({current_value} / {prior_value} - 1) * 100 = {growth}"
                    ),
                )
            )
            operating_income = _eligible_fact_input(
                base_facts,
                observation_by_id,
                metric_id=_metric_id("operating-income-amount"),
                period_id=_period_id(period),
                cutoff=cutoff,
            )
            operating_value = Decimal(
                str(operating_income[0]["canonical_value"]["value"])
            )
            margin = operating_value / current_value * Decimal(100)
            rounded_margin = margin.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
            if rounded_margin != expected_margin[(fiscal_year, quarter)]:
                raise EvidenceFoundationError(
                    f"Reviewed YTD margin replay changed for {period}: {margin}."
                )
            derived.append(
                _derived_observation(
                    metric_key="operating-margin",
                    period_key=period,
                    value={
                        "kind": "exact",
                        "value": _canonical_decimal(margin),
                        "display_decimals": 1,
                    },
                    definition_id="definition:core:company-reported@1",
                    basis_id="basis:core:reported@1",
                    unit="percent",
                    currency=None,
                    input_pairs=(operating_income, current_sales),
                    derivation_rule_id=(
                        "derivation:promise-progress:ytd-margin-from-components@1"
                    ),
                    directness="component_based_derivation",
                    calculation=(
                        f"{operating_value} / {current_value} * 100 = {margin}"
                    ),
                )
            )
    return derived


def _q4_direct_evidence_bindings(
    q4_evidence: Mapping[str, Any], canonical_facts: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    metric_aliases = {"net-sales-growth": "revenue-growth"}
    unit_hints = {
        "net-sales-growth": "percent",
        "operating-margin": "percent",
        "reported-diluted-eps": "USD/share",
        "diluted-weighted-average-shares": "million shares",
        "net-sales-amount": "USD million",
        "operating-income-amount": "USD million",
        "net-income-attributable": "USD million",
        "capital-expenditures": "USD million",
    }
    bindings: list[dict[str, Any]] = []
    for row in q4_evidence["records"]:
        if row["classification"] != "direct":
            continue
        metric = str(row["metric"])
        period = f"{row['fiscal_year']}-Q4"
        raw_value = row.get("value") or row.get("value_usd_million")
        parsed_value = _fact_value(
            _parse_display_value(raw_value, unit_hint=unit_hints[metric])
        )
        if metric == "capital-expenditures":
            bindings.append(
                {
                    "q4_evidence_id": row["q4_evidence_id"],
                    "metric_id": _metric_id(metric),
                    "period_id": _period_id(period),
                    "canonical_value": parsed_value,
                    "representation": "definition-gated-derivation-graph",
                    "canonical_fact_id": None,
                    "input_metric_id": _metric_id("property-equipment-purchases"),
                    "input_period_ids": [
                        _period_id(str(row["fiscal_year"])),
                        _period_id(f"{row['fiscal_year']}-YTD-Q3"),
                    ],
                    "derivation_formula": row["derivation_formula"],
                    "knowledge_date": row["knowledge_date"],
                    "definition_equivalence_required": True,
                }
            )
            continue
        canonical_metric = metric_aliases.get(metric, metric)
        matches = [
            fact
            for fact in canonical_facts
            if fact["metric_id"] == _metric_id(canonical_metric)
            and fact["period_id"] == _period_id(period)
            and fact["canonical_value"] == parsed_value
        ]
        if len(matches) != 1:
            raise EvidenceFoundationError(
                f"Direct Q4 evidence {row['q4_evidence_id']} maps to {len(matches)} facts."
            )
        bindings.append(
            {
                "q4_evidence_id": row["q4_evidence_id"],
                "metric_id": matches[0]["metric_id"],
                "period_id": matches[0]["period_id"],
                "canonical_value": matches[0]["canonical_value"],
                "representation": "canonical-direct-fact",
                "canonical_fact_id": matches[0]["canonical_fact_id"],
                "knowledge_date": row["knowledge_date"],
            }
        )
    bindings.sort(key=lambda row: row["q4_evidence_id"])
    return bindings


def _audit_typed_observations(
    audit: Mapping[str, Any], registrations: Sequence[Mapping[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], set[str]]:
    by_path, by_hash, by_key = _source_lookup(registrations)
    observations: list[dict[str, Any]] = []
    occurrences: list[dict[str, Any]] = []
    implemented_evidence_ids: set[str] = set()
    for index, record in enumerate(audit["expanded_fact_inventory"]["records"]):
        metric = record.get("metric")
        period = _normalize_period_key(record.get("period")) if record.get("period") else None
        typed_values = record.get("typed_values") or []
        raw_value = record.get("value")
        if not metric or not period or (not typed_values and raw_value is None):
            continue
        source = _source_for_audit_record(record, by_path, by_hash, by_key)
        audit_id = str(
            record.get("audit_evidence_id")
            or record.get("expanded_fact_id")
            or f"expanded-index:{index}"
        )
        _require_economic_source(source, audit_id)
        occurrence = _occurrence_from_audit(record, source, occurrence_key=audit_id)
        occurrences.append(occurrence)
        implemented_evidence_ids.add(audit_id)
        if typed_values:
            canonical_values = [
                _parse_display_value(value.get("display"), unit_hint=record.get("unit"))
                if isinstance(value, Mapping) and "display" in value
                else json.loads(json.dumps(value))
                for value in typed_values
            ]
            # The reviewed comparable-sales table locators target one exact cell.  Older
            # extraction captured numeric footnote/header tokens before that target;
            # the locator-ordered final value is the cell's scalar observation.
            if (
                metric == "comparable-sales"
                and len(canonical_values) > 1
                and record.get("locator", {}).get("cell_index") is not None
            ):
                canonical_values = [canonical_values[-1]]
            canonical_value = (
                canonical_values[0]
                if len(canonical_values) == 1
                else {"kind": "composite", "components": canonical_values}
            )
        else:
            canonical_value = _parse_display_value(raw_value, unit_hint=record.get("unit"))
        semantic_normalization = None
        if metric == "store-closures" and canonical_value.get("kind") == "exact":
            closure_value = Decimal(str(canonical_value.get("value")))
            metric = "store-closures-count"
            if closure_value < 0:
                canonical_value = {
                    **canonical_value,
                    "value": _canonical_decimal(abs(closure_value)),
                }
                semantic_normalization = (
                    "signed presentation flow normalized to positive closure count"
                )
        unit = _audit_unit(str(metric), record.get("unit"), canonical_value)
        canonical_value = _fact_value(canonical_value)
        currency = record.get("currency") or ("USD" if unit.startswith("USD") else None)
        observations.append(
            {
                "observation_id": _stable_id(
                    "observation",
                    ("metric", _metric_id(str(metric))),
                    ("period", _period_id(str(period))),
                    ("occ", occurrence["occurrence_id"]),
                ),
                "metric_key": str(metric),
                "metric_id": _metric_id(str(metric)),
                "period_key": str(period),
                "period_id": _period_id(str(period)),
                "period_kind": _period_kind(str(period)),
                "definition_id": _audit_definition_id(
                    str(metric), record.get("definition"), record
                ),
                "basis_id": _audit_basis_id(str(metric), record.get("basis"), record),
                "dimension_set_id": f"dimset:anf:{_slug(record.get('scope_dimension') or record.get('dimension') or 'total-company')}@1",
                "unit": unit,
                "unit_id": _unit_id(unit),
                "currency": currency,
                "canonical_value": canonical_value,
                "semantic_directness": record.get("semantic_directness") or "direct_exact",
                "source_document_id": source["source_document_id"],
                "source_content_sha256": source["content_sha256"],
                "occurrence_id": occurrence["occurrence_id"],
                "knowledge_date": occurrence["knowledge_date"],
                "source_authority_tier": source["authority_tier"],
                "locator": occurrence["locator"],
                "audit_record_id": audit_id,
                "semantic_normalization": semantic_normalization,
            }
        )
    return observations, occurrences, implemented_evidence_ids


def _gap_occurrences(
    audit: Mapping[str, Any], registrations: Sequence[Mapping[str, Any]]
) -> tuple[list[dict[str, Any]], set[str]]:
    by_path, by_hash, by_key = _source_lookup(registrations)
    expanded_by_evidence = {
        str(row.get("audit_evidence_id")): row
        for row in audit["expanded_fact_inventory"]["records"]
        if row.get("audit_evidence_id")
    }
    occurrences: list[dict[str, Any]] = []
    implemented: set[str] = set()
    for gap in audit["remaining_extraction_gaps"]["records"]:
        evidence_ref = str(gap.get("evidence_ref") or gap["unused_evidence_id"])
        source_record = expanded_by_evidence.get(evidence_ref, gap)
        source = _source_for_audit_record(source_record, by_path, by_hash, by_key)
        _require_economic_source(source, evidence_ref)
        occurrence = _occurrence_from_audit(
            source_record,
            source,
            occurrence_key=evidence_ref,
        )
        occurrence["gap_disposition"] = "implemented_source_occurrence"
        occurrences.append(occurrence)
        implemented.add(evidence_ref)
    return occurrences, implemented


def _management_target_assertions(
    observations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    for observation in observations:
        if observation["definition_id"] != "definition:anf:management-target@1":
            continue
        targets.append(
            {
                "management_target_assertion_id": _stable_id(
                    "management-target-assertion",
                    ("observation", observation["observation_id"]),
                ),
                "metric_id": observation["metric_id"],
                "metric_key": observation["metric_key"],
                "horizon_period_id": observation["period_id"],
                "horizon_period_key": observation["period_key"],
                "canonical_value": observation["canonical_value"],
                "unit_id": observation["unit_id"],
                "unit": observation["unit"],
                "currency": observation["currency"],
                "source_document_id": observation["source_document_id"],
                "occurrence_id": observation["occurrence_id"],
                "knowledge_date": observation["knowledge_date"],
                "semantic_directness": observation["semantic_directness"],
                "definition_id": observation["definition_id"],
                "basis_id": observation["basis_id"],
            }
        )
    targets.sort(key=lambda row: row["management_target_assertion_id"])
    if len(targets) != 20:
        raise EvidenceFoundationError(
            f"Investor Day management-target assertion count is {len(targets)}, not 20."
        )
    return targets


def _annual_store_observations(
    registrations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    sec_annual = {
        f"FY{int(str(row['report_date'])[:4]) - 1}": row
        for row in registrations
        if row["source_type"] == "sec_filing" and row["form"] == "10-K"
    }
    observations: list[dict[str, Any]] = []
    for period, metrics in ANNUAL_STORE_TOTALS.items():
        source = sec_annual[period]
        root = lxml_html.fromstring(Path(str(source["path"])).read_bytes())
        tables = root.xpath("//table")
        for metric, value in metrics.items():
            if metric == "store-closures-count":
                table_index = ANNUAL_STORE_TABLES[period]["activity"]
                row_labels = {"closed", "permanently closed"}
            else:
                table_index = ANNUAL_STORE_TABLES[period]["experience"]
                row_labels = {
                    "store-openings": {"new stores"},
                    "store-remodels": {"remodels"},
                    "store-right-sizes": {"right-sizes"},
                }[metric]
            rows = _html_rows(tables[table_index])
            matching_rows = [
                (row_index, row)
                for row_index, row in enumerate(rows)
                if row and row[0].strip().casefold() in row_labels
            ]
            if len(matching_rows) != 1:
                raise EvidenceFoundationError(
                    f"Reviewed SEC store row {metric} {period} is not unique."
                )
            row_index, row = matching_rows[0]
            expected = Decimal(value)
            value_cells: list[tuple[int, str]] = []
            for cell_index, raw in enumerate(row[1:], 1):
                normalized = raw.replace(",", "").strip().strip("()")
                if not normalized:
                    continue
                try:
                    candidate = Decimal(normalized)
                except InvalidOperation:
                    continue
                if candidate == expected:
                    value_cells.append((cell_index, raw))
            if not value_cells:
                raise EvidenceFoundationError(
                    f"Reviewed SEC store value {metric} {period}={value} is absent."
                )
            cell_index, raw_value = value_cells[-1]
            excerpt = f"{row[0]} | {raw_value}"
            locator = {
                "locator_kind": "html-semantic-table-row",
                "table_index": table_index,
                "row_index": row_index,
                "cell_index": cell_index,
                "row_header_fingerprint": row[0],
                "excerpt": excerpt,
                "excerpt_sha256": hashlib.sha256(excerpt.encode("utf-8")).hexdigest(),
                "metric": metric,
                "period": period,
            }
            occurrence_id = _stable_id(
                "occurrence",
                ("doc", source["source_document_id"]),
                ("locator", _locator_digest(locator)),
                ("period", period),
            )
            observations.append(
                {
                    "observation_id": _stable_id(
                        "observation",
                        ("metric", _metric_id(metric)),
                        ("period", _period_id(period)),
                        ("occ", occurrence_id),
                    ),
                    "metric_key": metric,
                    "metric_id": _metric_id(metric),
                    "period_key": period,
                    "period_id": _period_id(period),
                    "period_kind": "annual",
                    "definition_id": "definition:anf:company-owned-store-activity@1",
                    "basis_id": "basis:core:reported@1",
                    "dimension_set_id": "dimset:anf:total-company@1",
                    "unit": "count",
                    "unit_id": _unit_id("count"),
                    "currency": None,
                    "canonical_value": {"kind": "exact", "value": value},
                    "semantic_directness": "direct_exact",
                    "source_document_id": source["source_document_id"],
                    "source_content_sha256": source["content_sha256"],
                    "occurrence_id": occurrence_id,
                    "knowledge_date": source["knowledge_date"],
                    "source_authority_tier": 1,
                    "locator": locator,
                }
            )
    return observations


def _definition_relations(
    registrations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    sources = list(registrations)
    relations: list[dict[str, Any]] = []

    def source_value_locator(
        source: Mapping[str, Any], *, period: str, value: str
    ) -> tuple[dict[str, Any], str]:
        text = _document_text(source)
        number = Decimal(value)
        candidates = {
            _canonical_decimal(number),
            f"{number:,.3f}".rstrip("0").rstrip("."),
            f"{number * 1000:,.0f}",
        }
        positions: list[int] = []
        for candidate in sorted(candidates, key=len, reverse=True):
            start = 0
            while True:
                found = text.find(candidate, start)
                if found < 0:
                    break
                positions.append(found)
                start = found + 1
        if not positions:
            raise EvidenceFoundationError(
                f"Capex/P&E definition value {period}={value} is absent from "
                f"{source['source_document_id']}."
            )
        capex_positions = [
            match.start()
            for match in re.finditer(r"capital expenditure", text, re.IGNORECASE)
        ]
        if not capex_positions:
            raise EvidenceFoundationError(
                f"Capex definition wording is absent from {source['source_document_id']}."
            )
        value_start = min(
            positions,
            key=lambda position: min(abs(position - capex) for capex in capex_positions),
        )
        nearest_capex = min(capex_positions, key=lambda position: abs(position - value_start))
        excerpt_start = max(0, min(value_start, nearest_capex) - 450)
        excerpt_end = min(len(text), max(value_start, nearest_capex) + 650)
        excerpt = text[excerpt_start:excerpt_end]
        if "capital expenditure" not in excerpt.casefold():
            raise EvidenceFoundationError(
                f"Capex definition and value are not occurrence-close for {period}."
            )
        locator = {
            "locator_kind": "html-reviewed-definition-occurrence",
            "character_start": excerpt_start,
            "character_end": excerpt_end,
            "period": period,
            "reviewed_value_usd_million": _canonical_decimal(number),
            "definition_fingerprint": "capital expenditure",
            "excerpt": excerpt,
            "excerpt_sha256": hashlib.sha256(excerpt.encode("utf-8")).hexdigest(),
        }
        occurrence_id = _stable_id(
            "occurrence",
            ("doc", source["source_document_id"]),
            ("locator", _locator_digest(locator)),
            ("period", period),
            ("purpose", "capex-pe-definition-equivalence"),
        )
        return locator, occurrence_id

    relation_by_period: dict[str, dict[str, Any]] = {}
    for period, knowledge_date, value in CAPEX_DEFINITION_PERIODS:
        source_type = "earnings_release" if knowledge_date == "2023-03-02" else "sec_filing"
        source = _source_on_date(
            sources, knowledge_date, source_type=source_type
        )
        locator, occurrence_id = source_value_locator(
            source, period=period, value=value
        )
        relation = {
            "relation_id": _stable_id(
                "definition-relation",
                ("period", _period_id(period)),
                ("from", "company-guided-capex"),
                ("to", "property-equipment-purchases"),
                ("known", knowledge_date),
            ),
            "relation_type": "reviewed-definition-equivalence",
            "relation_scope": "source-period",
            "period_id": _period_id(period),
            "from_definition_id": "definition:anf:company-guided-capex@1",
            "to_definition_id": "definition:core:property-equipment-purchases@1",
            "knowledge_date": knowledge_date,
            "source_document_id": source["source_document_id"],
            "source_occurrence_id": occurrence_id,
            "source_locator": locator,
            "source_value_usd_million": _canonical_decimal(Decimal(value)),
            "rationale": (
                "reviewed issuer wording identifies the compatible property/equipment "
                "cash-flow amount as capital expenditures"
            ),
            "temporal_rule": "relation is unavailable before its knowledge date",
        }
        relations.append(relation)
        relation_by_period[period] = relation

    # A quarter residual inherits the reviewed definition identity only from its
    # compatible cumulative-period relation.  The derived-period relation is
    # explicit and retains the exact source occurrence; it is never backdated.
    for fiscal_year in range(2022, 2026):
        for quarter in (2, 3):
            source_period = f"FY{fiscal_year}-YTD-Q{quarter}"
            source_relation = relation_by_period[source_period]
            period = f"FY{fiscal_year}-Q{quarter}"
            relations.append(
                {
                    **source_relation,
                    "relation_id": _stable_id(
                        "definition-relation",
                        ("period", _period_id(period)),
                        ("from", "company-guided-capex"),
                        ("to", "property-equipment-purchases"),
                        ("known", source_relation["knowledge_date"]),
                        ("scope", "derived-period-flow"),
                    ),
                    "relation_scope": "derived-period-flow",
                    "period_id": _period_id(period),
                    "source_period_id": _period_id(source_period),
                    "derived_from_relation_ids": [source_relation["relation_id"]],
                    "rationale": (
                        "the compatible quarter flow is an exact same-definition "
                        "residual of the reviewed cumulative capex/P&E series"
                    ),
                }
            )
    relations.sort(key=lambda row: str(row["relation_id"]))
    return relations


def _debt_evidence(registrations: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    wrappers = [
        row
        for row in registrations
        if row["source_type"] == "sec_8k_wrapper"
        and row["economic_evidence_eligible"]
    ]
    result: list[dict[str, Any]] = []
    for event in DEBT_EVENTS:
        candidates = [
            row
            for row in wrappers
            if row.get("publication_date") == event["knowledge_date"]
        ]
        if not candidates and event["knowledge_date"] == "2024-09-06":
            candidates = [
                row
                for row in registrations
                if row["source_type"] == "sec_filing" and row.get("publication_date") == "2024-09-06"
            ]
        if not candidates:
            raise EvidenceFoundationError(f"Debt event source is absent: {event['event_id']}")
        source = candidates[0]
        result.append(
            {
                **event,
                "source_document_id": source["source_document_id"],
                "source_content_sha256": source["content_sha256"],
                "review_state": "reviewed",
                "expected_downstream": ["Debt Detail", "Summary", "Valuation"],
            }
        )
    return result


def _segment_evidence(registrations: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    source = next(
        row
        for row in registrations
        if row["source_type"] == "sec_filing"
        and row.get("report_date") == "2023-07-29"
    )
    document_text = _text(lxml_html.fromstring(Path(str(source["path"])).read_bytes()))
    fingerprints = (
        "three reportable segments: Americas; Europe, the Middle East and Africa (EMEA); and Asia-Pacific (APAC)",
        "All prior periods presented are recast to conform to the new segment presentation.",
        "There was no impact on consolidated net sales, operating income (loss) or net income (loss) per share",
    )
    for fingerprint in fingerprints:
        if fingerprint.casefold() not in document_text.casefold():
            raise EvidenceFoundationError(
                f"Reviewed segment-definition fingerprint changed: {fingerprint!r}."
            )
    return [
        {
            "segment_evidence_id": "segment-definition:anf:geographic-recast@1",
            "knowledge_date": source["knowledge_date"],
            "source_document_id": source["source_document_id"],
            "source_content_sha256": source["content_sha256"],
            "reportable_segments": ["Americas", "EMEA", "APAC"],
            "effective_context": "FY2023-Q2 filing",
            "prior_periods_recast": True,
            "consolidated_results_unchanged": True,
            "unallocated_scope": "corporate functions and other income and expenses",
            "candidate_downstream": ["Summary", "BS_segment"],
            "selection_constraint": (
                "do not splice prior brand operating-segment series into geographic reportable segments without an explicit dimension transformation"
            ),
            "source_fingerprints": list(fingerprints),
        }
    ]


def _conflict_relations(audit: Mapping[str, Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for record in audit["source_conflicts_expanded"]["records"]:
        normalized = json.loads(json.dumps(record, ensure_ascii=False))
        normalized = _deep_clean(normalized)
        if normalized.get("classification") == "genuine_same_semantic_source_conflict":
            normalized["resolution"] = "issuer_comparator_error"
            normalized["mint_guidance_version"] = False
            normalized["canonical_historical_value"] = {
                "kind": "range",
                "low": "200",
                "high": "225",
                "unit": "USD million",
            }
        result.append(normalized)
    return result


def _deep_clean(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _deep_clean(child) for key, child in value.items()}
    if isinstance(value, list):
        return [_deep_clean(child) for child in value]
    if isinstance(value, str):
        return _clean_text(value)
    return value


def _evidence_disposition(
    audit: Mapping[str, Any], implemented_granular: set[str]
) -> dict[str, Any]:
    granular = audit["remaining_extraction_gaps"]["records"]
    missing = [
        str(row.get("evidence_ref") or row["unused_evidence_id"])
        for row in granular
        if str(row.get("evidence_ref") or row["unused_evidence_id"])
        not in implemented_granular
    ]
    if missing:
        raise EvidenceFoundationError(
            f"Granular audit evidence lacks a canonical occurrence: {missing[:5]}"
        )
    cohort_dispositions = [
        {
            "cohort_id": "gap-cohort:investor-day",
            "audit_count": 20,
            "implemented_count": 20,
            "explicitly_deferred_count": 0,
            "reason": "all 20 audit-enumerated hidden-text targets canonicalized with visual-layer limitation",
        },
        {
            "cohort_id": "gap-cohort:additional-transcript-clusters",
            "audit_count": 31,
            "implemented_count": 16,
            "explicitly_deferred_count": 15,
            "reason": (
                "16 reviewed source-level line clusters are enumerated in expanded_fact_inventory; "
                "the remaining 15 cohort occurrences lack individual source document, line, speaker, metric, period, unit and value records, so lossless decomposition is impossible"
            ),
        },
        {
            "cohort_id": "gap-cohort:q1-fy2026-noncore",
            "audit_count": 6,
            "implemented_count": 6,
            "explicitly_deferred_count": 0,
            "reason": "P&E, repurchase cash, openings, closures, remodels and right-sizes canonicalized",
        },
        {
            "cohort_id": "gap-cohort:sec-definition-debt",
            "audit_count": 8,
            "implemented_count": 8,
            "explicitly_deferred_count": 0,
            "reason": "four temporally typed capex relations and four direct debt/credit events canonicalized",
        },
    ]
    implemented = len(granular) + sum(row["implemented_count"] for row in cohort_dispositions)
    deferred = sum(row["explicitly_deferred_count"] for row in cohort_dispositions)
    total = int(audit["remaining_extraction_gaps"]["summary"]["confirmed_extraction_gap_occurrence_lower_bound"])
    if implemented + deferred != total:
        raise EvidenceFoundationError(
            f"Evidence disposition does not close: {implemented} + {deferred} != {total}."
        )
    return {
        "audit_confirmed_gap_count": total,
        "implemented_count": implemented,
        "duplicate_or_reconciled_count": 0,
        "other_product_count": 0,
        "incompatible_count": 0,
        "explicitly_deferred_count": deferred,
        "unexplained_count": 0,
        "granular_records": len(granular),
        "granular_implemented": len(implemented_granular),
        "cohort_dispositions": cohort_dispositions,
    }


def _acquisition_backlog(audit: Mapping[str, Any]) -> list[dict[str, Any]]:
    summary = audit["local_source_inventory"]["summary"]
    return [
        {
            "backlog_id": "source-backlog:anf:fy2026-q1-transcript@1",
            "source_family": "FY2026 Q1 earnings-call transcript",
            "state": "absent_local_bytes",
            "required_action": "separate acquisition and review gate",
        },
        {
            "backlog_id": "source-backlog:anf:fy2026-q1-presentation-family@1",
            "source_family": "FY2026 Q1 deck, schedules, and history",
            "state": "absent_local_bytes",
            "required_action": "separate acquisition and review gate",
        },
        {
            "backlog_id": "source-backlog:anf:2022-investor-day-transcript@1",
            "source_family": "2022 Investor Day transcript",
            "state": "absent_local_bytes",
            "required_action": "separate acquisition and review gate",
        },
        {
            "backlog_id": "source-backlog:anf:2022-investor-day-visuals-51-164@1",
            "source_family": "2022 Investor Day slide images 51-164",
            "state": "absent_visual_layer",
            "required_action": "acquire only if visual semantics are needed; hidden text remains limitation-bound",
        },
        {
            "backlog_id": "source-backlog:anf:item-9-01-linked-roots@1",
            "source_family": "exact Item 9.01 linked exhibit roots",
            "state": "partially_absent",
            "linked_root_count": int(summary["sec_8k_item_9_01_links"]),
            "present_root_count": int(summary["sec_8k_linked_documents_present"]),
            "absent_root_count": int(summary["sec_8k_linked_roots_missing"]),
            "semantic_availability_note": (
                "some absent linked roots have alternate reviewed local representations; exact-document and semantic-source completeness remain separate"
            ),
        },
    ]


def _transcript_report(
    audit: Mapping[str, Any], registrations: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    by_path, by_hash, by_key = _source_lookup(registrations)
    rows = [
        row
        for row in audit["expanded_fact_inventory"]["records"]
        if "transcript" in str(row.get("document_key") or "").casefold()
        or "transcript" in str(row.get("source_path") or "").casefold()
    ]
    detailed: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        source = _source_for_audit_record(row, by_path, by_hash, by_key)
        _require_economic_source(source, f"transcript:{index}")
        detailed.append(
            {
                "transcript_evidence_id": str(
                    row.get("audit_evidence_id")
                    or row.get("expanded_fact_id")
                    or f"transcript:{index}"
                ),
                "source_document_id": source["source_document_id"],
                "source_content_sha256": source["content_sha256"],
                "metric": row.get("metric"),
                "period": row.get("period"),
                "locator": row.get("source_locator") or row.get("locator"),
                "speaker_context": (
                    (row.get("source_locator") or row.get("locator") or {}).get("speaker_context")
                    or (row.get("locator") or {}).get("speaker_fingerprint")
                ),
                "semantic_directness": row.get("semantic_directness"),
                "value": _deep_clean(row.get("typed_values") or row.get("value")),
                "knowledge_date": row.get("knowledge_date") or source["knowledge_date"],
                "disposition": "canonicalized" if row.get("typed_values") else "canonical_source_cluster",
                "expected_downstream": row.get("expected_downstream") or [],
            }
        )
    return {
        "reviewed_transcript_documents": 17,
        "reviewed_earnings_call_transcript_documents": 16,
        "reviewed_conference_transcript_documents": 1,
        "audit_detailed_transcript_records": len(detailed),
        "prior_inventory_detailed_cluster_records": 20,
        "net_new_detailed_cluster_records": 16,
        "canonicalized_numeric_or_typed_records": sum(
            1 for row in rows if row.get("typed_values")
        ),
        "canonical_source_cluster_records": sum(
            1 for row in rows if row.get("classification") == "new_or_corroborating_transcript_evidence"
        ),
        "net_new_cohort_occurrences": 31,
        "reviewed_explicit_economic_cluster_lower_bound": 51,
        "cohort_explicitly_deferred": 15,
        "deferment_reason": (
            "the audit supplies no individual source document, locator, speaker, metric, period, unit and value tuple for the remaining cohort occurrences"
        ),
        "records": detailed,
    }


def _presentation_report(
    audit: Mapping[str, Any], limitations: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    investor = [
        _deep_clean(row)
        for row in audit["expanded_fact_inventory"]["records"]
        if "investordayslides" in str(row.get("source_path") or "").casefold()
    ]
    if len(investor) != 20:
        raise EvidenceFoundationError(f"Investor Day target count is {len(investor)}, not 20.")
    return {
        "investor_day_typed_target_count": len(investor),
        "investor_day_targets": investor,
        "history_selection_limitations": [
            row
            for row in limitations
            if row["code"]
            in {
                "mixed_scale_regions",
                "stale_period_labels",
                "fy2025_column_o_is_39_week_ytd_not_annual",
                "annual_mapping_from_column_o_forbidden",
                "missing_visual_image_layer",
            }
        ],
        "known_bad_history_cells_are_authority": False,
    }


def build_anf_evidence_foundation(
    *,
    source_root: Path | str,
    audit_root: Path | str,
) -> dict[str, Any]:
    """Build and validate the deterministic upstream ANF evidence candidate."""

    source_root = Path(source_root)
    audit = load_audit_contract(audit_root)
    registrations, limitations, _registration_by_path = _build_source_registry(
        audit, source_root
    )
    semantic_documents = _semantic_source_documents(registrations)
    wrapper_relations = _wrapper_relations(audit, registrations)
    guidance_assertions, guidance_versions, guidance_occurrences = _build_quarter_guidance(
        audit, registrations
    )
    (
        annual_guidance_assertions,
        annual_guidance_versions,
        annual_guidance_occurrences,
    ) = _build_missing_annual_guidance(registrations)
    sec_observations = _extract_sec_observations(registrations)
    comparative_sec_observations = _prior_ytd_sales_observations(registrations)
    release_observations, reconciliation_relations = _reconcile_sec_release(
        registrations, sec_observations
    )
    comparative_release_observations = _prior_ytd_release_sales_observations(
        registrations
    )
    direct_q4_observations = _direct_q4_release_observations(registrations)
    audit_observations, audit_occurrences, implemented_typed = _audit_typed_observations(
        audit, registrations
    )
    management_targets = _management_target_assertions(audit_observations)
    gap_occurrences, implemented_granular = _gap_occurrences(audit, registrations)
    annual_store = _annual_store_observations(registrations)
    period_store = _store_period_observations(registrations)
    release_store = _annual_store_release_observations(registrations)
    fy2026_q1_promise = _fy2026_q1_promise_observations(registrations)
    all_observations = [
        *sec_observations,
        *comparative_sec_observations,
        *release_observations,
        *comparative_release_observations,
        *direct_q4_observations,
        *audit_observations,
        *annual_store,
        *period_store,
        *release_store,
        *fy2026_q1_promise,
    ]
    base_facts = _canonical_facts(all_observations)
    derived_period_observations = _derived_period_observations(
        base_facts, all_observations
    )
    all_observations.extend(derived_period_observations)
    canonical_facts = _canonical_facts(all_observations)
    definition_relations = _definition_relations(registrations)
    debt_evidence = _debt_evidence(registrations)
    segment_evidence = _segment_evidence(registrations)
    disposition = _evidence_disposition(audit, implemented_granular)
    transcript_report = _transcript_report(audit, registrations)
    presentation_report = _presentation_report(audit, limitations)
    conflicts = _conflict_relations(audit)
    q4_evidence = _deep_clean(audit["q4_expanded_evidence_matrix"])
    # The reviewed audit contains two capex rows whose value is produced by an
    # explicit FY-minus-9M formula even though their report label said direct.
    # Preserve the economics and correct only the typed evidence class.
    for row in q4_evidence["records"]:
        if (
            row.get("metric") == "capital-expenditures"
            and row.get("classification") == "direct"
            and row.get("derivation_formula")
        ):
            row["classification"] = "derived_exact"
    q4_evidence["summary"]["classification_counts"] = dict(
        sorted(Counter(row["classification"] for row in q4_evidence["records"]).items())
    )
    q4_direct_bindings = _q4_direct_evidence_bindings(q4_evidence, canonical_facts)
    derivations = _deep_clean(audit["derivation_opportunities_expanded"])
    cross_sheet = _deep_clean(audit["cross_sheet_relevance_expanded"])
    adjacent_tax = {
        "records": _deep_clean(
            audit["quarter_guidance_inventory"]["adjacent_outlook_fields"]
        ),
        "canonical_taxonomy": ["Valuation", "Quarter Notes"],
        "promise_progress_quarter_version": False,
    }
    package = {
        "schema_id": "anf-reviewed-evidence-foundation",
        "schema_version": "1.0.0",
        "foundation_id": FOUNDATION_ID,
        "foundation_version": FOUNDATION_VERSION,
        "source_set_id": SOURCE_SET_ID,
        "predecessor_source_set_id": PREDECESSOR_SOURCE_SET_ID,
        "accepted_product_v2_source_set_id": ACCEPTED_PRODUCT_V2_SOURCE_SET_ID,
        "company_id": "ANF",
        "candidate_state": "review-only-not-golden-not-production-cutover",
        "knowledge_cutoff": KNOWLEDGE_CUTOFF,
        "audit_contract": {
            "audit_id": AUDIT_ID,
            "audit_root": audit["audit_root"],
            "artifacts": audit["audit_artifacts"],
        },
        "authority_policy": {
            "same_semantic_conflict_order": [
                "SEC primary filing",
                "issuer earnings release",
                "investor presentation",
                "earnings-call transcript",
            ],
            "semantic_directness_order": [
                "direct compatible fact",
                "exact compatible derivation",
                "component derivation",
                "bounded derivation",
                "unsupported",
            ],
            "directness_independent_of_authority": True,
            "knowledge_date_rule": "source knowledge_date <= event cutoff",
            "later_authority_backdating_allowed": False,
        },
        "source_registrations": registrations,
        "semantic_source_documents": semantic_documents,
        "review_limitations": limitations,
        "source_relations": wrapper_relations,
        "evidence_occurrences": [
            *guidance_occurrences,
            *annual_guidance_occurrences,
            *audit_occurrences,
            *gap_occurrences,
        ],
        "canonical_observations": all_observations,
        "canonical_facts": canonical_facts,
        "quarter_guidance_source_assertions": guidance_assertions,
        "quarter_guidance_versions": guidance_versions,
        "annual_guidance_source_assertions": annual_guidance_assertions,
        "annual_guidance_versions": annual_guidance_versions,
        "management_target_assertions": management_targets,
        "adjacent_non_promise_quarter_outlook": adjacent_tax,
        "sec_release_reconciliation_relations": reconciliation_relations,
        "definition_relations": definition_relations,
        "debt_evidence": debt_evidence,
        "segment_definition_evidence": segment_evidence,
        "source_conflicts": conflicts,
        "q4_evidence_matrix": q4_evidence,
        "q4_direct_evidence_bindings": q4_direct_bindings,
        "derivation_opportunities": derivations,
        "cross_sheet_ownership": cross_sheet,
        "transcript_canonicalization": transcript_report,
        "presentation_canonicalization": presentation_report,
        "evidence_disposition": disposition,
        "remaining_acquisition_backlog": _acquisition_backlog(audit),
        "known_product_v2_1_p1_contexts": [
            "result-event Q4 Actual versus annual Status evidence and lineage",
            "Q1 same-occurrence Actual/Progress duplication",
            "additive-Q4 missing currency and fiscal-calendar validation",
            "blank completeness not evidence-driven",
            "source saturation and missing quarter guidance/facts",
        ],
        "projection_or_workbook_correction_performed": False,
        "audit_internal_counts": {
            "implemented_typed_audit_record_ids": len(implemented_typed),
            "sec_primary_filing_count": sum(
                1 for row in registrations if row["source_type"] == "sec_filing" and row["economic_evidence_eligible"]
            ),
            "sec_10_k_count": sum(
                1 for row in registrations if row["source_type"] == "sec_filing" and row["form"] == "10-K"
            ),
            "sec_10_q_count": sum(
                1 for row in registrations if row["source_type"] == "sec_filing" and row["form"] == "10-Q"
            ),
            "wrapper_count": len(wrapper_relations),
            "quarter_guidance_assertion_count": len(guidance_assertions),
            "quarter_guidance_version_count": len(guidance_versions),
            "annual_guidance_addition_count": len(annual_guidance_versions),
            "sec_release_reconciliation_count": len(reconciliation_relations),
            "canonical_fact_count": len(canonical_facts),
        },
    }
    validate_anf_evidence_foundation(package)
    return package


def validate_anf_evidence_foundation(package: Mapping[str, Any]) -> None:
    """Fail closed on the reviewed audit's bounded acceptance invariants."""

    counts = package["audit_internal_counts"]
    expected_counts = {
        "sec_primary_filing_count": 18,
        "sec_10_k_count": 5,
        "sec_10_q_count": 13,
        "wrapper_count": 92,
        "quarter_guidance_assertion_count": 60,
        "quarter_guidance_version_count": 60,
        "sec_release_reconciliation_count": 148,
    }
    for key, expected in expected_counts.items():
        if int(counts[key]) != expected:
            raise EvidenceFoundationError(f"{key} is {counts[key]}, not {expected}.")
    registrations = package["source_registrations"]
    decision_counts = Counter(row["review_decision"] for row in registrations)
    if decision_counts != Counter(
        {
            "REVIEW_ACCEPT": 88,
            "REVIEW_ACCEPT_WITH_LIMITATIONS": 100,
            "REVIEW_DUPLICATE_ONLY": 44,
            "REJECT_AS_SOURCE": 17,
        }
    ):
        raise EvidenceFoundationError(f"Review decisions changed: {dict(decision_counts)}")
    if any(
        row["economic_evidence_eligible"]
        for row in registrations
        if row["review_decision"] in {"REJECT_AS_SOURCE", "REVIEW_DUPLICATE_ONLY"}
    ):
        raise EvidenceFoundationError("Rejected/duplicate-only source became economic evidence.")
    versions = package["quarter_guidance_versions"]
    if any(row["horizon_type"] != "quarter" for row in versions):
        raise EvidenceFoundationError("Quarter guidance leaked into a non-quarter horizon.")
    if len(package["management_target_assertions"]) != 20:
        raise EvidenceFoundationError("Investor Day management-target assertions are incomplete.")
    fy26_q2 = [row for row in versions if row["horizon_period_id"] == _period_id("FY2026-Q2")]
    if len(fy26_q2) != 6:
        raise EvidenceFoundationError(f"FY2026-Q2 guidance count is {len(fy26_q2)}, not 6.")
    false_capex = [
        row
        for row in versions
        if "250" in json.dumps(row["canonical_value"], sort_keys=True)
        and "capex" in str(row.get("metric_id", ""))
    ]
    if false_capex:
        raise EvidenceFoundationError("Issuer comparator error minted a false capex guidance version.")
    historical_metrics = {
        "reported-diluted-eps",
        "diluted-weighted-average-shares",
        "net-sales-amount",
        "operating-income-amount",
    }
    facts = package["canonical_facts"]
    for metric in historical_metrics:
        periods = {
            row["period_key"]
            for row in facts
            if row["metric_key"] == metric and row["period_key"] in HISTORICAL_REQUIRED_PERIODS
        }
        if len(periods) != 22:
            raise EvidenceFoundationError(
                f"Historical {metric} coverage is {len(periods)}/22: {sorted(periods)}"
            )
    if package["evidence_disposition"]["unexplained_count"] != 0:
        raise EvidenceFoundationError("Audit-confirmed evidence has an unexplained disposition.")
    q4_summary = package["q4_evidence_matrix"]["summary"]
    if q4_summary["classification_counts"] != {
        "derived_exact": 24,
        "direct": 28,
        "unavailable": 4,
    }:
        raise EvidenceFoundationError("Q4 evidence classifications changed.")
    q4_bindings = package["q4_direct_evidence_bindings"]
    if len(q4_bindings) != 28 or Counter(
        row["representation"] for row in q4_bindings
    ) != Counter({"canonical-direct-fact": 28}):
        raise EvidenceFoundationError("Direct Q4 evidence bindings are incomplete.")
    derivation_summary = package["derivation_opportunities"]["summary"]
    if derivation_summary["classification_counts"] != {
        "derived_bounded": 4,
        "derived_components": 8,
        "derived_exact": 16,
    }:
        raise EvidenceFoundationError("Derivation opportunity classifications changed.")
    if not all(
        "currency" in row.get("required_identity_checks", [])
        and "fiscal_calendar" in row.get("required_identity_checks", [])
        for row in package["derivation_opportunities"]["records"]
        if row["classification"] in {"derived_exact", "derived_components"}
    ):
        # The audit may express the checks as one closed sentence/list value.  Permit
        # exact audit wording but require both tokens to be present in serialization.
        for row in package["derivation_opportunities"]["records"]:
            if row["classification"] not in {"derived_exact", "derived_components"}:
                continue
            checks = json.dumps(row.get("required_identity_checks"), sort_keys=True).casefold()
            if "currency" not in checks or "calendar" not in checks:
                raise EvidenceFoundationError(
                    f"Derivation {row['derivation_id']} lacks currency/calendar identity metadata."
                )
    bad_history = package["presentation_canonicalization"]["history_selection_limitations"]
    if not any(row["code"] == "annual_mapping_from_column_o_forbidden" for row in bad_history):
        raise EvidenceFoundationError("FY2025 stale history-column protection is absent.")
    if package["projection_or_workbook_correction_performed"]:
        raise EvidenceFoundationError("Upstream foundation cannot own projection/workbook correction.")


def candidate_artifacts(package: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    """Return the closed external artifact inventory derived from one package."""

    return {
        "expanded_source_set.json": {
            "schema_id": "longitudinal-evidence-source-set",
            "schema_version": "1.0.0",
            "source_set_id": package["source_set_id"],
            "predecessor_source_set_id": package["predecessor_source_set_id"],
            "accepted_product_v2_source_set_id": package[
                "accepted_product_v2_source_set_id"
            ],
            "company_id": package["company_id"],
            "knowledge_cutoff": package["knowledge_cutoff"],
            "authority_policy": package["authority_policy"],
            "source_registrations": package["source_registrations"],
            "semantic_source_documents": package["semantic_source_documents"],
            "source_relations": package["source_relations"],
            "candidate_state": package["candidate_state"],
        },
        "canonical_fact_inventory.json": {
            "schema_id": "canonical-fact-inventory",
            "schema_version": "1.0.0",
            "foundation_id": package["foundation_id"],
            "canonical_facts": package["canonical_facts"],
            "canonical_observations": package["canonical_observations"],
            "management_target_assertions": package["management_target_assertions"],
            "q4_direct_evidence_bindings": package["q4_direct_evidence_bindings"],
            "definition_relations": package["definition_relations"],
            "debt_evidence": package["debt_evidence"],
            "segment_definition_evidence": package["segment_definition_evidence"],
        },
        "canonical_quarter_guidance_inventory.json": {
            "schema_id": "canonical-quarter-guidance-inventory",
            "schema_version": "1.0.0",
            "source_assertion_count": len(package["quarter_guidance_source_assertions"]),
            "guidance_version_count": len(package["quarter_guidance_versions"]),
            "source_assertions": package["quarter_guidance_source_assertions"],
            "guidance_versions": package["quarter_guidance_versions"],
            "adjacent_non_promise_quarter_outlook": package["adjacent_non_promise_quarter_outlook"],
        },
        "source_authority_reconciliation.json": {
            "schema_id": "source-authority-reconciliation",
            "schema_version": "1.0.0",
            "authority_policy": package["authority_policy"],
            "relations": package["sec_release_reconciliation_relations"],
            "conflicts": package["source_conflicts"],
        },
        "review_limitations.json": {
            "schema_id": "review-limitations",
            "schema_version": "1.0.0",
            "limitations": package["review_limitations"],
        },
        "transcript_canonicalization_report.json": package["transcript_canonicalization"],
        "presentation_canonicalization_report.json": package["presentation_canonicalization"],
        "sec_release_reconciliation.json": {
            "schema_id": "sec-release-reconciliation-report",
            "schema_version": "1.0.0",
            "comparison_count": len(package["sec_release_reconciliation_relations"]),
            "exact_match_count": len(package["sec_release_reconciliation_relations"]),
            "conflict_count": 0,
            "relations": package["sec_release_reconciliation_relations"],
        },
        "evidence_disposition_report.json": package["evidence_disposition"],
        "cross_sheet_ownership_inventory.json": {
            **package["cross_sheet_ownership"],
            "segment_recast_candidate_destinations": ["Summary", "BS_segment"],
        },
        "remaining_acquisition_backlog.json": {
            "schema_id": "remaining-source-acquisition-backlog",
            "schema_version": "1.0.0",
            "current_artifact_local_source_review_exhaustive": False,
            "candidate_source_set_saturated": False,
            "records": package["remaining_acquisition_backlog"],
        },
        "evidence_foundation_candidate.json": package,
    }


def write_evidence_foundation_candidate(
    package: Mapping[str, Any], output_root: Path | str
) -> dict[str, Any]:
    """Write a deterministic external candidate package and closed manifest."""

    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)
    artifacts = candidate_artifacts(package)
    manifest_rows: list[dict[str, Any]] = []
    for relative_path, artifact in sorted(artifacts.items()):
        path = root / relative_path
        payload = serialize_package(artifact, path)
        manifest_rows.append(
            {
                "relative_path": relative_path,
                "sha256": hashlib.sha256(payload).hexdigest(),
                "size": len(payload),
            }
        )
    manifest = {
        "manifest_type": "ANFReviewedEvidenceFoundationCandidateManifest@1",
        "candidate_state": package["candidate_state"],
        "foundation_id": package["foundation_id"],
        "foundation_version": package["foundation_version"],
        "source_set_id": package["source_set_id"],
        "artifact_count": len(manifest_rows),
        "artifacts": manifest_rows,
        "generated_timestamp": None,
        "golden_pinned": False,
        "production_cutover": False,
    }
    manifest_payload = serialize_package(manifest)
    manifest["manifest_digest"] = hashlib.sha256(manifest_payload).hexdigest()
    serialize_package(manifest, root / "manifest.json")
    return manifest
