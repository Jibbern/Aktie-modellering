from __future__ import annotations

import dataclasses
from typing import List


@dataclasses.dataclass(frozen=True)
class MetricSpec:
    name: str
    tags: List[str]
    kind: str   # "duration" or "instant" or "formula"
    unit: str   # "USD" or "shares"
    prefer_forms: List[str]


GAAP_SPECS: List[MetricSpec] = [
    MetricSpec("revenue", [
        "Revenues",
        "SalesRevenueNet",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
    ], "duration", "USD", ["10-Q", "10-K"]),
    MetricSpec("cogs", [
        "CostOfRevenue",
        "CostOfGoodsAndServicesSold",
        "CostOfGoodsSold",
        "CostOfSales",
        "CostOfSalesAndServices",
        "CostOfServices",
        "CostOfService",
        "CostOfProductsSold",
        "CostOfProductsAndServices",
        "CostOfServicesAndProducts",
        # Common service-cost variants (PBI)
        "CostOfServicesLicensesAndMaintenanceAgreements",
        "CostOfServicesMaintenanceCosts",
        "OtherCostOfServices",
    ], "duration", "USD", ["10-Q", "10-K"]),
    MetricSpec("gross_profit", ["GrossProfit"], "duration", "USD", ["10-Q", "10-K"]),
    MetricSpec("op_income", ["OperatingIncomeLoss", "IncomeLossFromOperations"], "duration", "USD", ["10-Q", "10-K"]),
    MetricSpec("net_income", ["NetIncomeLoss"], "duration", "USD", ["10-Q", "10-K"]),
    MetricSpec("cfo", [
        "NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
    ], "duration", "USD", ["10-Q", "10-K"]),
    MetricSpec("capex", [
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "PurchasesOfPropertyPlantAndEquipment",
        "PaymentsToAcquireProductiveAssets",
        "PaymentsToAcquirePropertyPlantAndEquipmentAndInterestCapitalized",
        "PaymentsToAcquirePropertyPlantAndEquipmentAndIntangibleAssets",
        "PurchasesOfPropertyPlantAndEquipmentAndIntangibleAssets",
    ], "duration", "USD", ["10-Q", "10-K"]),
    MetricSpec("interest_paid", [
        "InterestPaidNet",
        "InterestPaid",
        "InterestPaidNetOfCapitalizedInterest",
        "InterestPaidNetOfCapitalizedInterestAndDividendIncome",
    ], "duration", "USD", ["10-Q", "10-K"]),
    MetricSpec("interest_expense_net", [
        "InterestExpense",
        "InterestExpenseNonoperating",
        "InterestIncomeExpenseNet",
        "InterestExpenseNet",
        "InterestExpenseNetOfCapitalizedInterest",
        "InterestExpenseNetOfCapitalizedInterestAndDividendIncome",
    ], "duration", "USD", ["10-Q", "10-K"]),
    MetricSpec("tax_paid", [
        "IncomeTaxesPaidNet",
        "IncomeTaxesPaid",
        "IncomeTaxesPaidNetOfRefunds",
        "IncomeTaxesPaidNetOfRefundsAndCredits",
    ], "duration", "USD", ["10-Q", "10-K"]),
    MetricSpec("da", [
        "DepreciationAndAmortization",
        "DepreciationDepletionAndAmortization",
        "DepreciationDepletionAndAmortizationExpense",
        "DepreciationAmortizationAndAccretionNet",
        "Depreciation",
        "AmortizationOfIntangibleAssets",
    ], "duration", "USD", ["10-Q", "10-K"]),
    MetricSpec("research_and_development", [
        "ResearchAndDevelopmentExpense",
    ], "duration", "USD", ["10-Q", "10-K"]),
    MetricSpec("buybacks_cash", [
        "PaymentsForRepurchaseOfCommonStock",
    ], "duration", "USD", ["10-Q", "10-K"]),
    MetricSpec("dividends_cash", [
        "PaymentsOfDividendsCommonStock",
    ], "duration", "USD", ["10-Q", "10-K"]),
    MetricSpec("acquisitions_cash", [
        "PaymentsToAcquireBusinessesNetOfCashAcquired",
        "PaymentsToAcquireBusinessesGross",
    ], "duration", "USD", ["10-Q", "10-K"]),
    MetricSpec("debt_repayment", [
        "RepaymentsOfLongTermDebt",
        "RepaymentsOfLongTermDebtAndCapitalSecurities",
        "RepaymentsOfOtherLongTermDebt",
        "RepaymentsOfCommercialPaper",
    ], "duration", "USD", ["10-Q", "10-K"]),
    MetricSpec("debt_issuance", [
        "ProceedsFromIssuanceOfLongTermDebt",
        "ProceedsFromLongTermDebt",
        "ProceedsFromLongTermDebtAndCapitalSecurities",
        "ProceedsFromShortTermDebt",
        "ProceedsFromCommercialPaper",
    ], "duration", "USD", ["10-Q", "10-K"]),
    MetricSpec("cash", [
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
    ], "instant", "USD", ["10-Q", "10-K"]),
    MetricSpec("short_term_investments", [
        "ShortTermInvestments",
        "AvailableForSaleSecuritiesCurrent",
    ], "instant", "USD", ["10-Q", "10-K"]),
    MetricSpec("assets", [
        "Assets",
    ], "instant", "USD", ["10-Q", "10-K"]),
    MetricSpec("liabilities", [
        "Liabilities",
    ], "instant", "USD", ["10-Q", "10-K"]),
    MetricSpec("assets_current", [
        "AssetsCurrent",
    ], "instant", "USD", ["10-Q", "10-K"]),
    MetricSpec("liabilities_current", [
        "LiabilitiesCurrent",
    ], "instant", "USD", ["10-Q", "10-K"]),
    MetricSpec("accounts_receivable", [
        "AccountsReceivableNetCurrent",
        "AccountsReceivableNet",
    ], "instant", "USD", ["10-Q", "10-K"]),
    MetricSpec("inventory", [
        "InventoryNet",
        "InventoryFinishedGoods",
        "InventoryNetOfAllowances",
    ], "instant", "USD", ["10-Q", "10-K"]),
    MetricSpec("accounts_payable_current", [
        "AccountsPayableCurrent",
        "AccountsPayableTradeCurrent",
    ], "instant", "USD", ["10-Q", "10-K"]),
    MetricSpec("accrued_liabilities_current", [
        "AccruedLiabilitiesCurrent",
        "AccountsPayableAndAccruedLiabilitiesCurrent",
    ], "instant", "USD", ["10-Q", "10-K"]),
    MetricSpec("debt_current", [
        "DebtCurrent",
        "LongTermDebtCurrent",
        "CurrentMaturitiesOfLongTermDebt",
    ], "instant", "USD", ["10-Q", "10-K"]),
    MetricSpec("property_plant_equipment_net", [
        "PropertyPlantAndEquipmentNet",
        "PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAssetAfterAccumulatedDepreciationAndAmortization",
        "PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAssetNet",
    ], "instant", "USD", ["10-Q", "10-K"]),
    MetricSpec("other_assets_noncurrent", [
        "OtherAssetsNoncurrent",
        "OtherNoncurrentAssets",
    ], "instant", "USD", ["10-Q", "10-K"]),
    MetricSpec("other_liabilities_noncurrent", [
        "OtherLiabilitiesNoncurrent",
        "OtherNoncurrentLiabilities",
    ], "instant", "USD", ["10-Q", "10-K"]),
    MetricSpec("total_equity", [
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        "StockholdersEquity",
        "StockholdersDeficit",
        "TotalEquity",
    ], "instant", "USD", ["10-Q", "10-K"]),
    MetricSpec("goodwill", [
        "Goodwill",
    ], "instant", "USD", ["10-Q", "10-K"]),
    MetricSpec("intangibles", [
        "IntangibleAssetsNetExcludingGoodwill",
        "IntangibleAssetsNet",
    ], "instant", "USD", ["10-Q", "10-K"]),
    MetricSpec("shares_outstanding", [
        "CommonStockSharesOutstanding",
        "EntityCommonStockSharesOutstanding",
    ], "instant", "shares", ["10-Q", "10-K"]),
    MetricSpec("pension_obligation_net", [
        "PensionAndOtherPostretirementObligations",
        "PensionAndOtherPostretirementBenefitObligations",
        "PensionAndOtherPostretirementDefinedBenefitPlansLiabilitiesNoncurrent",
        "PensionAndOtherPostretirementDefinedBenefitPlansCurrentLiabilities",
        "DefinedBenefitPensionPlanLiabilitiesNoncurrent",
        "OtherPostretirementDefinedBenefitPlanLiabilitiesNoncurrent",
        "DefinedBenefitPlanAmountsRecognizedInBalanceSheet",
    ], "instant", "USD", ["10-Q", "10-K"]),
    MetricSpec("total_debt", [], "formula", "USD", ["10-Q", "10-K"]),
    MetricSpec("debt_core", [], "formula", "USD", ["10-Q", "10-K"]),
    MetricSpec("lease_liabilities", [], "formula", "USD", ["10-Q", "10-K"]),
    MetricSpec("bank_deposits", [], "formula", "USD", ["10-Q", "10-K"]),
    MetricSpec("bank_finance_receivables", [], "formula", "USD", ["10-Q", "10-K"]),
    MetricSpec("bank_net_funding", [], "formula", "USD", ["10-Q", "10-K"]),
    MetricSpec("shares_diluted", [
        "WeightedAverageNumberOfDilutedSharesOutstanding",
        "WeightedAverageNumberOfSharesOutstandingDiluted",
        "WeightedAverageNumberOfSharesOutstandingBasicAndDiluted",
    ], "duration", "shares", ["10-Q", "10-K"]),
]


DEBT_TAGS_ORDERED = [
    "Debt",  # if provided as a single total (best)
    "LongTermDebtAndCapitalLeaseObligations",
    # otherwise sum parts cautiously
    "LongTermDebt",
    "LongTermDebtCurrent",
    "DebtCurrent",
]


_ADJ_EBIT_SYNONYMS = [
    "adjusted ebit",
    "adjusted operating income",
    "adjusted income from operations",
    "adjusted income (loss) from operations",
]
_GAAP_EBIT_SYNONYMS = [
    "operating income",
    "income from operations",
    "income (loss) from operations",
    "net (loss) income",
    "net income",
    "loss before taxes",
    "income before taxes",
]


# Income statement parsing rules for 10-Q/10-K table fallback
INCOME_STATEMENT_RULES: dict = {
    "default": {
        "titles_any": [
            "statements of operations",
            "statements of income",
            "statements of earnings",
        ],
        "period_markers": ["three months ended", "three-months ended"],
        "require_labels": [
            "total revenue|total revenues|revenues",
            "total costs and expenses|total expenses|total costs",
        ],
        "revenue_primary": ["total revenue", "total revenues", "net revenue", "net revenues"],
        "revenue_parts": ["services", "products", "financing and other"],
        "cogs_parts": ["cost of services", "cost of products", "cost of financing and other"],
        "cogs_alt": ["cost of business services", "cost of revenue", "cost of revenues", "cost of sales"],
        "anti_labels": ["rental", "amortization", "interest", "depreciation", "fees"],
        "cogs_min_ratio": 0.15,
        "cogs_max_ratio": 0.95,
    },
    "PBI": {
        "titles_any": [
            "condensed consolidated statements of operations",
            "consolidated statements of operations",
            "statements of operations",
        ],
        "period_markers": ["three months ended", "three-months ended"],
        "require_labels": [
            "total revenue|total revenues|revenues",
            "total costs and expenses|total expenses|total costs",
        ],
        "revenue_primary": ["total revenue", "total revenues", "revenues"],
        "revenue_parts": [
            "services",
            "products",
            "financing and other",
            "business services",
            "support services",
            "financing",
            "equipment sales",
            "supplies",
            "rentals",
        ],
        "cogs_parts_sets": [
            ["cost of services", "cost of products", "cost of financing and other"],
            ["cost of financing", "cost of services", "cost of products"],
            ["cost of business services", "cost of support services", "cost of equipment sales", "cost of supplies", "cost of rentals"],
        ],
        "cogs_alt": ["cost of business services", "cost of revenue", "cost of revenues", "cost of sales"],
        "anti_labels": ["rental", "amortization", "interest", "depreciation", "fees"],
        "cogs_min_ratio": 0.15,
        "cogs_max_ratio": 0.95,
    },
}


def get_income_statement_rules(ticker: str | None) -> dict:
    if not ticker:
        return INCOME_STATEMENT_RULES["default"]
    return INCOME_STATEMENT_RULES.get(ticker.upper(), INCOME_STATEMENT_RULES["default"])
