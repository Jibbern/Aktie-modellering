from __future__ import annotations

import ast
import copy
import hashlib
import json
import re
from pathlib import Path
from typing import Any

import pytest


ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"

LIFECYCLE_PATH = DOCS / "SYSTEM_LIFECYCLE_REGISTRY.json"
OWNERSHIP_PATH = DOCS / "OWNERSHIP_REGISTRY.json"
IMPACT_PATH = DOCS / "CHANGE_IMPACT_REGISTRY.json"
GATES_PATH = DOCS / "APPROVAL_GATES.json"

EXPECTED_REGISTRY_IDS = {
    "lifecycle": "registry:system-lifecycle@1",
    "ownership": "registry:canonical-ownership@1",
    "impact": "registry:change-impact@1",
    "gates": "registry:approval-gates@1",
}


class DuplicateKeyError(ValueError):
    pass


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise DuplicateKeyError(key)
        value[key] = item
    return value


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_unique_object)
    assert isinstance(value, dict)
    return value


@pytest.fixture(scope="module")
def registries() -> dict[str, dict[str, Any]]:
    return {
        "lifecycle": _load(LIFECYCLE_PATH),
        "ownership": _load(OWNERSHIP_PATH),
        "impact": _load(IMPACT_PATH),
        "gates": _load(GATES_PATH),
    }


LIFECYCLE_TOP_KEYS = {
    "registry_id",
    "registry_version",
    "checkpoint",
    "purpose",
    "lifecycle_states",
    "authority_levels",
    "mutability_states",
    "production_statuses",
    "components",
}
COMPONENT_KEYS = {
    "component_id",
    "name",
    "lifecycle_state",
    "authority_level",
    "canonical_owner",
    "repository_paths",
    "producer",
    "consumers",
    "mutable_or_immutable",
    "production_status",
    "replacement_or_successor",
    "safe_extension_boundary",
    "prohibited_ownership",
    "notes",
}
OWNERSHIP_TOP_KEYS = {
    "registry_id",
    "registry_version",
    "checkpoint",
    "purpose",
    "ownership_states",
    "concepts",
}
CONCEPT_KEYS = {
    "concept_id",
    "name",
    "canonical_owner_component_id",
    "canonical_owner_paths",
    "public_interfaces",
    "current_consumers",
    "ownership_state",
    "parallel_owners",
    "allowed_extension_point",
    "prohibited_owners",
    "notes",
}
PARALLEL_OWNER_KEYS = {
    "component_id",
    "path",
    "lifecycle_state",
    "authority_limit",
}
IMPACT_TOP_KEYS = {
    "registry_id",
    "registry_version",
    "checkpoint",
    "purpose",
    "changes",
}
CHANGE_KEYS = {
    "change_id",
    "name",
    "canonical_owner_concept_ids",
    "likely_direct_consumers",
    "schemas_contracts",
    "tests",
    "goldens",
    "parity_oracle_artifacts",
    "products",
    "workbook_effects",
    "migration_requirement",
    "approval_gate_ids",
    "notes",
}
GATES_TOP_KEYS = {
    "registry_id",
    "registry_version",
    "checkpoint",
    "purpose",
    "review_modes",
    "gates",
}
GATE_KEYS = {
    "gate_id",
    "triggering_change_classes",
    "trigger",
    "why_approval_is_required",
    "required_evidence",
    "protected_scope",
    "review_mode",
    "dry_run_required",
    "allowed_automation_before_approval",
    "prohibited_automation_before_approval",
}


# These are reviewed semantic identities, not byte-for-byte registry goldens.
# Order and explanatory prose are intentionally outside the closed sets.
REVIEWED_PUBLIC_INTERFACES = {
    "concept:progress@1": frozenset(
        {"ProgressSelection", "CLOSED_PROGRESS_ROLE_IDS"}
    ),
    "concept:status@1": frozenset(
        {"StatusAssessment", "CLOSED_STATUS_RULE_IDS", "assess_status"}
    ),
    "concept:product-row-ordering@1": frozenset(
        {"BLOCK_ORDER", "stable row_id", "ordered visible rows"}
    ),
}
OBSOLETE_PROMISE_PROGRESS_INTERFACES = frozenset(
    {"PROGRESS_ROLE_IDS", "STATUS_RULE_IDS", "PRODUCT_BLOCK_ORDER"}
)
REVIEWED_STYLE_OWNER_PATHS = frozenset(
    {
        "templates/standard_stock_model_template.xlsx",
        "docs/standard_template_shell_manifest.json",
        "docs/standard_template_style_policy.json",
        "pbi_xbrl/new_ticker_style_planner.py",
        "pbi_xbrl/new_ticker_style_application.py",
    }
)
STYLEPLAN_DEFINING_PATH = "pbi_xbrl/new_ticker_style_planner.py"
REVIEWED_STYLE_CANONICAL_COMPONENT_ID = "component:workbook-style-contract@1"
REVIEWED_STYLE_PARALLEL_OWNERS = {
    "component:legacy-writer-semantics@1": {
        "lifecycle_state": "compatibility",
        "paths": frozenset({"legacy writer styling/scaffold"}),
    }
}

REVIEWED_SPLIT_OWNER_PATHS = {
    "concept:free-cash-flow@1": {
        "component:legacy-workbook-production@1": {
            "lifecycle_state": "active",
            "paths": frozenset(
                {
                    "pbi_xbrl/signals.py",
                    "pbi_xbrl/quarter_notes.py",
                    "pbi_xbrl/excel_writer_core.py",
                    "pbi_xbrl/valuation.py",
                }
            ),
        },
        "component:legacy-writer-semantics@1": {
            "lifecycle_state": "compatibility",
            "paths": frozenset(
                {
                    "pbi_xbrl/non_gaap.py",
                    "pbi_xbrl/excel_writer_investment_case_support.py",
                    "pbi_xbrl/excel_writer_latest_quarter_qa.py",
                }
            ),
        },
        "component:normalized-package-contract@1": {
            "lifecycle_state": "transition",
            "paths": frozenset(
                {
                    "docs/normalized_company_data.schema.json",
                    "pbi_xbrl/standard_template_formula_contract.py",
                    "pbi_xbrl/anf_capital_return_source_adapter.py",
                    "pbi_xbrl/new_ticker_capital_return.py",
                }
            ),
        },
    },
    "concept:net-debt@1": {
        "component:legacy-workbook-production@1": {
            "lifecycle_state": "active",
            "paths": frozenset(
                {
                    "pbi_xbrl/pipeline.py",
                    "pbi_xbrl/excel_writer_core.py",
                    "pbi_xbrl/valuation.py",
                }
            ),
        },
        "component:legacy-writer-semantics@1": {
            "lifecycle_state": "compatibility",
            "paths": frozenset(
                {
                    "pbi_xbrl/doc_intel.py",
                    "pbi_xbrl/excel_writer_investment_case_support.py",
                    "pbi_xbrl/excel_writer_latest_quarter_qa.py",
                }
            ),
        },
        "component:normalized-package-contract@1": {
            "lifecycle_state": "transition",
            "paths": frozenset(
                {
                    "docs/normalized_company_data.schema.json",
                    "pbi_xbrl/standard_template_formula_contract.py",
                    "pbi_xbrl/new_ticker_debt_scope.py",
                    "pbi_xbrl/new_ticker_debt_projection.py",
                    "pbi_xbrl/anf_debt_source_adapter.py",
                }
            ),
        },
    },
}

REVIEWED_CHANGE_OWNER_CONCEPTS = {
    "change:free-cash-flow-definition@1": frozenset(
        {
            "concept:free-cash-flow@1",
            "concept:valuation-economics@1",
            "concept:capital-allocation-economics@1",
        }
    ),
    "change:net-debt-definition@1": frozenset(
        {"concept:net-debt@1", "concept:valuation-economics@1"}
    ),
}

# Existing registry records are the structured extension routes.  The Markdown guide
# only points to these identities; its surrounding prose does not define ownership.
REVIEWED_EXTENSION_ROUTE_OWNER_CONCEPTS = {
    "change:metric-definition@1": frozenset(
        {
            "concept:metric-identity@1",
            "concept:definition-basis-unit@1",
            "concept:dimensions@1",
        }
    ),
    **REVIEWED_CHANGE_OWNER_CONCEPTS,
}
REVIEWED_EXTENSION_GUIDE_ROWS = {
    "New source-native/sector metric": frozenset(
        {"concept:metric-identity@1", "change:metric-definition@1"}
    ),
    "Existing split-owner FCF or net-debt change": frozenset(
        {
            "concept:free-cash-flow@1",
            "concept:net-debt@1",
            "change:free-cash-flow-definition@1",
            "change:net-debt-definition@1",
        }
    ),
}

REQUIRED_CHANGE_IMPACT_TERMS = {
    "change:free-cash-flow-definition@1": frozenset(
        {
            "signals",
            "quarter-note",
            "valuation",
            "investment case",
            "latest quarter qa",
            "company-adjusted fcf",
            "normalized",
            "capital-return",
        }
    ),
    "change:net-debt-definition@1": frozenset(
        {
            "pipeline",
            "cash",
            "valuation",
            "investment case",
            "latest quarter qa",
            "lease",
            "revolver",
            "leverage",
            "normalized",
            "debt detail",
        }
    ),
}

POST_NATIVE_OWNER_DISCOVERY = {
    "contract:semantic-cache-identity@1": {
        "component": "component:semantic-cache-identity@1",
        "path": "pbi_xbrl/cache_semantics.py",
        "change": "change:semantic-cache-identity@1",
        "gate": "gate:semantic-cache-contract-change@1",
    },
    "contract:inline-xbrl-fact-text@1": {
        "component": "component:inline-xbrl-fact-text@1",
        "path": "pbi_xbrl/inline_xbrl_text.py",
        "change": "change:inline-xbrl-fact-text@1",
        "gate": "gate:authority-order-change@1",
    },
    "contract:debt-rate-semantic-ownership@1": {
        "component": "component:debt-rate-semantic-ownership@1",
        "path": "pbi_xbrl/debt_rate_semantics.py",
        "change": "change:debt-rate-semantic-ownership@1",
        "gate": "gate:authority-order-change@1",
    },
    "concept:source-acquisition@1": {
        "component": "component:source-acquisition@1",
        "path": "pbi_xbrl/source_acquisition.py",
        "change": "change:source-acquisition-publication@1",
        "gate": "gate:reviewed-source-acquisition@1",
    },
    "concept:workbook-finalization-publication@1": {
        "component": "component:workbook-finalization-publication@1",
        "path": "pbi_xbrl/excel_writer_core.py",
        "change": "change:workbook-finalization-publication@1",
        "gate": "gate:workbook-publication-contract-change@1",
    },
    "concept:debt-source-duplicate-ownership@1": {
        "component": "component:debt-source-duplicate-ownership@1",
        "path": "pbi_xbrl/debt_source_registry.py",
        "change": "change:debt-source-duplicate-ownership@1",
        "gate": "gate:authority-order-change@1",
    },
    "concept:quarter-notes-intentionally-empty@1": {
        "component": "component:quarter-notes-empty-state@1",
        "path": "pbi_xbrl/pipeline_types.py",
        "change": "change:quarter-notes-empty-state@1",
        "gate": "gate:product-contract-change@1",
    },
    "concept:derivative-materialization-failure@1": {
        "component": "component:derivative-materialization-failure@1",
        "path": "pbi_xbrl/excel_writer_economics_overlay_derivatives.py",
        "change": "change:derivative-materialization-contract@1",
        "gate": "gate:product-contract-change@1",
    },
}

LIVE_CONTRACT_ASSIGNMENTS = {
    "pbi_xbrl/cache_semantics.py": {
        "CACHE_IDENTITY_CONTRACT": "contract:semantic-cache-identity@1",
        "CACHE_IDENTITY_SERIALIZATION_VERSION": "v1_canonical_json_sha256",
    },
    "pbi_xbrl/inline_xbrl_text.py": {
        "INLINE_XBRL_FACT_TEXT_CONTRACT_ID": "contract:inline-xbrl-fact-text@1",
    },
    "pbi_xbrl/debt_rate_semantics.py": {
        "DEBT_RATE_OWNERSHIP_CONTRACT_ID": "contract:debt-rate-semantic-ownership@1",
    },
}

OWNERSHIP_EXTENSIBLE_UNIQUE_FIELDS = (
    "canonical_owner_paths",
    "public_interfaces",
    "current_consumers",
    "prohibited_owners",
)
CHANGE_EXTENSIBLE_UNIQUE_FIELDS = (
    "canonical_owner_concept_ids",
    "likely_direct_consumers",
    "schemas_contracts",
    "tests",
    "goldens",
    "parity_oracle_artifacts",
    "products",
    "approval_gate_ids",
)


def _duplicates(values: list[str]) -> set[str]:
    return {value for value in values if values.count(value) > 1}


def _normalized_stable_id_identity(value: str) -> str:
    """Normalize only for alias detection; canonical references stay exact."""

    return value.strip().casefold()


def _stable_id_duplicates(values: list[str]) -> set[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        identity = _normalized_stable_id_identity(value)
        if identity in seen:
            duplicates.add(identity)
        seen.add(identity)
    return duplicates


def _normalized_text_identity(value: str) -> str:
    return " ".join(value.split()).casefold()


def _normalized_path_identity(value: str) -> str:
    normalized = value.strip().replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized.casefold()


def _semantic_duplicates(
    values: list[str], *, paths: bool = False
) -> set[str]:
    normalize = _normalized_path_identity if paths else _normalized_text_identity
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        identity = normalize(value)
        if identity in seen:
            duplicates.add(identity)
        seen.add(identity)
    return duplicates


def _parallel_path_atoms(value: str) -> list[str]:
    return [atom.strip() for atom in value.split(";") if atom.strip()]


def _structured_extension_route_errors(impact: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    changes = {
        row.get("change_id"): row
        for row in impact.get("changes", [])
        if isinstance(row, dict)
    }
    for change_id, expected_owners in REVIEWED_EXTENSION_ROUTE_OWNER_CONCEPTS.items():
        row = changes.get(change_id)
        if row is None:
            errors.append(f"missing structured extension route: {change_id}")
            continue
        owners = row.get("canonical_owner_concept_ids", [])
        if not isinstance(owners, list):
            errors.append(f"structured extension route owners are not a list: {change_id}")
            continue
        if _stable_id_duplicates(owners):
            errors.append(f"duplicate structured extension route owner: {change_id}")
        if frozenset(owners) != expected_owners:
            errors.append(f"structured extension route owner mismatch: {change_id}")
    return errors


def _extension_guide_reference_errors(
    text: str, registries: dict[str, dict[str, Any]]
) -> list[str]:
    """Validate explicit stable-ID links, never infer ownership from English prose."""

    errors: list[str] = []
    lines = text.splitlines()
    for label, expected_ids in REVIEWED_EXTENSION_GUIDE_ROWS.items():
        rows = [line for line in lines if line.startswith(f"| {label} |")]
        if len(rows) != 1:
            errors.append(f"extension guide route is missing or duplicated: {label}")
            continue
        explicit_ids = frozenset(
            re.findall(
                r"\b(?:concept|change):[A-Za-z0-9-]+@\d+\b",
                rows[0],
                flags=re.IGNORECASE,
            )
        )
        if explicit_ids != expected_ids:
            errors.append(f"extension guide route ID mismatch: {label}")

    known_ids = {
        row["component_id"]
        for row in registries["lifecycle"]["components"]
    } | {
        row["concept_id"] for row in registries["ownership"]["concepts"]
    } | {
        row["change_id"] for row in registries["impact"]["changes"]
    } | {
        row["gate_id"] for row in registries["gates"]["gates"]
    }
    for referenced_id in re.findall(
        r"\b(?:component|concept|change|gate):[A-Za-z0-9-]+@\d+\b",
        text,
        flags=re.IGNORECASE,
    ):
        if referenced_id not in known_ids:
            errors.append(f"unresolved exact extension-guide reference: {referenced_id}")
    return errors


def _extensible_unique_list_errors(
    row: dict[str, Any], fields: tuple[str, ...], *, identity: str
) -> list[str]:
    errors: list[str] = []
    for field in fields:
        values = row.get(field)
        if not isinstance(values, list) or not all(
            isinstance(value, str) and value.strip() for value in values
        ):
            errors.append(f"{field} is not a non-empty-string list: {identity}")
            continue
        if _semantic_duplicates(values, paths=field == "canonical_owner_paths"):
            errors.append(f"duplicate {field} semantic identity: {identity}")
    return errors


def _repository_path_errors(paths: list[str]) -> list[str]:
    errors: list[str] = []
    for path in paths:
        if not path or Path(path).is_absolute() or ":\\" in path or path.startswith("/"):
            errors.append(f"non-relative repository path: {path!r}")
        elif not (ROOT / path).exists():
            errors.append(f"missing repository path: {path!r}")
    return errors


def _top_level_symbols(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    symbols: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            symbols.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    symbols.add(target.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            symbols.add(node.target.id)
    return symbols


def _top_level_literal_assignments(path: Path) -> dict[str, Any]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    assignments: dict[str, Any] = {}
    for node in tree.body:
        targets: list[ast.expr] = []
        value: ast.expr | None = None
        if isinstance(node, ast.Assign):
            targets = list(node.targets)
            value = node.value
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
            value = node.value
        if value is None:
            continue
        for target in targets:
            if not isinstance(target, ast.Name):
                continue
            try:
                assignments[target.id] = ast.literal_eval(value)
            except (ValueError, TypeError):
                continue
    return assignments


def _lifecycle_errors(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if payload.get("registry_id") != EXPECTED_REGISTRY_IDS["lifecycle"]:
        errors.append("unexpected lifecycle registry ID")
    if set(payload) != LIFECYCLE_TOP_KEYS:
        errors.append("lifecycle top-level keys are not closed")
    components = payload.get("components", [])
    ids = [row.get("component_id") for row in components]
    if not all(isinstance(value, str) and value for value in ids) or _duplicates(ids):
        errors.append("component IDs are missing or duplicated")
    elif _stable_id_duplicates(ids):
        errors.append("component IDs have normalized semantic duplicates")
    known_ids = set(ids)
    for row in components:
        if set(row) != COMPONENT_KEYS:
            errors.append(f"component keys are not closed: {row.get('component_id')}")
        if row.get("lifecycle_state") not in payload.get("lifecycle_states", []):
            errors.append(f"unknown lifecycle state: {row.get('component_id')}")
        if row.get("authority_level") not in payload.get("authority_levels", []):
            errors.append(f"unknown authority level: {row.get('component_id')}")
        if row.get("mutable_or_immutable") not in payload.get("mutability_states", []):
            errors.append(f"unknown mutability state: {row.get('component_id')}")
        if row.get("production_status") not in payload.get("production_statuses", []):
            errors.append(f"unknown production status: {row.get('component_id')}")
        successor = row.get("replacement_or_successor")
        if successor is not None and successor not in known_ids:
            errors.append(f"unresolved successor: {successor}")
        errors.extend(_repository_path_errors(row.get("repository_paths", [])))
        for consumer in row.get("consumers", []):
            if isinstance(consumer, str) and consumer.startswith("component:") and consumer not in known_ids:
                errors.append(f"unresolved component consumer: {consumer}")
    return errors


def _reviewed_ownership_exactness_errors(
    payload: dict[str, Any], lifecycle: dict[str, Any]
) -> list[str]:
    errors: list[str] = []
    concepts = {
        row.get("concept_id"): row
        for row in payload.get("concepts", [])
        if isinstance(row, dict)
    }

    for concept_id, expected in REVIEWED_PUBLIC_INTERFACES.items():
        row = concepts.get(concept_id)
        if row is None:
            errors.append(f"missing reviewed public-interface concept: {concept_id}")
            continue
        values = row.get("public_interfaces", [])
        if not isinstance(values, list):
            errors.append(f"reviewed public interfaces are not a list: {concept_id}")
            continue
        if len(values) != len(set(values)):
            errors.append(f"duplicate reviewed public interface: {concept_id}")
        if frozenset(values) != expected:
            errors.append(f"reviewed public interface set mismatch: {concept_id}")

    all_interfaces = {
        value
        for row in payload.get("concepts", [])
        if isinstance(row, dict)
        for value in row.get("public_interfaces", [])
        if isinstance(value, str)
    }
    if all_interfaces & OBSOLETE_PROMISE_PROGRESS_INTERFACES:
        errors.append("obsolete Promise Progress public interface is advertised")

    style = concepts.get("concept:workbook-style@1")
    if style is None:
        errors.append("missing workbook-style ownership concept")
    else:
        if (
            style.get("canonical_owner_component_id")
            != REVIEWED_STYLE_CANONICAL_COMPONENT_ID
        ):
            errors.append("reviewed workbook-style canonical component mismatch")
        paths = style.get("canonical_owner_paths", [])
        normalized_paths = {
            _normalized_path_identity(value)
            for value in paths
            if isinstance(value, str)
        }
        expected_paths = {
            _normalized_path_identity(value) for value in REVIEWED_STYLE_OWNER_PATHS
        }
        if len(paths) != len(normalized_paths):
            errors.append("duplicate workbook-style canonical owner path")
        if normalized_paths != expected_paths:
            errors.append("reviewed workbook-style owner path set mismatch")

        components = {
            row.get("component_id"): row
            for row in lifecycle.get("components", [])
            if isinstance(row, dict)
        }
        canonical_component = components.get(REVIEWED_STYLE_CANONICAL_COMPONENT_ID)
        if canonical_component is None:
            errors.append("missing workbook-style canonical component")
        else:
            component_paths = {
                _normalized_path_identity(value)
                for value in canonical_component.get("repository_paths", [])
                if isinstance(value, str)
            }
            if component_paths != expected_paths:
                errors.append("workbook-style paths do not belong to canonical component")

        parallel_owners = style.get("parallel_owners", [])
        parallel_ids = [
            owner.get("component_id")
            for owner in parallel_owners
            if isinstance(owner, dict)
        ]
        if not all(isinstance(value, str) and value for value in parallel_ids):
            errors.append("workbook-style parallel owner IDs are missing")
        elif _stable_id_duplicates(parallel_ids):
            errors.append("duplicate workbook-style parallel owner ID")
        if frozenset(parallel_ids) != frozenset(REVIEWED_STYLE_PARALLEL_OWNERS):
            errors.append("reviewed workbook-style parallel owner set mismatch")
        for owner in parallel_owners:
            if not isinstance(owner, dict):
                continue
            component_id = owner.get("component_id")
            expected_owner = REVIEWED_STYLE_PARALLEL_OWNERS.get(component_id)
            if expected_owner is None:
                continue
            if owner.get("lifecycle_state") != expected_owner["lifecycle_state"]:
                errors.append(
                    f"reviewed workbook-style parallel lifecycle mismatch: {component_id}"
                )
            atoms = _parallel_path_atoms(str(owner.get("path") or ""))
            normalized_atoms = {
                _normalized_path_identity(value) for value in atoms
            }
            expected_atoms = {
                _normalized_path_identity(value)
                for value in expected_owner["paths"]
            }
            if len(atoms) != len(normalized_atoms):
                errors.append(
                    f"duplicate workbook-style parallel owner path: {component_id}"
                )
            if normalized_atoms != expected_atoms:
                errors.append(
                    f"reviewed workbook-style parallel owner path mismatch: {component_id}"
                )

    for concept_id, expected_owners in REVIEWED_SPLIT_OWNER_PATHS.items():
        row = concepts.get(concept_id)
        if row is None:
            errors.append(f"missing reviewed split-owner concept: {concept_id}")
            continue
        if row.get("canonical_owner_component_id") is not None:
            errors.append(f"split-owner concept gained a canonical owner: {concept_id}")
        if row.get("canonical_owner_paths") != []:
            errors.append(f"split-owner concept gained canonical paths: {concept_id}")
        owners = row.get("parallel_owners", [])
        owner_ids = [
            owner.get("component_id")
            for owner in owners
            if isinstance(owner, dict)
        ]
        if len(owner_ids) != len(set(owner_ids)):
            errors.append(f"duplicate reviewed parallel owner ID: {concept_id}")
        if frozenset(owner_ids) != frozenset(expected_owners):
            errors.append(f"reviewed parallel owner set mismatch: {concept_id}")
        for owner in owners:
            if not isinstance(owner, dict):
                continue
            component_id = owner.get("component_id")
            expected_owner = expected_owners.get(component_id)
            if expected_owner is None:
                continue
            if owner.get("lifecycle_state") != expected_owner["lifecycle_state"]:
                errors.append(
                    f"reviewed parallel owner lifecycle mismatch: {concept_id}:{component_id}"
                )
            atoms = _parallel_path_atoms(str(owner.get("path") or ""))
            normalized_atoms = {
                _normalized_path_identity(value) for value in atoms
            }
            expected_atoms = {
                _normalized_path_identity(value)
                for value in expected_owner["paths"]
            }
            if len(atoms) != len(normalized_atoms):
                errors.append(
                    f"duplicate reviewed parallel owner path: {concept_id}:{component_id}"
                )
            if normalized_atoms != expected_atoms:
                errors.append(
                    f"reviewed parallel owner path set mismatch: {concept_id}:{component_id}"
                )
    return errors


def _ownership_errors(
    payload: dict[str, Any], lifecycle: dict[str, Any]
) -> list[str]:
    errors: list[str] = []
    if payload.get("registry_id") != EXPECTED_REGISTRY_IDS["ownership"]:
        errors.append("unexpected ownership registry ID")
    if set(payload) != OWNERSHIP_TOP_KEYS:
        errors.append("ownership top-level keys are not closed")
    component_ids = {row["component_id"] for row in lifecycle["components"]}
    lifecycle_states = set(lifecycle["lifecycle_states"])
    concepts = payload.get("concepts", [])
    ids = [row.get("concept_id") for row in concepts]
    names = [row.get("name") for row in concepts]
    if not all(isinstance(value, str) and value for value in ids) or _duplicates(ids):
        errors.append("concept IDs are missing or duplicated")
    elif _stable_id_duplicates(ids):
        errors.append("concept IDs have normalized semantic duplicates")
    if None in names or _duplicates(names):
        errors.append("ownership concept names are missing or duplicated")
    for row in concepts:
        if set(row) != CONCEPT_KEYS:
            errors.append(f"concept keys are not closed: {row.get('concept_id')}")
        errors.extend(
            _extensible_unique_list_errors(
                row,
                OWNERSHIP_EXTENSIBLE_UNIQUE_FIELDS,
                identity=str(row.get("concept_id")),
            )
        )
        owner = row.get("canonical_owner_component_id")
        if owner is None:
            if row.get("ownership_state") != "transition_conflict":
                errors.append(f"ownerless concept is not transition-conflicted: {row.get('concept_id')}")
            if row.get("canonical_owner_paths"):
                errors.append(f"ownerless concept has canonical paths: {row.get('concept_id')}")
        elif owner not in component_ids:
            errors.append(f"unresolved canonical component: {row.get('concept_id')}")
        if row.get("ownership_state") not in payload.get("ownership_states", []):
            errors.append(f"unknown ownership state: {row.get('concept_id')}")
        errors.extend(_repository_path_errors(row.get("canonical_owner_paths", [])))
        parallel_owners = row.get("parallel_owners", [])
        parallel_ids = [
            parallel.get("component_id")
            for parallel in parallel_owners
            if isinstance(parallel, dict)
        ]
        if not all(
            isinstance(value, str) and value for value in parallel_ids
        ) or _duplicates(parallel_ids):
            errors.append(f"parallel owner IDs are missing or duplicated: {row.get('concept_id')}")
        elif _stable_id_duplicates(parallel_ids):
            errors.append(
                f"parallel owner IDs have normalized semantic duplicates: {row.get('concept_id')}"
            )
        for parallel in parallel_owners:
            if set(parallel) != PARALLEL_OWNER_KEYS:
                errors.append(f"parallel owner keys are not closed: {row.get('concept_id')}")
            if parallel.get("component_id") not in component_ids:
                errors.append(f"unresolved parallel component: {row.get('concept_id')}")
            if parallel.get("lifecycle_state") not in lifecycle_states:
                errors.append(f"unknown parallel lifecycle: {row.get('concept_id')}")
            repository_atoms = [
                atom
                for atom in _parallel_path_atoms(str(parallel.get("path") or ""))
                if (ROOT / atom).exists()
                or Path(atom).suffix.casefold()
                in {".py", ".json", ".md", ".xlsx", ".xlsm"}
            ]
            errors.extend(_repository_path_errors(repository_atoms))
    errors.extend(_reviewed_ownership_exactness_errors(payload, lifecycle))
    return errors


def _reviewed_change_exactness_errors(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    changes = {
        row.get("change_id"): row
        for row in payload.get("changes", [])
        if isinstance(row, dict)
    }
    for change_id, expected_owners in REVIEWED_CHANGE_OWNER_CONCEPTS.items():
        row = changes.get(change_id)
        if row is None:
            errors.append(f"missing reviewed change-impact route: {change_id}")
            continue
        owners = row.get("canonical_owner_concept_ids", [])
        if len(owners) != len(set(owners)):
            errors.append(f"duplicate reviewed change owner: {change_id}")
        if frozenset(owners) != expected_owners:
            errors.append(f"reviewed change owner set mismatch: {change_id}")

        searchable_values: list[str] = []
        for field in (
            "likely_direct_consumers",
            "schemas_contracts",
            "tests",
            "goldens",
            "parity_oracle_artifacts",
            "products",
        ):
            searchable_values.extend(
                value for value in row.get(field, []) if isinstance(value, str)
            )
        searchable_values.extend(
            str(row.get(field) or "")
            for field in (
                "workbook_effects",
                "migration_requirement",
                "notes",
            )
        )
        searchable = "\n".join(searchable_values).casefold()
        missing_terms = sorted(
            term
            for term in REQUIRED_CHANGE_IMPACT_TERMS[change_id]
            if term.casefold() not in searchable
        )
        if missing_terms:
            errors.append(
                f"reviewed change-impact categories missing: {change_id}: {missing_terms}"
            )
    return errors


def _impact_gate_errors(
    impact: dict[str, Any], gates: dict[str, Any], ownership: dict[str, Any]
) -> list[str]:
    errors: list[str] = []
    if impact.get("registry_id") != EXPECTED_REGISTRY_IDS["impact"]:
        errors.append("unexpected impact registry ID")
    if gates.get("registry_id") != EXPECTED_REGISTRY_IDS["gates"]:
        errors.append("unexpected approval-gate registry ID")
    if set(impact) != IMPACT_TOP_KEYS:
        errors.append("impact top-level keys are not closed")
    if set(gates) != GATES_TOP_KEYS:
        errors.append("approval top-level keys are not closed")
    concept_ids = {row["concept_id"] for row in ownership["concepts"]}
    changes = impact.get("changes", [])
    change_ids = [row.get("change_id") for row in changes]
    gate_rows = gates.get("gates", [])
    gate_ids = [row.get("gate_id") for row in gate_rows]
    if not all(
        isinstance(value, str) and value for value in change_ids
    ) or _duplicates(change_ids):
        errors.append("change IDs are missing or duplicated")
    elif _stable_id_duplicates(change_ids):
        errors.append("change IDs have normalized semantic duplicates")
    if not all(
        isinstance(value, str) and value for value in gate_ids
    ) or _duplicates(gate_ids):
        errors.append("gate IDs are missing or duplicated")
    elif _stable_id_duplicates(gate_ids):
        errors.append("gate IDs have normalized semantic duplicates")
    known_changes = set(change_ids)
    known_gates = set(gate_ids)
    gates_by_id = {row.get("gate_id"): row for row in gate_rows}
    for row in changes:
        if set(row) != CHANGE_KEYS:
            errors.append(f"change keys are not closed: {row.get('change_id')}")
        errors.extend(
            _extensible_unique_list_errors(
                row,
                CHANGE_EXTENSIBLE_UNIQUE_FIELDS,
                identity=str(row.get("change_id")),
            )
        )
        if not row.get("canonical_owner_concept_ids"):
            errors.append(f"change has no canonical owner: {row.get('change_id')}")
        for owner in row.get("canonical_owner_concept_ids", []):
            if owner not in concept_ids:
                errors.append(f"unresolved owner concept: {owner}")
        for gate in row.get("approval_gate_ids", []):
            if gate not in known_gates:
                errors.append(f"unresolved approval gate: {gate}")
            elif row.get("change_id") not in gates_by_id[gate].get("triggering_change_classes", []):
                errors.append(f"non-reciprocal approval gate: {row.get('change_id')} -> {gate}")
    for row in gate_rows:
        if set(row) != GATE_KEYS:
            errors.append(f"gate keys are not closed: {row.get('gate_id')}")
        if row.get("review_mode") not in gates.get("review_modes", []):
            errors.append(f"unknown review mode: {row.get('gate_id')}")
        if not isinstance(row.get("dry_run_required"), bool):
            errors.append(f"gate dry-run flag is not boolean: {row.get('gate_id')}")
        triggering_changes = row.get("triggering_change_classes", [])
        if _semantic_duplicates(triggering_changes):
            errors.append(f"duplicate triggering change: {row.get('gate_id')}")
        for change in row.get("triggering_change_classes", []):
            if change not in known_changes:
                errors.append(f"unresolved triggering change: {change}")
    errors.extend(_reviewed_change_exactness_errors(impact))
    errors.extend(_structured_extension_route_errors(impact))
    return errors


def _all_registry_errors(registries: dict[str, dict[str, Any]]) -> list[str]:
    registry_ids = [
        payload.get("registry_id") for payload in registries.values()
    ]
    registry_errors = []
    if all(isinstance(value, str) and value for value in registry_ids):
        if _stable_id_duplicates(registry_ids):
            registry_errors.append("registry IDs have normalized semantic duplicates")
    else:
        registry_errors.append("registry IDs are missing")
    return [
        *registry_errors,
        *_lifecycle_errors(registries["lifecycle"]),
        *_ownership_errors(registries["ownership"], registries["lifecycle"]),
        *_impact_gate_errors(
            registries["impact"], registries["gates"], registries["ownership"]
        ),
    ]


def _row_by_id(payload: dict[str, Any], collection: str, key: str, value: str) -> dict[str, Any]:
    return next(row for row in payload[collection] if row[key] == value)


def _parallel_owner(row: dict[str, Any], component_id: str) -> dict[str, Any]:
    return next(
        owner
        for owner in row["parallel_owners"]
        if owner["component_id"] == component_id
    )


def _new_parallel_owner(
    component_id: str, path: str, lifecycle_state: str, authority_limit: str
) -> dict[str, str]:
    return {
        "component_id": component_id,
        "path": path,
        "lifecycle_state": lifecycle_state,
        "authority_limit": authority_limit,
    }


def _apply_forbidden_coexistence_mutation(
    registries: dict[str, dict[str, Any]], case: str
) -> None:
    ownership = registries["ownership"]
    impact = registries["impact"]
    gates = registries["gates"]
    progress = _row_by_id(
        ownership, "concepts", "concept_id", "concept:progress@1"
    )
    status = _row_by_id(
        ownership, "concepts", "concept_id", "concept:status@1"
    )
    ordering = _row_by_id(
        ownership, "concepts", "concept_id", "concept:product-row-ordering@1"
    )
    style = _row_by_id(
        ownership, "concepts", "concept_id", "concept:workbook-style@1"
    )
    fcf = _row_by_id(
        ownership, "concepts", "concept_id", "concept:free-cash-flow@1"
    )
    net_debt = _row_by_id(
        ownership, "concepts", "concept_id", "concept:net-debt@1"
    )
    fcf_change = _row_by_id(
        impact, "changes", "change_id", "change:free-cash-flow-definition@1"
    )
    net_debt_change = _row_by_id(
        impact, "changes", "change_id", "change:net-debt-definition@1"
    )

    if case == "obsolete-progress-symbol":
        progress["public_interfaces"].append("PROGRESS_ROLE_IDS")
    elif case == "duplicate-progress-interface":
        progress["public_interfaces"].append("CLOSED_PROGRESS_ROLE_IDS")
    elif case == "obsolete-status-symbol":
        status["public_interfaces"].append("STATUS_RULE_IDS")
    elif case == "obsolete-block-order-symbol":
        ordering["public_interfaces"].append("PRODUCT_BLOCK_ORDER")
    elif case == "false-styleplan-owner-path":
        style["canonical_owner_paths"].append("pbi_xbrl/excel_writer_core.py")
    elif case == "duplicate-styleplan-owner-path":
        style["canonical_owner_paths"].append(STYLEPLAN_DEFINING_PATH)
    elif case == "unrelated-fcf-owner":
        fcf["parallel_owners"].append(
            _new_parallel_owner(
                "component:source-native-longitudinal-core@1",
                "pbi_xbrl/longitudinal_memory",
                "active",
                "Unreviewed global FCF owner.",
            )
        )
    elif case == "sector-pack-fcf-owner":
        fcf["parallel_owners"].append(
            _new_parallel_owner(
                "component:source-native-sector-packs@1",
                "pbi_xbrl/longitudinal_memory/sector_packs/business_services.py",
                "active",
                "Unreviewed sector-pack FCF owner.",
            )
        )
    elif case == "duplicate-fcf-owner":
        fcf["parallel_owners"].append(copy.deepcopy(fcf["parallel_owners"][0]))
    elif case == "unrelated-net-debt-owner":
        net_debt["parallel_owners"].append(
            _new_parallel_owner(
                "component:promise-progress-product@1",
                "pbi_xbrl/longitudinal_memory/promise_progress_projection.py",
                "active",
                "Unreviewed net-debt owner.",
            )
        )
    elif case == "debt-detail-as-net-debt-owner":
        net_debt["parallel_owners"].append(
            _new_parallel_owner(
                "component:normalized-frozen-shell-engine@1",
                "pbi_xbrl/new_ticker_debt_projection.py",
                "transition",
                "Debt Detail incorrectly owns summary net debt.",
            )
        )
    elif case == "duplicate-net-debt-owner":
        net_debt["parallel_owners"].append(
            copy.deepcopy(net_debt["parallel_owners"][0])
        )
    elif case == "presentation-as-economic-owner":
        fcf["parallel_owners"].append(
            _new_parallel_owner(
                "component:workbook-style-contract@1",
                "pbi_xbrl/new_ticker_style_application.py",
                "transition",
                "Presentation incorrectly owns FCF economics.",
            )
        )
    elif case == "unrelated-fcf-impact-owner":
        fcf_change["canonical_owner_concept_ids"].append("concept:status@1")
    elif case == "unrelated-net-debt-impact-owner":
        net_debt_change["canonical_owner_concept_ids"].append("concept:status@1")
    elif case == "duplicate-fcf-impact-owner":
        fcf_change["canonical_owner_concept_ids"].append(
            "concept:free-cash-flow@1"
        )
    elif case == "duplicate-owner-id-altered-prose":
        duplicate = copy.deepcopy(fcf["parallel_owners"][0])
        duplicate["authority_limit"] = "Cosmetically different duplicate owner prose."
        fcf["parallel_owners"].append(duplicate)
    elif case == "duplicate-change-impact-id":
        duplicate = copy.deepcopy(fcf_change)
        duplicate["name"] = "Cosmetically renamed duplicate FCF route"
        impact["changes"].append(duplicate)
    elif case == "duplicate-approval-gate-id":
        duplicate = copy.deepcopy(gates["gates"][0])
        duplicate["trigger"] = "Cosmetically changed duplicate gate"
        gates["gates"].append(duplicate)
    elif case == "prohibited-ticker-owner":
        net_debt["parallel_owners"].append(
            _new_parallel_owner(
                "component:source-native-ticker-profiles@1",
                "pbi_xbrl/longitudinal_memory/ticker_profiles/anf.py",
                "active",
                "Ticker profile incorrectly owns net-debt economics.",
            )
        )
    elif case == "consumer-promoted-to-fcf-owner":
        owner = _parallel_owner(fcf, "component:legacy-workbook-production@1")
        owner["path"] += "; pbi_xbrl/excel_writer_valuation_history_grid_render.py"
    elif case == "sector-path-hidden-in-reviewed-fcf-owner":
        owner = _parallel_owner(fcf, "component:normalized-package-contract@1")
        owner["path"] += "; pbi_xbrl/longitudinal_memory/sector_packs/business_services.py"
    else:
        raise AssertionError(f"unknown coexistence mutation: {case}")


FORBIDDEN_COEXISTENCE_CASES = (
    ("obsolete-progress-symbol", "reviewed public interface set mismatch"),
    ("duplicate-progress-interface", "duplicate reviewed public interface"),
    ("obsolete-status-symbol", "reviewed public interface set mismatch"),
    ("obsolete-block-order-symbol", "reviewed public interface set mismatch"),
    ("false-styleplan-owner-path", "reviewed workbook-style owner path set mismatch"),
    ("duplicate-styleplan-owner-path", "duplicate workbook-style canonical owner path"),
    ("unrelated-fcf-owner", "reviewed parallel owner set mismatch"),
    ("sector-pack-fcf-owner", "reviewed parallel owner set mismatch"),
    ("duplicate-fcf-owner", "duplicate reviewed parallel owner ID"),
    ("unrelated-net-debt-owner", "reviewed parallel owner set mismatch"),
    ("debt-detail-as-net-debt-owner", "reviewed parallel owner set mismatch"),
    ("duplicate-net-debt-owner", "duplicate reviewed parallel owner ID"),
    ("presentation-as-economic-owner", "reviewed parallel owner set mismatch"),
    ("unrelated-fcf-impact-owner", "reviewed change owner set mismatch"),
    ("unrelated-net-debt-impact-owner", "reviewed change owner set mismatch"),
    ("duplicate-fcf-impact-owner", "duplicate reviewed change owner"),
    ("duplicate-owner-id-altered-prose", "duplicate reviewed parallel owner ID"),
    ("duplicate-change-impact-id", "change IDs are missing or duplicated"),
    ("duplicate-approval-gate-id", "gate IDs are missing or duplicated"),
    ("prohibited-ticker-owner", "reviewed parallel owner set mismatch"),
    ("consumer-promoted-to-fcf-owner", "reviewed parallel owner path set mismatch"),
    ("sector-path-hidden-in-reviewed-fcf-owner", "reviewed parallel owner path set mismatch"),
)

STYLE_PARALLEL_OWNER_CASES = (
    "consumer-as-style-parallel-owner",
    "nonexistent-style-parallel-path",
    "compatibility-writer-as-style-parallel-owner",
    "canonical-style-owner-duplicated-as-parallel",
    "canonical-style-path-alias-as-parallel",
    "unrelated-existing-module-as-style-parallel-owner",
)

STABLE_ID_CASES = (
    ("exact-component-duplicate", "component IDs are missing or duplicated"),
    ("case-component-duplicate", "component IDs have normalized semantic duplicates"),
    ("leading-space-component-duplicate", "component IDs have normalized semantic duplicates"),
    ("trailing-space-component-duplicate", "component IDs have normalized semantic duplicates"),
    ("exact-gate-duplicate", "gate IDs are missing or duplicated"),
    ("case-gate-duplicate", "gate IDs have normalized semantic duplicates"),
    ("leading-space-gate-duplicate", "gate IDs have normalized semantic duplicates"),
    ("trailing-space-gate-duplicate", "gate IDs have normalized semantic duplicates"),
    ("case-concept-duplicate", "concept IDs have normalized semantic duplicates"),
    (
        "whitespace-parallel-owner-duplicate",
        "parallel owner IDs have normalized semantic duplicates",
    ),
    ("case-change-duplicate", "change IDs have normalized semantic duplicates"),
    ("wrong-case-canonical-reference", "unresolved canonical component"),
)

STRUCTURED_ROUTE_CASES = (
    "fcf-route-replaced-by-sector-concept",
    "fcf-route-adds-unrelated-concept",
    "net-debt-route-replaced-by-debt-detail",
    "net-debt-route-adds-unrelated-concept",
    "sector-metric-route-replaced-by-fcf",
    "fcf-guide-points-to-wrong-change-route",
    "net-debt-guide-points-to-wrong-change-route",
)


def _apply_style_parallel_owner_mutation(
    registries: dict[str, dict[str, Any]], case: str
) -> None:
    style = _row_by_id(
        registries["ownership"],
        "concepts",
        "concept_id",
        "concept:workbook-style@1",
    )
    if case == "consumer-as-style-parallel-owner":
        style["parallel_owners"].append(
            _new_parallel_owner(
                "component:normalized-frozen-shell-engine@1",
                "pbi_xbrl/new_engine_orchestration.py",
                "transition",
                "Consumer incorrectly promoted to style owner.",
            )
        )
    elif case == "nonexistent-style-parallel-path":
        style["parallel_owners"][0]["path"] += "; pbi_xbrl/nonexistent_style_owner.py"
    elif case == "compatibility-writer-as-style-parallel-owner":
        style["parallel_owners"].append(
            _new_parallel_owner(
                "component:legacy-workbook-production@1",
                "pbi_xbrl/excel_writer_core.py",
                "active",
                "Compatibility writer incorrectly promoted to StylePlan owner.",
            )
        )
    elif case == "canonical-style-owner-duplicated-as-parallel":
        style["parallel_owners"].append(
            _new_parallel_owner(
                REVIEWED_STYLE_CANONICAL_COMPONENT_ID,
                STYLEPLAN_DEFINING_PATH,
                "transition",
                "Canonical owner incorrectly duplicated as parallel owner.",
            )
        )
    elif case == "canonical-style-path-alias-as-parallel":
        style["parallel_owners"].append(
            _new_parallel_owner(
                REVIEWED_STYLE_CANONICAL_COMPONENT_ID,
                ".\\PBI_XBRL\\NEW_TICKER_STYLE_PLANNER.PY",
                "transition",
                "Normalized canonical path alias incorrectly added as parallel owner.",
            )
        )
    elif case == "unrelated-existing-module-as-style-parallel-owner":
        style["parallel_owners"].append(
            _new_parallel_owner(
                "component:legacy-workbook-production@1",
                "pbi_xbrl/valuation.py",
                "active",
                "Unrelated existing module incorrectly promoted to style owner.",
            )
        )
    else:
        raise AssertionError(f"unknown style mutation: {case}")


def _apply_stable_id_mutation(
    registries: dict[str, dict[str, Any]], case: str
) -> None:
    lifecycle = registries["lifecycle"]
    ownership = registries["ownership"]
    impact = registries["impact"]
    gates = registries["gates"]

    if "component-duplicate" in case:
        duplicate = copy.deepcopy(lifecycle["components"][0])
        original = duplicate["component_id"]
        if case == "case-component-duplicate":
            duplicate["component_id"] = original.upper()
            duplicate["name"] = "Cosmetically renamed component alias"
        elif case == "leading-space-component-duplicate":
            duplicate["component_id"] = f" {original}"
        elif case == "trailing-space-component-duplicate":
            duplicate["component_id"] = f"{original} "
        lifecycle["components"].append(duplicate)
    elif "gate-duplicate" in case:
        duplicate = copy.deepcopy(gates["gates"][0])
        original = duplicate["gate_id"]
        if case == "case-gate-duplicate":
            duplicate["gate_id"] = original.upper()
            duplicate["trigger"] = "Cosmetically changed gate alias"
        elif case == "leading-space-gate-duplicate":
            duplicate["gate_id"] = f" {original}"
        elif case == "trailing-space-gate-duplicate":
            duplicate["gate_id"] = f"{original} "
        gates["gates"].append(duplicate)
    elif case == "case-concept-duplicate":
        duplicate = copy.deepcopy(ownership["concepts"][0])
        duplicate["concept_id"] = duplicate["concept_id"].upper()
        ownership["concepts"].append(duplicate)
    elif case == "whitespace-parallel-owner-duplicate":
        fcf = _row_by_id(
            ownership, "concepts", "concept_id", "concept:free-cash-flow@1"
        )
        duplicate = copy.deepcopy(fcf["parallel_owners"][0])
        duplicate["component_id"] = f" {duplicate['component_id']} "
        fcf["parallel_owners"].append(duplicate)
    elif case == "case-change-duplicate":
        duplicate = copy.deepcopy(impact["changes"][0])
        duplicate["change_id"] = duplicate["change_id"].upper()
        duplicate["name"] = "Cosmetically renamed change alias"
        impact["changes"].append(duplicate)
    elif case == "wrong-case-canonical-reference":
        unified = next(
            row
            for row in ownership["concepts"]
            if row["canonical_owner_component_id"] is not None
        )
        unified["canonical_owner_component_id"] = unified[
            "canonical_owner_component_id"
        ].upper()
    else:
        raise AssertionError(f"unknown stable-ID mutation: {case}")


def _apply_structured_route_mutation(
    registries: dict[str, dict[str, Any]], case: str
) -> None:
    impact = registries["impact"]
    fcf = _row_by_id(
        impact, "changes", "change_id", "change:free-cash-flow-definition@1"
    )
    net_debt = _row_by_id(
        impact, "changes", "change_id", "change:net-debt-definition@1"
    )
    sector_metric = _row_by_id(
        impact, "changes", "change_id", "change:metric-definition@1"
    )
    if case == "fcf-route-replaced-by-sector-concept":
        fcf["canonical_owner_concept_ids"] = ["concept:metric-identity@1"]
    elif case == "fcf-route-adds-unrelated-concept":
        fcf["canonical_owner_concept_ids"].append("concept:metric-identity@1")
    elif case == "net-debt-route-replaced-by-debt-detail":
        net_debt["canonical_owner_concept_ids"] = ["concept:debt-detail@1"]
    elif case == "net-debt-route-adds-unrelated-concept":
        net_debt["canonical_owner_concept_ids"].append("concept:workbook-binding@1")
    elif case == "sector-metric-route-replaced-by-fcf":
        sector_metric["canonical_owner_concept_ids"] = [
            "concept:free-cash-flow@1"
        ]
    else:
        raise AssertionError(f"unknown structured registry route mutation: {case}")


def test_registries_are_strict_closed_and_cross_referenced(registries) -> None:
    assert _lifecycle_errors(registries["lifecycle"]) == []
    assert _ownership_errors(registries["ownership"], registries["lifecycle"]) == []
    assert _impact_gate_errors(
        registries["impact"], registries["gates"], registries["ownership"]
    ) == []


def test_registry_vocabularies_and_checkpoint_are_closed(registries) -> None:
    for name, payload in registries.items():
        assert payload["registry_id"] == EXPECTED_REGISTRY_IDS[name]
        assert payload["checkpoint"] == "05b9446b272ed91a7068affd0716ed66bd9046cc"
        assert payload["registry_version"] == "1.2.0"

    lifecycle = registries["lifecycle"]
    assert lifecycle["lifecycle_states"] == [
        "active",
        "compatibility",
        "transition",
        "target_not_wired",
        "oracle",
        "deprecated",
        "generated",
        "test_fixture",
        "audit_only",
        "archive",
    ]
    assert lifecycle["authority_levels"] == [
        "canonical_semantic",
        "operational_production",
        "presentation_contract",
        "compatibility_only",
        "parity_oracle",
        "transition_contract",
        "test_evidence",
        "audit_evidence",
        "none",
    ]
    assert lifecycle["mutability_states"] == [
        "immutable_contract",
        "immutable_artifact",
        "mutable_runtime",
        "mixed",
    ]
    assert lifecycle["production_statuses"] == [
        "active_production",
        "compatibility_production",
        "accepted_not_workbook_wired",
        "validated_transition",
        "not_implemented",
        "read_only_reference",
        "test_only",
        "generated_only",
        "nonproduction",
    ]
    assert registries["ownership"]["ownership_states"] == [
        "unified",
        "unified_unwired",
        "transition_conflict",
        "split_by_layer",
    ]
    assert registries["gates"]["review_modes"] == [
        "human_review",
        "human_acceptance",
        "human_authorization",
    ]


def test_lifecycle_states_keep_accepted_source_native_bridges_unwired(registries) -> None:
    by_id = {
        row["component_id"]: row for row in registries["lifecycle"]["components"]
    }
    assert by_id["component:legacy-workbook-production@1"]["lifecycle_state"] == "active"
    assert by_id["component:normalized-frozen-shell-engine@1"]["lifecycle_state"] == "transition"
    assert by_id["component:source-native-longitudinal-core@1"]["lifecycle_state"] == "active"
    assert by_id["component:promise-progress-product@1"]["lifecycle_state"] == "active"
    bridge = by_id["component:promise-progress-workbook-bridge@1"]
    assert bridge["lifecycle_state"] == "target_not_wired"
    assert bridge["production_status"] == "not_implemented"
    summary_bs_product = by_id["component:summary-bs-source-native-product@1"]
    assert summary_bs_product["lifecycle_state"] == "active"
    assert summary_bs_product["production_status"] == "accepted_not_workbook_wired"
    summary_bs_bridge = by_id["component:summary-bs-workbook-bridge@1"]
    assert summary_bs_bridge["lifecycle_state"] == "target_not_wired"
    assert summary_bs_bridge["production_status"] == "accepted_not_workbook_wired"
    valuation_product = by_id["component:valuation-source-native-product@1"]
    assert valuation_product["lifecycle_state"] == "active"
    assert valuation_product["production_status"] == "accepted_not_workbook_wired"
    valuation_bridge = by_id["component:valuation-workbook-bridge@1"]
    assert valuation_bridge["lifecycle_state"] == "target_not_wired"
    assert valuation_bridge["production_status"] == "accepted_not_workbook_wired"
    oracle = by_id["component:legacy-workbook-oracles@1"]
    assert oracle["lifecycle_state"] == "oracle"
    assert oracle["authority_level"] == "parity_oracle"


def test_required_ownership_and_change_classes_are_complete(registries) -> None:
    concept_ids = {row["concept_id"] for row in registries["ownership"]["concepts"]}
    assert {
        "concept:source-acquisition@1",
        "concept:source-discovery@1",
        "concept:source-document-identity@1",
        "concept:evidence-occurrence@1",
        "concept:source-authority@1",
        "concept:publication-knowledge-dates@1",
        "concept:fiscal-calendar@1",
        "concept:period@1",
        "concept:metric-identity@1",
        "concept:definition-basis-unit@1",
        "concept:dimensions@1",
        "concept:guidance-series-version@1",
        "concept:promise-version@1",
        "concept:company-event@1",
        "concept:canonical-resolution@1",
        "concept:change-observation@1",
        "concept:actual@1",
        "concept:progress@1",
        "concept:status@1",
        "concept:product-row-ordering@1",
        "concept:product-display-text@1",
        "concept:promise-progress-projection@1",
        "concept:summary-bs-source-native-product@1",
        "concept:summary-bs-workbook-projection@1",
        "concept:valuation-workbook-projection@1",
        "concept:normalized-package@1",
        "concept:workbook-binding@1",
        "concept:workbook-style@1",
        "concept:free-cash-flow@1",
        "concept:net-debt@1",
        "concept:valuation-economics@1",
        "concept:capital-allocation-economics@1",
        "contract:semantic-cache-identity@1",
        "contract:inline-xbrl-fact-text@1",
        "contract:debt-rate-semantic-ownership@1",
        "concept:debt-source-duplicate-ownership@1",
        "concept:workbook-finalization-publication@1",
        "concept:quarter-notes-intentionally-empty@1",
        "concept:derivative-materialization-failure@1",
    } == concept_ids

    change_ids = {row["change_id"] for row in registries["impact"]["changes"]}
    assert change_ids == {
        "change:canonical-schema-field@1",
        "change:fiscal-calendar-rule@1",
        "change:source-role@1",
        "change:metric-definition@1",
        "change:free-cash-flow-definition@1",
        "change:net-debt-definition@1",
        "change:guidance-rule@1",
        "change:promise-status-rule@1",
        "change:product-field@1",
        "change:promise-progress-row-block@1",
        "change:workbook-destination-binding@1",
        "change:presentation-style-only@1",
        "change:new-ticker-activation@1",
        "change:new-sector-concept@1",
        "change:semantic-cache-identity@1",
        "change:inline-xbrl-fact-text@1",
        "change:debt-rate-semantic-ownership@1",
        "change:debt-source-duplicate-ownership@1",
        "change:source-acquisition-publication@1",
        "change:workbook-finalization-publication@1",
        "change:quarter-notes-empty-state@1",
        "change:derivative-materialization-contract@1",
    }


def test_registry_serialization_is_deterministic(registries) -> None:
    for payload in registries.values():
        first = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
        replay = json.loads(first.decode("utf-8"), object_pairs_hook=_unique_object)
        second = json.dumps(
            replay, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
        assert first == second
        assert hashlib.sha256(first).digest() == hashlib.sha256(second).digest()


def test_registry_validation_fails_closed_on_representative_mutations(registries) -> None:
    lifecycle = copy.deepcopy(registries["lifecycle"])
    lifecycle["registry_id"] = "registry:wrong@1"
    assert any("registry ID" in row for row in _lifecycle_errors(lifecycle))

    lifecycle = copy.deepcopy(registries["lifecycle"])
    lifecycle["components"][0]["lifecycle_state"] = "approximately-active"
    assert any("unknown lifecycle state" in row for row in _lifecycle_errors(lifecycle))

    lifecycle = copy.deepcopy(registries["lifecycle"])
    lifecycle["components"].append(copy.deepcopy(lifecycle["components"][0]))
    assert any("duplicated" in row for row in _lifecycle_errors(lifecycle))

    ownership = copy.deepcopy(registries["ownership"])
    ownership["concepts"][0]["canonical_owner_component_id"] = "component:missing@1"
    assert any(
        "unresolved canonical component" in row
        for row in _ownership_errors(ownership, registries["lifecycle"])
    )

    impact = copy.deepcopy(registries["impact"])
    impact["changes"][0]["approval_gate_ids"] = ["gate:missing@1"]
    assert any(
        "unresolved approval gate" in row
        for row in _impact_gate_errors(impact, registries["gates"], registries["ownership"])
    )

    ownership = copy.deepcopy(registries["ownership"])
    ownership["concepts"][0]["canonical_owner_component_id"] = None
    assert any(
        "ownerless concept" in row
        for row in _ownership_errors(ownership, registries["lifecycle"])
    )


@pytest.mark.parametrize(
    ("case", "expected_diagnostic"),
    FORBIDDEN_COEXISTENCE_CASES,
    ids=[case for case, _ in FORBIDDEN_COEXISTENCE_CASES],
)
def test_forbidden_contradictory_metadata_cannot_coexist(
    registries, case: str, expected_diagnostic: str
) -> None:
    mutated = copy.deepcopy(registries)
    _apply_forbidden_coexistence_mutation(mutated, case)
    errors = _all_registry_errors(mutated)
    assert errors, case
    assert errors == _all_registry_errors(mutated), case
    assert any(expected_diagnostic in error for error in errors), (case, errors)


@pytest.mark.parametrize("case", STYLE_PARALLEL_OWNER_CASES)
def test_workbook_style_parallel_owner_cardinality_is_closed(
    registries, case: str
) -> None:
    mutated = copy.deepcopy(registries)
    _apply_style_parallel_owner_mutation(mutated, case)
    errors = _all_registry_errors(mutated)
    assert errors == _all_registry_errors(mutated), case
    assert any("workbook-style parallel owner" in error for error in errors), (
        case,
        errors,
    )


@pytest.mark.parametrize(
    ("case", "expected_diagnostic"),
    STABLE_ID_CASES,
    ids=[case for case, _ in STABLE_ID_CASES],
)
def test_stable_ids_reject_exact_and_normalized_aliases(
    registries, case: str, expected_diagnostic: str
) -> None:
    mutated = copy.deepcopy(registries)
    _apply_stable_id_mutation(mutated, case)
    errors = _all_registry_errors(mutated)
    assert errors == _all_registry_errors(mutated), case
    assert any(expected_diagnostic in error for error in errors), (case, errors)


@pytest.mark.parametrize(
    "case",
    STRUCTURED_ROUTE_CASES[:5],
)
def test_structured_extension_routes_reject_false_owners(
    registries, case: str
) -> None:
    mutated = copy.deepcopy(registries)
    _apply_structured_route_mutation(mutated, case)
    errors = _all_registry_errors(mutated)
    assert errors == _all_registry_errors(mutated), case
    assert any("structured extension route owner mismatch" in error for error in errors), (
        case,
        errors,
    )


@pytest.mark.parametrize(
    ("source_id", "wrong_id", "label"),
    (
        (
            "change:free-cash-flow-definition@1",
            "change:net-debt-definition@1",
            "Existing split-owner FCF or net-debt change",
        ),
        (
            "change:net-debt-definition@1",
            "change:free-cash-flow-definition@1",
            "Existing split-owner FCF or net-debt change",
        ),
    ),
)
def test_extension_guide_rejects_wrong_machine_route_ids(
    registries, source_id: str, wrong_id: str, label: str
) -> None:
    extensions = (DOCS / "EXTENSION_POINTS.md").read_text(encoding="utf-8")
    mutated = extensions.replace(source_id, wrong_id, 1)
    errors = _extension_guide_reference_errors(mutated, registries)
    assert errors == _extension_guide_reference_errors(mutated, registries)
    assert f"extension guide route ID mismatch: {label}" in errors


def test_descriptive_metadata_remains_extensible_and_order_independent(registries) -> None:
    mutated = copy.deepcopy(registries)
    concepts = {
        row["concept_id"]: row for row in mutated["ownership"]["concepts"]
    }
    fcf = concepts["concept:free-cash-flow@1"]
    progress = concepts["concept:progress@1"]
    style = concepts["concept:workbook-style@1"]

    fcf["parallel_owners"].reverse()
    fcf["current_consumers"].append("future reviewed read-only FCF consumer")
    fcf["notes"] = "Updated explanatory prose; executable contracts remain authority."
    fcf["parallel_owners"][0]["authority_limit"] = (
        "Updated explanatory scope without changing semantic owner identity."
    )
    progress["public_interfaces"].reverse()
    style["canonical_owner_paths"].reverse()
    style["current_consumers"].append("future reviewed presentation consumer")
    style["current_consumers"].reverse()
    style["notes"] = "Updated explanatory style guidance."

    fcf_change = _row_by_id(
        mutated["impact"],
        "changes",
        "change_id",
        "change:free-cash-flow-definition@1",
    )
    fcf_change["canonical_owner_concept_ids"].reverse()
    fcf_change["likely_direct_consumers"].reverse()
    fcf_change["likely_direct_consumers"].append(
        "future reviewed downstream FCF consumer"
    )
    fcf_change["notes"] = (
        "Updated explanatory routing; executable contracts remain authority."
    )

    assert _all_registry_errors(mutated) == []


def test_extension_guide_prose_is_non_authoritative_but_ids_are_exact(
    registries,
) -> None:
    extensions = (DOCS / "EXTENSION_POINTS.md").read_text(encoding="utf-8")
    assert _extension_guide_reference_errors(extensions, registries) == []
    assert _structured_extension_route_errors(registries["impact"]) == []

    # This sentence is intentionally not parsed as architecture.  It cannot change
    # the structured route, while the equivalent registry mutation is rejected.
    contradictory = (
        extensions
        + "\nThe business-services sector pack is the authoritative economics owner "
        "for existing FCF and net debt.\n"
    )
    assert _extension_guide_reference_errors(contradictory, registries) == []

    mutated = copy.deepcopy(registries)
    _apply_structured_route_mutation(
        mutated, "fcf-route-replaced-by-sector-concept"
    )
    assert "structured extension route owner mismatch: change:free-cash-flow-definition@1" in (
        _structured_extension_route_errors(mutated["impact"])
    )


def test_split_fcf_net_debt_routes_and_documented_symbols_match_code(registries) -> None:
    concepts = {row["concept_id"]: row for row in registries["ownership"]["concepts"]}
    changes = {row["change_id"]: row for row in registries["impact"]["changes"]}

    fcf = concepts["concept:free-cash-flow@1"]
    net_debt = concepts["concept:net-debt@1"]
    for row in (fcf, net_debt):
        assert row["ownership_state"] == "transition_conflict"
        assert row["canonical_owner_component_id"] is None
        assert row["canonical_owner_paths"] == []
        assert {owner["component_id"] for owner in row["parallel_owners"]} == {
            "component:legacy-workbook-production@1",
            "component:legacy-writer-semantics@1",
            "component:normalized-package-contract@1",
        }
        for owner in row["parallel_owners"]:
            for path in owner["path"].split("; "):
                assert (ROOT / path).exists(), path

    assert changes["change:free-cash-flow-definition@1"]["canonical_owner_concept_ids"][0] == fcf["concept_id"]
    assert changes["change:net-debt-definition@1"]["canonical_owner_concept_ids"][0] == net_debt["concept_id"]
    assert "change:free-cash-flow-definition@1" in changes["change:metric-definition@1"]["notes"]
    assert "change:net-debt-definition@1" in changes["change:metric-definition@1"]["notes"]

    projection_symbols = _top_level_symbols(
        ROOT / "pbi_xbrl/longitudinal_memory/promise_progress_projection.py"
    )
    assert {
        "BLOCK_ORDER",
        "ProgressSelection",
        "CLOSED_PROGRESS_ROLE_IDS",
        "StatusAssessment",
        "CLOSED_STATUS_RULE_IDS",
        "assess_status",
    } <= projection_symbols
    for concept_id, expected in REVIEWED_PUBLIC_INTERFACES.items():
        assert frozenset(concepts[concept_id]["public_interfaces"]) == expected
    assert not (
        {
            interface
            for row in concepts.values()
            for interface in row["public_interfaces"]
        }
        & OBSOLETE_PROMISE_PROGRESS_INTERFACES
    )

    style = concepts["concept:workbook-style@1"]
    assert frozenset(style["canonical_owner_paths"]) == REVIEWED_STYLE_OWNER_PATHS
    styleplan_defining_paths = [
        path
        for path in style["canonical_owner_paths"]
        if path.endswith(".py") and "StylePlan" in _top_level_symbols(ROOT / path)
    ]
    assert styleplan_defining_paths == [STYLEPLAN_DEFINING_PATH]

    lifecycle = {row["component_id"]: row for row in registries["lifecycle"]["components"]}
    writer_notes = lifecycle["component:legacy-writer-semantics@1"]["notes"]
    workbook_notes = lifecycle["component:legacy-workbook-production@1"]["notes"]
    assert "WorkbookFinalizationError" in writer_notes
    assert "candidate-isolated" in workbook_notes and "atomic" in workbook_notes

    extensions = (DOCS / "EXTENSION_POINTS.md").read_text(encoding="utf-8")
    assert _extension_guide_reference_errors(extensions, registries) == []
    assert _structured_extension_route_errors(registries["impact"]) == []
    assert "Canonical schema field change" in extensions
    assert "universal schema owner" in extensions
    assert "first name the exact schema" in extensions
    assert "true repository cold start" in extensions


def test_post_native_owner_discovery_is_exact_and_runtime_backed(registries) -> None:
    components = {
        row["component_id"]: row for row in registries["lifecycle"]["components"]
    }
    concepts = {
        row["concept_id"]: row for row in registries["ownership"]["concepts"]
    }
    changes = {row["change_id"]: row for row in registries["impact"]["changes"]}
    gates = {row["gate_id"]: row for row in registries["gates"]["gates"]}

    assert len(POST_NATIVE_OWNER_DISCOVERY) == 8
    for concept_id, expected in POST_NATIVE_OWNER_DISCOVERY.items():
        assert sum(row["concept_id"] == concept_id for row in concepts.values()) == 1
        concept = concepts[concept_id]
        assert concept["canonical_owner_component_id"] == expected["component"]
        assert expected["path"] in concept["canonical_owner_paths"]
        assert expected["path"] in components[expected["component"]]["repository_paths"]

        change = changes[expected["change"]]
        assert concept_id in change["canonical_owner_concept_ids"]
        assert expected["gate"] in change["approval_gate_ids"]
        assert expected["change"] in gates[expected["gate"]]["triggering_change_classes"]

    for relative_path, assignments in LIVE_CONTRACT_ASSIGNMENTS.items():
        live = _top_level_literal_assignments(ROOT / relative_path)
        for symbol, expected_value in assignments.items():
            assert live[symbol] == expected_value

    cache_notes = concepts["contract:semantic-cache-identity@1"]["notes"]
    for exact_version in (
        "unit_norm=v1_table_local_source_unit",
        "adjustment_domain=v1_table_role_measure_domain",
        "document_period=v2_registered_document_identity",
        "debt_period=v1_visual_xbrl_context",
        "adjusted_history=v1_metric_definition_scope",
        "inline_xbrl_text=v1_continued_at_chain",
        "debt_rate=v1_role_period_authority",
    ):
        assert exact_version in cache_notes

    promise_bridge = components["component:promise-progress-workbook-bridge@1"]
    assert promise_bridge["lifecycle_state"] == "target_not_wired"
    assert promise_bridge["production_status"] == "not_implemented"

    extensions = (DOCS / "EXTENSION_POINTS.md").read_text(encoding="utf-8")
    assert "component:source-native-ticker-profiles@1" in extensions
    assert "shared source/domain engine -> reusable sector pack -> declarative ticker profile" in extensions
    assert "New debt rate role" in extensions
    assert "change:debt-rate-semantic-ownership@1" in extensions


def test_top_level_docs_route_agents_to_the_discoverability_layer() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    overview = (DOCS / "SYSTEM_OVERVIEW.md").read_text(encoding="utf-8")
    codebase_map = (DOCS / "CODEBASE_MAP.md").read_text(encoding="utf-8")
    extensions = (DOCS / "EXTENSION_POINTS.md").read_text(encoding="utf-8")

    for name in (
        "SYSTEM_LIFECYCLE_REGISTRY.json",
        "OWNERSHIP_REGISTRY.json",
        "EXTENSION_POINTS.md",
        "CHANGE_IMPACT_REGISTRY.json",
        "APPROVAL_GATES.json",
    ):
        assert name in readme
        assert name in codebase_map
    assert "target_not_wired" in overview
    assert "PromiseProgressProduct@1" in codebase_map
    assert "no current product consumer reads it" not in codebase_map.casefold()
    assert (
        "shared source/domain engine -> reusable sector pack -> declarative ticker profile"
        in extensions
    )
    assert "evidence_backed_synthesis" in extensions
    assert "Legacy workbook oracle" in extensions
    assert "Search guidance" in extensions
