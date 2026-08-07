from __future__ import annotations

import copy
import dataclasses
import hashlib
import json
import os
import random
import subprocess
import sys
from collections import Counter
from pathlib import Path

import pytest

from pbi_xbrl.json_schema_validation import load_json_strict, validate_json_schema
from pbi_xbrl.longitudinal_memory.promise_progress_projection import (
    ACTUAL_COVERAGE_EXCEPTION_ID,
    ANNUAL_BLOCK_ID,
    BLOCK_FIELD_LAYOUT,
    BLOCK_ORDER,
    CLOSED_PARITY_EXCEPTION_IDS,
    DISPLAY_NORMALIZATION_EXCEPTION_ID,
    EMPTY_TRACKER_EXCEPTION_ID,
    FUZZY_TRACE_EXCEPTION_ID,
    GENERIC_SOURCE_EXCEPTION_ID,
    LOSSY_MATRIX_EXCEPTION_ID,
    OPEN_BLOCK_ID,
    PRODUCT_CONTRACT_ID,
    PRODUCT_TYPE,
    PARITY_EXCEPTION_REGISTRY,
    PROGRESS_RUN_RATE_ID,
    PROGRESS_DELTA_ID,
    PROGRESS_REMAINING_ID,
    SCORECARD_BLOCK_ID,
    SCORECARD_EXCEPTION_ID,
    ROW_REMAP_EXCEPTION_ID,
    STATIC_STATUS_EXCEPTION_ID,
    TEMPORAL_EXCEPTION_ID,
    TIMELINE_BLOCK_ID,
    DisplayValue,
    PromiseProgressProduct,
    PromiseProgressProjectionError,
    SemanticIdentity,
    build_promise_progress_product,
    classify_timeline_change,
    display_value_from_spec,
    serialize_promise_progress_product,
    serialize_shadow_matrix,
    validate_promise_progress_product,
    validate_shadow_matrix,
)
from pbi_xbrl.longitudinal_memory.sector_packs.retail import RETAIL_SECTOR_PACK
from pbi_xbrl.longitudinal_memory.serialization import serialize_package
from pbi_xbrl.longitudinal_memory.source_adapter.builder import build_source_native_sidecar
from pbi_xbrl.longitudinal_memory.ticker_profiles.anf import load_anf_profile


REPO = Path(__file__).resolve().parents[1]
FIXTURES = REPO / "tests" / "fixtures" / "promise_progress"
LONGITUDINAL_FIXTURES = REPO / "tests" / "fixtures" / "longitudinal_memory"
SOURCE_ROOT = Path(r"C:\Users\Jibbe\Aktier\StockModelData")
SCHEMA = REPO / "docs" / "promise_progress_shadow_projection.schema.json"
PARITY_REGISTER = REPO / "docs" / "promise_progress_parity_exceptions.json"
ANF_ORACLE = FIXTURES / "anf_legacy_oracle.v1.json"
PBI_ORACLE = FIXTURES / "pbi_capability_oracle.v1.json"
GENERIC_ORACLE = FIXTURES / "gpre_capability_oracle.v1.json"


def _strict_json(path: Path) -> dict:
    def object_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key {key!r} in {path}")
            result[key] = value
        return result

    return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=object_pairs)


@pytest.fixture(scope="module")
def anf_oracle() -> dict:
    return _strict_json(ANF_ORACLE)


@pytest.fixture(scope="module")
def anf_package(anf_oracle: dict) -> dict:
    return build_source_native_sidecar(
        REPO / anf_oracle["source_package_fixture"],
        source_root=SOURCE_ROOT,
        reviewed_model_root=REPO,
        sector_pack=RETAIL_SECTOR_PACK,
        ticker_profile_loader=load_anf_profile,
    ).package


@pytest.fixture(scope="module")
def anf_product(anf_oracle: dict, anf_package: dict) -> PromiseProgressProduct:
    return build_promise_progress_product(anf_package, anf_oracle["projection_plan"])


@pytest.fixture(scope="module")
def pbi_oracle() -> dict:
    return _strict_json(PBI_ORACLE)


@pytest.fixture(scope="module")
def pbi_package(pbi_oracle: dict) -> dict:
    return _strict_json(REPO / pbi_oracle["source_package_fixture"])


@pytest.fixture(scope="module")
def pbi_product(pbi_oracle: dict, pbi_package: dict) -> PromiseProgressProduct:
    return build_promise_progress_product(pbi_package, pbi_oracle["projection_plan"])


@pytest.fixture(scope="module")
def generic_oracle() -> dict:
    return _strict_json(GENERIC_ORACLE)


@pytest.fixture(scope="module")
def generic_product(generic_oracle: dict) -> PromiseProgressProduct:
    return build_promise_progress_product(
        generic_oracle["source_package"], generic_oracle["projection_plan"]
    )


def _field_map(row) -> dict[str, object]:
    return {field.field_role: field for field in row.fields}


def _entity_id(package: dict, *, kind: str, **selectors: str) -> str:
    matches = [
        row["header"]["entity_id"]
        for row in package["entities"]
        if row["payload"].get("kind") == kind
        and all(row["payload"].get(key) == value for key, value in selectors.items())
    ]
    assert len(matches) == 1
    return matches[0]


def _row(product: PromiseProgressProduct, block_id: str, identity: str):
    matches = [
        row
        for block in product.blocks
        if block.block_id == block_id
        for row in block.rows
        if row.canonical_series_or_program_id == identity
    ]
    assert len(matches) == 1
    return matches[0]


def _rows(product: PromiseProgressProduct, block_id: str, identity: str):
    return [
        row
        for block in product.blocks
        if block.block_id == block_id
        for row in block.rows
        if row.canonical_series_or_program_id == identity
    ]


def _replace_field(product: PromiseProgressProduct, field_id: str, **changes) -> PromiseProgressProduct:
    blocks = []
    for block in product.blocks:
        rows = []
        for row in block.rows:
            fields = tuple(
                dataclasses.replace(field, **changes) if field.field_id == field_id else field
                for field in row.fields
            )
            rows.append(dataclasses.replace(row, fields=fields))
        blocks.append(dataclasses.replace(block, rows=tuple(rows)))
    return dataclasses.replace(product, blocks=tuple(blocks))


def _reverse_mapping_insertion(value):
    if isinstance(value, dict):
        return {
            key: _reverse_mapping_insertion(value[key])
            for key in reversed(tuple(value))
        }
    if isinstance(value, list):
        return [_reverse_mapping_insertion(item) for item in value]
    return value


def _stable_digest(value) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).hexdigest()


def _parity_binding_comparison_digest(binding: dict) -> str:
    return _stable_digest(
        {
            "product_id": binding["product_id"],
            "block_id": binding["block_id"],
            "reviewed_legacy_business_key": binding["reviewed_legacy_business_key"],
            "source_native_row_id": binding["source_native_row_id"],
            "row_type": binding["row_type"],
            "business_key_class": binding["business_key_class"],
            "field_role": binding["field_role"],
            "legacy_destination_cell": binding["legacy_destination_cell"],
            "source_native_destination_cell": binding["source_native_destination_cell"],
            "underlying_difference_class": binding["difference_class"],
            "legacy_display_value": binding["legacy_value"],
            "source_native_display_value": binding["source_native_value"],
        }
    )


def _recompute_parity_authorization(plan: dict, binding: dict, *, policy: dict | None = None) -> None:
    binding["comparison_digest"] = _parity_binding_comparison_digest(binding)
    if policy is None:
        matches = [
            candidate
            for candidate in plan["legacy_parity"]["exception_policies"]
            if candidate["exception_id"] == binding["exception_id"]
            and candidate["exception_policy_version"] == binding["exception_policy_version"]
        ]
        assert len(matches) == 1
        policy = matches[0]
    reviewed_scope = {
        key: value
        for key, value in binding.items()
        if key != "exception_authorization_digest"
    }
    binding["exception_authorization_digest"] = _stable_digest(
        {
            "exception_identity": {
                "exception_id": binding["exception_id"],
                "exception_policy_version": binding["exception_policy_version"],
            },
            "resolved_exception_policy": policy,
            "independently_derived_difference_reason": binding["semantic_reason_code"],
            "reviewed_binding_scope": reviewed_scope,
        }
    )


def _binding(plan: dict, *, field_role: str | None = None, legacy_destination: str | None = None) -> dict:
    matches = [
        binding
        for binding in plan["legacy_parity_accepted_differences"]
        if (field_role is None or binding["field_role"] == field_role)
        and (legacy_destination is None or binding["legacy_destination_cell"] == legacy_destination)
    ]
    assert matches
    return matches[0]


def _policy(plan: dict, exception_id: str) -> dict:
    matches = [
        policy
        for policy in plan["legacy_parity"]["exception_policies"]
        if policy["exception_id"] == exception_id and policy["authorization_kind"] == "field-difference"
    ]
    assert len(matches) == 1
    return matches[0]


def _policy_by_id(plan: dict, policy_id: str) -> dict:
    matches = [
        policy
        for policy in plan["legacy_parity"]["exception_policies"]
        if policy["policy_id"] == policy_id
    ]
    assert len(matches) == 1
    return matches[0]


def _disposition(
    plan: dict,
    *,
    legacy_row_id: str | None = None,
    source_native_row_id: str | None = None,
    kind: str | None = None,
) -> dict:
    matches = [
        disposition
        for disposition in plan["legacy_parity"]["row_dispositions"]
        if (legacy_row_id is None or disposition["legacy_row_id"] == legacy_row_id)
        and (source_native_row_id is None or disposition["source_native_row_id"] == source_native_row_id)
        and (kind is None or disposition["disposition_kind"] == kind)
    ]
    assert len(matches) == 1
    return matches[0]


def _legacy_row(plan: dict, visible_sheet_row: int) -> dict:
    matches = [
        row
        for row in plan["legacy_parity"]["rows"]
        if row["visible_sheet_row"] == visible_sheet_row
    ]
    assert len(matches) == 1
    return matches[0]


def _recompute_capture_manifest(plan: dict, *, update_independent: bool) -> None:
    oracle = plan["legacy_parity"]
    rows = sorted(
        oracle["rows"],
        key=lambda row: (BLOCK_ORDER.index(row["block_id"]), row["visible_sheet_row"]),
    )
    inventories = []
    for row in rows:
        fields = [
            {
                "field_role": role,
                "destination_cell": f"{column}{row['visible_sheet_row']}",
                "semantic_classification": semantic_classification,
                "structural_classification": row["structural_classification"],
            }
            for (role, column, _), semantic_classification in zip(
                BLOCK_FIELD_LAYOUT[row["block_id"]],
                row["semantic_classifications"],
                strict=True,
            )
        ]
        payload = {
            "legacy_row_id": row["legacy_row_id"],
            "block_id": row["block_id"],
            "visible_sheet_row": row["visible_sheet_row"],
            "fields": fields,
        }
        inventories.append({**payload, "field_inventory_digest": _stable_digest(payload)})
    manifest = oracle["capture_manifest"]
    manifest["ordered_legacy_row_ids"] = [row["legacy_row_id"] for row in rows]
    manifest["destination_cells"] = sorted(
        field["destination_cell"] for inventory in inventories for field in inventory["fields"]
    )
    manifest["row_field_inventories"] = inventories
    manifest["row_count"] = len(rows)
    manifest["field_count"] = sum(len(inventory["fields"]) for inventory in inventories)
    manifest["legacy_matrix_digest"] = _stable_digest(
        {"block_contracts": oracle["block_contracts"], "rows": rows}
    )
    if update_independent:
        plan["legacy_parity_capture_manifest_sha256"] = _stable_digest(manifest)


def _recompute_all_row_authorizations(plan: dict) -> None:
    for disposition in plan["legacy_parity"]["row_dispositions"]:
        policy = _policy_by_id(plan, disposition["policy_id"])
        scope = {
            key: value
            for key, value in disposition.items()
            if key != "authorization_digest"
        }
        disposition["authorization_digest"] = _stable_digest(
            {
                "disposition_identity": {
                    "disposition_id": disposition["disposition_id"],
                    "disposition_version": disposition["disposition_version"],
                },
                "resolved_disposition_policy": policy,
                "independently_derived_reason": disposition["reason_code"],
                "independently_replayed_counterpart_row_ids": disposition[
                    "counterpart_search_result"
                ],
                "reviewed_parity_scope": {
                    "legacy_capture_manifest_sha256": plan[
                        "legacy_parity_capture_manifest_sha256"
                    ],
                    "source_scope_manifest_sha256": plan[
                        "legacy_parity_source_scope_manifest_sha256"
                    ],
                },
                "reviewed_disposition_scope": scope,
            }
        )
    plan["legacy_parity_row_disposition_graph_sha256"] = _stable_digest(
        sorted(
            plan["legacy_parity"]["row_dispositions"],
            key=lambda disposition: disposition["disposition_id"],
        )
    )


def _recompute_structural_authorization(
    plan: dict,
    binding: dict,
    *,
    recompute_observation: bool = False,
    update_independent: bool = True,
) -> None:
    observation = next(
        item
        for item in plan["legacy_parity"]["structural_observations"]
        if item["structural_observation_id"] == binding["structural_observation_id"]
    )
    if recompute_observation:
        observation["comparison_digest"] = _stable_digest(
            {key: value for key, value in observation.items() if key != "comparison_digest"}
        )
        binding["comparison_digest"] = observation["comparison_digest"]
    policy = _policy_by_id(plan, binding["policy_id"])
    binding["structural_authorization_digest"] = _stable_digest(
        {
            "binding_scope": {
                key: value
                for key, value in binding.items()
                if key != "structural_authorization_digest"
            },
            "resolved_observation": observation,
            "resolved_structural_policy": policy,
            "independently_derived_reason": binding["difference_reason_code"],
        }
    )
    if update_independent:
        plan["legacy_parity_structural_observation_set_sha256"] = _stable_digest(
            sorted(
                plan["legacy_parity"]["structural_observations"],
                key=lambda item: item["structural_observation_id"],
            )
        )
        plan["legacy_parity_structural_binding_set_sha256"] = _stable_digest(
            sorted(
                plan["legacy_parity"]["structural_bindings"],
                key=lambda item: item["structural_binding_id"],
            )
        )


def _recompute_shadow_root(shadow: dict) -> None:
    shadow["lineage_digest"] = _stable_digest(
        {key: value for key, value in shadow.items() if key != "lineage_digest"}
    )


def _recompute_shadow_field_digest(field: dict) -> None:
    destination = field["destination"]
    payload = {
        "kind": "field",
        "product_id": field["product_id"],
        "block_id": field["block_id"],
        "row_id": field["row_id"],
        "field_id": field["field_id"],
        "field_role": field["field_role"],
        "anchor_cell": destination["anchor_cell"],
        "display_range": destination["display_range"],
        "display": field["display_value"],
        "canonical_record_ids": field["canonical_record_ids"],
        "target_version_id": field["target_version_id"],
        "actual_id": field["actual_selection_id"],
        "progress_id": field["progress_selection_id"],
        "status_id": field["status_assessment_id"],
        "semantic_identity": field["semantic_identity"],
        "period_or_horizon_id": field["period_or_horizon_id"],
        "ui_as_of_date": field["ui_as_of_date"],
        "knowledge_dates": field["knowledge_dates"],
        "source_occurrence_ids": field["source_occurrence_ids"],
        "source_document_ids": field["source_document_ids"],
        "method_id": field["selection_or_calculation_method_id"],
        "review_issue_ids": field["review_issue_ids"],
        "parity_exception_ids": field["parity_exception_ids"],
    }
    field["lineage_digest"] = _stable_digest(payload)


def _calculated_progress_binding(actual_binding: dict, role_id: str, semantic_class: str, direction: str) -> dict:
    assertion = copy.deepcopy(actual_binding["role_semantic_assertion"])
    assertion.pop("actual_role_id")
    assertion["progress_role_id"] = role_id
    assertion["semantic_class"] = semantic_class
    return {
        "progress_role_id": role_id,
        "role_semantic_assertion": assertion,
        "semantic_identity": copy.deepcopy(actual_binding["semantic_identity"]),
        "selector": copy.deepcopy(actual_binding["selector"]),
        "target_direction": direction,
    }


def test_contract_files_are_strict_json_and_register_is_closed():
    schema = _strict_json(SCHEMA)
    register = _strict_json(PARITY_REGISTER)
    for path in (ANF_ORACLE, PBI_ORACLE, GENERIC_ORACLE):
        _strict_json(path)
    assert schema["$defs"]["field"]["additionalProperties"] is False
    assert register["register_state"] == "closed-for-first-shadow"
    assert {row["exception_id"] for row in register["exceptions"]} == CLOSED_PARITY_EXCEPTION_IDS
    assert set(PARITY_EXCEPTION_REGISTRY) == CLOSED_PARITY_EXCEPTION_IDS
    assert set(register["closed_difference_reason_codes"])
    assert len(register["authorization_layers"]) == 6
    by_id = {row["exception_id"]: row for row in register["exceptions"]}
    registered_policies = []
    for exception_id, definition in PARITY_EXCEPTION_REGISTRY.items():
        assert by_id[exception_id]["acceptance_owner"] == definition["acceptance_owner"]
        assert by_id[exception_id]["duration"] == definition["duration"]
        assert by_id[exception_id]["authorization_policies"]
        registered_policies.extend(by_id[exception_id]["authorization_policies"])
        for policy in by_id[exception_id]["authorization_policies"]:
            assert set(policy) == set(register["policy_required_fields"])
            assert "*" not in json.dumps(policy, sort_keys=True)
        assert all(definition[key] for key in ("legacy_behavior", "source_native_behavior", "semantic_reason"))
    assert len(registered_policies) == 15
    assert len({policy["policy_id"] for policy in registered_policies}) == 15


@pytest.mark.parametrize("fixture_name", ["anf", "pbi", "generic"])
def test_one_product_owns_four_distinct_blocks(request, fixture_name: str):
    product = request.getfixturevalue(f"{fixture_name}_product")
    assert product.product_contract_id == PRODUCT_CONTRACT_ID
    assert product.product_type == PRODUCT_TYPE
    assert tuple(block.block_id for block in product.blocks) == BLOCK_ORDER
    assert len({type(block) for block in product.blocks}) == 4
    assert tuple(field for row in product.ordered_rows for field in row.fields) == product.fields
    assert json.loads(serialize_promise_progress_product(product))["shadow_matrix"] == product.shadow_matrix()


def test_product_value_payloads_are_deeply_immutable(anf_product: PromiseProgressProduct):
    composite = next(
        field.display_value for field in anf_product.fields
        if isinstance(field.display_value.machine_value, tuple) and field.display_value.machine_value
    )
    assert isinstance(composite.machine_value, tuple)
    with pytest.raises(TypeError):
        composite.machine_value[0]["value"] = "999"
    with pytest.raises(dataclasses.FrozenInstanceError):
        composite.display_text = "mutated"


@pytest.mark.parametrize("fixture_name", ["anf", "pbi", "generic"])
def test_shadow_matrix_validates_and_covers_every_visible_field(request, fixture_name: str):
    product = request.getfixturevalue(f"{fixture_name}_product")
    assert validate_shadow_matrix(product.shadow_matrix()) == []
    shadow = product.shadow_matrix()
    assert len(shadow["rows"]) == len(product.ordered_rows)
    assert len(shadow["fields"]) == len(product.fields)
    assert {row["row_id"] for row in shadow["rows"]} == {row.row_id for row in product.ordered_rows}
    assert {field["field_id"] for field in shadow["fields"]} == {field.field_id for field in product.fields}
    assert all(field["destination"]["sheet"] == "Promise_Progress_UI" for field in shadow["fields"])


@pytest.mark.parametrize("fixture_name", ["anf", "pbi", "generic"])
def test_checked_in_shadow_schema_validates_complete_products(request, fixture_name: str):
    product = request.getfixturevalue(f"{fixture_name}_product")
    assert validate_json_schema(product.shadow_matrix(), load_json_strict(SCHEMA)) == []


def test_shadow_schema_and_standalone_replay_reject_cross_block_corruption(
    generic_product: PromiseProgressProduct,
):
    schema = load_json_strict(SCHEMA)
    baseline = generic_product.shadow_matrix()

    wrong_block = copy.deepcopy(baseline)
    scorecard_field = next(
        field for field in wrong_block["fields"]
        if field["block_id"] == SCORECARD_BLOCK_ID and field["field_role"] == "category"
    )
    scorecard_field["block_id"] = ANNUAL_BLOCK_ID
    _recompute_shadow_field_digest(scorecard_field)
    _recompute_shadow_root(wrong_block)
    assert validate_json_schema(wrong_block, schema)
    assert {issue["rule_id"] for issue in validate_shadow_matrix(wrong_block)} >= {
        "shadow_field_block_owner",
        "shadow_field_role",
    }

    arbitrary_role = copy.deepcopy(baseline)
    arbitrary_role["fields"][0]["field_role"] = "arbitrary_role"
    _recompute_shadow_field_digest(arbitrary_role["fields"][0])
    _recompute_shadow_root(arbitrary_role)
    assert validate_json_schema(arbitrary_role, schema)
    assert "shadow_field_role" in {
        issue["rule_id"] for issue in validate_shadow_matrix(arbitrary_role)
    }

    broad_machine = copy.deepcopy(baseline)
    broad_machine["fields"][0]["display_value"] = {
        "value_form": "qualitative",
        "display_text": "opaque",
        "machine_value": {"arbitrary": {"economic": "payload"}},
    }
    _recompute_shadow_field_digest(broad_machine["fields"][0])
    _recompute_shadow_root(broad_machine)
    assert validate_json_schema(broad_machine, schema)
    assert "shadow_machine_value" in {
        issue["rule_id"] for issue in validate_shadow_matrix(broad_machine)
    }


def test_shadow_semantic_replay_rejects_owner_destination_and_reference_mutations(
    generic_product: PromiseProgressProduct,
):
    baseline = generic_product.shadow_matrix()

    timeline_destination = copy.deepcopy(baseline)
    annual_field = next(
        field for field in timeline_destination["fields"]
        if field["block_id"] == ANNUAL_BLOCK_ID and field["field_role"] == "metric"
    )
    annual_field["destination"] = {
        "sheet": "Promise_Progress_UI",
        "anchor_cell": "A61",
        "display_range": "A61",
    }
    _recompute_shadow_field_digest(annual_field)
    _recompute_shadow_root(timeline_destination)
    assert "shadow_field_destination_contract" in {
        issue["rule_id"] for issue in validate_shadow_matrix(timeline_destination)
    }

    missing_lineage = copy.deepcopy(baseline)
    missing_lineage["fields"][0].pop("source_occurrence_ids")
    _recompute_shadow_root(missing_lineage)
    assert validate_json_schema(missing_lineage, load_json_strict(SCHEMA))

    unresolved = copy.deepcopy(baseline)
    unresolved["fields"][0]["canonical_record_ids"] = ["record:missing:shadow-input@1"]
    _recompute_shadow_field_digest(unresolved["fields"][0])
    _recompute_shadow_root(unresolved)
    assert "shadow_canonical_reference" in {
        issue["rule_id"] for issue in validate_shadow_matrix(unresolved)
    }

    duplicate = copy.deepcopy(baseline)
    duplicate["fields"].append(copy.deepcopy(duplicate["fields"][0]))
    _recompute_shadow_root(duplicate)
    assert "shadow_duplicate_field" in {
        issue["rule_id"] for issue in validate_shadow_matrix(duplicate)
    }

    stale_field = copy.deepcopy(baseline)
    stale_field["fields"][0]["display_value"]["display_text"] += " mutated"
    _recompute_shadow_root(stale_field)
    assert "shadow_field_lineage_digest" in {
        issue["rule_id"] for issue in validate_shadow_matrix(stale_field)
    }


@pytest.mark.parametrize("fixture_name", ["anf", "pbi", "generic"])
def test_semantic_product_and_shadow_goldens(request, fixture_name: str):
    product = request.getfixturevalue(f"{fixture_name}_product")
    oracle = request.getfixturevalue(f"{fixture_name}_oracle")
    assert hashlib.sha256(serialize_promise_progress_product(product)).hexdigest() == oracle["expected_product_sha256"]
    assert hashlib.sha256(serialize_shadow_matrix(product)).hexdigest() == oracle["expected_shadow_sha256"]


def test_accepted_longitudinal_memory_goldens_remain_unchanged(anf_package: dict, pbi_package: dict):
    assert hashlib.sha256(serialize_package(anf_package)).hexdigest() == "b25584e692568b460dda20a620a9e8f8f50e80c89d89a5bc41c30fe0dab4e4e0"
    assert hashlib.sha256(serialize_package(pbi_package)).hexdigest() == "da8577e389be383aeec80f481c0889acf62c38edf604e25f62df736cf89c34a6"
    curated = LONGITUDINAL_FIXTURES / "anf_first_pass_expected.v1.json"
    assert _strict_json(curated)["serialization_sha256"] == "9fd73df61166105d83180da34e9ddcd5c126d83e498c1176c55f0f6a2c18ccc7"


@pytest.mark.parametrize("fixture_name", ["anf", "pbi", "generic"])
def test_scorecard_preserves_rows_without_fabricating_assessment(request, fixture_name: str):
    product = request.getfixturevalue(f"{fixture_name}_product")
    scorecard = product.blocks[0]
    assert scorecard.block_id == SCORECARD_BLOCK_ID
    assert len(scorecard.rows) == 5
    for row in scorecard.rows:
        fields = _field_map(row)
        assert fields["score"].display_value.display_text == "Needs Review"
        assert fields["read"].display_value.display_text == "Reviewed assessment required."
        assert row.canonical_series_or_program_id is None
        assert row.parity_exception_ids == (SCORECARD_EXCEPTION_ID,)


def test_anf_guidance_and_store_replay(anf_product: PromiseProgressProduct, anf_package: dict):
    revenue_id = _entity_id(
        anf_package,
        kind="GuidanceSeries",
        metric_id="metric:core:revenue-growth@1",
        horizon_period_id="period:anf:fy2025@1",
    )
    revenue = _field_map(_row(anf_product, ANNUAL_BLOCK_ID, revenue_id))
    assert [revenue[key].display_value.display_text for key in ("initial_guide", "q1_guide", "q2_guide", "q3_guide", "q4_guide")] == [
        "3–5%", "3–6%", "5–7%", "6–7%", "≥6%"
    ]
    assert revenue["actual"].display_value.display_text == ""
    assert revenue["status"].display_value.display_text == "Needs Review"
    assert ACTUAL_COVERAGE_EXCEPTION_ID in revenue["actual"].parity_exception_ids

    store_program_id = "fy2025-store-plan"
    store = _field_map(_row(anf_product, ANNUAL_BLOCK_ID, store_program_id))
    assert [store[key].display_value.display_text for key in ("initial_guide", "q1_guide", "q2_guide", "q3_guide", "q4_guide")] == ["~40"] * 5
    assert store["actual"].display_value.display_text == "62 openings / 22 closures; net 40"
    assert store["actual"].display_value.machine_value[-1]["value"] == "40"
    assert store["status"].display_value.display_text == "Needs Review"
    assert "tolerance" in store["notes_source"].display_value.display_text

    annual_by_label = {
        _field_map(row)["metric"].display_value.display_text: _field_map(row)
        for row in anf_product.blocks[1].rows
    }
    assert [annual_by_label["FY2025 operating margin"][key].display_value.display_text for key in (
        "initial_guide", "q1_guide", "q2_guide", "q3_guide", "q4_guide"
    )] == ["14–15%", "12.5–13.5%", "13–13.5%", "13–13.5%", "around 13%"]
    for label in ("FY2025 adjusted EPS", "FY2025 capital expenditures"):
        assert annual_by_label[label]["actual"].display_value.display_text == ""
        assert annual_by_label[label]["status"].display_value.display_text == "Needs Review"
        assert ACTUAL_COVERAGE_EXCEPTION_ID in annual_by_label[label]["actual"].parity_exception_ids


def test_reviewed_guidance_bucket_overrides_are_identity_and_event_bound(
    anf_oracle: dict,
    anf_package: dict,
):
    binding_index = next(
        index for index, binding in enumerate(anf_oracle["projection_plan"]["guidance_bindings"])
        if binding["metric_label"] == "FY2025 net sales growth"
    )

    swapped = copy.deepcopy(anf_oracle["projection_plan"])
    overrides = swapped["guidance_bindings"][binding_index]["bucket_overrides"]
    overrides["2025-05-29"], overrides["2026-01-12"] = (
        overrides["2026-01-12"],
        overrides["2025-05-29"],
    )
    with pytest.raises(PromiseProgressProjectionError, match="exact GuidanceVersion"):
        build_promise_progress_product(anf_package, swapped)

    wrong_event = copy.deepcopy(anf_oracle["projection_plan"])
    override = wrong_event["guidance_bindings"][binding_index]["bucket_overrides"]["2025-05-29"]
    override["bucket_id"] = "q4_guide"
    with pytest.raises(PromiseProgressProjectionError, match="reporting-event identity"):
        build_promise_progress_product(anf_package, wrong_event)

    cross_series = copy.deepcopy(anf_oracle["projection_plan"])
    sales_override = cross_series["guidance_bindings"][binding_index]["bucket_overrides"]["2025-05-29"]
    margin_binding = next(
        binding for binding in cross_series["guidance_bindings"]
        if binding["metric_label"] == "FY2025 operating margin"
    )
    sales_override["version_id_sha256"] = margin_binding["bucket_overrides"]["2025-05-29"]["version_id_sha256"]
    with pytest.raises(PromiseProgressProjectionError, match="exact GuidanceVersion"):
        build_promise_progress_product(anf_package, cross_series)


def test_annual_guidance_bucket_sequence_replays_dates_and_version_chain(
    anf_oracle: dict,
    anf_package: dict,
):
    series_id = _entity_id(
        anf_package,
        kind="GuidanceSeries",
        metric_id="metric:core:revenue-growth@1",
        horizon_period_id="period:anf:fy2025@1",
    )
    versions = {
        row["header"]["publication_date"]: row
        for row in anf_package["observations"]
        if row["payload"].get("kind") == "GuidanceVersion"
        and row["payload"].get("guidance_series_id") == series_id
    }

    decreasing_knowledge = copy.deepcopy(anf_package)
    q1_id = versions["2025-05-29"]["header"]["record_id"]
    next(
        row for row in decreasing_knowledge["observations"]
        if row["header"]["record_id"] == q1_id
    )["header"]["knowledge_date"] = "2025-09-01"
    with pytest.raises(PromiseProgressProjectionError, match="decreasing knowledge dates"):
        build_promise_progress_product(decreasing_knowledge, anf_oracle["projection_plan"])

    decreasing_publication = copy.deepcopy(anf_package)
    q2_id = versions["2025-08-28"]["header"]["record_id"]
    next(
        row for row in decreasing_publication["observations"]
        if row["header"]["record_id"] == q2_id
    )["header"]["publication_date"] = "2025-04-01"
    plan = copy.deepcopy(anf_oracle["projection_plan"])
    binding = next(
        value for value in plan["guidance_bindings"]
        if value["metric_label"] == "FY2025 net sales growth"
    )
    binding["bucket_overrides"]["2025-04-01"] = binding["bucket_overrides"].pop("2025-08-28")
    with pytest.raises(PromiseProgressProjectionError, match="decreasing publication dates"):
        build_promise_progress_product(decreasing_publication, plan)

    broken_chain = copy.deepcopy(anf_package)
    q1_record_id = versions["2025-05-29"]["header"]["record_id"]
    q2_record_id = versions["2025-08-28"]["header"]["record_id"]
    origin_id = versions[min(versions)]["header"]["record_id"]
    relation = next(
        row for row in broken_chain["relations"]
        if row.get("from_record_id") == q2_record_id
        and row.get("to_record_id") == q1_record_id
    )
    relation["to_record_id"] = origin_id
    with pytest.raises(PromiseProgressProjectionError, match="explicit version chain"):
        build_promise_progress_product(broken_chain, anf_oracle["projection_plan"])


def test_anf_open_guidance_is_current_and_no_q4_is_relabelled_fy(anf_product: PromiseProgressProduct):
    open_rows = anf_product.blocks[2].rows
    assert {row.block_id for row in open_rows} == {OPEN_BLOCK_ID}
    assert { _field_map(row)["status"].display_value.display_text for row in open_rows } == {"Open"}
    assert { _field_map(row)["horizon"].display_value.display_text for row in open_rows } == {"FY2026"}
    annual_actuals = [
        _field_map(row)["actual"]
        for row in anf_product.blocks[1].rows
        if row.row_variant == "annual_guidance_series"
    ]
    assert all(field.actual_observation_id is None for field in annual_actuals[:2])
    assert all("comparable-sales" not in " ".join(field.canonical_record_ids) for field in annual_actuals[:2])


def test_pbi_promise_projects_complete_history_and_run_rate_only_as_progress(
    pbi_product: PromiseProgressProduct, pbi_package: dict
):
    promise_program_id = "program:pbi:2024-cost-rationalization@1"
    open_fields = _field_map(_row(pbi_product, OPEN_BLOCK_ID, promise_program_id))
    assert open_fields["current_guide"].display_value.display_text == "$180m–200m potential annualized savings"
    assert open_fields["status"].display_value.display_text == "Needs Review"
    assert open_fields["notes_source"].display_value.display_text.count("$157m net annualized run rate") == 1
    actual_id = open_fields["notes_source"].actual_selection_id
    actual = next(value for value in pbi_product.actuals if value.actual_id == actual_id)
    assert actual.selection_state == "missing_by_absence"
    assert actual.display_value.display_text == ""
    progress_id = open_fields["notes_source"].progress_selection_id
    progress = next(value for value in pbi_product.progress_values if value.progress_id == progress_id)
    assert progress.progress_role_id == PROGRESS_RUN_RATE_ID
    assert progress.display_value.display_text == "$157m net annualized run rate"
    issue_identity_text = " ".join(open_fields["status"].review_issue_ids)
    assert "rule=gross-net-savings-bridge-missing" in issue_identity_text
    assert "rule=promise-run-rate-not-realized-savings" in issue_identity_text

    timeline = _rows(pbi_product, TIMELINE_BLOCK_ID, promise_program_id)
    assert len(timeline) == 6
    assert [_field_map(row)["current_guide"].display_value.display_text for row in timeline] == [
        "$60m–100m potential annualized savings",
        "$120m–160m potential annualized savings",
        "$120m–160m potential annualized savings",
        "$150m–170m potential annualized savings",
        "$170m–190m potential annualized savings",
        "$180m–200m potential annualized savings",
    ]
    assert [_field_map(row)["change_type"].display_value.machine_value for row in timeline] == [
        "origin", "target-increase", "reaffirmation", "target-increase", "target-increase", "target-increase"
    ]
    assert all(_field_map(row)["actual"].display_value.display_text == "" for row in timeline)
    assert _field_map(timeline[-1])["progress"].display_value.display_text == "$157m net annualized run rate"
    assert len({tuple(_field_map(row)["current_guide"].source_occurrence_ids) for row in timeline}) == 6
    assert all(_field_map(row)["horizon"].display_value.display_text == "Program; no exact deadline disclosed" for row in timeline)
    assert not any(
        record["payload"].get("kind") == "ModelInterpretation"
        for record in pbi_package["observations"]
    )


def test_pbi_definition_break_remains_visible_and_not_governing(pbi_product: PromiseProgressProduct):
    ambiguous = [
        row for row in pbi_product.blocks[1].rows
        if "basis ambiguous" in _field_map(row)["metric"].display_value.display_text
    ]
    assert len(ambiguous) == 1
    fields = _field_map(ambiguous[0])
    assert fields["initial_guide"].display_value.display_text == "$410m–460m"
    assert fields["status"].display_value.display_text == "Basis-dependent"
    compatible = [
        _field_map(row)["q2_guide"].display_value.display_text
        for row in pbi_product.blocks[1].rows
        if _field_map(row)["metric"].display_value.display_text == "FY2026 adjusted EBIT guidance"
    ]
    assert compatible == ["$445m–475m"]


def test_generic_policy_bound_milestone_and_contingent_capabilities(generic_product: PromiseProgressProduct):
    annual = { _field_map(row)["metric"].display_value.display_text: _field_map(row) for row in generic_product.blocks[1].rows }
    assert annual["Policy-dependent benefit"]["initial_guide"].display_value.display_text == "$40m–60m"
    assert annual["Policy-dependent benefit"]["status"].display_value.display_text == "Open"
    assert annual["Minimum cost savings"]["initial_guide"].display_value.display_text == "≥10m"
    assert annual["Minimum cost savings"]["actual"].display_value.display_text == "$12m"
    assert annual["Minimum cost savings"]["status"].display_value.display_text == "Beat"
    assert annual["Facility start milestone"]["actual"].display_value.display_text == "Facility operations started"
    assert annual["Facility start milestone"]["status"].display_value.display_text == "Completed"
    contingent = [
        _field_map(row) for row in generic_product.blocks[2].rows
        if _field_map(row)["metric"].display_value.display_text == "Policy-contingent qualitative milestone"
    ]
    assert len(contingent) == 1
    assert contingent[0]["horizon"].display_value.display_text == "Contingent on policy approval; timing unresolved"
    assert contingent[0]["status"].display_value.display_text == "Needs Review"
    assert contingent[0]["status"].review_issue_ids == ("review-issue:generic:unresolved-dependency@1",)


def test_milestone_completion_requires_reviewed_source_backed_completed_state(
    generic_oracle: dict,
):
    baseline = build_promise_progress_product(
        generic_oracle["source_package"], generic_oracle["projection_plan"]
    )
    row = next(
        row for row in baseline.blocks[1].rows
        if _field_map(row)["metric"].display_value.display_text == "Facility start milestone"
    )
    actual = next(
        value for value in baseline.actuals
        if value.actual_id == _field_map(row)["actual"].actual_selection_id
    )
    assert actual.milestone_state is not None
    assert actual.milestone_state.state == "completed"
    assert actual.milestone_state.source_text == "Facility operations started"
    assert _field_map(row)["status"].display_value.display_text == "Completed"

    stale_text = copy.deepcopy(generic_oracle)
    record = next(
        value for value in stale_text["source_package"]["observations"]
        if value["header"]["record_id"] == "record:generic:milestone-actual@1"
    )
    record["payload"]["value"]["text"] = "Facility operations have not started"
    with pytest.raises(PromiseProgressProjectionError, match="does not replay"):
        build_promise_progress_product(
            stale_text["source_package"], stale_text["projection_plan"]
        )

    not_started = copy.deepcopy(generic_oracle)
    record = next(
        value for value in not_started["source_package"]["observations"]
        if value["header"]["record_id"] == "record:generic:milestone-actual@1"
    )
    record["payload"]["value"]["text"] = "Facility operations have not started"
    milestone = not_started["projection_plan"]["promise_bindings"][1]["actual"]["milestone_state"]
    milestone["state"] = "not_started"
    milestone["exact_source_text"] = "Facility operations have not started"
    product = build_promise_progress_product(
        not_started["source_package"], not_started["projection_plan"]
    )
    row = next(
        row for row in product.blocks[1].rows
        if _field_map(row)["metric"].display_value.display_text == "Facility start milestone"
    )
    assert _field_map(row)["status"].display_value.display_text != "Completed"


def test_missing_conflicting_or_horizon_mismatched_milestone_state_needs_review(
    generic_oracle: dict,
):
    missing = copy.deepcopy(generic_oracle)
    missing["projection_plan"]["promise_bindings"][1]["actual"].pop("milestone_state")
    product = build_promise_progress_product(missing["source_package"], missing["projection_plan"])
    row = next(
        row for row in product.blocks[1].rows
        if _field_map(row)["metric"].display_value.display_text == "Facility start milestone"
    )
    assert _field_map(row)["status"].display_value.display_text == "Needs Review"

    conflict = copy.deepcopy(generic_oracle)
    second = copy.deepcopy(next(
        value for value in conflict["source_package"]["observations"]
        if value["header"]["record_id"] == "record:generic:milestone-actual@1"
    ))
    second["header"]["record_id"] = "record:generic:milestone-actual-conflict@1"
    second["payload"]["value"]["text"] = "Facility operations have not started"
    conflict["source_package"]["observations"].append(second)
    conflict["source_package"]["resolutions"].append({
        "resolution_id": "resolution:generic:milestone-actual-conflict@1",
        "status": "selected",
        "selected_record_id": second["header"]["record_id"],
    })
    product = build_promise_progress_product(conflict["source_package"], conflict["projection_plan"])
    row = next(
        row for row in product.blocks[1].rows
        if _field_map(row)["metric"].display_value.display_text == "Facility start milestone"
    )
    assert _field_map(row)["status"].display_value.display_text == "Needs Review"

    horizon_mismatch = copy.deepcopy(generic_oracle)
    version = next(
        value for value in horizon_mismatch["source_package"]["observations"]
        if value["header"]["record_id"] == "record:generic:milestone-origin@1"
    )
    version["payload"]["deadline"]["value"] = "period:generic:fy2027@1"
    with pytest.raises(PromiseProgressProjectionError, match="requested fiscal period"):
        build_promise_progress_product(
            horizon_mismatch["source_package"], horizon_mismatch["projection_plan"]
        )


def test_value_forms_preserve_approximation_range_bound_and_missing():
    percent = {"unit_kind": "percent", "scale": "1", "display_name": "Percent"}
    approximate = display_value_from_spec(
        {"kind": "approximate", "value": "6", "qualifier": "around", "tolerance": None}, unit=percent
    )
    assert approximate.to_dict() == {
        "value_form": "approximate",
        "display_text": "around 6%",
        "machine_value": {"value": "6", "qualifier": "around", "tolerance": None},
    }
    assert display_value_from_spec({"kind": "range", "low": "3", "high": "5"}, unit=percent).display_text == "3–5%"
    assert display_value_from_spec({"kind": "bound", "operator": "gte", "value": "6"}, unit=percent).display_text == "≥6%"
    assert display_value_from_spec(None).to_dict() == {"value_form": "missing", "display_text": "", "machine_value": None}


def test_actual_period_substitution_and_semantic_mismatch_fail_closed(generic_oracle: dict):
    wrong_period = copy.deepcopy(generic_oracle)
    wrong_period["source_package"]["periods"].append(
        {
            "period_id": "period:generic:fy2026-q4@1",
            "period_type": "quarter",
            "fiscal_year": 2026,
            "fiscal_quarter": 4,
            "fiscal_ordinal": 104,
            "start_date": "2026-10-01",
            "end_date": "2026-12-31",
        }
    )
    actual = next(
        row for row in wrong_period["source_package"]["observations"]
        if row["header"]["record_id"] == "record:generic:savings-actual@1"
    )
    actual["header"]["effective_period_id"] = "period:generic:fy2026-q4@1"
    binding = wrong_period["projection_plan"]["promise_bindings"][0]["actual"]
    binding["selector"]["effective_period_id"] = "period:generic:fy2026-q4@1"
    with pytest.raises(PromiseProgressProjectionError, match="period type is incompatible"):
        build_promise_progress_product(wrong_period["source_package"], wrong_period["projection_plan"])

    incompatible = copy.deepcopy(generic_oracle)
    incompatible_binding = incompatible["projection_plan"]["promise_bindings"][0]["actual"]
    incompatible_binding["semantic_identity"]["definition_id"] = "definition:generic:policy-dependent-target@1"
    with pytest.raises(PromiseProgressProjectionError, match="reviewed metric, definition, basis"):
        build_promise_progress_product(incompatible["source_package"], incompatible["projection_plan"])


def test_future_actual_remains_blank_instead_of_leaking_or_becoming_zero(generic_oracle: dict):
    mutated = copy.deepcopy(generic_oracle)
    actual = next(
        row for row in mutated["source_package"]["observations"]
        if row["header"]["record_id"] == "record:generic:savings-actual@1"
    )
    actual["header"]["knowledge_date"] = "2027-01-16"
    product = build_promise_progress_product(mutated["source_package"], mutated["projection_plan"])
    row = next(row for row in product.blocks[1].rows if _field_map(row)["metric"].display_value.display_text == "Minimum cost savings")
    fields = _field_map(row)
    assert fields["actual"].display_value.display_text == ""
    assert fields["actual"].display_value.machine_value is None
    assert fields["status"].display_value.display_text == "Needs Review"


def test_progress_role_cannot_relabel_run_rate_as_realized(pbi_oracle: dict, pbi_package: dict):
    mutated = copy.deepcopy(pbi_oracle["projection_plan"])
    progress = mutated["promise_bindings"][0]["progress"]
    progress["progress_role_id"] = "progress:promise-progress:realized-period-amount@1"
    with pytest.raises(PromiseProgressProjectionError, match="semantic assertion"):
        build_promise_progress_product(pbi_package, mutated)


def test_remaining_amount_calculates_from_target_and_observed_progress(
    generic_oracle: dict,
):
    mutated = copy.deepcopy(generic_oracle)
    binding = mutated["projection_plan"]["promise_bindings"][0]
    binding["progress"] = _calculated_progress_binding(
        binding["actual"], PROGRESS_REMAINING_ID, "remaining-amount", "upward-monotonic"
    )
    actual = next(
        value for value in mutated["source_package"]["observations"]
        if value["header"]["record_id"] == "record:generic:savings-actual@1"
    )
    actual["payload"]["value"]["value"] = "7"
    product = build_promise_progress_product(
        mutated["source_package"], mutated["projection_plan"]
    )
    progress = next(
        value for value in product.progress_values
        if value.progress_role_id == PROGRESS_REMAINING_ID
    )
    assert progress.display_value.machine_value == "3"
    assert progress.display_value.display_text == "$3m"
    assert progress.method_id == "calculation:promise-progress:remaining-amount@1"
    assert progress.governing_target_version_id in progress.canonical_input_ids
    assert "record:generic:savings-actual@1" in progress.canonical_input_ids
    assert len(progress.source_occurrence_ids) == 2

    changed_target = copy.deepcopy(mutated)
    version = next(
        value for value in changed_target["source_package"]["observations"]
        if value["header"]["record_id"] == progress.governing_target_version_id
    )
    version["payload"]["target"]["value"] = "9"
    recalculated = build_promise_progress_product(
        changed_target["source_package"], changed_target["projection_plan"]
    )
    changed = next(
        value for value in recalculated.progress_values
        if value.progress_role_id == PROGRESS_REMAINING_ID
    )
    assert changed.display_value.machine_value == "2"
    assert changed.lineage_digest != progress.lineage_digest

    changed_observation = copy.deepcopy(mutated)
    next(
        value for value in changed_observation["source_package"]["observations"]
        if value["header"]["record_id"] == "record:generic:savings-actual@1"
    )["payload"]["value"]["value"] = "8"
    recalculated = build_promise_progress_product(
        changed_observation["source_package"], changed_observation["projection_plan"]
    )
    changed = next(
        value for value in recalculated.progress_values
        if value.progress_role_id == PROGRESS_REMAINING_ID
    )
    assert changed.display_value.machine_value == "2"
    assert changed.lineage_digest != progress.lineage_digest


def test_delta_to_target_calculates_signed_point_difference(
    generic_oracle: dict,
):
    mutated = copy.deepcopy(generic_oracle)
    binding = mutated["projection_plan"]["promise_bindings"][0]
    binding["progress"] = _calculated_progress_binding(
        binding["actual"], PROGRESS_DELTA_ID, "delta-to-target", "higher"
    )
    version = next(
        value for value in mutated["source_package"]["observations"]
        if value["header"]["record_id"] == "record:generic:savings-origin@1"
    )
    version["payload"]["target"] = {"kind": "exact", "value": "10"}
    product = build_promise_progress_product(
        mutated["source_package"], mutated["projection_plan"]
    )
    progress = next(
        value for value in product.progress_values
        if value.progress_role_id == PROGRESS_DELTA_ID
    )
    assert progress.display_value.machine_value == "2"
    assert progress.display_value.display_text == "$2m"
    assert progress.method_id == "calculation:promise-progress:delta-to-target@1"
    assert progress.governing_target_version_id == version["header"]["record_id"]

    observed_below = copy.deepcopy(mutated)
    next(
        value for value in observed_below["source_package"]["observations"]
        if value["header"]["record_id"] == "record:generic:savings-actual@1"
    )["payload"]["value"]["value"] = "7"
    product = build_promise_progress_product(
        observed_below["source_package"], observed_below["projection_plan"]
    )
    progress = next(
        value for value in product.progress_values
        if value.progress_role_id == PROGRESS_DELTA_ID
    )
    assert progress.display_value.machine_value == "-3"
    assert progress.display_value.display_text == "$-3m"


@pytest.mark.parametrize(
    ("target", "role_id", "semantic_class", "direction"),
    [
        ({"kind": "range", "low": "8", "high": "12"}, PROGRESS_DELTA_ID, "delta-to-target", "higher"),
        ({"kind": "approximate", "value": "10", "qualifier": "around", "tolerance": None}, PROGRESS_REMAINING_ID, "remaining-amount", "upward-monotonic"),
        (None, PROGRESS_REMAINING_ID, "remaining-amount", "upward-monotonic"),
    ],
)
def test_calculated_progress_fails_closed_for_unsupported_or_missing_target(
    generic_oracle: dict,
    target,
    role_id: str,
    semantic_class: str,
    direction: str,
):
    mutated = copy.deepcopy(generic_oracle)
    binding = mutated["projection_plan"]["promise_bindings"][0]
    binding["progress"] = _calculated_progress_binding(
        binding["actual"], role_id, semantic_class, direction
    )
    version = next(
        value for value in mutated["source_package"]["observations"]
        if value["header"]["record_id"] == "record:generic:savings-origin@1"
    )
    version["payload"]["target"] = target
    product = build_promise_progress_product(
        mutated["source_package"], mutated["projection_plan"]
    )
    assert not any(value.progress_role_id == role_id for value in product.progress_values)


def test_calculated_progress_rejects_incompatible_semantics_and_missing_governing_version(
    generic_oracle: dict,
):
    incompatible = copy.deepcopy(generic_oracle)
    binding = incompatible["projection_plan"]["promise_bindings"][0]
    binding["progress"] = _calculated_progress_binding(
        binding["actual"], PROGRESS_REMAINING_ID, "remaining-amount", "upward-monotonic"
    )
    binding["target_semantic_identity"]["basis_id"] = "basis:generic:policy-dependent@1"
    with pytest.raises(PromiseProgressProjectionError, match="target and observed input differ"):
        build_promise_progress_product(
            incompatible["source_package"], incompatible["projection_plan"]
        )

    no_governing = copy.deepcopy(generic_oracle)
    binding = no_governing["projection_plan"]["promise_bindings"][0]
    binding["progress"] = _calculated_progress_binding(
        binding["actual"], PROGRESS_DELTA_ID, "delta-to-target", "higher"
    )
    version = next(
        value for value in no_governing["source_package"]["observations"]
        if value["header"]["record_id"] == "record:generic:savings-origin@1"
    )
    version["payload"]["target"] = {"kind": "exact", "value": "10"}
    no_governing["source_package"]["observations"] = [
        value for value in no_governing["source_package"]["observations"]
        if value["header"]["record_id"] != version["header"]["record_id"]
    ]
    with pytest.raises(PromiseProgressProjectionError, match="exactly one material origin"):
        build_promise_progress_product(
            no_governing["source_package"], no_governing["projection_plan"]
        )


@pytest.mark.parametrize(
    ("semantic_key", "mutated_value"),
    [
        ("unit_id", "unit:core:qualitative@1"),
        ("dimension_set_id", "dimset:generic:missing-dimensions@1"),
    ],
)
def test_calculated_progress_rejects_incompatible_unit_or_dimensions(
    generic_oracle: dict,
    semantic_key: str,
    mutated_value: str,
):
    mutated = copy.deepcopy(generic_oracle)
    binding = mutated["projection_plan"]["promise_bindings"][0]
    binding["progress"] = _calculated_progress_binding(
        binding["actual"], PROGRESS_REMAINING_ID, "remaining-amount", "upward-monotonic"
    )
    binding["progress"]["semantic_identity"][semantic_key] = mutated_value
    with pytest.raises(PromiseProgressProjectionError):
        build_promise_progress_product(
            mutated["source_package"], mutated["projection_plan"]
        )


def test_closed_actual_and_progress_role_assertions_reject_relabelling(
    generic_oracle: dict, pbi_oracle: dict, pbi_package: dict
):
    missing_assertion = copy.deepcopy(generic_oracle)
    missing_assertion["projection_plan"]["promise_bindings"][0]["actual"].pop("role_semantic_assertion")
    with pytest.raises(PromiseProgressProjectionError, match="Actual binding requires"):
        build_promise_progress_product(
            missing_assertion["source_package"], missing_assertion["projection_plan"]
        )

    wrong_class = copy.deepcopy(generic_oracle)
    assertion = wrong_class["projection_plan"]["promise_bindings"][0]["actual"]["role_semantic_assertion"]
    assertion["semantic_class"] = "cumulative-outcome"
    with pytest.raises(PromiseProgressProjectionError, match="closed class"):
        build_promise_progress_product(wrong_class["source_package"], wrong_class["projection_plan"])

    cumulative_without_basis = copy.deepcopy(pbi_oracle["projection_plan"])
    cumulative_assertion = cumulative_without_basis["promise_bindings"][0]["actual"]["role_semantic_assertion"]
    cumulative_assertion["allowed_basis_ids"] = []
    with pytest.raises(PromiseProgressProjectionError, match="cumulative semantics require"):
        build_promise_progress_product(pbi_package, cumulative_without_basis)

    incompatible_progress_basis = copy.deepcopy(pbi_oracle["projection_plan"])
    progress_assertion = incompatible_progress_basis["promise_bindings"][0]["progress"]["role_semantic_assertion"]
    progress_assertion["allowed_basis_ids"] = ["basis:business-services:net-annualized@1"]
    with pytest.raises(PromiseProgressProjectionError, match="Progress input basis is incompatible"):
        build_promise_progress_product(pbi_package, incompatible_progress_basis)

    run_rate_as_actual = copy.deepcopy(pbi_oracle["projection_plan"])
    binding = run_rate_as_actual["promise_bindings"][0]
    binding["actual"]["selector"] = copy.deepcopy(binding["progress"]["selector"])
    binding["actual"]["semantic_identity"] = {
        **binding["actual"]["semantic_identity"],
        "definition_id": "definition:business-services:annualized-run-rate@1",
        "basis_id": "basis:business-services:annualized-run-rate@1",
    }
    with pytest.raises(PromiseProgressProjectionError, match="Actual input definition is incompatible"):
        build_promise_progress_product(pbi_package, run_rate_as_actual)


def test_selected_economic_inputs_and_versions_require_source_occurrences(generic_oracle: dict):
    missing_actual_evidence = copy.deepcopy(generic_oracle)
    actual = next(
        row for row in missing_actual_evidence["source_package"]["observations"]
        if row["header"]["record_id"] == "record:generic:savings-actual@1"
    )
    actual["header"]["evidence_occurrence_ids"] = []
    with pytest.raises(PromiseProgressProjectionError, match="Selected Actual.*no source EvidenceOccurrence"):
        build_promise_progress_product(
            missing_actual_evidence["source_package"], missing_actual_evidence["projection_plan"]
        )

    missing_version_evidence = copy.deepcopy(generic_oracle)
    version = next(
        row for row in missing_version_evidence["source_package"]["observations"]
        if row["payload"].get("kind") == "GuidanceVersion"
    )
    version["header"]["evidence_occurrence_ids"] = []
    with pytest.raises(PromiseProgressProjectionError, match="Displayed.*no source EvidenceOccurrence"):
        build_promise_progress_product(
            missing_version_evidence["source_package"], missing_version_evidence["projection_plan"]
        )


def test_newer_incompatible_observation_is_not_selected(generic_oracle: dict):
    mutated = copy.deepcopy(generic_oracle)
    incompatible = copy.deepcopy(
        next(
            row for row in mutated["source_package"]["observations"]
            if row["header"]["record_id"] == "record:generic:savings-actual@1"
        )
    )
    incompatible["header"]["record_id"] = "record:generic:savings-newer-incompatible@1"
    incompatible["header"]["knowledge_date"] = "2027-01-14"
    incompatible["header"]["publication_date"] = "2027-01-14"
    incompatible["payload"]["definition_id"] = "definition:generic:policy-dependent-target@1"
    incompatible["payload"]["value"]["value"] = "999"
    mutated["source_package"]["observations"].append(incompatible)
    mutated["source_package"]["resolutions"].append(
        {
            "resolution_id": "resolution:generic:savings-newer-incompatible@1",
            "status": "selected",
            "selected_record_id": incompatible["header"]["record_id"],
        }
    )
    product = build_promise_progress_product(mutated["source_package"], mutated["projection_plan"])
    row = next(
        row for row in product.blocks[1].rows
        if _field_map(row)["metric"].display_value.display_text == "Minimum cost savings"
    )
    assert _field_map(row)["actual"].display_value.display_text == "$12m"


def test_timeline_classifier_fails_closed_for_basis_and_semantic_breaks():
    compatible = SemanticIdentity("metric:test@1", "definition:test:a@1", "basis:test:a@1", "unit:test@1", ())
    basis_changed = SemanticIdentity("metric:test@1", "definition:test:b@1", "basis:test:b@1", "unit:test@1", ())
    incompatible = SemanticIdentity("metric:test@1", "definition:test:a@1", "basis:test:a@1", "unit:other@1", ())
    value = {"kind": "range", "low": "10", "high": "20"}
    assert classify_timeline_change(
        value, value, current_semantic=basis_changed, previous_semantic=compatible
    ) == "basis-change"
    assert classify_timeline_change(
        value, value, current_semantic=incompatible, previous_semantic=compatible
    ) == "unresolved-comparison"
    assert classify_timeline_change(
        {"kind": "exact", "value": "15"}, value,
        current_semantic=compatible, previous_semantic=compatible,
    ) == "unresolved-comparison"


def test_unknown_progress_role_and_unregistered_parity_exception_fail_closed(pbi_oracle: dict, pbi_package: dict):
    unknown_role = copy.deepcopy(pbi_oracle["projection_plan"])
    unknown_role["promise_bindings"][0]["progress"]["progress_role_id"] = "progress:promise-progress:completion-percent@1"
    with pytest.raises(PromiseProgressProjectionError, match="Unknown Progress role"):
        build_promise_progress_product(pbi_package, unknown_role)
    unknown_exception = copy.deepcopy(pbi_oracle["projection_plan"])
    unknown_exception["parity_exception_ids"].append("parity:unreviewed:suppress-all@1")
    with pytest.raises(PromiseProgressProjectionError, match="unregistered parity"):
        build_promise_progress_product(pbi_package, unknown_exception)


def test_approximate_target_without_tolerance_cannot_be_marked_completed(anf_product: PromiseProgressProduct):
    statuses = [
        status for status in anf_product.status_assessments
        if status.assessment_rule_id == "assessment:promise-progress:approximate-target@1"
    ]
    assert statuses
    assert {status.status_code for status in statuses} == {"needs_review"}
    assert all("tolerance" in status.explanation for status in statuses)


def test_range_cannot_be_assessed_by_point_rule(generic_oracle: dict):
    mutated = copy.deepcopy(generic_oracle)
    mutated["projection_plan"]["guidance_bindings"][0]["status_rule_id"] = "assessment:promise-progress:numeric-point-target@1"
    product = build_promise_progress_product(mutated["source_package"], mutated["projection_plan"])
    annual = next(row for row in product.blocks[1].rows if row.canonical_series_or_program_id == "gseries:generic:policy-benefit@1")
    assert _field_map(annual)["status"].display_value.display_text == "Open"
    # Once the horizon closes, the wrong rule still cannot produce Hit or Miss.
    assert not any(
        status.status_code in {"hit", "beat", "missed"}
        and status.assessment_rule_id == "assessment:promise-progress:numeric-point-target@1"
        for status in product.status_assessments
    )


def test_stored_display_status_and_lineage_are_non_authoritative(
    generic_product: PromiseProgressProduct, generic_oracle: dict
):
    status_field = next(
        field for field in generic_product.fields
        if field.field_role == "status" and field.display_value.display_text == "Beat"
    )
    mutated = _replace_field(
        generic_product,
        status_field.field_id,
        display_value=DisplayValue("qualitative", "Completed", "completed"),
    )
    issues = validate_promise_progress_product(
        mutated,
        package=generic_oracle["source_package"],
        plan=generic_oracle["projection_plan"],
    )
    assert {issue["rule_id"] for issue in issues} >= {
        "promise_progress_lineage_digest", "promise_progress_semantic_replay"
    }


def test_destination_canonical_lineage_and_parity_scope_mutations_fail(
    generic_product: PromiseProgressProduct, generic_oracle: dict
):
    field = next(value for value in generic_product.fields if value.field_role == "metric")
    wrong_destination = _replace_field(generic_product, field.field_id, anchor_cell="M1")
    destination_issues = validate_promise_progress_product(
        wrong_destination,
        package=generic_oracle["source_package"],
        plan=generic_oracle["projection_plan"],
    )
    assert "promise_progress_destination_mapping" in {issue["rule_id"] for issue in destination_issues}

    missing_input = _replace_field(
        generic_product,
        field.field_id,
        canonical_record_ids=("record:missing:canonical-input@1",),
    )
    input_issues = validate_promise_progress_product(
        missing_input,
        package=generic_oracle["source_package"],
        plan=generic_oracle["projection_plan"],
    )
    assert "promise_progress_missing_canonical_input" in {issue["rule_id"] for issue in input_issues}

    wrong_scope = _replace_field(
        generic_product,
        field.field_id,
        parity_exception_ids=(STATIC_STATUS_EXCEPTION_ID,),
    )
    scope_issues = validate_promise_progress_product(
        wrong_scope,
        package=generic_oracle["source_package"],
        plan=generic_oracle["projection_plan"],
    )
    assert "promise_progress_exception_scope" in {issue["rule_id"] for issue in scope_issues}


def test_wrong_target_version_and_static_status_cannot_override_replay(
    pbi_product: PromiseProgressProduct, pbi_package: dict, pbi_oracle: dict
):
    status_field = next(
        field for field in pbi_product.fields
        if field.field_role == "status" and field.target_version_id is not None
    )
    other_version = next(
        record["header"]["record_id"]
        for record in pbi_package["observations"]
        if record["payload"].get("kind") in {"GuidanceVersion", "PromiseVersion"}
        and record["header"]["record_id"] != status_field.target_version_id
    )
    mutated = _replace_field(
        pbi_product,
        status_field.field_id,
        target_version_id=other_version,
        display_value=DisplayValue("qualitative", "Hit", "hit"),
    )
    issues = validate_promise_progress_product(
        mutated,
        package=pbi_package,
        plan=pbi_oracle["projection_plan"],
    )
    assert {issue["rule_id"] for issue in issues} >= {
        "promise_progress_lineage_digest",
        "promise_progress_semantic_replay",
    }


def test_blank_to_zero_and_approximate_to_exact_mutations_fail_replay(
    anf_product: PromiseProgressProduct, anf_package: dict, anf_oracle: dict
):
    blank = next(field for field in anf_product.fields if field.field_role == "actual" and field.display_value.value_form == "missing")
    blank_mutation = _replace_field(anf_product, blank.field_id, display_value=DisplayValue("exact", "0", "0"))
    assert any(
        issue["rule_id"] == "promise_progress_semantic_replay"
        for issue in validate_promise_progress_product(blank_mutation, package=anf_package, plan=anf_oracle["projection_plan"])
    )
    approximate = next(field for field in anf_product.fields if field.display_value.value_form == "approximate")
    exact_mutation = _replace_field(
        anf_product,
        approximate.field_id,
        display_value=DisplayValue("exact", approximate.display_value.display_text.lstrip("~"), "40"),
    )
    assert any(
        issue["rule_id"] == "promise_progress_semantic_replay"
        for issue in validate_promise_progress_product(exact_mutation, package=anf_package, plan=anf_oracle["projection_plan"])
    )


def test_duplicate_version_and_ambiguous_actual_fail_closed(generic_oracle: dict):
    duplicate = copy.deepcopy(generic_oracle)
    duplicate["source_package"]["observations"].append(copy.deepcopy(duplicate["source_package"]["observations"][0]))
    with pytest.raises(PromiseProgressProjectionError, match="Duplicate observations identity"):
        build_promise_progress_product(duplicate["source_package"], duplicate["projection_plan"])

    conflict = copy.deepcopy(generic_oracle)
    second = copy.deepcopy(next(
        row for row in conflict["source_package"]["observations"]
        if row["header"]["record_id"] == "record:generic:savings-actual@1"
    ))
    second["header"]["record_id"] = "record:generic:savings-actual-conflict@1"
    second["payload"]["value"]["value"] = "11"
    conflict["source_package"]["observations"].append(second)
    conflict["source_package"]["resolutions"].append(
        {"resolution_id": "resolution:generic:savings-actual-conflict@1", "status": "selected", "selected_record_id": second["header"]["record_id"]}
    )
    product = build_promise_progress_product(conflict["source_package"], conflict["projection_plan"])
    row = next(row for row in product.blocks[1].rows if _field_map(row)["metric"].display_value.display_text == "Minimum cost savings")
    assert _field_map(row)["actual"].display_value.display_text == ""
    assert _field_map(row)["status"].display_value.display_text == "Needs Review"


def test_history_mutations_change_golden_and_do_not_silently_select(pbi_oracle: dict, pbi_package: dict, pbi_product: PromiseProgressProduct):
    promise_program_id = "program:pbi:2024-cost-rationalization@1"
    reaffirmation = next(
        row for row in pbi_package["observations"]
        if row["payload"].get("kind") == "PromiseVersion" and row["payload"].get("change_kind") == "reaffirmation"
    )
    missing = copy.deepcopy(pbi_package)
    missing["observations"] = [row for row in missing["observations"] if row["header"]["record_id"] != reaffirmation["header"]["record_id"]]
    missing["relations"] = [
        row for row in missing["relations"]
        if reaffirmation["header"]["record_id"] not in {row.get("from_record_id"), row.get("to_record_id")}
    ]
    with pytest.raises(PromiseProgressProjectionError, match="predecessor is absent"):
        build_promise_progress_product(missing, pbi_oracle["projection_plan"])

    origin_mutation = copy.deepcopy(pbi_package)
    update = next(
        row for row in origin_mutation["observations"]
        if row["payload"].get("kind") == "PromiseVersion" and row["header"].get("publication_date") == "2024-07-01"
    )
    update["payload"]["change_kind"] = "origin"
    with pytest.raises(PromiseProgressProjectionError, match="exactly one material origin"):
        build_promise_progress_product(origin_mutation, pbi_oracle["projection_plan"])


@pytest.mark.parametrize("fixture_name", ["anf", "pbi", "generic"])
def test_reverse_and_seeded_shuffle_are_byte_identical(request, fixture_name: str):
    oracle = request.getfixturevalue(f"{fixture_name}_oracle")
    package = copy.deepcopy(
        oracle["source_package"] if "source_package" in oracle else request.getfixturevalue(f"{fixture_name}_package")
    )
    baseline = request.getfixturevalue(f"{fixture_name}_product")
    list_keys = [
        "entities", "observations", "relations", "resolutions", "review_issues",
        "source_documents", "evidence_occurrences", "periods"
    ]
    for key in list_keys:
        package[key] = list(reversed(package.get(key, [])))
    for collection in package.get("catalog", {}).values():
        if isinstance(collection, list):
            collection.reverse()
    reversed_product = build_promise_progress_product(package, oracle["projection_plan"])
    assert serialize_promise_progress_product(reversed_product) == serialize_promise_progress_product(baseline)
    assert serialize_shadow_matrix(reversed_product) == serialize_shadow_matrix(baseline)

    rng = random.Random(20260803)
    for key in list_keys:
        rng.shuffle(package[key])
    for collection in package.get("catalog", {}).values():
        if isinstance(collection, list):
            rng.shuffle(collection)
    shuffled = build_promise_progress_product(package, oracle["projection_plan"])
    assert serialize_promise_progress_product(shuffled) == serialize_promise_progress_product(baseline)

    reversed_mappings = build_promise_progress_product(
        _reverse_mapping_insertion(package),
        _reverse_mapping_insertion(copy.deepcopy(oracle["projection_plan"])),
    )
    assert serialize_promise_progress_product(reversed_mappings) == serialize_promise_progress_product(baseline)


@pytest.mark.parametrize("fixture_name", ["anf", "pbi", "generic"])
def test_independent_process_hash_seed_determinism(request, fixture_name: str):
    oracle = request.getfixturevalue(f"{fixture_name}_oracle")
    script = (
        "import hashlib,json; from pathlib import Path; "
        "from pbi_xbrl.longitudinal_memory.promise_progress_projection import build_promise_progress_product,serialize_promise_progress_product; "
        "from pbi_xbrl.longitudinal_memory.source_adapter.builder import build_source_native_sidecar; "
        "from pbi_xbrl.longitudinal_memory.sector_packs.retail import RETAIL_SECTOR_PACK; "
        "from pbi_xbrl.longitudinal_memory.ticker_profiles.anf import load_anf_profile; "
        f"n={fixture_name!r}; r=Path.cwd(); "
        "o={'anf':'anf_legacy_oracle.v1.json','pbi':'pbi_capability_oracle.v1.json','generic':'gpre_capability_oracle.v1.json'}[n]; "
        "f=json.loads((r/'tests/fixtures/promise_progress'/o).read_text(encoding='utf-8')); "
        "p=(f['source_package'] if n=='generic' else build_source_native_sidecar(r/f['source_package_fixture'],source_root=Path(r'C:\\Users\\Jibbe\\Aktier\\StockModelData'),reviewed_model_root=r,sector_pack=RETAIL_SECTOR_PACK,ticker_profile_loader=load_anf_profile).package if n=='anf' else json.loads((r/f['source_package_fixture']).read_text(encoding='utf-8'))); "
        "print(hashlib.sha256(serialize_promise_progress_product(build_promise_progress_product(p,f['projection_plan']))).hexdigest())"
    )
    outputs = []
    for seed in ("1", "777", "4294967295"):
        env = dict(os.environ, PYTHONHASHSEED=seed, PYTHONDONTWRITEBYTECODE="1")
        outputs.append(
            subprocess.check_output([sys.executable, "-c", script], cwd=REPO, env=env, text=True).strip()
        )
    assert outputs == [oracle["expected_product_sha256"]] * 3


def test_static_product_runtime_has_no_ticker_company_or_workbook_branch():
    source = (REPO / "pbi_xbrl" / "longitudinal_memory" / "promise_progress_projection.py").read_text(encoding="utf-8")
    forbidden = ("SendTech", "Presort", "Pitney Bowes", "Abercrombie", "GPRE", "openpyxl", "win32com", "Excel.Application")
    assert all(token not in source for token in forbidden)
    assert "source_adapter" not in source
    assert "items[0]" not in source
    assert "abs(day_count" not in source


def test_visible_destinations_reserve_m_and_n_and_row_ids_are_nonopaque(
    anf_product: PromiseProgressProduct,
):
    serialized = json.loads(serialize_promise_progress_product(anf_product))
    assert serialized["template"]["hidden_columns"] == {"M": "reserved-blank", "N": "reserved-blank", "O": "row_id"}
    assert all(field.anchor_cell[0] not in {"M", "N", "O"} for field in anf_product.fields)
    assert all(row.row_id.startswith("promise-progress-row:") and len(row.row_id) < 90 for row in anf_product.ordered_rows)
    assert len({row.row_id for row in anf_product.ordered_rows}) == len(anf_product.ordered_rows)


def test_applied_parity_exceptions_are_exactly_field_scoped(anf_product: PromiseProgressProduct):
    used = {exception for field in anf_product.fields for exception in field.parity_exception_ids}
    structural = {EMPTY_TRACKER_EXCEPTION_ID, LOSSY_MATRIX_EXCEPTION_ID, FUZZY_TRACE_EXCEPTION_ID}
    authorized = {
        row["exception_id"]
        for row in anf_product.parity_report()["field_comparisons"]
        if row["classification"] == "registered-authorized-exception"
    }
    assert set(anf_product.structural_parity_exception_ids) == structural
    assert used | structural | authorized == set(anf_product.applied_parity_exception_ids)
    assert used <= CLOSED_PARITY_EXCEPTION_IDS
    assert {SCORECARD_EXCEPTION_ID, STATIC_STATUS_EXCEPTION_ID, GENERIC_SOURCE_EXCEPTION_ID, ACTUAL_COVERAGE_EXCEPTION_ID} <= used
    assert {DISPLAY_NORMALIZATION_EXCEPTION_ID, ROW_REMAP_EXCEPTION_ID} <= authorized
    assert anf_product.parity_report()["unregistered_difference_count"] == 0
    report = {row["exception_id"]: row for row in anf_product.parity_report()["applied"]}
    assert report[FUZZY_TRACE_EXCEPTION_ID]["affected_row_ids"] == [
        row.row_id for row in anf_product.ordered_rows
    ]
    assert report[EMPTY_TRACKER_EXCEPTION_ID]["affected_product_ids"] == [anf_product.product_id]


def test_parity_report_compares_all_frozen_legacy_fields_and_declares_missing_oracles(
    anf_product: PromiseProgressProduct,
    pbi_product: PromiseProgressProduct,
    generic_product: PromiseProgressProduct,
):
    report = anf_product.parity_report()
    assert report["comparison_scope"]["state"] == "declared"
    assert report["comparison_scope"]["legacy_field_count"] == 284
    assert report["comparison_scope"]["source_native_field_count"] == 284
    assert report["comparison_scope"]["semantic_pair_count"] == 14
    assert report["comparison_scope"]["legacy_only_row_count"] == 17
    assert report["comparison_scope"]["source_native_only_row_count"] == 17
    assert report["comparison_scope"]["accepted_binding_count"] == 90
    assert report["comparison_scope"]["exception_policy_definition_count"] == 15
    assert len(report["field_comparisons"]) == 467
    assert sum(report["comparison_counts"].values()) == 467
    assert report["unregistered_difference_count"] == 0
    assert report["comparison_counts"] == {
        "accepted-semantic-match": 3,
        "exact-match": 8,
        "legacy-only-field": 183,
        "mapping-alignment-defect": 0,
        "registered-authorized-exception": 90,
        "source-native-only-field": 183,
        "structurally-incomparable": 0,
        "unauthorized-exception-binding": 0,
        "unregistered-difference": 0,
    }
    assert report["unused_accepted_difference_bindings"] == []
    assert report["unused_registered_exception_ids"] == []
    assert report["row_disposition_counts"] == {
        "paired_rows": 14,
        "authorized_legacy_only_rows": 17,
        "authorized_source_native_only_rows": 17,
        "unauthorized_one_sided_rows": 0,
        "missing_row_dispositions": 0,
        "duplicate_row_dispositions": 0,
        "counterpart_conflicts": 0,
        "mapping_alignment_defects": 0,
    }
    assert report["structural_counts"] == {
        "observed_structural_differences": 3,
        "authorized_structural_bindings": 3,
        "unauthorized_structural_bindings": 0,
        "unused_active_structural_policies": 0,
        "overbroad_structural_policy_scopes": 0,
    }
    assert len(report["row_dispositions"]) == 48
    assert len(report["structural_bindings"]) == 3
    assert report["completeness"]["legacy_capture_digest_state"] == "exact"
    assert report["completeness"]["source_scope_digest_state"] == "exact"
    assert report["completeness"]["field_inventory_state"] == "exact"

    for product in (pbi_product, generic_product):
        unavailable = product.parity_report()
        assert unavailable["comparison_scope"]["state"] == "not-declared"
        assert unavailable["unregistered_difference_count"] is None
        assert unavailable["field_comparisons"] == []


def test_parity_comparator_blocks_mutated_legacy_or_source_native_values(
    anf_oracle: dict,
    anf_package: dict,
    anf_product: PromiseProgressProduct,
):
    legacy_mutation = copy.deepcopy(anf_oracle["projection_plan"])
    legacy_mutation["legacy_parity"]["rows"][0]["display_values"][0] += " mutated"
    with pytest.raises(PromiseProgressProjectionError, match="capture manifest|unregistered differences|unused or overbroad"):
        build_promise_progress_product(anf_package, legacy_mutation)

    field = next(value for value in anf_product.fields if value.anchor_cell == "A5")
    source_mutation = _replace_field(
        anf_product,
        field.field_id,
        display_value=DisplayValue("qualitative", "Mutated category", "Mutated category"),
    )
    mutated_report = source_mutation.parity_report()
    assert mutated_report["unregistered_difference_count"] == 1
    comparison = next(
        row
        for row in mutated_report["field_comparisons"]
        if row["legacy_destination_cell"] == "A5"
        and row["source_native_destination_cell"] == "A5"
    )
    assert comparison["classification"] == "unregistered-difference"


def test_parity_exception_binding_is_exact_to_destination_class_and_digest(
    anf_oracle: dict,
    anf_package: dict,
):
    removed = copy.deepcopy(anf_oracle["projection_plan"])
    binding = removed["legacy_parity_accepted_differences"].pop(0)
    with pytest.raises(PromiseProgressProjectionError, match="unregistered differences|unused or overbroad"):
        build_promise_progress_product(anf_package, removed)

    wrong_scope = copy.deepcopy(anf_oracle["projection_plan"])
    moved = wrong_scope["legacy_parity_accepted_differences"][0]
    moved["legacy_destination_cell"] = "A5"
    moved["source_native_destination_cell"] = "A5"
    _recompute_parity_authorization(wrong_scope, moved)
    with pytest.raises(PromiseProgressProjectionError, match="unregistered differences|unused or overbroad"):
        build_promise_progress_product(anf_package, wrong_scope)


def test_parity_authorization_groups_are_closed_and_lossy_matrix_never_authorizes_fields(
    anf_oracle: dict,
    anf_product: PromiseProgressProduct,
):
    plan = anf_oracle["projection_plan"]
    bindings = plan["legacy_parity_accepted_differences"]
    assert len(bindings) == 90
    assert Counter(binding["semantic_reason_code"] for binding in bindings) == {
        "accepted_annual_actual_unavailable": 9,
        "equivalent_label_display_normalized": 7,
        "equivalent_value_display_normalized": 13,
        "legacy_generic_source_note_replaced_by_lineage": 2,
        "legacy_unreviewed_scorecard_value": 15,
        "reviewed_row_destination_remap": 44,
    }
    assert Counter(binding["exception_id"] for binding in bindings) == {
        ACTUAL_COVERAGE_EXCEPTION_ID: 9,
        DISPLAY_NORMALIZATION_EXCEPTION_ID: 20,
        GENERIC_SOURCE_EXCEPTION_ID: 2,
        ROW_REMAP_EXCEPTION_ID: 44,
        SCORECARD_EXCEPTION_ID: 15,
    }
    assert all(binding["exception_id"] != LOSSY_MATRIX_EXCEPTION_ID for binding in bindings)
    assert len({binding["binding_id"] for binding in bindings}) == 90
    assert len({binding["exception_authorization_digest"] for binding in bindings}) == 90
    report = anf_product.parity_report()
    assert report["comparison_counts"]["mapping-alignment-defect"] == 0
    assert report["comparison_counts"]["unauthorized-exception-binding"] == 0
    assert report["comparison_counts"]["unregistered-difference"] == 0

    register = _strict_json(PARITY_REGISTER)
    registered_policies = sorted(
        [
            policy
            for definition in register["exceptions"]
            for policy in definition["authorization_policies"]
        ],
        key=lambda policy: policy["policy_id"],
    )
    assert registered_policies == sorted(
        plan["legacy_parity"]["exception_policies"], key=lambda policy: policy["policy_id"]
    )


@pytest.mark.parametrize(
    ("legacy_destination", "target_exception", "replace_reason"),
    [
        ("A13", SCORECARD_EXCEPTION_ID, False),
        ("A13", SCORECARD_EXCEPTION_ID, True),
        ("B5", TEMPORAL_EXCEPTION_ID, True),
    ],
)
def test_active_but_semantically_wrong_exception_and_reason_fail(
    anf_oracle: dict,
    anf_package: dict,
    legacy_destination: str,
    target_exception: str,
    replace_reason: bool,
):
    plan = copy.deepcopy(anf_oracle["projection_plan"])
    binding = _binding(plan, legacy_destination=legacy_destination)
    binding["exception_id"] = target_exception
    target_policy = next(
        policy
        for policy in plan["legacy_parity"]["exception_policies"]
        if policy["exception_id"] == target_exception
    )
    binding["exception_policy_version"] = target_policy["exception_policy_version"]
    if replace_reason:
        binding["semantic_reason_code"] = target_policy["allowed_difference_reason_codes"][0]
    _recompute_parity_authorization(plan, binding, policy=target_policy)
    with pytest.raises(
        PromiseProgressProjectionError,
        match="unregistered differences|unused or overbroad|field role outside its block contract",
    ):
        build_promise_progress_product(anf_package, plan)


@pytest.mark.parametrize(
    ("selector_role", "target_exception"),
    [
        ("notes_source", STATIC_STATUS_EXCEPTION_ID),
        ("status", GENERIC_SOURCE_EXCEPTION_ID),
    ],
)
def test_cross_group_exception_substitution_fails(
    anf_oracle: dict,
    anf_package: dict,
    selector_role: str,
    target_exception: str,
):
    plan = copy.deepcopy(anf_oracle["projection_plan"])
    binding = _binding(plan, field_role=selector_role)
    binding["exception_id"] = target_exception
    target_policy = next(
        policy
        for policy in plan["legacy_parity"]["exception_policies"]
        if policy["exception_id"] == target_exception
    )
    binding["exception_policy_version"] = target_policy["exception_policy_version"]
    binding["semantic_reason_code"] = target_policy["allowed_difference_reason_codes"][0]
    _recompute_parity_authorization(plan, binding, policy=target_policy)
    with pytest.raises(PromiseProgressProjectionError, match="unregistered differences|unused or overbroad"):
        build_promise_progress_product(anf_package, plan)


def test_binding_reason_is_replayed_independently(
    anf_oracle: dict,
    anf_package: dict,
):
    plan = copy.deepcopy(anf_oracle["projection_plan"])
    binding = _binding(plan, legacy_destination="A13")
    assert binding["semantic_reason_code"] == "equivalent_label_display_normalized"
    binding["semantic_reason_code"] = "equivalent_value_display_normalized"
    _recompute_parity_authorization(plan, binding)
    with pytest.raises(PromiseProgressProjectionError, match="unregistered differences|unused or overbroad"):
        build_promise_progress_product(anf_package, plan)


@pytest.mark.parametrize(
    "policy_member",
    [
        "allowed_block_ids",
        "allowed_field_roles",
        "allowed_destination_pairs",
        "allowed_business_key_classes",
        "allowed_reviewed_business_keys",
    ],
)
def test_exact_exception_policy_rejects_unauthorized_scope(
    anf_oracle: dict,
    anf_package: dict,
    policy_member: str,
):
    plan = copy.deepcopy(anf_oracle["projection_plan"])
    binding = _binding(plan, legacy_destination="A13")
    policy = _policy(plan, binding["exception_id"])
    expected = {
        "allowed_block_ids": binding["block_id"],
        "allowed_field_roles": binding["field_role"],
        "allowed_business_key_classes": binding["business_key_class"],
        "allowed_reviewed_business_keys": binding["reviewed_legacy_business_key"],
    }
    if policy_member == "allowed_destination_pairs":
        pair = {
            "legacy_destination_cell": binding["legacy_destination_cell"],
            "source_native_destination_cell": binding["source_native_destination_cell"],
        }
        policy[policy_member].remove(pair)
    else:
        policy[policy_member].remove(expected[policy_member])
    with pytest.raises(
        PromiseProgressProjectionError,
        match="unregistered differences|unused or overbroad|field role outside its block contract",
    ):
        build_promise_progress_product(anf_package, plan)


def test_authorization_digest_policy_version_reason_and_cardinality_are_replayed(
    anf_oracle: dict,
    anf_package: dict,
):
    stale_digest = copy.deepcopy(anf_oracle["projection_plan"])
    stale_binding = _binding(stale_digest, legacy_destination="A13")
    stale_binding["exception_authorization_digest"] = "0" * 64
    with pytest.raises(PromiseProgressProjectionError, match="unregistered differences|unused or overbroad"):
        build_promise_progress_product(anf_package, stale_digest)

    changed_policy = copy.deepcopy(anf_oracle["projection_plan"])
    changed = _policy(changed_policy, DISPLAY_NORMALIZATION_EXCEPTION_ID)
    changed["source_native_rationale"] += " mutated"
    with pytest.raises(PromiseProgressProjectionError, match="unregistered differences|unused or overbroad"):
        build_promise_progress_product(anf_package, changed_policy)

    changed_version = copy.deepcopy(anf_oracle["projection_plan"])
    _policy(changed_version, DISPLAY_NORMALIZATION_EXCEPTION_ID)["exception_policy_version"] = (
        "parity-policy-version:2@1"
    )
    with pytest.raises(PromiseProgressProjectionError, match="unregistered differences|unused or overbroad"):
        build_promise_progress_product(anf_package, changed_version)

    changed_reason = copy.deepcopy(anf_oracle["projection_plan"])
    _policy(changed_reason, DISPLAY_NORMALIZATION_EXCEPTION_ID)["allowed_difference_reason_codes"] = [
        "legacy_unreviewed_scorecard_value"
    ]
    with pytest.raises(PromiseProgressProjectionError, match="unregistered differences|unused or overbroad"):
        build_promise_progress_product(anf_package, changed_reason)

    zero_scope = copy.deepcopy(anf_oracle["projection_plan"])
    zero_scope["legacy_parity"]["exception_policies"] = [
        policy
        for policy in zero_scope["legacy_parity"]["exception_policies"]
        if policy["exception_id"] != DISPLAY_NORMALIZATION_EXCEPTION_ID
    ]
    with pytest.raises(PromiseProgressProjectionError, match="unregistered differences|unused or overbroad"):
        build_promise_progress_product(anf_package, zero_scope)

    multiple_scope = copy.deepcopy(anf_oracle["projection_plan"])
    duplicate = copy.deepcopy(_policy(multiple_scope, DISPLAY_NORMALIZATION_EXCEPTION_ID))
    duplicate["policy_id"] = "policy:promise-progress:equivalent-display-normalization-duplicate@1"
    multiple_scope["legacy_parity"]["exception_policies"].append(duplicate)
    with pytest.raises(PromiseProgressProjectionError, match="unregistered differences|unused or overbroad"):
        build_promise_progress_product(anf_package, multiple_scope)


def test_wildcard_inactive_and_removed_exception_policy_fail_closed(
    anf_oracle: dict,
    anf_package: dict,
):
    wildcard = copy.deepcopy(anf_oracle["projection_plan"])
    _policy(wildcard, DISPLAY_NORMALIZATION_EXCEPTION_ID)["allowed_field_roles"].append("*")
    with pytest.raises(PromiseProgressProjectionError, match="Wildcard parity authorization"):
        build_promise_progress_product(anf_package, wildcard)

    inactive = copy.deepcopy(anf_oracle["projection_plan"])
    _policy(inactive, DISPLAY_NORMALIZATION_EXCEPTION_ID)["state"] = "inactive"
    with pytest.raises(PromiseProgressProjectionError, match="unregistered differences|unused or overbroad"):
        build_promise_progress_product(anf_package, inactive)

    removed = copy.deepcopy(anf_oracle["projection_plan"])
    removed["legacy_parity"]["exception_policies"] = [
        policy
        for policy in removed["legacy_parity"]["exception_policies"]
        if policy["exception_id"] != DISPLAY_NORMALIZATION_EXCEPTION_ID
    ]
    with pytest.raises(PromiseProgressProjectionError, match="unregistered differences|unused or overbroad"):
        build_promise_progress_product(anf_package, removed)

    overbroad = copy.deepcopy(anf_oracle["projection_plan"])
    broad_policy = _policy(overbroad, DISPLAY_NORMALIZATION_EXCEPTION_ID)
    broad_policy["allowed_field_roles"].append("status")
    for binding in overbroad["legacy_parity_accepted_differences"]:
        if binding["exception_id"] == DISPLAY_NORMALIZATION_EXCEPTION_ID:
            _recompute_parity_authorization(overbroad, binding, policy=broad_policy)
    with pytest.raises(PromiseProgressProjectionError, match="unused or overbroad"):
        build_promise_progress_product(anf_package, overbroad)


def test_semantic_row_pair_and_reviewed_business_key_cannot_be_reassigned(
    anf_oracle: dict,
    anf_package: dict,
):
    swapped = copy.deepcopy(anf_oracle["projection_plan"])
    rows = swapped["legacy_parity"]["rows"]
    revenue = next(row for row in rows if row["visible_sheet_row"] == 13)
    margin = next(row for row in rows if row["visible_sheet_row"] == 14)
    for key in (
        "row_type",
        "business_key_class",
        "reviewed_semantic_identity",
        "counterpart_signature",
    ):
        revenue[key], margin[key] = copy.deepcopy(margin[key]), copy.deepcopy(revenue[key])
    with pytest.raises(PromiseProgressProjectionError, match="capture manifest|counterpart|row disposition"):
        build_promise_progress_product(anf_package, swapped)

    wrong_business_key = copy.deepcopy(anf_oracle["projection_plan"])
    binding = _binding(wrong_business_key, legacy_destination="A13")
    other = _binding(wrong_business_key, legacy_destination="A14")
    binding["reviewed_legacy_business_key"] = other["reviewed_legacy_business_key"]
    _recompute_parity_authorization(wrong_business_key, binding)
    with pytest.raises(PromiseProgressProjectionError, match="unregistered differences|unused or overbroad"):
        build_promise_progress_product(anf_package, wrong_business_key)


def test_source_value_or_source_semantic_reason_drift_invalidates_reviewed_authorization(
    anf_product: PromiseProgressProduct,
):
    comparison = next(
        row
        for row in anf_product.parity_report()["field_comparisons"]
        if row["classification"] == "registered-authorized-exception"
        and row["difference_reason_code"] == "equivalent_label_display_normalized"
    )
    value_mutation = _replace_field(
        anf_product,
        comparison["source_native_field_id"],
        display_value=DisplayValue("qualitative", "Mutated source-native label", "Mutated source-native label"),
    )
    assert value_mutation.parity_report()["unregistered_difference_count"] == 1
    assert any(
        row["classification"] == "unauthorized-exception-binding"
        and row["source_native_field_id"] == comparison["source_native_field_id"]
        for row in value_mutation.parity_report()["field_comparisons"]
    )

    coverage = next(
        row
        for row in anf_product.parity_report()["field_comparisons"]
        if row["classification"] == "registered-authorized-exception"
        and row["difference_reason_code"] == "accepted_annual_actual_unavailable"
        and row["field_role"] == "actual"
    )
    field_value = next(field for field in anf_product.fields if field.field_id == coverage["source_native_field_id"])
    reason_mutation = _replace_field(
        anf_product,
        field_value.field_id,
        parity_exception_ids=tuple(
            value for value in field_value.parity_exception_ids if value != ACTUAL_COVERAGE_EXCEPTION_ID
        ),
    )
    mutated = next(
        row
        for row in reason_mutation.parity_report()["field_comparisons"]
        if row["source_native_field_id"] == field_value.field_id
    )
    assert mutated["classification"] == "unauthorized-exception-binding"
    assert mutated["difference_reason_code"] == "equivalent_value_display_normalized"


def test_parity_comparison_is_read_only_over_reviewed_bindings(
    anf_oracle: dict,
    anf_package: dict,
):
    plan = copy.deepcopy(anf_oracle["projection_plan"])
    before = copy.deepcopy(plan["legacy_parity_accepted_differences"])
    product = build_promise_progress_product(anf_package, plan)
    product.parity_report()
    assert plan["legacy_parity_accepted_differences"] == before


def test_reviewed_row_disposition_graph_is_complete_closed_and_field_accounted(
    anf_oracle: dict,
    anf_product: PromiseProgressProduct,
):
    plan = anf_oracle["projection_plan"]
    dispositions = plan["legacy_parity"]["row_dispositions"]
    assert Counter(item["disposition_kind"] for item in dispositions) == {
        "paired": 14,
        "legacy_only": 17,
        "source_native_only": 17,
    }
    assert Counter(item["reason_code"] for item in dispositions) == {
        "reviewed_semantic_row_pair": 14,
        "legacy_unsupported_by_accepted_product": 15,
        "legacy_terminal_summary_replaced_by_typed_history": 2,
        "source_native_canonical_row_legacy_omitted": 2,
        "source_native_typed_history_legacy_omitted": 15,
    }
    legacy_rows = plan["legacy_parity"]["rows"]
    assert {item["legacy_row_id"] for item in dispositions if item["legacy_row_id"]} == {
        row["legacy_row_id"] for row in legacy_rows
    }
    assert {
        item["source_native_row_id"] for item in dispositions if item["source_native_row_id"]
    } == {row.row_id for row in anf_product.ordered_rows}
    assert len({item["disposition_id"] for item in dispositions}) == 48
    assert all(item["authorization_digest"] != "0" * 64 for item in dispositions)

    report = anf_product.parity_report()
    one_sided = [
        item
        for item in report["field_comparisons"]
        if item["classification"] in {"legacy-only-field", "source-native-only-field"}
    ]
    assert Counter(item["classification"] for item in one_sided) == {
        "legacy-only-field": 183,
        "source-native-only-field": 183,
    }
    assert all(item["row_disposition_id"] for item in one_sided)
    assert all(item["row_disposition_policy_id"] for item in one_sided)
    assert all(item["row_disposition_authorization_digest"] for item in one_sided)
    assert all(
        item["difference_reason_code"] == item["row_disposition_reason_code"]
        for item in one_sided
    )


@pytest.mark.parametrize(
    "mutation",
    [
        "delete-row",
        "delete-field",
        "add-row",
        "change-destination",
        "change-row-id",
        "change-matrix-local-digest",
        "change-capture-manifest",
        "change-workbook-sha",
        "change-sheet",
        "change-count",
    ],
)
def test_frozen_legacy_capture_completeness_is_independently_pinned(
    anf_oracle: dict,
    anf_package: dict,
    mutation: str,
):
    plan = copy.deepcopy(anf_oracle["projection_plan"])
    oracle = plan["legacy_parity"]
    if mutation == "delete-row":
        oracle["rows"] = [row for row in oracle["rows"] if row["visible_sheet_row"] != 16]
        _recompute_capture_manifest(plan, update_independent=False)
    elif mutation == "delete-field":
        row = _legacy_row(plan, 16)
        row["display_values"].pop()
    elif mutation == "add-row":
        row = copy.deepcopy(_legacy_row(plan, 16))
        row["legacy_row_id"] = "legacy-row:promise-progress:annual:18@1"
        row["visible_sheet_row"] = 18
        row["reviewed_legacy_business_key"] = "legacy-business-key:anf:annual:row-18@1"
        oracle["rows"].append(row)
        _recompute_capture_manifest(plan, update_independent=False)
    elif mutation == "change-destination":
        _legacy_row(plan, 16)["visible_sheet_row"] = 18
        _recompute_capture_manifest(plan, update_independent=False)
    elif mutation == "change-row-id":
        _legacy_row(plan, 16)["legacy_row_id"] = "legacy-row:promise-progress:annual:16-mutated@1"
        _recompute_capture_manifest(plan, update_independent=False)
    elif mutation == "change-matrix-local-digest":
        _legacy_row(plan, 16)["display_values"][0] = "Mutated legacy value"
        _recompute_capture_manifest(plan, update_independent=False)
    elif mutation == "change-capture-manifest":
        oracle["capture_manifest"]["used_comparison_scope"] = (
            "Promise_Progress_UI!A5:L88:reviewed-populated-product-rows@1"
        )
    elif mutation == "change-workbook-sha":
        oracle["workbook_oracle_sha256"] = "0" * 64
        oracle["capture_manifest"]["workbook_oracle_sha256"] = "0" * 64
        _recompute_capture_manifest(plan, update_independent=False)
    elif mutation == "change-sheet":
        oracle["sheet_name"] = "Promise_Progress_UI_Mutated"
    else:
        oracle["capture_manifest"]["row_count"] += 1
    with pytest.raises(PromiseProgressProjectionError):
        build_promise_progress_product(anf_package, plan)


def test_source_native_scope_manifest_detects_disappearing_source_only_row(
    anf_oracle: dict,
    anf_package: dict,
):
    plan = copy.deepcopy(anf_oracle["projection_plan"])
    plan["coverage_gaps"] = [
        item
        for item in plan["coverage_gaps"]
        if item["metric_label"] != "FY2025 capital expenditures"
    ]
    with pytest.raises(PromiseProgressProjectionError, match="Source-native parity scope"):
        build_promise_progress_product(anf_package, plan)


@pytest.mark.parametrize(
    "mutation",
    [
        "duplicate-legacy-only",
        "duplicate-source-only",
        "omit-legacy-only",
        "omit-source-only",
        "wrong-reason",
        "wrong-policy",
        "wrong-business-key",
        "wrong-row-type",
        "wrong-block",
        "stale-field-inventory",
    ],
)
def test_one_sided_disposition_mutations_fail_closed(
    anf_oracle: dict,
    anf_package: dict,
    mutation: str,
):
    plan = copy.deepcopy(anf_oracle["projection_plan"])
    dispositions = plan["legacy_parity"]["row_dispositions"]
    legacy = next(item for item in dispositions if item["disposition_kind"] == "legacy_only")
    source = next(item for item in dispositions if item["disposition_kind"] == "source_native_only")
    if mutation == "duplicate-legacy-only":
        dispositions.append(copy.deepcopy(legacy))
    elif mutation == "duplicate-source-only":
        dispositions.append(copy.deepcopy(source))
    elif mutation == "omit-legacy-only":
        dispositions.remove(legacy)
    elif mutation == "omit-source-only":
        dispositions.remove(source)
    elif mutation == "wrong-reason":
        legacy["reason_code"] = "legacy_terminal_summary_replaced_by_typed_history"
    elif mutation == "wrong-policy":
        legacy["policy_id"] = "policy:promise-progress:legacy-terminal-summary@1"
    elif mutation == "wrong-business-key":
        legacy["legacy_business_key"] = "legacy-business-key:anf:annual:wrong@1"
    elif mutation == "wrong-row-type":
        legacy["row_type"] = "legacy-row-type:timeline-terminal-summary@1"
    elif mutation == "wrong-block":
        legacy["block_id"] = TIMELINE_BLOCK_ID
    else:
        legacy["expected_legacy_field_inventory_digest"] = "0" * 64
    plan["legacy_parity_row_disposition_graph_sha256"] = _stable_digest(
        sorted(dispositions, key=lambda item: item["disposition_id"])
    )
    with pytest.raises(PromiseProgressProjectionError):
        build_promise_progress_product(anf_package, plan)


def test_valid_counterpart_cannot_remain_two_one_sided_rows_even_after_digest_repin(
    anf_oracle: dict,
    anf_package: dict,
):
    plan = copy.deepcopy(anf_oracle["projection_plan"])
    legacy = _legacy_row(plan, 16)
    source_scope = next(
        row
        for row in plan["legacy_parity"]["source_scope_manifest"]["row_scope"]
        if row["block_id"] == ANNUAL_BLOCK_ID and row["visible_sheet_row"] == 16
    )
    legacy["row_type"] = source_scope["row_type"]
    legacy["business_key_class"] = source_scope["business_key_class"]
    legacy["reviewed_semantic_identity"] = copy.deepcopy(
        source_scope["counterpart_signature"]["reviewed_semantic_identity"]
    )
    legacy["counterpart_signature"] = copy.deepcopy(source_scope["counterpart_signature"])
    _recompute_capture_manifest(plan, update_independent=True)

    disposition = _disposition(plan, legacy_row_id=legacy["legacy_row_id"])
    disposition["row_type"] = source_scope["row_type"]
    disposition["business_key_class"] = source_scope["business_key_class"]
    disposition["counterpart_search_result"] = [source_scope["source_native_row_id"]]
    disposition["expected_legacy_field_inventory_digest"] = next(
        item["field_inventory_digest"]
        for item in plan["legacy_parity"]["capture_manifest"]["row_field_inventories"]
        if item["legacy_row_id"] == legacy["legacy_row_id"]
    )
    policy = _policy_by_id(plan, disposition["policy_id"])
    if disposition["row_type"] not in policy["allowed_row_types"]:
        policy["allowed_row_types"].append(disposition["row_type"])
    if disposition["business_key_class"] not in policy["allowed_business_key_classes"]:
        policy["allowed_business_key_classes"].append(disposition["business_key_class"])
    _recompute_all_row_authorizations(plan)
    with pytest.raises(PromiseProgressProjectionError, match="reason differs|counterpart"):
        build_promise_progress_product(anf_package, plan)


def test_paired_row_cannot_be_split_into_one_sided_rows_or_drop_field_bindings(
    anf_oracle: dict,
    anf_package: dict,
):
    plan = copy.deepcopy(anf_oracle["projection_plan"])
    paired = next(
        item
        for item in plan["legacy_parity"]["row_dispositions"]
        if item["disposition_kind"] == "paired"
        and item["legacy_business_key"] == "legacy-business-key:anf:annual:row-13@1"
    )
    source_id = paired["source_native_row_id"]
    split_source = copy.deepcopy(paired)
    split_source["disposition_id"] = "row-disposition:promise-progress:source-annual-13:source-native-only@1"
    split_source["disposition_kind"] = "source_native_only"
    split_source["legacy_row_id"] = None
    split_source["legacy_business_key"] = None
    split_source["semantic_counterpart_class"] = None
    split_source["mapping_kind"] = "none"
    split_source["reviewed_mapping_reason"] = None
    split_source["reason_code"] = "source_native_canonical_row_legacy_omitted"
    split_source["expected_legacy_field_inventory_digest"] = None
    split_source["policy_id"] = "policy:promise-progress:source-canonical-row-legacy-omitted@1"
    split_source["counterpart_search_result"] = [paired["legacy_row_id"]]
    paired["disposition_kind"] = "legacy_only"
    paired["source_native_row_id"] = None
    paired["source_native_business_key"] = None
    paired["semantic_counterpart_class"] = None
    paired["mapping_kind"] = "none"
    paired["reviewed_mapping_reason"] = None
    paired["reason_code"] = "legacy_unsupported_by_accepted_product"
    paired["expected_source_native_field_inventory_digest"] = None
    paired["policy_id"] = "policy:promise-progress:legacy-unsupported-row@1"
    paired["counterpart_search_result"] = [source_id]
    plan["legacy_parity"]["row_dispositions"].append(split_source)
    plan["legacy_parity_accepted_differences"] = [
        binding
        for binding in plan["legacy_parity_accepted_differences"]
        if binding["source_native_row_id"] != source_id
    ]
    plan["legacy_parity_row_disposition_graph_sha256"] = _stable_digest(
        sorted(
            plan["legacy_parity"]["row_dispositions"],
            key=lambda item: item["disposition_id"],
        )
    )
    with pytest.raises(PromiseProgressProjectionError, match="one semantic counterpart|reason differs|exactly one"):
        build_promise_progress_product(anf_package, plan)


def test_structural_observations_and_policy_scopes_are_exact(
    anf_oracle: dict,
    anf_product: PromiseProgressProduct,
):
    plan = anf_oracle["projection_plan"]
    observations = plan["legacy_parity"]["structural_observations"]
    bindings = plan["legacy_parity"]["structural_bindings"]
    assert len(observations) == len(bindings) == 3
    assert {item["condition_type"] for item in observations} == {
        "empty-tracker-parallel-ownership",
        "fuzzy-hidden-trace-identity",
        "lossy-support-matrix-ownership",
    }
    assert all(item["comparison_digest"] != "0" * 64 for item in observations)
    assert all(item["structural_authorization_digest"] != "0" * 64 for item in bindings)
    assert anf_product.parity_report()["structural_counts"]["overbroad_structural_policy_scopes"] == 0


@pytest.mark.parametrize(
    "mutation",
    [
        "unrelated-product",
        "unused-block",
        "unused-condition",
        "unused-destination",
        "row-policy-used-structurally",
        "remove-binding",
        "duplicate-binding",
        "mutate-reason",
        "mutate-observation",
        "active-zero-use",
        "inactive-current-use",
    ],
)
def test_structural_policy_and_binding_mutations_fail_closed(
    anf_oracle: dict,
    anf_package: dict,
    mutation: str,
):
    plan = copy.deepcopy(anf_oracle["projection_plan"])
    bindings = plan["legacy_parity"]["structural_bindings"]
    binding = next(item for item in bindings if item["condition_type"] == "fuzzy-hidden-trace-identity")
    policy = _policy_by_id(plan, binding["policy_id"])
    if mutation == "unrelated-product":
        policy["allowed_product_ids"].append("promise-progress-product:UNRELATED:2099-01-01@1")
        _recompute_structural_authorization(plan, binding)
    elif mutation == "unused-block":
        policy["allowed_block_ids"].append(ANNUAL_BLOCK_ID)
        _recompute_structural_authorization(plan, binding)
    elif mutation == "unused-condition":
        policy["allowed_structural_condition_types"].append("empty-tracker-parallel-ownership")
        _recompute_structural_authorization(plan, binding)
    elif mutation == "unused-destination":
        policy["allowed_destination_pairs"].append(
            {"legacy_destination_cell": "A61", "source_native_destination_cell": "A82"}
        )
    elif mutation == "row-policy-used-structurally":
        target = _policy_by_id(plan, "policy:promise-progress:row-pair-reviewed@1")
        binding["policy_id"] = target["policy_id"]
        binding["exception_id"] = target["exception_id"]
        _recompute_structural_authorization(plan, binding)
    elif mutation == "remove-binding":
        bindings.remove(binding)
        plan["legacy_parity_structural_binding_set_sha256"] = _stable_digest(
            sorted(bindings, key=lambda item: item["structural_binding_id"])
        )
    elif mutation == "duplicate-binding":
        bindings.append(copy.deepcopy(binding))
        plan["legacy_parity_structural_binding_set_sha256"] = _stable_digest(
            sorted(bindings, key=lambda item: item["structural_binding_id"])
        )
    elif mutation == "mutate-reason":
        binding["difference_reason_code"] = "legacy_parallel_tracker_ownership_removed"
        _recompute_structural_authorization(plan, binding)
    elif mutation == "mutate-observation":
        observation = next(
            item
            for item in plan["legacy_parity"]["structural_observations"]
            if item["structural_observation_id"] == binding["structural_observation_id"]
        )
        observation["observed_legacy_state"] += " mutated"
        _recompute_structural_authorization(plan, binding, recompute_observation=True)
    elif mutation == "active-zero-use":
        extra = copy.deepcopy(policy)
        extra["policy_id"] = "policy:promise-progress:fuzzy-trace-key-unused@1"
        plan["legacy_parity"]["exception_policies"].append(extra)
    else:
        policy["state"] = "inactive"
        _recompute_structural_authorization(plan, binding)
    with pytest.raises(PromiseProgressProjectionError):
        build_promise_progress_product(anf_package, plan)


def test_policy_types_cannot_cross_authorization_layers(
    anf_oracle: dict,
    anf_package: dict,
):
    field_via_structural = copy.deepcopy(anf_oracle["projection_plan"])
    field_binding = _binding(field_via_structural, legacy_destination="A13")
    structural_policy = _policy_by_id(
        field_via_structural, "policy:promise-progress:lossy-support-matrix@1"
    )
    field_binding["exception_id"] = structural_policy["exception_id"]
    field_binding["exception_policy_version"] = structural_policy["exception_policy_version"]
    _recompute_parity_authorization(
        field_via_structural, field_binding, policy=structural_policy
    )
    with pytest.raises(PromiseProgressProjectionError):
        build_promise_progress_product(anf_package, field_via_structural)

    row_via_structural = copy.deepcopy(anf_oracle["projection_plan"])
    row_disposition = next(
        item
        for item in row_via_structural["legacy_parity"]["row_dispositions"]
        if item["disposition_kind"] == "legacy_only"
    )
    row_disposition["policy_id"] = "policy:promise-progress:lossy-support-matrix@1"
    _recompute_all_row_authorizations(row_via_structural)
    with pytest.raises(PromiseProgressProjectionError):
        build_promise_progress_product(anf_package, row_via_structural)
