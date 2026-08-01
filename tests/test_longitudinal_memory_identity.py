from __future__ import annotations

import base64
import hashlib

import pytest

import pbi_xbrl.longitudinal_memory.identity as identity_module
from pbi_xbrl.longitudinal_memory.identity import (
    IdentityError,
    assert_identity_digest_pairs,
    build_identity,
    dimension_set_identity,
    identity_digest,
    model_interpretation_identity,
    numerical_fact_identity,
    sorted_reference_digest,
    source_document_identity,
)


def test_readable_identity_has_fixed_order_rfc3986_encoding_and_nfc():
    identity = build_identity("statement", (("co", "TEST"), ("topic", "Cafe\u0301 / margin")))
    assert identity == "statement:v1|co=TEST|topic=Caf%C3%A9%20%2F%20margin"
    assert build_identity("statement", (("co", "TEST"), ("topic", "Café / margin"))) == identity


def test_digest_is_first_20_sha256_bytes_lowercase_base32():
    readable = "doc:v1|co=TEST|publisher=test|type=release|pub=2026-03-04|key=q4|rev=1"
    expected = base64.b32encode(hashlib.sha256(readable.encode("utf-8")).digest()[:20]).decode().lower().rstrip("=")
    assert identity_digest(readable) == f"sha256-160:{expected}"


def test_source_document_identity_is_fixed_and_mutable_fields_are_absent():
    readable = source_document_identity(
        company_id="test",
        publisher_id="company",
        document_type="earnings-release",
        publication_date="2026-03-04",
        document_key="q4-release",
        revision=2,
    )
    assert readable == "doc:v1|co=TEST|publisher=company|type=earnings-release|pub=2026-03-04|key=q4-release|rev=2"
    assert all(token not in readable for token in ("title", "review", "confidence", "wording", "value"))


def test_fact_identity_uses_business_semantics_and_immutable_provenance_not_value():
    business = {
        "company_id": "TEST",
        "metric_id": "metric:core:comparable-sales@1",
        "definition_id": "definition:core:reported@1",
        "basis_id": "basis:core:reported@1",
        "period_id": "period:test:fy2025-q4@1",
        "dimension_set_id": "dimset:v1|members=x",
        "unit_id": "unit:core:percent@1",
        "currency": None,
    }
    first = numerical_fact_identity(provenance_key="occ:v1|key=one", **business)
    second = numerical_fact_identity(provenance_key="occ:v1|key=one", **business)
    assert first == second
    assert "value" not in first


def test_dimension_set_is_sorted_unique_and_total_cannot_be_empty():
    members = [
        ("dimension:core:geography@1", "member:core:geography:geography:emea@1"),
        ("dimension:core:company@1", "member:core:company:company:total-company@1"),
    ]
    assert dimension_set_identity(members) == dimension_set_identity(reversed(members))
    with pytest.raises(IdentityError):
        dimension_set_identity([])
    with pytest.raises(IdentityError):
        dimension_set_identity([members[0], (members[0][0], "member:core:geography:geography:apac@1")])


def test_sorted_input_digest_and_interpretation_identity_are_order_invariant():
    assert sorted_reference_digest(["b", "a", "a"]) == sorted_reference_digest(["a", "b"])
    common = {
        "company_id": "TEST",
        "interpretation_key": "baseline-vs-guide",
        "as_of_period_id": "period:test:fy2025-q4@1",
        "method_id": "method:core:reviewed@1",
        "producer_id": "reviewer",
        "revision": 1,
    }
    assert model_interpretation_identity(input_record_ids=["b", "a"], **common) == model_interpretation_identity(input_record_ids=["a", "b"], **common)


def test_wrong_digest_and_digest_collision_are_p1_identity_failures(monkeypatch):
    with pytest.raises(IdentityError, match="does not match"):
        assert_identity_digest_pairs([("doc:v1|key=a", "sha256-160:not-the-digest")])

    monkeypatch.setattr(identity_module, "identity_digest", lambda _: "sha256-160:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    with pytest.raises(IdentityError, match="collision"):
        identity_module.assert_identity_digest_pairs(
            [
                ("doc:v1|key=a", "sha256-160:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                ("doc:v1|key=b", "sha256-160:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            ]
        )


@pytest.mark.parametrize("value", ["Metric:core:sales@1", "metric:Core:sales@1", "metric:core:sales", "metric_core_sales@1"])
def test_noncanonical_semantic_ids_fail(value):
    with pytest.raises(IdentityError):
        identity_module.validate_semantic_id(value, prefix="metric")
