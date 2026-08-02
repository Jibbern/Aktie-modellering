from __future__ import annotations

import hashlib
import random
from copy import deepcopy
from decimal import Decimal
from pathlib import Path

import pytest

from pbi_xbrl.json_schema_validation import load_json_strict
from pbi_xbrl.longitudinal_memory.calendar_rules import (
    CALENDAR_YEAR_RULE_ID,
    SOURCE_LABELLED_52_53_WEEK_RULE_ID,
)
from pbi_xbrl.longitudinal_memory.changes import derive_percentage_point_change
from pbi_xbrl.longitudinal_memory.identity import (
    availability_observation_identity,
    build_identity,
    change_observation_identity,
    company_event_identity,
    dimension_set_identity,
    evidence_occurrence_identity,
    guidance_series_identity,
    guidance_version_identity,
    identity_digest,
    management_statement_identity,
    model_interpretation_identity,
    numerical_business_key,
    numerical_fact_identity,
    promise_identity,
    promise_version_identity,
    relation_identity,
    source_document_identity,
)
from pbi_xbrl.longitudinal_memory.reconciliation import resolve_observations, values_compatible
from pbi_xbrl.longitudinal_memory.serialization import (
    runtime_sidecar_filename,
    semantic_snapshot_identity,
    serialize_package,
)
from pbi_xbrl.longitudinal_memory.validation import (
    validate_package,
    validate_package_schema,
)


FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "longitudinal_memory"
INPUT_PATH = FIXTURE_DIR / "anf_first_pass_input.v1.json"
EXPECTED_PATH = FIXTURE_DIR / "anf_first_pass_expected.v1.json"

METRIC = {
    "comparable-sales": "metric:core:comparable-sales@1",
    "store-openings": "metric:core:store-openings@1",
    "store-closures": "metric:core:store-closures@1",
    "ending-stores": "metric:core:ending-stores@1",
    "revenue-growth": "metric:core:revenue-growth@1",
    "operating-margin": "metric:core:operating-margin@1",
}
DEFINITION_REPORTED = "definition:core:company-reported@1"
DEFINITION_GUIDANCE = "definition:core:company-guidance@1"
BASIS_REPORTED = "basis:core:reported@1"
BASIS_GUIDED = "basis:core:guided@1"
UNIT_PERCENT = "unit:core:percent@1"
UNIT_PP = "unit:core:percentage-point@1"
UNIT_COUNT = "unit:core:count@1"
CALENDAR_ID = "calendar:anf:fiscal@1"
COMPANY_DIM = "dimension:core:company@1"
GEOGRAPHY_DIM = "dimension:core:geography@1"
BRAND_DIM = "dimension:core:brand@1"
TOTAL_MEMBER = "member:core:company:company:total-company@1"
APAC_MEMBER = "member:core:geography:geography:apac@1"
EMEA_MEMBER = "member:core:geography:geography:emea@1"
ABERCROMBIE_MEMBER = "member:core:brand:brand:abercrombie@1"
HOLLISTER_MEMBER = "member:core:brand:brand:hollister@1"


def _value(raw):
    if raw is None:
        return None
    value = dict(raw)
    if value["kind"] == "range":
        value.setdefault("low_inclusive", True)
        value.setdefault("high_inclusive", True)
    return value


def _catalog(dimension_sets):
    def common(identity_key, identity, name, description):
        return {identity_key: identity, "display_name": name, "description": description, "aliases": [], "status": "active", "supersedes_id": None}

    metrics = [common("metric_id", metric_id, slug.replace("-", " ").title(), f"Versioned {slug} metric.") for slug, metric_id in METRIC.items()]
    definitions = [
        {**common("definition_id", DEFINITION_REPORTED, "Company reported", "Company-reported definition."), "gaap_status": "operational"},
        {**common("definition_id", DEFINITION_GUIDANCE, "Company guidance", "Company guidance definition."), "gaap_status": "operational"},
    ]
    bases = [
        {**common("basis_id", BASIS_REPORTED, "Reported", "Reported realized basis."), "realization_state": "reported"},
        {**common("basis_id", BASIS_GUIDED, "Guided", "Forward guided basis."), "realization_state": "guided"},
    ]
    units = [
        {"unit_id": UNIT_PERCENT, "display_name": "Percent", "unit_kind": "percent", "scale": "1", "currency_behavior": "forbidden", "aliases": ["%"], "status": "active", "supersedes_id": None},
        {"unit_id": UNIT_PP, "display_name": "Percentage point", "unit_kind": "percentage-point", "scale": "1", "currency_behavior": "forbidden", "aliases": ["pp"], "status": "active", "supersedes_id": None},
        {"unit_id": UNIT_COUNT, "display_name": "Count", "unit_kind": "count", "scale": "1", "currency_behavior": "forbidden", "aliases": [], "status": "active", "supersedes_id": None},
    ]
    dimensions = [
        common("dimension_id", COMPANY_DIM, "Company", "Company scope axis."),
        common("dimension_id", GEOGRAPHY_DIM, "Geography", "Geography scope axis."),
        common("dimension_id", BRAND_DIM, "Brand", "Brand scope axis."),
    ]
    members = [
        {"member_id": TOTAL_MEMBER, "dimension_id": COMPANY_DIM, "scope": "company", "display_name": "Total Company", "aliases": [], "status": "active", "supersedes_id": None},
        {"member_id": APAC_MEMBER, "dimension_id": GEOGRAPHY_DIM, "scope": "geography", "display_name": "APAC", "aliases": [], "status": "active", "supersedes_id": None},
        {"member_id": EMEA_MEMBER, "dimension_id": GEOGRAPHY_DIM, "scope": "geography", "display_name": "EMEA", "aliases": [], "status": "active", "supersedes_id": None},
        {"member_id": ABERCROMBIE_MEMBER, "dimension_id": BRAND_DIM, "scope": "brand", "display_name": "Abercrombie", "aliases": [], "status": "active", "supersedes_id": None},
        {"member_id": HOLLISTER_MEMBER, "dimension_id": BRAND_DIM, "scope": "brand", "display_name": "Hollister", "aliases": [], "status": "active", "supersedes_id": None},
    ]
    policies = [
        {"policy_id": f"policy:core:{name}@1", "assertion_type": name, "description": f"Assertion-specific {name} precedence."}
        for name in ("reported-numerical", "guidance", "management-explanation", "company-event", "model-interpretation")
    ]
    rules = [
        {"rule_id": "rule:core:qoq-percentage-point@1", "change_kind": "qoq-percentage-point", "input_unit_kind": "percent", "output_unit_id": UNIT_PP, "description": "Adjacent-quarter percentage-point change."},
        {"rule_id": "rule:core:yoy-percentage-point@1", "change_kind": "yoy-percentage-point", "input_unit_kind": "percent", "output_unit_id": UNIT_PP, "description": "Same-quarter year-over-year percentage-point change."},
    ]
    methods = [{"method_id": "method:core:reviewed-investment-case@1", "producer_id": "accepted-normalized-package", "description": "Reviewed investment-case interpretation."}]
    return {
        "metrics": metrics,
        "definitions": definitions,
        "bases": bases,
        "units": units,
        "dimensions": dimensions,
        "dimension_members": members,
        "dimension_sets": [
            {"dimension_set_id": identity, "members": [{"dimension_id": dim, "member_id": member} for dim, member in pairs]}
            for identity, pairs in dimension_sets.values()
        ],
        "policies": policies,
        "change_rules": rules,
        "methods": methods,
    }


def _header(record_id, record_type, company_id, subject_id, publication_date, period_id, period_type, dimension_set_id, assertion_mode, evidence_ids, *, fiscal_period_id=None, review_state="accepted"):
    return {
        "record_id": record_id,
        "identity_digest": identity_digest(record_id),
        "record_type": record_type,
        "schema_version": "1.0.0",
        "company_id": company_id,
        "subject_id": subject_id,
        "publication_date": publication_date,
        "knowledge_date": publication_date or "2026-03-04",
        "effective_period_id": period_id,
        "fiscal_period_id": fiscal_period_id,
        "period_type": period_type,
        "dimension_set_id": dimension_set_id,
        "assertion_mode": assertion_mode,
        "evidence_occurrence_ids": sorted(evidence_ids),
        "review_state": review_state,
        "confidence": None,
    }


def _make_relation(relation_type, source_id, target_id, rule_id, evidence_ids=()):
    relation_id = relation_identity(relation_type=relation_type, from_record_id=source_id, to_record_id=target_id, rule_id=rule_id)
    return {
        "relation_id": relation_id,
        "identity_digest": identity_digest(relation_id),
        "schema_version": "1.0.0",
        "relation_type": relation_type,
        "from_record_id": source_id,
        "to_record_id": target_id,
        "rule_id": rule_id,
        "evidence_occurrence_ids": sorted(evidence_ids),
    }


def _materialize(raw):
    company_id = raw["company_id"]
    document_by_key = {}
    source_documents = []
    for source in raw["sources"]:
        document_id = source_document_identity(
            company_id=company_id,
            publisher_id="abercrombie-fitch",
            document_type=source["document_type"],
            publication_date=source["publication_date"],
            document_key=source["key"],
        )
        row = {
            "source_document_id": document_id,
            "identity_digest": identity_digest(document_id),
            "schema_version": "1.0.0",
            "company_id": company_id,
            "publisher_id": "abercrombie-fitch",
            "document_type": source["document_type"],
            "publication_date": source["publication_date"],
            "document_key": source["key"],
            "revision": 1,
            "origin_document_id": None,
            "title": source["title"],
            "source_path_hint": source["path"] if source["document_type"] == "normalized-package" else f"{raw['source_root']}/{source['path']}",
            "canonical_url": None,
            "content_sha256": None,
            "authority_class": source["authority_class"],
            "review_state": "accepted",
        }
        document_by_key[source["key"]] = row
        source_documents.append(row)

    occurrence_by_key = {}
    evidence_occurrences = []
    for evidence in raw["evidence"]:
        source = document_by_key[evidence["source"]]
        occurrence_id = evidence_occurrence_identity(
            company_id=company_id,
            document_key=source["document_key"],
            document_revision=source["revision"],
            locator_kind=evidence["locator_kind"],
            locator_key=evidence["locator_key"],
        )
        row = {
            "evidence_occurrence_id": occurrence_id,
            "identity_digest": identity_digest(occurrence_id),
            "schema_version": "1.0.0",
            "company_id": company_id,
            "source_document_id": source["source_document_id"],
            "occurrence_key": evidence["key"],
            "locator_kind": evidence["locator_kind"],
            "locator_key": evidence["locator_key"],
            "ordinal": 1,
            "excerpt": evidence["excerpt"],
            "review_state": "accepted",
        }
        occurrence_by_key[evidence["key"]] = row
        evidence_occurrences.append(row)

    period_by_key = {}
    periods = []
    period_evidence = {
        "fy2024-q4": "comp-total-fy2024-q4", "fy2025-q3": "comp-total-fy2025-q3", "fy2025-q4": "comp-total-fy2025-q4",
        "fy2025": "store-count-ending", "fy2026": "fy2026-release-guidance", "2026-mar": "erp-go-live-event",
    }
    for raw_period in raw["periods"]:
        period_id = f"period:anf:{raw_period['key']}@1"
        evidence_id = occurrence_by_key[period_evidence[raw_period["key"]]]["evidence_occurrence_id"]
        row = {
            "period_id": period_id,
            "calendar_id": CALENDAR_ID,
            "company_id": company_id,
            **{key: raw_period[key] for key in ("fiscal_year", "fiscal_quarter", "period_type", "start_date", "end_date", "day_count", "week_count", "fiscal_ordinal", "is_53_week_year")},
            "evidence_occurrence_ids": [evidence_id],
            "reconciliation_state": "reconciled",
        }
        period_by_key[raw_period["key"]] = row
        periods.append(row)

    dimensions = {
        "total-company": [(COMPANY_DIM, TOTAL_MEMBER)],
        "apac": [(COMPANY_DIM, TOTAL_MEMBER), (GEOGRAPHY_DIM, APAC_MEMBER)],
        "emea": [(COMPANY_DIM, TOTAL_MEMBER), (GEOGRAPHY_DIM, EMEA_MEMBER)],
        "abercrombie": [(BRAND_DIM, ABERCROMBIE_MEMBER), (COMPANY_DIM, TOTAL_MEMBER)],
        "hollister": [(BRAND_DIM, HOLLISTER_MEMBER), (COMPANY_DIM, TOTAL_MEMBER)],
    }
    dimension_sets = {
        key: (dimension_set_identity(pairs), sorted(pairs)) for key, pairs in dimensions.items()
    }
    total_dimension_id = dimension_sets["total-company"][0]

    observations = []
    facts_by_evidence = {}
    for fact in raw["comparable_sales"]:
        occurrence = occurrence_by_key[fact["evidence"]]
        period = period_by_key[fact["period"]]
        dimset_id = dimension_sets[fact["scope"]][0]
        business = numerical_business_key(
            company_id=company_id,
            metric_id=METRIC["comparable-sales"],
            definition_id=DEFINITION_REPORTED,
            basis_id=BASIS_REPORTED,
            period_id=period["period_id"],
            dimension_set_id=dimset_id,
            unit_id=UNIT_PERCENT,
            currency=None,
        )
        record_id = numerical_fact_identity(
            provenance_key=occurrence["evidence_occurrence_id"],
            company_id=company_id,
            metric_id=METRIC["comparable-sales"],
            definition_id=DEFINITION_REPORTED,
            basis_id=BASIS_REPORTED,
            period_id=period["period_id"],
            dimension_set_id=dimset_id,
            unit_id=UNIT_PERCENT,
            currency=None,
        )
        record = {
            "header": _header(record_id, "NumericalFact", company_id, METRIC["comparable-sales"], document_by_key[next(row["source"] for row in raw["evidence"] if row["key"] == fact["evidence"])]["publication_date"], period["period_id"], "quarter", dimset_id, "reported", [occurrence["evidence_occurrence_id"]], fiscal_period_id=period["period_id"]),
            "payload": {"kind": "NumericalFact", "business_key": business, "metric_id": METRIC["comparable-sales"], "definition_id": DEFINITION_REPORTED, "basis_id": BASIS_REPORTED, "unit_id": UNIT_PERCENT, "currency": None, "value": {"kind": "exact", "value": fact["value"]}},
        }
        observations.append(record)
        facts_by_evidence[fact["evidence"]] = record

    store_facts = {}
    annual_period = period_by_key["fy2025"]
    for fact in raw["store_count"]:
        occurrence = occurrence_by_key[fact["evidence"]]
        business = numerical_business_key(company_id=company_id, metric_id=METRIC[fact["metric"]], definition_id=DEFINITION_REPORTED, basis_id=BASIS_REPORTED, period_id=annual_period["period_id"], dimension_set_id=total_dimension_id, unit_id=UNIT_COUNT, currency=None)
        record_id = numerical_fact_identity(provenance_key=occurrence["evidence_occurrence_id"], company_id=company_id, metric_id=METRIC[fact["metric"]], definition_id=DEFINITION_REPORTED, basis_id=BASIS_REPORTED, period_id=annual_period["period_id"], dimension_set_id=total_dimension_id, unit_id=UNIT_COUNT, currency=None)
        record = {"header": _header(record_id, "NumericalFact", company_id, METRIC[fact["metric"]], "2026-03-04", annual_period["period_id"], "annual", total_dimension_id, "reported", [occurrence["evidence_occurrence_id"]], fiscal_period_id=annual_period["period_id"]), "payload": {"kind": "NumericalFact", "business_key": business, "metric_id": METRIC[fact["metric"]], "definition_id": DEFINITION_REPORTED, "basis_id": BASIS_REPORTED, "unit_id": UNIT_COUNT, "currency": None, "value": {"kind": "exact", "value": fact["value"]}}}
        observations.append(record)
        store_facts[fact["metric"]] = record

    guidance_entities = {}
    entities = []
    for year_key, period_key in (("fy2025", "fy2025"), ("fy2026", "fy2026")):
        for metric_key in ("revenue-growth", "operating-margin"):
            series_id = guidance_series_identity(company_id=company_id, metric_id=METRIC[metric_key], definition_id=DEFINITION_GUIDANCE, basis_id=BASIS_GUIDED, horizon_period_id=period_by_key[period_key]["period_id"], dimension_set_id=total_dimension_id, unit_id=UNIT_PERCENT, currency=None)
            guidance_entities[(year_key, metric_key)] = series_id
            entities.append({"header": {"entity_id": series_id, "identity_digest": identity_digest(series_id), "entity_type": "GuidanceSeries", "schema_version": "1.0.0", "company_id": company_id, "evidence_occurrence_ids": []}, "payload": {"kind": "GuidanceSeries", "metric_id": METRIC[metric_key], "definition_id": DEFINITION_GUIDANCE, "basis_id": BASIS_GUIDED, "horizon_period_id": period_by_key[period_key]["period_id"], "dimension_set_id": total_dimension_id, "unit_id": UNIT_PERCENT, "currency": None}})

    guidance_records = {key: [] for key in guidance_entities}
    for index, version in enumerate(raw["fy2025_guidance"]):
        occurrence = occurrence_by_key[version["evidence"]]
        for metric_key, raw_key in (("revenue-growth", "revenue"), ("operating-margin", "margin")):
            series_id = guidance_entities[("fy2025", metric_key)]
            record_id = guidance_version_identity(guidance_series_id=series_id, occurrence_id=occurrence["evidence_occurrence_id"])
            record = {"header": _header(record_id, "GuidanceVersion", company_id, series_id, version["date"], period_by_key["fy2025"]["period_id"], "annual", total_dimension_id, "guided", [occurrence["evidence_occurrence_id"]], fiscal_period_id=period_by_key["fy2025"]["period_id"]), "payload": {"kind": "GuidanceVersion", "guidance_series_id": series_id, "version_kind": "origin" if index == 0 else "replacement", "value": _value(version[raw_key]), "wording": occurrence["excerpt"]}}
            observations.append(record); guidance_records[("fy2025", metric_key)].append(record)

    for version in raw["fy2026_guidance"]:
        occurrence = occurrence_by_key[version["evidence"]]
        for metric_key, raw_key in (("revenue-growth", "revenue"), ("operating-margin", "margin")):
            series_id = guidance_entities[("fy2026", metric_key)]
            record_id = guidance_version_identity(guidance_series_id=series_id, occurrence_id=occurrence["evidence_occurrence_id"])
            record = {"header": _header(record_id, "GuidanceVersion", company_id, series_id, "2026-03-04", period_by_key["fy2026"]["period_id"], "annual", total_dimension_id, "guided", [occurrence["evidence_occurrence_id"]], fiscal_period_id=period_by_key["fy2026"]["period_id"]), "payload": {"kind": "GuidanceVersion", "guidance_series_id": series_id, "version_kind": "origin" if version["source"] == "release" else "reaffirmation", "value": _value(version[raw_key]), "wording": occurrence["excerpt"]}}
            observations.append(record); guidance_records[("fy2026", metric_key)].append(record)

    promise_raw = raw["promise"]
    origin_occurrence = occurrence_by_key[promise_raw["origin_evidence"]]
    promise_id = promise_identity(company_id=company_id, subject_id=promise_raw["subject"], program_id=promise_raw["program"], origin_occurrence_id=origin_occurrence["evidence_occurrence_id"])
    origin_version_id = promise_version_identity(promise_id=promise_id, occurrence_id=origin_occurrence["evidence_occurrence_id"])
    reaffirm_occurrence = occurrence_by_key[promise_raw["reaffirmation_evidence"]]
    reaffirm_version_id = promise_version_identity(promise_id=promise_id, occurrence_id=reaffirm_occurrence["evidence_occurrence_id"])
    entities.append({"header": {"entity_id": promise_id, "identity_digest": identity_digest(promise_id), "entity_type": "Promise", "schema_version": "1.0.0", "company_id": company_id, "evidence_occurrence_ids": [origin_occurrence["evidence_occurrence_id"]]}, "payload": {"kind": "Promise", "promise_subject_id": promise_raw["subject"], "program_id": promise_raw["program"], "origin_occurrence_id": origin_occurrence["evidence_occurrence_id"], "origin_version_id": origin_version_id, "original_wording": promise_raw["wording"], "original_target": _value(promise_raw["target"]), "original_baseline": None, "original_deadline": promise_raw["deadline"]}})
    promise_versions = [
        {"header": _header(origin_version_id, "PromiseVersion", company_id, promise_id, "2025-03-06", period_by_key["fy2025"]["period_id"], "annual", total_dimension_id, "stated", [origin_occurrence["evidence_occurrence_id"]], fiscal_period_id=period_by_key["fy2025"]["period_id"]), "payload": {"kind": "PromiseVersion", "promise_id": promise_id, "previous_version_id": None, "change_kind": "origin", "version_state": "active", "wording": promise_raw["wording"], "target": _value(promise_raw["target"]), "baseline": None, "deadline": promise_raw["deadline"]}},
        {"header": _header(reaffirm_version_id, "PromiseVersion", company_id, promise_id, "2026-01-12", period_by_key["fy2025"]["period_id"], "annual", total_dimension_id, "stated", [reaffirm_occurrence["evidence_occurrence_id"]], fiscal_period_id=period_by_key["fy2025"]["period_id"]), "payload": {"kind": "PromiseVersion", "promise_id": promise_id, "previous_version_id": origin_version_id, "change_kind": "reaffirmation", "version_state": "reaffirmed", "wording": promise_raw["wording"], "target": _value(promise_raw["target"]), "baseline": None, "deadline": promise_raw["deadline"]}},
    ]
    observations.extend(promise_versions)

    management_occ = occurrence_by_key[raw["management_explanation"]["evidence"]]
    statement_id = management_statement_identity(company_id=company_id, statement_kind="explanation", topic_id=raw["management_explanation"]["topic"], period_id=period_by_key["fy2025-q4"]["period_id"], speaker_id="chief-financial-officer", occurrence_id=management_occ["evidence_occurrence_id"])
    statement = {"header": _header(statement_id, "ManagementStatement", company_id, raw["management_explanation"]["topic"], "2026-03-04", period_by_key["fy2025-q4"]["period_id"], "quarter", total_dimension_id, "stated", [management_occ["evidence_occurrence_id"]], fiscal_period_id=period_by_key["fy2025-q4"]["period_id"]), "payload": {"kind": "ManagementStatement", "statement_kind": "explanation", "topic_id": raw["management_explanation"]["topic"], "statement_period_id": period_by_key["fy2025-q4"]["period_id"], "speaker_id": "chief-financial-officer", "statement": management_occ["excerpt"]}}
    observations.append(statement)

    event_raw = raw["company_event"]; event_occ = occurrence_by_key[event_raw["evidence"]]
    event_id = company_event_identity(company_id=company_id, event_type=event_raw["event_type"], event_subject_id=event_raw["subject"], event_stage=event_raw["stage"], effective_period_id=period_by_key["2026-mar"]["period_id"], occurrence_id=event_occ["evidence_occurrence_id"])
    event = {"header": _header(event_id, "CompanyEvent", company_id, event_raw["subject"], "2026-03-04", period_by_key["2026-mar"]["period_id"], "month", total_dimension_id, "stated", [event_occ["evidence_occurrence_id"]], fiscal_period_id=None), "payload": {"kind": "CompanyEvent", "event_type": event_raw["event_type"], "event_subject_id": event_raw["subject"], "event_stage": event_raw["stage"], "description": event_occ["excerpt"], "effective_date": None, "effective_month": event_raw["effective_month"], "effective_precision": "month"}}
    observations.append(event)

    documents = source_documents
    occurrences = evidence_occurrences
    total_q4_candidates = [facts_by_evidence[key] for key in ("comp-total-fy2025-q4", "comp-total-geography-repeat", "comp-total-brand-repeat")]
    total_resolution = resolve_observations(total_q4_candidates, policy_id="policy:core:reported-numerical@1", as_of_date=raw["knowledge_cutoff"], source_documents=documents, evidence_occurrences=occurrences)
    q3_resolution = resolve_observations([facts_by_evidence["comp-total-fy2025-q3"]], policy_id="policy:core:reported-numerical@1", as_of_date=raw["knowledge_cutoff"], source_documents=documents, evidence_occurrences=occurrences)
    prior_q4_resolution = resolve_observations([facts_by_evidence["comp-total-fy2024-q4"]], policy_id="policy:core:reported-numerical@1", as_of_date=raw["knowledge_cutoff"], source_documents=documents, evidence_occurrences=occurrences)
    q4_primary = next(record for record in total_q4_candidates if record["header"]["record_id"] == total_resolution.resolution["selected_record_id"])
    fy2026_margin_release = guidance_records[("fy2026", "operating-margin")][0]
    interpretation_raw = raw["interpretation"]; interpretation_occ = occurrence_by_key[interpretation_raw["evidence"]]
    interpretation_id = model_interpretation_identity(company_id=company_id, interpretation_key=interpretation_raw["key"], as_of_period_id=period_by_key["fy2025-q4"]["period_id"], method_id=interpretation_raw["method"], producer_id=interpretation_raw["producer"], input_record_ids=[q4_primary["header"]["record_id"], fy2026_margin_release["header"]["record_id"]], revision=interpretation_raw["revision"])
    interpretation = {"header": _header(interpretation_id, "ModelInterpretation", company_id, interpretation_raw["key"], "2026-03-04", period_by_key["fy2025-q4"]["period_id"], "quarter", total_dimension_id, "interpreted", [interpretation_occ["evidence_occurrence_id"]], fiscal_period_id=period_by_key["fy2025-q4"]["period_id"], review_state="reviewed"), "payload": {"kind": "ModelInterpretation", "interpretation_key": interpretation_raw["key"], "as_of_period_id": period_by_key["fy2025-q4"]["period_id"], "method_id": interpretation_raw["method"], "producer_id": interpretation_raw["producer"], "input_record_ids": sorted([q4_primary["header"]["record_id"], fy2026_margin_release["header"]["record_id"]]), "revision": interpretation_raw["revision"], "interpretation": interpretation_occ["excerpt"], "authority_class": "accepted-normalized"}}
    observations.append(interpretation)

    fiscal_calendar = {
        "calendar_id": CALENDAR_ID,
        "calendar_rule_id": SOURCE_LABELLED_52_53_WEEK_RULE_ID,
        "company_id": company_id,
        "profile_hint": "late-January/early-February 52/53-week year",
        "week_pattern": "source-declared",
        "coverage_state": "partial",
        "evidence_occurrence_ids": [
            occurrence_by_key["comp-total-fy2025-q4"]["evidence_occurrence_id"]
        ],
        "reconciliation_state": "reconciled",
    }
    qoq = derive_percentage_point_change(
        facts_by_evidence["comp-total-fy2025-q3"],
        q4_primary,
        earlier_period=period_by_key["fy2025-q3"],
        later_period=period_by_key["fy2025-q4"],
        earlier_calendar=fiscal_calendar,
        later_calendar=fiscal_calendar,
        change_kind="qoq-percentage-point",
        rule_id="rule:core:qoq-percentage-point@1",
        change_unit_id=UNIT_PP,
    )
    yoy = derive_percentage_point_change(
        facts_by_evidence["comp-total-fy2024-q4"],
        q4_primary,
        earlier_period=period_by_key["fy2024-q4"],
        later_period=period_by_key["fy2025-q4"],
        earlier_calendar=fiscal_calendar,
        later_calendar=fiscal_calendar,
        change_kind="yoy-percentage-point",
        rule_id="rule:core:yoy-percentage-point@1",
        change_unit_id=UNIT_PP,
    )
    observations.extend([qoq, yoy])

    relations = list(total_resolution.inferred_relations)
    resolutions = [total_resolution.resolution, q3_resolution.resolution, prior_q4_resolution.resolution]

    for key, records in guidance_records.items():
        explicit = []
        if key[0] == "fy2025":
            for older, newer in zip(records, records[1:]):
                explicit.append(_make_relation("supersedes", newer["header"]["record_id"], older["header"]["record_id"], "rule:core:guidance-explicit-replacement@1", newer["header"]["evidence_occurrence_ids"]))
        result = resolve_observations(records, policy_id="policy:core:guidance@1", as_of_date=raw["knowledge_cutoff"], source_documents=documents, evidence_occurrences=occurrences, relations=explicit)
        relations.extend(explicit); relations.extend(result.inferred_relations); resolutions.append(result.resolution)

    relations.append(_make_relation("reaffirms", reaffirm_version_id, origin_version_id, "rule:core:promise-reaffirmation@1", [reaffirm_occurrence["evidence_occurrence_id"]]))
    relations.append(_make_relation("evidences", store_facts["store-openings"]["header"]["record_id"], promise_id, "rule:core:promise-evidence@1", store_facts["store-openings"]["header"]["evidence_occurrence_ids"]))
    for record, policy in ((statement, "policy:core:management-explanation@1"), (event, "policy:core:company-event@1"), (interpretation, "policy:core:model-interpretation@1")):
        result = resolve_observations([record], policy_id=policy, as_of_date=raw["knowledge_cutoff"], source_documents=documents, evidence_occurrences=occurrences)
        resolutions.append(result.resolution)

    review_id = build_identity("review", (("rule", "promise-approximate-tolerance-missing"), ("promise", promise_id)))
    review_issues = [{"issue_id": review_id, "severity": "P2", "rule_id": "promise_approximate_tolerance_missing", "entity_ids": [promise_id], "business_key": promise_id, "message": "The approximate store target has no source-supplied tolerance and cannot be marked achieved automatically.", "evidence_occurrence_ids": [origin_occurrence["evidence_occurrence_id"]], "candidate_record_ids": [origin_version_id, store_facts["store-openings"]["header"]["record_id"], store_facts["store-closures"]["header"]["record_id"]], "suggested_action": "Obtain an explicit tolerance or perform reviewed promise assessment.", "promotion_blocking": False, "review_state": "needs_review"}]

    package = {
        "schema_id": "longitudinal-company-memory", "schema_version": "1.0.0", "identity_contract_version": "1", "artifact_state": "accepted",
        "company_id": company_id, "knowledge_cutoff": raw["knowledge_cutoff"],
        "normalized_package_ref": {"semantic_snapshot_id": semantic_snapshot_identity(raw["normalized_snapshot"]), "source_package_schema_version": raw["normalized_snapshot"]["schema_version"], "source_package_company_id": company_id, "source_package_ref": "docs/anf_normalized_text_quality_audit.json#text_excerpt@253"},
        "catalog": _catalog(dimension_sets),
        "fiscal_calendars": [fiscal_calendar],
        "periods": periods, "source_documents": source_documents, "evidence_occurrences": evidence_occurrences, "entities": entities,
        "observations": observations, "relations": relations, "resolutions": resolutions, "review_issues": review_issues,
    }
    return package


def _projection(package):
    observations = package["observations"]
    by_id = {row["header"]["record_id"]: row for row in observations}
    changes = {row["payload"].get("change_kind"): row for row in observations if row["payload"]["kind"] == "ChangeObservation"}
    apac = next(row for row in observations if row["payload"]["kind"] == "NumericalFact" and row["payload"]["metric_id"] == METRIC["comparable-sales"] and row["header"]["dimension_set_id"] != next(item["dimension_set_id"] for item in package["catalog"]["dimension_sets"] if len(item["members"]) == 1) and row["payload"]["value"]["value"] == "0")
    store_values = {row["payload"]["metric_id"]: Decimal(row["payload"]["value"]["value"]) for row in observations if row["payload"]["kind"] == "NumericalFact" and row["payload"]["metric_id"] in {METRIC["store-openings"], METRIC["store-closures"]}}
    selected_guidance = {}
    for resolution in package["resolutions"]:
        selected = resolution["selected_record_id"]
        if selected and by_id[selected]["payload"]["kind"] == "GuidanceVersion":
            series = by_id[selected]["payload"]["guidance_series_id"]
            entity = next(row for row in package["entities"] if row["header"]["entity_id"] == series)
            selected_guidance[(entity["payload"]["horizon_period_id"], entity["payload"]["metric_id"])] = by_id[selected]["payload"]["value"]
    statement = next(row for row in observations if row["payload"]["kind"] == "ManagementStatement")
    event = next(row for row in observations if row["payload"]["kind"] == "CompanyEvent")
    interpretation = next(row for row in observations if row["payload"]["kind"] == "ModelInterpretation")
    return {
        "fixture_id": "anf-first-pass-v1", "artifact_state": package["artifact_state"],
        "qoq_percentage_point_change": changes["qoq-percentage-point"]["payload"]["value"]["value"],
        "yoy_percentage_point_change": changes["yoy-percentage-point"]["payload"]["value"]["value"],
        "apac_fy2025_q4": apac["payload"]["value"], "emea_fy2025_q4": "missing-by-absence",
        "derived_net_store_openings": str(store_values[METRIC["store-openings"]] + store_values[METRIC["store-closures"]]),
        "promise_achievement": "needs-review-no-source-tolerance",
        "fy2025_latest_revenue_guidance": selected_guidance[("period:anf:fy2025@1", METRIC["revenue-growth"])],
        "fy2025_latest_margin_guidance": selected_guidance[("period:anf:fy2025@1", METRIC["operating-margin"])],
        "fy2026_revenue_guidance": selected_guidance[("period:anf:fy2026@1", METRIC["revenue-growth"])],
        "fy2026_margin_guidance": selected_guidance[("period:anf:fy2026@1", METRIC["operating-margin"])],
        "q4_management_explanation_topic": statement["payload"]["topic_id"], "erp_event_effective_month": event["payload"]["effective_month"],
        "accepted_interpretation": interpretation["payload"]["interpretation"],
        "accepted_conflict_count": sum(1 for row in package["review_issues"] if row["severity"] == "P1"),
        "serialization_sha256": hashlib.sha256(serialize_package(package)).hexdigest(),
    }


def test_exact_anf_golden_fixture_is_closed_valid_and_source_backed(tmp_path):
    raw = load_json_strict(INPUT_PATH); expected = load_json_strict(EXPECTED_PATH)
    package = _materialize(raw)
    assert validate_package(package) == []
    assert _projection(package) == expected
    output = tmp_path / runtime_sidecar_filename(raw["company_id"])
    payload = serialize_package(package, output)
    assert output.read_bytes() == payload
    assert not payload.startswith(b"\xef\xbb\xbf")
    assert b"\r\n" not in payload


def test_calendar_rule_field_is_the_only_c1_golden_serialization_delta():
    package = _materialize(load_json_strict(INPUT_PATH))
    assert package["fiscal_calendars"][0]["calendar_rule_id"] == SOURCE_LABELLED_52_53_WEEK_RULE_ID
    legacy_shape = deepcopy(package)
    legacy_shape["fiscal_calendars"][0].pop("calendar_rule_id")
    assert hashlib.sha256(serialize_package(legacy_shape)).hexdigest() == "d0e434c250a86d5278b69f516291590bef9f5eb4fece4acb68c1cc87aadc2367"
    assert hashlib.sha256(serialize_package(package)).hexdigest() == "9fd73df61166105d83180da34e9ddcd5c126d83e498c1176c55f0f6a2c18ccc7"


def test_missing_malformed_unknown_and_misapplied_calendar_rules_fail_closed():
    missing = _materialize(load_json_strict(INPUT_PATH))
    missing["fiscal_calendars"][0].pop("calendar_rule_id")
    assert any("calendar_rule_id" in row.message for row in validate_package_schema(missing))

    malformed = _materialize(load_json_strict(INPUT_PATH))
    malformed["fiscal_calendars"][0]["calendar_rule_id"] = "calendar-year"
    assert any("calendar_rule_id" in row.normalized_path for row in validate_package_schema(malformed))

    unknown = _materialize(load_json_strict(INPUT_PATH))
    unknown["fiscal_calendars"][0]["calendar_rule_id"] = "rule:core:unknown-calendar@1"
    assert "fiscal_calendar_rule" in {row.rule_id for row in validate_package(unknown)}

    misapplied = _materialize(load_json_strict(INPUT_PATH))
    misapplied["fiscal_calendars"][0]["calendar_rule_id"] = CALENDAR_YEAR_RULE_ID
    assert "fiscal_period_calendar_rule" in {row.rule_id for row in validate_package(misapplied)}


def _append_calendar_year_change(package):
    calendar_id = "calendar:test:calendar-year@1"
    total_dimension_id = next(
        row["dimension_set_id"]
        for row in package["catalog"]["dimension_sets"]
        if len(row["members"]) == 1
    )
    templates = {
        row["header"]["effective_period_id"]: row
        for row in package["observations"]
        if row["payload"].get("kind") == "NumericalFact"
        and row["payload"].get("metric_id") == METRIC["comparable-sales"]
        and row["header"]["dimension_set_id"] == total_dimension_id
        and row["header"]["effective_period_id"]
        in {"period:anf:fy2025-q3@1", "period:anf:fy2025-q4@1"}
    }
    earlier_template = templates["period:anf:fy2025-q3@1"]
    later_template = templates["period:anf:fy2025-q4@1"]
    earlier_evidence = earlier_template["header"]["evidence_occurrence_ids"][0]
    later_evidence = later_template["header"]["evidence_occurrence_ids"][0]
    calendar = {
        "calendar_id": calendar_id,
        "calendar_rule_id": CALENDAR_YEAR_RULE_ID,
        "company_id": package["company_id"],
        "profile_hint": "reviewed calendar-year fiscal rule",
        "week_pattern": "calendar",
        "coverage_state": "partial",
        "evidence_occurrence_ids": sorted({earlier_evidence, later_evidence}),
        "reconciliation_state": "reconciled",
    }
    earlier_period = {
        "period_id": "period:test:calendar-fy2026-q1@1",
        "calendar_id": calendar_id,
        "company_id": package["company_id"],
        "fiscal_year": 2026,
        "fiscal_quarter": 1,
        "period_type": "quarter",
        "start_date": "2026-01-01",
        "end_date": "2026-03-31",
        "day_count": 90,
        "week_count": None,
        "fiscal_ordinal": 201,
        "is_53_week_year": False,
        "evidence_occurrence_ids": [earlier_evidence],
        "reconciliation_state": "reconciled",
    }
    later_period = {
        "period_id": "period:test:calendar-fy2026-q2@1",
        "calendar_id": calendar_id,
        "company_id": package["company_id"],
        "fiscal_year": 2026,
        "fiscal_quarter": 2,
        "period_type": "quarter",
        "start_date": "2026-04-01",
        "end_date": "2026-06-30",
        "day_count": 91,
        "week_count": None,
        "fiscal_ordinal": 202,
        "is_53_week_year": False,
        "evidence_occurrence_ids": [later_evidence],
        "reconciliation_state": "reconciled",
    }

    def fact(template, period, value, occurrence_id):
        payload = deepcopy(template["payload"])
        payload["business_key"] = numerical_business_key(
            company_id=package["company_id"],
            metric_id=payload["metric_id"],
            definition_id=payload["definition_id"],
            basis_id=payload["basis_id"],
            period_id=period["period_id"],
            dimension_set_id=template["header"]["dimension_set_id"],
            unit_id=payload["unit_id"],
            currency=payload["currency"],
        )
        payload["value"] = {"kind": "exact", "value": value}
        record_id = numerical_fact_identity(
            provenance_key=occurrence_id,
            company_id=package["company_id"],
            metric_id=payload["metric_id"],
            definition_id=payload["definition_id"],
            basis_id=payload["basis_id"],
            period_id=period["period_id"],
            dimension_set_id=template["header"]["dimension_set_id"],
            unit_id=payload["unit_id"],
            currency=payload["currency"],
        )
        header = deepcopy(template["header"])
        header.update(
            {
                "record_id": record_id,
                "identity_digest": identity_digest(record_id),
                "knowledge_date": package["knowledge_cutoff"],
                "effective_period_id": period["period_id"],
                "fiscal_period_id": period["period_id"],
                "period_type": "quarter",
                "evidence_occurrence_ids": [occurrence_id],
            }
        )
        return {"header": header, "payload": payload}

    earlier = fact(earlier_template, earlier_period, "-8", earlier_evidence)
    later = fact(later_template, later_period, "-5", later_evidence)
    for record in (earlier, later):
        resolution = resolve_observations(
            [record],
            policy_id="policy:core:reported-numerical@1",
            as_of_date=package["knowledge_cutoff"],
            source_documents=package["source_documents"],
            evidence_occurrences=package["evidence_occurrences"],
        )
        package["observations"].append(record)
        package["resolutions"].append(resolution.resolution)
    change = derive_percentage_point_change(
        earlier,
        later,
        earlier_period=earlier_period,
        later_period=later_period,
        earlier_calendar=calendar,
        later_calendar=calendar,
        change_kind="qoq-percentage-point",
        rule_id="rule:core:qoq-percentage-point@1",
        change_unit_id=UNIT_PP,
    )
    package["fiscal_calendars"].append(calendar)
    package["periods"].extend([earlier_period, later_period])
    package["observations"].append(change)
    return change


def test_full_package_calendar_year_change_constructs_and_replays():
    package = _materialize(load_json_strict(INPUT_PATH))
    change = _append_calendar_year_change(package)
    assert change["payload"]["value"] == {"kind": "exact", "value": "3"}
    assert change["payload"]["comparability"]["checks"]["same_duration"] is False
    assert validate_package(package) == []

    change["payload"]["comparability"]["checks"]["same_duration"] = True
    assert "change_semantic_binding" in {row.rule_id for row in validate_package(package)}


@pytest.mark.parametrize(
    "change_kind", ["qoq-percentage-point", "yoy-percentage-point"]
)
@pytest.mark.parametrize("mutation_direction", ["false-to-true", "true-to-false"])
def test_full_package_replays_source_labelled_year_classification_mismatch(
    change_kind, mutation_direction
):
    package = _materialize(load_json_strict(INPUT_PATH))
    change = next(
        row
        for row in package["observations"]
        if row["payload"].get("change_kind") == change_kind
    )
    observations = {
        row["header"]["record_id"]: row for row in package["observations"]
    }
    periods = {row["period_id"]: row for row in package["periods"]}
    earlier = periods[
        observations[change["payload"]["from_record_id"]]["header"]["fiscal_period_id"]
    ]
    later = periods[
        observations[change["payload"]["to_record_id"]]["header"]["fiscal_period_id"]
    ]
    stored_comparability = deepcopy(change["payload"]["comparability"])
    assert earlier["is_53_week_year"] is False
    assert later["is_53_week_year"] is False
    if mutation_direction == "false-to-true":
        later["is_53_week_year"] = True
    else:
        earlier["is_53_week_year"] = True

    assert change["payload"]["comparability"] == stored_comparability
    assert change["payload"]["comparability"]["comparable"] is True
    issues = validate_package(package)
    assert "change_semantic_binding" in {row.rule_id for row in issues}
    assert any(
        "fiscal-year-length classification differs" in row.message
        for row in issues
        if row.rule_id == "change_semantic_binding"
    )


def test_missing_calendar_reference_and_calendar_rule_mutation_fail_full_replay():
    missing_calendar = _materialize(load_json_strict(INPUT_PATH))
    q3 = _period_for(missing_calendar, year=2025, quarter=3, period_type="quarter")
    q3["calendar_id"] = "calendar:test:missing@1"
    missing_rules = {row.rule_id for row in validate_package(missing_calendar)}
    assert "fiscal_calendar_reference" in missing_rules
    assert "change_semantic_binding" in missing_rules

    wrong_rule = _materialize(load_json_strict(INPUT_PATH))
    wrong_rule["fiscal_calendars"][0]["calendar_rule_id"] = CALENDAR_YEAR_RULE_ID
    wrong_rules = {row.rule_id for row in validate_package(wrong_rule)}
    assert "fiscal_period_calendar_rule" in wrong_rules
    assert "change_semantic_binding" in wrong_rules


def test_all_source_and_record_permutations_are_byte_identical():
    package = _materialize(load_json_strict(INPUT_PATH))
    permuted = deepcopy(package)
    for key in ("periods", "source_documents", "evidence_occurrences", "entities", "observations", "relations", "resolutions", "review_issues"):
        permuted[key].reverse()
    for key in permuted["catalog"]:
        permuted["catalog"][key].reverse()
    assert validate_package(permuted) == []
    assert serialize_package(package) == serialize_package(permuted)

    shuffled = deepcopy(package)
    rng = random.Random(20260304)
    for key in ("periods", "source_documents", "evidence_occurrences", "entities", "observations", "relations", "resolutions", "review_issues"):
        rng.shuffle(shuffled[key])
    for key in shuffled["catalog"]:
        rng.shuffle(shuffled["catalog"][key])
    assert validate_package(shuffled) == []
    assert serialize_package(package) == serialize_package(shuffled)


def test_accepted_fixture_has_no_fabricated_conflict_and_in_memory_apac_clone_is_p1():
    package = _materialize(load_json_strict(INPUT_PATH))
    apac = next(row for row in package["observations"] if row["payload"].get("value") == {"kind": "exact", "value": "0"} and row["payload"].get("metric_id") == METRIC["comparable-sales"])
    clone = deepcopy(apac)
    source_document = package["source_documents"][0]
    test_occurrence_id = evidence_occurrence_identity(company_id="ANF", document_key=source_document["document_key"], document_revision=1, locator_kind="cell", locator_key="test-only-conflicting-apac")
    test_occurrence = {"evidence_occurrence_id": test_occurrence_id, "source_document_id": source_document["source_document_id"], "company_id": "ANF", "review_state": "accepted"}
    clone_id = numerical_fact_identity(provenance_key=test_occurrence_id, company_id="ANF", metric_id=clone["payload"]["metric_id"], definition_id=clone["payload"]["definition_id"], basis_id=clone["payload"]["basis_id"], period_id=clone["header"]["fiscal_period_id"], dimension_set_id=clone["header"]["dimension_set_id"], unit_id=clone["payload"]["unit_id"], currency=clone["payload"]["currency"])
    clone["header"]["record_id"] = clone_id; clone["header"]["identity_digest"] = identity_digest(clone_id); clone["header"]["evidence_occurrence_ids"] = [test_occurrence_id]
    clone["payload"]["value"] = {"kind": "exact", "value": "1"}
    result = resolve_observations([apac, clone], policy_id="policy:core:reported-numerical@1", as_of_date="2026-03-04", source_documents=package["source_documents"], evidence_occurrences=[*package["evidence_occurrences"], test_occurrence])
    assert result.resolution["status"] == "unresolved"
    assert result.review_issues[0]["severity"] == "P1"
    assert all(row["severity"] != "P1" for row in package["review_issues"])


def test_exact_replayed_conflict_and_mandatory_issue_form_a_valid_blocked_artifact():
    package = _materialize(load_json_strict(INPUT_PATH))
    apac = next(row for row in package["observations"] if row["payload"].get("value") == {"kind": "exact", "value": "0"} and row["payload"].get("metric_id") == METRIC["comparable-sales"])
    source_document = next(
        row
        for row in package["source_documents"]
        if row["source_document_id"] == next(
            occurrence["source_document_id"]
            for occurrence in package["evidence_occurrences"]
            if occurrence["evidence_occurrence_id"] == apac["header"]["evidence_occurrence_ids"][0]
        )
    )
    occurrence = deepcopy(next(row for row in package["evidence_occurrences"] if row["evidence_occurrence_id"] == apac["header"]["evidence_occurrence_ids"][0]))
    occurrence_id = evidence_occurrence_identity(company_id="ANF", document_key=source_document["document_key"], document_revision=1, locator_kind="cell", locator_key="test-only-blocked-apac-conflict")
    occurrence["evidence_occurrence_id"] = occurrence_id
    occurrence["identity_digest"] = identity_digest(occurrence_id)
    occurrence["occurrence_key"] = "test-only-blocked-apac-conflict"
    occurrence["locator_kind"] = "cell"
    occurrence["locator_key"] = "test-only-blocked-apac-conflict"
    occurrence["excerpt"] = "Test-only APAC conflict value 1%."
    clone = deepcopy(apac)
    clone_id = numerical_fact_identity(provenance_key=occurrence_id, company_id="ANF", metric_id=clone["payload"]["metric_id"], definition_id=clone["payload"]["definition_id"], basis_id=clone["payload"]["basis_id"], period_id=clone["header"]["fiscal_period_id"], dimension_set_id=clone["header"]["dimension_set_id"], unit_id=clone["payload"]["unit_id"], currency=clone["payload"]["currency"])
    clone["header"]["record_id"] = clone_id
    clone["header"]["identity_digest"] = identity_digest(clone_id)
    clone["header"]["evidence_occurrence_ids"] = [occurrence_id]
    clone["payload"]["value"] = {"kind": "exact", "value": "1"}
    replay = resolve_observations([apac, clone], policy_id="policy:core:reported-numerical@1", as_of_date=package["knowledge_cutoff"], source_documents=package["source_documents"], evidence_occurrences=[*package["evidence_occurrences"], occurrence])
    assert replay.resolution["status"] == "unresolved"
    package["evidence_occurrences"].append(occurrence)
    package["observations"].append(clone)
    package["relations"].extend(replay.inferred_relations)
    package["resolutions"].append(replay.resolution)
    package["review_issues"].extend(replay.review_issues)
    package["artifact_state"] = "needs_review"
    assert validate_package(package) == []


def test_approximate_store_promise_is_not_auto_achieved_without_tolerance():
    raw = load_json_strict(INPUT_PATH)
    target = _value(raw["promise"]["target"])
    actual = {"kind": "exact", "value": "40"}
    assert values_compatible(actual, target) is False
    package = _materialize(raw)
    assert any(row["rule_id"] == "promise_approximate_tolerance_missing" for row in package["review_issues"])


def test_ambiguous_raw_promise_update_is_blocked_without_inventing_a_version():
    package = _materialize(load_json_strict(INPUT_PATH))
    promise, origin, later = _promise_parts(package)
    occurrence_id = later["header"]["evidence_occurrence_ids"][0]
    subject_id = promise["payload"]["promise_subject_id"]
    other_program = "parallel-store-plan"
    other_promise_id = promise_identity(company_id="ANF", subject_id=subject_id, program_id=other_program, origin_occurrence_id=occurrence_id)
    other_version_id = promise_version_identity(promise_id=other_promise_id, occurrence_id=occurrence_id)
    other_entity = deepcopy(promise)
    other_entity["header"]["entity_id"] = other_promise_id
    other_entity["header"]["identity_digest"] = identity_digest(other_promise_id)
    other_entity["header"]["evidence_occurrence_ids"] = [occurrence_id]
    other_entity["payload"]["program_id"] = other_program
    other_entity["payload"]["origin_occurrence_id"] = occurrence_id
    other_entity["payload"]["origin_version_id"] = other_version_id
    other_entity["payload"]["original_wording"] = later["payload"]["wording"]
    other_entity["payload"]["original_target"] = deepcopy(later["payload"]["target"])
    other_entity["payload"]["original_baseline"] = deepcopy(later["payload"]["baseline"])
    other_entity["payload"]["original_deadline"] = deepcopy(later["payload"]["deadline"])
    other_version = deepcopy(later)
    other_version["header"]["record_id"] = other_version_id
    other_version["header"]["identity_digest"] = identity_digest(other_version_id)
    other_version["header"]["subject_id"] = other_promise_id
    other_version["payload"]["promise_id"] = other_promise_id
    other_version["payload"]["previous_version_id"] = None
    other_version["payload"]["change_kind"] = "origin"
    other_version["payload"]["version_state"] = "active"

    period_id = origin["header"]["fiscal_period_id"]
    statement_id = management_statement_identity(company_id="ANF", statement_kind="commitment", topic_id=subject_id, period_id=period_id, speaker_id="management", occurrence_id=occurrence_id)
    statement = {
        "header": _header(statement_id, "ManagementStatement", "ANF", subject_id, later["header"]["publication_date"], period_id, "annual", later["header"]["dimension_set_id"], "stated", [occurrence_id], fiscal_period_id=period_id),
        "payload": {"kind": "ManagementStatement", "statement_kind": "commitment", "topic_id": subject_id, "statement_period_id": period_id, "speaker_id": "management", "statement": "A later source statement that cannot be matched to exactly one store-plan promise."},
    }
    matching_ids = sorted([promise["header"]["entity_id"], other_promise_id])
    issue_id = build_identity("review", (("rule", "promise-match-cardinality"), ("business", subject_id)))
    issue = {
        "issue_id": issue_id,
        "severity": "P1",
        "rule_id": "promise_match_cardinality",
        "entity_ids": matching_ids,
        "business_key": subject_id,
        "message": "The source-backed promise update matches multiple Promise entities.",
        "evidence_occurrence_ids": [occurrence_id],
        "candidate_record_ids": matching_ids,
        "suggested_action": "Resolve one promise match before emitting a PromiseVersion.",
        "promotion_blocking": True,
        "review_state": "needs_review",
    }
    package["entities"].append(other_entity)
    package["observations"].extend([other_version, statement])
    package["review_issues"].append(issue)
    package["artifact_state"] = "needs_review"
    assert statement["payload"]["kind"] == "ManagementStatement"
    assert not any(row["payload"].get("kind") == "PromiseVersion" and row["header"]["record_id"] == statement_id for row in package["observations"])
    assert validate_package(package) == []


def test_missing_explicit_zero_and_unavailable_have_distinct_representations():
    package = _materialize(load_json_strict(INPUT_PATH))
    zero = next(row for row in package["observations"] if row["payload"].get("value") == {"kind": "exact", "value": "0"})
    assert zero["payload"]["kind"] == "NumericalFact"
    unavailable_business = build_identity("business-fact", (("key", "test-unavailable"),))
    occurrence_id = package["evidence_occurrences"][0]["evidence_occurrence_id"]
    unavailable_id = availability_observation_identity(company_id="ANF", business_key=unavailable_business, availability_state="not-disclosed", occurrence_id=occurrence_id)
    unavailable = {"record_id": unavailable_id, "payload": {"kind": "AvailabilityObservation", "availability_state": "not-disclosed"}}
    assert unavailable["payload"]["kind"] != zero["payload"]["kind"]
    missing = None
    assert missing is None


def test_change_validation_fails_closed_when_input_is_unselected_or_value_is_tampered():
    package = _materialize(load_json_strict(INPUT_PATH))
    change = next(row for row in package["observations"] if row["payload"].get("change_kind") == "qoq-percentage-point")
    earlier_id = change["payload"]["from_record_id"]
    package["resolutions"] = [row for row in package["resolutions"] if row.get("selected_record_id") != earlier_id]
    issues = validate_package(package)
    assert any(row.rule_id == "change_input_selection" for row in issues)

    package = _materialize(load_json_strict(INPUT_PATH))
    change = next(row for row in package["observations"] if row["payload"].get("change_kind") == "qoq-percentage-point")
    change["payload"]["value"] = {"kind": "exact", "value": "999"}
    issues = validate_package(package)
    assert any(row.rule_id == "change_semantic_binding" for row in issues)


def _observations_by_id(package):
    return {row["header"]["record_id"]: row for row in package["observations"]}


def _fy2025_guidance_resolution(package):
    observations = _observations_by_id(package)
    return next(
        row
        for row in package["resolutions"]
        if row["record_type"] == "GuidanceVersion"
        and len(row["candidate_record_ids"]) == 5
        and observations[row["selected_record_id"]]["payload"]["guidance_series_id"] == row["business_key"]
    )


def _replace_relation_identity(relation):
    relation_id = relation_identity(
        relation_type=relation["relation_type"],
        from_record_id=relation["from_record_id"],
        to_record_id=relation["to_record_id"],
        rule_id=relation["rule_id"],
    )
    relation["relation_id"] = relation_id
    relation["identity_digest"] = identity_digest(relation_id)


def _append_relation(package, relation_type, source, target, rule_id):
    relation = _make_relation(
        relation_type,
        source["header"]["record_id"],
        target["header"]["record_id"],
        rule_id,
        source["header"]["evidence_occurrence_ids"],
    )
    package["relations"].append(relation)
    return relation


def _mutate_selected_older_guidance(package):
    resolution = _fy2025_guidance_resolution(package)
    observations = _observations_by_id(package)
    oldest = min(resolution["candidate_record_ids"], key=lambda value: observations[value]["header"]["knowledge_date"])
    assert oldest != resolution["selected_record_id"]
    resolution["selected_record_id"] = oldest


def _mutate_unresolved_without_review(package):
    resolution = _fy2025_guidance_resolution(package)
    resolution["selected_record_id"] = None
    resolution["selection_cardinality"] = 0
    resolution["status"] = "unresolved"
    resolution["reason_codes"] = ["canonical_equal_authority_conflict"]
    resolution["review_issue_ids"] = []


def _mutate_rejected_source_document(package):
    package["source_documents"][0]["review_state"] = "rejected"


def _mutate_rejected_occurrence(package):
    package["evidence_occurrences"][0]["review_state"] = "rejected"


def _mutate_ineligible_source_authority(package):
    package["source_documents"][0]["authority_class"] = "model-generated"


def _promise_parts(package):
    promise = next(row for row in package["entities"] if row["payload"]["kind"] == "Promise")
    versions = [row for row in package["observations"] if row["payload"]["kind"] == "PromiseVersion"]
    origin = next(row for row in versions if row["payload"]["change_kind"] == "origin")
    later = next(row for row in versions if row is not origin)
    return promise, origin, later


def _mutate_reaffirmation_withdrawn(package):
    _, _, later = _promise_parts(package)
    later["payload"]["version_state"] = "withdrawn"


def _mutate_origin_owned_by_another_promise(package):
    promise, _, later = _promise_parts(package)
    later_occurrence = later["header"]["evidence_occurrence_ids"][0]
    other_promise_id = promise_identity(
        company_id="ANF",
        subject_id=promise["payload"]["promise_subject_id"],
        program_id="test-other-program",
        origin_occurrence_id=later_occurrence,
    )
    other_version_id = promise_version_identity(promise_id=other_promise_id, occurrence_id=later_occurrence)
    other_entity = deepcopy(promise)
    other_entity["header"]["entity_id"] = other_promise_id
    other_entity["header"]["identity_digest"] = identity_digest(other_promise_id)
    other_entity["header"]["evidence_occurrence_ids"] = [later_occurrence]
    other_entity["payload"]["program_id"] = "test-other-program"
    other_entity["payload"]["origin_occurrence_id"] = later_occurrence
    other_entity["payload"]["origin_version_id"] = other_version_id
    other_entity["payload"]["original_wording"] = later["payload"]["wording"]
    other_entity["payload"]["original_target"] = deepcopy(later["payload"]["target"])
    other_entity["payload"]["original_baseline"] = deepcopy(later["payload"]["baseline"])
    other_entity["payload"]["original_deadline"] = deepcopy(later["payload"]["deadline"])
    later["header"]["record_id"] = other_version_id
    later["header"]["identity_digest"] = identity_digest(other_version_id)
    later["header"]["subject_id"] = other_promise_id
    later["payload"]["promise_id"] = other_promise_id
    later["payload"]["previous_version_id"] = None
    later["payload"]["change_kind"] = "origin"
    later["payload"]["version_state"] = "active"
    package["relations"] = [row for row in package["relations"] if row["relation_type"] != "reaffirms"]
    package["entities"].append(other_entity)
    promise["payload"]["origin_version_id"] = other_version_id


def _mutate_origin_occurrence_mismatch(package):
    promise, _, later = _promise_parts(package)
    promise["payload"]["origin_occurrence_id"] = later["header"]["evidence_occurrence_ids"][0]


def _mutate_promise_change_kind_mismatch(package):
    _, _, later = _promise_parts(package)
    later["payload"]["target"] = {"kind": "exact", "value": "45"}


def _mutate_ambiguous_promise_match(package):
    promise, _, later = _promise_parts(package)
    occurrence_id = later["header"]["evidence_occurrence_ids"][0]
    other_promise_id = promise_identity(company_id="ANF", subject_id=promise["payload"]["promise_subject_id"], program_id=promise["payload"]["program_id"], origin_occurrence_id=occurrence_id)
    other_version_id = promise_version_identity(promise_id=other_promise_id, occurrence_id=occurrence_id)
    other_entity = deepcopy(promise)
    other_entity["header"]["entity_id"] = other_promise_id
    other_entity["header"]["identity_digest"] = identity_digest(other_promise_id)
    other_entity["header"]["evidence_occurrence_ids"] = [occurrence_id]
    other_entity["payload"]["origin_occurrence_id"] = occurrence_id
    other_entity["payload"]["origin_version_id"] = other_version_id
    other_entity["payload"]["original_wording"] = later["payload"]["wording"]
    other_entity["payload"]["original_target"] = deepcopy(later["payload"]["target"])
    other_entity["payload"]["original_baseline"] = deepcopy(later["payload"]["baseline"])
    other_entity["payload"]["original_deadline"] = deepcopy(later["payload"]["deadline"])
    other_version = deepcopy(later)
    other_version["header"]["record_id"] = other_version_id
    other_version["header"]["identity_digest"] = identity_digest(other_version_id)
    other_version["header"]["subject_id"] = other_promise_id
    other_version["payload"]["promise_id"] = other_promise_id
    other_version["payload"]["previous_version_id"] = None
    other_version["payload"]["change_kind"] = "origin"
    other_version["payload"]["version_state"] = "active"
    package["entities"].append(other_entity)
    package["observations"].append(other_version)


def _mutate_qoq_output_to_percent(package):
    change = next(row for row in package["observations"] if row["payload"].get("change_kind") == "qoq-percentage-point")
    change["payload"]["unit_id"] = UNIT_PERCENT


def _mutate_quarter_header_to_annual(package):
    change = next(row for row in package["observations"] if row["payload"].get("change_kind") == "qoq-percentage-point")
    _observations_by_id(package)[change["payload"]["from_record_id"]]["header"]["period_type"] = "annual"


def _mutate_period_to_needs_review(package):
    change = next(row for row in package["observations"] if row["payload"].get("change_kind") == "qoq-percentage-point")
    from_period = _observations_by_id(package)[change["payload"]["from_record_id"]]["header"]["fiscal_period_id"]
    next(row for row in package["periods"] if row["period_id"] == from_period)["reconciliation_state"] = "needs_review"


def _mutate_duplicate_period(package):
    package["periods"].append(deepcopy(package["periods"][0]))


def _mutate_duplicate_calendar(package):
    package["fiscal_calendars"].append(deepcopy(package["fiscal_calendars"][0]))


def _mutate_invalid_correction_endpoint(package):
    observations = package["observations"]
    guidance = next(row for row in observations if row["payload"]["kind"] == "GuidanceVersion")
    fact = next(row for row in observations if row["payload"]["kind"] == "NumericalFact")
    _append_relation(package, "corrects", guidance, fact, "rule:core:test-invalid-correction@1")


def _mutate_cross_business_supersession(package):
    facts = [row for row in package["observations"] if row["payload"]["kind"] == "NumericalFact"]
    source = next(row for row in facts if row["payload"]["metric_id"] == METRIC["store-openings"])
    target = next(row for row in facts if row["payload"]["metric_id"] == METRIC["ending-stores"])
    _append_relation(package, "supersedes", source, target, "rule:core:test-cross-business@1")


def _mutate_backward_supersession(package):
    relation = next(row for row in package["relations"] if row["relation_type"] == "supersedes")
    observations = _observations_by_id(package)
    relation["from_record_id"], relation["to_record_id"] = relation["to_record_id"], relation["from_record_id"]
    relation["evidence_occurrence_ids"] = list(observations[relation["from_record_id"]]["header"]["evidence_occurrence_ids"])
    _replace_relation_identity(relation)


def _mutate_relation_cycle(package):
    resolution = _fy2025_guidance_resolution(package)
    observations = _observations_by_id(package)
    ordered = sorted((observations[value] for value in resolution["candidate_record_ids"]), key=lambda row: row["header"]["knowledge_date"])
    _append_relation(package, "supersedes", ordered[0], ordered[-1], "rule:core:test-cycle@1")


def _mutate_stored_maxima(package):
    resolution = _fy2025_guidance_resolution(package)
    older = next(value for value in resolution["candidate_record_ids"] if value != resolution["selected_record_id"])
    resolution["maximal_candidate_ids"] = [older]


def _mutate_missing_mandatory_issue(package):
    observations = _observations_by_id(package)
    resolution = next(row for row in package["resolutions"] if row["record_type"] == "NumericalFact" and len(row["candidate_record_ids"]) > 1)
    clone_id = next(value for value in resolution["candidate_record_ids"] if value != resolution["selected_record_id"])
    observations[clone_id]["payload"]["value"] = {"kind": "exact", "value": "2"}
    replay = resolve_observations(
        [observations[value] for value in resolution["candidate_record_ids"]],
        policy_id=resolution["policy_id"],
        as_of_date=resolution["as_of_date"],
        source_documents=package["source_documents"],
        evidence_occurrences=package["evidence_occurrences"],
        relations=package["relations"],
    )
    package["resolutions"][package["resolutions"].index(resolution)] = replay.resolution
    assert replay.resolution["status"] == "unresolved"
    assert replay.review_issues


def _mutate_change_to_nonselected_fact(package):
    observations = _observations_by_id(package)
    total_resolution = next(row for row in package["resolutions"] if row["record_type"] == "NumericalFact" and len(row["candidate_record_ids"]) > 1)
    nonselected_id = next(value for value in total_resolution["candidate_record_ids"] if value != total_resolution["selected_record_id"])
    change = next(row for row in package["observations"] if row["payload"].get("change_kind") == "qoq-percentage-point")
    earlier = observations[change["payload"]["from_record_id"]]
    nonselected = observations[nonselected_id]
    change["payload"]["to_record_id"] = nonselected_id
    change["payload"]["input_record_ids"] = sorted([change["payload"]["from_record_id"], nonselected_id])
    change["header"]["evidence_occurrence_ids"] = sorted(set(earlier["header"]["evidence_occurrence_ids"]) | set(nonselected["header"]["evidence_occurrence_ids"]))
    record_id = change_observation_identity(
        company_id="ANF",
        change_kind=change["payload"]["change_kind"],
        from_record_id=change["payload"]["from_record_id"],
        to_record_id=nonselected_id,
        rule_id=change["payload"]["rule_id"],
    )
    change["header"]["record_id"] = record_id
    change["header"]["identity_digest"] = identity_digest(record_id)


def _mutate_interpretation_unreviewed(package):
    interpretation = next(row for row in package["observations"] if row["payload"]["kind"] == "ModelInterpretation")
    interpretation["header"]["review_state"] = "accepted"


@pytest.mark.parametrize(
    ("case", "mutation", "expected_rules"),
    [
        ("selected older non-maximal guidance", _mutate_selected_older_guidance, {"resolution_replay_mismatch"}),
        ("unresolved resolution without review", _mutate_unresolved_without_review, {"resolution_replay_mismatch", "mandatory_review_issue_missing"}),
        ("accepted observation rejected source", _mutate_rejected_source_document, {"accepted_record_source_ineligible"}),
        ("accepted observation rejected occurrence", _mutate_rejected_occurrence, {"accepted_record_source_ineligible"}),
        ("assertion-ineligible source authority", _mutate_ineligible_source_authority, {"accepted_record_source_ineligible"}),
        ("reaffirmation withdrawn", _mutate_reaffirmation_withdrawn, {"promise_change_kind", "promise_version_state"}),
        ("origin version belongs to another promise", _mutate_origin_owned_by_another_promise, {"promise_origin_version_ownership"}),
        ("origin occurrence mismatch", _mutate_origin_occurrence_mismatch, {"promise_origin_occurrence_mismatch"}),
        ("promise change kind mismatch", _mutate_promise_change_kind_mismatch, {"promise_change_kind"}),
        ("ambiguous multi-promise match", _mutate_ambiguous_promise_match, {"promise_match_cardinality"}),
        ("qoq output unit percent", _mutate_qoq_output_to_percent, {"change_semantic_binding"}),
        ("quarter fact header annual", _mutate_quarter_header_to_annual, {"accepted_record_period_type"}),
        ("referenced period needs review", _mutate_period_to_needs_review, {"accepted_record_period_needs_review"}),
        ("duplicate period id", _mutate_duplicate_period, {"period_identity_unique"}),
        ("duplicate fiscal calendar id", _mutate_duplicate_calendar, {"fiscal_calendar_identity_unique"}),
        ("invalid correction endpoint", _mutate_invalid_correction_endpoint, {"history_relation_endpoint_type"}),
        ("cross business key supersession", _mutate_cross_business_supersession, {"history_relation_business_identity"}),
        ("backward history relation", _mutate_backward_supersession, {"history_relation_time_direction"}),
        ("relation cycle", _mutate_relation_cycle, {"history_relation_cycle"}),
        ("stored maxima differs from replay", _mutate_stored_maxima, {"resolution_replay_mismatch"}),
        ("missing mandatory p1 while accepted", _mutate_missing_mandatory_issue, {"resolution_review_issue_missing", "artifact_state_fail_closed"}),
        ("change uses nonselected candidate", _mutate_change_to_nonselected_fact, {"change_input_selection"}),
        ("accepted interpretation lacks reviewed state", _mutate_interpretation_unreviewed, {"interpretation_review_input_state"}),
    ],
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_full_package_semantic_mutations_fail_closed(case, mutation, expected_rules):
    package = _materialize(load_json_strict(INPUT_PATH))
    assert validate_package(package) == [], f"accepted fixture precondition failed for {case}"
    mutation(package)
    issues = validate_package(package)
    actual_rules = {row.rule_id for row in issues}
    assert expected_rules <= actual_rules, f"{case}: expected {expected_rules}, got {actual_rules}"


@pytest.mark.parametrize(
    ("collection", "expected_rule"),
    [
        ("catalog.metrics", "catalog_identity_unique"),
        ("catalog.dimension_sets", "dimension_set_identity_unique"),
        ("source_documents", "source_document_identity_unique"),
        ("evidence_occurrences", "evidence_occurrence_identity_unique"),
        ("entities", "entity_identity_unique"),
        ("observations", "observation_identity_unique"),
        ("relations", "relation_identity_unique"),
        ("resolutions", "resolution_identity_unique"),
        ("review_issues", "review_issue_identity_unique"),
    ],
)
def test_full_package_rejects_every_duplicate_identity_index(collection, expected_rule):
    package = _materialize(load_json_strict(INPUT_PATH))
    assert validate_package(package) == []
    if collection.startswith("catalog."):
        rows = package["catalog"][collection.split(".", 1)[1]]
    else:
        rows = package[collection]
    rows.append(deepcopy(rows[0]))
    assert expected_rule in {row.rule_id for row in validate_package(package)}


def _period_for(package, *, year, quarter=None, period_type=None):
    return next(
        row
        for row in package["periods"]
        if row["fiscal_year"] == year
        and row["fiscal_quarter"] == quarter
        and (period_type is None or row["period_type"] == period_type)
    )


def _apac_fact(package):
    return next(
        row
        for row in package["observations"]
        if row["payload"].get("kind") == "NumericalFact"
        and row["payload"].get("metric_id") == METRIC["comparable-sales"]
        and row["payload"].get("value") == {"kind": "exact", "value": "0"}
    )


def _guidance_version_and_series(package, *, fiscal_year=2025):
    entities = {row["header"]["entity_id"]: row for row in package["entities"]}
    version = next(
        row
        for row in package["observations"]
        if row["payload"].get("kind") == "GuidanceVersion"
        and entities[row["payload"]["guidance_series_id"]]["payload"]["horizon_period_id"]
        == _period_for(package, year=fiscal_year, quarter=None, period_type="annual")["period_id"]
    )
    return version, entities[version["payload"]["guidance_series_id"]]


def _typed_period_record(package, kind):
    return next(row for row in package["observations"] if row["payload"].get("kind") == kind)


def _mutate_fact_without_fiscal_period(package):
    _apac_fact(package)["header"]["fiscal_period_id"] = None


def _mutate_fact_conflicting_effective_and_fiscal_periods(package):
    fact = _apac_fact(package)
    fact["header"]["effective_period_id"] = _period_for(package, year=2025, quarter=3)["period_id"]


def _mutate_guidance_without_fiscal_period(package):
    version, _ = _guidance_version_and_series(package)
    version["header"]["fiscal_period_id"] = None


def _mutate_guidance_effective_period_away_from_horizon(package):
    version, _ = _guidance_version_and_series(package)
    version["header"]["effective_period_id"] = _period_for(package, year=2026, quarter=None, period_type="annual")["period_id"]


def _mutate_guidance_fiscal_period_away_from_horizon(package):
    version, _ = _guidance_version_and_series(package)
    version["header"]["fiscal_period_id"] = _period_for(package, year=2026, quarter=None, period_type="annual")["period_id"]


def _mutate_guidance_dimension_away_from_series(package):
    version, _ = _guidance_version_and_series(package)
    version["header"]["dimension_set_id"] = _apac_fact(package)["header"]["dimension_set_id"]


def _mutate_statement_without_fiscal_period(package):
    _typed_period_record(package, "ManagementStatement")["header"]["fiscal_period_id"] = None


def _mutate_statement_effective_period_away_from_payload(package):
    statement = _typed_period_record(package, "ManagementStatement")
    statement["header"]["effective_period_id"] = _period_for(package, year=2025, quarter=3)["period_id"]


def _mutate_interpretation_without_fiscal_period(package):
    _typed_period_record(package, "ModelInterpretation")["header"]["fiscal_period_id"] = None


def _mutate_interpretation_effective_period_away_from_payload(package):
    interpretation = _typed_period_record(package, "ModelInterpretation")
    interpretation["header"]["effective_period_id"] = _period_for(package, year=2025, quarter=3)["period_id"]


def _mutate_promise_period_away_from_deadline(package):
    _, _, version = _promise_parts(package)
    wrong_period_id = _period_for(package, year=2026, quarter=None, period_type="annual")["period_id"]
    version["header"]["effective_period_id"] = wrong_period_id
    version["header"]["fiscal_period_id"] = wrong_period_id


def _mutate_change_and_inputs_without_fiscal_periods(package):
    observations = _observations_by_id(package)
    change = next(row for row in package["observations"] if row["payload"].get("change_kind") == "qoq-percentage-point")
    observations[change["payload"]["from_record_id"]]["header"]["fiscal_period_id"] = None
    observations[change["payload"]["to_record_id"]]["header"]["fiscal_period_id"] = None
    change["header"]["fiscal_period_id"] = None


def _mutate_change_fiscal_period_away_from_later_fact(package):
    change = next(row for row in package["observations"] if row["payload"].get("change_kind") == "qoq-percentage-point")
    change["header"]["fiscal_period_id"] = _period_for(package, year=2025, quarter=3)["period_id"]


def _mutate_change_period_type_away_from_later_fact(package):
    change = next(row for row in package["observations"] if row["payload"].get("change_kind") == "qoq-percentage-point")
    change["header"]["period_type"] = "annual"


def _mutate_change_effective_period_away_from_later_fact(package):
    change = next(row for row in package["observations"] if row["payload"].get("change_kind") == "qoq-percentage-point")
    change["header"]["effective_period_id"] = _period_for(package, year=2025, quarter=3)["period_id"]


def _mutate_event_with_inconsistent_fiscal_context(package):
    event = _typed_period_record(package, "CompanyEvent")
    event["header"]["fiscal_period_id"] = _period_for(package, year=2025, quarter=4)["period_id"]


@pytest.mark.parametrize(
    ("case", "mutation", "expected_rules"),
    [
        ("NumericalFact fiscal period removed", _mutate_fact_without_fiscal_period, {"numerical_fact_period_binding"}),
        ("NumericalFact effective and fiscal periods conflict", _mutate_fact_conflicting_effective_and_fiscal_periods, {"numerical_fact_period_binding"}),
        ("GuidanceVersion fiscal period removed", _mutate_guidance_without_fiscal_period, {"guidance_period_binding"}),
        ("GuidanceVersion effective period differs from series horizon", _mutate_guidance_effective_period_away_from_horizon, {"guidance_period_binding"}),
        ("GuidanceVersion fiscal period differs from series horizon", _mutate_guidance_fiscal_period_away_from_horizon, {"guidance_period_binding"}),
        ("GuidanceVersion dimension differs from series", _mutate_guidance_dimension_away_from_series, {"guidance_series_binding"}),
        ("ManagementStatement fiscal period removed", _mutate_statement_without_fiscal_period, {"management_statement_period_binding"}),
        ("ManagementStatement effective period differs from payload", _mutate_statement_effective_period_away_from_payload, {"management_statement_period_binding"}),
        ("ModelInterpretation fiscal period removed", _mutate_interpretation_without_fiscal_period, {"model_interpretation_period_binding"}),
        ("ModelInterpretation effective period differs from payload", _mutate_interpretation_effective_period_away_from_payload, {"model_interpretation_period_binding"}),
        ("PromiseVersion period differs from period deadline", _mutate_promise_period_away_from_deadline, {"promise_deadline_period_binding"}),
        ("selected NumericalFacts and ChangeObservation lose fiscal identity", _mutate_change_and_inputs_without_fiscal_periods, {"numerical_fact_period_binding", "change_semantic_binding"}),
        ("ChangeObservation fiscal period differs from later fact", _mutate_change_fiscal_period_away_from_later_fact, {"change_semantic_binding"}),
        ("ChangeObservation period type differs from later fact", _mutate_change_period_type_away_from_later_fact, {"change_semantic_binding"}),
        ("ChangeObservation effective period differs from later fact", _mutate_change_effective_period_away_from_later_fact, {"change_semantic_binding"}),
        ("CompanyEvent claims inconsistent fiscal context", _mutate_event_with_inconsistent_fiscal_context, {"company_event_period_binding"}),
    ],
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_full_package_typed_period_mutations_fail_closed(case, mutation, expected_rules):
    package = _materialize(load_json_strict(INPUT_PATH))
    assert validate_package(package) == [], f"accepted fixture precondition failed for {case}"
    mutation(package)
    actual_rules = {row.rule_id for row in validate_package(package)}
    assert expected_rules <= actual_rules, f"{case}: expected {expected_rules}, got {actual_rules}"


def test_accepted_package_has_explicit_valid_typed_period_bindings():
    package = _materialize(load_json_strict(INPUT_PATH))
    observations = _observations_by_id(package)
    periods = {row["period_id"]: row for row in package["periods"]}
    entities = {row["header"]["entity_id"]: row for row in package["entities"]}

    for fact in (row for row in observations.values() if row["payload"].get("kind") == "NumericalFact"):
        assert fact["header"]["fiscal_period_id"] == fact["header"]["effective_period_id"]
    for version in (row for row in observations.values() if row["payload"].get("kind") == "GuidanceVersion"):
        series = entities[version["payload"]["guidance_series_id"]]
        horizon_id = series["payload"]["horizon_period_id"]
        assert version["header"]["effective_period_id"] == horizon_id
        assert version["header"]["fiscal_period_id"] == horizon_id
        assert version["header"]["dimension_set_id"] == series["payload"]["dimension_set_id"]
        assert version["header"]["subject_id"] == series["header"]["entity_id"]
    statement = _typed_period_record(package, "ManagementStatement")
    assert statement["header"]["effective_period_id"] == statement["payload"]["statement_period_id"]
    assert statement["header"]["fiscal_period_id"] == statement["payload"]["statement_period_id"]
    interpretation = _typed_period_record(package, "ModelInterpretation")
    assert interpretation["header"]["effective_period_id"] == interpretation["payload"]["as_of_period_id"]
    assert interpretation["header"]["fiscal_period_id"] == interpretation["payload"]["as_of_period_id"]
    for version in (row for row in observations.values() if row["payload"].get("kind") == "PromiseVersion"):
        assert version["payload"]["deadline"]["kind"] == "period"
        assert version["header"]["effective_period_id"] == version["payload"]["deadline"]["value"]
        assert version["header"]["fiscal_period_id"] == version["payload"]["deadline"]["value"]
    for change in (row for row in observations.values() if row["payload"].get("kind") == "ChangeObservation"):
        later = observations[change["payload"]["to_record_id"]]
        assert change["header"]["effective_period_id"] == later["header"]["effective_period_id"]
        assert change["header"]["fiscal_period_id"] == later["header"]["fiscal_period_id"]
        assert change["header"]["period_type"] == later["header"]["period_type"]
    event = _typed_period_record(package, "CompanyEvent")
    assert event["header"]["fiscal_period_id"] is None
    assert periods[event["header"]["effective_period_id"]]["period_type"] == "month"
    assert event["payload"]["effective_month"] == "2026-03"
    assert validate_package(package) == []
