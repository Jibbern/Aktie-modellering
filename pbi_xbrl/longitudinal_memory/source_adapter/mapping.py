"""Temporary typed candidate mapping through sector and ticker configuration."""
from __future__ import annotations

from typing import Any, Mapping

from .types import ExtractedEvidence, MappedCandidate, MappingError, SourceSet


def map_candidates(
    source_set: SourceSet,
    evidence: tuple[ExtractedEvidence, ...],
    *,
    sector_pack: Any,
    ticker_profile: Any,
) -> tuple[MappedCandidate, ...]:
    if source_set.sector_pack_id != sector_pack.sector_pack_id:
        raise MappingError(
            f"SourceSet activates {source_set.sector_pack_id!r}, not {sector_pack.sector_pack_id!r}."
        )
    evidence_by_key = {row.assertion_key: row for row in evidence}
    if len(evidence_by_key) != len(evidence):
        raise MappingError("Extracted evidence assertion keys must be unique.")
    assertions = {str(row["assertion_key"]): row for row in source_set.required_assertions}
    result: list[MappedCandidate] = []
    for assertion_key in sorted(assertions):
        assertion = assertions[assertion_key]
        extracted = evidence_by_key.get(assertion_key)
        if extracted is None:
            raise MappingError(f"Required assertion {assertion_key!r} produced no evidence.")
        kind = str(assertion["assertion_kind"])
        source_document = next(
            row for row in source_set.documents if row.document_key == assertion["document_key"]
        )
        if source_document.source_family not in sector_pack.permitted_source_families(kind):
            raise MappingError(
                f"Source family {source_document.source_family!r} is not eligible for {kind!r}."
            )
        value = None
        semantic_key = kind
        period_key: str | None = None
        dimension_alias: str | None = None
        metadata: dict[str, Any] = {"review_state": assertion["review_state"]}
        if kind == "numerical_fact":
            semantic_key = str(assertion["metric_key"])
            sector_pack.metric_semantics(semantic_key)
            period_key = str(assertion["period_key"])
            dimension_alias = str(assertion["dimension_alias"])
            expected_member = ticker_profile.member_id(dimension_alias)
            row_fingerprint = assertion["locator"].get("row_header_fingerprint")
            observed_member = (
                ticker_profile.evidence_member_id(str(row_fingerprint))
                if row_fingerprint is not None
                else None
            )
            total_member = ticker_profile.member_id(sector_pack.total_dimension_alias)
            if observed_member is not None and observed_member != expected_member:
                raise MappingError(
                    f"Evidence row member disagrees with dimension alias {dimension_alias!r}."
                )
            if expected_member != total_member and observed_member is None:
                raise MappingError(
                    f"Dimension alias {dimension_alias!r} is not present in its evidence row."
                )
            if extracted.value_text is None:
                raise MappingError(f"Numerical assertion {assertion_key!r} has no value text.")
            value = sector_pack.parse_value(str(assertion["value_parser_id"]), extracted.value_text)
        elif kind == "guidance":
            semantic_key = str(assertion["metric_key"])
            period_key = str(assertion["horizon_period_key"])
            dimension_alias = sector_pack.total_dimension_alias
            if extracted.value_text is None:
                raise MappingError(f"Guidance assertion {assertion_key!r} has no value text.")
            value = sector_pack.parse_value(str(assertion["value_parser_id"]), extracted.value_text)
            previous_value = None
            replacement_evidence = assertion["replacement_evidence_kind"]
            link_key = assertion["required_reviewed_link_key"]
            document = source_document
            if document.source_family == "issuer-transcript" and link_key is None:
                raise MappingError(
                    "Transcript guidance requires an explicit reviewed same-event link."
                )
            if link_key is not None:
                link = ticker_profile.reviewed_link(str(link_key))
                if link.get("relation_type") != "same-event" or link.get(
                    "from_document_key"
                ) != assertion["document_key"]:
                    raise MappingError(
                        f"Guidance link {link_key!r} does not establish its reviewed source event."
                    )
            if assertion["supersedes_assertion_key"] is not None:
                if assertion["version_kind"] != "replacement" or replacement_evidence is None:
                    raise MappingError(
                        "Supersession requires a replacement version and explicit replacement evidence."
                    )
                if extracted.comparison_text is None:
                    raise MappingError(f"Replacement guidance {assertion_key!r} lacks a previous value column.")
                if replacement_evidence == "explicit-replaces-wording" and (
                    "replaces all previous" not in extracted.excerpt.casefold()
                ):
                    raise MappingError(
                        f"Replacement guidance {assertion_key!r} lacks explicit replacement wording."
                    )
                if replacement_evidence == "current-previous-columns" and (
                    "current" not in str(assertion["locator"]["column_header_fingerprint"]).casefold()
                    or "previous:" not in extracted.excerpt.casefold()
                ):
                    raise MappingError(
                        f"Replacement guidance {assertion_key!r} lacks reproducible current/previous columns."
                    )
                previous_value = sector_pack.parse_value(
                    str(assertion["value_parser_id"]), extracted.comparison_text
                )
            elif replacement_evidence is not None or assertion["version_kind"] == "replacement":
                raise MappingError(
                    "Replacement guidance requires one explicit predecessor; chronology is insufficient."
                )
            metadata.update(
                {
                    "version_kind": assertion["version_kind"],
                    "supersedes_assertion_key": assertion["supersedes_assertion_key"],
                    "previous_value": previous_value,
                    "required_reviewed_link_key": link_key,
                    "replacement_evidence_kind": replacement_evidence,
                }
            )
        elif kind == "promise_version":
            semantic_key = str(assertion["promise_subject_id"])
            period_key = str(assertion["deadline_period_key"])
            dimension_alias = str(assertion["dimension_alias"])
            expected_metric, expected_definition, expected_basis, _unit_id = (
                sector_pack.metric_semantics(semantic_key, guidance=True)
            )
            declared_target = (
                str(assertion["target_metric_id"]),
                str(assertion["target_definition_id"]),
                str(assertion["target_basis_id"]),
            )
            if declared_target != (expected_metric, expected_definition, expected_basis):
                raise MappingError(
                    f"Promise assertion {assertion_key!r} has incompatible target economics."
                )
            if ticker_profile.member_id(dimension_alias) != ticker_profile.member_id(
                sector_pack.total_dimension_alias
            ):
                raise MappingError(
                    f"Promise assertion {assertion_key!r} has an unsupported dimensional scope."
                )
            if extracted.value_text is None:
                raise MappingError(f"Promise assertion {assertion_key!r} has no source wording.")
            value = sector_pack.parse_value(str(assertion["value_parser_id"]), extracted.value_text)
            metadata.update(
                {
                    "promise_subject_id": assertion["promise_subject_id"],
                    "program_id": assertion["program_id"],
                    "target_metric_id": assertion["target_metric_id"],
                    "target_definition_id": assertion["target_definition_id"],
                    "target_basis_id": assertion["target_basis_id"],
                    "target_dimension_alias": assertion["dimension_alias"],
                    "change_kind": assertion["change_kind"],
                    "version_state": assertion["version_state"],
                    "previous_assertion_key": assertion["previous_assertion_key"],
                }
            )
        elif kind == "management_statement":
            semantic_key = str(assertion["topic_id"])
            period_key = str(assertion["statement_period_key"])
            dimension_alias = sector_pack.total_dimension_alias
            metadata.update(
                {
                    "statement_kind": assertion["statement_kind"],
                    "topic_id": assertion["topic_id"],
                    "speaker_id": assertion["speaker_id"],
                }
            )
        elif kind == "company_event":
            semantic_key = str(assertion["event_subject_id"])
            period_key = str(assertion["effective_period_key"])
            dimension_alias = sector_pack.total_dimension_alias
            link_key = assertion["required_reviewed_link_key"]
            document = source_document
            if (
                document.source_family == "issuer-transcript"
                and assertion["effective_precision"] == "month"
                and link_key is None
            ):
                raise MappingError(
                    "A transcript-relative month event requires an explicit reviewed date link."
                )
            if link_key is not None:
                link = ticker_profile.reviewed_link(str(link_key))
                if (
                    link.get("relation_type") != "event-date-support"
                    or link.get("from_document_key") != assertion["document_key"]
                ):
                    raise MappingError(f"Event link {link_key!r} does not support its source document.")
            metadata.update(
                {
                    "event_type": assertion["event_type"],
                    "event_subject_id": assertion["event_subject_id"],
                    "event_stage": assertion["event_stage"],
                    "effective_precision": assertion["effective_precision"],
                    "required_reviewed_link_key": link_key,
                }
            )
        elif kind == "period_evidence":
            semantic_key = str(assertion["period_key"])
            period_key = str(assertion["period_key"])
        else:  # pragma: no cover - closed schema guards this
            raise MappingError(f"Unsupported candidate kind {kind!r}.")
        result.append(
            MappedCandidate(
                assertion_key=assertion_key,
                candidate_kind=kind,
                document_key=str(assertion["document_key"]),
                evidence=extracted,
                period_key=period_key,
                semantic_key=semantic_key,
                dimension_alias=dimension_alias,
                value=value,
                metadata=metadata,
            )
        )
    return tuple(sorted(result, key=lambda row: row.assertion_key))
