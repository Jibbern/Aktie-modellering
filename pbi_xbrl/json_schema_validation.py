"""Dependency-free JSON Schema validation for checked-in new-ticker contracts.

The project environment intentionally has no third-party ``jsonschema``
dependency.  This module therefore implements the Draft 2020-12 assertion
keywords used by the checked-in contracts and fails closed on unknown assertion
keywords.  Annotation keywords and ``x-*`` contract metadata are ignored.
"""
from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit


SchemaFailure = tuple[str, str, str]

SUPPORTED_SCHEMA_KEYWORDS = frozenset(
    {
        "$schema",
        "$id",
        "$ref",
        "$defs",
        "$comment",
        "title",
        "description",
        "default",
        "examples",
        "type",
        "properties",
        "required",
        "additionalProperties",
        "items",
        "prefixItems",
        "enum",
        "const",
        "pattern",
        "format",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
        "minItems",
        "maxItems",
        "uniqueItems",
        "minLength",
        "maxLength",
        "minProperties",
        "maxProperties",
        "allOf",
        "anyOf",
        "oneOf",
        "not",
    }
)


class DuplicateJsonKeyError(ValueError):
    """Raised when a JSON contract or package contains a duplicate object key."""


def load_json_strict(path: Path | str) -> Any:
    """Load JSON while rejecting duplicate keys instead of accepting the last one."""

    return json.loads(
        Path(path).read_text(encoding="utf-8"),
        object_pairs_hook=_unique_object,
        parse_constant=_reject_json_constant,
    )


def validate_json_schema(instance: Any, schema: Mapping[str, Any]) -> list[SchemaFailure]:
    """Validate an instance against the supported Draft 2020-12 contract subset."""

    failures: list[SchemaFailure] = []
    _validate_node(instance, schema, schema, "$", failures)
    return failures


def schema_keywords(schema: Mapping[str, Any]) -> set[str]:
    """Return schema keywords used in schema positions, excluding property names."""

    result: set[str] = set()

    def walk(node: Any) -> None:
        if not isinstance(node, Mapping):
            return
        for key, value in node.items():
            if key.startswith("$") or key.startswith("x-") or key in SUPPORTED_SCHEMA_KEYWORDS:
                result.add(str(key))
            if key in {"properties", "$defs"} and isinstance(value, Mapping):
                for child in value.values():
                    walk(child)
            elif key in {"items", "not", "additionalProperties"} and isinstance(value, Mapping):
                walk(value)
            elif key in {"prefixItems", "allOf", "anyOf", "oneOf"} and isinstance(value, list):
                for child in value:
                    walk(child)

    walk(schema)
    return result


def _validate_node(
    value: Any,
    schema: Mapping[str, Any],
    root_schema: Mapping[str, Any],
    path: str,
    failures: list[SchemaFailure],
) -> None:
    unsupported = sorted(
        key
        for key in schema
        if key not in SUPPORTED_SCHEMA_KEYWORDS and not str(key).startswith("x-")
    )
    for key in unsupported:
        failures.append((path, "unsupportedKeyword", f"Unsupported JSON Schema keyword {key!r}."))

    ref = schema.get("$ref")
    if isinstance(ref, str):
        resolved = _resolve_ref(root_schema, ref)
        if resolved is None:
            failures.append((path, "ref", f"Unsupported or missing schema reference {ref!r}."))
            return
        _validate_node(value, resolved, root_schema, path, failures)

    for candidate in _schema_list(schema.get("allOf")):
        _validate_node(value, candidate, root_schema, path, failures)

    any_of = _schema_list(schema.get("anyOf"))
    if any_of:
        candidate_results = [_candidate_failures(value, candidate, root_schema, path) for candidate in any_of]
        if all(result for result in candidate_results):
            failures.append((path, "anyOf", "Value does not match any allowed schema variant."))

    one_of = _schema_list(schema.get("oneOf"))
    if one_of:
        matches = sum(not _candidate_failures(value, candidate, root_schema, path) for candidate in one_of)
        if matches != 1:
            failures.append((path, "oneOf", f"Value must match exactly one schema variant; matched {matches}."))

    not_schema = schema.get("not")
    if isinstance(not_schema, Mapping) and not _candidate_failures(value, not_schema, root_schema, path):
        failures.append((path, "not", "Value matches a schema that is explicitly forbidden."))

    expected_type = schema.get("type")
    if expected_type is not None and not _type_matches(value, expected_type):
        failures.append((path, "type", f"Expected {_type_label(expected_type)}, got {type(value).__name__}."))
        return

    if "const" in schema and not _json_equal(value, schema["const"]):
        failures.append((path, "const", f"Value must equal {schema['const']!r}."))

    enum = schema.get("enum")
    if isinstance(enum, list) and not any(_json_equal(value, allowed) for allowed in enum):
        failures.append((path, "enum", f"Value {value!r} is not an allowed enum member."))

    if isinstance(value, str):
        _validate_string(value, schema, path, failures)
    if _is_number(value):
        _validate_number(value, schema, path, failures)
    if isinstance(value, Mapping):
        _validate_object(value, schema, root_schema, path, failures)
    if isinstance(value, list):
        _validate_array(value, schema, root_schema, path, failures)


def _validate_string(value: str, schema: Mapping[str, Any], path: str, failures: list[SchemaFailure]) -> None:
    min_length = schema.get("minLength")
    if isinstance(min_length, int) and len(value) < min_length:
        failures.append((path, "minLength", f"Expected at least {min_length} character(s)."))
    max_length = schema.get("maxLength")
    if isinstance(max_length, int) and len(value) > max_length:
        failures.append((path, "maxLength", f"Expected at most {max_length} character(s)."))
    pattern = schema.get("pattern")
    if isinstance(pattern, str):
        try:
            matched = re.search(pattern, value) is not None
        except re.error as exc:
            failures.append((path, "pattern", f"Invalid schema regex: {exc}."))
        else:
            if not matched:
                failures.append((path, "pattern", "String does not match the required pattern."))
    format_name = schema.get("format")
    if isinstance(format_name, str):
        validator = _FORMAT_VALIDATORS.get(format_name)
        if validator is None:
            failures.append((path, "format", f"Unsupported contract format {format_name!r}."))
        elif not validator(value):
            failures.append((path, "format", f"String is not a valid {format_name}."))


def _validate_number(value: int | float, schema: Mapping[str, Any], path: str, failures: list[SchemaFailure]) -> None:
    limits = (
        ("minimum", lambda current, limit: current >= limit, ">="),
        ("maximum", lambda current, limit: current <= limit, "<="),
        ("exclusiveMinimum", lambda current, limit: current > limit, ">"),
        ("exclusiveMaximum", lambda current, limit: current < limit, "<"),
    )
    for keyword, predicate, operator in limits:
        limit = schema.get(keyword)
        if _is_number(limit) and not predicate(value, limit):
            failures.append((path, keyword, f"Number must be {operator} {limit}."))
    multiple = schema.get("multipleOf")
    if _is_number(multiple):
        if multiple <= 0:
            failures.append((path, "multipleOf", "Schema multipleOf must be greater than zero."))
        else:
            quotient = value / multiple
            if not math.isclose(quotient, round(quotient), rel_tol=1e-12, abs_tol=1e-12):
                failures.append((path, "multipleOf", f"Number must be a multiple of {multiple}."))


def _validate_object(
    value: Mapping[str, Any],
    schema: Mapping[str, Any],
    root_schema: Mapping[str, Any],
    path: str,
    failures: list[SchemaFailure],
) -> None:
    min_properties = schema.get("minProperties")
    if isinstance(min_properties, int) and len(value) < min_properties:
        failures.append((path, "minProperties", f"Expected at least {min_properties} properties."))
    max_properties = schema.get("maxProperties")
    if isinstance(max_properties, int) and len(value) > max_properties:
        failures.append((path, "maxProperties", f"Expected at most {max_properties} properties."))
    properties = schema.get("properties") if isinstance(schema.get("properties"), Mapping) else {}
    required = schema.get("required") if isinstance(schema.get("required"), list) else []
    for key in required:
        if key not in value:
            failures.append((path, "required", f"Required property {key!r} is missing."))
    for key, child_schema in properties.items():
        if key in value and isinstance(child_schema, Mapping):
            _validate_node(value[key], child_schema, root_schema, f"{path}.{key}", failures)
    additional = schema.get("additionalProperties", True)
    if additional is False:
        for key in value:
            if key not in properties:
                failures.append((f"{path}.{key}", "additionalProperties", "Property is not allowed by the schema."))
    elif isinstance(additional, Mapping):
        for key, child_value in value.items():
            if key not in properties:
                _validate_node(child_value, additional, root_schema, f"{path}.{key}", failures)


def _validate_array(
    value: list[Any],
    schema: Mapping[str, Any],
    root_schema: Mapping[str, Any],
    path: str,
    failures: list[SchemaFailure],
) -> None:
    min_items = schema.get("minItems")
    if isinstance(min_items, int) and len(value) < min_items:
        failures.append((path, "minItems", f"Expected at least {min_items} item(s), got {len(value)}."))
    max_items = schema.get("maxItems")
    if isinstance(max_items, int) and len(value) > max_items:
        failures.append((path, "maxItems", f"Expected at most {max_items} item(s), got {len(value)}."))
    if schema.get("uniqueItems") is True:
        seen: set[str] = set()
        for index, item in enumerate(value):
            key = _canonical_json(item)
            if key in seen:
                failures.append((f"{path}.{index}", "uniqueItems", "Array items must be unique."))
            seen.add(key)
    prefix_items = schema.get("prefixItems")
    if isinstance(prefix_items, list):
        for index, child_schema in enumerate(prefix_items[: len(value)]):
            if isinstance(child_schema, Mapping):
                _validate_node(value[index], child_schema, root_schema, f"{path}.{index}", failures)
    items = schema.get("items")
    start = len(prefix_items) if isinstance(prefix_items, list) else 0
    if items is False and len(value) > start:
        failures.append((path, "items", f"Array contains {len(value) - start} disallowed additional item(s)."))
    elif isinstance(items, Mapping):
        for index, item in enumerate(value[start:], start=start):
            _validate_node(item, items, root_schema, f"{path}.{index}", failures)


def _resolve_ref(root_schema: Mapping[str, Any], ref: str) -> Mapping[str, Any] | None:
    if ref == "#":
        return root_schema
    if not ref.startswith("#/"):
        return None
    current: Any = root_schema
    for raw_part in ref[2:].split("/"):
        part = raw_part.replace("~1", "/").replace("~0", "~")
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current if isinstance(current, Mapping) else None


def _candidate_failures(value: Any, schema: Mapping[str, Any], root: Mapping[str, Any], path: str) -> list[SchemaFailure]:
    candidate: list[SchemaFailure] = []
    _validate_node(value, schema, root, path, candidate)
    return candidate


def _schema_list(value: Any) -> list[Mapping[str, Any]]:
    return [item for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []


def _type_matches(value: Any, expected: Any) -> bool:
    kinds = expected if isinstance(expected, list) else [expected]
    return any(
        kind == "object" and isinstance(value, Mapping)
        or kind == "array" and isinstance(value, list)
        or kind == "string" and isinstance(value, str)
        or kind == "boolean" and isinstance(value, bool)
        or kind == "integer" and isinstance(value, int) and not isinstance(value, bool)
        or kind == "number" and _is_number(value)
        or kind == "null" and value is None
        for kind in kinds
    )


def _type_label(expected: Any) -> str:
    return "/".join(str(item) for item in expected) if isinstance(expected, list) else str(expected)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))


def _json_equal(left: Any, right: Any) -> bool:
    return _canonical_json(left) == _canonical_json(right)


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _unique_object(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise DuplicateJsonKeyError(f"Duplicate JSON key {key!r}.")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> Any:
    raise ValueError(f"Non-standard JSON numeric constant {value!r} is not allowed.")


def _valid_date(value: str) -> bool:
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return False
    return parsed.isoformat() == value


def _valid_datetime(value: str) -> bool:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})", value):
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def _valid_uri(value: str) -> bool:
    if not value or re.search(r"\s", value):
        return False
    parsed = urlsplit(value)
    return bool(parsed.scheme and (parsed.netloc or parsed.path))


def _valid_uri_reference(value: str) -> bool:
    return bool(value) and re.search(r"\s", value) is None


_FORMAT_VALIDATORS = {
    "date": _valid_date,
    "date-time": _valid_datetime,
    "uri": _valid_uri,
    "uri-reference": _valid_uri_reference,
}
