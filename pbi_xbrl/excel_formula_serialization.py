"""Deterministic Excel/OOXML formula serialization for the new-ticker engine.

Formula contracts use invariant English Excel syntax.  This module is the only
boundary that converts those logical formulas to the namespaced spelling stored
in XLSX packages.  The registry is deliberately bounded so a newly introduced
function cannot silently reach desktop Excel with ambiguous compatibility.
"""
from __future__ import annotations

import posixpath
import re
import zipfile
import xml.etree.ElementTree as ET
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from openpyxl.formula import Tokenizer


FORMULA_SERIALIZATION_CONTRACT_VERSION = "1.0.0"

ORDINARY_FUNCTIONS = frozenset(
    {
        "ABS",
        "AND",
        "COUNTA",
        "COUNTIF",
        "COUNTIFS",
        "IF",
        "IFERROR",
        "INDEX",
        "INT",
        "ISNUMBER",
        "LEFT",
        "LEN",
        "LOOKUP",
        "MATCH",
        "MAX",
        "MID",
        "MIN",
        "MOD",
        "NOT",
        "OR",
        "RIGHT",
        "ROUND",
        "SUMIFS",
        "SUMPRODUCT",
        "TEXT",
        "VALUE",
    }
)

FUTURE_FUNCTION_PREFIXES: Mapping[str, str] = {
    "MAXIFS": "_xlfn.",
    "MINIFS": "_xlfn.",
    "LET": "_xlfn.",
}

_FUNCTION_PREFIXES = ("_xlfn.", "_xlws.")
_LET_LOCAL_PREFIX = "_xlpm."
_LET_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")
_SPREADSHEET_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_DOCUMENT_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_PACKAGE_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"


class FormulaSerializationError(ValueError):
    """Raised when a formula cannot be serialized under the bounded contract."""


@dataclass(frozen=True)
class _LetLocal:
    name: str
    declaration_index: int
    available_from: int


@dataclass(frozen=True)
class _LetScope:
    start: int
    end: int
    locals: tuple[_LetLocal, ...]


def _function_name(token_value: str) -> tuple[str, str]:
    value = token_value[:-1] if token_value.endswith("(") else token_value
    lower = value.lower()
    for prefix in _FUNCTION_PREFIXES:
        if lower.startswith(prefix):
            return value[len(prefix) :].upper(), prefix
    return value.upper(), ""


def _tokenize(expression: str) -> tuple[list[Any], bool]:
    has_equals = expression.startswith("=")
    candidate = expression if has_equals else f"={expression}"
    try:
        return list(Tokenizer(candidate).items), has_equals
    except Exception as exc:
        raise FormulaSerializationError(f"Formula tokenization failed: {expression!r}: {exc}") from exc


def _function_arguments(tokens: list[Any], open_index: int) -> tuple[int, tuple[tuple[int, int], ...]]:
    depth = 1
    argument_start = open_index + 1
    arguments: list[tuple[int, int]] = []
    for index in range(open_index + 1, len(tokens)):
        token = tokens[index]
        if token.type in {"FUNC", "PAREN"} and token.subtype == "OPEN":
            depth += 1
            continue
        if token.type in {"FUNC", "PAREN"} and token.subtype == "CLOSE":
            depth -= 1
            if depth == 0:
                arguments.append((argument_start, index))
                return index, tuple(arguments)
            continue
        if token.type == "SEP" and token.subtype == "ARG" and depth == 1:
            arguments.append((argument_start, index))
            argument_start = index + 1
    raise FormulaSerializationError("Formula contains an unterminated function call.")


def _let_scopes(tokens: list[Any]) -> tuple[_LetScope, ...]:
    scopes: list[_LetScope] = []
    for open_index, token in enumerate(tokens):
        if token.type != "FUNC" or token.subtype != "OPEN":
            continue
        function_name, _ = _function_name(str(token.value))
        if function_name != "LET":
            continue
        end_index, arguments = _function_arguments(tokens, open_index)
        if len(arguments) < 3 or len(arguments) % 2 == 0:
            raise FormulaSerializationError("LET requires name/value pairs followed by one calculation expression.")
        locals_: list[_LetLocal] = []
        seen: set[str] = set()
        for argument_index in range(0, len(arguments) - 1, 2):
            start, end = arguments[argument_index]
            meaningful = [index for index in range(start, end) if tokens[index].type != "WSPACE"]
            if len(meaningful) != 1:
                raise FormulaSerializationError("Each LET local declaration must be one bounded identifier.")
            declaration_index = meaningful[0]
            declaration = tokens[declaration_index]
            if declaration.type != "OPERAND" or declaration.subtype != "RANGE":
                raise FormulaSerializationError("Each LET local declaration must be a range-token identifier.")
            raw_name = str(declaration.value)
            name = raw_name[len(_LET_LOCAL_PREFIX) :] if raw_name.lower().startswith(_LET_LOCAL_PREFIX) else raw_name
            if not _LET_NAME_RE.fullmatch(name):
                raise FormulaSerializationError(f"Invalid LET local identifier: {raw_name!r}.")
            key = name.casefold()
            if key in seen:
                raise FormulaSerializationError(f"Duplicate LET local identifier: {name!r}.")
            seen.add(key)
            available_from = arguments[argument_index + 2][0]
            locals_.append(_LetLocal(name=name, declaration_index=declaration_index, available_from=available_from))
        scopes.append(_LetScope(start=open_index, end=end_index, locals=tuple(locals_)))
    return tuple(scopes)


def serialize_formula_expression(expression: str) -> str:
    """Serialize one logical formula expression to exact OOXML spelling."""

    if not isinstance(expression, str) or not expression:
        return expression
    tokens, has_equals = _tokenize(expression)
    scopes = _let_scopes(tokens)
    declarations = {
        local.declaration_index: local.name
        for scope in scopes
        for local in scope.locals
    }

    for index, name in declarations.items():
        tokens[index].value = f"{_LET_LOCAL_PREFIX}{name}"

    for index, token in enumerate(tokens):
        if token.type != "OPERAND" or token.subtype != "RANGE" or index in declarations:
            continue
        token_value = str(token.value)
        prefixed = token_value.lower().startswith(_LET_LOCAL_PREFIX)
        candidate = token_value[len(_LET_LOCAL_PREFIX) :] if prefixed else token_value
        matching = [
            (scope, local)
            for scope in scopes
            for local in scope.locals
            if scope.start < index < scope.end
            and index >= local.available_from
            and candidate.casefold() == local.name.casefold()
        ]
        if matching:
            _, local = min(matching, key=lambda row: row[0].end - row[0].start)
            token.value = f"{_LET_LOCAL_PREFIX}{local.name}"
        elif prefixed:
            raise FormulaSerializationError(f"LET-local reference {token_value!r} is outside its valid scope.")

    for token in tokens:
        if token.type != "FUNC" or token.subtype != "OPEN":
            continue
        name, prefix = _function_name(str(token.value))
        if name in FUTURE_FUNCTION_PREFIXES:
            expected = FUTURE_FUNCTION_PREFIXES[name]
            if prefix and prefix.lower() != expected:
                raise FormulaSerializationError(
                    f"Future function {name} uses incompatible prefix {prefix!r}; expected {expected!r}."
                )
            token.value = f"{expected}{name}("
        elif name in ORDINARY_FUNCTIONS:
            if prefix:
                raise FormulaSerializationError(f"Ordinary function {name} must not use future-function prefix {prefix!r}.")
            token.value = f"{name}("
        else:
            raise FormulaSerializationError(f"Unsupported formula function: {name!r}.")

    serialized = "".join(str(token.value) for token in tokens)
    return f"={serialized}" if has_equals else serialized


def _iter_workbook_formula_expressions(workbook: Any) -> Iterable[tuple[str, str, Any, str]]:
    for ws in workbook.worksheets:
        for cell in tuple(ws._cells.values()):
            value = cell.value
            if isinstance(value, str) and value.startswith("="):
                yield "cell", f"{ws.title}!{cell.coordinate}", cell, "value"
        for index, validation in enumerate(ws.data_validations.dataValidation, start=1):
            for attribute in ("formula1", "formula2"):
                value = getattr(validation, attribute, None)
                if value not in (None, ""):
                    yield "data_validation", f"{ws.title}!validation[{index}].{attribute}", validation, attribute
        for conditional_range in ws.conditional_formatting:
            for rule_index, rule in enumerate(ws.conditional_formatting[conditional_range], start=1):
                for formula_index, value in enumerate(rule.formula or []):
                    if value not in (None, ""):
                        yield (
                            "conditional_formatting",
                            f"{ws.title}!{conditional_range}.rule[{rule_index}].formula[{formula_index}]",
                            rule.formula,
                            str(formula_index),
                        )
    for name in workbook.defined_names:
        defined_name = workbook.defined_names[name]
        value = getattr(defined_name, "attr_text", None)
        if value not in (None, ""):
            yield "defined_name", f"defined_name:{name}", defined_name, "attr_text"


def _expression_value(owner: Any, attribute: str) -> str:
    if isinstance(owner, list):
        return str(owner[int(attribute)])
    return str(getattr(owner, attribute))


def _set_expression_value(owner: Any, attribute: str, value: str) -> None:
    if isinstance(owner, list):
        owner[int(attribute)] = value
    else:
        setattr(owner, attribute, value)


def serialize_workbook_formulas_for_ooxml(workbook: Any) -> dict[str, Any]:
    """Serialize every formula-bearing workbook surface immediately before save."""

    counts: Counter[str] = Counter()
    for surface, location, owner, attribute in _iter_workbook_formula_expressions(workbook):
        raw = _expression_value(owner, attribute)
        try:
            serialized = serialize_formula_expression(raw)
        except FormulaSerializationError as exc:
            raise FormulaSerializationError(f"{location}: {exc}") from exc
        _set_expression_value(owner, attribute, serialized)
        counts[surface] += 1
    return {"contract_version": FORMULA_SERIALIZATION_CONTRACT_VERSION, "surface_counts": dict(sorted(counts.items()))}


def _inspect_expression(expression: str) -> dict[str, Any]:
    tokens, _ = _tokenize(expression)
    function_counts: Counter[str] = Counter()
    unprefixed_future: Counter[str] = Counter()
    unsupported: Counter[str] = Counter()
    local_occurrences = 0
    for token in tokens:
        if token.type == "FUNC" and token.subtype == "OPEN":
            name, prefix = _function_name(str(token.value))
            function_counts[name] += 1
            if name in FUTURE_FUNCTION_PREFIXES:
                if prefix.lower() != FUTURE_FUNCTION_PREFIXES[name]:
                    unprefixed_future[name] += 1
            elif name not in ORDINARY_FUNCTIONS:
                unsupported[name] += 1
            elif prefix:
                unsupported[f"{prefix}{name}"] += 1
        if token.type == "OPERAND" and token.subtype == "RANGE" and str(token.value).lower().startswith(_LET_LOCAL_PREFIX):
            local_occurrences += 1
    malformed = ""
    try:
        serialized = serialize_formula_expression(expression)
        if serialized != expression:
            malformed = "Formula is not in canonical serialized OOXML form."
    except FormulaSerializationError as exc:
        malformed = str(exc)
    return {
        "function_counts": function_counts,
        "unprefixed_future": unprefixed_future,
        "unsupported": unsupported,
        "let_local_occurrences": local_occurrences,
        "malformed": malformed,
    }


def inventory_workbook_formulas(workbook: Any) -> dict[str, Any]:
    """Inventory all formula-bearing surfaces in an open workbook."""

    surface_counts: Counter[str] = Counter()
    function_counts: Counter[str] = Counter()
    unprefixed: Counter[str] = Counter()
    unsupported: Counter[str] = Counter()
    malformed: list[dict[str, str]] = []
    local_occurrences = 0
    future_cell_locations: list[str] = []
    for surface, location, owner, attribute in _iter_workbook_formula_expressions(workbook):
        expression = _expression_value(owner, attribute)
        inspection = _inspect_expression(expression)
        surface_counts[surface] += 1
        function_counts.update(inspection["function_counts"])
        unprefixed.update(inspection["unprefixed_future"])
        unsupported.update(inspection["unsupported"])
        local_occurrences += int(inspection["let_local_occurrences"])
        if inspection["malformed"]:
            malformed.append({"location": location, "message": str(inspection["malformed"])})
        if surface == "cell" and any(name in inspection["function_counts"] for name in FUTURE_FUNCTION_PREFIXES):
            future_cell_locations.append(location)
    return {
        "contract_version": FORMULA_SERIALIZATION_CONTRACT_VERSION,
        "surface_counts": dict(sorted(surface_counts.items())),
        "function_counts": dict(sorted(function_counts.items())),
        "unprefixed_future_functions": dict(sorted(unprefixed.items())),
        "unsupported_functions": dict(sorted(unsupported.items())),
        "let_local_occurrences": local_occurrences,
        "future_function_cell_count": len(future_cell_locations),
        "future_function_cells": sorted(future_cell_locations),
        "malformed_expressions": malformed,
    }


def validate_workbook_formula_compatibility(workbook: Any) -> list[dict[str, str]]:
    inventory = inventory_workbook_formulas(workbook)
    issues: list[dict[str, str]] = []
    if inventory["unprefixed_future_functions"]:
        issues.append(
            {
                "rule_id": "formula_future_function_unprefixed",
                "message": f"Unprefixed future functions remain: {inventory['unprefixed_future_functions']!r}.",
            }
        )
    if inventory["unsupported_functions"]:
        issues.append(
            {
                "rule_id": "formula_function_unsupported",
                "message": f"Unsupported functions remain: {inventory['unsupported_functions']!r}.",
            }
        )
    for row in inventory["malformed_expressions"][:20]:
        issues.append(
            {
                "rule_id": "formula_serialization_not_canonical",
                "message": f"{row['location']}: {row['message']}",
            }
        )
    return issues


def _worksheet_parts(package: zipfile.ZipFile) -> dict[str, str]:
    workbook_root = ET.fromstring(package.read("xl/workbook.xml"))
    relationships_root = ET.fromstring(package.read("xl/_rels/workbook.xml.rels"))
    targets = {
        str(row.attrib["Id"]): str(row.attrib["Target"])
        for row in relationships_root.findall(f"{{{_PACKAGE_REL_NS}}}Relationship")
    }
    result: dict[str, str] = {}
    for sheet in workbook_root.findall(f".//{{{_SPREADSHEET_NS}}}sheet"):
        relationship_id = str(sheet.attrib.get(f"{{{_DOCUMENT_REL_NS}}}id") or "")
        target = targets.get(relationship_id, "")
        if not target:
            continue
        package_target = target.lstrip("/")
        normalized = (
            posixpath.normpath(package_target)
            if package_target.startswith("xl/")
            else posixpath.normpath(posixpath.join("xl", package_target))
        )
        result[str(sheet.attrib.get("name") or "")] = normalized
    return result


def inventory_xlsx_formula_xml(path: Path | str) -> dict[str, Any]:
    """Inspect actual worksheet ``<f>`` XML rather than openpyxl cell values."""

    workbook_path = Path(path)
    function_counts: Counter[str] = Counter()
    unprefixed: Counter[str] = Counter()
    unsupported: Counter[str] = Counter()
    cell_locations: list[str] = []
    future_locations: list[str] = []
    let_locations: list[str] = []
    local_occurrences = 0
    malformed: list[dict[str, str]] = []
    with zipfile.ZipFile(workbook_path, "r") as package:
        for sheet_name, part_name in _worksheet_parts(package).items():
            root = ET.fromstring(package.read(part_name))
            for cell in root.findall(f".//{{{_SPREADSHEET_NS}}}c"):
                formula = cell.find(f"{{{_SPREADSHEET_NS}}}f")
                if formula is None:
                    continue
                coordinate = str(cell.attrib.get("r") or "")
                location = f"{sheet_name}!{coordinate}"
                expression = f"={formula.text or ''}"
                inspection = _inspect_expression(expression)
                cell_locations.append(location)
                function_counts.update(inspection["function_counts"])
                unprefixed.update(inspection["unprefixed_future"])
                unsupported.update(inspection["unsupported"])
                local_occurrences += int(inspection["let_local_occurrences"])
                if any(name in inspection["function_counts"] for name in FUTURE_FUNCTION_PREFIXES):
                    future_locations.append(location)
                if inspection["function_counts"].get("LET"):
                    let_locations.append(location)
                if inspection["malformed"]:
                    malformed.append({"location": location, "message": str(inspection["malformed"])})
    return {
        "contract_version": FORMULA_SERIALIZATION_CONTRACT_VERSION,
        "cell_formula_count": len(cell_locations),
        "function_counts": dict(sorted(function_counts.items())),
        "unprefixed_future_functions": dict(sorted(unprefixed.items())),
        "unsupported_functions": dict(sorted(unsupported.items())),
        "let_local_occurrences": local_occurrences,
        "future_function_cell_count": len(future_locations),
        "future_function_cells": sorted(future_locations),
        "let_cells": sorted(let_locations),
        "malformed_expressions": malformed,
    }


def validate_xlsx_formula_compatibility(path: Path | str) -> list[dict[str, str]]:
    inventory = inventory_xlsx_formula_xml(path)
    issues: list[dict[str, str]] = []
    if inventory["unprefixed_future_functions"]:
        issues.append(
            {
                "rule_id": "formula_xml_future_function_unprefixed",
                "message": f"Worksheet XML contains unprefixed future functions: {inventory['unprefixed_future_functions']!r}.",
            }
        )
    if inventory["unsupported_functions"]:
        issues.append(
            {
                "rule_id": "formula_xml_function_unsupported",
                "message": f"Worksheet XML contains unsupported functions: {inventory['unsupported_functions']!r}.",
            }
        )
    for row in inventory["malformed_expressions"][:20]:
        issues.append(
            {
                "rule_id": "formula_xml_serialization_not_canonical",
                "message": f"{row['location']}: {row['message']}",
            }
        )
    return issues
