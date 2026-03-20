from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import xml.etree.ElementTree as ET


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _ns_uri(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[0].strip("{")
    return ""


def _parse_date(s: Any) -> Optional[pd.Timestamp]:
    try:
        return pd.to_datetime(str(s), errors="coerce").date()
    except Exception:
        return None


def _build_contexts(root: ET.Element) -> Dict[str, Dict[str, Any]]:
    ctxs: Dict[str, Dict[str, Any]] = {}
    for ctx in root.findall(".//{*}context"):
        cid = ctx.get("id")
        if not cid:
            continue
        entity = None
        identifier = ctx.find(".//{*}identifier")
        if identifier is not None and identifier.text:
            entity = identifier.text.strip()
        period = ctx.find(".//{*}period")
        start = end = instant = None
        if period is not None:
            st = period.find("{*}startDate")
            en = period.find("{*}endDate")
            inst = period.find("{*}instant")
            if st is not None and st.text:
                start = _parse_date(st.text)
            if en is not None and en.text:
                end = _parse_date(en.text)
            if inst is not None and inst.text:
                instant = _parse_date(inst.text)
        ctxs[cid] = {
            "entity": entity,
            "start": start,
            "end": end,
            "instant": instant,
        }
    return ctxs


def _build_units(root: ET.Element) -> Dict[str, str]:
    units: Dict[str, str] = {}
    for u in root.findall(".//{*}unit"):
        uid = u.get("id")
        if not uid:
            continue
        measures = [m.text.strip() for m in u.findall(".//{*}measure") if m.text]
        units[uid] = "|".join(measures) if measures else ""
    return units


@dataclasses.dataclass(frozen=True)
class InstanceMetadata:
    accession: str
    form: str
    filedDate: Optional[str]
    reportDate: Optional[str]
    primaryDoc: Optional[str] = None


def parse_instance(xml_bytes: bytes, metadata: InstanceMetadata) -> pd.DataFrame:
    """
    Parse XBRL instance XML into a tidy facts table.
    """
    root = ET.fromstring(xml_bytes)
    contexts = _build_contexts(root)
    units = _build_units(root)

    rows: List[Dict[str, Any]] = []
    for el in root.iter():
        if el is root:
            continue
        ctx_ref = el.attrib.get("contextRef")
        if not ctx_ref:
            continue
        if el.attrib.get("{http://www.w3.org/2001/XMLSchema-instance}nil") == "true":
            continue
        val = (el.text or "").strip()
        if val == "":
            continue

        ctx = contexts.get(ctx_ref, {})
        unit_ref = el.attrib.get("unitRef")
        rows.append({
            "fact_name": _local_name(el.tag),
            "namespace_uri": _ns_uri(el.tag),
            "value": val,
            "unit": units.get(unit_ref, "") if unit_ref else "",
            "decimals": el.attrib.get("decimals"),
            "context_ref": ctx_ref,
            "entity_cik": ctx.get("entity"),
            "period_start": ctx.get("start"),
            "period_end": ctx.get("end"),
            "period_instant": ctx.get("instant"),
            "accession": metadata.accession,
            "form": metadata.form,
            "filedDate": metadata.filedDate,
            "reportDate": metadata.reportDate,
            "primaryDoc": metadata.primaryDoc,
        })
    return pd.DataFrame(rows)
