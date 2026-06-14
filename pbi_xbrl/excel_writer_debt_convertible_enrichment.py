"""Debt / convertible enrichment support for workbook writer inputs."""

from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd


@dataclass(frozen=True)
class DebtConvertibleEnrichmentDeps:
    ticker: str
    cache_dir: Any
    ticker_roots: Sequence[Any]
    document_cache: Any
    context_helpers: Mapping[str, Any]


class DebtConvertibleEnrichmentSupport:
    def __init__(self, deps: DebtConvertibleEnrichmentDeps) -> None:
        self.deps = deps

    def _helper(self, name: str) -> Any:
        return self.deps.context_helpers[name]

    def _normalize_accn_local(self, val: Any) -> str:
        helper = self.deps.context_helpers.get("_normalize_accn_local")
        if helper is not None:
            return str(helper(val))
        return re.sub(r"[^0-9]", "", str(val or ""))

    def _sec_cache_roots_local(self) -> List[Path]:
        return list(self._helper("_sec_cache_roots_local")())

    def _sec_cache_doc_paths_local(self, root: Path) -> List[Path]:
        return list(self._helper("_sec_cache_doc_paths_local")(root))

    def _path_belongs_to_ticker(self, path_in: Any) -> bool:
        return bool(
            self._helper("_path_belongs_to_ticker")(
                path_in,
                self.deps.ticker,
                self.deps.ticker_roots,
            )
        )

    def _normalize_text(self, value: Any) -> str:
        return str(self._helper("glx_normalize_text")(value))

    def _coerce_number(self, value: Any) -> Any:
        return self._helper("coerce_number")(value)

    def find_latest_xbrl_docs(self, accns: Any = None) -> List[Path]:
        roots = self._sec_cache_roots_local()
        accn_keys = {
            self._normalize_accn_local(x)
            for x in list(accns or [])
            if self._normalize_accn_local(x)
        }
        out: List[Path] = []
        seen_local: set[str] = set()

        def _add_doc_candidate(raw_path: Any) -> None:
            if not raw_path:
                return
            try:
                path_in = Path(str(raw_path))
            except Exception:
                return
            if not path_in.exists() or not path_in.is_file():
                return
            if path_in.suffix.lower() not in {".xml", ".htm", ".html"}:
                return
            sp = str(path_in)
            if sp in seen_local:
                return
            seen_local.add(sp)
            out.append(path_in)

        def _inline_xbrl_html_candidates(root: Path, accn_key: str) -> List[Path]:
            candidates: List[Path] = []
            idx_path = root / f"index_{accn_key}.json"
            item_names: List[str] = []
            if idx_path.exists():
                try:
                    idx_payload = json.loads(idx_path.read_text(encoding="utf-8", errors="ignore"))
                except Exception:
                    idx_payload = {}
                for item in list(((idx_payload.get("directory") or {}).get("item")) or []):
                    name_txt = str((item or {}).get("name") or "").strip()
                    if name_txt:
                        item_names.append(name_txt)
            preferred_html_names: List[str] = []
            for name_txt in item_names:
                if str(name_txt).lower().endswith("_htm.xml"):
                    preferred_html_names.append(re.sub(r"_htm\.xml$", ".htm", str(name_txt), flags=re.I))
            if not preferred_html_names:
                for name_txt in item_names:
                    low = str(name_txt).lower()
                    if not low.endswith((".htm", ".html")):
                        continue
                    if "index" in low or low.endswith(".txt"):
                        continue
                    if re.fullmatch(r"r\d+\.htm", low):
                        continue
                    if re.match(r"ex\d", low):
                        continue
                    preferred_html_names.append(str(name_txt))
            for name_txt in preferred_html_names:
                for cand in sorted(root.glob(f"doc_{accn_key}_{name_txt}")):
                    candidates.append(cand)
                for cand in sorted(root.glob(f"*/{accn_key}/docs/{name_txt}")):
                    candidates.append(cand)
                for cand in sorted(root.glob(f"{accn_key}/docs/{name_txt}")):
                    candidates.append(cand)
                for cand in sorted(root.glob(f"*/{accn_key}/xbrl/{name_txt}")):
                    candidates.append(cand)
                for cand in sorted(root.glob(f"{accn_key}/xbrl/{name_txt}")):
                    candidates.append(cand)
            if not candidates:
                for cand in sorted(root.glob(f"doc_{accn_key}_*.htm")):
                    low = cand.name.lower()
                    if "index" in low or re.search(r"_r\d+\.htm$", low):
                        continue
                    candidates.append(cand)
                for cand in sorted(root.glob(f"doc_{accn_key}_*.html")):
                    low = cand.name.lower()
                    if "index" in low or re.search(r"_r\d+\.html$", low):
                        continue
                    candidates.append(cand)
            uniq_inline: List[Path] = []
            seen_inline: set[str] = set()
            for cand in candidates:
                sp = str(cand)
                if sp in seen_inline:
                    continue
                seen_inline.add(sp)
                uniq_inline.append(cand)
            return uniq_inline

        for root in roots:
            if accn_keys:
                for accn_key in accn_keys:
                    for cand in sorted(root.glob(f"*/{accn_key}/xbrl/*_htm.xml")):
                        _add_doc_candidate(cand)
                    for cand in sorted(root.glob(f"{accn_key}/xbrl/*_htm.xml")):
                        _add_doc_candidate(cand)
                    if not out:
                        for cand in _inline_xbrl_html_candidates(root, accn_key):
                            _add_doc_candidate(cand)
            if out:
                break
        if not out:
            for root in roots:
                for cand in sorted(
                    p
                    for p in root.rglob("*_htm.xml")
                    if self._path_belongs_to_ticker(p)
                ):
                    _add_doc_candidate(cand)
                if out:
                    break
        seen_paths: set[str] = set()
        uniq: List[Path] = []
        for p in sorted(out, key=lambda z: (z.stat().st_mtime if z.exists() else 0), reverse=True):
            sp = str(p)
            if sp in seen_paths:
                continue
            seen_paths.add(sp)
            uniq.append(p)
        return uniq[:6]

    def latest_sec_text_docs_for_convertibles(
        self,
        accns: Any = None,
        max_docs: int = 80,
    ) -> List[Path]:
        accn_keys = {
            self._normalize_accn_local(x)
            for x in list(accns or [])
            if self._normalize_accn_local(x)
        }
        doc_paths: List[Path] = []
        seen: set[str] = set()

        def _add_path(raw_path: Any) -> None:
            if not raw_path:
                return
            try:
                p = Path(str(raw_path))
            except Exception:
                return
            if not p.exists() or not p.is_file():
                return
            if p.suffix.lower() not in {".htm", ".html", ".txt"}:
                return
            if not self._path_belongs_to_ticker(p):
                return
            sp = str(p)
            if sp in seen:
                return
            seen.add(sp)
            doc_paths.append(p)

        for root in self._sec_cache_roots_local():
            idx_path = root / "sec_index" / "files.csv"
            if not idx_path.exists():
                continue
            try:
                idx_df = pd.read_csv(idx_path)
            except Exception:
                continue
            if idx_df.empty or "local_path" not in idx_df.columns:
                continue
            local = idx_df.copy()
            local["ext"] = local["local_path"].astype(str).str.extract(r"(\.[A-Za-z0-9]+)$", expand=False).str.lower()
            local = local[local["ext"].isin({".htm", ".html", ".txt"})]
            if "form" in local.columns:
                local = local[local["form"].astype(str).str.upper().isin({"8-K", "8-K/A", "10-K", "10-Q"})]
            if accn_keys and "accession" in local.columns:
                accn_col = local["accession"].astype(str).str.replace(r"[^0-9]", "", regex=True)
                local = local[accn_col.isin(accn_keys) | local["form"].astype(str).str.upper().isin({"8-K", "8-K/A"})]
            if "filedDate" in local.columns:
                local["filedDate"] = pd.to_datetime(local["filedDate"], errors="coerce")
                local = local.sort_values("filedDate", ascending=False)
            for _, rec in local.head(max_docs).iterrows():
                _add_path(rec.get("local_path"))
        if doc_paths:
            return doc_paths[:max_docs]
        for root in self._sec_cache_roots_local():
            for p in self._sec_cache_doc_paths_local(root):
                if p.suffix.lower() not in {".htm", ".html"}:
                    continue
                _add_path(p)
                if len(doc_paths) >= max_docs:
                    return doc_paths
        return doc_paths[:max_docs]

    def _htmlish_to_text(self, txt_in: str) -> str:
        txt = html.unescape(str(txt_in or ""))
        txt = re.sub(r"<br\s*/?>", "\n", txt, flags=re.I)
        txt = re.sub(r"</?(?:div|p|tr|li|td|th|table|span|font|b|strong|em|u)[^>]*>", " ", txt, flags=re.I)
        txt = re.sub(r"<[^>]+>", " ", txt)
        txt = txt.replace("\xa0", " ")
        return self._normalize_text(txt)

    def _safe_text_value(self, v: Any) -> str:
        try:
            if pd.isna(v):
                return ""
        except Exception:
            pass
        return str(v or "").strip()

    def extract_convertible_capital_actions(self, accns: Any = None) -> List[Dict[str, Any]]:
        return self._extract_convertible_capital_actions(accns=accns)

    def _extract_convertible_capital_actions(self, accns: Any = None) -> List[Dict[str, Any]]:
        def _scan_docs(doc_paths: List[Path]) -> List[Dict[str, Any]]:
            rows_local: List[Dict[str, Any]] = []
            seen_paths_local: set[str] = set()
            for path in doc_paths:
                spath = str(path)
                if spath in seen_paths_local:
                    continue
                seen_paths_local.add(spath)
                try:
                    raw_text = path.read_text(encoding="utf-8", errors="ignore")
                except Exception:
                    continue
                txt = self._htmlish_to_text(raw_text)
                low = txt.lower()
                if "convert" not in low and "subscription transactions" not in low and "exchange transactions" not in low:
                    continue
                rec: Dict[str, Any] = {
                    "path": spath,
                    "text": txt,
                    "instrument_hint": "",
                    "concurrent_repurchase_amount": None,
                    "concurrent_repurchase_shares": None,
                    "hedge_or_call_spread": "",
                    "settlement_type": "",
                    "conversion_conditions_note": "",
                }
                if re.search(r"\b2032\b", low):
                    rec["instrument_hint"] = "2032"
                elif re.search(r"\b(1\.50%|1\.5%)\b", low):
                    rec["instrument_hint"] = "1.5"
                elif re.search(r"\b(2030|5\.25%)\b", low):
                    rec["instrument_hint"] = "2030"
                elif re.search(r"\b(2027|2\.25%)\b", low):
                    rec["instrument_hint"] = "2027"

                rep_m = re.search(
                    r"used approximately \$\s*([0-9][0-9,]*(?:\.\d+)?)\s*(million|billion|m|bn)?[^.]{0,240}?(?:to\s+)?repurchase(?: approximately)?\s*([0-9][0-9,]*(?:\.\d+)?)\s*(million|billion|m|bn)?\s+shares",
                    txt,
                    re.I,
                )
                if rep_m:
                    try:
                        amt = float(str(rep_m.group(1)).replace(",", ""))
                        amt_unit = str(rep_m.group(2) or "").lower()
                        if amt_unit in {"million", "m"}:
                            amt *= 1e6
                        elif amt_unit in {"billion", "bn"}:
                            amt *= 1e9
                        shares = float(str(rep_m.group(3)).replace(",", ""))
                        sh_unit = str(rep_m.group(4) or "").lower()
                        if sh_unit in {"million", "m"}:
                            shares *= 1e6
                        elif sh_unit in {"billion", "bn"}:
                            shares *= 1e9
                        rec["concurrent_repurchase_amount"] = amt
                        rec["concurrent_repurchase_shares"] = shares
                    except Exception:
                        pass

                settlement_m = re.search(
                    r"(cash,\s*shares?\s+of\s+(?:the\s+company['â€™]s\s+)?common\s+stock,\s*or\s*a\s+combination(?:\s+of\s+cash\s+and\s+(?:shares|stock))?|cash,\s*(?:the\s+company['â€™]s\s+)?common\s+stock,\s*or\s*a\s+combination)",
                    txt,
                    re.I,
                )
                if settlement_m:
                    rec["settlement_type"] = self._normalize_text(settlement_m.group(1))

                hedge_m = re.search(
                    r"\b(capped call|call spread|bond hedge|note hedge|convertible note hedge)\b[^.]{0,120}",
                    txt,
                    re.I,
                )
                if hedge_m:
                    rec["hedge_or_call_spread"] = self._normalize_text(hedge_m.group(0))

                cond_m = re.search(
                    r"((?:prior to|before)\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}[^.]{0,220}?(?:certain conditions|conversion conditions|satisfaction of certain conditions))",
                    txt,
                    re.I,
                )
                if cond_m:
                    rec["conversion_conditions_note"] = self._normalize_text(cond_m.group(1))

                has_capital_action = False
                for k in (
                    "concurrent_repurchase_amount",
                    "concurrent_repurchase_shares",
                    "hedge_or_call_spread",
                    "settlement_type",
                    "conversion_conditions_note",
                ):
                    val = rec.get(k)
                    if isinstance(val, str) and val.strip():
                        has_capital_action = True
                        break
                    if pd.notna(pd.to_numeric(val, errors="coerce")):
                        has_capital_action = True
                        break
                if has_capital_action:
                    rows_local.append(rec)
            return rows_local

        docs = self.latest_sec_text_docs_for_convertibles(accns=accns, max_docs=100)
        rows = _scan_docs(docs)

        # SEC index coverage can miss older exhibit docs that still exist in cache.
        fallback_docs: List[Path] = []
        fallback_seen: set[str] = set()
        for root in self._sec_cache_roots_local():
            candidates = [p for p in self._sec_cache_doc_paths_local(root) if p.suffix.lower() in {".htm", ".html"}]
            for p in candidates[:250]:
                if not self._path_belongs_to_ticker(p):
                    continue
                sp = str(p)
                if sp in fallback_seen:
                    continue
                fallback_seen.add(sp)
                fallback_docs.append(p)
        if fallback_docs:
            existing_paths = {str(rec.get("path") or "") for rec in rows}
            for rec in _scan_docs(fallback_docs):
                if str(rec.get("path") or "") in existing_paths:
                    continue
                rows.append(rec)
        return rows

    def extract_convertible_terms_from_xbrl(self, xbrl_path: Any) -> Dict[str, Dict[str, Any]]:
        return self._extract_convertible_terms_from_xbrl(Path(xbrl_path))

    def _extract_convertible_terms_from_xbrl(self, xbrl_path: Path) -> Dict[str, Dict[str, Any]]:
        try:
            xml_txt = xbrl_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return {}
        ctx_to_member: Dict[str, str] = {}
        for m_ctx in re.finditer(r'<(?:xbrli:)?context\s+id="([^"]+)">(.*?)</(?:xbrli:)?context>', xml_txt, flags=re.I | re.S):
            ctx_id = str(m_ctx.group(1) or "").strip()
            body = str(m_ctx.group(2) or "")
            m_member = re.search(
                r'<xbrldi:explicitMember[^>]*dimension="us-gaap:DebtInstrumentAxis"[^>]*>([^<]+)</xbrldi:explicitMember>',
                body,
                flags=re.I | re.S,
            )
            if m_member:
                ctx_to_member[ctx_id] = html.unescape(str(m_member.group(1) or "")).strip()

        def _fact_map(tag_names: Any) -> Dict[str, float]:
            out_map: Dict[str, float] = {}
            wanted = [str(x or "").strip() for x in ([tag_names] if isinstance(tag_names, str) else list(tag_names or []))]
            wanted = [x for x in wanted if x]
            for tag_name in wanted:
                pattern = re.compile(
                    rf'<[A-Za-z0-9_.-]+:{tag_name}\b[^>]*contextRef="([^"]+)"[^>]*>([^<]+)</[A-Za-z0-9_.-]+:{tag_name}>',
                    flags=re.I | re.S,
                )
                for m_fact in pattern.finditer(xml_txt):
                    ctx = str(m_fact.group(1) or "").strip()
                    raw_val = html.unescape(str(m_fact.group(2) or "")).strip()
                    num = self._coerce_number(raw_val)
                    if num is None:
                        try:
                            num = float(raw_val)
                        except Exception:
                            num = None
                    if num is None:
                        continue
                    out_map[ctx] = float(num)
                inline_pattern = re.compile(
                    r"<ix:(?:nonFraction|nonNumeric)\b([^>]*)>(.*?)</ix:(?:nonFraction|nonNumeric)>",
                    flags=re.I | re.S,
                )
                for m_fact in inline_pattern.finditer(xml_txt):
                    attrs = str(m_fact.group(1) or "")
                    if not re.search(rf'\bname="[^"]*:{re.escape(tag_name)}"', attrs, flags=re.I):
                        continue
                    m_ctx = re.search(r'\bcontextRef="([^"]+)"', attrs, flags=re.I)
                    if not m_ctx:
                        continue
                    ctx = str(m_ctx.group(1) or "").strip()
                    raw_val = html.unescape(re.sub(r"<[^>]+>", "", str(m_fact.group(2) or ""))).strip()
                    num = self._coerce_number(raw_val)
                    if num is None:
                        try:
                            num = float(raw_val)
                        except Exception:
                            num = None
                    if num is None:
                        continue
                    out_map[ctx] = float(num)
            return out_map

        price_by_ctx = _fact_map("DebtInstrumentConvertibleConversionPrice1")
        ratio_by_ctx = _fact_map(["DebtInstrumentConvertibleConversionRatio1", "DebtConversionConvertedInstrumentRate"])
        out_terms: Dict[str, Dict[str, Any]] = {}
        for ctx, member in ctx_to_member.items():
            member_key = re.sub(r"[^a-z0-9]+", " ", str(member or "").lower()).strip()
            if "convert" not in member_key:
                continue
            if ctx not in price_by_ctx and ctx not in ratio_by_ctx:
                continue
            rec = out_terms.setdefault(
                member_key,
                {
                    "member": str(member or ""),
                    "conversion_price": None,
                    "conversion_rate_per_1000": None,
                    "source": str(xbrl_path),
                    "note": "Initial/current public conversion terms; may be subject to anti-dilution adjustment.",
                },
            )
            if ctx in price_by_ctx and rec.get("conversion_price") in (None, ""):
                rec["conversion_price"] = float(price_by_ctx[ctx])
            if ctx in ratio_by_ctx and rec.get("conversion_rate_per_1000") in (None, ""):
                rec["conversion_rate_per_1000"] = float(ratio_by_ctx[ctx])
            if ctx in ratio_by_ctx and rec.get("conversion_price") in (None, ""):
                try:
                    ratio_val = float(ratio_by_ctx[ctx])
                except Exception:
                    ratio_val = 0.0
                # SEC Inline XBRL often reports the conversion ratio as shares per $1 of principal,
                # while the workbook logic expects shares per $1,000. Derive price from the raw
                # ratio so downstream normalization can recover the right display basis.
                if ratio_val > 0.0 and ratio_val < 1.0:
                    rec["conversion_price"] = 1.0 / ratio_val
        return out_terms

    def extract_convertible_terms_from_text_docs(self, doc_paths: List[Path]) -> Dict[str, Dict[str, Any]]:
        return self._extract_convertible_terms_from_text_docs(doc_paths)

    def _extract_convertible_terms_from_text_docs(self, doc_paths: List[Path]) -> Dict[str, Dict[str, Any]]:
        out_terms: Dict[str, Dict[str, Any]] = {}
        seen_paths: set[str] = set()
        for path in doc_paths:
            spath = str(path)
            if spath in seen_paths:
                continue
            seen_paths.add(spath)
            try:
                raw_text = path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            txt = self._htmlish_to_text(raw_text)
            low = txt.lower()
            if "convertible" not in low:
                continue
            for m_inst in re.finditer(
                r"\$?\s*([0-9][0-9,]*(?:\.\d+)?)\s*(million|billion|m|bn)?\s+aggregate principal amount of (?:its|the)?\s*"
                r"([0-9]+(?:\.\d+)?)%\s+convertible senior notes due(?:\s+([A-Za-z]+))?\s+(20\d{2})",
                txt,
                re.I,
            ):
                principal_txt = str(m_inst.group(1) or "").replace(",", "").strip()
                principal_unit = str(m_inst.group(2) or "").strip().lower()
                coupon_txt = str(m_inst.group(3) or "").strip()
                due_month = str(m_inst.group(4) or "").strip()
                due_year = str(m_inst.group(5) or "").strip()
                principal_amount = None
                try:
                    principal_amount = float(principal_txt)
                    if principal_unit in {"million", "m"}:
                        principal_amount *= 1e6
                    elif principal_unit in {"billion", "bn"}:
                        principal_amount *= 1e9
                except Exception:
                    principal_amount = None
                window = txt[max(0, int(m_inst.start()) - 240) : min(len(txt), int(m_inst.end()) + 4000)]
                price_match = re.search(
                    r"\b(?:which represents an )?initial conversion price of approximately\s+\$?\s*([0-9]+(?:\.\d+)?)\s+per share\b",
                    window,
                    re.I,
                )
                if not price_match:
                    price_match = re.search(
                        r"\bconversion price of approximately\s+\$?\s*([0-9]+(?:\.\d+)?)\s+per share\b",
                        window,
                        re.I,
                    )
                rate_match = re.search(
                    r"\binitial conversion rate is\s+([0-9]+(?:\.\d+)?)\s+shares\b[\s\S]{0,120}?per \$1,000 principal amount\b",
                    window,
                    re.I,
                )
                if not rate_match:
                    rate_match = re.search(
                        r"\b([0-9]+(?:\.\d+)?)\s+shares(?: of [^.]+?)?\s+per \$1,000 principal amount\b",
                        window,
                        re.I,
                    )
                if not price_match and not rate_match:
                    continue
                rep_match = re.search(
                    r"used approximately \$\s*([0-9][0-9,]*(?:\.\d+)?)\s*(million|billion|m|bn)?[^.]{0,240}?(?:to\s+)?repurchase(?: approximately)?\s*([0-9][0-9,]*(?:\.\d+)?)\s*(million|billion|m|bn)?\s+shares",
                    txt,
                    re.I,
                )
                member_bits = [coupon_txt, "convertible senior notes", "due"]
                if due_month:
                    member_bits.append(due_month)
                if due_year:
                    member_bits.append(due_year)
                member_key = re.sub(r"[^a-z0-9]+", " ", " ".join(member_bits).lower()).strip()
                rec = out_terms.setdefault(
                    member_key,
                    {
                        "member": " ".join([b for b in member_bits if b]).strip(),
                        "conversion_price": None,
                        "conversion_rate_per_1000": None,
                        "coupon_pct": None,
                        "principal_amount": None,
                        "concurrent_repurchase_amount": None,
                        "concurrent_repurchase_shares": None,
                        "source": spath,
                        "note": "Publicly disclosed conversion terms from SEC text filing.",
                    },
                )
                if coupon_txt and rec.get("coupon_pct") in (None, ""):
                    try:
                        rec["coupon_pct"] = float(coupon_txt)
                    except Exception:
                        pass
                if principal_amount is not None and rec.get("principal_amount") in (None, ""):
                    rec["principal_amount"] = float(principal_amount)
                if price_match and rec.get("conversion_price") in (None, ""):
                    try:
                        rec["conversion_price"] = float(price_match.group(1))
                    except Exception:
                        pass
                if rate_match and rec.get("conversion_rate_per_1000") in (None, ""):
                    try:
                        rec["conversion_rate_per_1000"] = float(rate_match.group(1))
                    except Exception:
                        pass
                if rep_match:
                    try:
                        rep_amt = float(str(rep_match.group(1)).replace(",", ""))
                        rep_amt_unit = str(rep_match.group(2) or "").lower()
                        if rep_amt_unit in {"million", "m"}:
                            rep_amt *= 1e6
                        elif rep_amt_unit in {"billion", "bn"}:
                            rep_amt *= 1e9
                        rep_shares = float(str(rep_match.group(3)).replace(",", ""))
                        rep_shares_unit = str(rep_match.group(4) or "").lower()
                        if rep_shares_unit in {"million", "m"}:
                            rep_shares *= 1e6
                        elif rep_shares_unit in {"billion", "bn"}:
                            rep_shares *= 1e9
                        if rec.get("concurrent_repurchase_amount") in (None, ""):
                            rec["concurrent_repurchase_amount"] = rep_amt
                        if rec.get("concurrent_repurchase_shares") in (None, ""):
                            rec["concurrent_repurchase_shares"] = rep_shares
                    except Exception:
                        pass
        return out_terms

    def enrich_latest_debt_convertibles(self, debt_tranches_latest: Any) -> Any:
        df_in = debt_tranches_latest
        if df_in is None or df_in.empty:
            return pd.DataFrame() if df_in is None else df_in
        out_df = df_in.copy()
        for col_name in [
            "conversion_rate_per_1000",
            "conversion_price",
            "shares_on_full_conversion",
            "conversion_terms_source",
            "conversion_terms_note",
            "concurrent_repurchase_amount",
            "concurrent_repurchase_shares",
            "hedge_or_call_spread",
            "settlement_type",
            "conversion_conditions_note",
            "dilution_structure_note",
        ]:
            if col_name not in out_df.columns:
                out_df[col_name] = pd.NA
        if "instrument_type" not in out_df.columns:
            return out_df
        conv_mask = out_df["instrument_type"].astype(str).str.lower().eq("convertible")
        if not bool(conv_mask.any()):
            return out_df
        accn_candidates = []
        if "accn" in out_df.columns:
            accn_candidates = [str(v) for v in out_df.loc[conv_mask, "accn"].dropna().tolist() if str(v).strip()]
        terms_by_member: Dict[str, Dict[str, Any]] = {}

        def _term_value_missing(v: Any) -> bool:
            num_v = pd.to_numeric(v, errors="coerce")
            if pd.notna(num_v):
                return False
            return self._safe_text_value(v) == ""

        def _merge_convertible_term_record(prev: Optional[Dict[str, Any]], rec: Dict[str, Any]) -> Dict[str, Any]:
            if prev is None:
                return dict(rec)
            merged = dict(prev)
            for field_name in [
                "conversion_price",
                "conversion_rate_per_1000",
                "coupon_pct",
                "principal_amount",
                "concurrent_repurchase_amount",
                "concurrent_repurchase_shares",
            ]:
                prev_val = merged.get(field_name)
                rec_val = rec.get(field_name)
                prev_missing = _term_value_missing(prev_val)
                rec_present = not _term_value_missing(rec_val)
                if prev_missing and rec_present:
                    merged[field_name] = rec_val
            if not self._safe_text_value(merged.get("source")) and self._safe_text_value(rec.get("source")):
                merged["source"] = rec.get("source")
            if not self._safe_text_value(merged.get("note")) and self._safe_text_value(rec.get("note")):
                merged["note"] = rec.get("note")
            return merged

        for xbrl_path in self.find_latest_xbrl_docs(accn_candidates):
            for member_key, rec in self._extract_convertible_terms_from_xbrl(xbrl_path).items():
                terms_by_member[member_key] = _merge_convertible_term_record(terms_by_member.get(member_key), rec)
        for member_key, rec in self._extract_convertible_terms_from_text_docs(
            self.latest_sec_text_docs_for_convertibles(accn_candidates, max_docs=100)
        ).items():
            terms_by_member[member_key] = _merge_convertible_term_record(terms_by_member.get(member_key), rec)
        fallback_term_docs: List[Path] = []
        fallback_term_seen: set[str] = set()
        for root in self._sec_cache_roots_local():
            for p in self._sec_cache_doc_paths_local(root):
                if p.suffix.lower() not in {".htm", ".html", ".txt"}:
                    continue
                if not self._path_belongs_to_ticker(p):
                    continue
                sp = str(p)
                if sp in fallback_term_seen:
                    continue
                fallback_term_seen.add(sp)
                fallback_term_docs.append(p)
                if len(fallback_term_docs) >= 250:
                    break
            if len(fallback_term_docs) >= 250:
                break
        for member_key, rec in self._extract_convertible_terms_from_text_docs(fallback_term_docs).items():
            terms_by_member[member_key] = _merge_convertible_term_record(terms_by_member.get(member_key), rec)
        capital_action_rows = self._extract_convertible_capital_actions(accn_candidates)

        if not terms_by_member and not capital_action_rows:
            return out_df

        def _pick_terms(row: pd.Series) -> Optional[Dict[str, Any]]:
            row_name = str(row.get("tranche_name") or "").lower()
            row_norm = re.sub(r"[^a-z0-9]+", " ", row_name).strip()
            row_year = pd.to_numeric(row.get("maturity_year"), errors="coerce")
            row_coupon_match = re.search(r"([0-9]+(?:\.\d+)?)%", row_name)
            row_coupon = str(row_coupon_match.group(1) or "").strip() if row_coupon_match else ""
            row_coupon_num = pd.to_numeric(row_coupon, errors="coerce")
            row_principal = pd.to_numeric(row.get("amount_principal"), errors="coerce")
            best_key = None
            best_score = -1
            for member_key, rec in terms_by_member.items():
                score = 0
                rec_member = str(rec.get("member") or member_key or "").lower()
                if "convert" in member_key:
                    score += 2
                if pd.notna(row_year) and str(int(row_year)) in member_key:
                    score += 4
                if "senior" in row_norm and "senior" in member_key:
                    score += 1
                if "note" in row_norm and "note" in member_key:
                    score += 1
                if row_coupon and row_coupon in member_key:
                    score += 4
                rec_coupon_num = pd.to_numeric(rec.get("coupon_pct"), errors="coerce")
                if (
                    pd.notna(row_coupon_num)
                    and pd.notna(rec_coupon_num)
                    and abs(float(row_coupon_num) - float(rec_coupon_num)) <= 0.05
                ):
                    score += 5
                rec_principal = pd.to_numeric(rec.get("principal_amount"), errors="coerce")
                if (
                    pd.notna(row_principal)
                    and pd.notna(rec_principal)
                    and abs(float(row_principal) - float(rec_principal)) <= max(1.0, abs(float(rec_principal)) * 0.05)
                ):
                    score += 4
                if member_key in row_norm or row_norm in member_key:
                    score += 2
                if rec_member and (rec_member in row_name or row_name in rec_member):
                    score += 2
                if score > best_score:
                    best_score = score
                    best_key = member_key
            if best_score < 4 and pd.notna(row_coupon_num) and pd.notna(row_principal):
                strict_matches: List[Tuple[str, Dict[str, Any]]] = []
                for member_key, rec in terms_by_member.items():
                    rec_coupon_num = pd.to_numeric(rec.get("coupon_pct"), errors="coerce")
                    rec_principal = pd.to_numeric(rec.get("principal_amount"), errors="coerce")
                    if not pd.notna(rec_coupon_num) or not pd.notna(rec_principal):
                        continue
                    if abs(float(row_coupon_num) - float(rec_coupon_num)) > 0.05:
                        continue
                    if abs(float(row_principal) - float(rec_principal)) > max(1.0, abs(float(rec_principal)) * 0.05):
                        continue
                    strict_matches.append((member_key, rec))
                if len(strict_matches) == 1:
                    return strict_matches[0][1]
            return terms_by_member.get(best_key) if best_score >= 4 and best_key is not None else None

        def _pick_capital_actions(row: pd.Series) -> Optional[Dict[str, Any]]:
            if not capital_action_rows:
                return None
            row_name = str(row.get("tranche_name") or "").lower()
            maturity_disp = str(row.get("maturity_display") or row.get("maturity_year") or "").lower()
            row_coupon_match = re.search(r"([0-9]+(?:\.\d+)?)%", row_name)
            row_coupon = str(row_coupon_match.group(1) or "").strip() if row_coupon_match else ""
            row_coupon_num = pd.to_numeric(row.get("coupon_pct"), errors="coerce")
            row_principal = pd.to_numeric(row.get("amount_principal"), errors="coerce")
            if not row_coupon and pd.notna(row_coupon_num):
                coupon_pct_val = float(row_coupon_num)
                if abs(coupon_pct_val) <= 1.0:
                    coupon_pct_val *= 100.0
                row_coupon = f"{coupon_pct_val:.2f}".rstrip("0").rstrip(".")
            best_row: Optional[Dict[str, Any]] = None
            best_score = -1
            for rec in capital_action_rows:
                score = 0
                hint = str(rec.get("instrument_hint") or "").strip().lower()
                txt = str(rec.get("text") or "").lower()
                has_repurchase = bool(
                    pd.notna(pd.to_numeric(rec.get("concurrent_repurchase_amount"), errors="coerce"))
                    or pd.notna(pd.to_numeric(rec.get("concurrent_repurchase_shares"), errors="coerce"))
                )
                hint_matches_row = not hint or hint in row_name or hint in maturity_disp
                if has_repurchase and hint and not hint_matches_row:
                    continue
                if hint and hint in row_name:
                    score += 6
                if hint and hint in maturity_disp:
                    score += 5
                if "2032" in row_name and "2032" in txt:
                    score += 4
                if "2030" in row_name and "2030" in txt:
                    score += 4
                if "2027" in row_name and "2027" in txt:
                    score += 4
                if "1.5%" in row_name and "1.50%" in txt:
                    score += 4
                if "5.25%" in row_name and "5.25%" in txt:
                    score += 4
                if "2.25%" in row_name and "2.25%" in txt:
                    score += 4
                if pd.notna(pd.to_numeric(rec.get("concurrent_repurchase_amount"), errors="coerce")):
                    score += 4
                if pd.notna(pd.to_numeric(rec.get("concurrent_repurchase_shares"), errors="coerce")):
                    score += 4
                if self._safe_text_value(rec.get("settlement_type")):
                    score += 1
                if score > best_score:
                    best_score = score
                    best_row = rec
            best_has_repurchase = bool(
                best_row is not None
                and (
                    pd.notna(pd.to_numeric(best_row.get("concurrent_repurchase_amount"), errors="coerce"))
                    or pd.notna(pd.to_numeric(best_row.get("concurrent_repurchase_shares"), errors="coerce"))
                )
            )
            if row_coupon and (best_score < 4 or not best_has_repurchase):
                strict_matches: List[Dict[str, Any]] = []
                for rec in capital_action_rows:
                    txt = str(rec.get("text") or "").lower()
                    if row_coupon not in txt and row_coupon.replace(".0", "") not in txt:
                        continue
                    if pd.notna(row_principal):
                        principal_m = float(row_principal) / 1e6 if abs(float(row_principal)) > 1e6 else float(row_principal)
                        principal_tokens = {
                            f"{principal_m:.1f}",
                            f"{principal_m:.0f}",
                        }
                        if not any(
                            f"{tok} million" in txt or f"${tok} million" in txt
                            for tok in principal_tokens
                        ):
                            continue
                    if not (
                        pd.notna(pd.to_numeric(rec.get("concurrent_repurchase_amount"), errors="coerce"))
                        or pd.notna(pd.to_numeric(rec.get("concurrent_repurchase_shares"), errors="coerce"))
                    ):
                        continue
                    strict_matches.append(rec)
                if len(strict_matches) == 1:
                    return strict_matches[0]
            return best_row if best_score >= 4 else None

        def _normalized_conversion_rate_per_1000(
            price_in: Any,
            ratio_in: Any,
        ) -> Any:
            price_num = pd.to_numeric(price_in, errors="coerce")
            ratio_num = pd.to_numeric(ratio_in, errors="coerce")
            if not pd.notna(ratio_num):
                return pd.NA
            ratio_val = float(ratio_num)
            if not pd.notna(price_num) or float(price_num) <= 0 or ratio_val <= 0:
                return ratio_val
            price_val = float(price_num)
            per_dollar = 1.0 / price_val
            per_thousand = 1000.0 / price_val
            if abs(ratio_val - per_dollar) <= max(1e-6, abs(per_dollar) * 0.10):
                return ratio_val * 1000.0
            if abs(ratio_val - per_thousand) <= max(1e-3, abs(per_thousand) * 0.10):
                return ratio_val
            return ratio_val

        def _convertible_text_matches_row(txt_in: Any, row_in: pd.Series, require_convert: bool = True) -> bool:
            txt = self._htmlish_to_text(txt_in)
            low = str(txt or "").lower()
            if require_convert and "convert" not in low:
                return False
            score = 0
            maturity_txt = str(row_in.get("maturity_display") or row_in.get("maturity_year") or "").strip().lower()
            if maturity_txt and maturity_txt in low:
                score += 3
            year_match = re.search(r"(20\d{2})", maturity_txt)
            if year_match and year_match.group(1) in low:
                score += 2
            coupon_num = pd.to_numeric(row_in.get("coupon_pct"), errors="coerce")
            coupon_tokens: set[str] = set()
            if pd.notna(coupon_num):
                coupon_val = float(coupon_num)
                if abs(coupon_val) <= 1.0:
                    coupon_val *= 100.0
                coupon_tokens.update(
                    {
                        f"{coupon_val:.2f}%".rstrip("0").rstrip("."),
                        f"{coupon_val:.2f}%",
                        f"{coupon_val:.1f}%".rstrip("0").rstrip("."),
                    }
                )
            if any(tok and tok.lower() in low for tok in coupon_tokens):
                score += 3
            principal_val = pd.to_numeric(row_in.get("amount_principal"), errors="coerce")
            if pd.notna(principal_val):
                principal_m = float(principal_val)
                if abs(principal_m) > 1e6:
                    principal_m /= 1e6
                principal_tokens = {
                    f"${principal_m:.1f} million",
                    f"{principal_m:.1f} million",
                    f"${principal_m:.0f} million",
                    f"{principal_m:.0f} million",
                }
                if any(tok.lower() in low for tok in principal_tokens):
                    score += 2
            tranche_low = re.sub(r"[^a-z0-9% ]+", " ", str(row_in.get("tranche_name") or "").lower()).strip()
            if tranche_low and tranche_low in re.sub(r"[^a-z0-9% ]+", " ", low):
                score += 2
            return score >= 4

        def _convertible_dilution_structure_note_from_row(row_in: pd.Series) -> str:
            hedge_low = self._safe_text_value(row_in.get("hedge_or_call_spread")).lower()
            if "capped call" in hedge_low:
                return "Capped call may reduce dilution."
            if hedge_low and any(
                token in hedge_low
                for token in (
                    "call spread",
                    "bond hedge",
                    "note hedge",
                    "convertible note hedge",
                    "warrant overlay",
                    "warrant transaction",
                    "warrants sold",
                )
            ):
                return "Capped call or related hedge structure may reduce dilution."
            matched_doc_lows: List[str] = []
            for raw_path in str(row_in.get("conversion_terms_source") or "").split(" | "):
                doc_path = Path(str(raw_path).strip())
                if not doc_path.exists() or not doc_path.is_file():
                    continue
                try:
                    raw_text = doc_path.read_text(encoding="utf-8", errors="ignore")
                except Exception:
                    continue
                if not _convertible_text_matches_row(raw_text, row_in, require_convert=True):
                    continue
                low = self._htmlish_to_text(raw_text).lower()
                if "capped call" in low:
                    return "Capped call may reduce dilution."
                if any(
                    token in low
                    for token in (
                        "call spread",
                        "bond hedge",
                        "note hedge",
                        "convertible note hedge",
                        "warrant overlay",
                        "warrant transaction",
                        "warrants sold",
                    )
                ):
                    return "Capped call or related hedge structure may reduce dilution."
                matched_doc_lows.append(low)
            settlement_blob = " ".join(
                [
                    self._safe_text_value(row_in.get("settlement_type")).lower(),
                    self._safe_text_value(row_in.get("conversion_conditions_note")).lower(),
                    self._safe_text_value(row_in.get("conversion_terms_note")).lower(),
                ]
            )
            settlement_tokens = (
                "net share settlement",
                "cash settlement",
                "cash, shares",
                "or a combination",
                "paying cash up to the aggregate principal amount",
                "delivering shares",
                "forced conversion",
                "issuer call",
            )
            if any(token in settlement_blob for token in settlement_tokens) or (
                "make-whole" in settlement_blob and "increase the conversion rate" in settlement_blob
            ):
                return "Related hedge / settlement structure may reduce dilution."
            for low in matched_doc_lows:
                if any(token in low for token in settlement_tokens) or (
                    "make-whole" in low and "increase the conversion rate" in low
                ):
                    return "Related hedge / settlement structure may reduce dilution."
            return ""

        for idx, row in out_df.loc[conv_mask].iterrows():
            terms = _pick_terms(row)
            price_val = pd.to_numeric((terms or {}).get("conversion_price"), errors="coerce")
            ratio_val = pd.to_numeric((terms or {}).get("conversion_rate_per_1000"), errors="coerce")
            principal_val = pd.to_numeric(row.get("amount_principal"), errors="coerce")
            terms_rep_amt = pd.to_numeric((terms or {}).get("concurrent_repurchase_amount"), errors="coerce")
            terms_rep_sh = pd.to_numeric((terms or {}).get("concurrent_repurchase_shares"), errors="coerce")
            ratio_val = _normalized_conversion_rate_per_1000(price_val, ratio_val)
            shares_val = None
            if pd.notna(principal_val) and pd.notna(ratio_val) and float(ratio_val) > 0:
                shares_val = (float(principal_val) / 1000.0) * float(ratio_val)
                if not pd.notna(price_val):
                    price_val = 1000.0 / float(ratio_val)
            elif pd.notna(principal_val) and pd.notna(price_val) and float(price_val) > 0:
                shares_val = float(principal_val) / float(price_val)
            out_df.at[idx, "conversion_rate_per_1000"] = float(ratio_val) if pd.notna(ratio_val) else pd.NA
            out_df.at[idx, "conversion_price"] = float(price_val) if pd.notna(price_val) else pd.NA
            out_df.at[idx, "shares_on_full_conversion"] = float(shares_val) if shares_val is not None else pd.NA
            out_df.at[idx, "conversion_terms_source"] = self._safe_text_value((terms or {}).get("source"))
            out_df.at[idx, "conversion_terms_note"] = self._safe_text_value((terms or {}).get("note"))
            if pd.notna(terms_rep_amt):
                out_df.at[idx, "concurrent_repurchase_amount"] = float(terms_rep_amt)
            if pd.notna(terms_rep_sh):
                out_df.at[idx, "concurrent_repurchase_shares"] = float(terms_rep_sh)
            cap_rec = _pick_capital_actions(row)
            if cap_rec is not None:
                cap_amt = pd.to_numeric(cap_rec.get("concurrent_repurchase_amount"), errors="coerce")
                cap_sh = pd.to_numeric(cap_rec.get("concurrent_repurchase_shares"), errors="coerce")
                out_df.at[idx, "concurrent_repurchase_amount"] = float(cap_amt) if pd.notna(cap_amt) else pd.NA
                out_df.at[idx, "concurrent_repurchase_shares"] = float(cap_sh) if pd.notna(cap_sh) else pd.NA
                out_df.at[idx, "hedge_or_call_spread"] = self._safe_text_value(cap_rec.get("hedge_or_call_spread"))
                out_df.at[idx, "settlement_type"] = self._safe_text_value(cap_rec.get("settlement_type"))
                note_bits = [self._safe_text_value(out_df.at[idx, "conversion_terms_note"])]
                cond_note = self._safe_text_value(cap_rec.get("conversion_conditions_note"))
                if cond_note:
                    note_bits.append(cond_note)
                    out_df.at[idx, "conversion_conditions_note"] = cond_note
                merged_note = " | ".join([x for x in note_bits if x])
                if merged_note:
                    out_df.at[idx, "conversion_terms_note"] = merged_note
                src_bits: List[str] = []
                for raw_src in [out_df.at[idx, "conversion_terms_source"], cap_rec.get("path")]:
                    src_txt = self._safe_text_value(raw_src)
                    if not src_txt or src_txt in src_bits:
                        continue
                    src_bits.append(src_txt)
                if src_bits:
                    out_df.at[idx, "conversion_terms_source"] = " | ".join(src_bits)
            out_df.at[idx, "dilution_structure_note"] = _convertible_dilution_structure_note_from_row(out_df.loc[idx])
        return out_df
