from __future__ import annotations

from dataclasses import dataclass
from typing import Any, MutableMapping


@dataclass(frozen=True)
class AnfQASupportDeps:
    runtime: MutableMapping[str, Any]


class AnfQASupport:
    def __init__(self, deps: AnfQASupportDeps) -> None:
        self._runtime = deps.runtime

    def normalize_qa_status_rows(
        self,
        checks: Any,
        *,
        is_anf_profile: bool = False,
    ) -> Any:
        pd = self._runtime["pd"]
        re = self._runtime["re"]

        if checks is None or checks.empty:
            return checks
        out = checks.copy()
        if "status" not in out.columns:
            out["status"] = ""
        if "severity" not in out.columns:
            out["severity"] = ""

        def _clean_status_token(value: Any) -> str:
            token = str(value if value is not None else "").strip()
            if token.lower() in {"", "nan", "none", "null", "<na>", "nat"}:
                return ""
            low = token.lower()
            return {
                "passed": "pass",
                "passing": "pass",
                "pass": "pass",
                "warn": "warn",
                "warning": "warn",
                "fail": "fail",
                "failed": "fail",
                "info": "info",
                "informational": "info",
                "skip": "skip",
                "skipped": "skip",
            }.get(low, token.lower())

        for idx, rr in out.iterrows():
            status = _clean_status_token(rr.get("status"))
            severity = _clean_status_token(rr.get("severity"))
            check = str(rr.get("check") or "").strip()
            message = str(rr.get("message") or "").strip()
            low = f"{check} {message}".lower()
            new_status = status
            if not new_status:
                if "pass" in low:
                    new_status = "pass"
                elif "fail" in severity:
                    new_status = "fail"
                elif "warn" in severity:
                    new_status = "warn"
                elif "skip" in low:
                    new_status = "skip"
                else:
                    new_status = "info"
            if is_anf_profile:
                expected_gap = (
                    ("hidden_flag" in low and any(tok in low for tok in ("shares_out", "market", "price", "fcf_yield", "dividend_ps", "interest_coverage")))
                    or ("debt" in low and "coverage" in low and re.search(r"\$0\.[0-9]+m", low))
                    or ("cash_identity" in low and any(tok in low for tok in ("approx", "coverage", "definition", "bridge")))
                )
                if expected_gap and str(new_status).lower() == "fail":
                    new_status = "warn"
                    if str(severity).lower() == "fail":
                        severity = "warn"
            if not severity:
                severity = new_status if new_status in {"fail", "warn"} else "info"
            out.at[idx, "severity"] = severity
            out.at[idx, "status"] = new_status
        return out
