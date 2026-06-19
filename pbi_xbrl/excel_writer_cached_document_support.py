from __future__ import annotations

from dataclasses import dataclass
from typing import Any, MutableMapping


@dataclass(frozen=True)
class CachedDocumentSupportDeps:
    runtime: MutableMapping[str, Any]


class CachedDocumentSupport:
    def __init__(self, deps: CachedDocumentSupportDeps) -> None:
        self.deps = deps

    def _rt(self, name: str, default: Any = None) -> Any:
        return self.deps.runtime.get(name, default)

    def path_cache_key(self, path_in: Any) -> str:
        source_path_cache_key = self._rt("source_path_cache_key")
        return source_path_cache_key(path_in)

    def read_cached_doc_raw(self, path_in: Any) -> Any:
        source_read_cached_doc_raw = self._rt("source_read_cached_doc_raw")
        return source_read_cached_doc_raw(
            path_in,
            document_cache=self._rt("document_cache"),
            pdf_text_cache_root=self._rt("pdf_text_cache_root"),
            rebuild_doc_text_cache=self._rt("rebuild_doc_text_cache"),
            quiet_pdf_warnings=self._rt("quiet_pdf_warnings"),
        )

    def read_cached_doc_text(self, path_in: Any, *, normalize: bool = False) -> Any:
        source_read_cached_doc_text = self._rt("source_read_cached_doc_text")
        return source_read_cached_doc_text(
            path_in,
            document_cache=self._rt("document_cache"),
            pdf_text_cache_root=self._rt("pdf_text_cache_root"),
            rebuild_doc_text_cache=self._rt("rebuild_doc_text_cache"),
            quiet_pdf_warnings=self._rt("quiet_pdf_warnings"),
            normalize=normalize,
        )

    def infer_cached_doc_quarter(
        self,
        path_in: Any,
        *,
        text: Any = None,
        latest_q_hint: Any = None,
        include_follow_text: bool = False,
    ) -> Any:
        source_infer_cached_doc_quarter = self._rt("source_infer_cached_doc_quarter")
        return source_infer_cached_doc_quarter(
            path_in,
            document_cache=self._rt("document_cache"),
            parse_quarter_from_filename=self._rt("_parse_quarter_from_filename"),
            parse_quarter_from_follow_text=self._rt("_parse_quarter_from_follow_text"),
            text=text,
            latest_q_hint=latest_q_hint,
            include_follow_text=include_follow_text,
        )

    def sec_docs_for_accession(self, accn_in: Any) -> Any:
        source_sec_docs_for_accession = self._rt("source_sec_docs_for_accession")
        return source_sec_docs_for_accession(
            accn_in,
            cache_root=self._rt("cache_root"),
            document_cache=self._rt("document_cache"),
        )

    def submission_cache_files(self, *, max_files: Any = None) -> Any:
        source_submission_cache_files = self._rt("source_submission_cache_files")
        return source_submission_cache_files(
            cache_roots=tuple(self._rt("cache_roots") or ()),
            document_cache=self._rt("document_cache"),
            max_files=max_files,
            path_filter=self._rt("_ticker_specific_submission_path"),
        )

    def submission_recent_row_quarter(self, row: Any) -> Any:
        parse_date = self._rt("parse_date")
        _is_quarter_end = self._rt("_is_quarter_end")
        _coerce_prev_quarter_end = self._rt("_coerce_prev_quarter_end")

        rep_d = parse_date(row.get("report"))
        if rep_d is not None:
            return rep_d if _is_quarter_end(rep_d) else _coerce_prev_quarter_end(rep_d)
        filed_d = parse_date(row.get("filed"))
        if filed_d is not None:
            return _coerce_prev_quarter_end(filed_d)
        return None

    def submission_recent_rows(self, *, max_files: Any = None) -> Any:
        source_submission_recent_rows = self._rt("source_submission_recent_rows")
        return source_submission_recent_rows(
            cache_roots=tuple(self._rt("cache_roots") or ()),
            document_cache=self._rt("document_cache"),
            raw_reader=self.read_cached_doc_raw,
            max_files=max_files,
            path_filter=self._rt("_ticker_specific_submission_path"),
        )

    def resolve_cached_doc_path(
        self,
        *,
        accn: Any = "",
        doc_name: Any = "",
        path_hint: Any = "",
    ) -> Any:
        source_resolve_cached_doc_path = self._rt("source_resolve_cached_doc_path")
        return source_resolve_cached_doc_path(
            cache_roots=tuple(self._rt("cache_roots") or ()),
            accession_doc_lookup=self.sec_docs_for_accession,
            accn=accn,
            doc_name=doc_name,
            path_hint=path_hint,
        )
