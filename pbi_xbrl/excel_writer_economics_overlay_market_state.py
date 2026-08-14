"""Market-state assembly helpers for the Economics_Overlay writer."""
from __future__ import annotations

import datetime as dt
import os
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

from .cache_semantics import (
    GPRE_BASIS_PROXY_WRITER_CACHE_VERSION,
    build_cache_identity,
    module_content_identity,
)


@dataclass(frozen=True)
class EconomicsOverlayMarketStateDeps:
    is_gpre_profile: bool
    ticker: str
    ticker_roots: Sequence[Path]
    economics_market_rows: Sequence[Mapping[str, Any]]
    row_map: Mapping[Tuple[str, date], Mapping[str, Any]]
    overlay_display_quarters: Sequence[date]
    overlay_market_as_of: date
    gpre_commercial_setup_rows: Sequence[Mapping[str, Any]]
    cache_dir: Any
    company_profile: Any
    state: Dict[str, Any]
    overlay_coefficient_detail: Callable[[str], Mapping[str, Any]]
    parse_quarter_label_text: Callable[[Any], Optional[date]]
    quarter_label_short: Callable[[Any], str]
    record_writer_substage: Callable[[str, float], None]
    build_gpre_plant_capacity_history: Callable[..., Mapping[str, Any]]
    load_or_download_gpre_corn_bids_snapshot: Callable[..., Mapping[str, Any]]
    fetch_gpre_corn_bids_snapshot: Callable[..., Mapping[str, Any]]
    build_gpre_official_proxy_snapshot: Callable[..., Mapping[str, Any]]
    build_gpre_official_proxy_history_series: Callable[..., Sequence[Mapping[str, Any]]]
    build_prior_quarter_simple_crush_snapshot: Callable[..., Mapping[str, Any]]
    build_current_qtd_simple_crush_snapshot: Callable[..., Mapping[str, Any]]
    build_simple_crush_history_series: Callable[..., Sequence[Mapping[str, Any]]]
    build_next_quarter_thesis_snapshot: Callable[..., Mapping[str, Any]]
    market_input_fingerprint: Callable[..., Mapping[str, Any]]
    data_root_from_sec_cache_path: Callable[..., Optional[Path]]
    build_gpre_basis_proxy_model: Callable[..., Mapping[str, Any]]
    build_gpre_overlay_proxy_preview_bundle: Callable[..., Mapping[str, Any]]
    resolve_gpre_quarter_open_snapshot: Callable[..., Mapping[str, Any]]
    market_build_gpre_proxy_implied_results_bundle: Callable[..., Mapping[str, Any]]
    persist_gpre_frozen_thesis_snapshot: Callable[..., None]


@dataclass(frozen=True)
class EconomicsOverlayMarketStateResult:
    gpre_ticker_root_local: Optional[Path]
    gpre_bids_snapshot: Dict[str, Any]
    gpre_plant_capacity_history: Dict[str, Any]
    prior_q_market_snapshot: Dict[str, Any]
    current_qtd_market_snapshot: Dict[str, Any]
    next_quarter_thesis_snapshot: Dict[str, Any]
    simple_crush_history_rows: List[Dict[str, Any]]
    gpre_basis_model_result: Dict[str, Any]
    prior_market_status: str
    current_market_status: str
    prior_market_available: bool
    current_market_available: bool
    prior_market_display_quarter: Any
    current_market_display_quarter: Any
    prior_market_display_quarter_txt: str
    current_market_display_quarter_txt: str
    next_thesis_quarter_end: Any
    next_thesis_quarter_txt: str
    prior_process_status: str
    current_process_status: str
    gpre_reported_margin_by_quarter: Dict[Any, float]
    gpre_underlying_margin_by_quarter: Dict[Any, float]
    gpre_denominator_policy_by_quarter: Dict[Any, str]
    gpre_reported_gallons_by_quarter: Dict[Any, float]
    gpre_reported_gallons_sold_by_quarter: Dict[Any, float]
    gpre_reported_gallons_produced_by_quarter: Dict[Any, float]
    gpre_basis_quarter_map: Dict[Any, Dict[str, Any]]
    gpre_basis_weights_latest: List[Dict[str, Any]]
    gpre_official_market_rows: List[Dict[str, Any]]
    gpre_official_market_summary: str
    gpre_official_weighting_method: str
    gpre_official_ethanol_method: str
    gpre_official_basis_method: str
    gpre_official_gas_method: str
    gpre_official_fallback_policy: str
    gpre_overlay_preview_bundle: Dict[str, Any]
    gpre_best_forward_preview_bundle: Dict[str, Any]
    gpre_proxy_implied_results_bundle: Dict[str, Any]
    gpre_current_qtd_trend_tracking: Dict[str, Any]
    quarter_open_market_snapshot: Dict[str, Any]
    quarter_open_proxy_status: str
    quarter_open_provenance: str
    quarter_open_display_quarter: Any
    quarter_open_display_quarter_txt: str
    quarter_open_subheader_txt: str
    chosen_preview_quality: str
    chosen_preview_mae: Any
    chosen_preview_max_error: Any
    chosen_preview_top_miss: str
    chosen_preview_worst_phase: str
    quarterly_df: Any


def build_economics_overlay_market_state(
    deps: EconomicsOverlayMarketStateDeps,
) -> EconomicsOverlayMarketStateResult:
    is_gpre_profile = deps.is_gpre_profile
    ticker = deps.ticker
    ticker_roots = deps.ticker_roots
    economics_market_rows = deps.economics_market_rows
    row_map = deps.row_map
    overlay_display_quarters = deps.overlay_display_quarters
    overlay_market_as_of = deps.overlay_market_as_of
    gpre_commercial_setup_rows = deps.gpre_commercial_setup_rows
    cache_dir = deps.cache_dir
    company_profile = deps.company_profile
    state = deps.state
    _overlay_coefficient_detail = deps.overlay_coefficient_detail
    _parse_quarter_label_text = deps.parse_quarter_label_text
    _quarter_label_short = deps.quarter_label_short
    _record_writer_substage = deps.record_writer_substage
    build_gpre_plant_capacity_history = deps.build_gpre_plant_capacity_history
    load_or_download_gpre_corn_bids_snapshot = deps.load_or_download_gpre_corn_bids_snapshot
    fetch_gpre_corn_bids_snapshot = deps.fetch_gpre_corn_bids_snapshot
    build_gpre_official_proxy_snapshot = deps.build_gpre_official_proxy_snapshot
    build_gpre_official_proxy_history_series = deps.build_gpre_official_proxy_history_series
    build_prior_quarter_simple_crush_snapshot = deps.build_prior_quarter_simple_crush_snapshot
    build_current_qtd_simple_crush_snapshot = deps.build_current_qtd_simple_crush_snapshot
    build_simple_crush_history_series = deps.build_simple_crush_history_series
    build_next_quarter_thesis_snapshot = deps.build_next_quarter_thesis_snapshot
    market_input_fingerprint = deps.market_input_fingerprint
    data_root_from_sec_cache_path = deps.data_root_from_sec_cache_path
    build_gpre_basis_proxy_model = deps.build_gpre_basis_proxy_model
    build_gpre_overlay_proxy_preview_bundle = deps.build_gpre_overlay_proxy_preview_bundle
    resolve_gpre_quarter_open_snapshot = deps.resolve_gpre_quarter_open_snapshot
    market_build_gpre_proxy_implied_results_bundle = deps.market_build_gpre_proxy_implied_results_bundle
    persist_gpre_frozen_thesis_snapshot = deps.persist_gpre_frozen_thesis_snapshot
    gpre_underlying_margin_by_quarter: Dict[date, float] = {}
    gpre_reported_gallons_by_quarter: Dict[date, float] = {}
    gpre_reported_gallons_sold_by_quarter: Dict[date, float] = {}
    gpre_reported_gallons_produced_by_quarter: Dict[date, float] = {}
    quarterly_df: Any = None
    gpre_reported_margin_by_quarter: Dict[date, float] = {}
    gpre_denominator_policy_by_quarter: Dict[date, str] = {}
    def _gpre_material_root_score(path_in: Any) -> Tuple[int, int, str]:
        if not isinstance(path_in, Path) or not path_in.exists() or not path_in.is_dir():
            return (-1, -1, "")
        material_markers = (
            "basis_proxy",
            "corn_bids",
            "corn_futures",
            "Ethanol_futures",
            "naturalGas_futures",
            "USDA_bioenergy_reports",
            "USDA_daily_data",
            "USDA_weekly_data",
        )
        marker_count = sum(1 for marker in material_markers if (path_in / marker).exists())
        cache_penalty = 1 if str(path_in.parent.name or "").strip().lower() == "sec_cache" else 0
        try:
            path_key = str(path_in.resolve()).lower()
        except Exception:
            path_key = str(path_in).lower()
        return (marker_count, -cache_penalty, path_key)

    gpre_ticker_root_candidates = [
        cand for cand in ticker_roots
        if isinstance(cand, Path) and cand.exists() and cand.is_dir()
    ]
    gpre_ticker_root_local = (
        sorted(gpre_ticker_root_candidates, key=_gpre_material_root_score, reverse=True)[0]
        if gpre_ticker_root_candidates
        else None
    )
    gpre_bids_snapshot: Dict[str, Any] = {}
    gpre_plant_capacity_history: Dict[str, Any] = {}
    prior_q_market_snapshot: Dict[str, Any] = {}
    current_qtd_market_snapshot: Dict[str, Any] = {}
    next_quarter_thesis_snapshot: Dict[str, Any] = {}
    simple_crush_history_rows: List[Dict[str, Any]] = []
    overlay_market_snapshots_started = time.perf_counter()
    if is_gpre_profile and gpre_commercial_setup_rows:
        overlay_capacity_history_started = time.perf_counter()
        # The GPRE snapshots, history series, and fitted-model preview all use the
        # same quarter-aware plant timeline. Resolve it once here so the overlay
        # write path does not repeatedly rebuild filing-backed footprint metadata.
        gpre_plant_capacity_history = build_gpre_plant_capacity_history(
            ticker_root=gpre_ticker_root_local,
        )
        _record_writer_substage(
            "write_excel.drivers.render.economics_overlay.market_snapshots.plant_capacity_history",
            overlay_capacity_history_started,
        )
        if not str(os.environ.get("PYTEST_CURRENT_TEST") or "").strip():
            overlay_bids_snapshot_started = time.perf_counter()
            try:
                if isinstance(gpre_ticker_root_local, Path):
                    gpre_bids_snapshot = load_or_download_gpre_corn_bids_snapshot(
                        gpre_ticker_root_local,
                        as_of_date=overlay_market_as_of,
                        timeout_seconds=1.5,
                    )
                else:
                    gpre_bids_snapshot = fetch_gpre_corn_bids_snapshot(
                        as_of_date=overlay_market_as_of,
                        timeout_seconds=1.5,
                    )
            except Exception:
                gpre_bids_snapshot = {}
            _record_writer_substage(
                "write_excel.drivers.render.economics_overlay.market_snapshots.bids_snapshot",
                overlay_bids_snapshot_started,
            )
        overlay_prior_snapshot_started = time.perf_counter()
        prior_q_market_snapshot = dict(build_gpre_official_proxy_snapshot(
            economics_market_rows,
            ethanol_yield=_overlay_coefficient_detail("ethanol_yield").get("value"),
            natural_gas_usage=_overlay_coefficient_detail("natural_gas_usage").get("value"),
            as_of_date=overlay_market_as_of,
            prior_quarter=True,
            ticker_root=gpre_ticker_root_local,
            plant_capacity_history=gpre_plant_capacity_history,
        ) or {})
        _record_writer_substage(
            "write_excel.drivers.render.economics_overlay.market_snapshots.prior_snapshot",
            overlay_prior_snapshot_started,
        )
        overlay_current_snapshot_started = time.perf_counter()
        current_qtd_market_snapshot = dict(build_gpre_official_proxy_snapshot(
            economics_market_rows,
            ethanol_yield=_overlay_coefficient_detail("ethanol_yield").get("value"),
            natural_gas_usage=_overlay_coefficient_detail("natural_gas_usage").get("value"),
            as_of_date=overlay_market_as_of,
            prior_quarter=False,
            ticker_root=gpre_ticker_root_local,
            bids_snapshot=gpre_bids_snapshot,
            plant_capacity_history=gpre_plant_capacity_history,
        ) or {})
        _record_writer_substage(
            "write_excel.drivers.render.economics_overlay.market_snapshots.current_snapshot",
            overlay_current_snapshot_started,
        )
        overlay_history_series_started = time.perf_counter()
        simple_crush_history_rows = list(build_gpre_official_proxy_history_series(
            economics_market_rows,
            ethanol_yield=_overlay_coefficient_detail("ethanol_yield").get("value"),
            natural_gas_usage=_overlay_coefficient_detail("natural_gas_usage").get("value"),
            as_of_date=overlay_market_as_of,
            lookback_weeks=None,
            start_date=date(2023, 1, 1),
            ticker_root=gpre_ticker_root_local,
            bids_snapshot=gpre_bids_snapshot,
            plant_capacity_history=gpre_plant_capacity_history,
        ) or [])
        _record_writer_substage(
            "write_excel.drivers.render.economics_overlay.market_snapshots.history_series",
            overlay_history_series_started,
        )
    else:
        overlay_prior_snapshot_started = time.perf_counter()
        prior_q_market_snapshot = dict(build_prior_quarter_simple_crush_snapshot(
            economics_market_rows,
            ethanol_yield=_overlay_coefficient_detail("ethanol_yield").get("value"),
            natural_gas_usage=_overlay_coefficient_detail("natural_gas_usage").get("value"),
            as_of_date=overlay_market_as_of,
        ) or {})
        _record_writer_substage(
            "write_excel.drivers.render.economics_overlay.market_snapshots.prior_snapshot",
            overlay_prior_snapshot_started,
        )
        overlay_current_snapshot_started = time.perf_counter()
        current_qtd_market_snapshot = dict(build_current_qtd_simple_crush_snapshot(
            economics_market_rows,
            ethanol_yield=_overlay_coefficient_detail("ethanol_yield").get("value"),
            natural_gas_usage=_overlay_coefficient_detail("natural_gas_usage").get("value"),
            as_of_date=overlay_market_as_of,
        ) or {})
        _record_writer_substage(
            "write_excel.drivers.render.economics_overlay.market_snapshots.current_snapshot",
            overlay_current_snapshot_started,
        )
        overlay_history_series_started = time.perf_counter()
        simple_crush_history_rows = list(build_simple_crush_history_series(
            economics_market_rows,
            ethanol_yield=_overlay_coefficient_detail("ethanol_yield").get("value"),
            natural_gas_usage=_overlay_coefficient_detail("natural_gas_usage").get("value"),
            as_of_date=overlay_market_as_of,
            lookback_weeks=None,
            start_date=date(2023, 1, 1),
        ) or [])
        _record_writer_substage(
            "write_excel.drivers.render.economics_overlay.market_snapshots.history_series",
            overlay_history_series_started,
        )
    overlay_next_thesis_started = time.perf_counter()
    next_quarter_thesis_snapshot = dict(build_next_quarter_thesis_snapshot(
        economics_market_rows,
        as_of_date=overlay_market_as_of,
        ticker_root=gpre_ticker_root_local,
        bids_snapshot=gpre_bids_snapshot,
        plant_capacity_history=gpre_plant_capacity_history,
    ) or {})
    _record_writer_substage(
        "write_excel.drivers.render.economics_overlay.market_snapshots.next_quarter_thesis",
        overlay_next_thesis_started,
    )
    _record_writer_substage("write_excel.drivers.render.economics_overlay.market_snapshots", overlay_market_snapshots_started)
    gpre_basis_model_result: Dict[str, Any] = {}

    prior_market_status = str(prior_q_market_snapshot.get("status") or "").strip().lower()
    current_market_status = str(current_qtd_market_snapshot.get("status") or "").strip().lower()
    prior_market_available = prior_market_status == "ok_prior"
    current_market_available = current_market_status == "ok_current"
    prior_market_display_quarter = prior_q_market_snapshot.get("display_quarter") if isinstance(prior_q_market_snapshot, dict) else None
    current_market_display_quarter = current_qtd_market_snapshot.get("display_quarter") if isinstance(current_qtd_market_snapshot, dict) else None
    prior_market_display_quarter_txt = _quarter_label_short(prior_market_display_quarter) if isinstance(prior_market_display_quarter, date) else ""
    current_market_display_quarter_txt = _quarter_label_short(current_market_display_quarter) if isinstance(current_market_display_quarter, date) else ""
    next_thesis_quarter_end = next_quarter_thesis_snapshot.get("target_quarter_end") if isinstance(next_quarter_thesis_snapshot, dict) else None
    next_thesis_quarter_txt = _quarter_label_short(next_thesis_quarter_end) if isinstance(next_thesis_quarter_end, date) else ""
    prior_process_status = str(prior_q_market_snapshot.get("process_status") or "").strip().lower()
    current_process_status = str(current_qtd_market_snapshot.get("process_status") or "").strip().lower()

    def _first_gpre_ticker_root_local() -> Optional[Path]:
        return gpre_ticker_root_local

    def _precompute_gpre_reported_margin_inputs() -> Tuple[Dict[date, float], Dict[date, float], Dict[date, str], Dict[date, float], Dict[date, float], Dict[date, float]]:
        reported_map: Dict[date, float] = {}
        underlying_map: Dict[date, float] = {}
        denominator_map: Dict[date, str] = {}
        gallons_map: Dict[date, float] = {}
        sold_gallons_map: Dict[date, float] = {}
        produced_gallons_map: Dict[date, float] = {}
        ethanol_yield_num = pd.to_numeric((_overlay_coefficient_detail("ethanol_yield") or {}).get("value"), errors="coerce")
        for qd in overlay_display_quarters:
            gallon_basis = None
            basis_label = ""
            sold_rec = row_map.get(("ethanol_gallons_sold", qd))
            sold_num = pd.to_numeric((sold_rec or {}).get("Value"), errors="coerce")
            if pd.notna(sold_num) and float(sold_num) != 0.0:
                # Operating_Drivers stores sold volume in million gallons.
                # The proxy-implied service bundle expects raw gallons.
                sold_gallons_map[qd] = float(sold_num) * 1_000_000.0
            produced_rec = row_map.get(("ethanol_gallons_produced", qd))
            produced_num = pd.to_numeric((produced_rec or {}).get("Value"), errors="coerce")
            if pd.notna(produced_num) and float(produced_num) != 0.0:
                # Operating_Drivers stores produced volume in million gallons.
                # The proxy-implied service bundle expects raw gallons.
                produced_gallons_map[qd] = float(produced_num) * 1_000_000.0
            for row_key, label_txt in (("ethanol_gallons_sold", "ethanol gallons sold"), ("ethanol_gallons_produced", "ethanol gallons produced")):
                rec = row_map.get((row_key, qd))
                val_num = pd.to_numeric((rec or {}).get("Value"), errors="coerce")
                if pd.notna(val_num) and float(val_num) != 0.0:
                    gallon_basis = float(val_num)
                    basis_label = label_txt
                    break
            if gallon_basis is None and pd.notna(ethanol_yield_num):
                corn_consumed_num = pd.to_numeric((row_map.get(("corn_consumed", qd)) or {}).get("Value"), errors="coerce")
                if pd.notna(corn_consumed_num):
                    inferred_gallons = float(corn_consumed_num) * float(ethanol_yield_num)
                    if inferred_gallons != 0.0:
                        gallon_basis = inferred_gallons
                        basis_label = "estimated gallons from corn consumed and ethanol yield"
            if gallon_basis is None or abs(float(gallon_basis)) < 1e-9:
                continue
            gallons_map[qd] = float(gallon_basis)
            denominator_map[qd] = basis_label
            reported_rec = row_map.get(("consolidated_ethanol_crush_margin", qd))
            reported_num = pd.to_numeric((reported_rec or {}).get("Value"), errors="coerce")
            if pd.notna(reported_num):
                reported_map[qd] = float(reported_num) / float(gallon_basis)
            underlying_rec = row_map.get(("underlying_crush_margin", qd))
            underlying_num = pd.to_numeric((underlying_rec or {}).get("Value"), errors="coerce")
            if pd.notna(underlying_num):
                underlying_map[qd] = float(underlying_num) / float(gallon_basis)
        return reported_map, underlying_map, denominator_map, gallons_map, sold_gallons_map, produced_gallons_map

    if is_gpre_profile and gpre_commercial_setup_rows:
        overlay_gpre_basis_model_started = time.perf_counter()
        (
            gpre_reported_margin_by_quarter,
            gpre_underlying_margin_by_quarter,
            gpre_denominator_policy_by_quarter,
            gpre_reported_gallons_by_quarter,
            gpre_reported_gallons_sold_by_quarter,
            gpre_reported_gallons_produced_by_quarter,
        ) = _precompute_gpre_reported_margin_inputs()
        if not gpre_plant_capacity_history:
            gpre_plant_capacity_history = build_gpre_plant_capacity_history(
                ticker_root=_first_gpre_ticker_root_local(),
            )
        gpre_ticker_root_for_model = _first_gpre_ticker_root_local()
        gpre_ethanol_yield_for_model = pd.to_numeric((_overlay_coefficient_detail("ethanol_yield") or {}).get("value"), errors="coerce")
        gpre_gas_usage_for_model = pd.to_numeric((_overlay_coefficient_detail("natural_gas_usage") or {}).get("value"), errors="coerce")

        def _gpre_basis_model_cache_key_local() -> str:
            def _map_payload(map_in: Dict[Any, Any]) -> List[Tuple[str, Any]]:
                out: List[Tuple[str, Any]] = []
                for key_in, val_in in dict(map_in or {}).items():
                    qd_key = _parse_quarter_label_text(key_in)
                    key_txt = qd_key.isoformat() if isinstance(qd_key, date) else str(key_in)
                    num_val = pd.to_numeric(val_in, errors="coerce")
                    out.append((key_txt, None if pd.isna(num_val) else round(float(num_val), 8)))
                return sorted(out)

            market_fp = ""
            try:
                if cache_dir is not None:
                    market_fp = str(
                        market_input_fingerprint(
                            Path(cache_dir),
                            str(ticker or ""),
                            profile=company_profile,
                            include_sidecars=False,
                        ).get("fingerprint")
                        or ""
                    )
            except Exception:
                market_fp = ""
            code_identity = module_content_identity(
                Path(__file__).resolve().parent,
                ("cache_semantics.py", "excel_writer_economics_overlay_market_state.py", "market_data/service.py"),
                contract_id="gpre-basis-proxy-writer-code",
            )
            payload = {
                "as_of": overlay_market_as_of.isoformat() if isinstance(overlay_market_as_of, date) else "",
                "reported_margin": _map_payload(gpre_reported_margin_by_quarter),
                "underlying_margin": _map_payload(gpre_underlying_margin_by_quarter),
                "denominator_policy": sorted(
                    (
                        (
                            _parse_quarter_label_text(key_in).isoformat()
                            if isinstance(_parse_quarter_label_text(key_in), date)
                            else str(key_in),
                            str(val_in or ""),
                        )
                        for key_in, val_in in dict(gpre_denominator_policy_by_quarter or {}).items()
                    )
                ),
                "sold_gallons": _map_payload(gpre_reported_gallons_sold_by_quarter),
                "produced_gallons": _map_payload(gpre_reported_gallons_produced_by_quarter),
                "configuration": {
                    "ethanol_yield": None if pd.isna(gpre_ethanol_yield_for_model) else round(float(gpre_ethanol_yield_for_model), 8),
                    "natural_gas_usage": None if pd.isna(gpre_gas_usage_for_model) else round(float(gpre_gas_usage_for_model), 8),
                },
                "bids_snapshot": {
                    key: gpre_bids_snapshot.get(key)
                    for key in ("source_kind", "source_url", "snapshot_date", "as_of_date")
                    if isinstance(gpre_bids_snapshot, dict) and key in gpre_bids_snapshot
                },
                "code_identity": code_identity,
                "market_content_identity": market_fp,
                "semantic_versions": {
                    "writer_cache": GPRE_BASIS_PROXY_WRITER_CACHE_VERSION,
                },
                "ticker_profile": str(ticker or "").upper(),
            }
            return build_cache_identity(
                "gpre-basis-proxy-writer",
                payload,
                required_fields=("as_of", "code_identity", "ticker_profile"),
            ).key

        def _gpre_basis_model_cache_path_local() -> Optional[Path]:
            if not isinstance(gpre_ticker_root_for_model, Path):
                return None
            portable_root = data_root_from_sec_cache_path(Path(cache_dir)) if cache_dir is not None else None
            if portable_root is not None:
                return portable_root / "basis_proxy" / "gpre_basis_proxy_model_writer_cache.pkl"
            return gpre_ticker_root_for_model / "basis_proxy" / "gpre_basis_proxy_model_writer_cache.pkl"

        def _load_gpre_basis_model_cache_local(cache_key: str) -> Optional[Dict[str, Any]]:
            if not cache_key:
                return None
            cache_path = _gpre_basis_model_cache_path_local()
            if cache_path is None:
                return None
            try:
                payload = pd.read_pickle(cache_path)
            except Exception:
                return None
            if not isinstance(payload, dict) or str(payload.get("key") or "") != str(cache_key):
                return None
            result = payload.get("result")
            return result if isinstance(result, dict) else None

        def _save_gpre_basis_model_cache_local(cache_key: str, result: Dict[str, Any]) -> None:
            if not cache_key or not isinstance(result, dict):
                return
            cache_path = _gpre_basis_model_cache_path_local()
            if cache_path is None:
                return
            try:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                pd.to_pickle(
                    {
                        "key": cache_key,
                        "saved_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                        "result": result,
                    },
                    cache_path,
                )
            except Exception:
                pass

        gpre_basis_model_cache_key = _gpre_basis_model_cache_key_local()
        cached_gpre_basis_model_result = _load_gpre_basis_model_cache_local(gpre_basis_model_cache_key)
        if cached_gpre_basis_model_result is not None:
            gpre_basis_model_result = cached_gpre_basis_model_result
        else:
            gpre_basis_model_result = build_gpre_basis_proxy_model(
                economics_market_rows,
                ticker_root=gpre_ticker_root_for_model,
                reported_margin_by_quarter=gpre_reported_margin_by_quarter,
                underlying_margin_by_quarter=gpre_underlying_margin_by_quarter,
                denominator_policy_by_quarter=gpre_denominator_policy_by_quarter,
                reported_gallons_sold_by_quarter=gpre_reported_gallons_sold_by_quarter,
                reported_gallons_produced_by_quarter=gpre_reported_gallons_produced_by_quarter,
                as_of_date=overlay_market_as_of,
                ethanol_yield=gpre_ethanol_yield_for_model,
                natural_gas_usage=gpre_gas_usage_for_model,
                bids_snapshot=gpre_bids_snapshot,
                plant_capacity_history=gpre_plant_capacity_history,
                prior_market_snapshot=prior_q_market_snapshot,
                current_qtd_market_snapshot=current_qtd_market_snapshot,
                next_quarter_thesis_snapshot=next_quarter_thesis_snapshot,
                simple_crush_history_rows=simple_crush_history_rows,
            )
            _save_gpre_basis_model_cache_local(gpre_basis_model_cache_key, gpre_basis_model_result)
        _record_writer_substage("write_excel.drivers.render.economics_overlay.gpre_basis_model_build", overlay_gpre_basis_model_started)
    gpre_basis_quarter_map: Dict[date, Dict[str, Any]] = {}
    gpre_basis_weights_latest: List[Dict[str, Any]] = []
    gpre_official_market_rows: List[Dict[str, Any]] = []
    gpre_official_market_summary = ""
    gpre_official_weighting_method = ""
    gpre_official_ethanol_method = ""
    gpre_official_basis_method = ""
    gpre_official_gas_method = ""
    gpre_official_fallback_policy = ""
    gpre_overlay_preview_bundle: Dict[str, Any] = {}
    gpre_best_forward_preview_bundle: Dict[str, Any] = {}
    gpre_proxy_implied_results_bundle: Dict[str, Any] = {}
    gpre_current_qtd_trend_tracking: Dict[str, Any] = {}
    quarter_open_market_snapshot: Dict[str, Any] = {}
    quarter_open_proxy_status = "no_snapshot"
    quarter_open_provenance = "unavailable"
    quarter_open_display_quarter = current_market_display_quarter if isinstance(locals().get("current_market_display_quarter"), date) else None
    quarter_open_display_quarter_txt = ""
    quarter_open_subheader_txt = ""
    chosen_preview_quality = ""
    chosen_preview_mae = float("nan")
    chosen_preview_max_error = float("nan")
    chosen_preview_top_miss = ""
    chosen_preview_worst_phase = ""
    if isinstance(gpre_basis_model_result, dict):
        quarterly_df = gpre_basis_model_result.get("quarterly_df")
        weights_df = gpre_basis_model_result.get("weights_df")
        leaderboard_df = (
            gpre_basis_model_result.get("leaderboard_df")
            if isinstance(gpre_basis_model_result.get("leaderboard_df"), pd.DataFrame)
            else pd.DataFrame()
        )
        production_winner_model_key = str(
            gpre_basis_model_result.get("production_winner_model_key")
            or gpre_basis_model_result.get("gpre_proxy_model_key")
            or ""
        )
        best_forward_lens_model_key = str(gpre_basis_model_result.get("best_forward_lens_model_key") or "")
        if isinstance(quarterly_df, pd.DataFrame) and not quarterly_df.empty:
            for rec in quarterly_df.to_dict("records"):
                qd = pd.to_datetime(rec.get("quarter"), errors="coerce")
                if pd.notna(qd):
                    gpre_basis_quarter_map[pd.Timestamp(qd).date()] = rec
        if isinstance(weights_df, pd.DataFrame) and not weights_df.empty:
            plant_weights = weights_df[weights_df["model_key"].astype(str) == "plant_count_weighted"].copy()
            plant_weights["quarter_ts"] = pd.to_datetime(plant_weights["quarter"], errors="coerce")
            latest_weight_q = plant_weights["quarter_ts"].dropna().max()
            if pd.notna(latest_weight_q):
                plant_weights = plant_weights[plant_weights["quarter_ts"] == latest_weight_q].copy()
                gpre_basis_weights_latest = plant_weights.sort_values("weight", ascending=False).to_dict("records")
        gpre_official_market_rows = list(gpre_basis_model_result.get("official_market_rows") or [])
        gpre_official_market_summary = str(gpre_basis_model_result.get("official_market_summary") or "")
        gpre_official_weighting_method = str(gpre_basis_model_result.get("official_weighting_method") or "")
        gpre_official_ethanol_method = str(gpre_basis_model_result.get("official_ethanol_method") or "")
        gpre_official_basis_method = str(gpre_basis_model_result.get("official_basis_method") or "")
        gpre_official_gas_method = str(gpre_basis_model_result.get("official_gas_method") or "")
        gpre_official_fallback_policy = str(gpre_basis_model_result.get("official_fallback_policy") or "")
        gpre_overlay_preview_bundle = dict(gpre_basis_model_result.get("overlay_preview_bundle") or {})
        gpre_current_qtd_trend_tracking = dict(gpre_basis_model_result.get("current_qtd_trend_tracking") or {})
        if isinstance(state, dict):
            state["gpre_current_qtd_trend_tracking"] = dict(gpre_current_qtd_trend_tracking)
            state["gpre_current_qtd_pending_history_write"] = dict(
                (gpre_current_qtd_trend_tracking or {}).get("pending_history_write") or {}
            )
            state["gpre_current_qtd_history_store_meta"] = dict(
                (gpre_current_qtd_trend_tracking or {}).get("history_store_meta") or {}
            )
        if best_forward_lens_model_key:
            if best_forward_lens_model_key == production_winner_model_key:
                gpre_best_forward_preview_bundle = dict(gpre_overlay_preview_bundle)
            else:
                best_forward_row = (
                    leaderboard_df[leaderboard_df["model_key"].astype(str) == best_forward_lens_model_key].iloc[0].to_dict()
                    if not leaderboard_df.empty
                    and not leaderboard_df[leaderboard_df["model_key"].astype(str) == best_forward_lens_model_key].empty
                    else {}
                )
                try:
                    gpre_best_forward_preview_bundle = dict(
                        build_gpre_overlay_proxy_preview_bundle(
                            economics_market_rows,
                            ethanol_yield=pd.to_numeric((_overlay_coefficient_detail("ethanol_yield") or {}).get("value"), errors="coerce"),
                            natural_gas_usage=pd.to_numeric((_overlay_coefficient_detail("natural_gas_usage") or {}).get("value"), errors="coerce"),
                            as_of_date=overlay_market_as_of,
                            ticker_root=gpre_ticker_root_local,
                            bids_snapshot=gpre_bids_snapshot,
                            plant_capacity_history=gpre_plant_capacity_history,
                            gpre_basis_model_result={
                                "quarterly_df": quarterly_df,
                                "gpre_proxy_model_key": best_forward_lens_model_key,
                                "gpre_proxy_family": str(best_forward_row.get("family") or ""),
                                "gpre_proxy_family_label": str(best_forward_row.get("family_label") or ""),
                                "gpre_proxy_timing_rule": str(best_forward_row.get("timing_rule") or ""),
                            },
                            prior_market_snapshot=prior_q_market_snapshot,
                            current_qtd_market_snapshot=current_qtd_market_snapshot,
                            next_quarter_thesis_snapshot=next_quarter_thesis_snapshot,
                            simple_crush_history_rows=simple_crush_history_rows,
                        )
                        or {}
                    )
                except Exception:
                    gpre_best_forward_preview_bundle = {}
        gpre_proxy_implied_results_bundle = dict(
            gpre_basis_model_result.get("proxy_implied_results")
            or gpre_overlay_preview_bundle.get("proxy_implied_results")
            or {}
        )
        chosen_preview_quality = str(gpre_basis_model_result.get("gpre_proxy_live_preview_quality_status") or "").strip()
        chosen_preview_mae = pd.to_numeric(gpre_basis_model_result.get("gpre_proxy_live_preview_mae"), errors="coerce")
        chosen_preview_max_error = pd.to_numeric(gpre_basis_model_result.get("gpre_proxy_live_preview_max_error"), errors="coerce")
        chosen_preview_top_miss = str(gpre_basis_model_result.get("gpre_proxy_live_preview_top_miss_quarters") or "").strip()
        chosen_preview_worst_phase = str(gpre_basis_model_result.get("gpre_proxy_live_preview_worst_phase") or "").strip()
        quarter_open_market_snapshot = dict(gpre_overlay_preview_bundle.get("quarter_open_market_snapshot") or {})
        quarter_open_proxy_status = str(gpre_overlay_preview_bundle.get("quarter_open_snapshot_status") or "no_snapshot")
        quarter_open_provenance = str(gpre_overlay_preview_bundle.get("quarter_open_provenance") or quarter_open_market_snapshot.get("quarter_open_provenance") or "unavailable")
        quarter_open_display_quarter = gpre_overlay_preview_bundle.get("quarter_open_target_quarter_end")
        quarter_open_display_quarter_txt = _quarter_label_short(quarter_open_display_quarter) if isinstance(quarter_open_display_quarter, date) else ""
        quarter_open_has_market_inputs = bool(
            isinstance(quarter_open_market_snapshot, dict)
            and str(quarter_open_market_snapshot.get("status") or "").strip().lower() not in {"", "no_snapshot", "no_data"}
            and isinstance(quarter_open_market_snapshot.get("current_market"), dict)
            and any(
                pd.notna(pd.to_numeric((quarter_open_market_snapshot.get("current_market") or {}).get(key), errors="coerce"))
                for key in ("corn_price", "cbot_corn_front_price", "ethanol_price", "natural_gas_price")
            )
        )
        if (
            not quarter_open_has_market_inputs
            and isinstance(gpre_ticker_root_local, Path)
            and isinstance(current_market_display_quarter, date)
        ):
            try:
                # Keep the visible market-input frame tied to the same resolver used
                # by the service layer. This preserves the quarter-open contract:
                # frozen prior-quarter snapshot first, then the closest eligible
                # local manual/futures snapshot at the quarter boundary.
                resolved_quarter_open = resolve_gpre_quarter_open_snapshot(
                    gpre_ticker_root_local,
                    current_quarter_end=current_market_display_quarter,
                    rows=economics_market_rows,
                    ethanol_yield=pd.to_numeric((_overlay_coefficient_detail("ethanol_yield") or {}).get("value"), errors="coerce"),
                    natural_gas_usage=pd.to_numeric((_overlay_coefficient_detail("natural_gas_usage") or {}).get("value"), errors="coerce"),
                    bids_snapshot=gpre_bids_snapshot,
                    plant_capacity_history=gpre_plant_capacity_history,
                )
            except Exception:
                resolved_quarter_open = {}
            resolved_snapshot = (
                resolved_quarter_open.get("official_market_snapshot")
                if isinstance(resolved_quarter_open, dict)
                else None
            )
            if isinstance(resolved_snapshot, dict) and str(resolved_quarter_open.get("status") or "") == "ok":
                quarter_open_market_snapshot = dict(resolved_snapshot)
                quarter_open_proxy_status = str(resolved_quarter_open.get("status") or "ok")
                quarter_open_provenance = str(
                    resolved_quarter_open.get("provenance")
                    or quarter_open_market_snapshot.get("quarter_open_provenance")
                    or quarter_open_provenance
                )
                quarter_open_display_quarter = resolved_quarter_open.get("target_quarter_end") or current_market_display_quarter
                quarter_open_display_quarter_txt = _quarter_label_short(quarter_open_display_quarter) if isinstance(quarter_open_display_quarter, date) else ""
                gpre_overlay_preview_bundle["quarter_open_market_snapshot"] = quarter_open_market_snapshot
                gpre_overlay_preview_bundle["quarter_open_snapshot_status"] = quarter_open_proxy_status
                gpre_overlay_preview_bundle["quarter_open_provenance"] = quarter_open_provenance
                gpre_overlay_preview_bundle["quarter_open_target_quarter_end"] = quarter_open_display_quarter
                gpre_overlay_preview_bundle["quarter_open_snapshot_as_of"] = resolved_quarter_open.get("snapshot_as_of")
                gpre_overlay_preview_bundle["quarter_open_official_proxy_usd_per_gal"] = resolved_quarter_open.get("official_simple_proxy_usd_per_gal")
                gpre_overlay_preview_bundle["quarter_open_gpre_proxy_usd_per_gal"] = resolved_quarter_open.get("gpre_proxy_official_usd_per_gal")
                gpre_overlay_preview_bundle["quarter_open_market_inputs"] = dict(quarter_open_market_snapshot.get("current_market") or {})
                gpre_overlay_preview_bundle["quarter_open_process_inputs"] = dict(quarter_open_market_snapshot.get("current_process") or {})
                official_frames = dict((gpre_overlay_preview_bundle.get("official_frames") or {}))
                official_frame = dict(official_frames.get("quarter_open") or {})
                official_frame.update(
                    {
                        "quarter_end": quarter_open_display_quarter,
                        "value": resolved_quarter_open.get("official_simple_proxy_usd_per_gal"),
                        "status": quarter_open_proxy_status,
                    }
                )
                official_frames["quarter_open"] = official_frame
                gpre_overlay_preview_bundle["official_frames"] = official_frames
                gpre_frames = dict((gpre_overlay_preview_bundle.get("gpre_proxy_frames") or {}))
                gpre_frame = dict(gpre_frames.get("quarter_open") or {})
                gpre_frame.update(
                    {
                        "quarter_end": quarter_open_display_quarter,
                        "value": resolved_quarter_open.get("gpre_proxy_official_usd_per_gal"),
                        "status": quarter_open_proxy_status,
                    }
                )
                gpre_frames["quarter_open"] = gpre_frame
                gpre_overlay_preview_bundle["gpre_proxy_frames"] = gpre_frames
                try:
                    # The proxy-implied bridge is derived from the frame map.
                    # If the writer has to repair a stale/missing quarter-open
                    # preview from the resolver, refresh the bridge bundle here
                    # so the visible $m panel stays in lockstep with the $/gal
                    # proxy comparison.
                    gpre_proxy_implied_results_bundle = dict(
                        market_build_gpre_proxy_implied_results_bundle(
                            gpre_overlay_preview_bundle,
                            reported_gallons_produced_by_quarter=gpre_reported_gallons_produced_by_quarter,
                            denominator_policy_by_quarter=gpre_denominator_policy_by_quarter,
                            ticker_root=gpre_ticker_root_local,
                            plant_capacity_history=gpre_plant_capacity_history,
                        )
                        or {}
                    )
                    if gpre_proxy_implied_results_bundle:
                        gpre_overlay_preview_bundle["proxy_implied_results"] = dict(gpre_proxy_implied_results_bundle)
                except Exception:
                    pass
                gpre_basis_model_result["overlay_preview_bundle"] = dict(gpre_overlay_preview_bundle)
                if gpre_proxy_implied_results_bundle:
                    gpre_basis_model_result["proxy_implied_results"] = dict(gpre_proxy_implied_results_bundle)
        if quarter_open_display_quarter_txt and quarter_open_provenance == "frozen_snapshot":
            quarter_open_subheader_txt = ""
        elif quarter_open_display_quarter_txt and quarter_open_provenance == "manual_local_snapshot":
            quarter_open_subheader_txt = ""
        else:
            quarter_open_subheader_txt = ""
        frozen_snapshot_entry = gpre_basis_model_result.get("next_thesis_frozen_snapshot_entry")
        if isinstance(frozen_snapshot_entry, dict) and gpre_ticker_root_local is not None:
            try:
                persist_gpre_frozen_thesis_snapshot(gpre_ticker_root_local, frozen_snapshot_entry)
            except Exception:
                pass
    if not quarter_open_market_snapshot:
        quarter_open_market_snapshot = {
            "status": "no_snapshot",
            "display_quarter": current_market_display_quarter if isinstance(current_market_display_quarter, date) else None,
            "calendar_quarter": current_market_display_quarter if isinstance(current_market_display_quarter, date) else None,
            "message": (
                f"No frozen prior-quarter thesis snapshot for {current_market_display_quarter_txt}."
                if current_market_display_quarter_txt
                else "No frozen prior-quarter thesis snapshot."
            ),
            "current_market": {},
            "current_process": {},
            "market_meta": {},
            "process_status": "no_data",
            "quarter_open_provenance": quarter_open_provenance,
        }
        if not quarter_open_display_quarter_txt and isinstance(current_market_display_quarter, date):
            quarter_open_display_quarter = current_market_display_quarter
            quarter_open_display_quarter_txt = current_market_display_quarter_txt
            quarter_open_subheader_txt = ""



    return EconomicsOverlayMarketStateResult(
        gpre_ticker_root_local=gpre_ticker_root_local,
        gpre_bids_snapshot=gpre_bids_snapshot,
        gpre_plant_capacity_history=gpre_plant_capacity_history,
        prior_q_market_snapshot=prior_q_market_snapshot,
        current_qtd_market_snapshot=current_qtd_market_snapshot,
        next_quarter_thesis_snapshot=next_quarter_thesis_snapshot,
        simple_crush_history_rows=simple_crush_history_rows,
        gpre_basis_model_result=gpre_basis_model_result,
        prior_market_status=prior_market_status,
        current_market_status=current_market_status,
        prior_market_available=prior_market_available,
        current_market_available=current_market_available,
        prior_market_display_quarter=prior_market_display_quarter,
        current_market_display_quarter=current_market_display_quarter,
        prior_market_display_quarter_txt=prior_market_display_quarter_txt,
        current_market_display_quarter_txt=current_market_display_quarter_txt,
        next_thesis_quarter_end=next_thesis_quarter_end,
        next_thesis_quarter_txt=next_thesis_quarter_txt,
        prior_process_status=prior_process_status,
        current_process_status=current_process_status,
        gpre_reported_margin_by_quarter=gpre_reported_margin_by_quarter,
        gpre_underlying_margin_by_quarter=gpre_underlying_margin_by_quarter,
        gpre_denominator_policy_by_quarter=gpre_denominator_policy_by_quarter,
        gpre_reported_gallons_by_quarter=gpre_reported_gallons_by_quarter,
        gpre_reported_gallons_sold_by_quarter=gpre_reported_gallons_sold_by_quarter,
        gpre_reported_gallons_produced_by_quarter=gpre_reported_gallons_produced_by_quarter,
        gpre_basis_quarter_map=gpre_basis_quarter_map,
        gpre_basis_weights_latest=gpre_basis_weights_latest,
        gpre_official_market_rows=gpre_official_market_rows,
        gpre_official_market_summary=gpre_official_market_summary,
        gpre_official_weighting_method=gpre_official_weighting_method,
        gpre_official_ethanol_method=gpre_official_ethanol_method,
        gpre_official_basis_method=gpre_official_basis_method,
        gpre_official_gas_method=gpre_official_gas_method,
        gpre_official_fallback_policy=gpre_official_fallback_policy,
        gpre_overlay_preview_bundle=gpre_overlay_preview_bundle,
        gpre_best_forward_preview_bundle=gpre_best_forward_preview_bundle,
        gpre_proxy_implied_results_bundle=gpre_proxy_implied_results_bundle,
        gpre_current_qtd_trend_tracking=gpre_current_qtd_trend_tracking,
        quarter_open_market_snapshot=quarter_open_market_snapshot,
        quarter_open_proxy_status=quarter_open_proxy_status,
        quarter_open_provenance=quarter_open_provenance,
        quarter_open_display_quarter=quarter_open_display_quarter,
        quarter_open_display_quarter_txt=quarter_open_display_quarter_txt,
        quarter_open_subheader_txt=quarter_open_subheader_txt,
        chosen_preview_quality=chosen_preview_quality,
        chosen_preview_mae=chosen_preview_mae,
        chosen_preview_max_error=chosen_preview_max_error,
        chosen_preview_top_miss=chosen_preview_top_miss,
        chosen_preview_worst_phase=chosen_preview_worst_phase,
        quarterly_df=quarterly_df,
    )
