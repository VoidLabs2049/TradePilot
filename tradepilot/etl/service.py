"""Executable Stage B ETL orchestration service."""

from __future__ import annotations

from calendar import monthrange
from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
import json
import math
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from tradepilot import db
from tradepilot.etl.constants import (
    ETF_AW_REQUIRED_ROLES,
    ETF_AW_ROLE_ORDER,
    ETF_AW_ROLE_RANK,
)
from tradepilot.etl.datasets import DatasetDefinition
from tradepilot.etl.etf_aw_universe import (
    ETF_AW_SLEEVES,
    ETF_AW_SLEEVE_CODE_BY_ROLE,
    ETF_AW_SLEEVE_CODES,
    ETF_AW_SLEEVE_ROLES,
    ETF_AW_SLEEVE_ROLE_ORDER,
    etf_aw_role_sort_key,
    etf_aw_sleeve_codes_frame,
    etf_aw_sleeves_frame,
)
from tradepilot.etl.models import (
    CanonicalWriteResult,
    DatasetSyncResult,
    DependencyType,
    IngestionRequest,
    RunStatus,
    SourceFetchResult,
    StorageZone,
    TriggerMode,
    ValidationResultRecord,
    ValidationStatus,
    normalize_request_window,
)
from tradepilot.etl.normalizers import get_normalizer
from tradepilot.etl.registry import DatasetRegistry, register_stage_b_datasets
from tradepilot.etl.sources import BaseSourceAdapter, TushareSourceAdapter
from tradepilot.etl.storage import (
    build_dataset_file_path,
    cleanup_temp_files,
    write_dataset_parquet,
    write_raw_parquet,
)
from tradepilot.etl.validators import (
    get_validator,
    has_blocking_failures,
    validation_counts,
)

_ID_SEQUENCES: dict[tuple[str, str], str] = {
    ("etl_ingestion_runs", "run_id"): "etl_ingestion_runs_run_id_seq",
    ("etl_raw_batches", "raw_batch_id"): "etl_raw_batches_raw_batch_id_seq",
    (
        "etl_validation_results",
        "validation_id",
    ): "etl_validation_results_validation_id_seq",
}

_TRADING_CALENDAR_FULL_HISTORY_PROFILE = "reference.trading_calendar.full_history"
_TRADING_CALENDAR_HISTORY_START = date(2016, 1, 1)
_TRADING_CALENDAR_BOOTSTRAP_EXCHANGES = ["SH", "SZ"]
_REBALANCE_CALENDAR_MONTHLY_PROFILE = "reference.rebalance_calendar.monthly_post_20"
_REBALANCE_CALENDAR_NAME = "etf_aw_v1_monthly_post_20"
_REBALANCE_ANCHOR_DAY = 20
_ETF_AW_SLEEVES_PROFILE = "reference.etf_aw_sleeves.frozen_v1"
_ETF_AW_SLEEVE_DAILY_PROFILE = "derived.etf_aw_sleeve_daily.build"
_ETF_AW_REBALANCE_SNAPSHOT_PROFILE = "derived.etf_aw_rebalance_snapshot.build"
_ETF_AW_REBALANCE_SNAPSHOT_DATASET = "derived.etf_aw_rebalance_snapshot"
_ETF_AW_REGIME_SCORE_PROFILE = "derived.etf_aw_regime_score.build"
_ETF_AW_REGIME_SCORE_DATASET = "derived.etf_aw_regime_score"
_ETF_AW_REGIME_SCHEMA_VERSION = "etf_aw_regime_score_v1"
_ETF_AW_REGIME_SCORER_NAME = "etf_aw_market_only_regime"
_ETF_AW_REGIME_SCORER_VERSION = "v1"
_ETF_AW_MARKET_FEATURES_PROFILE = "derived.etf_aw_market_features.build"
_ETF_AW_MARKET_FEATURES_DATASET = "derived.etf_aw_market_features"
_ETF_AW_MARKET_FEATURES_SCHEMA_VERSION = "etf_aw_market_features_v1"
_ETF_AW_STRATEGY_CONTEXT_PROFILE = "derived.etf_aw_strategy_context.build"
_ETF_AW_STRATEGY_CONTEXT_DATASET = "derived.etf_aw_strategy_context"
_ETF_AW_STRATEGY_CONTEXT_SCHEMA_VERSION = "etf_aw_strategy_context_v1"
_ETF_AW_STRATEGY_CONTEXT_CONTRACT_VERSION = "etf_aw_strategy_context_contract_v1"
_ETF_AW_RISK_BUDGET_PROFILE = "derived.etf_aw_risk_budget.build"
_ETF_AW_RISK_BUDGET_DATASET = "derived.etf_aw_risk_budget"
_ETF_AW_RISK_BUDGET_SCHEMA_VERSION = "etf_aw_risk_budget_v1"
_ETF_AW_RISK_BUDGET_CONTRACT_VERSION = "etf_aw_risk_budget_contract_v1"
_ETF_AW_RISK_BUDGET_STRATEGY_VERSION = "risk_budget_v1"
_ETF_AW_TARGET_WEIGHT_PROFILE = "derived.etf_aw_target_weight.build"
_ETF_AW_TARGET_WEIGHT_DATASET = "derived.etf_aw_target_weight"
_ETF_AW_TARGET_WEIGHT_SCHEMA_VERSION = "etf_aw_target_weight_v1"
_ETF_AW_TARGET_WEIGHT_CONTRACT_VERSION = "etf_aw_target_weight_contract_v1"
_ETF_AW_TARGET_WEIGHT_STRATEGY_VERSION = "target_weight_inverse_vol_v1"
_ETF_AW_TARGET_WEIGHT_STATUSES = {
    "complete",
    "partial",
    "stale",
    "missing",
    "unavailable",
}
_ETF_AW_TARGET_WEIGHT_VOL_WINDOW = 63
_ETF_AW_TARGET_WEIGHT_MIN_OBSERVATIONS = 42
_ETF_AW_TARGET_WEIGHT_PANEL_LOOKBACK_DAYS = _ETF_AW_TARGET_WEIGHT_VOL_WINDOW * 3
_ETF_AW_TARGET_WEIGHT_VOL_FLOOR = 0.005
_ETF_AW_TARGET_WEIGHT_NO_TRADE_BAND = 0.0025
_ETF_AW_TARGET_WEIGHT_CAPS = {
    "equity_large": 0.45,
    "equity_small": 0.45,
    "bond": 0.45,
    "gold": 0.45,
    "cash": 0.35,
}
_ETF_AW_TARGET_WEIGHT_RISK_BUDGET_COLUMNS = {
    "calendar_name",
    "rebalance_date",
    "strategy_name",
    "strategy_version",
    "sleeve_role",
    "tilted_budget",
    "budget_status",
    "source_strategy_context_rebalance_date",
    "source_regime_rebalance_date",
}
_ETF_AW_TARGET_WEIGHT_PANEL_RETURN_COLUMNS = {
    "sleeve_code",
    "trade_date",
}
_ETF_AW_TARGET_WEIGHT_PREVIOUS_COLUMNS = {
    "calendar_name",
    "rebalance_date",
    "strategy_name",
    "strategy_version",
    "sleeve_code",
    "target_weight",
}
_ETF_AW_BACKTEST_KERNEL_PROFILE = "derived.etf_aw_backtest_kernel.build"
_ETF_AW_BACKTEST_KERNEL_DATASET = "derived.etf_aw_backtest_kernel"
_ETF_AW_BACKTEST_KERNEL_SCHEMA_VERSION = "etf_aw_backtest_kernel_v1"
_ETF_AW_BACKTEST_KERNEL_STRATEGY_NAME = "equal_weight_fixture"
_ETF_AW_BACKTEST_KERNEL_STRATEGY_VERSION = "fixture_v1"
_ETF_AW_BASELINE_WEIGHT_PROFILE = "derived.etf_aw_baseline_weight.build"
_ETF_AW_BASELINE_WEIGHT_DATASET = "derived.etf_aw_baseline_weight"
_ETF_AW_BASELINE_WEIGHT_SCHEMA_VERSION = "etf_aw_baseline_weight_v1"
_ETF_AW_BASELINE_WEIGHT_CONTRACT_VERSION = "etf_aw_baseline_weight_contract_v1"
_ETF_AW_BASELINE_NAME = "static_inverse_vol"
_ETF_AW_BASELINE_VERSION = "static_inverse_vol_v1"
_ETF_AW_BASELINE_VOL_WINDOW = 63
_ETF_AW_BASELINE_MIN_OBSERVATIONS = 42
_ETF_AW_BASELINE_PANEL_LOOKBACK_DAYS = _ETF_AW_BASELINE_VOL_WINDOW * 3
_ETF_AW_BACKTEST_WEIGHT_SOURCE_TARGET = "target_weight"
_ETF_AW_BACKTEST_WEIGHT_SOURCE_BASELINE = "baseline"
_ETF_AW_BACKTEST_WEIGHT_SOURCE_ALIASES = {
    "baseline_weight": _ETF_AW_BACKTEST_WEIGHT_SOURCE_BASELINE,
}
_ETF_AW_BACKTEST_WEIGHT_SOURCES = {
    _ETF_AW_BACKTEST_WEIGHT_SOURCE_TARGET,
    _ETF_AW_BACKTEST_WEIGHT_SOURCE_BASELINE,
}
_ETF_AW_MONTHLY_EXPLAINABILITY_PROFILE = "derived.etf_aw_monthly_explainability.build"
_ETF_AW_MONTHLY_EXPLAINABILITY_DATASET = "derived.etf_aw_monthly_explainability"
_ETF_AW_MONTHLY_EXPLAINABILITY_SCHEMA_VERSION = "etf_aw_monthly_explainability_v1"
_ETF_AW_STRATEGY_NAME = "etf_aw_v1"
_ETF_AW_STRATEGY_VERSION = "stage_g_v1"
_ETF_AW_SLEEVE_DAILY_RETURN_LOOKBACK_DAYS = 31
_ETF_AW_SNAPSHOT_LOOKBACK_DAYS = 420
_ETF_AW_SNAPSHOT_WINDOWS = {
    "return_1m": (21, 15),
    "return_3m": (63, 45),
    "return_6m": (126, 90),
    "volatility_3m": (63, 45),
    "max_drawdown_6m": (126, 90),
}
_ETF_AW_SNAPSHOT_STATUSES = {"complete", "partial", "missing", "stale"}
_ETF_AW_REGIME_STATUSES = {"complete", "degraded", "unavailable"}
_ETF_AW_REGIME_LABELS = {
    "risk_on",
    "defensive",
    "hedge_bid",
    "mixed",
    "insufficient_data",
}
_ETF_AW_MARKET_FEATURE_STATUSES = {"complete", "partial", "missing", "stale"}
_ETF_AW_MARKET_FEATURE_SCOPE_NAMES = {
    "sleeve": {
        "direction_score",
        "return_1m",
        "return_3m",
        "return_6m",
        "volatility_3m",
        "max_drawdown_6m",
    },
    "group": {"equity_score", "bond_score", "gold_score", "cash_score"},
    "regime": {
        "market_score",
        "market_confidence_score",
        "market_confidence_cap",
    },
}
_ETF_AW_MARKET_FEATURE_UNITS = {
    "direction_score": "score",
    "return_1m": "decimal_return",
    "return_3m": "decimal_return",
    "return_6m": "decimal_return",
    "volatility_3m": "ratio",
    "max_drawdown_6m": "decimal_return",
    "equity_score": "score",
    "bond_score": "score",
    "gold_score": "score",
    "cash_score": "score",
    "market_score": "score",
    "market_confidence_score": "ratio",
    "market_confidence_cap": "ratio",
}
_ETF_AW_STRATEGY_CONTEXT_STATUSES = {
    "complete",
    "partial",
    "stale",
    "unavailable",
}
_ETF_AW_READINESS_LEVELS = {
    "research_ready",
    "degraded_research",
    "not_ready",
}
_ETF_AW_CONTEXT_BASES = {
    "market_only",
    "market_plus_rates",
    "market_plus_macro_rates",
}
_ETF_AW_RISK_BUDGET_STATUSES = {
    "complete",
    "partial",
    "stale",
    "missing",
    "unavailable",
}
_ETF_AW_RISK_BUDGET_BASE = {
    "equity_large": 0.20,
    "equity_small": 0.20,
    "bond": 0.20,
    "gold": 0.20,
    "cash": 0.20,
}
_ETF_AW_RISK_BUDGET_DELTAS = {
    "risk_on": {
        "equity_large": 0.05,
        "equity_small": 0.05,
        "bond": -0.02,
        "gold": -0.03,
        "cash": -0.05,
    },
    "hedge_bid": {
        "equity_large": -0.04,
        "equity_small": -0.05,
        "bond": 0.02,
        "gold": 0.05,
        "cash": 0.02,
    },
    "defensive": {
        "equity_large": -0.05,
        "equity_small": -0.05,
        "bond": 0.05,
        "gold": 0.01,
        "cash": 0.04,
    },
    "mixed": {
        "equity_large": 0.0,
        "equity_small": 0.0,
        "bond": 0.0,
        "gold": 0.0,
        "cash": 0.0,
    },
    "insufficient_data": {
        "equity_large": 0.0,
        "equity_small": 0.0,
        "bond": 0.0,
        "gold": 0.0,
        "cash": 0.0,
    },
}
_ETF_AW_MACRO_RATES_CONTEXT_STATUSES = {
    "complete",
    "partial",
    "stale",
    "unavailable",
    "deferred",
}
_ETF_AW_STAGE_G_DEFERRED_PRIMARY_FIELDS = [
    "official_pmi",
    "cn_gov_10y_yield",
]
_ETF_AW_STAGE_G_DEFERRED_CONFIRMATORY_FIELDS = [
    "cn_yield_curve_slope_10y_1y",
]
_ETF_AW_FORBIDDEN_STRATEGY_FIELD_TOKENS = {
    "target_weight",
    "target_weights",
    "risk_budget",
    "trade_action",
    "order_instruction",
    "buy_list",
    "sell_list",
}
_ETF_AW_RISK_BUDGET_VALIDATION_CHECKS = (
    "non_empty",
    "required_columns_present",
    "no_duplicate_business_keys",
    "five_roles_per_rebalance_date",
    "budget_sums_valid",
    "status_values_allowed",
    "quality_notes_json",
    "forbidden_fields_absent",
    "point_in_time_sources",
    "market_regime_label_allowed_for_tilt",
)
_ETF_AW_DIRECTION_RULES = {
    # Return thresholds are decimal returns, e.g. 0.015 means 1.5%.
    "return_1m": (0.015, -0.015, 0.25),
    "return_3m": (0.030, -0.030, 0.45),
    "return_6m": (0.050, -0.050, 0.30),
}
_ETF_AW_SLEEVES = ETF_AW_SLEEVES
_ETF_AW_SLEEVE_CODES = ETF_AW_SLEEVE_CODES
_ETF_AW_SLEEVE_ROLES = ETF_AW_SLEEVE_ROLES


class ETLService:
    """Application service for Stage B single-dataset syncs."""

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection | None = None,
        registry: DatasetRegistry | None = None,
        source_adapters: Iterable[BaseSourceAdapter] | None = None,
        lakehouse_root: Path | None = None,
    ) -> None:
        self.conn = conn or db.get_conn()
        db.ensure_stage_b_sequences(self.conn)
        self.registry = registry or DatasetRegistry()
        register_stage_b_datasets(self.registry)
        adapters = list(source_adapters or [TushareSourceAdapter()])
        self.source_adapters = {adapter.source_name: adapter for adapter in adapters}
        self.lakehouse_root = lakehouse_root

    def run_dataset_sync(
        self, dataset_name: str, request: IngestionRequest
    ) -> DatasetSyncResult:
        """Run one dataset through fetch, raw landing, normalize, validate, and load."""

        definition = self.registry.get_dataset(dataset_name)
        source = self._source_adapter(definition)
        cleanup_temp_files(dataset_name, lakehouse_root=self.lakehouse_root)
        self._mark_stale_running_runs(dataset_name)
        self._ensure_source_registry(source.source_name)

        run_id = self._next_id("etl_ingestion_runs", "run_id")
        started_at = _utc_now()
        self._insert_run(run_id, definition, source.source_name, request, started_at)
        raw_batch_ids: list[int] = []
        watermark_updated = False
        try:
            dependency_results = self._ensure_dependencies(definition, request, run_id)
            if dependency_results:
                self._persist_validation_results(dependency_results)
                if has_blocking_failures(dependency_results):
                    raise RuntimeError("dependency preflight failed")

            effective_request = self._augment_market_request(definition, request)
            fetch_result = source.fetch(dataset_name, effective_request)
            self._assert_source_contract(fetch_result)

            raw_batch_id = self._next_id("etl_raw_batches", "raw_batch_id")
            raw_batch_ids.append(raw_batch_id)
            raw_partition = fetch_result.partition_hints or _raw_partition_hints(
                definition.dataset_name, effective_request
            )
            raw_write = write_raw_parquet(
                fetch_result.payload,
                dataset_name=dataset_name,
                partition_parts=raw_partition.items(),
                raw_batch_id=raw_batch_id,
                lakehouse_root=self.lakehouse_root,
            )
            self._insert_raw_batch(raw_batch_id, run_id, fetch_result, raw_write)

            context = dict(effective_request.context)
            context.update(
                {
                    "dataset_name": dataset_name,
                    "source_name": source.source_name,
                    "raw_batch_id": raw_batch_id,
                    "run_id": run_id,
                    "conn": self.conn,
                    "instrument_type": _instrument_type_for_dataset(dataset_name),
                }
            )
            normalizer = get_normalizer(dataset_name)
            normalized = normalizer.normalize(fetch_result.payload, context)
            canonical = normalized.canonical_payload

            validator = get_validator(dataset_name)
            validation_results = self._source_payload_validation(
                definition, fetch_result, run_id, raw_batch_id
            )
            validation_results.extend(validator.validate(canonical, context))
            counts = validation_counts(validation_results)
            self._persist_validation_results(validation_results)

            if has_blocking_failures(validation_results):
                records_failed = sum(
                    1
                    for result in validation_results
                    if result.status == ValidationStatus.FAIL
                )
                self._finish_run(
                    run_id,
                    RunStatus.FAILED,
                    records_discovered=fetch_result.row_count,
                    records_failed=records_failed,
                    error_message="validation failed",
                )
                return DatasetSyncResult(
                    run_id=run_id,
                    dataset_name=dataset_name,
                    status=RunStatus.FAILED,
                    raw_batch_ids=raw_batch_ids,
                    validation_counts=counts,
                    records_discovered=fetch_result.row_count,
                    records_written=0,
                    watermark_updated=False,
                    started_at=started_at,
                    finished_at=_utc_now(),
                    error_message="validation failed",
                )

            quality_status = _quality_status(validation_results)
            if "quality_status" in canonical.columns:
                canonical = canonical.copy()
                canonical["quality_status"] = quality_status

            write_result = self._write_canonical(definition, canonical)
            if not canonical.empty:
                self._advance_watermark(
                    definition, source.source_name, run_id, canonical
                )
                watermark_updated = True
            self._finish_run(
                run_id,
                RunStatus.SUCCESS,
                records_discovered=fetch_result.row_count,
                records_inserted=write_result.records_inserted,
                records_updated=write_result.records_updated,
                partitions_written=write_result.partitions_written,
            )
            finished_at = _utc_now()
            return DatasetSyncResult(
                run_id=run_id,
                dataset_name=dataset_name,
                status=RunStatus.SUCCESS,
                raw_batch_ids=raw_batch_ids,
                validation_counts=counts,
                records_discovered=fetch_result.row_count,
                records_written=write_result.records_written,
                watermark_updated=watermark_updated,
                started_at=started_at,
                finished_at=finished_at,
            )
        except Exception as exc:
            self._finish_run(
                run_id,
                RunStatus.FAILED,
                records_discovered=0,
                records_failed=1,
                error_message=str(exc),
            )
            return DatasetSyncResult(
                run_id=run_id,
                dataset_name=dataset_name,
                status=RunStatus.FAILED,
                raw_batch_ids=raw_batch_ids,
                validation_counts={},
                records_discovered=0,
                records_written=0,
                watermark_updated=watermark_updated,
                started_at=started_at,
                finished_at=_utc_now(),
                error_message=str(exc),
            )

    def run_multi_dataset_sync(
        self,
        dataset_names: list[str],
        request: IngestionRequest,
    ) -> dict[str, DatasetSyncResult]:
        """Run datasets sequentially without Stage C profile scheduling."""

        return {
            dataset_name: self.run_dataset_sync(dataset_name, request)
            for dataset_name in dataset_names
        }

    def run_bootstrap(
        self,
        profile_name: str,
        *,
        start: date | None = None,
        end: date | None = None,
    ) -> dict:
        """Run a narrow Stage C materialization profile.

        Source-backed datasets keep using run_dataset_sync. These profiles cover
        static or derived datasets until they have first-class source adapters.
        """

        if profile_name == _TRADING_CALENDAR_FULL_HISTORY_PROFILE:
            return self._bootstrap_trading_calendar_full_history(
                start or _TRADING_CALENDAR_HISTORY_START,
                end or date.today(),
            )
        if profile_name == _REBALANCE_CALENDAR_MONTHLY_PROFILE:
            return self._bootstrap_rebalance_calendar_monthly_post_20(
                start or _TRADING_CALENDAR_HISTORY_START,
                end or date.today(),
            )
        if profile_name == _ETF_AW_SLEEVES_PROFILE:
            return self._bootstrap_etf_aw_sleeves()
        if profile_name == _ETF_AW_SLEEVE_DAILY_PROFILE:
            return self._build_etf_aw_sleeve_daily(
                start or _TRADING_CALENDAR_HISTORY_START,
                end or date.today(),
            )
        if profile_name == _ETF_AW_REBALANCE_SNAPSHOT_PROFILE:
            return self._build_etf_aw_rebalance_snapshot(
                start or _TRADING_CALENDAR_HISTORY_START,
                end or date.today(),
            )
        if profile_name == _ETF_AW_REGIME_SCORE_PROFILE:
            return self._build_etf_aw_regime_score(
                start or _TRADING_CALENDAR_HISTORY_START,
                end or date.today(),
            )
        if profile_name == _ETF_AW_MARKET_FEATURES_PROFILE:
            return self._build_etf_aw_market_features(
                start or _TRADING_CALENDAR_HISTORY_START,
                end or date.today(),
            )
        if profile_name == _ETF_AW_STRATEGY_CONTEXT_PROFILE:
            return self._build_etf_aw_strategy_context(
                start or _TRADING_CALENDAR_HISTORY_START,
                end or date.today(),
            )
        if profile_name == _ETF_AW_RISK_BUDGET_PROFILE:
            return self._build_etf_aw_risk_budget(
                start or _TRADING_CALENDAR_HISTORY_START,
                end or date.today(),
            )
        if profile_name == _ETF_AW_TARGET_WEIGHT_PROFILE:
            return self._build_etf_aw_target_weight(
                start or _TRADING_CALENDAR_HISTORY_START,
                end or date.today(),
            )
        if profile_name == _ETF_AW_BASELINE_WEIGHT_PROFILE:
            return self._build_etf_aw_baseline_weight(
                start or _TRADING_CALENDAR_HISTORY_START,
                end or date.today(),
            )
        if profile_name == _ETF_AW_BACKTEST_KERNEL_PROFILE:
            return self._build_etf_aw_backtest_kernel(
                start or _TRADING_CALENDAR_HISTORY_START,
                end or date.today(),
            )
        if profile_name == _ETF_AW_MONTHLY_EXPLAINABILITY_PROFILE:
            return self._build_etf_aw_monthly_explainability(
                start or _TRADING_CALENDAR_HISTORY_START,
                end or date.today(),
            )
        raise KeyError(f"unsupported bootstrap profile: {profile_name}")

    def list_runs(self, dataset_name: str | None = None) -> list[dict]:
        """List ETL run history."""

        if dataset_name is None:
            rows = self.conn.execute(
                "SELECT * FROM etl_ingestion_runs ORDER BY started_at DESC"
            ).fetchdf()
        else:
            rows = self.conn.execute(
                "SELECT * FROM etl_ingestion_runs WHERE dataset_name = ? ORDER BY"
                " started_at DESC",
                [dataset_name],
            ).fetchdf()
        return rows.where(pd.notna(rows), None).to_dict("records")

    def list_validation_results(
        self,
        dataset_name: str | None = None,
        run_id: int | None = None,
    ) -> list[dict]:
        """List persisted validation results."""

        clauses: list[str] = []
        params: list[Any] = []
        if dataset_name is not None:
            clauses.append("dataset_name = ?")
            params.append(dataset_name)
        if run_id is not None:
            clauses.append("run_id = ?")
            params.append(run_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.conn.execute(
            f"SELECT * FROM etl_validation_results {where} ORDER BY created_at DESC",
            params,
        ).fetchdf()
        return rows.where(pd.notna(rows), None).to_dict("records")

    def _source_adapter(self, definition: DatasetDefinition) -> BaseSourceAdapter:
        adapter = self.source_adapters.get(definition.primary_source)
        if adapter is None:
            raise KeyError(f"missing source adapter: {definition.primary_source}")
        if not adapter.supports_dataset(definition.dataset_name):
            raise KeyError(
                f"source adapter {adapter.source_name} does not support"
                f" {definition.dataset_name}"
            )
        return adapter

    def _ensure_dependencies(
        self, definition: DatasetDefinition, request: IngestionRequest, run_id: int
    ) -> list[ValidationResultRecord]:
        if not definition.dependencies:
            return []
        results: list[ValidationResultRecord] = []
        for dependency in definition.dependencies:
            dependency_type = definition.dependency_types.get(
                dependency, DependencyType.WINDOW
            )
            ok = self._dependency_available(
                definition, dependency, request, dependency_type
            )
            auto_run_attempted = False
            if not ok:
                dep_request = self._dependency_request(definition, dependency, request)
                auto_run_attempted = True
                self.run_dataset_sync(dependency, dep_request)
                ok = self._dependency_available(
                    definition, dependency, request, dependency_type
                )
            if ok and auto_run_attempted:
                status = ValidationStatus.PASS_WITH_CAVEAT
            elif ok:
                status = ValidationStatus.PASS
            else:
                status = ValidationStatus.FAIL
            results.append(
                ValidationResultRecord(
                    validation_id=0,
                    run_id=run_id,
                    raw_batch_id=None,
                    dataset_name=definition.dataset_name,
                    check_name=f"dependency_preflight.{dependency_type.value}_missing",
                    check_level="dependency",
                    status=status,
                    subject_key=dependency,
                    metric_value=1 if ok else 0,
                    threshold_value=1,
                    details_json=json.dumps(
                        {
                            "auto_run_attempted": auto_run_attempted,
                            "dependency_type": dependency_type.value,
                        },
                        sort_keys=True,
                    ),
                    created_at=_utc_now(),
                )
            )
        return results

    def _dependency_available(
        self,
        definition: DatasetDefinition,
        dependency: str,
        request: IngestionRequest,
        dependency_type: DependencyType | None = None,
    ) -> bool:
        if dependency_type == DependencyType.FRESHNESS:
            return self._fresh_dependency_available(dependency, request)
        if dependency == "reference.instruments":
            instrument_type = _instrument_type_for_dataset(definition.dataset_name)
            ids = request.context.get("instrument_ids")
            if ids:
                id_list = _unique_strings(ids if isinstance(ids, list) else [ids])
                self.conn.register(
                    "stage_b_required_instruments",
                    pd.DataFrame({"instrument_id": id_list}),
                )
                count = self.conn.execute(
                    """
                    SELECT COUNT(*) FROM canonical_instruments c
                    JOIN stage_b_required_instruments r
                      ON c.instrument_id = r.instrument_id
                    WHERE ? IS NULL OR c.instrument_type = ?
                    """,
                    [instrument_type, instrument_type],
                ).fetchone()[0]
                self.conn.unregister("stage_b_required_instruments")
                return int(count) == len(id_list)
            count = self.conn.execute(
                """
                SELECT COUNT(*) FROM canonical_instruments
                WHERE ? IS NULL OR instrument_type = ?
                """,
                [instrument_type, instrument_type],
            ).fetchone()[0]
            return int(count) > 0
        if dependency == "reference.trading_calendar":
            start, end = normalize_request_window(request)
            exchanges = self._required_calendar_exchanges(definition, request)
            if not exchanges:
                return False
            return self._calendar_window_covered(start, end, exchanges)
        return True

    def _fresh_dependency_available(
        self, dependency: str, request: IngestionRequest
    ) -> bool:
        as_of = request.request_end or request.request_start or date.today()
        max_age_days = int(request.context.get("freshness_max_age_days", 0) or 0)
        minimum_fresh_date = as_of - timedelta(days=max_age_days)
        count = self.conn.execute(
            """
            SELECT COUNT(*) FROM etl_source_watermarks
            WHERE dataset_name = ?
              AND latest_fetched_date IS NOT NULL
              AND latest_fetched_date >= ?
            """,
            [dependency, minimum_fresh_date],
        ).fetchone()[0]
        return int(count) > 0

    def _required_calendar_exchanges(
        self, definition: DatasetDefinition, request: IngestionRequest
    ) -> list[str]:
        instrument_type = _instrument_type_for_dataset(definition.dataset_name)
        if instrument_type is None:
            return ["SH", "SZ"]
        ids = request.context.get("instrument_ids")
        if ids:
            id_list = _unique_strings(ids if isinstance(ids, list) else [ids])
            self.conn.register(
                "stage_b_calendar_required_instruments",
                pd.DataFrame({"instrument_id": id_list}),
            )
            try:
                frame = self.conn.execute(
                    """
                    SELECT DISTINCT c.exchange
                    FROM canonical_instruments c
                    JOIN stage_b_calendar_required_instruments r
                      ON c.instrument_id = r.instrument_id
                    WHERE c.instrument_type = ?
                    ORDER BY c.exchange
                    """,
                    [instrument_type],
                ).fetchdf()
            finally:
                self.conn.unregister("stage_b_calendar_required_instruments")
        else:
            frame = self.conn.execute(
                """
                SELECT DISTINCT exchange
                FROM canonical_instruments
                WHERE instrument_type = ? AND is_active = TRUE
                ORDER BY exchange
                """,
                [instrument_type],
            ).fetchdf()
        return [str(exchange) for exchange in frame["exchange"].dropna().tolist()]

    def _dependency_request(
        self, definition: DatasetDefinition, dependency: str, request: IngestionRequest
    ) -> IngestionRequest:
        if dependency == "reference.instruments":
            instrument_type = _instrument_type_for_dataset(definition.dataset_name)
            context: dict[str, Any] = {}
            if instrument_type:
                context["instrument_type"] = instrument_type
            return IngestionRequest(
                trigger_mode=TriggerMode.MANUAL,
                context=context,
            )
        return IngestionRequest(
            request_start=request.request_start,
            request_end=request.request_end,
            trigger_mode=TriggerMode.MANUAL,
        )

    def _augment_market_request(
        self, definition: DatasetDefinition, request: IngestionRequest
    ) -> IngestionRequest:
        if not definition.dataset_name.startswith("market."):
            return request
        if request.context.get("instrument_ids"):
            return request
        instrument_type = _instrument_type_for_dataset(definition.dataset_name)
        frame = self.conn.execute(
            """
            SELECT instrument_id FROM canonical_instruments
            WHERE instrument_type = ? AND is_active = TRUE
            ORDER BY instrument_id
            """,
            [instrument_type],
        ).fetchdf()
        context = dict(request.context)
        context["instrument_ids"] = frame["instrument_id"].tolist()
        return IngestionRequest(
            request_start=request.request_start,
            request_end=request.request_end,
            full_refresh=request.full_refresh,
            trigger_mode=request.trigger_mode,
            context=context,
        )

    def _write_canonical(
        self, definition: DatasetDefinition, canonical: pd.DataFrame
    ) -> CanonicalWriteResult:
        if definition.dataset_name == "reference.trading_calendar":
            return self._write_trading_calendar(canonical)
        if definition.dataset_name == "reference.instruments":
            return self._write_instruments(canonical)
        if definition.dataset_name == "market.etf_adj_factor":
            return self._write_etf_adj_factor(definition, canonical)
        if definition.dataset_name == "macro.slow_fields":
            return self._write_macro_slow_fields(definition, canonical)
        if definition.dataset_name == "rates.daily_rates":
            return self._write_daily_rates(definition, canonical)
        if definition.dataset_name == "rates.lpr":
            return self._write_lpr(definition, canonical)
        if definition.dataset_name == "rates.gov_curve_points":
            return self._write_gov_curve_points(definition, canonical)
        return self._write_market_daily(definition, canonical)

    def _write_trading_calendar(self, canonical: pd.DataFrame) -> CanonicalWriteResult:
        if canonical.empty:
            return CanonicalWriteResult()
        frame = canonical.copy()
        frame["updated_at"] = _utc_now()
        self.conn.register("stage_b_calendar", frame)
        existing = int(
            self.conn.execute(
                """
                SELECT COUNT(*) FROM canonical_trading_calendar
                WHERE (exchange, trade_date) IN (
                    SELECT exchange, trade_date FROM stage_b_calendar
                )
                """
            ).fetchone()[0]
        )
        self.conn.execute(
            """
            DELETE FROM canonical_trading_calendar
            WHERE (exchange, trade_date) IN (
                SELECT exchange, trade_date FROM stage_b_calendar
            )
            """
        )
        self.conn.execute(
            """
            INSERT INTO canonical_trading_calendar
            SELECT exchange, trade_date, is_open, pretrade_date, updated_at
            FROM stage_b_calendar
            """
        )
        self.conn.unregister("stage_b_calendar")
        return CanonicalWriteResult(
            records_written=len(frame),
            records_inserted=max(len(frame) - existing, 0),
            records_updated=existing,
        )

    def _write_instruments(self, canonical: pd.DataFrame) -> CanonicalWriteResult:
        if canonical.empty:
            return CanonicalWriteResult()
        frame = canonical.copy()
        frame["updated_at"] = _utc_now()
        self.conn.register("stage_b_instruments", frame)
        existing = int(
            self.conn.execute(
                """
                SELECT COUNT(*) FROM canonical_instruments
                WHERE instrument_id IN (
                    SELECT instrument_id FROM stage_b_instruments
                )
                """
            ).fetchone()[0]
        )
        self.conn.execute(
            """
            DELETE FROM canonical_instruments
            WHERE instrument_id IN (
                SELECT instrument_id FROM stage_b_instruments
            )
            """
        )
        self.conn.execute(
            """
            INSERT INTO canonical_instruments
            SELECT instrument_id, source_instrument_id, instrument_name,
                   instrument_type, exchange, list_date, delist_date,
                   is_active, source_name, updated_at
            FROM stage_b_instruments
            """
        )
        self.conn.unregister("stage_b_instruments")
        return CanonicalWriteResult(
            records_written=len(frame),
            records_inserted=max(len(frame) - existing, 0),
            records_updated=existing,
        )

    def _write_market_daily(
        self, definition: DatasetDefinition, canonical: pd.DataFrame
    ) -> CanonicalWriteResult:
        return self._write_year_month_partition_upsert(
            dataset_name=definition.dataset_name,
            zone=StorageZone.NORMALIZED,
            canonical=canonical,
            key_columns=("instrument_id", "trade_date"),
            sort_columns=("instrument_id", "trade_date", "ingested_at"),
        )

    def _write_etf_adj_factor(
        self, definition: DatasetDefinition, canonical: pd.DataFrame
    ) -> CanonicalWriteResult:
        return self._write_year_month_partition_upsert(
            dataset_name=definition.dataset_name,
            zone=StorageZone.NORMALIZED,
            canonical=canonical,
            key_columns=("instrument_id", "trade_date"),
            sort_columns=("instrument_id", "trade_date", "ingested_at"),
        )

    def _write_daily_rates(
        self, definition: DatasetDefinition, canonical: pd.DataFrame
    ) -> CanonicalWriteResult:
        return self._write_year_month_partition_upsert(
            dataset_name=definition.dataset_name,
            zone=StorageZone.NORMALIZED,
            canonical=canonical,
            key_columns=("field_name", "trade_date"),
            sort_columns=("field_name", "trade_date", "ingested_at"),
        )

    def _write_macro_slow_fields(
        self, definition: DatasetDefinition, canonical: pd.DataFrame
    ) -> CanonicalWriteResult:
        return self._write_year_month_partition_upsert(
            dataset_name=definition.dataset_name,
            zone=StorageZone.NORMALIZED,
            canonical=canonical,
            key_columns=("field_name", "period_label"),
            sort_columns=("field_name", "period_label", "ingested_at"),
            partition_date_column="effective_date",
        )

    def _write_lpr(
        self, definition: DatasetDefinition, canonical: pd.DataFrame
    ) -> CanonicalWriteResult:
        return self._write_year_month_partition_upsert(
            dataset_name=definition.dataset_name,
            zone=StorageZone.NORMALIZED,
            canonical=canonical,
            key_columns=("field_name", "quote_date"),
            sort_columns=("field_name", "quote_date", "ingested_at"),
            partition_date_column="effective_date",
        )

    def _write_gov_curve_points(
        self, definition: DatasetDefinition, canonical: pd.DataFrame
    ) -> CanonicalWriteResult:
        return self._write_year_month_partition_upsert(
            dataset_name=definition.dataset_name,
            zone=StorageZone.NORMALIZED,
            canonical=canonical,
            key_columns=("curve_code", "curve_date", "tenor_years"),
            sort_columns=("curve_code", "curve_date", "tenor_years", "ingested_at"),
            partition_date_column="curve_date",
        )

    def _write_year_month_partition_upsert(
        self,
        *,
        dataset_name: str,
        zone: StorageZone,
        canonical: pd.DataFrame,
        key_columns: tuple[str, ...],
        sort_columns: tuple[str, ...],
        partition_date_column: str = "trade_date",
        existing_column_defaults: dict[str, object] | None = None,
    ) -> CanonicalWriteResult:
        if canonical.empty:
            return CanonicalWriteResult()
        frame = canonical.copy()
        frame[partition_date_column] = pd.to_datetime(
            frame[partition_date_column], errors="coerce"
        )
        frame["year"] = frame[partition_date_column].dt.year
        frame["month"] = frame[partition_date_column].dt.month
        storage_paths: list[str] = []
        partitions_written = 0
        records_inserted = 0
        records_updated = 0
        for (year, month), partition in frame.groupby(["year", "month"], dropna=True):
            parts = [("year", int(year)), ("month", f"{int(month):02d}")]
            final_path = build_dataset_file_path(
                dataset_name, zone, parts, lakehouse_root=self.lakehouse_root
            )
            partition_frame = partition.drop(columns=["year", "month"]).copy()
            if final_path.exists():
                existing = pd.read_parquet(final_path)
                for column, default in (existing_column_defaults or {}).items():
                    if column not in existing.columns:
                        existing[column] = default
                merged = pd.concat([existing, partition_frame], ignore_index=True)
                existing_keys = _business_keys(existing, key_columns)
            else:
                merged = partition_frame
                existing_keys = set()
            partition_keys = _business_keys(partition_frame, key_columns)
            if "trade_date" in merged.columns:
                merged["trade_date"] = pd.to_datetime(
                    merged["trade_date"], errors="coerce"
                )
            if "rebalance_date" in merged.columns:
                merged["rebalance_date"] = pd.to_datetime(
                    merged["rebalance_date"], errors="coerce"
                )
            if "quote_date" in merged.columns:
                merged["quote_date"] = pd.to_datetime(
                    merged["quote_date"], errors="coerce"
                )
            if "release_date" in merged.columns:
                merged["release_date"] = pd.to_datetime(
                    merged["release_date"], errors="coerce"
                )
            if "effective_date" in merged.columns:
                merged["effective_date"] = pd.to_datetime(
                    merged["effective_date"], errors="coerce"
                )
            if "observation_date" in merged.columns:
                merged["observation_date"] = pd.to_datetime(
                    merged["observation_date"], errors="coerce"
                )
            for column in sort_columns:
                if isinstance(merged[column].dtype, pd.CategoricalDtype):
                    merged[column] = merged[column].astype(str)
            sort_by = list(sort_columns)
            if dataset_name == _ETF_AW_RISK_BUDGET_DATASET:
                merged["_sleeve_role_order"] = merged["sleeve_role"].map(
                    ETF_AW_ROLE_RANK
                )
                sort_by = [
                    "_sleeve_role_order" if column == "sleeve_role" else column
                    for column in sort_by
                ]
            merged = (
                merged.sort_values(sort_by)
                .drop_duplicates(list(key_columns), keep="last")
                .reset_index(drop=True)
            )
            if "_sleeve_role_order" in merged.columns:
                merged = merged.drop(columns=["_sleeve_role_order"])
            if "trade_date" in merged.columns:
                merged["trade_date"] = pd.to_datetime(
                    merged["trade_date"], errors="coerce"
                ).dt.date
            if "rebalance_date" in merged.columns:
                merged["rebalance_date"] = pd.to_datetime(
                    merged["rebalance_date"], errors="coerce"
                ).dt.date
            if "quote_date" in merged.columns:
                merged["quote_date"] = pd.to_datetime(
                    merged["quote_date"], errors="coerce"
                ).dt.date
            if "release_date" in merged.columns:
                merged["release_date"] = pd.to_datetime(
                    merged["release_date"], errors="coerce"
                ).dt.date
            if "effective_date" in merged.columns:
                merged["effective_date"] = pd.to_datetime(
                    merged["effective_date"], errors="coerce"
                ).dt.date
            if "observation_date" in merged.columns:
                merged["observation_date"] = pd.to_datetime(
                    merged["observation_date"], errors="coerce"
                ).dt.date
            write_result = write_dataset_parquet(
                merged,
                dataset_name,
                zone,
                parts,
                lakehouse_root=self.lakehouse_root,
            )
            storage_paths.append(write_result.relative_path)
            partitions_written += 1
            records_inserted += len(partition_keys - existing_keys)
            records_updated += len(partition_keys & existing_keys)
        return CanonicalWriteResult(
            records_written=len(canonical),
            records_inserted=records_inserted,
            records_updated=records_updated,
            partitions_written=partitions_written,
            storage_paths=storage_paths,
        )

    def _source_payload_validation(
        self,
        definition: DatasetDefinition,
        fetch_result: SourceFetchResult,
        run_id: int,
        raw_batch_id: int,
    ) -> list[ValidationResultRecord]:
        if fetch_result.row_count > 0:
            return []
        status = (
            ValidationStatus.FAIL
            if definition.dataset_name.startswith("reference.")
            else ValidationStatus.PASS_WITH_CAVEAT
        )
        return [
            ValidationResultRecord(
                validation_id=0,
                run_id=run_id,
                raw_batch_id=raw_batch_id,
                dataset_name=definition.dataset_name,
                check_name="source_contract.empty_payload",
                check_level="contract",
                status=status,
                subject_key=definition.dataset_name,
                metric_value=0,
                threshold_value=1,
                details_json=json.dumps(
                    {"message": "source returned no rows"}, ensure_ascii=False
                ),
                created_at=_utc_now(),
            )
        ]

    def _next_id(self, table: str, column: str) -> int:
        sequence_name = _ID_SEQUENCES.get((table, column))
        if sequence_name is None:
            raise KeyError(f"no id sequence registered for {table}.{column}")
        while True:
            value = int(
                self.conn.execute("SELECT nextval(?)", [sequence_name]).fetchone()[0]
            )
            max_existing = int(
                self.conn.execute(
                    f"SELECT COALESCE(MAX({column}), 0) FROM {table}"
                ).fetchone()[0]
            )
            if value > max_existing:
                return value

    def _insert_run(
        self,
        run_id: int,
        definition: DatasetDefinition,
        source_name: str,
        request: IngestionRequest,
        started_at: datetime,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO etl_ingestion_runs (
                run_id, job_name, dataset_name, source_name, trigger_mode,
                status, started_at, request_start, request_end
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                f"{definition.dataset_name}_sync",
                definition.dataset_name,
                source_name,
                request.trigger_mode.value,
                RunStatus.RUNNING.value,
                started_at,
                request.request_start,
                request.request_end,
            ],
        )

    def _insert_raw_batch(
        self,
        raw_batch_id: int,
        run_id: int,
        fetch_result: SourceFetchResult,
        write_result: Any,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO etl_raw_batches (
                raw_batch_id, run_id, dataset_name, source_name, source_endpoint,
                storage_path, file_format, compression, partition_year,
                partition_month, window_start, window_end, row_count,
                content_hash, fetched_at, schema_version, is_fallback_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                raw_batch_id,
                run_id,
                fetch_result.dataset_name,
                fetch_result.source_name,
                fetch_result.source_endpoint,
                write_result.relative_path,
                "parquet",
                None,
                _optional_int(fetch_result.partition_hints.get("year")),
                _optional_int(fetch_result.partition_hints.get("month")),
                fetch_result.window_start,
                fetch_result.window_end,
                write_result.row_count,
                write_result.content_hash,
                fetch_result.fetched_at,
                fetch_result.schema_version,
                fetch_result.is_fallback_source,
            ],
        )

    def _persist_validation_results(
        self, results: list[ValidationResultRecord]
    ) -> None:
        for result in results:
            validation_id = self._next_id("etl_validation_results", "validation_id")
            self.conn.execute(
                """
                INSERT INTO etl_validation_results (
                    validation_id, run_id, raw_batch_id, dataset_name, check_name,
                    check_level, status, subject_key, metric_value,
                    threshold_value, details_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    validation_id,
                    result.run_id,
                    result.raw_batch_id,
                    result.dataset_name,
                    result.check_name,
                    result.check_level,
                    result.status.value,
                    result.subject_key,
                    result.metric_value,
                    result.threshold_value,
                    result.details_json,
                    result.created_at,
                ],
            )

    def _finish_run(
        self,
        run_id: int,
        status: RunStatus,
        records_discovered: int = 0,
        records_inserted: int = 0,
        records_updated: int = 0,
        records_failed: int = 0,
        partitions_written: int = 0,
        error_message: str | None = None,
    ) -> None:
        self.conn.execute(
            """
            UPDATE etl_ingestion_runs
            SET status = ?, finished_at = ?, records_discovered = ?,
                records_inserted = ?, records_updated = ?, records_failed = ?,
                partitions_written = ?, error_message = ?
            WHERE run_id = ?
            """,
            [
                status.value,
                _utc_now(),
                records_discovered,
                records_inserted,
                records_updated,
                records_failed,
                partitions_written,
                error_message,
                run_id,
            ],
        )

    def _advance_watermark(
        self,
        definition: DatasetDefinition,
        source_name: str,
        run_id: int,
        canonical: pd.DataFrame,
    ) -> None:
        watermark_key = definition.watermark_key
        if (
            watermark_key is not None
            and watermark_key in canonical.columns
            and not canonical.empty
        ):
            latest = pd.to_datetime(canonical[watermark_key], errors="coerce").max()
            latest_date = latest.date() if pd.notna(latest) else None
        elif "trade_date" in canonical.columns and not canonical.empty:
            latest = pd.to_datetime(canonical["trade_date"], errors="coerce").max()
            latest_date = latest.date() if pd.notna(latest) else None
        elif definition.dataset_name == "reference.instruments":
            latest_date = date.today()
        else:
            latest_date = None
        self.conn.execute(
            """
            INSERT INTO etl_source_watermarks (
                dataset_name, source_name, latest_available_date,
                latest_fetched_date, latest_successful_run_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (dataset_name, source_name) DO UPDATE SET
                latest_available_date = GREATEST(
                    etl_source_watermarks.latest_available_date,
                    excluded.latest_available_date
                ),
                latest_fetched_date = GREATEST(
                    etl_source_watermarks.latest_fetched_date,
                    excluded.latest_fetched_date
                ),
                latest_successful_run_id = CASE
                    WHEN etl_source_watermarks.latest_fetched_date IS NULL
                      OR excluded.latest_fetched_date >= etl_source_watermarks.latest_fetched_date
                    THEN excluded.latest_successful_run_id
                    ELSE etl_source_watermarks.latest_successful_run_id
                END,
                updated_at = excluded.updated_at
            """,
            [
                definition.dataset_name,
                source_name,
                latest_date,
                latest_date,
                run_id,
                _utc_now(),
            ],
        )

    def _bootstrap_trading_calendar_full_history(self, start: date, end: date) -> dict:
        start, end = _ordered_dates(start, end)
        windows = _month_windows(start, end)
        processed: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        runs: list[dict[str, Any]] = []

        for window_start, window_end in windows:
            window = {
                "start": window_start.isoformat(),
                "end": window_end.isoformat(),
            }
            if self._calendar_window_covered(
                window_start, window_end, _TRADING_CALENDAR_BOOTSTRAP_EXCHANGES
            ):
                skipped.append(window)
                continue
            result = self.run_dataset_sync(
                "reference.trading_calendar",
                IngestionRequest(
                    request_start=window_start,
                    request_end=window_end,
                    trigger_mode=TriggerMode.BACKFILL,
                    context={"exchanges": _TRADING_CALENDAR_BOOTSTRAP_EXCHANGES},
                ),
            )
            run = {
                **window,
                "run_id": result.run_id,
                "status": result.status.value,
                "records_written": result.records_written,
            }
            runs.append(run)
            processed.append(window)
            if result.status != RunStatus.SUCCESS:
                return {
                    "profile_name": _TRADING_CALENDAR_FULL_HISTORY_PROFILE,
                    "dataset_name": "reference.trading_calendar",
                    "status": RunStatus.FAILED.value,
                    "requested_start": start.isoformat(),
                    "requested_end": end.isoformat(),
                    "windows_total": len(windows),
                    "windows_processed": len(processed),
                    "windows_skipped": len(skipped),
                    "runs": runs,
                    "skipped_windows": skipped,
                    "error_message": result.error_message,
                }

        final_coverage_ok = self._calendar_window_covered(
            start, end, _TRADING_CALENDAR_BOOTSTRAP_EXCHANGES
        )
        duplicate_keys = self._trading_calendar_duplicate_key_count(
            start,
            end,
            _TRADING_CALENDAR_BOOTSTRAP_EXCHANGES,
        )
        final_validation_results = self._validate_trading_calendar_window(
            start,
            end,
            _TRADING_CALENDAR_BOOTSTRAP_EXCHANGES,
        )
        final_validation_counts = validation_counts(final_validation_results)
        final_validation_passed = not has_blocking_failures(final_validation_results)
        status = (
            RunStatus.SUCCESS.value
            if final_coverage_ok and duplicate_keys == 0 and final_validation_passed
            else RunStatus.FAILED.value
        )
        return {
            "profile_name": _TRADING_CALENDAR_FULL_HISTORY_PROFILE,
            "dataset_name": "reference.trading_calendar",
            "status": status,
            "requested_start": start.isoformat(),
            "requested_end": end.isoformat(),
            "windows_total": len(windows),
            "windows_processed": len(processed),
            "windows_skipped": len(skipped),
            "runs": runs,
            "skipped_windows": skipped,
            "final_coverage_ok": final_coverage_ok,
            "duplicate_business_keys": duplicate_keys,
            "final_validation_passed": final_validation_passed,
            "final_validation_counts": final_validation_counts,
        }

    def _bootstrap_etf_aw_sleeves(self) -> dict:
        rows = [dict(row) for row in _ETF_AW_SLEEVES]
        self._write_etf_aw_sleeves(rows)
        self._ensure_etf_aw_sleeve_instruments(rows)
        validation = self._validate_etf_aw_sleeves()
        status = (
            RunStatus.SUCCESS.value
            if all(validation.values())
            else RunStatus.FAILED.value
        )
        return {
            "profile_name": _ETF_AW_SLEEVES_PROFILE,
            "dataset_name": "reference.etf_aw_sleeves",
            "status": status,
            "records_written": len(rows),
            "sleeve_codes": _ETF_AW_SLEEVE_CODES,
            "validation": validation,
        }

    def _write_etf_aw_sleeves(self, rows: list[dict[str, Any]]) -> None:
        frame = pd.DataFrame(rows)
        frame["sleeve_type"] = frame["sleeve_role"]
        frame["is_active"] = True
        now = _utc_now()
        frame["created_at"] = now
        frame["updated_at"] = now
        self.conn.register("stage_c_etf_aw_sleeves", frame)
        try:
            self.conn.execute(
                """
                DELETE FROM canonical_sleeves
                WHERE sleeve_code IN (
                    SELECT sleeve_code FROM stage_c_etf_aw_sleeves
                )
                """
            )
            self.conn.execute(
                """
                INSERT INTO canonical_sleeves (
                    sleeve_code, sleeve_name, sleeve_type, is_active, updated_at,
                    sleeve_role, listing_exchange, benchmark_name, list_date,
                    exposure_note, created_at
                )
                SELECT sleeve_code, sleeve_name, sleeve_type, is_active, updated_at,
                       sleeve_role, listing_exchange, benchmark_name, list_date,
                       exposure_note, created_at
                FROM stage_c_etf_aw_sleeves
                """
            )
        finally:
            self.conn.unregister("stage_c_etf_aw_sleeves")

    def _ensure_etf_aw_sleeve_instruments(self, rows: list[dict[str, Any]]) -> None:
        frame = pd.DataFrame(
            [
                {
                    "instrument_id": row["sleeve_code"],
                    "source_instrument_id": row["sleeve_code"],
                    "instrument_name": row["sleeve_name"],
                    "instrument_type": "etf",
                    "exchange": row["listing_exchange"],
                    "list_date": row["list_date"],
                    "delist_date": None,
                    "is_active": True,
                    "source_name": "static_etf_aw_v1",
                }
                for row in rows
            ]
        )
        self.conn.register("stage_c_etf_aw_instruments", frame)
        try:
            self.conn.execute(
                """
                INSERT INTO canonical_instruments (
                    instrument_id, source_instrument_id, instrument_name,
                    instrument_type, exchange, list_date, delist_date, is_active,
                    source_name, updated_at
                )
                SELECT s.instrument_id, s.source_instrument_id, s.instrument_name,
                       s.instrument_type, s.exchange, s.list_date, s.delist_date,
                       s.is_active, s.source_name, CURRENT_TIMESTAMP
                FROM stage_c_etf_aw_instruments s
                LEFT JOIN canonical_instruments c
                  ON s.instrument_id = c.instrument_id
                WHERE c.instrument_id IS NULL
                """
            )
        finally:
            self.conn.unregister("stage_c_etf_aw_instruments")

    def _validate_etf_aw_sleeves(self) -> dict[str, bool]:
        self.conn.register("stage_c_etf_aw_codes", _etf_aw_sleeve_codes_frame())
        try:
            rows = self.conn.execute(
                """
                SELECT s.sleeve_code, s.sleeve_role, s.listing_exchange,
                       s.exposure_note, s.is_active
                FROM canonical_sleeves s
                JOIN stage_c_etf_aw_codes c
                  ON s.sleeve_code = c.sleeve_code
                ORDER BY s.sleeve_code
                """
            ).fetchall()
            instrument_count = int(
                self.conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM canonical_instruments i
                    JOIN stage_c_etf_aw_codes c
                      ON i.instrument_id = c.sleeve_code
                    WHERE i.instrument_type = 'etf'
                    """
                ).fetchone()[0]
            )
        finally:
            self.conn.unregister("stage_c_etf_aw_codes")
        active_codes = [row[0] for row in rows if row[4] is True]
        roles = {row[1] for row in rows}
        exchanges = {row[0]: row[2] for row in rows}
        notes_present = all(bool(str(row[3] or "").strip()) for row in rows)
        return {
            "exact_frozen_codes": active_codes == sorted(_ETF_AW_SLEEVE_CODES),
            "roles_supported": roles == _ETF_AW_SLEEVE_ROLES,
            "listing_exchange_matches_suffix": all(
                code.rsplit(".", 1)[-1] == exchange
                for code, exchange in exchanges.items()
            ),
            "exposure_notes_present": notes_present,
            "canonical_instruments_available": instrument_count == len(rows),
        }

    def _build_etf_aw_sleeve_daily(self, start: date, end: date) -> dict:
        start, end = _ordered_dates(start, end)
        self._bootstrap_etf_aw_sleeves()
        read_start = start - timedelta(days=_ETF_AW_SLEEVE_DAILY_RETURN_LOOKBACK_DAYS)
        daily = self._read_partitioned_dataset(
            "market.etf_daily",
            read_start,
            end,
            StorageZone.NORMALIZED,
        )
        adj = self._read_partitioned_dataset(
            "market.etf_adj_factor",
            read_start,
            end,
            StorageZone.NORMALIZED,
        )
        missing_inputs = []
        if daily.empty:
            missing_inputs.append("market.etf_daily")
        if adj.empty:
            missing_inputs.append("market.etf_adj_factor")
        if missing_inputs:
            return {
                "profile_name": _ETF_AW_SLEEVE_DAILY_PROFILE,
                "dataset_name": "derived.etf_aw_sleeve_daily",
                "status": RunStatus.FAILED.value,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "records_written": 0,
                "missing_inputs": missing_inputs,
                "error_message": "required normalized market inputs are missing",
            }

        panel = self._make_etf_aw_sleeve_daily_frame(daily, adj, start, end)
        validation = _validate_sleeve_daily_frame(panel)
        if not all(validation.values()):
            return {
                "profile_name": _ETF_AW_SLEEVE_DAILY_PROFILE,
                "dataset_name": "derived.etf_aw_sleeve_daily",
                "status": RunStatus.FAILED.value,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "records_written": 0,
                "validation": validation,
                "error_message": "derived sleeve daily validation failed",
            }

        write_result = self._write_etf_aw_sleeve_daily(panel)
        return {
            "profile_name": _ETF_AW_SLEEVE_DAILY_PROFILE,
            "dataset_name": "derived.etf_aw_sleeve_daily",
            "status": RunStatus.SUCCESS.value,
            "requested_start": start.isoformat(),
            "requested_end": end.isoformat(),
            "records_written": write_result.records_written,
            "records_inserted": write_result.records_inserted,
            "records_updated": write_result.records_updated,
            "partitions_written": write_result.partitions_written,
            "storage_paths": write_result.storage_paths,
            "return_semantics": "adj_pct_chg is adjacent available observation return",
            "validation": validation,
        }

    def _read_partitioned_dataset(
        self,
        dataset_name: str,
        start: date,
        end: date,
        zone: StorageZone,
    ) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for window_start, _ in _month_windows(start, end):
            path = build_dataset_file_path(
                dataset_name,
                zone,
                [
                    ("year", window_start.year),
                    ("month", f"{window_start.month:02d}"),
                ],
                lakehouse_root=self.lakehouse_root,
            )
            if path.exists():
                frames.append(pd.read_parquet(path))
        if not frames:
            return pd.DataFrame()
        frame = pd.concat(frames, ignore_index=True)
        if "trade_date" in frame.columns:
            frame["trade_date"] = pd.to_datetime(
                frame["trade_date"], errors="coerce"
            ).dt.date
            frame = frame[
                frame["trade_date"].between(start, end, inclusive="both")
            ].copy()
        if "rebalance_date" in frame.columns:
            frame["rebalance_date"] = pd.to_datetime(
                frame["rebalance_date"], errors="coerce"
            ).dt.date
            frame = frame[
                frame["rebalance_date"].between(start, end, inclusive="both")
            ].copy()
        return frame

    def _make_etf_aw_sleeve_daily_frame(
        self,
        daily: pd.DataFrame,
        adj: pd.DataFrame,
        start: date,
        end: date,
    ) -> pd.DataFrame:
        self.conn.register("stage_c_etf_aw_codes", _etf_aw_sleeve_codes_frame())
        try:
            sleeves = self.conn.execute(
                """
                SELECT s.sleeve_code, s.sleeve_role
                FROM canonical_sleeves s
                JOIN stage_c_etf_aw_codes c
                  ON s.sleeve_code = c.sleeve_code
                WHERE s.is_active = TRUE
                """
            ).fetchdf()
        finally:
            self.conn.unregister("stage_c_etf_aw_codes")
        daily = daily.copy()
        adj = adj.copy()
        daily["trade_date"] = pd.to_datetime(
            daily["trade_date"], errors="coerce"
        ).dt.date
        adj["trade_date"] = pd.to_datetime(adj["trade_date"], errors="coerce").dt.date
        merged = daily.merge(
            adj.loc[:, ["instrument_id", "trade_date", "adj_factor"]],
            on=["instrument_id", "trade_date"],
            how="inner",
        )
        merged = merged.merge(
            sleeves.rename(columns={"sleeve_code": "instrument_id"}),
            on="instrument_id",
            how="inner",
        )
        merged = merged.sort_values(["instrument_id", "trade_date"]).reset_index(
            drop=True
        )
        merged["adj_close"] = merged["close"] * merged["adj_factor"]
        # Return between adjacent available observations after the input merge.
        merged["adj_pct_chg"] = (
            merged.groupby("instrument_id")["adj_close"].pct_change() * 100
        )
        merged = merged[
            merged["trade_date"].between(start, end, inclusive="both")
        ].copy()
        merged["sleeve_code"] = merged["instrument_id"]
        merged["source_name"] = "derived.market_etf_daily_plus_adj_factor"
        merged["ingested_at"] = _utc_now()
        merged["quality_status"] = "pass"
        columns = [
            "sleeve_code",
            "sleeve_role",
            "instrument_id",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "adj_factor",
            "adj_close",
            "pct_chg",
            "adj_pct_chg",
            "volume",
            "amount",
            "source_name",
            "ingested_at",
            "quality_status",
        ]
        return merged.loc[:, columns].copy()

    def _write_etf_aw_sleeve_daily(
        self, canonical: pd.DataFrame
    ) -> CanonicalWriteResult:
        return self._write_year_month_partition_upsert(
            dataset_name="derived.etf_aw_sleeve_daily",
            zone=StorageZone.DERIVED,
            canonical=canonical,
            key_columns=("sleeve_code", "trade_date"),
            sort_columns=("sleeve_code", "trade_date", "ingested_at"),
        )

    def _build_etf_aw_rebalance_snapshot(self, start: date, end: date) -> dict:
        start, end = _ordered_dates(start, end)
        rebalance = self._read_rebalance_calendar(start, end)
        if rebalance.empty:
            return {
                "profile_name": _ETF_AW_REBALANCE_SNAPSHOT_PROFILE,
                "dataset_name": _ETF_AW_REBALANCE_SNAPSHOT_DATASET,
                "status": RunStatus.FAILED.value,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "records_written": 0,
                "error_message": "canonical rebalance calendar is missing",
            }
        daily_start = start - timedelta(days=_ETF_AW_SNAPSHOT_LOOKBACK_DAYS)
        panel = self._read_partitioned_dataset(
            "derived.etf_aw_sleeve_daily",
            daily_start,
            end,
            StorageZone.DERIVED,
        )
        if panel.empty:
            return {
                "profile_name": _ETF_AW_REBALANCE_SNAPSHOT_PROFILE,
                "dataset_name": _ETF_AW_REBALANCE_SNAPSHOT_DATASET,
                "status": RunStatus.FAILED.value,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "records_written": 0,
                "error_message": "derived sleeve daily panel is missing",
            }
        snapshot = self._make_etf_aw_rebalance_snapshot_frame(
            rebalance=rebalance,
            panel=panel,
            watermarks=self._latest_market_watermarks(),
        )
        validation = _validate_rebalance_snapshot_frame(
            snapshot, set(rebalance["rebalance_date"].tolist())
        )
        if not all(validation.values()):
            return {
                "profile_name": _ETF_AW_REBALANCE_SNAPSHOT_PROFILE,
                "dataset_name": _ETF_AW_REBALANCE_SNAPSHOT_DATASET,
                "status": RunStatus.FAILED.value,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "records_written": 0,
                "validation": validation,
                "error_message": "rebalance snapshot validation failed",
            }
        write_result = self._write_etf_aw_rebalance_snapshot(snapshot)
        return {
            "profile_name": _ETF_AW_REBALANCE_SNAPSHOT_PROFILE,
            "dataset_name": _ETF_AW_REBALANCE_SNAPSHOT_DATASET,
            "status": RunStatus.SUCCESS.value,
            "requested_start": start.isoformat(),
            "requested_end": end.isoformat(),
            "records_written": write_result.records_written,
            "records_inserted": write_result.records_inserted,
            "records_updated": write_result.records_updated,
            "partitions_written": write_result.partitions_written,
            "storage_paths": write_result.storage_paths,
            "validation": validation,
            "data_status_counts": snapshot["data_status"].value_counts().to_dict(),
        }

    def _read_rebalance_calendar(self, start: date, end: date) -> pd.DataFrame:
        frame = self.conn.execute(
            """
            SELECT calendar_name, calendar_month, rebalance_date, effective_date
            FROM canonical_rebalance_calendar
            WHERE calendar_name = ?
              AND rebalance_date BETWEEN ? AND ?
            ORDER BY rebalance_date
            """,
            [_REBALANCE_CALENDAR_NAME, start, end],
        ).fetchdf()
        if frame.empty:
            return frame
        frame["rebalance_date"] = pd.to_datetime(
            frame["rebalance_date"], errors="coerce"
        ).dt.date
        frame["effective_date"] = pd.to_datetime(
            frame["effective_date"], errors="coerce"
        ).dt.date
        return frame

    def _latest_market_watermarks(self) -> dict[str, date | None]:
        rows = self.conn.execute(
            """
            SELECT dataset_name, latest_fetched_date
            FROM etl_source_watermarks
            WHERE dataset_name IN (
                'market.etf_daily',
                'market.etf_adj_factor',
                'reference.trading_calendar'
            )
            """
        ).fetchall()
        return {str(row[0]): row[1] for row in rows}

    def _make_etf_aw_rebalance_snapshot_frame(
        self,
        *,
        rebalance: pd.DataFrame,
        panel: pd.DataFrame,
        watermarks: dict[str, date | None],
    ) -> pd.DataFrame:
        sleeves = self._active_etf_aw_sleeves_frame()
        panel = panel.copy()
        panel["trade_date"] = pd.to_datetime(
            panel["trade_date"], errors="coerce"
        ).dt.date
        panel = panel.sort_values(["sleeve_code", "trade_date"]).reset_index(drop=True)
        panel_max_trade_date = (
            max(panel["trade_date"].dropna().tolist()) if not panel.empty else None
        )
        rows: list[dict[str, Any]] = []
        ingested_at = _utc_now()
        for _, rebalance_row in rebalance.iterrows():
            rebalance_date = rebalance_row["rebalance_date"]
            for _, sleeve in sleeves.iterrows():
                rows.append(
                    self._make_snapshot_row(
                        rebalance_row=rebalance_row,
                        sleeve_code=str(sleeve["sleeve_code"]),
                        sleeve_role=str(sleeve["sleeve_role"]),
                        sleeve_panel=panel[
                            panel["sleeve_code"].astype(str)
                            == str(sleeve["sleeve_code"])
                        ],
                        rebalance_date=rebalance_date,
                        panel_max_trade_date=panel_max_trade_date,
                        watermarks=watermarks,
                        ingested_at=ingested_at,
                    )
                )
        return pd.DataFrame(rows)

    def _active_etf_aw_sleeves_frame(self) -> pd.DataFrame:
        self.conn.register("stage_d_etf_aw_codes", _etf_aw_sleeve_codes_frame())
        try:
            frame = self.conn.execute(
                """
                SELECT s.sleeve_code, s.sleeve_role
                FROM canonical_sleeves s
                JOIN stage_d_etf_aw_codes c
                  ON s.sleeve_code = c.sleeve_code
                WHERE s.is_active = TRUE
                """
            ).fetchdf()
        finally:
            self.conn.unregister("stage_d_etf_aw_codes")
        if frame.empty:
            frame = etf_aw_sleeves_frame().loc[:, ["sleeve_code", "sleeve_role"]]
        return frame.sort_values("sleeve_role", key=etf_aw_role_sort_key).reset_index(
            drop=True
        )

    def _make_snapshot_row(
        self,
        *,
        rebalance_row: pd.Series,
        sleeve_code: str,
        sleeve_role: str,
        sleeve_panel: pd.DataFrame,
        rebalance_date: date,
        panel_max_trade_date: date | None,
        watermarks: dict[str, date | None],
        ingested_at: datetime,
    ) -> dict[str, Any]:
        available = sleeve_panel[sleeve_panel["trade_date"] <= rebalance_date].copy()
        source_max_trade_date = (
            max(sleeve_panel["trade_date"].tolist()) if not sleeve_panel.empty else None
        )
        target = available[available["trade_date"] == rebalance_date]
        row = {
            "calendar_name": str(rebalance_row["calendar_name"]),
            "calendar_month": str(rebalance_row["calendar_month"]),
            "rebalance_date": rebalance_date,
            "effective_date": rebalance_row["effective_date"],
            "sleeve_code": sleeve_code,
            "sleeve_role": sleeve_role,
            "close": None,
            "adj_factor": None,
            "adj_close": None,
            "return_1m": None,
            "return_3m": None,
            "return_6m": None,
            "volatility_3m": None,
            "max_drawdown_6m": None,
            "data_status": "missing",
            "quality_notes": "",
            "source_max_trade_date": source_max_trade_date,
            "ingested_at": ingested_at,
        }
        notes: dict[str, Any] = {
            "window_observations": {},
            "minimum_observations": {
                key: minimum for key, (_, minimum) in _ETF_AW_SNAPSHOT_WINDOWS.items()
            },
            "calculation": "trailing available observations ending at rebalance_date",
        }
        core_missing = target.empty
        if not core_missing:
            target_row = target.sort_values("trade_date").iloc[-1]
            row["close"] = _nullable_float(target_row.get("close"))
            row["adj_factor"] = _nullable_float(target_row.get("adj_factor"))
            row["adj_close"] = _nullable_float(target_row.get("adj_close"))
            core_missing = (
                row["close"] is None
                or row["adj_factor"] is None
                or row["adj_factor"] <= 0
                or row["adj_close"] is None
                or row["adj_close"] <= 0
            )
        stale_sources = [
            dataset
            for dataset in (
                "market.etf_daily",
                "market.etf_adj_factor",
                "reference.trading_calendar",
            )
            if watermarks.get(dataset) is None or watermarks[dataset] < rebalance_date
        ]
        source_lagged = (
            panel_max_trade_date is None or panel_max_trade_date < rebalance_date
        )
        if source_lagged:
            notes["source_lag"] = {
                "panel_max_trade_date": (
                    panel_max_trade_date.isoformat()
                    if panel_max_trade_date is not None
                    else None
                ),
                "rebalance_date": rebalance_date.isoformat(),
            }
        if stale_sources:
            notes["stale_sources"] = stale_sources

        if stale_sources or source_lagged:
            row["data_status"] = "stale"
            if not core_missing:
                features, partial_reasons = _snapshot_features(available)
                row.update(features)
                notes["window_observations"] = {
                    key: value["observations"] for key, value in partial_reasons.items()
                }
                if any(value["partial"] for value in partial_reasons.values()):
                    notes["partial_features"] = [
                        key
                        for key, value in partial_reasons.items()
                        if value["partial"]
                    ]
        elif core_missing:
            notes["missing_reason"] = "rebalance_date row or core price fields missing"
            row["data_status"] = "missing"
        else:
            features, partial_reasons = _snapshot_features(available)
            row.update(features)
            notes["window_observations"] = {
                key: value["observations"] for key, value in partial_reasons.items()
            }
            if any(value["partial"] for value in partial_reasons.values()):
                row["data_status"] = "partial"
                notes["partial_features"] = [
                    key for key, value in partial_reasons.items() if value["partial"]
                ]
            else:
                row["data_status"] = "complete"
        row["quality_notes"] = json.dumps(notes, sort_keys=True)
        return row

    def _write_etf_aw_rebalance_snapshot(
        self, canonical: pd.DataFrame
    ) -> CanonicalWriteResult:
        return self._write_year_month_partition_upsert(
            dataset_name=_ETF_AW_REBALANCE_SNAPSHOT_DATASET,
            zone=StorageZone.DERIVED,
            canonical=canonical,
            key_columns=("calendar_name", "rebalance_date", "sleeve_code"),
            sort_columns=(
                "calendar_name",
                "rebalance_date",
                "sleeve_code",
                "ingested_at",
            ),
            partition_date_column="rebalance_date",
        )

    def _build_etf_aw_regime_score(self, start: date, end: date) -> dict:
        start, end = _ordered_dates(start, end)
        snapshot = self._read_partitioned_dataset(
            _ETF_AW_REBALANCE_SNAPSHOT_DATASET,
            start,
            end,
            StorageZone.DERIVED,
        )
        if snapshot.empty:
            return {
                "profile_name": _ETF_AW_REGIME_SCORE_PROFILE,
                "dataset_name": _ETF_AW_REGIME_SCORE_DATASET,
                "status": RunStatus.FAILED.value,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "records_written": 0,
                "error_message": "ETF all-weather rebalance snapshot is missing",
            }
        score = self._make_etf_aw_regime_score_frame(snapshot)
        if score.empty:
            return {
                "profile_name": _ETF_AW_REGIME_SCORE_PROFILE,
                "dataset_name": _ETF_AW_REGIME_SCORE_DATASET,
                "status": RunStatus.FAILED.value,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "records_written": 0,
                "error_message": (
                    "ETF all-weather regime score has no valid rebalance keys"
                ),
            }
        validation = _validate_regime_score_frame(score)
        if not all(validation.values()):
            return {
                "profile_name": _ETF_AW_REGIME_SCORE_PROFILE,
                "dataset_name": _ETF_AW_REGIME_SCORE_DATASET,
                "status": RunStatus.FAILED.value,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "records_written": 0,
                "validation": validation,
                "error_message": "ETF all-weather regime score validation failed",
            }
        write_result = self._write_etf_aw_regime_score(score)
        return {
            "profile_name": _ETF_AW_REGIME_SCORE_PROFILE,
            "dataset_name": _ETF_AW_REGIME_SCORE_DATASET,
            "status": RunStatus.SUCCESS.value,
            "requested_start": start.isoformat(),
            "requested_end": end.isoformat(),
            "records_written": write_result.records_written,
            "records_inserted": write_result.records_inserted,
            "records_updated": write_result.records_updated,
            "partitions_written": write_result.partitions_written,
            "storage_paths": write_result.storage_paths,
            "validation": validation,
            "scoring_status_counts": _value_counts_dict(score["scoring_status"]),
            "label_counts": _value_counts_dict(score["market_regime_label"]),
        }

    def _make_etf_aw_regime_score_frame(self, snapshot: pd.DataFrame) -> pd.DataFrame:
        frame = snapshot.copy()
        frame["rebalance_date"] = pd.to_datetime(
            frame["rebalance_date"], errors="coerce"
        ).dt.date
        frame = frame.dropna(subset=["calendar_name", "rebalance_date"])
        rows: list[dict[str, Any]] = []
        ingested_at = _utc_now()
        group_columns = ["calendar_name", "rebalance_date"]
        for _, group in frame.groupby(group_columns, sort=True):
            rows.append(_regime_score_row(group, ingested_at))
        return pd.DataFrame(rows)

    def _write_etf_aw_regime_score(
        self, canonical: pd.DataFrame
    ) -> CanonicalWriteResult:
        return self._write_year_month_partition_upsert(
            dataset_name=_ETF_AW_REGIME_SCORE_DATASET,
            zone=StorageZone.DERIVED,
            canonical=canonical,
            key_columns=(
                "calendar_name",
                "rebalance_date",
                "scorer_name",
                "scorer_version",
            ),
            sort_columns=(
                "calendar_name",
                "rebalance_date",
                "scorer_name",
                "scorer_version",
                "ingested_at",
            ),
            partition_date_column="rebalance_date",
        )

    def _build_etf_aw_market_features(self, start: date, end: date) -> dict:
        start, end = _ordered_dates(start, end)
        snapshot = self._read_partitioned_dataset(
            _ETF_AW_REBALANCE_SNAPSHOT_DATASET,
            start,
            end,
            StorageZone.DERIVED,
        )
        if snapshot.empty:
            return {
                "profile_name": _ETF_AW_MARKET_FEATURES_PROFILE,
                "dataset_name": _ETF_AW_MARKET_FEATURES_DATASET,
                "status": RunStatus.FAILED.value,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "records_written": 0,
                "error_message": "ETF all-weather rebalance snapshot is missing",
            }
        regime = self._read_partitioned_dataset(
            _ETF_AW_REGIME_SCORE_DATASET,
            start,
            end,
            StorageZone.DERIVED,
        )
        features = self._make_etf_aw_market_features_frame(snapshot, regime)
        if features.empty:
            return {
                "profile_name": _ETF_AW_MARKET_FEATURES_PROFILE,
                "dataset_name": _ETF_AW_MARKET_FEATURES_DATASET,
                "status": RunStatus.FAILED.value,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "records_written": 0,
                "error_message": (
                    "ETF all-weather market features have no valid rebalance keys"
                ),
            }
        validation = _validate_market_features_frame(features)
        if not all(validation.values()):
            return {
                "profile_name": _ETF_AW_MARKET_FEATURES_PROFILE,
                "dataset_name": _ETF_AW_MARKET_FEATURES_DATASET,
                "status": RunStatus.FAILED.value,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "records_written": 0,
                "validation": validation,
                "error_message": "ETF all-weather market features validation failed",
            }
        write_result = self._write_etf_aw_market_features(features)
        return {
            "profile_name": _ETF_AW_MARKET_FEATURES_PROFILE,
            "dataset_name": _ETF_AW_MARKET_FEATURES_DATASET,
            "status": RunStatus.SUCCESS.value,
            "requested_start": start.isoformat(),
            "requested_end": end.isoformat(),
            "records_written": write_result.records_written,
            "records_inserted": write_result.records_inserted,
            "records_updated": write_result.records_updated,
            "partitions_written": write_result.partitions_written,
            "storage_paths": write_result.storage_paths,
            "validation": validation,
            "feature_status_counts": _value_counts_dict(features["feature_status"]),
        }

    def _make_etf_aw_market_features_frame(
        self, snapshot: pd.DataFrame, regime: pd.DataFrame
    ) -> pd.DataFrame:
        snapshot = _normalize_rebalance_date_frame(snapshot)
        snapshot = snapshot.dropna(subset=["calendar_name", "rebalance_date"])
        if snapshot.empty:
            return pd.DataFrame()
        regime_by_key = _latest_regime_by_key(regime)
        rows: list[dict[str, Any]] = []
        ingested_at = _utc_now()
        for key, group in snapshot.groupby(
            ["calendar_name", "rebalance_date"], sort=True
        ):
            regime_row = regime_by_key.get(key)
            rows.extend(
                _market_feature_rows(
                    group=group,
                    regime_row=regime_row,
                    ingested_at=ingested_at,
                )
            )
        return pd.DataFrame(rows)

    def _write_etf_aw_market_features(
        self, canonical: pd.DataFrame
    ) -> CanonicalWriteResult:
        return self._write_year_month_partition_upsert(
            dataset_name=_ETF_AW_MARKET_FEATURES_DATASET,
            zone=StorageZone.DERIVED,
            canonical=canonical,
            key_columns=(
                "calendar_name",
                "rebalance_date",
                "feature_name",
                "feature_scope",
                "feature_subject",
            ),
            sort_columns=(
                "calendar_name",
                "rebalance_date",
                "feature_name",
                "feature_scope",
                "feature_subject",
                "ingested_at",
            ),
            partition_date_column="rebalance_date",
        )

    def _build_etf_aw_strategy_context(self, start: date, end: date) -> dict:
        start, end = _ordered_dates(start, end)
        features = self._read_partitioned_dataset(
            _ETF_AW_MARKET_FEATURES_DATASET,
            start,
            end,
            StorageZone.DERIVED,
        )
        if features.empty:
            return {
                "profile_name": _ETF_AW_STRATEGY_CONTEXT_PROFILE,
                "dataset_name": _ETF_AW_STRATEGY_CONTEXT_DATASET,
                "status": RunStatus.FAILED.value,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "records_written": 0,
                "error_message": "ETF all-weather market features are missing",
            }
        regime = self._read_partitioned_dataset(
            _ETF_AW_REGIME_SCORE_DATASET,
            start,
            end,
            StorageZone.DERIVED,
        )
        context = self._make_etf_aw_strategy_context_frame(features, regime)
        if context.empty:
            return {
                "profile_name": _ETF_AW_STRATEGY_CONTEXT_PROFILE,
                "dataset_name": _ETF_AW_STRATEGY_CONTEXT_DATASET,
                "status": RunStatus.FAILED.value,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "records_written": 0,
                "error_message": (
                    "ETF all-weather strategy context has no valid rebalance keys"
                ),
            }
        validation = _validate_strategy_context_frame(context)
        if not all(validation.values()):
            return {
                "profile_name": _ETF_AW_STRATEGY_CONTEXT_PROFILE,
                "dataset_name": _ETF_AW_STRATEGY_CONTEXT_DATASET,
                "status": RunStatus.FAILED.value,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "records_written": 0,
                "validation": validation,
                "error_message": "ETF all-weather strategy context validation failed",
            }
        write_result = self._write_etf_aw_strategy_context(context)
        return {
            "profile_name": _ETF_AW_STRATEGY_CONTEXT_PROFILE,
            "dataset_name": _ETF_AW_STRATEGY_CONTEXT_DATASET,
            "status": RunStatus.SUCCESS.value,
            "requested_start": start.isoformat(),
            "requested_end": end.isoformat(),
            "records_written": write_result.records_written,
            "records_inserted": write_result.records_inserted,
            "records_updated": write_result.records_updated,
            "partitions_written": write_result.partitions_written,
            "storage_paths": write_result.storage_paths,
            "validation": validation,
            "context_status_counts": _value_counts_dict(context["context_status"]),
        }

    def _make_etf_aw_strategy_context_frame(
        self, features: pd.DataFrame, regime: pd.DataFrame
    ) -> pd.DataFrame:
        from tradepilot.etl.read_models import get_latest_etf_aw_macro_rates_context

        features = _normalize_rebalance_date_frame(features)
        features = features.dropna(subset=["calendar_name", "rebalance_date"])
        if features.empty:
            return pd.DataFrame()
        regime_by_key = _latest_regime_by_key(regime)
        rows: list[dict[str, Any]] = []
        ingested_at = _utc_now()
        for key, group in features.groupby(
            ["calendar_name", "rebalance_date"], sort=True
        ):
            macro_rates_context = get_latest_etf_aw_macro_rates_context(
                as_of_date=key[1],
                lakehouse_root=self.lakehouse_root,
            )
            rows.append(
                _strategy_context_row(
                    group=group,
                    regime_row=regime_by_key.get(key),
                    macro_rates_context=macro_rates_context,
                    ingested_at=ingested_at,
                )
            )
        return pd.DataFrame(rows)

    def _write_etf_aw_strategy_context(
        self, canonical: pd.DataFrame
    ) -> CanonicalWriteResult:
        return self._write_year_month_partition_upsert(
            dataset_name=_ETF_AW_STRATEGY_CONTEXT_DATASET,
            zone=StorageZone.DERIVED,
            canonical=canonical,
            key_columns=(
                "calendar_name",
                "rebalance_date",
                "strategy_name",
                "strategy_version",
            ),
            sort_columns=(
                "calendar_name",
                "rebalance_date",
                "strategy_name",
                "strategy_version",
                "ingested_at",
            ),
            partition_date_column="rebalance_date",
        )

    def _build_etf_aw_risk_budget(self, start: date, end: date) -> dict:
        start, end = _ordered_dates(start, end)
        context = self._read_partitioned_dataset(
            _ETF_AW_STRATEGY_CONTEXT_DATASET,
            start,
            end,
            StorageZone.DERIVED,
        )
        if context.empty:
            return {
                "profile_name": _ETF_AW_RISK_BUDGET_PROFILE,
                "dataset_name": _ETF_AW_RISK_BUDGET_DATASET,
                "status": RunStatus.FAILED.value,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "records_written": 0,
                "error_message": "ETF all-weather strategy context is missing",
            }
        regime = self._read_partitioned_dataset(
            _ETF_AW_REGIME_SCORE_DATASET,
            start,
            end,
            StorageZone.DERIVED,
        )
        budget = self._make_etf_aw_risk_budget_frame(context, regime)
        if budget.empty:
            return {
                "profile_name": _ETF_AW_RISK_BUDGET_PROFILE,
                "dataset_name": _ETF_AW_RISK_BUDGET_DATASET,
                "status": RunStatus.FAILED.value,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "records_written": 0,
                "error_message": (
                    "ETF all-weather risk budget has no valid rebalance keys"
                ),
            }
        health_findings = _risk_budget_health_findings(budget)
        validation = _validate_risk_budget_frame(budget)
        if not all(validation.values()):
            return {
                "profile_name": _ETF_AW_RISK_BUDGET_PROFILE,
                "dataset_name": _ETF_AW_RISK_BUDGET_DATASET,
                "status": RunStatus.FAILED.value,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "records_written": 0,
                "validation": validation,
                "health_findings": health_findings,
                "error_message": "ETF all-weather risk budget validation failed",
            }
        write_result = self._write_etf_aw_risk_budget(budget)
        return {
            "profile_name": _ETF_AW_RISK_BUDGET_PROFILE,
            "dataset_name": _ETF_AW_RISK_BUDGET_DATASET,
            "status": RunStatus.SUCCESS.value,
            "requested_start": start.isoformat(),
            "requested_end": end.isoformat(),
            "records_written": write_result.records_written,
            "records_inserted": write_result.records_inserted,
            "records_updated": write_result.records_updated,
            "partitions_written": write_result.partitions_written,
            "storage_paths": write_result.storage_paths,
            "validation": validation,
            "health_findings": health_findings,
            "budget_status_counts": _value_counts_dict(budget["budget_status"]),
        }

    def _make_etf_aw_risk_budget_frame(
        self, strategy_context: pd.DataFrame, regime: pd.DataFrame
    ) -> pd.DataFrame:
        return _make_etf_aw_risk_budget_frame(strategy_context, regime)

    def _write_etf_aw_risk_budget(
        self, canonical: pd.DataFrame
    ) -> CanonicalWriteResult:
        return self._write_year_month_partition_upsert(
            dataset_name=_ETF_AW_RISK_BUDGET_DATASET,
            zone=StorageZone.DERIVED,
            canonical=canonical,
            key_columns=(
                "calendar_name",
                "rebalance_date",
                "strategy_name",
                "strategy_version",
                "sleeve_role",
            ),
            sort_columns=(
                "calendar_name",
                "rebalance_date",
                "strategy_name",
                "strategy_version",
                "sleeve_role",
                "ingested_at",
            ),
            partition_date_column="rebalance_date",
        )

    def _build_etf_aw_target_weight(self, start: date, end: date) -> dict:
        start, end = _ordered_dates(start, end)

        def _failed(error_message: str, **extra: Any) -> dict:
            result = {
                "profile_name": _ETF_AW_TARGET_WEIGHT_PROFILE,
                "dataset_name": _ETF_AW_TARGET_WEIGHT_DATASET,
                "status": RunStatus.FAILED.value,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "records_written": 0,
                "error_message": error_message,
            }
            result.update(extra)
            return result

        rebalance = self._read_rebalance_calendar(start, end)
        if rebalance.empty:
            return _failed("canonical rebalance calendar is missing")
        risk_budget = self._read_partitioned_dataset(
            _ETF_AW_RISK_BUDGET_DATASET,
            start,
            end,
            StorageZone.DERIVED,
        )
        if risk_budget.empty:
            return _failed("ETF all-weather risk budget is missing")
        risk_budget = _filter_target_weight_risk_budget_to_calendar(
            risk_budget, rebalance
        )
        if risk_budget.empty:
            return _failed(
                "ETF all-weather risk budget has no calendar-aligned rebalance keys",
            )
        panel = self._read_partitioned_dataset(
            "derived.etf_aw_sleeve_daily",
            start - timedelta(days=_ETF_AW_TARGET_WEIGHT_PANEL_LOOKBACK_DAYS),
            end,
            StorageZone.DERIVED,
        )
        if panel.empty:
            return _failed("derived sleeve daily panel is missing")
        previous_target_weight = self._read_partitioned_dataset(
            _ETF_AW_TARGET_WEIGHT_DATASET,
            start - timedelta(days=370),
            start - timedelta(days=1),
            StorageZone.DERIVED,
        )
        target_weight = self._make_etf_aw_target_weight_frame(
            risk_budget,
            panel,
            previous_target_weight=previous_target_weight,
        )
        if target_weight.empty:
            return _failed("ETF all-weather target weight has no valid rebalance keys")
        validation = _validate_target_weight_frame(target_weight)
        if not all(validation.values()):
            return _failed(
                "ETF all-weather target weight validation failed",
                validation=validation,
            )
        write_result = self._write_etf_aw_target_weight(target_weight)
        return {
            "profile_name": _ETF_AW_TARGET_WEIGHT_PROFILE,
            "dataset_name": _ETF_AW_TARGET_WEIGHT_DATASET,
            "status": RunStatus.SUCCESS.value,
            "requested_start": start.isoformat(),
            "requested_end": end.isoformat(),
            "records_written": write_result.records_written,
            "records_inserted": write_result.records_inserted,
            "records_updated": write_result.records_updated,
            "partitions_written": write_result.partitions_written,
            "storage_paths": write_result.storage_paths,
            "validation": validation,
            "target_weight_status_counts": _value_counts_dict(
                target_weight["target_weight_status"]
            ),
        }

    def _make_etf_aw_target_weight_frame(
        self,
        risk_budget: pd.DataFrame,
        panel: pd.DataFrame,
        previous_target_weight: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        return _make_etf_aw_target_weight_frame(
            risk_budget,
            panel,
            previous_target_weight=previous_target_weight,
        )

    def _write_etf_aw_target_weight(
        self, canonical: pd.DataFrame
    ) -> CanonicalWriteResult:
        return self._write_year_month_partition_upsert(
            dataset_name=_ETF_AW_TARGET_WEIGHT_DATASET,
            zone=StorageZone.DERIVED,
            canonical=canonical,
            key_columns=(
                "calendar_name",
                "rebalance_date",
                "strategy_name",
                "strategy_version",
                "sleeve_code",
            ),
            sort_columns=(
                "calendar_name",
                "rebalance_date",
                "strategy_name",
                "strategy_version",
                "sleeve_role",
                "ingested_at",
            ),
            partition_date_column="rebalance_date",
        )

    def _build_etf_aw_baseline_weight(self, start: date, end: date) -> dict:
        start, end = _ordered_dates(start, end)

        def _failed(error_message: str, **extra: Any) -> dict:
            result = {
                "profile_name": _ETF_AW_BASELINE_WEIGHT_PROFILE,
                "dataset_name": _ETF_AW_BASELINE_WEIGHT_DATASET,
                "status": RunStatus.FAILED.value,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "records_written": 0,
                "error_message": error_message,
            }
            result.update(extra)
            return result

        rebalance = self._read_rebalance_calendar(start, end)
        if rebalance.empty:
            return _failed("canonical rebalance calendar is missing")
        panel = self._read_partitioned_dataset(
            "derived.etf_aw_sleeve_daily",
            start - timedelta(days=_ETF_AW_BASELINE_PANEL_LOOKBACK_DAYS),
            end,
            StorageZone.DERIVED,
        )
        if panel.empty:
            return _failed("derived sleeve daily panel is missing")
        baseline = self._make_etf_aw_baseline_weight_frame(
            rebalance=rebalance,
            panel=panel,
        )
        if baseline.empty:
            return _failed("ETF all-weather baseline weight has no complete vectors")
        validation = _validate_baseline_weight_frame(baseline)
        if not all(validation.values()):
            return _failed(
                "ETF all-weather baseline weight validation failed",
                validation=validation,
            )
        write_result = self._write_etf_aw_baseline_weight(baseline)
        return {
            "profile_name": _ETF_AW_BASELINE_WEIGHT_PROFILE,
            "dataset_name": _ETF_AW_BASELINE_WEIGHT_DATASET,
            "status": RunStatus.SUCCESS.value,
            "requested_start": start.isoformat(),
            "requested_end": end.isoformat(),
            "records_written": write_result.records_written,
            "records_inserted": write_result.records_inserted,
            "records_updated": write_result.records_updated,
            "partitions_written": write_result.partitions_written,
            "storage_paths": write_result.storage_paths,
            "validation": validation,
        }

    def _make_etf_aw_baseline_weight_frame(
        self, *, rebalance: pd.DataFrame, panel: pd.DataFrame
    ) -> pd.DataFrame:
        return _make_etf_aw_baseline_weight_frame(rebalance=rebalance, panel=panel)

    def _write_etf_aw_baseline_weight(
        self, canonical: pd.DataFrame
    ) -> CanonicalWriteResult:
        return self._write_year_month_partition_upsert(
            dataset_name=_ETF_AW_BASELINE_WEIGHT_DATASET,
            zone=StorageZone.DERIVED,
            canonical=canonical,
            key_columns=(
                "calendar_name",
                "rebalance_date",
                "baseline_name",
                "baseline_version",
                "sleeve_code",
            ),
            sort_columns=(
                "calendar_name",
                "rebalance_date",
                "baseline_name",
                "baseline_version",
                "sleeve_role",
                "ingested_at",
            ),
            partition_date_column="rebalance_date",
        )

    def _build_etf_aw_backtest_kernel(
        self,
        start: date,
        end: date,
        *,
        weight_source_type: str = "target_weight",
        baseline_name: str = _ETF_AW_BASELINE_NAME,
        baseline_version: str = _ETF_AW_BASELINE_VERSION,
    ) -> dict:
        start, end = _ordered_dates(start, end)
        requested_weight_source_type = weight_source_type
        weight_source_type = _normalize_backtest_weight_source_type(
            requested_weight_source_type
        )
        if weight_source_type is None:
            return {
                "profile_name": _ETF_AW_BACKTEST_KERNEL_PROFILE,
                "dataset_name": _ETF_AW_BACKTEST_KERNEL_DATASET,
                "status": RunStatus.FAILED.value,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "records_written": 0,
                "weight_source_type": str(requested_weight_source_type),
                "source_weight_dataset": None,
                "error_message": "ETF all-weather weight_source_type is invalid",
            }
        source_dataset = (
            _ETF_AW_TARGET_WEIGHT_DATASET
            if weight_source_type == _ETF_AW_BACKTEST_WEIGHT_SOURCE_TARGET
            else _ETF_AW_BASELINE_WEIGHT_DATASET
        )
        profile_name = (
            _ETF_AW_BACKTEST_KERNEL_PROFILE
            if weight_source_type == _ETF_AW_BACKTEST_WEIGHT_SOURCE_TARGET
            else (
                f"{_ETF_AW_BACKTEST_KERNEL_PROFILE}.{baseline_name}.{baseline_version}"
            )
        )

        def _failed(error_message: str, **extra: Any) -> dict:
            result = {
                "profile_name": profile_name,
                "dataset_name": _ETF_AW_BACKTEST_KERNEL_DATASET,
                "status": RunStatus.FAILED.value,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "records_written": 0,
                "weight_source_type": weight_source_type,
                "source_weight_dataset": source_dataset,
                "error_message": error_message,
            }
            if weight_source_type == _ETF_AW_BACKTEST_WEIGHT_SOURCE_BASELINE:
                result["baseline_name"] = baseline_name
                result["baseline_version"] = baseline_version
            result.update(extra)
            return result

        if weight_source_type == _ETF_AW_BACKTEST_WEIGHT_SOURCE_BASELINE and (
            not str(baseline_name).strip() or not str(baseline_version).strip()
        ):
            return _failed("ETF all-weather baseline name/version is required")

        rebalance = self._read_rebalance_calendar(start, end)
        if rebalance.empty:
            return _failed("canonical rebalance calendar is missing")
        panel = self._read_partitioned_dataset(
            "derived.etf_aw_sleeve_daily",
            start,
            end,
            StorageZone.DERIVED,
        )
        if panel.empty:
            return _failed("derived sleeve daily panel is missing")
        weights = self._read_partitioned_dataset(
            source_dataset,
            start,
            end,
            StorageZone.DERIVED,
        )
        if (
            weight_source_type == _ETF_AW_BACKTEST_WEIGHT_SOURCE_BASELINE
            and not weights.empty
        ):
            weights = weights[
                weights["baseline_name"].astype(str).eq(baseline_name)
                & weights["baseline_version"].astype(str).eq(baseline_version)
            ].copy()
            weights["strategy_name"] = weights["baseline_name"].astype(str)
            weights["strategy_version"] = weights["baseline_version"].astype(str)
        if weights.empty:
            return _failed(f"ETF all-weather {weight_source_type} is missing")
        weights["weight_source_type"] = weight_source_type
        weights["source_weight_dataset"] = source_dataset
        backtest = self._make_etf_aw_backtest_kernel_frame(
            panel=panel,
            rebalance=rebalance,
            weights=weights,
            start=start,
            end=end,
        )
        validation = _validate_backtest_kernel_frame(backtest)
        if not all(validation.values()):
            return _failed(
                "ETF all-weather backtest kernel validation failed",
                validation=validation,
            )
        write_result = self._write_etf_aw_backtest_kernel(backtest)
        result = {
            "profile_name": profile_name,
            "dataset_name": _ETF_AW_BACKTEST_KERNEL_DATASET,
            "status": RunStatus.SUCCESS.value,
            "requested_start": start.isoformat(),
            "requested_end": end.isoformat(),
            "records_written": write_result.records_written,
            "records_inserted": write_result.records_inserted,
            "records_updated": write_result.records_updated,
            "partitions_written": write_result.partitions_written,
            "storage_paths": write_result.storage_paths,
            "validation": validation,
            "weight_source_type": weight_source_type,
            "source_weight_dataset": source_dataset,
            "observation_type_counts": _value_counts_dict(backtest["observation_type"]),
        }
        if weight_source_type == _ETF_AW_BACKTEST_WEIGHT_SOURCE_BASELINE:
            result["baseline_name"] = baseline_name
            result["baseline_version"] = baseline_version
        return result

    def _make_etf_aw_backtest_kernel_frame(
        self,
        *,
        panel: pd.DataFrame,
        rebalance: pd.DataFrame,
        weights: pd.DataFrame,
        start: date,
        end: date,
    ) -> pd.DataFrame:
        return _make_etf_aw_backtest_kernel_frame(
            panel=panel,
            rebalance=rebalance,
            weights=weights,
            start=start,
            end=end,
        )

    def _write_etf_aw_backtest_kernel(
        self, canonical: pd.DataFrame
    ) -> CanonicalWriteResult:
        canonical = canonical.copy()
        if "weight_source_type" not in canonical.columns:
            canonical["weight_source_type"] = "target_weight"
        if "source_weight_dataset" not in canonical.columns:
            canonical["source_weight_dataset"] = _ETF_AW_TARGET_WEIGHT_DATASET
        return self._write_year_month_partition_upsert(
            dataset_name=_ETF_AW_BACKTEST_KERNEL_DATASET,
            zone=StorageZone.DERIVED,
            canonical=canonical,
            key_columns=(
                "calendar_name",
                "strategy_name",
                "strategy_version",
                "weight_source_type",
                "observation_type",
                "observation_date",
                "metric_name",
            ),
            sort_columns=(
                "calendar_name",
                "strategy_name",
                "strategy_version",
                "weight_source_type",
                "observation_type",
                "observation_date",
                "metric_name",
                "ingested_at",
            ),
            partition_date_column="observation_date",
            existing_column_defaults={
                "weight_source_type": _ETF_AW_BACKTEST_WEIGHT_SOURCE_TARGET,
                "source_weight_dataset": _ETF_AW_TARGET_WEIGHT_DATASET,
            },
        )

    def _build_etf_aw_monthly_explainability(self, start: date, end: date) -> dict:
        start, end = _ordered_dates(start, end)
        context = self._read_partitioned_dataset(
            _ETF_AW_STRATEGY_CONTEXT_DATASET,
            start,
            end,
            StorageZone.DERIVED,
        )
        risk_budget = self._read_partitioned_dataset(
            _ETF_AW_RISK_BUDGET_DATASET,
            start,
            end,
            StorageZone.DERIVED,
        )
        target_weight = self._read_partitioned_dataset(
            _ETF_AW_TARGET_WEIGHT_DATASET,
            start,
            end,
            StorageZone.DERIVED,
        )
        backtest = self._read_partitioned_dataset(
            _ETF_AW_BACKTEST_KERNEL_DATASET,
            start,
            end,
            StorageZone.DERIVED,
        )
        if context.empty or risk_budget.empty or target_weight.empty or backtest.empty:
            return {
                "profile_name": _ETF_AW_MONTHLY_EXPLAINABILITY_PROFILE,
                "dataset_name": _ETF_AW_MONTHLY_EXPLAINABILITY_DATASET,
                "status": RunStatus.FAILED.value,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "records_written": 0,
                "error_message": "ETF all-weather explainability inputs are missing",
            }
        explainability = self._make_etf_aw_monthly_explainability_frame(
            strategy_context=context,
            risk_budget=risk_budget,
            target_weight=target_weight,
            backtest_kernel=backtest,
        )
        if explainability.empty:
            return {
                "profile_name": _ETF_AW_MONTHLY_EXPLAINABILITY_PROFILE,
                "dataset_name": _ETF_AW_MONTHLY_EXPLAINABILITY_DATASET,
                "status": RunStatus.FAILED.value,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "records_written": 0,
                "error_message": (
                    "ETF all-weather explainability has no valid rebalance keys"
                ),
            }
        validation = _validate_monthly_explainability_frame(explainability)
        if not all(validation.values()):
            return {
                "profile_name": _ETF_AW_MONTHLY_EXPLAINABILITY_PROFILE,
                "dataset_name": _ETF_AW_MONTHLY_EXPLAINABILITY_DATASET,
                "status": RunStatus.FAILED.value,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "records_written": 0,
                "validation": validation,
                "error_message": "ETF all-weather explainability validation failed",
            }
        write_result = self._write_etf_aw_monthly_explainability(explainability)
        return {
            "profile_name": _ETF_AW_MONTHLY_EXPLAINABILITY_PROFILE,
            "dataset_name": _ETF_AW_MONTHLY_EXPLAINABILITY_DATASET,
            "status": RunStatus.SUCCESS.value,
            "requested_start": start.isoformat(),
            "requested_end": end.isoformat(),
            "records_written": write_result.records_written,
            "records_inserted": write_result.records_inserted,
            "records_updated": write_result.records_updated,
            "partitions_written": write_result.partitions_written,
            "storage_paths": write_result.storage_paths,
            "validation": validation,
        }

    def _make_etf_aw_monthly_explainability_frame(
        self,
        *,
        strategy_context: pd.DataFrame,
        risk_budget: pd.DataFrame,
        target_weight: pd.DataFrame,
        backtest_kernel: pd.DataFrame,
    ) -> pd.DataFrame:
        return _make_etf_aw_monthly_explainability_frame(
            strategy_context=strategy_context,
            risk_budget=risk_budget,
            target_weight=target_weight,
            backtest_kernel=backtest_kernel,
        )

    def _write_etf_aw_monthly_explainability(
        self, canonical: pd.DataFrame
    ) -> CanonicalWriteResult:
        return self._write_year_month_partition_upsert(
            dataset_name=_ETF_AW_MONTHLY_EXPLAINABILITY_DATASET,
            zone=StorageZone.DERIVED,
            canonical=canonical,
            key_columns=(
                "calendar_name",
                "rebalance_date",
                "strategy_name",
                "strategy_version",
            ),
            sort_columns=(
                "calendar_name",
                "rebalance_date",
                "strategy_name",
                "strategy_version",
                "ingested_at",
            ),
            partition_date_column="rebalance_date",
        )

    def _bootstrap_rebalance_calendar_monthly_post_20(
        self, start: date, end: date
    ) -> dict:
        start, end = _ordered_dates(start, end)
        month_starts = _month_starts_for_anchor_range(start, end, _REBALANCE_ANCHOR_DAY)
        generated: list[dict[str, Any]] = []
        missing_calendar_windows: list[dict[str, str]] = []

        for month_start in month_starts:
            anchor = date(month_start.year, month_start.month, _REBALANCE_ANCHOR_DAY)
            search_end = date(
                month_start.year,
                month_start.month,
                monthrange(month_start.year, month_start.month)[1],
            )
            if not self._calendar_window_covered(
                anchor, search_end, _TRADING_CALENDAR_BOOTSTRAP_EXCHANGES
            ):
                missing_calendar_windows.append(
                    {
                        "calendar_month": _calendar_month(month_start),
                        "start": anchor.isoformat(),
                        "end": search_end.isoformat(),
                    }
                )
                continue
            rebalance_date = self._first_common_open_day(
                anchor,
                search_end,
                _TRADING_CALENDAR_BOOTSTRAP_EXCHANGES,
            )
            if rebalance_date is None:
                missing_calendar_windows.append(
                    {
                        "calendar_month": _calendar_month(month_start),
                        "start": anchor.isoformat(),
                        "end": search_end.isoformat(),
                    }
                )
                continue
            generated.append(
                {
                    "calendar_month": _calendar_month(month_start),
                    "rebalance_date": rebalance_date,
                    "effective_date": rebalance_date,
                    "notes": json.dumps(
                        {
                            "anchor_day": _REBALANCE_ANCHOR_DAY,
                            "calendar_month": _calendar_month(month_start),
                            "exchanges": _TRADING_CALENDAR_BOOTSTRAP_EXCHANGES,
                            "rule_name": "first_common_open_day_on_or_after_20th",
                        },
                        sort_keys=True,
                    ),
                }
            )

        if missing_calendar_windows:
            return {
                "profile_name": _REBALANCE_CALENDAR_MONTHLY_PROFILE,
                "dataset_name": "reference.rebalance_calendar",
                "calendar_name": _REBALANCE_CALENDAR_NAME,
                "status": RunStatus.FAILED.value,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "months_total": len(month_starts),
                "months_processed": len(generated),
                "records_written": 0,
                "missing_calendar_windows": missing_calendar_windows,
                "error_message": "trading calendar coverage is incomplete",
            }

        self._write_rebalance_calendar_rows(generated)
        duplicate_rows = self._rebalance_calendar_duplicate_months(start, end)
        status = (
            RunStatus.SUCCESS.value if duplicate_rows == 0 else RunStatus.FAILED.value
        )
        return {
            "profile_name": _REBALANCE_CALENDAR_MONTHLY_PROFILE,
            "dataset_name": "reference.rebalance_calendar",
            "calendar_name": _REBALANCE_CALENDAR_NAME,
            "status": status,
            "requested_start": start.isoformat(),
            "requested_end": end.isoformat(),
            "months_total": len(month_starts),
            "months_processed": len(generated),
            "records_written": len(generated),
            "duplicate_calendar_months": duplicate_rows,
            "rows": [
                {
                    **row,
                    "rebalance_date": row["rebalance_date"].isoformat(),
                    "effective_date": row["effective_date"].isoformat(),
                }
                for row in generated
            ],
        }

    def _first_common_open_day(
        self, start: date, end: date, exchanges: Iterable[str]
    ) -> date | None:
        exchange_list = _unique_strings(exchanges)
        if not exchange_list:
            return None
        self.conn.register(
            "stage_c_common_open_exchanges",
            _trading_calendar_exchange_frame(exchange_list),
        )
        try:
            row = self.conn.execute(
                """
                SELECT c.trade_date
                FROM canonical_trading_calendar c
                JOIN stage_c_common_open_exchanges r
                  ON c.exchange = r.exchange
                WHERE c.is_open = TRUE
                  AND c.trade_date BETWEEN ? AND ?
                GROUP BY c.trade_date
                HAVING COUNT(DISTINCT c.exchange) = ?
                ORDER BY c.trade_date
                LIMIT 1
                """,
                [start, end, len(exchange_list)],
            ).fetchone()
        finally:
            self.conn.unregister("stage_c_common_open_exchanges")
        return row[0] if row else None

    def _write_rebalance_calendar_rows(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        frame = pd.DataFrame(rows)
        frame["calendar_name"] = _REBALANCE_CALENDAR_NAME
        frame["updated_at"] = _utc_now()
        self.conn.register("stage_c_rebalance_calendar", frame)
        try:
            self.conn.execute(
                """
                DELETE FROM canonical_rebalance_calendar
                WHERE calendar_name = ?
                  AND (
                      calendar_month IN (
                          SELECT calendar_month FROM stage_c_rebalance_calendar
                      )
                      OR (
                          calendar_month IS NULL
                          AND json_extract_string(notes, '$.calendar_month') IN (
                              SELECT calendar_month FROM stage_c_rebalance_calendar
                          )
                      )
                  )
                """,
                [_REBALANCE_CALENDAR_NAME],
            )
            self.conn.execute(
                """
                INSERT INTO canonical_rebalance_calendar (
                    calendar_name, calendar_month, rebalance_date, effective_date,
                    notes, updated_at
                )
                SELECT calendar_name, calendar_month, rebalance_date, effective_date,
                       notes, updated_at
                FROM stage_c_rebalance_calendar
                """
            )
        finally:
            self.conn.unregister("stage_c_rebalance_calendar")

    def _rebalance_calendar_duplicate_months(self, start: date, end: date) -> int:
        month_starts = _month_starts_for_anchor_range(start, end, _REBALANCE_ANCHOR_DAY)
        if not month_starts:
            return 0
        month_values = [_calendar_month(month_start) for month_start in month_starts]
        self.conn.register(
            "stage_c_rebalance_months",
            pd.DataFrame({"calendar_month": month_values}),
        )
        try:
            return int(
                self.conn.execute(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT derived_calendar_month, COUNT(*) AS rows_per_month
                        FROM (
                            SELECT COALESCE(
                                       calendar_month,
                                       json_extract_string(notes, '$.calendar_month')
                                   ) AS derived_calendar_month
                            FROM canonical_rebalance_calendar
                            WHERE calendar_name = ?
                        )
                        WHERE derived_calendar_month IN (
                              SELECT calendar_month FROM stage_c_rebalance_months
                        )
                        GROUP BY derived_calendar_month
                        HAVING COUNT(*) > 1
                    )
                    """,
                    [_REBALANCE_CALENDAR_NAME],
                ).fetchone()[0]
            )
        finally:
            self.conn.unregister("stage_c_rebalance_months")

    def _calendar_window_covered(
        self, start: date, end: date, exchanges: Iterable[str]
    ) -> bool:
        start, end = _ordered_dates(start, end)
        exchange_list = _unique_strings(exchanges)
        if not exchange_list:
            return False
        self.conn.register(
            "etl_required_calendar_exchanges",
            _trading_calendar_exchange_frame(exchange_list),
        )
        try:
            rows = self.conn.execute(
                """
                SELECT c.exchange,
                       COUNT(DISTINCT c.trade_date) AS covered_days,
                       MIN(c.trade_date) AS min_date,
                       MAX(c.trade_date) AS max_date
                FROM canonical_trading_calendar c
                JOIN etl_required_calendar_exchanges r
                  ON c.exchange = r.exchange
                WHERE c.trade_date BETWEEN ? AND ?
                GROUP BY c.exchange
                """,
                [start, end],
            ).fetchall()
        finally:
            self.conn.unregister("etl_required_calendar_exchanges")
        expected_days = (end - start).days + 1
        if len(rows) != len(exchange_list):
            return False
        return all(
            int(count) == expected_days and min_date == start and max_date == end
            for _, count, min_date, max_date in rows
        )

    def _trading_calendar_duplicate_key_count(
        self, start: date, end: date, exchanges: Iterable[str]
    ) -> int:
        start, end = _ordered_dates(start, end)
        exchange_list = _unique_strings(exchanges)
        if not exchange_list:
            return 0
        self.conn.register(
            "stage_c_duplicate_calendar_exchanges",
            _trading_calendar_exchange_frame(exchange_list),
        )
        try:
            return int(
                self.conn.execute(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT c.exchange, c.trade_date, COUNT(*) AS rows_per_key
                        FROM canonical_trading_calendar c
                        JOIN stage_c_duplicate_calendar_exchanges r
                          ON c.exchange = r.exchange
                        WHERE c.trade_date BETWEEN ? AND ?
                        GROUP BY c.exchange, c.trade_date
                        HAVING COUNT(*) > 1
                    )
                    """,
                    [start, end],
                ).fetchone()[0]
            )
        finally:
            self.conn.unregister("stage_c_duplicate_calendar_exchanges")

    def _validate_trading_calendar_window(
        self, start: date, end: date, exchanges: Iterable[str]
    ) -> list[ValidationResultRecord]:
        exchange_list = _unique_strings(exchanges)
        if not exchange_list:
            frame = pd.DataFrame(
                columns=["exchange", "trade_date", "is_open", "pretrade_date"]
            )
        else:
            self.conn.register(
                "stage_c_validation_calendar_exchanges",
                _trading_calendar_exchange_frame(exchange_list),
            )
            try:
                frame = self.conn.execute(
                    """
                    SELECT c.exchange, c.trade_date, c.is_open, c.pretrade_date
                    FROM canonical_trading_calendar c
                    JOIN stage_c_validation_calendar_exchanges r
                      ON c.exchange = r.exchange
                    WHERE c.trade_date BETWEEN ? AND ?
                    ORDER BY c.exchange, c.trade_date
                    """,
                    [start, end],
                ).fetchdf()
            finally:
                self.conn.unregister("stage_c_validation_calendar_exchanges")
        validator = get_validator("reference.trading_calendar")
        return validator.validate(
            frame,
            {
                "dataset_name": "reference.trading_calendar",
                "run_id": 0,
            },
        )

    def _ensure_source_registry(self, source_name: str) -> None:
        self.conn.execute(
            "DELETE FROM source_registry WHERE source_name = ?", [source_name]
        )
        self.conn.execute(
            """
            INSERT INTO source_registry (
                source_name, source_type, source_role, is_active, base_note, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                source_name,
                "market_data",
                "primary",
                True,
                "Stage B ETL source",
                _utc_now(),
            ],
        )

    def _mark_stale_running_runs(self, dataset_name: str) -> None:
        self.conn.execute(
            """
            UPDATE etl_ingestion_runs
            SET status = ?, finished_at = ?, error_message = ?
            WHERE dataset_name = ? AND status = ?
            """,
            [
                RunStatus.FAILED.value,
                _utc_now(),
                "marked failed by Stage B recovery before new run",
                dataset_name,
                RunStatus.RUNNING.value,
            ],
        )

    def _assert_source_contract(self, fetch_result: SourceFetchResult) -> None:
        if not isinstance(fetch_result.payload, pd.DataFrame):
            raise TypeError("SourceFetchResult.payload must be a pandas DataFrame")
        if fetch_result.row_count != len(fetch_result.payload):
            raise ValueError(
                "SourceFetchResult.row_count does not match payload length"
            )


def _raw_partition_hints(
    dataset_name: str, request: IngestionRequest
) -> dict[str, str | int]:
    if dataset_name == "reference.instruments":
        return {
            "snapshot_date": str(request.context.get("snapshot_date") or date.today())
        }
    start = request.request_start or request.request_end or date.today()
    return {"year": start.year, "month": f"{start.month:02d}"}


def _unique_strings(values: Iterable[object]) -> list[str]:
    """Return stringified values deduplicated in first-seen order."""

    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value)
        if text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _ordered_dates(start: date, end: date) -> tuple[date, date]:
    """Return dates in ascending order."""

    if start > end:
        return end, start
    return start, end


def _month_windows(start: date, end: date) -> list[tuple[date, date]]:
    """Split an inclusive date range into calendar-month windows."""

    start, end = _ordered_dates(start, end)
    windows: list[tuple[date, date]] = []
    cursor = date(start.year, start.month, 1)
    while cursor <= end:
        month_start = max(start, cursor)
        month_end = min(
            end,
            date(
                cursor.year,
                cursor.month,
                monthrange(cursor.year, cursor.month)[1],
            ),
        )
        windows.append((month_start, month_end))
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)
    return windows


def _month_starts_for_anchor_range(
    start: date, end: date, anchor_day: int
) -> list[date]:
    """Return month starts whose anchor day falls inside an inclusive range."""

    start, end = _ordered_dates(start, end)
    month_starts: list[date] = []
    cursor = date(start.year, start.month, 1)
    while cursor <= end:
        anchor = date(cursor.year, cursor.month, anchor_day)
        if start <= anchor <= end:
            month_starts.append(cursor)
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)
    return month_starts


def _calendar_month(month_start: date) -> str:
    """Return the canonical YYYY-MM label for a calendar month."""

    return f"{month_start.year:04d}-{month_start.month:02d}"


def _business_keys(frame: pd.DataFrame, key_columns: tuple[str, ...]) -> set[tuple]:
    """Return business keys from a canonical frame."""

    if frame.empty:
        return set()
    key_frame = frame.loc[:, list(key_columns)].copy()
    if "trade_date" in key_frame.columns:
        key_frame["trade_date"] = pd.to_datetime(
            key_frame["trade_date"], errors="coerce"
        ).dt.date
    if "rebalance_date" in key_frame.columns:
        key_frame["rebalance_date"] = pd.to_datetime(
            key_frame["rebalance_date"], errors="coerce"
        ).dt.date
    if "observation_date" in key_frame.columns:
        key_frame["observation_date"] = pd.to_datetime(
            key_frame["observation_date"], errors="coerce"
        ).dt.date
    return {
        tuple(str(value) if isinstance(value, str) else value for value in row)
        for row in key_frame.itertuples(index=False)
        if all(pd.notna(value) for value in row)
    }


def _etf_aw_sleeve_codes_frame() -> pd.DataFrame:
    """Return the frozen ETF all-weather sleeve universe as a query frame."""

    return etf_aw_sleeve_codes_frame()


def _trading_calendar_exchange_frame(exchanges: Iterable[str]) -> pd.DataFrame:
    """Return normalized trading calendar exchanges as a query frame."""

    return pd.DataFrame({"exchange": _unique_strings(exchanges)})


def _validate_sleeve_daily_frame(frame: pd.DataFrame) -> dict[str, bool]:
    """Validate the minimum contract for the ETF all-weather sleeve panel."""

    if frame.empty:
        return {
            "non_empty": False,
            "no_duplicate_business_keys": False,
            "adj_factor_present": False,
            "adj_close_positive": False,
            "known_frozen_sleeves_only": False,
        }
    duplicate_count = int(frame.duplicated(["sleeve_code", "trade_date"]).sum())
    known_codes = set(frame["sleeve_code"].dropna().astype(str).tolist())
    return {
        "non_empty": True,
        "no_duplicate_business_keys": duplicate_count == 0,
        "adj_factor_present": bool(frame["adj_factor"].notna().all()),
        "adj_close_positive": bool((frame["adj_close"] > 0).all()),
        "known_frozen_sleeves_only": known_codes.issubset(set(_ETF_AW_SLEEVE_CODES)),
    }


def _snapshot_features(
    available: pd.DataFrame,
) -> tuple[dict[str, float | None], dict[str, dict[str, int | bool]]]:
    available = available.sort_values("trade_date").copy()
    features: dict[str, float | None] = {}
    checks: dict[str, dict[str, int | bool]] = {}
    for key in ("return_1m", "return_3m", "return_6m"):
        window, minimum = _ETF_AW_SNAPSHOT_WINDOWS[key]
        values = available["adj_close"].dropna().astype(float).tail(window + 1)
        observations = max(len(values) - 1, 0)
        checks[key] = {
            "observations": observations,
            "partial": observations < minimum,
        }
        features[key] = (
            float(values.iloc[-1] / values.iloc[0] - 1)
            if observations >= minimum and values.iloc[0] > 0
            else None
        )
    window, minimum = _ETF_AW_SNAPSHOT_WINDOWS["volatility_3m"]
    returns = available["adj_pct_chg"].dropna().astype(float).tail(window) / 100
    checks["volatility_3m"] = {
        "observations": len(returns),
        "partial": len(returns) < minimum,
    }
    features["volatility_3m"] = (
        float(returns.std(ddof=1) * math.sqrt(252)) if len(returns) >= minimum else None
    )
    window, minimum = _ETF_AW_SNAPSHOT_WINDOWS["max_drawdown_6m"]
    values = available["adj_close"].dropna().astype(float).tail(window)
    checks["max_drawdown_6m"] = {
        "observations": len(values),
        "partial": len(values) < minimum,
    }
    if len(values) >= minimum:
        running_max = values.cummax()
        drawdown = values / running_max - 1
        features["max_drawdown_6m"] = float(drawdown.min())
    else:
        features["max_drawdown_6m"] = None
    return features, checks


def _validate_rebalance_snapshot_frame(
    frame: pd.DataFrame, valid_rebalance_dates: set[date]
) -> dict[str, bool]:
    """Validate the minimum contract for ETF all-weather rebalance snapshots."""

    if frame.empty:
        return {
            "non_empty": False,
            "five_rows_per_rebalance_date": False,
            "no_duplicate_business_keys": False,
            "rebalance_dates_from_calendar": False,
            "known_frozen_sleeves_only": False,
            "data_status_allowed": False,
            "quality_notes_json": False,
            "complete_rows_have_features": False,
        }
    duplicate_count = int(
        frame.duplicated(["calendar_name", "rebalance_date", "sleeve_code"]).sum()
    )
    rows_per_date = frame.groupby("rebalance_date")["sleeve_code"].nunique()
    known_codes = set(frame["sleeve_code"].dropna().astype(str).tolist())
    statuses = set(frame["data_status"].dropna().astype(str).tolist())
    complete = frame[frame["data_status"] == "complete"]
    feature_columns = [
        "close",
        "adj_factor",
        "adj_close",
        "return_1m",
        "return_3m",
        "return_6m",
        "volatility_3m",
        "max_drawdown_6m",
    ]
    complete_has_features = (
        True
        if complete.empty
        else bool(complete.loc[:, feature_columns].notna().all().all())
    )
    return {
        "non_empty": True,
        "five_rows_per_rebalance_date": bool((rows_per_date == 5).all()),
        "no_duplicate_business_keys": duplicate_count == 0,
        "rebalance_dates_from_calendar": (
            set(frame["rebalance_date"].dropna().tolist()).issubset(
                valid_rebalance_dates
            )
        ),
        "known_frozen_sleeves_only": known_codes.issubset(set(_ETF_AW_SLEEVE_CODES)),
        "data_status_allowed": statuses.issubset(_ETF_AW_SNAPSHOT_STATUSES),
        "quality_notes_json": all(
            _is_json_text(value) for value in frame["quality_notes"]
        ),
        "complete_rows_have_features": complete_has_features,
    }


def _regime_score_row(group: pd.DataFrame, ingested_at: datetime) -> dict[str, Any]:
    ordered = group.sort_values(["sleeve_role", "sleeve_code"]).copy()
    first = ordered.iloc[0]
    signals = [_sleeve_signal(row) for _, row in ordered.iterrows()]
    role_scores: dict[str, list[float | None]] = {}
    for signal in signals:
        role_scores.setdefault(str(signal["sleeve_role"]), []).append(
            signal["direction_score"]
        )
    equity_score = _average_scores(
        [
            _average_scores(role_scores.get("equity_large", [])),
            _average_scores(role_scores.get("equity_small", [])),
        ]
    )
    bond_score = _average_scores(role_scores.get("bond", []))
    gold_score = _average_scores(role_scores.get("gold", []))
    cash_score = _average_scores(role_scores.get("cash", []))
    market_score = _clamp(
        0.70 * _score_value(equity_score)
        - 0.15 * max(_score_value(bond_score), 0.0)
        - 0.15 * max(_score_value(gold_score), 0.0),
        -100.0,
        100.0,
    )
    confidence_cap, scoring_status, cap_reasons = _regime_confidence_cap(ordered)
    label = _regime_label(
        scoring_status=scoring_status,
        equity_score=equity_score,
        bond_score=bond_score,
        cash_score=cash_score,
        gold_score=gold_score,
        market_score=market_score,
    )
    agreement_score = _agreement_score(label, signals)
    strength_score = min(abs(market_score) / 100.0, 1.0)
    drawdown_penalty = _drawdown_penalty(signals)
    volatility_penalty = _volatility_penalty(signals)
    raw_confidence = (
        0.35
        + 0.35 * agreement_score
        + 0.30 * strength_score
        - drawdown_penalty
        - volatility_penalty
    )
    confidence_score = min(confidence_cap, max(raw_confidence, 0.0))
    quality_notes = {
        "market_only": True,
        "macro_inputs_available": False,
        "rates_inputs_available": False,
        "confidence_cap_reasons": cap_reasons,
        "agreement_score": agreement_score,
        "strength_score": strength_score,
        "drawdown_penalty": drawdown_penalty,
        "volatility_penalty": volatility_penalty,
        "input_data_status_counts": ordered["data_status"].value_counts().to_dict(),
    }
    return {
        "schema_version": _ETF_AW_REGIME_SCHEMA_VERSION,
        "calendar_name": str(first["calendar_name"]),
        "calendar_month": str(first["calendar_month"]),
        "rebalance_date": first["rebalance_date"],
        "scorer_name": _ETF_AW_REGIME_SCORER_NAME,
        "scorer_version": _ETF_AW_REGIME_SCORER_VERSION,
        "input_snapshot_status": _snapshot_status_from_rows(ordered),
        "scoring_status": scoring_status,
        "market_regime_label": label,
        "market_score": market_score,
        "confidence_score": confidence_score,
        "confidence_level": _confidence_level(confidence_score),
        "confidence_cap": confidence_cap,
        "signal_summary": _signal_summary(label, scoring_status),
        "signals_json": json.dumps(signals, sort_keys=True),
        "quality_notes": json.dumps(quality_notes, sort_keys=True),
        "source_snapshot_rebalance_date": first["rebalance_date"],
        "ingested_at": ingested_at,
    }


def _sleeve_signal(row: pd.Series) -> dict[str, Any]:
    direction_score = 0.0
    available_weight = 0.0
    metric_signals: dict[str, int | None] = {}
    for metric, (positive, negative, weight) in _ETF_AW_DIRECTION_RULES.items():
        signal = _metric_signal(row.get(metric), positive, negative)
        metric_signals[metric] = signal
        if signal is not None:
            direction_score += weight * signal
            available_weight += weight
    normalized_direction_score = (
        direction_score / available_weight if available_weight > 0 else None
    )
    return {
        "sleeve_code": str(row["sleeve_code"]),
        "sleeve_role": str(row["sleeve_role"]),
        "direction_score": (
            float(normalized_direction_score)
            if normalized_direction_score is not None
            else None
        ),
        "metric_signals": metric_signals,
        "return_1m": _nullable_float(row.get("return_1m")),
        "return_3m": _nullable_float(row.get("return_3m")),
        "return_6m": _nullable_float(row.get("return_6m")),
        "volatility_3m": _nullable_float(row.get("volatility_3m")),
        "max_drawdown_6m": _nullable_float(row.get("max_drawdown_6m")),
        "data_status": str(row["data_status"]),
    }


def _metric_signal(
    value: object, positive_threshold: float, negative_threshold: float
) -> int | None:
    number = _nullable_float(value)
    if number is None:
        return None
    if number >= positive_threshold:
        return 100
    if number <= negative_threshold:
        return -100
    return 0


def _average_scores(values: list[float | None]) -> float | None:
    available = [float(value) for value in values if value is not None]
    if not available:
        return None
    return sum(available) / len(available)


def _score_value(value: float | None) -> float:
    return float(value) if value is not None else 0.0


def _regime_confidence_cap(frame: pd.DataFrame) -> tuple[float, str, list[str]]:
    statuses = set(frame["data_status"].dropna().astype(str).tolist())
    roles = set(frame["sleeve_role"].dropna().astype(str).tolist())
    if len(frame) < len(_ETF_AW_SLEEVE_CODES) or not ETF_AW_REQUIRED_ROLES.issubset(
        roles
    ):
        return 0.20, "unavailable", ["frozen_sleeve_rows_incomplete"]
    if "missing" in statuses:
        return 0.20, "unavailable", ["missing_sleeve_data"]
    if "stale" in statuses:
        return 0.35, "degraded", ["stale_sleeve_data"]
    if "partial" in statuses:
        return 0.55, "degraded", ["partial_sleeve_data"]
    return 0.70, "complete", ["market_only_inputs"]


def _regime_label(
    *,
    scoring_status: str,
    equity_score: float | None,
    bond_score: float | None,
    cash_score: float | None,
    gold_score: float | None,
    market_score: float,
) -> str:
    if scoring_status == "unavailable":
        return "insufficient_data"
    if (
        gold_score is not None
        and equity_score is not None
        and gold_score >= 45
        and gold_score - equity_score >= 40
    ):
        return "hedge_bid"
    if (
        equity_score is not None
        and equity_score <= -35
        and (
            (bond_score is not None and bond_score >= 0)
            or (cash_score is not None and cash_score >= 0)
        )
    ):
        return "defensive"
    if equity_score is not None and equity_score >= 35 and market_score >= 25:
        return "risk_on"
    return "mixed"


def _agreement_score(label: str, signals: list[dict[str, Any]]) -> float:
    if label == "mixed":
        return 0.25
    if label == "insufficient_data":
        return 0.0
    available = [signal for signal in signals if _signal_available(signal)]
    if not available:
        return 0.0
    consistent = 0
    for signal in available:
        role = str(signal["sleeve_role"])
        score = float(signal["direction_score"])
        if label == "risk_on" and role in {"equity_large", "equity_small"}:
            consistent += int(score > 0)
        elif label == "defensive":
            consistent += int(
                (role in {"equity_large", "equity_small"} and score < 0)
                or (role in {"bond", "cash"} and score >= 0)
            )
        elif label == "hedge_bid":
            consistent += int(
                (role == "gold" and score > 0)
                or (role in {"equity_large", "equity_small"} and score <= 0)
            )
    return consistent / len(available)


def _signal_available(signal: dict[str, Any]) -> bool:
    if signal["data_status"] == "missing":
        return False
    if signal.get("direction_score") is None:
        return False
    metric_signals = signal.get("metric_signals", {})
    return any(value is not None for value in metric_signals.values())


def _drawdown_penalty(signals: list[dict[str, Any]]) -> float:
    drawdowns = [
        float(signal["max_drawdown_6m"])
        for signal in signals
        if signal.get("max_drawdown_6m") is not None and _signal_available(signal)
    ]
    if any(value <= -0.12 for value in drawdowns):
        return 0.15
    if any(value <= -0.08 for value in drawdowns):
        return 0.08
    return 0.0


def _volatility_penalty(signals: list[dict[str, Any]]) -> float:
    for signal in signals:
        if not _signal_available(signal) or signal.get("volatility_3m") is None:
            continue
        role = str(signal["sleeve_role"])
        volatility = float(signal["volatility_3m"])
        if role in {"equity_large", "equity_small"} and volatility >= 0.28:
            return 0.10
        if role not in {"equity_large", "equity_small"} and volatility >= 0.18:
            return 0.10
    return 0.0


def _snapshot_status_from_rows(frame: pd.DataFrame) -> str:
    statuses = set(frame["data_status"].dropna().astype(str).tolist())
    if "missing" in statuses:
        return "missing"
    if "stale" in statuses:
        return "stale"
    if "partial" in statuses:
        return "partial"
    return "complete"


def _confidence_level(confidence_score: float) -> str:
    if confidence_score < 0.35:
        return "low"
    if confidence_score < 0.60:
        return "medium"
    return "high"


def _signal_summary(label: str, scoring_status: str) -> str:
    if label == "insufficient_data":
        return "ETF all-weather market-only regime score is unavailable."
    if scoring_status == "degraded":
        return f"ETF all-weather market-only label is {label} with degraded inputs."
    return f"ETF all-weather market-only label is {label}."


def _validate_regime_score_frame(frame: pd.DataFrame) -> dict[str, bool]:
    """Validate the minimum contract for ETF all-weather regime score rows."""

    if frame.empty:
        return {
            "non_empty": False,
            "no_duplicate_business_keys": False,
            "schema_version_supported": False,
            "scoring_status_allowed": False,
            "label_allowed": False,
            "market_score_finite": False,
            "market_score_in_range": False,
            "confidence_score_finite": False,
            "confidence_score_capped": False,
            "confidence_cap_valid": False,
            "json_fields_valid": False,
            "no_budget_weight_trade_fields": False,
        }
    duplicate_count = int(
        frame.duplicated(
            ["calendar_name", "rebalance_date", "scorer_name", "scorer_version"]
        ).sum()
    )
    market_scores = [_nullable_float(value) for value in frame["market_score"]]
    confidence_scores = [_nullable_float(value) for value in frame["confidence_score"]]
    confidence_caps = [_nullable_float(value) for value in frame["confidence_cap"]]
    blocked_fields = {
        column
        for column in frame.columns
        if any(token in column for token in ("budget", "weight", "trade_action"))
    }
    return {
        "non_empty": True,
        "no_duplicate_business_keys": duplicate_count == 0,
        "schema_version_supported": (
            set(frame["schema_version"].astype(str)) == {_ETF_AW_REGIME_SCHEMA_VERSION}
        ),
        "scoring_status_allowed": (
            set(frame["scoring_status"].astype(str)).issubset(_ETF_AW_REGIME_STATUSES)
        ),
        "label_allowed": (
            set(frame["market_regime_label"].astype(str)).issubset(
                _ETF_AW_REGIME_LABELS
            )
        ),
        "market_score_finite": all(value is not None for value in market_scores),
        "market_score_in_range": all(
            value is not None and -100 <= value <= 100 for value in market_scores
        ),
        "confidence_score_finite": all(
            value is not None for value in confidence_scores
        ),
        "confidence_score_capped": all(
            score is not None and cap is not None and 0 <= score <= cap
            for score, cap in zip(confidence_scores, confidence_caps, strict=True)
        ),
        "confidence_cap_valid": all(
            cap in {0.0, 0.20, 0.35, 0.55, 0.70} for cap in confidence_caps
        ),
        "json_fields_valid": all(
            _is_json_text(row["signals_json"]) and _is_json_text(row["quality_notes"])
            for _, row in frame.iterrows()
        ),
        "no_budget_weight_trade_fields": not blocked_fields,
    }


def _normalize_rebalance_date_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with rebalance_date normalized to Python dates."""

    if frame.empty or "rebalance_date" not in frame.columns:
        return pd.DataFrame()
    normalized = frame.copy()
    normalized["rebalance_date"] = pd.to_datetime(
        normalized["rebalance_date"], errors="coerce"
    ).dt.date
    if "effective_date" in normalized.columns:
        normalized["effective_date"] = pd.to_datetime(
            normalized["effective_date"], errors="coerce"
        ).dt.date
    if "source_rebalance_date" in normalized.columns:
        normalized["source_rebalance_date"] = pd.to_datetime(
            normalized["source_rebalance_date"], errors="coerce"
        ).dt.date
    if "source_snapshot_rebalance_date" in normalized.columns:
        normalized["source_snapshot_rebalance_date"] = pd.to_datetime(
            normalized["source_snapshot_rebalance_date"], errors="coerce"
        ).dt.date
    return normalized


def _latest_regime_by_key(regime: pd.DataFrame) -> dict[tuple[str, date], pd.Series]:
    """Return the latest Stage E regime score row for each rebalance key."""

    if regime.empty:
        return {}
    frame = _normalize_rebalance_date_frame(regime)
    required = {"calendar_name", "rebalance_date"}
    if frame.empty or not required.issubset(frame.columns):
        return {}
    frame = frame.dropna(subset=["calendar_name", "rebalance_date"]).copy()
    if frame.empty:
        return {}
    if "ingested_at" in frame.columns:
        frame["ingested_at"] = pd.to_datetime(frame["ingested_at"], errors="coerce")
        sort_columns = ["calendar_name", "rebalance_date", "ingested_at"]
    else:
        sort_columns = ["calendar_name", "rebalance_date"]
    frame = frame.sort_values(sort_columns)
    latest: dict[tuple[str, date], pd.Series] = {}
    for _, row in frame.iterrows():
        latest[(str(row["calendar_name"]), row["rebalance_date"])] = row
    return latest


def _market_feature_rows(
    *,
    group: pd.DataFrame,
    regime_row: pd.Series | None,
    ingested_at: datetime,
) -> list[dict[str, Any]]:
    """Build long-form Stage G market feature rows for one rebalance date."""

    ordered = group.sort_values(["sleeve_role", "sleeve_code"]).copy()
    first = ordered.iloc[0]
    rows: list[dict[str, Any]] = []
    signals: list[dict[str, Any]] = []
    for _, row in ordered.iterrows():
        signal = _sleeve_signal(row)
        signals.append(signal)
        rows.append(
            _market_feature_row(
                first=first,
                feature_name="direction_score",
                feature_scope="sleeve",
                feature_subject=str(row["sleeve_role"]),
                feature_value=signal["direction_score"],
                source_dataset=_ETF_AW_REBALANCE_SNAPSHOT_DATASET,
                source_status=str(row["data_status"]),
                feature_status=_feature_status_from_source(
                    str(row["data_status"]), signal["direction_score"]
                ),
                quality_notes={
                    "computed_with_stage_e_scorer_helper": True,
                    "metrics_used": [
                        metric
                        for metric, value in signal["metric_signals"].items()
                        if value is not None
                    ],
                    "missing_metrics": [
                        metric
                        for metric, value in signal["metric_signals"].items()
                        if value is None
                    ],
                    "source_quality_notes": _json_object_or_value(
                        row.get("quality_notes")
                    ),
                },
                source_rebalance_date=row.get("rebalance_date"),
                ingested_at=ingested_at,
            )
        )
        for feature_name in (
            "return_1m",
            "return_3m",
            "return_6m",
            "volatility_3m",
            "max_drawdown_6m",
        ):
            value = _nullable_float(row.get(feature_name))
            rows.append(
                _market_feature_row(
                    first=first,
                    feature_name=feature_name,
                    feature_scope="sleeve",
                    feature_subject=str(row["sleeve_role"]),
                    feature_value=value,
                    source_dataset=_ETF_AW_REBALANCE_SNAPSHOT_DATASET,
                    source_status=str(row["data_status"]),
                    feature_status=_feature_status_from_source(
                        str(row["data_status"]), value
                    ),
                    quality_notes={
                        "source_quality_notes": _json_object_or_value(
                            row.get("quality_notes")
                        )
                    },
                    source_rebalance_date=row.get("rebalance_date"),
                    ingested_at=ingested_at,
                )
            )
    rows.extend(
        _group_market_feature_rows(
            first=first,
            signals=signals,
            ingested_at=ingested_at,
        )
    )
    rows.extend(
        _regime_market_feature_rows(
            first=first,
            regime_row=regime_row,
            ingested_at=ingested_at,
        )
    )
    return rows


def _market_feature_row(
    *,
    first: pd.Series,
    feature_name: str,
    feature_scope: str,
    feature_subject: str,
    feature_value: float | None,
    source_dataset: str,
    source_status: str,
    feature_status: str,
    quality_notes: dict[str, Any],
    source_rebalance_date: object,
    ingested_at: datetime,
) -> dict[str, Any]:
    """Return one Stage G long-form market feature row."""

    return {
        "schema_version": _ETF_AW_MARKET_FEATURES_SCHEMA_VERSION,
        "calendar_name": str(first["calendar_name"]),
        "calendar_month": str(first["calendar_month"]),
        "rebalance_date": first["rebalance_date"],
        "feature_name": feature_name,
        "feature_scope": feature_scope,
        "feature_subject": feature_subject,
        "feature_value": feature_value,
        "unit": _ETF_AW_MARKET_FEATURE_UNITS[feature_name],
        "source_dataset": source_dataset,
        "source_status": source_status,
        "feature_status": feature_status,
        "quality_notes": json.dumps(quality_notes, sort_keys=True),
        "source_rebalance_date": source_rebalance_date,
        "ingested_at": ingested_at,
    }


def _group_market_feature_rows(
    *,
    first: pd.Series,
    signals: list[dict[str, Any]],
    ingested_at: datetime,
) -> list[dict[str, Any]]:
    """Return group-level score rows derived from sleeve direction scores."""

    role_signals: dict[str, list[dict[str, Any]]] = {}
    for signal in signals:
        role_signals.setdefault(str(signal["sleeve_role"]), []).append(signal)
    group_specs = {
        "equity_score": ("equity", ["equity_large", "equity_small"]),
        "bond_score": ("bond", ["bond"]),
        "gold_score": ("gold", ["gold"]),
        "cash_score": ("cash", ["cash"]),
    }
    rows: list[dict[str, Any]] = []
    for feature_name, (subject, roles) in group_specs.items():
        selected = [signal for role in roles for signal in role_signals.get(role, [])]
        scores = [signal.get("direction_score") for signal in selected]
        value = _average_scores(scores)
        source_statuses = [str(signal.get("data_status")) for signal in selected]
        rows.append(
            _market_feature_row(
                first=first,
                feature_name=feature_name,
                feature_scope="group",
                feature_subject=subject,
                feature_value=value,
                source_dataset=_ETF_AW_REBALANCE_SNAPSHOT_DATASET,
                source_status=_aggregate_source_status(source_statuses),
                feature_status=_aggregate_feature_status(
                    source_statuses,
                    value,
                    expected_count=len(roles),
                    actual_count=len(selected),
                ),
                quality_notes={
                    "computed_with_stage_e_scorer_helper": True,
                    "roles": roles,
                    "available_role_count": len(
                        [score for score in scores if score is not None]
                    ),
                },
                source_rebalance_date=first.get("rebalance_date"),
                ingested_at=ingested_at,
            )
        )
    return rows


def _regime_market_feature_rows(
    *,
    first: pd.Series,
    regime_row: pd.Series | None,
    ingested_at: datetime,
) -> list[dict[str, Any]]:
    """Return regime-level feature rows from the Stage E market-only score."""

    specs = {
        "market_score": "market_score",
        "market_confidence_score": "confidence_score",
        "market_confidence_cap": "confidence_cap",
    }
    rows: list[dict[str, Any]] = []
    for feature_name, source_column in specs.items():
        value = (
            _nullable_float(regime_row.get(source_column))
            if regime_row is not None
            else None
        )
        source_status = (
            str(regime_row.get("scoring_status"))
            if regime_row is not None
            else "missing"
        )
        input_status = (
            str(regime_row.get("input_snapshot_status"))
            if regime_row is not None
            else "missing"
        )
        rows.append(
            _market_feature_row(
                first=first,
                feature_name=feature_name,
                feature_scope="regime",
                feature_subject="market_only",
                feature_value=value,
                source_dataset=_ETF_AW_REGIME_SCORE_DATASET,
                source_status=source_status,
                feature_status=_regime_feature_status(
                    source_status, input_status, value
                ),
                quality_notes={
                    "market_only": True,
                    "source_quality_notes": _json_object_or_value(
                        regime_row.get("quality_notes")
                        if regime_row is not None
                        else None
                    ),
                },
                source_rebalance_date=(
                    regime_row.get("rebalance_date") if regime_row is not None else None
                ),
                ingested_at=ingested_at,
            )
        )
    return rows


def _feature_status_from_source(source_status: str, value: float | None) -> str:
    """Map one source status and feature value to a Stage G feature status."""

    if source_status == "stale":
        return "stale"
    if source_status == "missing":
        return "missing"
    if value is None:
        return "partial" if source_status == "partial" else "missing"
    if source_status == "partial":
        return "partial"
    return "complete"


def _aggregate_source_status(statuses: list[str]) -> str:
    """Aggregate source statuses without upgrading weak inputs."""

    if not statuses:
        return "missing"
    if "stale" in statuses:
        return "stale"
    if all(status == "missing" for status in statuses):
        return "missing"
    if any(status in {"partial", "missing"} for status in statuses):
        return "partial"
    return "complete"


def _aggregate_feature_status(
    statuses: list[str],
    value: float | None,
    *,
    expected_count: int,
    actual_count: int,
) -> str:
    """Aggregate feature status for a group-level market feature."""

    if actual_count < expected_count:
        return "missing"
    if "stale" in statuses:
        return "stale"
    if value is None:
        return "missing"
    if any(status in {"partial", "missing"} for status in statuses):
        return "partial"
    return "complete"


def _regime_feature_status(
    source_status: str, input_snapshot_status: str, value: float | None
) -> str:
    """Map Stage E status to a Stage G regime feature status."""

    if input_snapshot_status == "stale":
        return "stale"
    if source_status == "unavailable":
        return "missing"
    if value is None:
        return "missing"
    if source_status == "degraded":
        return "partial"
    if source_status == "complete":
        return "complete"
    return "missing"


def _validate_market_features_frame(frame: pd.DataFrame) -> dict[str, bool]:
    """Validate the Stage G market features contract."""

    required_columns = {
        "schema_version",
        "calendar_name",
        "calendar_month",
        "rebalance_date",
        "feature_name",
        "feature_scope",
        "feature_subject",
        "feature_value",
        "unit",
        "source_dataset",
        "source_status",
        "feature_status",
        "quality_notes",
        "source_rebalance_date",
        "ingested_at",
    }
    if frame.empty:
        return {
            "non_empty": False,
            "required_columns_present": False,
            "no_duplicate_business_keys": False,
            "schema_version_supported": False,
            "feature_scope_allowed": False,
            "feature_name_matches_scope": False,
            "feature_status_allowed": False,
            "complete_values_finite": False,
            "quality_notes_json": False,
            "no_macro_rates_features": False,
        }
    missing_columns = required_columns - set(frame.columns)
    if missing_columns:
        return {
            "non_empty": True,
            "required_columns_present": False,
            "no_duplicate_business_keys": False,
            "schema_version_supported": False,
            "feature_scope_allowed": False,
            "feature_name_matches_scope": False,
            "feature_status_allowed": False,
            "complete_values_finite": False,
            "quality_notes_json": False,
            "no_macro_rates_features": False,
        }
    duplicate_count = int(
        frame.duplicated(
            [
                "calendar_name",
                "rebalance_date",
                "feature_name",
                "feature_scope",
                "feature_subject",
            ]
        ).sum()
    )
    complete = frame[frame["feature_status"].astype(str) == "complete"]
    complete_values = [
        _nullable_float(value) for value in complete["feature_value"].tolist()
    ]
    return {
        "non_empty": True,
        "required_columns_present": True,
        "no_duplicate_business_keys": duplicate_count == 0,
        "schema_version_supported": (
            set(frame["schema_version"].astype(str))
            == {_ETF_AW_MARKET_FEATURES_SCHEMA_VERSION}
        ),
        "feature_scope_allowed": (
            set(frame["feature_scope"].astype(str)).issubset(
                set(_ETF_AW_MARKET_FEATURE_SCOPE_NAMES)
            )
        ),
        "feature_name_matches_scope": all(
            str(row["feature_name"])
            in _ETF_AW_MARKET_FEATURE_SCOPE_NAMES.get(str(row["feature_scope"]), set())
            for _, row in frame.iterrows()
        ),
        "feature_status_allowed": (
            set(frame["feature_status"].astype(str)).issubset(
                _ETF_AW_MARKET_FEATURE_STATUSES
            )
        ),
        "complete_values_finite": all(value is not None for value in complete_values),
        "quality_notes_json": all(
            _is_json_text(value) for value in frame["quality_notes"]
        ),
        "no_macro_rates_features": not any(
            any(
                token in str(name)
                for token in ("macro", "rate", "lpr", "curve", "yield")
            )
            for name in frame["feature_name"]
        ),
    }


def _strategy_context_row(
    *,
    group: pd.DataFrame,
    regime_row: pd.Series | None,
    macro_rates_context: dict[str, Any] | None,
    ingested_at: datetime,
) -> dict[str, Any]:
    """Build one Stage G strategy context row."""

    ordered = group.sort_values(["feature_scope", "feature_subject", "feature_name"])
    first = ordered.iloc[0]
    market_context_status = _market_context_status(ordered)
    macro_rates_context_status = _macro_rates_context_status(macro_rates_context)
    context_status, readiness_level = _stage_g_context_status(
        market_context_status,
        macro_rates_context_status,
    )
    context_basis = _stage_g_context_basis(market_context_status, macro_rates_context)
    source_snapshot_rebalance_date = _source_rebalance_date_for_dataset(
        ordered, _ETF_AW_REBALANCE_SNAPSHOT_DATASET
    )
    source_regime_rebalance_date = _source_rebalance_date_for_dataset(
        ordered, _ETF_AW_REGIME_SCORE_DATASET
    )
    market_values = _market_feature_values(ordered)
    point_notes = _stage_g_v0_point_in_time_notes(
        market_context_status,
        macro_rates_context,
    )
    source_caveats = _stage_g_source_caveats(macro_rates_context)
    revision_caveats = _stage_g_revision_caveats(macro_rates_context)
    missing_primary_fields = _stage_g_missing_fields(
        macro_rates_context,
        "missing_primary_fields",
    )
    missing_confirmatory_fields = _stage_g_missing_fields(
        macro_rates_context,
        "missing_confirmatory_fields",
    )
    available_fields = _stage_g_available_fields(macro_rates_context)
    return {
        "schema_version": _ETF_AW_STRATEGY_CONTEXT_SCHEMA_VERSION,
        "contract_version": _ETF_AW_STRATEGY_CONTEXT_CONTRACT_VERSION,
        "calendar_name": str(first["calendar_name"]),
        "calendar_month": str(first["calendar_month"]),
        "rebalance_date": first["rebalance_date"],
        "effective_date": first["rebalance_date"],
        "strategy_name": _ETF_AW_STRATEGY_NAME,
        "strategy_version": _ETF_AW_STRATEGY_VERSION,
        "context_status": context_status,
        "readiness_level": readiness_level,
        "context_basis": context_basis,
        "market_context_status": market_context_status,
        "market_regime_label": (
            _optional_text(regime_row.get("market_regime_label"))
            if regime_row is not None
            else None
        ),
        "market_score": market_values.get("market_score"),
        "market_confidence_score": market_values.get("market_confidence_score"),
        "market_confidence_cap": market_values.get("market_confidence_cap"),
        "macro_rates_context_status": macro_rates_context_status,
        "missing_primary_fields_json": json.dumps(missing_primary_fields),
        "missing_confirmatory_fields_json": json.dumps(missing_confirmatory_fields),
        "available_fields_json": json.dumps(available_fields, sort_keys=True),
        "source_caveats_json": json.dumps(source_caveats, sort_keys=True),
        "revision_caveats_json": json.dumps(revision_caveats, sort_keys=True),
        "point_in_time_notes_json": json.dumps(point_notes, sort_keys=True),
        "market_features_json": json.dumps(
            _market_features_payload(ordered), sort_keys=True
        ),
        "source_snapshot_rebalance_date": source_snapshot_rebalance_date,
        "source_regime_rebalance_date": source_regime_rebalance_date,
        "source_macro_rates_rebalance_date": _stage_g_macro_rates_rebalance_date(
            macro_rates_context
        ),
        "ingested_at": ingested_at,
    }


def _market_context_status(group: pd.DataFrame) -> str:
    """Aggregate Stage G market feature statuses for strategy context."""

    expected = _expected_market_feature_keys()
    actual = {
        (
            str(row["feature_scope"]),
            str(row["feature_name"]),
            str(row["feature_subject"]),
        )
        for _, row in group.iterrows()
    }
    if not expected.issubset(actual):
        return "unavailable"
    statuses = set(group["feature_status"].dropna().astype(str).tolist())
    if "missing" in statuses:
        return "unavailable"
    if "stale" in statuses:
        return "stale"
    if "partial" in statuses:
        return "partial"
    return "complete"


def _expected_market_feature_keys() -> set[tuple[str, str, str]]:
    """Return the complete Stage G v0 market feature key set."""

    keys: set[tuple[str, str, str]] = set()
    for role in ETF_AW_REQUIRED_ROLES:
        for feature_name in _ETF_AW_MARKET_FEATURE_SCOPE_NAMES["sleeve"]:
            keys.add(("sleeve", feature_name, role))
    for feature_name, subject in (
        ("equity_score", "equity"),
        ("bond_score", "bond"),
        ("gold_score", "gold"),
        ("cash_score", "cash"),
    ):
        keys.add(("group", feature_name, subject))
    for feature_name in _ETF_AW_MARKET_FEATURE_SCOPE_NAMES["regime"]:
        keys.add(("regime", feature_name, "market_only"))
    return keys


def _stage_g_context_status(
    market_context_status: str, macro_rates_context_status: str
) -> tuple[str, str]:
    """Apply Stage G status priority across market and macro/rates contexts."""

    if market_context_status == "unavailable":
        return "unavailable", "not_ready"
    if market_context_status == "stale":
        return "stale", "not_ready"
    if macro_rates_context_status in {"stale", "unavailable"}:
        return "partial", "degraded_research"
    if market_context_status == "complete" and macro_rates_context_status == "complete":
        return "complete", "research_ready"
    return "partial", "degraded_research"


def _macro_rates_context_status(macro_rates_context: dict[str, Any] | None) -> str:
    """Return the Stage F macro/rates context status visible to Stage G."""

    if macro_rates_context is None:
        return "deferred"
    status = str(macro_rates_context.get("context_status") or "unavailable")
    if status in _ETF_AW_MACRO_RATES_CONTEXT_STATUSES:
        return status
    return "unavailable"


def _stage_g_context_basis(
    market_context_status: str, macro_rates_context: dict[str, Any] | None
) -> str:
    """Return the evidence basis actually available to Stage G."""

    if market_context_status == "unavailable":
        return "market_only"
    if _macro_rates_context_status(macro_rates_context) == "complete":
        return "market_plus_macro_rates"
    if _stage_g_rates_primary_available(macro_rates_context):
        return "market_plus_rates"
    return "market_only"


def _stage_g_rates_primary_available(
    macro_rates_context: dict[str, Any] | None,
) -> bool:
    """Return whether required primary rates fields are eligible."""

    if macro_rates_context is None:
        return False
    names = {
        str(field.get("field_name"))
        for field in macro_rates_context.get("available_fields", [])
        if isinstance(field, dict)
    }
    return {"shibor_1w", "lpr_1y"}.issubset(names)


def _market_feature_values(group: pd.DataFrame) -> dict[str, float | None]:
    """Return regime feature scalar values by feature name."""

    values: dict[str, float | None] = {}
    for _, row in group[group["feature_scope"].astype(str) == "regime"].iterrows():
        values[str(row["feature_name"])] = _nullable_float(row.get("feature_value"))
    return values


def _source_rebalance_date_for_dataset(
    group: pd.DataFrame, dataset_name: str
) -> date | None:
    """Return the first non-null source rebalance date for one source dataset."""

    if (
        "source_dataset" not in group.columns
        or "source_rebalance_date" not in group.columns
    ):
        return None
    candidates = group[group["source_dataset"].astype(str) == dataset_name]
    for value in candidates["source_rebalance_date"].tolist():
        parsed = pd.to_datetime(value, errors="coerce")
        if not pd.isna(parsed):
            return parsed.date()
    return None


def _stage_g_v0_point_in_time_notes(
    market_context_status: str, macro_rates_context: dict[str, Any] | None
) -> dict[str, Any]:
    """Return repo-visible Stage F audit and Stage G v0 deferred notes."""

    macro_rates_notes = (
        macro_rates_context.get("quality_notes", {})
        if macro_rates_context is not None
        else {}
    )
    return {
        "stage_f_audit": {
            "rates.daily_rates_registered": True,
            "rates.lpr_registered": True,
            "macro.slow_fields_registered": True,
            "rates.gov_curve_points_registered": True,
            "macro_rates_read_service_available": macro_rates_context is not None,
        },
        "macro_fields_deferred": bool(
            macro_rates_notes.get("macro_fields_deferred", macro_rates_context is None)
        ),
        "curve_fields_deferred": bool(
            macro_rates_notes.get("curve_fields_deferred", macro_rates_context is None)
        ),
        "rates_read_service_deferred": macro_rates_context is None,
        "rates_primary_fields_available": _stage_g_rates_primary_available(
            macro_rates_context
        ),
        "market_context_status": market_context_status,
        "macro_rates_context_status": _macro_rates_context_status(macro_rates_context),
        "macro_rates_quality_notes": macro_rates_notes,
        "stage_g_v0_completion": True,
        "point_in_time_rule": (
            "Stage G consumes Stage F observations only when effective_date is "
            "less than or equal to rebalance_date."
        ),
    }


def _stage_g_source_caveats(
    macro_rates_context: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """Return source caveats propagated into Stage G strategy context."""

    caveats = (
        list(macro_rates_context.get("source_caveats", []))
        if macro_rates_context is not None
        else []
    )
    quality_notes = (
        macro_rates_context.get("quality_notes", {})
        if macro_rates_context is not None
        else {}
    )
    if quality_notes.get("macro_fields_deferred", macro_rates_context is None):
        caveats.append(
            {
                "field_family": "macro.slow_fields",
                "status": "deferred",
                "reason": "required_macro_fields_missing",
            }
        )
    if quality_notes.get("curve_fields_deferred", macro_rates_context is None):
        caveats.append(
            {
                "field_family": "rates.gov_curve_points",
                "status": "deferred",
                "reason": "required_curve_fields_missing",
            }
        )
    if macro_rates_context is None:
        caveats.append(
            {
                "field_family": "macro_rates_read_service",
                "status": "deferred",
                "reason": "get_latest_etf_aw_macro_rates_context_not_available",
            }
        )
    return caveats


def _stage_g_revision_caveats(
    macro_rates_context: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """Return revision caveats propagated into Stage G strategy context."""

    caveats = (
        list(macro_rates_context.get("revision_caveats", []))
        if macro_rates_context is not None
        else []
    )
    if macro_rates_context is None:
        caveats.append(
            {
                "field_family": "macro_or_curve",
                "status": "deferred",
                "reason": "revision_caveats_deferred_until_macro_curve_datasets_exist",
            }
        )
    return caveats


def _stage_g_missing_fields(
    macro_rates_context: dict[str, Any] | None, key: str
) -> list[str]:
    """Return missing macro/rates fields for Stage G JSON fields."""

    if macro_rates_context is None:
        if key == "missing_primary_fields":
            return sorted(_ETF_AW_STAGE_G_DEFERRED_PRIMARY_FIELDS)
        return sorted(_ETF_AW_STAGE_G_DEFERRED_CONFIRMATORY_FIELDS)
    values = macro_rates_context.get(key, [])
    return sorted(str(value) for value in values)


def _stage_g_available_fields(
    macro_rates_context: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """Return eligible macro/rates fields for Stage G JSON fields."""

    if macro_rates_context is None:
        return []
    return [
        field
        for field in macro_rates_context.get("available_fields", [])
        if isinstance(field, dict)
    ]


def _stage_g_macro_rates_rebalance_date(
    macro_rates_context: dict[str, Any] | None,
) -> date | None:
    """Return the Stage F context rebalance date."""

    if macro_rates_context is None:
        return None
    parsed = pd.to_datetime(macro_rates_context.get("rebalance_date"), errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


def _market_features_payload(group: pd.DataFrame) -> dict[str, Any]:
    """Return API-friendly selected market features."""

    payload: dict[str, Any] = {"sleeve": {}, "group": {}, "regime": {}}
    for _, row in group.iterrows():
        scope = str(row["feature_scope"])
        subject = str(row["feature_subject"])
        feature_name = str(row["feature_name"])
        scoped = payload.setdefault(scope, {})
        subject_payload = scoped.setdefault(subject, {})
        subject_payload[feature_name] = {
            "value": _nullable_float(row.get("feature_value")),
            "status": str(row["feature_status"]),
            "unit": str(row["unit"]),
        }
    return payload


def _validate_strategy_context_frame(frame: pd.DataFrame) -> dict[str, bool]:
    """Validate the Stage G strategy context contract."""

    if frame.empty:
        return {
            "non_empty": False,
            "no_duplicate_business_keys": False,
            "status_values_allowed": False,
            "complete_has_no_missing_primary_fields": False,
            "complete_macro_status_allowed": False,
            "research_ready_requires_complete": False,
            "context_basis_allowed": False,
            "forbidden_fields_absent": False,
            "json_fields_valid": False,
            "deferred_context_has_point_in_time_notes": False,
        }
    duplicate_count = int(
        frame.duplicated(
            [
                "calendar_name",
                "rebalance_date",
                "strategy_name",
                "strategy_version",
            ]
        ).sum()
    )
    forbidden_fields = {
        column
        for column in frame.columns
        if any(token in column for token in _ETF_AW_FORBIDDEN_STRATEGY_FIELD_TOKENS)
    }
    complete = frame[frame["context_status"].astype(str) == "complete"]
    json_columns = [
        "missing_primary_fields_json",
        "missing_confirmatory_fields_json",
        "available_fields_json",
        "source_caveats_json",
        "revision_caveats_json",
        "point_in_time_notes_json",
        "market_features_json",
    ]
    return {
        "non_empty": True,
        "no_duplicate_business_keys": duplicate_count == 0,
        "status_values_allowed": (
            set(frame["context_status"].astype(str)).issubset(
                _ETF_AW_STRATEGY_CONTEXT_STATUSES
            )
            and set(frame["readiness_level"].astype(str)).issubset(
                _ETF_AW_READINESS_LEVELS
            )
            and set(frame["macro_rates_context_status"].astype(str)).issubset(
                _ETF_AW_MACRO_RATES_CONTEXT_STATUSES
            )
        ),
        "complete_has_no_missing_primary_fields": all(
            _json_list_or_empty(row["missing_primary_fields_json"]) == []
            for _, row in complete.iterrows()
        ),
        "complete_macro_status_allowed": all(
            row["macro_rates_context_status"] == "complete"
            for _, row in complete.iterrows()
        ),
        "research_ready_requires_complete": all(
            row["context_status"] == "complete"
            for _, row in frame[
                frame["readiness_level"].astype(str) == "research_ready"
            ].iterrows()
        ),
        "context_basis_allowed": (
            set(frame["context_basis"].astype(str)).issubset(_ETF_AW_CONTEXT_BASES)
        ),
        "forbidden_fields_absent": not forbidden_fields,
        "json_fields_valid": all(
            _is_json_text(row[column])
            for _, row in frame.iterrows()
            for column in json_columns
        ),
        "deferred_context_has_point_in_time_notes": all(
            bool(_json_object_or_value(row["point_in_time_notes_json"]))
            for _, row in frame[
                frame["macro_rates_context_status"].astype(str) != "complete"
            ].iterrows()
        ),
    }


def _make_etf_aw_risk_budget_frame(
    strategy_context: pd.DataFrame, regime: pd.DataFrame
) -> pd.DataFrame:
    """Build V1 sleeve risk budgets from strategy context and regime rows."""

    if strategy_context.empty:
        return pd.DataFrame()
    context = _normalize_rebalance_date_frame(strategy_context)
    context = context.dropna(subset=["calendar_name", "rebalance_date"])
    if context.empty:
        return pd.DataFrame()
    context = context.sort_values(["rebalance_date", "ingested_at"])
    context = context.drop_duplicates(
        ["calendar_name", "rebalance_date", "strategy_name", "strategy_version"],
        keep="last",
    )
    regime_by_key = _latest_regime_by_key(regime)
    rows: list[dict[str, Any]] = []
    ingested_at = _utc_now()
    for _, context_row in context.iterrows():
        key = (str(context_row["calendar_name"]), context_row["rebalance_date"])
        rows.extend(
            _risk_budget_rows(
                context_row=context_row,
                regime_row=regime_by_key.get(key),
                ingested_at=ingested_at,
            )
        )
    return pd.DataFrame(rows)


def _risk_budget_rows(
    *,
    context_row: pd.Series,
    regime_row: pd.Series | None,
    ingested_at: datetime,
) -> list[dict[str, Any]]:
    rebalance_date = context_row["rebalance_date"]
    context_status = str(context_row.get("context_status") or "unavailable")
    readiness = str(context_row.get("readiness_level") or "not_ready")
    macro_status = str(context_row.get("macro_rates_context_status") or "unavailable")
    market_label = _optional_text(context_row.get("market_regime_label"))
    if regime_row is not None:
        market_label = (
            _optional_text(regime_row.get("market_regime_label")) or market_label
        )
    regime_status = (
        str(regime_row.get("scoring_status") or "unavailable")
        if regime_row is not None
        else "missing"
    )
    raw_confidence = _risk_budget_raw_confidence(context_row, regime_row)
    source_context_date = _series_date(context_row.get("rebalance_date"))
    source_regime_date = (
        _series_date(regime_row.get("rebalance_date"))
        if regime_row is not None
        else None
    )
    budget_status, effective_confidence, basis, reasons = _risk_budget_decision(
        context_status=context_status,
        readiness_level=readiness,
        regime_status=regime_status,
        market_label=market_label,
        confidence_score=raw_confidence,
        rebalance_date=rebalance_date,
        source_context_date=source_context_date,
        source_regime_date=source_regime_date,
    )
    delta = _ETF_AW_RISK_BUDGET_DELTAS.get(
        market_label or "insufficient_data",
        _ETF_AW_RISK_BUDGET_DELTAS["insufficient_data"],
    )
    raw_budget = {
        role: _ETF_AW_RISK_BUDGET_BASE[role] + effective_confidence * delta[role]
        for role in ETF_AW_ROLE_ORDER
    }
    if min(raw_budget.values()) < 0.05:
        budget_status = "partial"
        basis = "degraded_neutral_budget"
        effective_confidence = 0.0
        raw_budget = dict(_ETF_AW_RISK_BUDGET_BASE)
        reasons.append("raw_budget_floor_breach")
    total = sum(raw_budget.values())
    tilted = _round_role_budgets(
        {role: value / total for role, value in raw_budget.items()}
    )
    notes = {
        "reasons": sorted(set(reasons)),
        "source_context_status": context_status,
        "source_readiness_level": readiness,
        "source_regime_status": regime_status,
        "raw_confidence_score": raw_confidence,
        "effective_confidence_score": effective_confidence,
        "raw_budget_min": min(raw_budget.values()),
        "delta_budget_sum": round(sum(delta.values()), 12),
        "tilted_budget_sum": round(sum(tilted.values()), 6),
        "macro_rates_context_status": macro_status,
        "caveats": _risk_budget_caveats(context_row),
    }
    common = {
        "schema_version": _ETF_AW_RISK_BUDGET_SCHEMA_VERSION,
        "contract_version": _ETF_AW_RISK_BUDGET_CONTRACT_VERSION,
        "calendar_name": str(context_row["calendar_name"]),
        "rebalance_date": rebalance_date,
        "strategy_name": _ETF_AW_STRATEGY_NAME,
        "strategy_version": _ETF_AW_RISK_BUDGET_STRATEGY_VERSION,
        "confidence_score": raw_confidence,
        "effective_confidence_score": effective_confidence,
        "market_regime_label": market_label or "insufficient_data",
        "budget_status": budget_status,
        "budget_basis": basis,
        "quality_notes_json": json.dumps(notes, sort_keys=True),
        "source_strategy_context_rebalance_date": source_context_date,
        "source_regime_rebalance_date": source_regime_date,
        "ingested_at": ingested_at,
    }
    return [
        {
            **common,
            "sleeve_role": role,
            "base_budget": round(_ETF_AW_RISK_BUDGET_BASE[role], 6),
            "delta_budget": round(delta[role], 6),
            "tilted_budget": tilted[role],
        }
        for role in ETF_AW_ROLE_ORDER
    ]


def _round_role_budgets(budgets: dict[str, float]) -> dict[str, float]:
    """Round sleeve budgets while keeping the monthly sum exactly stable."""

    scale = 1_000_000
    raw_units = {role: budgets[role] * scale for role in ETF_AW_ROLE_ORDER}
    rounded_units = {role: math.floor(raw_units[role]) for role in ETF_AW_ROLE_ORDER}
    remainder = scale - sum(rounded_units.values())
    if remainder > 0:
        roles = sorted(
            ETF_AW_ROLE_ORDER,
            key=lambda role: raw_units[role] - rounded_units[role],
            reverse=True,
        )
        for role in roles[:remainder]:
            rounded_units[role] += 1
    return {role: rounded_units[role] / scale for role in ETF_AW_ROLE_ORDER}


def _risk_budget_decision(
    *,
    context_status: str,
    readiness_level: str,
    regime_status: str,
    market_label: str | None,
    confidence_score: float | None,
    rebalance_date: date,
    source_context_date: date | None,
    source_regime_date: date | None,
) -> tuple[str, float, str, list[str]]:
    reasons: list[str] = []
    if source_context_date is not None and source_context_date > rebalance_date:
        return (
            "unavailable",
            0.0,
            "unavailable_neutral_budget",
            ["source_context_after_rebalance_date"],
        )
    if source_regime_date is not None and source_regime_date > rebalance_date:
        return (
            "unavailable",
            0.0,
            "unavailable_neutral_budget",
            ["source_regime_after_rebalance_date"],
        )
    if context_status == "missing":
        return (
            "missing",
            0.0,
            "unavailable_neutral_budget",
            ["strategy_context_missing"],
        )
    if context_status == "stale":
        return "stale", 0.0, "degraded_neutral_budget", ["strategy_context_stale"]
    if context_status == "unavailable":
        return (
            "unavailable",
            0.0,
            "unavailable_neutral_budget",
            ["strategy_context_unavailable"],
        )
    confidence_cap = 0.70
    budget_status = "complete"
    if context_status == "partial" or readiness_level == "degraded_research":
        budget_status = "partial"
        confidence_cap = 0.35
        reasons.append("strategy_context_partial")
    if regime_status == "missing":
        return "unavailable", 0.0, "unavailable_neutral_budget", ["regime_missing"]
    if regime_status == "stale":
        return "stale", 0.0, "degraded_neutral_budget", ["regime_stale"]
    if regime_status == "unavailable":
        return "partial", 0.0, "degraded_neutral_budget", ["regime_insufficient_data"]
    if regime_status == "partial":
        budget_status = "partial"
        confidence_cap = min(confidence_cap, 0.35)
        reasons.append("regime_partial")
    if market_label == "insufficient_data":
        return (
            "partial",
            0.0,
            "degraded_neutral_budget",
            [
                *reasons,
                "regime_insufficient_data",
            ],
        )
    if market_label not in _ETF_AW_RISK_BUDGET_DELTAS:
        return (
            "unavailable",
            0.0,
            "unavailable_neutral_budget",
            [
                *reasons,
                "unsupported_regime_label",
            ],
        )
    if confidence_score is None or confidence_score < 0.25:
        return (
            "partial",
            0.0,
            "degraded_neutral_budget",
            [
                *reasons,
                "low_or_missing_confidence",
            ],
        )
    effective = _clamp(float(confidence_score), 0.0, confidence_cap)
    basis = "market_regime_tilt" if effective > 0.0 else "neutral_equal_risk_budget"
    return budget_status, effective, basis, reasons


def _risk_budget_raw_confidence(
    context_row: pd.Series, regime_row: pd.Series | None
) -> float | None:
    if regime_row is not None:
        value = _nullable_float(regime_row.get("confidence_score"))
        if value is not None:
            return value
    return _nullable_float(context_row.get("market_confidence_score"))


def _risk_budget_caveats(context_row: pd.Series) -> list[dict[str, Any]]:
    caveats: list[dict[str, Any]] = []
    for column in ("source_caveats_json", "revision_caveats_json"):
        value = context_row.get(column)
        if not _is_json_text(value):
            continue
        loaded = json.loads(value)
        if isinstance(loaded, list):
            caveats.extend(item for item in loaded if isinstance(item, dict))
    return caveats


def _series_date(value: object) -> date | None:
    parsed = pd.to_datetime(value, errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


def _validate_risk_budget_frame(frame: pd.DataFrame) -> dict[str, bool]:
    """Validate the ETF all-weather risk budget contract."""

    failed_checks = {
        str(finding.get("check_name"))
        for finding in _risk_budget_health_findings(frame)
        if str(finding.get("level")) == "FAIL"
    }
    return {
        check_name: check_name not in failed_checks
        for check_name in _ETF_AW_RISK_BUDGET_VALIDATION_CHECKS
    }


def _risk_budget_health_findings(frame: pd.DataFrame) -> list[dict[str, Any]]:
    """Return row-level health findings for risk budget write validation."""

    if frame.empty:
        return [
            _risk_budget_finding(
                "FAIL",
                "non_empty",
                None,
                None,
                "risk budget output is empty",
            )
        ]
    required_columns = {
        "calendar_name",
        "rebalance_date",
        "strategy_name",
        "strategy_version",
        "sleeve_role",
        "base_budget",
        "delta_budget",
        "tilted_budget",
        "budget_status",
        "budget_basis",
        "market_regime_label",
        "quality_notes_json",
        "source_strategy_context_rebalance_date",
        "source_regime_rebalance_date",
    }
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        return [
            _risk_budget_finding(
                "FAIL",
                "required_columns_present",
                None,
                None,
                f"missing required columns: {', '.join(missing_columns)}",
            )
        ]

    findings: list[dict[str, Any]] = []
    key_columns = [
        "calendar_name",
        "rebalance_date",
        "strategy_name",
        "strategy_version",
        "sleeve_role",
    ]
    duplicated = frame[frame.duplicated(key_columns, keep=False)]
    for _, row in duplicated.iterrows():
        findings.append(
            _risk_budget_finding(
                "FAIL",
                "no_duplicate_business_keys",
                row["rebalance_date"],
                row["sleeve_role"],
                "duplicate risk budget business key",
            )
        )

    forbidden_fields = sorted(
        set(frame.columns)
        & {
            "target_weight",
            "raw_target_weight",
            "constrained_target_weight",
            "trade_action",
            "order_instruction",
            "rebalance_instruction",
        }
    )
    if forbidden_fields:
        findings.append(
            _risk_budget_finding(
                "FAIL",
                "forbidden_fields_absent",
                None,
                None,
                f"forbidden output fields present: {', '.join(forbidden_fields)}",
            )
        )

    grouped = frame.groupby(
        ["calendar_name", "rebalance_date", "strategy_name", "strategy_version"],
        dropna=False,
    )
    for _, group in grouped:
        rebalance_date = group.iloc[0]["rebalance_date"]
        roles = set(group["sleeve_role"].astype(str))
        if roles != ETF_AW_REQUIRED_ROLES or len(group) != len(ETF_AW_REQUIRED_ROLES):
            findings.append(
                _risk_budget_finding(
                    "FAIL",
                    "five_roles_per_rebalance_date",
                    rebalance_date,
                    None,
                    "risk budget must output exactly five frozen sleeve roles",
                )
            )
        base = pd.to_numeric(group["base_budget"], errors="coerce")
        delta = pd.to_numeric(group["delta_budget"], errors="coerce")
        tilted = pd.to_numeric(group["tilted_budget"], errors="coerce")
        if (
            base.isna().any()
            or delta.isna().any()
            or tilted.isna().any()
            or bool((base < 0).any())
            or bool((tilted < 0).any())
            or abs(float(base.sum()) - 1.0) > 1e-6
            or abs(float(delta.sum())) > 1e-6
            or abs(float(tilted.sum()) - 1.0) > 1e-6
        ):
            findings.append(
                _risk_budget_finding(
                    "FAIL",
                    "budget_sums_valid",
                    rebalance_date,
                    None,
                    "base, delta, and tilted budgets violate sum or numeric checks",
                )
            )

    invalid_statuses = (
        set(frame["budget_status"].astype(str)) - _ETF_AW_RISK_BUDGET_STATUSES
    )
    if invalid_statuses:
        findings.append(
            _risk_budget_finding(
                "FAIL",
                "status_values_allowed",
                None,
                None,
                f"invalid budget_status values: {', '.join(sorted(invalid_statuses))}",
            )
        )
    for _, row in frame.iterrows():
        if not _is_json_text(row["quality_notes_json"]):
            findings.append(
                _risk_budget_finding(
                    "FAIL",
                    "quality_notes_json",
                    row["rebalance_date"],
                    row["sleeve_role"],
                    "quality_notes_json is not valid JSON",
                )
            )
        if (
            str(row["budget_basis"]) == "market_regime_tilt"
            and str(row["market_regime_label"]) not in _ETF_AW_RISK_BUDGET_DELTAS
        ):
            findings.append(
                _risk_budget_finding(
                    "FAIL",
                    "market_regime_label_allowed_for_tilt",
                    row["rebalance_date"],
                    row["sleeve_role"],
                    "active risk budget tilt uses an unsupported regime label",
                )
            )
    source_context_dates = pd.to_datetime(
        frame["source_strategy_context_rebalance_date"], errors="coerce"
    ).dt.date
    source_regime_dates = pd.to_datetime(
        frame["source_regime_rebalance_date"], errors="coerce"
    ).dt.date
    rebalance_dates = pd.to_datetime(frame["rebalance_date"], errors="coerce").dt.date
    for row, source_context, source_regime, rebalance in zip(
        frame.to_dict("records"),
        source_context_dates,
        source_regime_dates,
        rebalance_dates,
        strict=True,
    ):
        if (not pd.isna(source_context) and source_context > rebalance) or (
            not pd.isna(source_regime) and source_regime > rebalance
        ):
            findings.append(
                _risk_budget_finding(
                    "FAIL",
                    "point_in_time_sources",
                    row["rebalance_date"],
                    row["sleeve_role"],
                    "source context or regime date is after rebalance_date",
                )
            )
    return findings


def _risk_budget_finding(
    level: str,
    check_name: str,
    rebalance_date: object,
    sleeve_role: object,
    message: str,
) -> dict[str, Any]:
    parsed_date = _series_date(rebalance_date)
    return {
        "level": level,
        "check_name": check_name,
        "rebalance_date": parsed_date.isoformat() if parsed_date is not None else None,
        "sleeve_role": None if pd.isna(sleeve_role) else str(sleeve_role),
        "message": message,
    }


def _make_etf_aw_target_weight_frame(
    risk_budget: pd.DataFrame,
    panel: pd.DataFrame,
    *,
    previous_target_weight: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build V1 budgeted inverse-vol target weights from frozen artifacts."""

    if risk_budget.empty or panel.empty:
        return pd.DataFrame()
    if not _ETF_AW_TARGET_WEIGHT_RISK_BUDGET_COLUMNS.issubset(risk_budget.columns):
        return pd.DataFrame()
    if not _ETF_AW_TARGET_WEIGHT_PANEL_RETURN_COLUMNS.issubset(panel.columns):
        return pd.DataFrame()
    if "daily_return" not in panel.columns and "adj_close" not in panel.columns:
        return pd.DataFrame()
    budget = _normalize_rebalance_date_frame(risk_budget)
    budget = budget.dropna(subset=["calendar_name", "rebalance_date"])
    budget = budget[
        budget["strategy_name"].astype(str).eq(_ETF_AW_STRATEGY_NAME)
        & budget["strategy_version"]
        .astype(str)
        .eq(_ETF_AW_RISK_BUDGET_STRATEGY_VERSION)
    ].copy()
    if budget.empty:
        return pd.DataFrame()
    budget = budget.sort_values(["rebalance_date", "ingested_at"])
    budget = budget.drop_duplicates(
        [
            "calendar_name",
            "rebalance_date",
            "strategy_name",
            "strategy_version",
            "sleeve_role",
        ],
        keep="last",
    )
    panel = _target_weight_panel_returns(panel)
    rows: list[dict[str, Any]] = []
    previous_by_key = _latest_previous_target_by_key(previous_target_weight)
    ingested_at = _utc_now()
    for key, group in budget.groupby(
        ["calendar_name", "rebalance_date", "strategy_name", "strategy_version"],
        sort=True,
    ):
        group = group.sort_values("sleeve_role", key=etf_aw_role_sort_key)
        generated = _target_weight_rows_for_rebalance(
            key=key,
            budget_group=group,
            panel=panel,
            previous_target=previous_by_key.get(
                (
                    str(key[0]),
                    _ETF_AW_STRATEGY_NAME,
                    _ETF_AW_TARGET_WEIGHT_STRATEGY_VERSION,
                )
            ),
            ingested_at=ingested_at,
        )
        if not generated:
            continue
        rows.extend(generated)
        previous_by_key[
            (
                str(key[0]),
                _ETF_AW_STRATEGY_NAME,
                _ETF_AW_TARGET_WEIGHT_STRATEGY_VERSION,
            )
        ] = {str(row["sleeve_code"]): float(row["target_weight"]) for row in generated}
    return pd.DataFrame(rows)


def _filter_target_weight_risk_budget_to_calendar(
    risk_budget: pd.DataFrame, rebalance: pd.DataFrame
) -> pd.DataFrame:
    if risk_budget.empty or rebalance.empty:
        return pd.DataFrame()
    budget = _normalize_rebalance_date_frame(risk_budget)
    calendar = _normalize_rebalance_date_frame(rebalance)
    calendar_keys = calendar.dropna(subset=["calendar_name", "rebalance_date"])[
        ["calendar_name", "rebalance_date"]
    ].copy()
    if calendar_keys.empty:
        return pd.DataFrame()
    calendar_index = pd.MultiIndex.from_frame(
        calendar_keys.assign(calendar_name=calendar_keys["calendar_name"].astype(str))
    )
    budget_keys = budget[["calendar_name", "rebalance_date"]].copy()
    budget_index = pd.MultiIndex.from_frame(
        budget_keys.assign(calendar_name=budget_keys["calendar_name"].astype(str))
    )
    return budget[budget_index.isin(calendar_index)].copy()


def _latest_previous_target_by_key(
    previous_target_weight: pd.DataFrame | None,
) -> dict[tuple[str, str, str], dict[str, float]]:
    if previous_target_weight is None or previous_target_weight.empty:
        return {}
    if not _ETF_AW_TARGET_WEIGHT_PREVIOUS_COLUMNS.issubset(
        previous_target_weight.columns
    ):
        return {}
    previous = _normalize_rebalance_date_frame(previous_target_weight)
    previous = previous[
        previous["strategy_name"].astype(str).eq(_ETF_AW_STRATEGY_NAME)
        & previous["strategy_version"]
        .astype(str)
        .eq(_ETF_AW_TARGET_WEIGHT_STRATEGY_VERSION)
    ].copy()
    if previous.empty:
        return {}
    previous = previous.sort_values(["rebalance_date", "ingested_at"])
    latest_date_by_calendar = previous.groupby("calendar_name")["rebalance_date"].max()
    result: dict[tuple[str, str, str], dict[str, float]] = {}
    for calendar_name, latest_date in latest_date_by_calendar.items():
        group = previous[
            (previous["calendar_name"].astype(str) == str(calendar_name))
            & (previous["rebalance_date"] == latest_date)
        ].copy()
        group = group.drop_duplicates("sleeve_code", keep="last")
        if set(group["sleeve_code"].astype(str)) != set(_ETF_AW_SLEEVE_CODES):
            continue
        result[
            (
                str(calendar_name),
                _ETF_AW_STRATEGY_NAME,
                _ETF_AW_TARGET_WEIGHT_STRATEGY_VERSION,
            )
        ] = {
            str(row["sleeve_code"]): float(row["target_weight"])
            for _, row in group.iterrows()
        }
    return result


def _target_weight_panel_returns(panel: pd.DataFrame) -> pd.DataFrame:
    panel = panel.copy()
    panel["trade_date"] = pd.to_datetime(panel["trade_date"], errors="coerce").dt.date
    panel = panel[panel["sleeve_code"].astype(str).isin(_ETF_AW_SLEEVE_CODES)].copy()
    panel = panel.sort_values(["sleeve_code", "trade_date"]).reset_index(drop=True)
    if "daily_return" in panel.columns:
        panel["daily_return"] = panel["daily_return"].apply(_nullable_float)
    else:
        panel["adj_close"] = panel["adj_close"].apply(_nullable_float)
        panel["daily_return"] = panel.groupby("sleeve_code")["adj_close"].pct_change()
    return panel


def _target_weight_rows_for_rebalance(
    *,
    key: tuple[str, date, str, str],
    budget_group: pd.DataFrame,
    panel: pd.DataFrame,
    previous_target: dict[str, float] | None,
    ingested_at: datetime,
) -> list[dict[str, Any]]:
    calendar_name, rebalance_date, _, _ = key
    if not _target_weight_budget_group_valid(budget_group, rebalance_date):
        return []
    vol_by_role, source_max_by_role, reasons_by_role = _target_weight_volatility(
        panel, rebalance_date
    )
    insufficient_roles = [
        role
        for role, reasons in reasons_by_role.items()
        if "insufficient_volatility_observations" in reasons
    ]
    if len(insufficient_roles) > 1:
        return []

    risk_budget = {
        str(row["sleeve_role"]): float(row["tilted_budget"])
        for _, row in budget_group.iterrows()
    }
    raw_weights = _budgeted_inverse_vol_weights(risk_budget, vol_by_role)
    constrained = _apply_target_weight_caps(raw_weights)
    if not constrained:
        return []
    target, no_trade_band_drift = _apply_no_trade_band(constrained, previous_target)
    raw_rounded = _round_role_weights(raw_weights)
    constrained_rounded = _round_role_weights(constrained)
    target_rounded = _round_code_weights(target)
    turnover = (
        None
        if previous_target is None
        else round(
            0.5
            * sum(
                abs(
                    target[_role_code(role)]
                    - previous_target.get(_role_code(role), 0.0)
                )
                for role in ETF_AW_SLEEVE_ROLE_ORDER
            ),
            6,
        )
    )
    rows: list[dict[str, Any]] = []
    for _, row in budget_group.iterrows():
        role = str(row["sleeve_role"])
        code = _role_code(role)
        reasons = list(reasons_by_role.get(role, []))
        if previous_target is None:
            reasons.append("first_period_turnover_not_observable")
        status = "partial" if role in insufficient_roles else str(row["budget_status"])
        if status not in _ETF_AW_TARGET_WEIGHT_STATUSES:
            status = "partial"
        rows.append(
            {
                "schema_version": _ETF_AW_TARGET_WEIGHT_SCHEMA_VERSION,
                "contract_version": _ETF_AW_TARGET_WEIGHT_CONTRACT_VERSION,
                "calendar_name": str(calendar_name),
                "rebalance_date": rebalance_date,
                "effective_date": rebalance_date,
                "strategy_name": _ETF_AW_STRATEGY_NAME,
                "strategy_version": _ETF_AW_TARGET_WEIGHT_STRATEGY_VERSION,
                "sleeve_code": code,
                "sleeve_role": role,
                "risk_budget": round(risk_budget[role], 6),
                "volatility_estimate": round(vol_by_role[role], 6),
                "volatility_floor": _ETF_AW_TARGET_WEIGHT_VOL_FLOOR,
                "raw_target_weight": raw_rounded[role],
                "constrained_target_weight": constrained_rounded[role],
                "target_weight": target_rounded[code],
                "target_weight_status": status,
                "optimizer_name": "budgeted_inverse_vol",
                "optimizer_basis": (
                    "risk_budget divided by trailing adjusted-close volatility "
                    "with sleeve caps and no-trade band"
                ),
                "turnover_estimate": turnover,
                "quality_notes_json": json.dumps(
                    {
                        "reasons": sorted(set(reasons)),
                        "volatility_window": _ETF_AW_TARGET_WEIGHT_VOL_WINDOW,
                        "minimum_observations": _ETF_AW_TARGET_WEIGHT_MIN_OBSERVATIONS,
                        "no_trade_band": _ETF_AW_TARGET_WEIGHT_NO_TRADE_BAND,
                        "no_trade_band_normalization_drift": round(
                            no_trade_band_drift,
                            12,
                        ),
                        "sleeve_cap": _ETF_AW_TARGET_WEIGHT_CAPS[role],
                        "source_budget_status": str(row["budget_status"]),
                    },
                    sort_keys=True,
                ),
                "source_risk_budget_rebalance_date": rebalance_date,
                "source_sleeve_daily_max_trade_date": source_max_by_role.get(role),
                "ingested_at": ingested_at,
            }
        )
    return rows


def _target_weight_budget_group_valid(
    budget_group: pd.DataFrame, rebalance_date: date
) -> bool:
    if len(budget_group) != len(ETF_AW_SLEEVE_ROLE_ORDER):
        return False
    if set(budget_group["sleeve_role"].astype(str)) != ETF_AW_REQUIRED_ROLES:
        return False
    if any(str(value).upper() == "FAIL" for value in budget_group.get("finding", [])):
        return False
    budget_sum = budget_group["tilted_budget"].astype(float).sum()
    if abs(budget_sum - 1.0) > 1e-6:
        return False
    source_context_dates = pd.to_datetime(
        budget_group["source_strategy_context_rebalance_date"], errors="coerce"
    ).dt.date
    source_regime_dates = pd.to_datetime(
        budget_group["source_regime_rebalance_date"], errors="coerce"
    ).dt.date
    return all(
        (pd.isna(context_date) or context_date <= rebalance_date)
        and (pd.isna(regime_date) or regime_date <= rebalance_date)
        for context_date, regime_date in zip(
            source_context_dates, source_regime_dates, strict=True
        )
    )


def _target_weight_volatility(
    panel: pd.DataFrame,
    rebalance_date: date,
    *,
    window_days: int = _ETF_AW_TARGET_WEIGHT_VOL_WINDOW,
    min_observations: int = _ETF_AW_TARGET_WEIGHT_MIN_OBSERVATIONS,
) -> tuple[dict[str, float], dict[str, date | None], dict[str, list[str]]]:
    vol_by_role: dict[str, float] = {}
    source_max_by_role: dict[str, date | None] = {}
    reasons_by_role: dict[str, list[str]] = {}
    eligible = panel[panel["trade_date"] <= rebalance_date].copy()
    if not eligible.empty:
        eligible["sleeve_code"] = eligible["sleeve_code"].astype(str)
    panel_by_code = {
        str(code): group for code, group in eligible.groupby("sleeve_code", sort=False)
    }
    for role in ETF_AW_SLEEVE_ROLE_ORDER:
        code = _role_code(role)
        sleeve_panel = panel_by_code.get(code, pd.DataFrame())
        source_max = (
            max(sleeve_panel["trade_date"].dropna().tolist())
            if not sleeve_panel.empty
            else None
        )
        returns = (
            sleeve_panel["daily_return"].dropna().tail(window_days)
            if "daily_return" in sleeve_panel.columns
            else pd.Series(dtype=float)
        )
        reasons: list[str] = []
        if len(returns) < min_observations:
            volatility = _ETF_AW_TARGET_WEIGHT_VOL_FLOOR
            reasons.extend(
                [
                    "insufficient_volatility_observations",
                    "volatility_floor_applied",
                ]
            )
        else:
            estimated = float(returns.std(ddof=1))
            volatility = max(estimated, _ETF_AW_TARGET_WEIGHT_VOL_FLOOR)
            if volatility == _ETF_AW_TARGET_WEIGHT_VOL_FLOOR:
                reasons.append("volatility_floor_applied")
        vol_by_role[role] = volatility
        source_max_by_role[role] = source_max
        reasons_by_role[role] = reasons
    return vol_by_role, source_max_by_role, reasons_by_role


def _budgeted_inverse_vol_weights(
    risk_budget: dict[str, float], vol_by_role: dict[str, float]
) -> dict[str, float]:
    scores = {
        role: (
            risk_budget[role] / max(vol_by_role[role], _ETF_AW_TARGET_WEIGHT_VOL_FLOOR)
        )
        for role in ETF_AW_SLEEVE_ROLE_ORDER
    }
    total = sum(scores.values())
    return {role: scores[role] / total for role in ETF_AW_SLEEVE_ROLE_ORDER}


def _apply_target_weight_caps(weights: dict[str, float]) -> dict[str, float]:
    capped = {
        role: min(weights[role], _ETF_AW_TARGET_WEIGHT_CAPS[role]) for role in weights
    }
    while True:
        excess = 1.0 - sum(capped.values())
        if excess <= 1e-12:
            break
        open_roles = [
            role
            for role in ETF_AW_SLEEVE_ROLE_ORDER
            if capped[role] < _ETF_AW_TARGET_WEIGHT_CAPS[role] - 1e-12
        ]
        if not open_roles:
            return {}
        open_total = sum(capped[role] for role in open_roles)
        if open_total <= 0.0:
            return {}
        changed = False
        for role in open_roles:
            addition = excess * capped[role] / open_total
            next_weight = min(capped[role] + addition, _ETF_AW_TARGET_WEIGHT_CAPS[role])
            changed = changed or abs(next_weight - capped[role]) > 1e-12
            capped[role] = next_weight
        if not changed:
            return {}
    total = sum(capped.values())
    if total <= 0.0:
        return {}
    return {role: capped[role] / total for role in ETF_AW_SLEEVE_ROLE_ORDER}


def _apply_no_trade_band(
    constrained: dict[str, float], previous_target: dict[str, float] | None
) -> tuple[dict[str, float], float]:
    if previous_target is None:
        return (
            {_role_code(role): constrained[role] for role in ETF_AW_SLEEVE_ROLE_ORDER},
            0.0,
        )
    target = {}
    for role in ETF_AW_SLEEVE_ROLE_ORDER:
        code = _role_code(role)
        previous = previous_target.get(code)
        if previous is not None and abs(constrained[role] - previous) < (
            _ETF_AW_TARGET_WEIGHT_NO_TRADE_BAND
        ):
            target[code] = previous
        else:
            target[code] = constrained[role]
    total = sum(target.values())
    normalized = {code: value / total for code, value in target.items()}
    recapped = _apply_target_weight_caps(
        {role: normalized[_role_code(role)] for role in ETF_AW_SLEEVE_ROLE_ORDER}
    )
    final_target = {
        _role_code(role): recapped[role] for role in ETF_AW_SLEEVE_ROLE_ORDER
    }
    drift = sum(abs(final_target[code] - target[code]) for code in target)
    return final_target, drift


def _round_role_weights(weights: dict[str, float]) -> dict[str, float]:
    rounded = {role: round(weights[role], 6) for role in ETF_AW_SLEEVE_ROLE_ORDER}
    drift = round(1.0 - sum(rounded.values()), 6)
    rounded[ETF_AW_SLEEVE_ROLE_ORDER[-1]] = round(
        rounded[ETF_AW_SLEEVE_ROLE_ORDER[-1]] + drift,
        6,
    )
    return rounded


def _round_code_weights(weights: dict[str, float]) -> dict[str, float]:
    rounded = {
        _role_code(role): round(weights[_role_code(role)], 6)
        for role in ETF_AW_SLEEVE_ROLE_ORDER
    }
    last_code = _role_code(ETF_AW_SLEEVE_ROLE_ORDER[-1])
    drift = round(1.0 - sum(rounded.values()), 6)
    rounded[last_code] = round(rounded[last_code] + drift, 6)
    return rounded


def _role_code(role: str) -> str:
    return ETF_AW_SLEEVE_CODE_BY_ROLE[role]


def _normalize_backtest_weight_source_type(value: str) -> str | None:
    """Return the canonical backtest weight source type when supported."""

    text = str(value)
    text = _ETF_AW_BACKTEST_WEIGHT_SOURCE_ALIASES.get(text, text)
    if text in _ETF_AW_BACKTEST_WEIGHT_SOURCES:
        return text
    return None


def _make_etf_aw_baseline_weight_frame(
    *, rebalance: pd.DataFrame, panel: pd.DataFrame
) -> pd.DataFrame:
    """Build static inverse-vol baseline weights from frozen sleeve returns."""

    if rebalance.empty or panel.empty:
        return pd.DataFrame()
    required_panel = {"sleeve_code", "trade_date"}
    if not required_panel.issubset(panel.columns):
        return pd.DataFrame()
    if "daily_return" not in panel.columns and "adj_close" not in panel.columns:
        return pd.DataFrame()
    calendar = _normalize_rebalance_date_frame(rebalance)
    calendar = calendar.dropna(subset=["calendar_name", "rebalance_date"])
    if calendar.empty:
        return pd.DataFrame()
    calendar = calendar.sort_values(["calendar_name", "rebalance_date"])
    calendar = calendar.drop_duplicates(["calendar_name", "rebalance_date"])
    panel = _target_weight_panel_returns(panel)
    rows: list[dict[str, Any]] = []
    ingested_at = _utc_now()
    for _, calendar_row in calendar.iterrows():
        rebalance_date = calendar_row["rebalance_date"]
        vol_by_role, source_max_by_role, reasons_by_role = _target_weight_volatility(
            panel,
            rebalance_date,
            window_days=_ETF_AW_BASELINE_VOL_WINDOW,
            min_observations=_ETF_AW_BASELINE_MIN_OBSERVATIONS,
        )
        if any(
            "insufficient_volatility_observations" in reasons
            for reasons in reasons_by_role.values()
        ):
            continue
        weights = _static_inverse_vol_weights(vol_by_role)
        rounded = _round_role_weights(weights)
        for role in ETF_AW_SLEEVE_ROLE_ORDER:
            code = _role_code(role)
            rows.append(
                {
                    "schema_version": _ETF_AW_BASELINE_WEIGHT_SCHEMA_VERSION,
                    "contract_version": _ETF_AW_BASELINE_WEIGHT_CONTRACT_VERSION,
                    "calendar_name": str(calendar_row["calendar_name"]),
                    "rebalance_date": rebalance_date,
                    "effective_date": rebalance_date,
                    "baseline_name": _ETF_AW_BASELINE_NAME,
                    "baseline_version": _ETF_AW_BASELINE_VERSION,
                    "sleeve_code": code,
                    "sleeve_role": role,
                    "target_weight": rounded[role],
                    "estimation_window_days": _ETF_AW_BASELINE_VOL_WINDOW,
                    "min_observation_days": _ETF_AW_BASELINE_MIN_OBSERVATIONS,
                    "volatility_estimate": round(vol_by_role[role], 6),
                    "optimizer_name": _ETF_AW_BASELINE_NAME,
                    "optimizer_basis": (
                        "one divided by trailing adjusted-close volatility, "
                        "normalized across frozen sleeves"
                    ),
                    "quality_notes_json": json.dumps(
                        {
                            "reasons": sorted(set(reasons_by_role.get(role, []))),
                            "volatility_floor": _ETF_AW_TARGET_WEIGHT_VOL_FLOOR,
                            "volatility_window": _ETF_AW_BASELINE_VOL_WINDOW,
                            "minimum_observations": _ETF_AW_BASELINE_MIN_OBSERVATIONS,
                        },
                        sort_keys=True,
                    ),
                    "source_sleeve_daily_max_trade_date": source_max_by_role.get(role),
                    "ingested_at": ingested_at,
                }
            )
    return pd.DataFrame(rows)


def _static_inverse_vol_weights(vol_by_role: dict[str, float]) -> dict[str, float]:
    risk_budget = {role: 1.0 for role in ETF_AW_SLEEVE_ROLE_ORDER}
    return _budgeted_inverse_vol_weights(risk_budget, vol_by_role)


def _validate_baseline_weight_frame(frame: pd.DataFrame) -> dict[str, bool]:
    """Validate the static baseline weight artifact contract."""

    if frame.empty:
        return {
            "non_empty": False,
            "missing_required_columns": False,
            "no_duplicate_business_keys": False,
            "five_roles_per_rebalance_date": False,
            "weight_sums_valid": False,
            "quality_notes_json": False,
            "forbidden_fields_absent": False,
        }
    required_columns = {
        "schema_version",
        "contract_version",
        "calendar_name",
        "rebalance_date",
        "effective_date",
        "baseline_name",
        "baseline_version",
        "sleeve_code",
        "sleeve_role",
        "target_weight",
        "estimation_window_days",
        "min_observation_days",
        "volatility_estimate",
        "optimizer_name",
        "optimizer_basis",
        "quality_notes_json",
        "source_sleeve_daily_max_trade_date",
        "ingested_at",
    }
    if not required_columns.issubset(frame.columns):
        return {
            "non_empty": True,
            "missing_required_columns": False,
            "no_duplicate_business_keys": False,
            "five_roles_per_rebalance_date": False,
            "weight_sums_valid": False,
            "quality_notes_json": False,
            "forbidden_fields_absent": False,
        }
    key_columns = [
        "calendar_name",
        "rebalance_date",
        "baseline_name",
        "baseline_version",
        "sleeve_code",
    ]
    group_columns = [
        "calendar_name",
        "rebalance_date",
        "baseline_name",
        "baseline_version",
    ]
    group_checks = []
    weight_checks = []
    for _, group in frame.groupby(group_columns):
        group_checks.append(
            len(group) == len(ETF_AW_SLEEVE_ROLE_ORDER)
            and set(group["sleeve_role"].astype(str)) == ETF_AW_REQUIRED_ROLES
            and set(group["sleeve_code"].astype(str)) == set(_ETF_AW_SLEEVE_CODES)
        )
        weights = [_nullable_float(value) for value in group["target_weight"].tolist()]
        weight_checks.append(
            all(value is not None and value >= 0.0 for value in weights)
            and abs(sum(value or 0.0 for value in weights) - 1.0) <= 1e-6
        )
    forbidden_fields = {
        column
        for column in frame.columns
        if column
        in {
            "trade_action",
            "order_instruction",
            "rebalance_instruction",
            "order_quantity",
            "order_amount",
            "broker_account",
        }
    }
    return {
        "non_empty": True,
        "missing_required_columns": True,
        "no_duplicate_business_keys": int(frame.duplicated(key_columns).sum()) == 0,
        "five_roles_per_rebalance_date": all(group_checks),
        "weight_sums_valid": all(weight_checks),
        "quality_notes_json": all(
            _is_json_text(value) for value in frame["quality_notes_json"].tolist()
        ),
        "forbidden_fields_absent": not forbidden_fields,
    }


def _validate_target_weight_frame(frame: pd.DataFrame) -> dict[str, bool]:
    """Validate the ETF all-weather target weight contract."""

    if frame.empty:
        return {
            "non_empty": False,
            "missing_required_columns": False,
            "no_duplicate_business_keys": False,
            "five_roles_per_rebalance_date": False,
            "weight_sums_valid": False,
            "status_values_allowed": False,
            "quality_notes_json": False,
            "forbidden_fields_absent": False,
            "point_in_time_sources": False,
            "caps_respected": False,
        }
    required_columns = {
        "calendar_name",
        "rebalance_date",
        "strategy_name",
        "strategy_version",
        "sleeve_code",
        "sleeve_role",
        "risk_budget",
        "raw_target_weight",
        "constrained_target_weight",
        "target_weight",
        "target_weight_status",
        "quality_notes_json",
        "source_risk_budget_rebalance_date",
    }
    if not required_columns.issubset(frame.columns):
        return {
            "non_empty": True,
            "missing_required_columns": False,
            "no_duplicate_business_keys": False,
            "five_roles_per_rebalance_date": False,
            "weight_sums_valid": False,
            "status_values_allowed": False,
            "quality_notes_json": False,
            "forbidden_fields_absent": False,
            "point_in_time_sources": False,
            "caps_respected": False,
        }
    key_columns = [
        "calendar_name",
        "rebalance_date",
        "strategy_name",
        "strategy_version",
        "sleeve_code",
    ]
    grouped = frame.groupby(
        ["calendar_name", "rebalance_date", "strategy_name", "strategy_version"],
        dropna=False,
    )
    forbidden_fields = {
        "trade_action",
        "order_instruction",
        "rebalance_instruction",
        "order_quantity",
        "order_amount",
        "broker_account",
    }.intersection(set(frame.columns))
    source_budget_dates = pd.to_datetime(
        frame["source_risk_budget_rebalance_date"], errors="coerce"
    ).dt.date
    rebalance_dates = pd.to_datetime(frame["rebalance_date"], errors="coerce").dt.date
    return {
        "non_empty": True,
        "no_duplicate_business_keys": int(frame.duplicated(key_columns).sum()) == 0,
        "five_roles_per_rebalance_date": all(
            set(group["sleeve_role"].astype(str)) == ETF_AW_REQUIRED_ROLES
            and len(group) == len(ETF_AW_REQUIRED_ROLES)
            for _, group in grouped
        ),
        "weight_sums_valid": all(
            abs(group["risk_budget"].astype(float).sum() - 1.0) <= 1e-6
            and abs(group["raw_target_weight"].astype(float).sum() - 1.0) <= 1e-6
            and abs(group["constrained_target_weight"].astype(float).sum() - 1.0)
            <= 1e-6
            and abs(group["target_weight"].astype(float).sum() - 1.0) <= 1e-6
            and bool(
                (
                    group[
                        [
                            "risk_budget",
                            "raw_target_weight",
                            "constrained_target_weight",
                            "target_weight",
                        ]
                    ]
                    >= 0.0
                )
                .all()
                .all()
            )
            for _, group in grouped
        ),
        "status_values_allowed": (
            set(frame["target_weight_status"].astype(str)).issubset(
                _ETF_AW_TARGET_WEIGHT_STATUSES
            )
        ),
        "quality_notes_json": all(
            _is_json_text(value) for value in frame["quality_notes_json"]
        ),
        "forbidden_fields_absent": not forbidden_fields,
        "point_in_time_sources": all(
            pd.isna(source_date) or pd.isna(rebalance) or source_date <= rebalance
            for source_date, rebalance in zip(
                source_budget_dates, rebalance_dates, strict=True
            )
        ),
        "caps_respected": all(
            float(row["constrained_target_weight"])
            <= _ETF_AW_TARGET_WEIGHT_CAPS[str(row["sleeve_role"])] + 1e-6
            and float(row["target_weight"])
            <= _ETF_AW_TARGET_WEIGHT_CAPS[str(row["sleeve_role"])] + 1e-6
            for _, row in frame.iterrows()
        ),
    }


def _equal_weight_backtest_fixture(rebalance: pd.DataFrame) -> pd.DataFrame:
    """Return equal monthly weights for the frozen v1 ETF sleeves."""

    rebalance = _normalize_rebalance_date_frame(rebalance)
    rows: list[dict[str, Any]] = []
    weight = 1.0 / len(_ETF_AW_SLEEVE_CODES)
    for _, row in rebalance.dropna(subset=["rebalance_date"]).iterrows():
        for sleeve_code in _ETF_AW_SLEEVE_CODES:
            rows.append(
                {
                    "calendar_name": str(row["calendar_name"]),
                    "rebalance_date": row["rebalance_date"],
                    "sleeve_code": sleeve_code,
                    "target_weight": weight,
                }
            )
    return pd.DataFrame(rows)


def _make_etf_aw_backtest_kernel_frame(
    *,
    panel: pd.DataFrame,
    rebalance: pd.DataFrame,
    weights: pd.DataFrame,
    start: date,
    end: date,
) -> pd.DataFrame:
    """Build daily NAV, metric, and turnover rows for supplied monthly weights."""

    ingested_at = _utc_now()
    calendar_name = _backtest_kernel_calendar_name(rebalance, weights)
    strategy_name = _backtest_kernel_strategy_value(
        weights, "strategy_name", _ETF_AW_BACKTEST_KERNEL_STRATEGY_NAME
    )
    strategy_version = _backtest_kernel_strategy_value(
        weights, "strategy_version", _ETF_AW_BACKTEST_KERNEL_STRATEGY_VERSION
    )
    weight_source_type = _backtest_kernel_strategy_value(
        weights, "weight_source_type", "target_weight"
    )
    source_weight_dataset = _backtest_kernel_strategy_value(
        weights, "source_weight_dataset", _ETF_AW_TARGET_WEIGHT_DATASET
    )
    diagnostic_rebalance_dates = _backtest_diagnostic_rebalance_dates(
        rebalance, weights, start
    )
    diagnostics = _backtest_input_diagnostics(panel, rebalance, weights)
    if diagnostics["blocking"]:
        return _backtest_diagnostic_rows(
            diagnostics=diagnostics,
            calendar_name=calendar_name,
            strategy_name=strategy_name,
            strategy_version=strategy_version,
            rebalance_dates=diagnostic_rebalance_dates,
            weight_source_type=weight_source_type,
            source_weight_dataset=source_weight_dataset,
            ingested_at=ingested_at,
        )

    panel = panel.copy()
    panel["trade_date"] = pd.to_datetime(panel["trade_date"], errors="coerce").dt.date
    panel = panel[
        panel["trade_date"].between(start, end, inclusive="both")
        & panel["sleeve_code"].astype(str).isin(_ETF_AW_SLEEVE_CODES)
    ].copy()
    panel["daily_return"] = panel["adj_pct_chg"].apply(_nullable_float)
    panel["daily_return"] = panel["daily_return"].fillna(0.0) / 100.0
    returns = (
        panel.pivot_table(
            index="trade_date",
            columns="sleeve_code",
            values="daily_return",
            aggfunc="last",
        )
        .sort_index()
        .dropna(how="all")
    )
    if returns.empty:
        return _backtest_diagnostic_rows(
            diagnostics={"blocking": True, "reasons": ["no_daily_returns"]},
            calendar_name=calendar_name,
            strategy_name=strategy_name,
            strategy_version=strategy_version,
            rebalance_dates=diagnostic_rebalance_dates,
            weight_source_type=weight_source_type,
            source_weight_dataset=source_weight_dataset,
            ingested_at=ingested_at,
        )

    weights = weights.copy()
    weights["rebalance_date"] = pd.to_datetime(
        weights["rebalance_date"], errors="coerce"
    ).dt.date
    rows: list[dict[str, Any]] = []
    nav = 1.0
    previous_target: dict[str, float] | None = None
    monthly_returns: list[float] = []
    return_values: list[float] = []

    weight_by_date = {
        key: group.set_index("sleeve_code")["target_weight"].astype(float).to_dict()
        for key, group in weights.groupby("rebalance_date")
    }
    effective_dates = sorted(weight_by_date)
    effective_index = -1
    current_weight: dict[str, float] | None = None
    current_effective_date: date | None = None
    last_month: tuple[int, int] | None = None
    month_start_nav = nav

    for trade_date, row in returns.iterrows():
        month_key = (trade_date.year, trade_date.month)
        if last_month is None:
            last_month = month_key
            month_start_nav = nav
        elif month_key != last_month:
            monthly_returns.append(nav / month_start_nav - 1.0)
            month_start_nav = nav
            last_month = month_key
        while (
            effective_index + 1 < len(effective_dates)
            and effective_dates[effective_index + 1] <= trade_date
        ):
            effective_index += 1
        if effective_index < 0:
            continue
        effective_date = effective_dates[effective_index]
        next_weight = weight_by_date[effective_date]
        if current_effective_date != effective_date:
            turnover = (
                0.0
                if previous_target is None
                else 0.5
                * sum(
                    abs(next_weight.get(code, 0.0) - previous_target.get(code, 0.0))
                    for code in _ETF_AW_SLEEVE_CODES
                )
            )
            rows.append(
                _backtest_row(
                    calendar_name=calendar_name,
                    strategy_name=strategy_name,
                    strategy_version=strategy_version,
                    weight_source_type=weight_source_type,
                    source_weight_dataset=source_weight_dataset,
                    observation_type="turnover",
                    observation_date=trade_date,
                    metric_name="monthly_turnover",
                    metric_value=turnover,
                    net_value=None,
                    portfolio_return=None,
                    quality_notes={
                        "rebalance_date": effective_date.isoformat(),
                        "turnover_basis": "previous_target_weight",
                    },
                    ingested_at=ingested_at,
                )
            )
            current_weight = next_weight
            current_effective_date = effective_date
            previous_target = dict(next_weight)
        portfolio_return = float(
            sum(
                current_weight.get(code, 0.0) * float(row.get(code, 0.0))
                for code in _ETF_AW_SLEEVE_CODES
            )
        )
        nav *= 1.0 + portfolio_return
        return_values.append(portfolio_return)
        rows.append(
            _backtest_row(
                calendar_name=calendar_name,
                strategy_name=strategy_name,
                strategy_version=strategy_version,
                weight_source_type=weight_source_type,
                source_weight_dataset=source_weight_dataset,
                observation_type="daily_nav",
                observation_date=trade_date,
                metric_name="net_value",
                metric_value=nav,
                net_value=nav,
                portfolio_return=portfolio_return,
                quality_notes={"effective_rebalance_date": effective_date.isoformat()},
                ingested_at=ingested_at,
            )
        )
    if last_month is not None:
        monthly_returns.append(nav / month_start_nav - 1.0)

    metric_values = _backtest_metric_values(return_values, monthly_returns, nav)
    metric_date = max(returns.index)
    for metric_name, metric_value in metric_values.items():
        rows.append(
            _backtest_row(
                calendar_name=calendar_name,
                strategy_name=strategy_name,
                strategy_version=strategy_version,
                weight_source_type=weight_source_type,
                source_weight_dataset=source_weight_dataset,
                observation_type="metric",
                observation_date=metric_date,
                metric_name=metric_name,
                metric_value=metric_value,
                net_value=None,
                portfolio_return=None,
                quality_notes=diagnostics,
                ingested_at=ingested_at,
            )
        )
    return pd.DataFrame(rows)


def _backtest_input_diagnostics(
    panel: pd.DataFrame, rebalance: pd.DataFrame, weights: pd.DataFrame
) -> dict[str, Any]:
    """Return blocking diagnostics for the backtest kernel inputs."""

    reasons: list[str] = []
    if panel.empty:
        reasons.append("empty_panel")
    if rebalance.empty:
        reasons.append("empty_rebalance_calendar")
    if weights.empty:
        reasons.append("empty_weights")
    if not panel.empty and {"trade_date", "sleeve_code"}.issubset(panel.columns):
        normalized_panel = panel.copy()
        normalized_panel["trade_date"] = pd.to_datetime(
            normalized_panel["trade_date"], errors="coerce"
        ).dt.date
        panel_codes_by_date = normalized_panel.groupby("trade_date")["sleeve_code"].agg(
            lambda values: set(str(value) for value in values)
        )
        for codes in panel_codes_by_date.tolist():
            if codes != set(_ETF_AW_SLEEVE_CODES):
                reasons.append("missing_sleeve_return")
                break
        if not rebalance.empty and "rebalance_date" in rebalance.columns:
            rebalance_dates = set(
                pd.to_datetime(rebalance["rebalance_date"], errors="coerce")
                .dt.date.dropna()
                .tolist()
            )
            trade_dates = set(panel_codes_by_date.index.dropna().tolist())
            if any(value not in trade_dates for value in rebalance_dates):
                reasons.append("rebalance_date_without_trading_day")
    required_weight_columns = {
        "calendar_name",
        "rebalance_date",
        "sleeve_code",
        "target_weight",
    }
    missing_columns = sorted(required_weight_columns - set(weights.columns))
    if missing_columns:
        reasons.append("missing_weight_columns")
    if not missing_columns and not weights.empty:
        normalized = weights.copy()
        normalized["rebalance_date"] = pd.to_datetime(
            normalized["rebalance_date"], errors="coerce"
        ).dt.date
        duplicates = int(
            normalized.duplicated(
                ["calendar_name", "rebalance_date", "sleeve_code"]
            ).sum()
        )
        if duplicates:
            reasons.append("duplicate_weight_rows")
        for _, group in normalized.groupby(["calendar_name", "rebalance_date"]):
            codes = set(group["sleeve_code"].astype(str).tolist())
            if codes != set(_ETF_AW_SLEEVE_CODES):
                reasons.append("missing_sleeve_weight")
                break
            weight_sum = group["target_weight"].astype(float).sum()
            if abs(weight_sum - 1.0) > 1e-6:
                reasons.append("weight_sum_not_one")
                break
    return {
        "blocking": bool(reasons),
        "reasons": sorted(set(reasons)),
    }


def _backtest_kernel_calendar_name(
    rebalance: pd.DataFrame, weights: pd.DataFrame
) -> str:
    """Return the calendar name visible to backtest kernel diagnostics."""

    if (
        "calendar_name" in weights.columns
        and not weights["calendar_name"].dropna().empty
    ):
        return str(weights["calendar_name"].dropna().iloc[0])
    if (
        "calendar_name" in rebalance.columns
        and not rebalance["calendar_name"].dropna().empty
    ):
        return str(rebalance["calendar_name"].dropna().iloc[0])
    return _REBALANCE_CALENDAR_NAME


def _backtest_kernel_strategy_value(
    weights: pd.DataFrame, column: str, fallback: str
) -> str:
    """Return the strategy value carried by target weights when available."""

    if column in weights.columns and not weights[column].dropna().empty:
        return str(weights[column].dropna().iloc[0])
    return fallback


def _backtest_diagnostic_rebalance_dates(
    rebalance: pd.DataFrame, weights: pd.DataFrame, fallback: date
) -> list[date]:
    """Return rebalance-cycle dates for blocked backtest diagnostics."""

    dates: set[date] = set()
    for frame in (weights, rebalance):
        if "rebalance_date" not in frame.columns:
            continue
        values = pd.to_datetime(frame["rebalance_date"], errors="coerce").dt.date
        dates.update(value for value in values.dropna().tolist())
    return sorted(dates) or [fallback]


def _backtest_diagnostic_rows(
    *,
    diagnostics: dict[str, Any],
    calendar_name: str,
    strategy_name: str,
    strategy_version: str,
    rebalance_dates: list[date],
    ingested_at: datetime,
    weight_source_type: str = "target_weight",
    source_weight_dataset: str = _ETF_AW_TARGET_WEIGHT_DATASET,
) -> pd.DataFrame:
    """Return a validation-visible diagnostic row for blocked kernel inputs."""

    return pd.DataFrame(
        [
            _backtest_row(
                calendar_name=calendar_name,
                strategy_name=strategy_name,
                strategy_version=strategy_version,
                weight_source_type=weight_source_type,
                source_weight_dataset=source_weight_dataset,
                observation_type="diagnostic",
                observation_date=rebalance_date,
                metric_name="input_validation",
                metric_value=None,
                net_value=None,
                portfolio_return=None,
                quality_notes={
                    **diagnostics,
                    "rebalance_date": rebalance_date.isoformat(),
                },
                ingested_at=ingested_at,
            )
            for rebalance_date in rebalance_dates
        ]
    )


def _backtest_row(
    *,
    calendar_name: str,
    strategy_name: str,
    strategy_version: str,
    weight_source_type: str,
    source_weight_dataset: str,
    observation_type: str,
    observation_date: date,
    metric_name: str,
    metric_value: float | None,
    net_value: float | None,
    portfolio_return: float | None,
    quality_notes: dict[str, Any],
    ingested_at: datetime,
) -> dict[str, Any]:
    """Return one normalized backtest kernel observation row."""

    return {
        "schema_version": _ETF_AW_BACKTEST_KERNEL_SCHEMA_VERSION,
        "calendar_name": calendar_name,
        "strategy_name": strategy_name,
        "strategy_version": strategy_version,
        "weight_source_type": weight_source_type,
        "source_weight_dataset": source_weight_dataset,
        "observation_type": observation_type,
        "observation_date": observation_date,
        "metric_name": metric_name,
        "metric_value": metric_value,
        "net_value": net_value,
        "portfolio_return": portfolio_return,
        "quality_notes_json": json.dumps(quality_notes, sort_keys=True),
        "ingested_at": ingested_at,
    }


def _backtest_metric_values(
    daily_returns: list[float], monthly_returns: list[float], final_nav: float
) -> dict[str, float | None]:
    """Calculate the minimal deterministic backtest metric set."""

    if not daily_returns:
        return {
            "total_return": None,
            "annualized_return": None,
            "annualized_volatility": None,
            "sharpe_ratio": None,
            "max_drawdown": None,
            "monthly_periods": None,
        }
    series = pd.Series(daily_returns, dtype=float)
    nav = (1.0 + series).cumprod()
    annualized_return = final_nav ** (252.0 / len(series)) - 1.0
    annualized_volatility = float(series.std(ddof=1) * math.sqrt(252))
    sharpe_ratio = (
        float(annualized_return / annualized_volatility)
        if annualized_volatility > 0
        else None
    )
    drawdown = nav / nav.cummax() - 1.0
    return {
        "total_return": float(final_nav - 1.0),
        "annualized_return": float(annualized_return),
        "annualized_volatility": annualized_volatility,
        "sharpe_ratio": sharpe_ratio,
        "max_drawdown": float(drawdown.min()),
        "monthly_periods": float(len(monthly_returns)),
    }


def _make_etf_aw_monthly_explainability_frame(
    *,
    strategy_context: pd.DataFrame,
    risk_budget: pd.DataFrame,
    target_weight: pd.DataFrame,
    backtest_kernel: pd.DataFrame,
) -> pd.DataFrame:
    """Build one monthly explanation row from frozen ETF all-weather artifacts."""

    if (
        strategy_context.empty
        or risk_budget.empty
        or target_weight.empty
        or backtest_kernel.empty
    ):
        return pd.DataFrame()
    context = _latest_context_by_rebalance(strategy_context)
    budget = _latest_budget_groups_by_rebalance(risk_budget)
    weights = _latest_weight_groups_by_rebalance(target_weight)
    turnover = _backtest_turnover_by_rebalance(backtest_kernel)
    diagnostics = _backtest_diagnostics_by_rebalance(backtest_kernel)
    backtest_keys = set(turnover) | set(diagnostics)
    keys = sorted(set(context) & set(budget) & set(weights) & backtest_keys)
    rows: list[dict[str, Any]] = []
    ingested_at = _utc_now()
    for key in keys:
        context_row = context[key]
        budget_group = budget[key]
        weight_group = weights[key]
        rows.append(
            _monthly_explainability_row(
                key=key,
                context_row=context_row,
                budget_group=budget_group,
                weight_group=weight_group,
                backtest_turnover=turnover.get(key),
                backtest_diagnostics=diagnostics.get(key, []),
                ingested_at=ingested_at,
            )
        )
    return pd.DataFrame(rows)


def _latest_context_by_rebalance(
    frame: pd.DataFrame,
) -> dict[tuple[str, date], pd.Series]:
    required = {
        "calendar_name",
        "rebalance_date",
        "strategy_name",
        "strategy_version",
        "ingested_at",
    }
    if not required.issubset(frame.columns):
        return {}
    context = _normalize_rebalance_date_frame(frame)
    context = context.dropna(subset=["calendar_name", "rebalance_date"])
    context = context[
        context["strategy_name"].astype(str).eq(_ETF_AW_STRATEGY_NAME)
        & context["strategy_version"].astype(str).eq(_ETF_AW_STRATEGY_VERSION)
    ].copy()
    if context.empty:
        return {}
    context = context.sort_values(["rebalance_date", "ingested_at"])
    context = context.drop_duplicates(["calendar_name", "rebalance_date"], keep="last")
    return {
        (str(row["calendar_name"]), row["rebalance_date"]): row
        for _, row in context.iterrows()
    }


def _latest_budget_groups_by_rebalance(
    frame: pd.DataFrame,
) -> dict[tuple[str, date], pd.DataFrame]:
    required = {
        "calendar_name",
        "rebalance_date",
        "strategy_name",
        "strategy_version",
        "sleeve_role",
        "ingested_at",
    }
    if not required.issubset(frame.columns):
        return {}
    budget = _normalize_rebalance_date_frame(frame)
    budget = budget.dropna(subset=["calendar_name", "rebalance_date"])
    budget = budget[
        budget["strategy_name"].astype(str).eq(_ETF_AW_STRATEGY_NAME)
        & budget["strategy_version"]
        .astype(str)
        .eq(_ETF_AW_RISK_BUDGET_STRATEGY_VERSION)
    ].copy()
    if budget.empty:
        return {}
    budget = budget.sort_values(["rebalance_date", "ingested_at"])
    budget = budget.drop_duplicates(
        [
            "calendar_name",
            "rebalance_date",
            "strategy_name",
            "strategy_version",
            "sleeve_role",
        ],
        keep="last",
    )
    return {
        (str(key[0]), key[1]): group.copy()
        for key, group in budget.groupby(["calendar_name", "rebalance_date"])
    }


def _latest_weight_groups_by_rebalance(
    frame: pd.DataFrame,
) -> dict[tuple[str, date], pd.DataFrame]:
    required = {
        "calendar_name",
        "rebalance_date",
        "strategy_name",
        "strategy_version",
        "sleeve_code",
        "sleeve_role",
        "ingested_at",
    }
    if not required.issubset(frame.columns):
        return {}
    weights = _normalize_rebalance_date_frame(frame)
    weights = weights.dropna(subset=["calendar_name", "rebalance_date"])
    weights = weights[
        weights["strategy_name"].astype(str).eq(_ETF_AW_STRATEGY_NAME)
        & weights["strategy_version"]
        .astype(str)
        .eq(_ETF_AW_TARGET_WEIGHT_STRATEGY_VERSION)
    ].copy()
    if weights.empty:
        return {}
    weights = weights.sort_values(["rebalance_date", "ingested_at"])
    weights = weights.drop_duplicates(
        [
            "calendar_name",
            "rebalance_date",
            "strategy_name",
            "strategy_version",
            "sleeve_code",
        ],
        keep="last",
    )
    return {
        (str(key[0]), key[1]): group.copy()
        for key, group in weights.groupby(["calendar_name", "rebalance_date"])
    }


def _backtest_turnover_by_rebalance(
    frame: pd.DataFrame,
) -> dict[tuple[str, date], float | None]:
    required = {
        "calendar_name",
        "strategy_name",
        "strategy_version",
        "observation_type",
        "observation_date",
        "metric_name",
        "metric_value",
        "quality_notes_json",
        "ingested_at",
    }
    if not required.issubset(frame.columns):
        return {}
    kernel = frame[
        frame["strategy_name"].astype(str).eq(_ETF_AW_STRATEGY_NAME)
        & frame["strategy_version"]
        .astype(str)
        .eq(_ETF_AW_TARGET_WEIGHT_STRATEGY_VERSION)
    ].copy()
    turnover = kernel[
        kernel["observation_type"].astype(str).eq("turnover")
        & kernel["metric_name"].astype(str).eq("monthly_turnover")
    ].copy()
    turnover = turnover.sort_values(["observation_date", "ingested_at"])
    result: dict[tuple[str, date], float | None] = {}
    for _, row in turnover.iterrows():
        notes = _json_object_or_value(row.get("quality_notes_json"))
        if not isinstance(notes, dict) or "rebalance_date" not in notes:
            continue
        rebalance_date = _series_date(notes["rebalance_date"])
        if rebalance_date is None:
            continue
        result[(str(row["calendar_name"]), rebalance_date)] = _nullable_float(
            row.get("metric_value")
        )
    return result


def _backtest_diagnostics_by_rebalance(
    frame: pd.DataFrame,
) -> dict[tuple[str, date], list[dict[str, Any]]]:
    required = {
        "calendar_name",
        "strategy_name",
        "strategy_version",
        "observation_type",
        "observation_date",
        "metric_name",
        "quality_notes_json",
        "ingested_at",
    }
    if not required.issubset(frame.columns):
        return {}
    kernel = frame[
        frame["strategy_name"].astype(str).eq(_ETF_AW_STRATEGY_NAME)
        & frame["strategy_version"]
        .astype(str)
        .eq(_ETF_AW_TARGET_WEIGHT_STRATEGY_VERSION)
    ].copy()
    diagnostics = kernel[kernel["observation_type"].astype(str).eq("diagnostic")].copy()
    diagnostics = diagnostics.sort_values(
        ["calendar_name", "observation_date", "metric_name", "ingested_at"]
    )
    result: dict[tuple[str, date], list[dict[str, Any]]] = {}
    seen: set[tuple[str, date, str]] = set()
    for _, row in diagnostics.iterrows():
        notes = _json_object_or_value(row.get("quality_notes_json"))
        if not isinstance(notes, dict):
            continue
        rebalance_date = _series_date(
            notes.get("rebalance_date") or row.get("observation_date")
        )
        if rebalance_date is None:
            continue
        key = (str(row["calendar_name"]), rebalance_date)
        item_key = (key[0], key[1], str(row["metric_name"]))
        if item_key in seen:
            continue
        seen.add(item_key)
        result.setdefault(key, []).append(notes)
    return result


def _monthly_explainability_row(
    *,
    key: tuple[str, date],
    context_row: pd.Series,
    budget_group: pd.DataFrame,
    weight_group: pd.DataFrame,
    backtest_turnover: float | None,
    backtest_diagnostics: list[dict[str, Any]],
    ingested_at: datetime,
) -> dict[str, Any]:
    calendar_name, rebalance_date = key
    budget_status = _group_status(budget_group, "budget_status")
    weight_status = _group_status(weight_group, "target_weight_status")
    budget_notes = _merged_quality_notes(budget_group)
    weight_notes = _merged_quality_notes(weight_group)
    constraints = _target_weight_constraint_flags(weight_group)
    target_weights = _target_weight_explanation(weight_group)
    target_turnover = _first_non_null_float(weight_group.get("turnover_estimate", []))
    diagnostics = {
        "risk_budget_reasons": budget_notes["reasons"],
        "target_weight_reasons": weight_notes["reasons"],
        "backtest_diagnostics": backtest_diagnostics,
    }
    return {
        "schema_version": _ETF_AW_MONTHLY_EXPLAINABILITY_SCHEMA_VERSION,
        "calendar_name": calendar_name,
        "calendar_month": rebalance_date.strftime("%Y-%m"),
        "rebalance_date": rebalance_date,
        "strategy_name": _ETF_AW_STRATEGY_NAME,
        "strategy_version": _ETF_AW_TARGET_WEIGHT_STRATEGY_VERSION,
        "source_context_strategy_version": _optional_text(
            context_row.get("strategy_version")
        ),
        "source_risk_budget_strategy_version": _optional_text(
            budget_group.iloc[0].get("strategy_version")
        ),
        "source_target_weight_strategy_version": _optional_text(
            weight_group.iloc[0].get("strategy_version")
        ),
        "market_regime_label": _optional_text(context_row.get("market_regime_label")),
        "market_state_summary": _market_state_summary(context_row),
        "context_status": _optional_text(context_row.get("context_status")),
        "readiness_level": _optional_text(context_row.get("readiness_level")),
        "macro_rates_context_status": _optional_text(
            context_row.get("macro_rates_context_status")
        ),
        "macro_rates_missing": bool(
            _json_list_or_empty(context_row.get("missing_primary_fields_json"))
            or _json_list_or_empty(context_row.get("missing_confirmatory_fields_json"))
        ),
        "risk_budget_status": budget_status,
        "risk_budget_explanation_json": json.dumps(
            {
                "budget_basis": _optional_text(
                    budget_group.iloc[0].get("budget_basis")
                ),
                "reasons": budget_notes["reasons"],
                "source_status": budget_notes["source_status"],
                "budgets": _budget_explanation(budget_group),
            },
            sort_keys=True,
        ),
        "target_weight_status": weight_status,
        "target_weight_explanation_json": json.dumps(
            {
                "optimizer_name": _optional_text(
                    weight_group.iloc[0].get("optimizer_name")
                ),
                "optimizer_basis": _optional_text(
                    weight_group.iloc[0].get("optimizer_basis")
                ),
                "reasons": weight_notes["reasons"],
                "weights": target_weights,
            },
            sort_keys=True,
        ),
        "constraint_flags_json": json.dumps(constraints, sort_keys=True),
        "turnover_estimate": target_turnover,
        "backtest_turnover": backtest_turnover,
        "diagnostics_json": json.dumps(diagnostics, sort_keys=True),
        "source_strategy_context_rebalance_date": rebalance_date,
        "source_risk_budget_rebalance_date": _series_date(
            weight_group.iloc[0].get("source_risk_budget_rebalance_date")
        ),
        "source_backtest_observation_date": _latest_backtest_source_date(
            backtest_diagnostics, rebalance_date
        ),
        "ingested_at": ingested_at,
    }


def _group_status(frame: pd.DataFrame, column: str) -> str | None:
    if column not in frame.columns:
        return None
    statuses = set(frame[column].dropna().astype(str).tolist())
    if not statuses:
        return None
    if len(statuses) == 1:
        return next(iter(statuses))
    for status in ("unavailable", "missing", "stale", "partial", "complete"):
        if status in statuses:
            return status
    return sorted(statuses)[0]


def _merged_quality_notes(frame: pd.DataFrame) -> dict[str, Any]:
    reasons: set[str] = set()
    source_status: dict[str, Any] = {}
    if "quality_notes_json" not in frame.columns:
        return {"reasons": [], "source_status": source_status}
    for _, row in frame.iterrows():
        notes = _json_object_or_value(row.get("quality_notes_json"))
        if not isinstance(notes, dict):
            continue
        for reason in notes.get("reasons", []):
            reasons.add(str(reason))
        for key in (
            "source_context_status",
            "source_readiness_level",
            "source_regime_status",
            "source_budget_status",
        ):
            if key in notes:
                source_status[key] = notes[key]
    return {"reasons": sorted(reasons), "source_status": source_status}


def _target_weight_constraint_flags(frame: pd.DataFrame) -> dict[str, Any]:
    floor_roles: list[str] = []
    cap_roles: list[str] = []
    no_trade_roles: list[str] = []
    for _, row in frame.iterrows():
        role = str(row.get("sleeve_role"))
        notes = _json_object_or_value(row.get("quality_notes_json"))
        reasons = notes.get("reasons", []) if isinstance(notes, dict) else []
        volatility = _nullable_float(row.get("volatility_estimate"))
        floor = _nullable_float(row.get("volatility_floor"))
        raw = _nullable_float(row.get("raw_target_weight"))
        constrained = _nullable_float(row.get("constrained_target_weight"))
        target = _nullable_float(row.get("target_weight"))
        if "volatility_floor_applied" in reasons or (
            volatility is not None and floor is not None and volatility <= floor
        ):
            floor_roles.append(role)
        if (
            raw is not None
            and constrained is not None
            and abs(raw - constrained) > 1e-6
        ):
            cap_roles.append(role)
        if (
            constrained is not None
            and target is not None
            and abs(constrained - target) > 1e-6
        ):
            no_trade_roles.append(role)
    return {
        "vol_floor_triggered": bool(floor_roles),
        "vol_floor_roles": _sort_sleeve_roles(floor_roles),
        "cap_triggered": bool(cap_roles),
        "cap_roles": _sort_sleeve_roles(cap_roles),
        "no_trade_band_triggered": bool(no_trade_roles),
        "no_trade_band_roles": _sort_sleeve_roles(no_trade_roles),
    }


def _sort_sleeve_roles(roles: Iterable[str]) -> list[str]:
    frame = pd.DataFrame({"sleeve_role": sorted(set(roles))})
    if frame.empty:
        return []
    return frame.sort_values("sleeve_role", key=etf_aw_role_sort_key)[
        "sleeve_role"
    ].tolist()


def _budget_explanation(frame: pd.DataFrame) -> list[dict[str, Any]]:
    ordered = frame.sort_values("sleeve_role", key=etf_aw_role_sort_key)
    return [
        {
            "sleeve_role": str(row["sleeve_role"]),
            "base_budget": _nullable_float(row.get("base_budget")),
            "delta_budget": _nullable_float(row.get("delta_budget")),
            "risk_budget": _nullable_float(row.get("tilted_budget")),
        }
        for _, row in ordered.iterrows()
    ]


def _target_weight_explanation(frame: pd.DataFrame) -> list[dict[str, Any]]:
    ordered = frame.sort_values("sleeve_role", key=etf_aw_role_sort_key)
    return [
        {
            "sleeve_code": str(row["sleeve_code"]),
            "sleeve_role": str(row["sleeve_role"]),
            "risk_budget": _nullable_float(row.get("risk_budget")),
            "volatility_estimate": _nullable_float(row.get("volatility_estimate")),
            "raw_target_weight": _nullable_float(row.get("raw_target_weight")),
            "constrained_target_weight": _nullable_float(
                row.get("constrained_target_weight")
            ),
            "target_weight": _nullable_float(row.get("target_weight")),
            "target_weight_status": _optional_text(row.get("target_weight_status")),
        }
        for _, row in ordered.iterrows()
    ]


def _market_state_summary(row: pd.Series) -> str:
    label = _optional_text(row.get("market_regime_label")) or "unknown"
    score = _nullable_float(row.get("market_score"))
    confidence = _nullable_float(row.get("market_confidence_score"))
    parts = [f"regime={label}"]
    if score is not None:
        parts.append(f"score={score:.2f}")
    if confidence is not None:
        parts.append(f"confidence={confidence:.2f}")
    return ", ".join(parts)


def _first_non_null_float(values: Iterable[object]) -> float | None:
    for value in values:
        parsed = _nullable_float(value)
        if parsed is not None:
            return parsed
    return None


def _latest_backtest_source_date(
    diagnostics: list[dict[str, Any]], rebalance_date: date
) -> date:
    for item in diagnostics:
        parsed = _series_date(item.get("observation_date"))
        if parsed is not None:
            return parsed
    return rebalance_date


def _validate_monthly_explainability_frame(frame: pd.DataFrame) -> dict[str, bool]:
    """Validate the ETF all-weather monthly explainability output contract."""

    if frame.empty:
        return {
            "non_empty": False,
            "missing_required_columns": False,
            "no_duplicate_business_keys": False,
            "json_fields_valid": False,
            "forbidden_fields_absent": False,
        }
    required_columns = {
        "schema_version",
        "calendar_name",
        "calendar_month",
        "rebalance_date",
        "strategy_name",
        "strategy_version",
        "source_context_strategy_version",
        "source_risk_budget_strategy_version",
        "source_target_weight_strategy_version",
        "market_state_summary",
        "macro_rates_missing",
        "risk_budget_status",
        "risk_budget_explanation_json",
        "target_weight_status",
        "target_weight_explanation_json",
        "constraint_flags_json",
        "turnover_estimate",
        "backtest_turnover",
        "diagnostics_json",
    }
    if not required_columns.issubset(frame.columns):
        return {
            "non_empty": True,
            "missing_required_columns": False,
            "no_duplicate_business_keys": False,
            "json_fields_valid": False,
            "forbidden_fields_absent": False,
        }
    key_columns = [
        "calendar_name",
        "rebalance_date",
        "strategy_name",
        "strategy_version",
    ]
    forbidden_fields = {
        column
        for column in frame.columns
        if column
        in {
            "trade_action",
            "order_instruction",
            "rebalance_instruction",
            "order_quantity",
            "rebalance_plan",
        }
    }
    json_columns = [
        "risk_budget_explanation_json",
        "target_weight_explanation_json",
        "constraint_flags_json",
        "diagnostics_json",
    ]
    return {
        "non_empty": True,
        "missing_required_columns": True,
        "no_duplicate_business_keys": int(frame.duplicated(key_columns).sum()) == 0,
        "json_fields_valid": all(
            _is_json_text(value)
            for column in json_columns
            for value in frame[column].tolist()
        ),
        "forbidden_fields_absent": not forbidden_fields,
    }


def _validate_backtest_kernel_frame(frame: pd.DataFrame) -> dict[str, bool]:
    """Validate the minimal backtest kernel output contract."""

    allowed_types = {"daily_nav", "metric", "turnover", "diagnostic"}
    if frame.empty:
        return {
            "non_empty": False,
            "missing_required_columns": False,
            "no_duplicate_business_keys": False,
            "observation_type_allowed": False,
            "metric_values_finite": False,
            "quality_notes_json": False,
        }
    required_columns = {
        "calendar_name",
        "strategy_name",
        "strategy_version",
        "weight_source_type",
        "source_weight_dataset",
        "observation_type",
        "observation_date",
        "metric_name",
        "metric_value",
        "quality_notes_json",
    }
    if not required_columns.issubset(frame.columns):
        return {
            "non_empty": True,
            "missing_required_columns": False,
            "no_duplicate_business_keys": False,
            "observation_type_allowed": False,
            "metric_values_finite": False,
            "quality_notes_json": False,
        }
    duplicate_count = int(
        frame.duplicated(
            [
                "calendar_name",
                "strategy_name",
                "strategy_version",
                "weight_source_type",
                "observation_type",
                "observation_date",
                "metric_name",
            ]
        ).sum()
    )
    metric_values = [
        _nullable_float(value)
        for value in frame[
            frame["observation_type"]
            .astype(str)
            .isin({"daily_nav", "metric", "turnover"})
        ]["metric_value"].tolist()
    ]
    return {
        "non_empty": True,
        "missing_required_columns": True,
        "no_duplicate_business_keys": duplicate_count == 0,
        "observation_type_allowed": (
            set(frame["observation_type"].astype(str).tolist()).issubset(allowed_types)
        ),
        "metric_values_finite": all(value is not None for value in metric_values),
        "quality_notes_json": all(
            _is_json_text(value) for value in frame["quality_notes_json"]
        ),
    }


def _json_object_or_value(value: object) -> dict[str, Any] | list[Any] | str | None:
    """Parse JSON text for quality/caveat propagation."""

    if value is None or pd.isna(value):
        return None
    if not isinstance(value, str):
        return str(value)
    if not value.strip():
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _json_list_or_empty(value: object) -> list[Any]:
    """Parse a JSON list, returning an empty list for invalid values."""

    if not isinstance(value, str):
        return []
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return []
    return loaded if isinstance(loaded, list) else []


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def _value_counts_dict(series: pd.Series) -> dict[str, int]:
    return {str(key): int(value) for key, value in series.value_counts().items()}


def _nullable_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    number = float(value)
    if not math.isfinite(number):
        return None
    return number


def _is_json_text(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        json.loads(value)
    except json.JSONDecodeError:
        return False
    return True


def _instrument_type_for_dataset(dataset_name: str) -> str | None:
    if dataset_name in {"market.etf_daily", "market.etf_adj_factor"}:
        return "etf"
    if dataset_name == "market.index_daily":
        return "index"
    return None


def _quality_status(results: list[ValidationResultRecord]) -> str:
    statuses = {result.status for result in results}
    if ValidationStatus.WARNING in statuses:
        return ValidationStatus.WARNING.value
    if ValidationStatus.PASS_WITH_CAVEAT in statuses:
        return ValidationStatus.PASS_WITH_CAVEAT.value
    return ValidationStatus.PASS.value


def _optional_text(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    return str(value)


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _utc_now() -> datetime:
    """Return a naive UTC timestamp for DuckDB compatibility."""

    return datetime.now(UTC).replace(tzinfo=None)
