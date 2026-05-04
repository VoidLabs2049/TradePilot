"""Executable Stage B ETL orchestration service."""

from __future__ import annotations

from calendar import monthrange
from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from tradepilot import db
from tradepilot.etl.datasets import DatasetDefinition
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
_ETF_AW_SLEEVES: list[dict[str, Any]] = [
    {
        "sleeve_code": "510300.SH",
        "sleeve_role": "equity_large",
        "sleeve_name": "沪深300ETF华泰柏瑞",
        "listing_exchange": "SH",
        "benchmark_name": "沪深300指数",
        "list_date": date(2012, 5, 28),
        "exposure_note": "Large-cap China equity beta proxy.",
    },
    {
        "sleeve_code": "159845.SZ",
        "sleeve_role": "equity_small",
        "sleeve_name": "中证1000ETF华夏",
        "listing_exchange": "SZ",
        "benchmark_name": "中证1000指数收益率",
        "list_date": date(2021, 3, 31),
        "exposure_note": "Small-cap and higher-beta China equity proxy.",
    },
    {
        "sleeve_code": "511010.SH",
        "sleeve_role": "bond",
        "sleeve_name": "国债ETF国泰",
        "listing_exchange": "SH",
        "benchmark_name": "上证5年期国债指数收益率",
        "list_date": date(2013, 3, 25),
        "exposure_note": (
            "Duration-bearing sovereign bond defense sleeve; not a universal "
            "bond factor or maximally convex crisis hedge."
        ),
    },
    {
        "sleeve_code": "518850.SH",
        "sleeve_role": "gold",
        "sleeve_name": "黄金ETF华夏",
        "listing_exchange": "SH",
        "benchmark_name": "上海黄金交易所黄金现货实盘合约Au99.99价格收益率",
        "list_date": date(2020, 6, 5),
        "exposure_note": "Gold hedge sleeve for inflation and stress diversification.",
    },
    {
        "sleeve_code": "159001.SZ",
        "sleeve_role": "cash",
        "sleeve_name": "货币ETF易方达",
        "listing_exchange": "SZ",
        "benchmark_name": "活期存款基准利率*(1-利息税税率)",
        "list_date": date(2014, 10, 20),
        "exposure_note": (
            "Cash-like neutral buffer sleeve with low-volatility behavior."
        ),
    },
]
_ETF_AW_SLEEVE_CODES = [str(row["sleeve_code"]) for row in _ETF_AW_SLEEVES]
_ETF_AW_SLEEVE_ROLES = {str(row["sleeve_role"]) for row in _ETF_AW_SLEEVES}


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
        raise KeyError(f"unsupported bootstrap profile: {profile_name}")

    def list_runs(self, dataset_name: str | None = None) -> list[dict]:
        """List ETL run history."""

        if dataset_name is None:
            rows = self.conn.execute(
                "SELECT * FROM etl_ingestion_runs ORDER BY started_at DESC"
            ).fetchdf()
        else:
            rows = self.conn.execute(
                "SELECT * FROM etl_ingestion_runs WHERE dataset_name = ? ORDER BY started_at DESC",
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
                f"source adapter {adapter.source_name} does not support {definition.dataset_name}"
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
        return self._write_market_daily(definition, canonical)

    def _write_trading_calendar(self, canonical: pd.DataFrame) -> CanonicalWriteResult:
        if canonical.empty:
            return CanonicalWriteResult()
        frame = canonical.copy()
        frame["updated_at"] = _utc_now()
        self.conn.register("stage_b_calendar", frame)
        existing = int(self.conn.execute("""
                SELECT COUNT(*) FROM canonical_trading_calendar
                WHERE (exchange, trade_date) IN (
                    SELECT exchange, trade_date FROM stage_b_calendar
                )
                """).fetchone()[0])
        self.conn.execute("""
            DELETE FROM canonical_trading_calendar
            WHERE (exchange, trade_date) IN (
                SELECT exchange, trade_date FROM stage_b_calendar
            )
            """)
        self.conn.execute("""
            INSERT INTO canonical_trading_calendar
            SELECT exchange, trade_date, is_open, pretrade_date, updated_at
            FROM stage_b_calendar
            """)
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
        existing = int(self.conn.execute("""
                SELECT COUNT(*) FROM canonical_instruments
                WHERE instrument_id IN (
                    SELECT instrument_id FROM stage_b_instruments
                )
                """).fetchone()[0])
        self.conn.execute("""
            DELETE FROM canonical_instruments
            WHERE instrument_id IN (
                SELECT instrument_id FROM stage_b_instruments
            )
            """)
        self.conn.execute("""
            INSERT INTO canonical_instruments
            SELECT instrument_id, source_instrument_id, instrument_name,
                   instrument_type, exchange, list_date, delist_date,
                   is_active, source_name, updated_at
            FROM stage_b_instruments
            """)
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

    def _write_year_month_partition_upsert(
        self,
        *,
        dataset_name: str,
        zone: StorageZone,
        canonical: pd.DataFrame,
        key_columns: tuple[str, ...],
        sort_columns: tuple[str, ...],
    ) -> CanonicalWriteResult:
        if canonical.empty:
            return CanonicalWriteResult()
        frame = canonical.copy()
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
        frame["year"] = frame["trade_date"].dt.year
        frame["month"] = frame["trade_date"].dt.month
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
            for column in sort_columns:
                if isinstance(merged[column].dtype, pd.CategoricalDtype):
                    merged[column] = merged[column].astype(str)
            merged = (
                merged.sort_values(list(sort_columns))
                .drop_duplicates(list(key_columns), keep="last")
                .reset_index(drop=True)
            )
            if "trade_date" in merged.columns:
                merged["trade_date"] = pd.to_datetime(
                    merged["trade_date"], errors="coerce"
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
        if "trade_date" in canonical.columns and not canonical.empty:
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
            self.conn.execute("""
                DELETE FROM canonical_sleeves
                WHERE sleeve_code IN (
                    SELECT sleeve_code FROM stage_c_etf_aw_sleeves
                )
                """)
            self.conn.execute("""
                INSERT INTO canonical_sleeves (
                    sleeve_code, sleeve_name, sleeve_type, is_active, updated_at,
                    sleeve_role, listing_exchange, benchmark_name, list_date,
                    exposure_note, created_at
                )
                SELECT sleeve_code, sleeve_name, sleeve_type, is_active, updated_at,
                       sleeve_role, listing_exchange, benchmark_name, list_date,
                       exposure_note, created_at
                FROM stage_c_etf_aw_sleeves
                """)
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
            self.conn.execute("""
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
                """)
        finally:
            self.conn.unregister("stage_c_etf_aw_instruments")

    def _validate_etf_aw_sleeves(self) -> dict[str, bool]:
        self.conn.register("stage_c_etf_aw_codes", _etf_aw_sleeve_codes_frame())
        try:
            rows = self.conn.execute("""
                SELECT s.sleeve_code, s.sleeve_role, s.listing_exchange,
                       s.exposure_note, s.is_active
                FROM canonical_sleeves s
                JOIN stage_c_etf_aw_codes c
                  ON s.sleeve_code = c.sleeve_code
                ORDER BY s.sleeve_code
                """).fetchall()
            instrument_count = int(self.conn.execute("""
                    SELECT COUNT(*)
                    FROM canonical_instruments i
                    JOIN stage_c_etf_aw_codes c
                      ON i.instrument_id = c.sleeve_code
                    WHERE i.instrument_type = 'etf'
                    """).fetchone()[0])
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
        daily = self._read_partitioned_dataset(
            "market.etf_daily",
            start,
            end,
            StorageZone.NORMALIZED,
        )
        adj = self._read_partitioned_dataset(
            "market.etf_adj_factor",
            start,
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
            sleeves = self.conn.execute("""
                SELECT s.sleeve_code, s.sleeve_role
                FROM canonical_sleeves s
                JOIN stage_c_etf_aw_codes c
                  ON s.sleeve_code = c.sleeve_code
                WHERE s.is_active = TRUE
                """).fetchdf()
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
        merged = merged[
            merged["trade_date"].between(start, end, inclusive="both")
        ].copy()
        merged = merged.sort_values(["instrument_id", "trade_date"]).reset_index(
            drop=True
        )
        merged["adj_close"] = merged["close"] * merged["adj_factor"]
        # Return between adjacent available observations after the input merge.
        merged["adj_pct_chg"] = (
            merged.groupby("instrument_id")["adj_close"].pct_change() * 100
        )
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
            self.conn.execute("""
                INSERT INTO canonical_rebalance_calendar (
                    calendar_name, calendar_month, rebalance_date, effective_date,
                    notes, updated_at
                )
                SELECT calendar_name, calendar_month, rebalance_date, effective_date,
                       notes, updated_at
                FROM stage_c_rebalance_calendar
                """)
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
    return {
        tuple(str(value) if isinstance(value, str) else value for value in row)
        for row in key_frame.itertuples(index=False)
        if all(pd.notna(value) for value in row)
    }


def _etf_aw_sleeve_codes_frame() -> pd.DataFrame:
    """Return the frozen ETF all-weather sleeve universe as a query frame."""

    return pd.DataFrame({"sleeve_code": _ETF_AW_SLEEVE_CODES})


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
