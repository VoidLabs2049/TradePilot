"""Executable Stage B ETL orchestration service."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from tradepilot import db
from tradepilot.etl.datasets import DatasetDefinition
from tradepilot.etl.models import (
    CanonicalWriteResult,
    DatasetSyncResult,
    IngestionRequest,
    RunStatus,
    SourceFetchResult,
    TriggerMode,
    ValidationResultRecord,
    ValidationStatus,
)
from tradepilot.etl.normalizers import get_normalizer
from tradepilot.etl.registry import DatasetRegistry, register_stage_b_datasets
from tradepilot.etl.sources import BaseSourceAdapter, TushareSourceAdapter
from tradepilot.etl.storage import (
    build_normalized_file_path,
    cleanup_temp_files,
    write_normalized_parquet,
    write_raw_parquet,
)
from tradepilot.etl.validators import (
    get_validator,
    has_blocking_failures,
    validation_counts,
)


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
            validation_results = validator.validate(canonical, context)
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
            self._advance_watermark(definition, source.source_name, run_id, canonical)
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

    def run_bootstrap(self, profile_name: str) -> dict:
        """Stage B does not implement profile bootstrap runners."""

        raise NotImplementedError("profile bootstrap is deferred beyond Stage B")

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
            ok = self._dependency_available(definition, dependency, request)
            if not ok:
                dep_request = self._dependency_request(definition, dependency, request)
                self.run_dataset_sync(dependency, dep_request)
                ok = self._dependency_available(definition, dependency, request)
            status = ValidationStatus.PASS_WITH_CAVEAT if ok else ValidationStatus.FAIL
            results.append(
                ValidationResultRecord(
                    validation_id=0,
                    run_id=run_id,
                    raw_batch_id=None,
                    dataset_name=definition.dataset_name,
                    check_name=(
                        "dependency_preflight.snapshot_missing"
                        if dependency == "reference.instruments"
                        else "dependency_preflight.window_missing"
                    ),
                    check_level="dependency",
                    status=status,
                    subject_key=dependency,
                    metric_value=1 if ok else 0,
                    threshold_value=1,
                    details_json='{"auto_run_attempted": true}',
                    created_at=_utc_now(),
                )
            )
        return results

    def _dependency_available(
        self, definition: DatasetDefinition, dependency: str, request: IngestionRequest
    ) -> bool:
        if dependency == "reference.instruments":
            instrument_type = _instrument_type_for_dataset(definition.dataset_name)
            ids = request.context.get("instrument_ids")
            if ids:
                id_list = ids if isinstance(ids, list) else [ids]
                self.conn.register(
                    "stage_b_required_instruments",
                    pd.DataFrame({"instrument_id": [str(item) for item in id_list]}),
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
            start = request.request_start or request.request_end or date.today()
            end = request.request_end or request.request_start or date.today()
            count = self.conn.execute(
                """
                SELECT COUNT(*) FROM canonical_trading_calendar
                WHERE trade_date BETWEEN ? AND ?
                """,
                [start, end],
            ).fetchone()[0]
            return int(count) > 0
        return True

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
        if canonical.empty:
            return CanonicalWriteResult()
        frame = canonical.copy()
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
        frame["year"] = frame["trade_date"].dt.year
        frame["month"] = frame["trade_date"].dt.month
        storage_paths: list[str] = []
        partitions_written = 0
        for (year, month), partition in frame.groupby(["year", "month"], dropna=True):
            parts = [("year", int(year)), ("month", f"{int(month):02d}")]
            final_path = build_normalized_file_path(
                definition.dataset_name, parts, lakehouse_root=self.lakehouse_root
            )
            partition_frame = partition.drop(columns=["year", "month"]).copy()
            if final_path.exists():
                existing = pd.read_parquet(final_path)
                existing["trade_date"] = pd.to_datetime(
                    existing["trade_date"], errors="coerce"
                )
                merged = pd.concat([existing, partition_frame], ignore_index=True)
            else:
                merged = partition_frame
            merged = (
                merged.sort_values(["instrument_id", "trade_date", "ingested_at"])
                .drop_duplicates(["instrument_id", "trade_date"], keep="last")
                .reset_index(drop=True)
            )
            merged["trade_date"] = pd.to_datetime(
                merged["trade_date"], errors="coerce"
            ).dt.date
            write_result = write_normalized_parquet(
                merged,
                definition.dataset_name,
                parts,
                lakehouse_root=self.lakehouse_root,
            )
            storage_paths.append(write_result.relative_path)
            partitions_written += 1
        return CanonicalWriteResult(
            records_written=len(canonical),
            records_inserted=len(canonical),
            records_updated=0,
            partitions_written=partitions_written,
            storage_paths=storage_paths,
        )

    def _next_id(self, table: str, column: str) -> int:
        value = self.conn.execute(
            f"SELECT COALESCE(MAX({column}), 0) + 1 FROM {table}"
        ).fetchone()[0]
        return int(value)

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
            DELETE FROM etl_source_watermarks
            WHERE dataset_name = ? AND source_name = ?
            """,
            [definition.dataset_name, source_name],
        )
        self.conn.execute(
            """
            INSERT INTO etl_source_watermarks (
                dataset_name, source_name, latest_available_date,
                latest_fetched_date, latest_successful_run_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
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


def _instrument_type_for_dataset(dataset_name: str) -> str | None:
    if dataset_name == "market.etf_daily":
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
