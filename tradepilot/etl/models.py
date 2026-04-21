"""Core models for the generic ETL foundation."""

from __future__ import annotations

from datetime import date, datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class DatasetCategory(StrEnum):
    """Supported dataset families."""

    # Static reference data that changes infrequently.
    REFERENCE = "reference"
    # Time-series market data such as prices, volume, and quotes.
    MARKET = "market"
    # Interest rates, yields, and other rate-oriented datasets.
    RATES = "rates"
    # Macro-economic indicators and broad economic releases.
    MACRO = "macro"
    # Alternative or non-traditional data sources.
    ALT = "alt"
    # Datasets computed from other raw or normalized datasets.
    DERIVED = "derived"


class StorageZone(StrEnum):
    """Top-level lakehouse storage zones."""

    # Immutable source payloads captured before normalization.
    RAW = "raw"
    # Canonicalized records ready for downstream consumption.
    NORMALIZED = "normalized"
    # Higher-level outputs produced from transformed upstream data.
    DERIVED = "derived"


class TriggerMode(StrEnum):
    """How an ETL run is triggered."""

    # Started explicitly by a person or ad hoc tool.
    MANUAL = "manual"
    # Started by a scheduler or cron-like automation.
    SCHEDULED = "scheduled"
    # Started to fill historical gaps over a prior time range.
    BACKFILL = "backfill"
    # Started as a retry after an earlier failed attempt.
    RETRY_FAILED = "retry_failed"
    # Started only to run validation without a full ingestion.
    VALIDATION_ONLY = "validation_only"


class RunStatus(StrEnum):
    """Framework-level status for one ETL run."""

    # Created but not yet started.
    PENDING = "pending"
    # Currently executing.
    RUNNING = "running"
    # Finished successfully.
    SUCCESS = "success"
    # Finished with a terminal failure.
    FAILED = "failed"
    # Finished with mixed results across processed work.
    PARTIAL_SUCCESS = "partial_success"


class ValidationStatus(StrEnum):
    """Structured validation result status."""

    # Check passed with no notable concerns.
    PASS = "pass"
    # Check passed but requires operator awareness.
    PASS_WITH_CAVEAT = "pass_with_caveat"
    # Check surfaced a warning that may need follow-up.
    WARNING = "warning"
    # Validation was run without a full ingestion workflow.
    VALIDATION_ONLY = "validation_only"
    # Validation decision is intentionally deferred.
    DEFER = "defer"
    # Check failed and indicates a blocking issue.
    FAIL = "fail"


class IngestionRequest(BaseModel):
    """Common request envelope for future dataset sync entrypoints."""

    request_start: date | None = Field(
        default=None,
        description="Inclusive start date requested for the ingestion window.",
    )
    request_end: date | None = Field(
        default=None,
        description="Inclusive end date requested for the ingestion window.",
    )
    full_refresh: bool = Field(
        default=False,
        description="Whether to ignore watermarks and rebuild the requested data.",
    )
    trigger_mode: TriggerMode = Field(
        default=TriggerMode.MANUAL,
        description="Operational reason that started the ingestion run.",
    )
    context: dict[str, str | int | float | bool | None] = Field(
        default_factory=dict,
        description="Additional primitive request parameters passed to source adapters.",
    )


class IngestionRunRecord(BaseModel):
    """Execution record for one ETL run."""

    run_id: int = Field(description="Unique metadata identifier for the ingestion run.")
    job_name: str = Field(
        description="Human-readable job or workflow name for this run."
    )
    dataset_name: str = Field(description="Dataset registry key processed by this run.")
    source_name: str = Field(description="Source adapter name used by this run.")
    trigger_mode: TriggerMode = Field(
        description="Operational reason that started the ingestion run."
    )
    status: RunStatus = Field(
        default=RunStatus.PENDING,
        description="Current framework-level execution status.",
    )
    started_at: datetime = Field(description="Timestamp when the run started.")
    finished_at: datetime | None = Field(
        default=None, description="Timestamp when the run finished, if complete."
    )
    request_start: date | None = Field(
        default=None, description="Inclusive requested ingestion window start date."
    )
    request_end: date | None = Field(
        default=None, description="Inclusive requested ingestion window end date."
    )
    records_discovered: int = Field(
        default=0,
        description="Number of source records discovered before filtering or writes.",
    )
    records_inserted: int = Field(
        default=0, description="Number of records inserted by the run."
    )
    records_updated: int = Field(
        default=0, description="Number of existing records updated by the run."
    )
    records_failed: int = Field(
        default=0, description="Number of records that failed processing or validation."
    )
    partitions_written: int = Field(
        default=0, description="Number of lakehouse partitions written by the run."
    )
    error_message: str | None = Field(
        default=None,
        description="Failure summary captured when the run does not succeed.",
    )
    code_version: str | None = Field(
        default=None, description="Application or git version associated with the run."
    )


class RawBatchRecord(BaseModel):
    """Manifest row for one immutable raw batch."""

    raw_batch_id: int = Field(
        description="Unique metadata identifier for the raw batch."
    )
    run_id: int = Field(description="Ingestion run that produced this raw batch.")
    dataset_name: str = Field(
        description="Dataset registry key contained in the raw batch."
    )
    source_name: str = Field(
        description="Source adapter name that fetched the raw batch."
    )
    source_endpoint: str | None = Field(
        default=None,
        description="Provider endpoint or resource used to fetch the batch.",
    )
    storage_path: str = Field(
        description="Lakehouse path where the immutable raw batch is stored."
    )
    file_format: str = Field(
        description="Serialized file format used for the raw batch."
    )
    compression: str | None = Field(
        default=None, description="Compression codec applied to the stored raw batch."
    )
    partition_year: int | None = Field(
        default=None,
        description="Year partition value for the raw batch, when applicable.",
    )
    partition_month: int | None = Field(
        default=None,
        description="Month partition value for the raw batch, when applicable.",
    )
    window_start: date | None = Field(
        default=None,
        description="Inclusive source data window start represented by the batch.",
    )
    window_end: date | None = Field(
        default=None,
        description="Inclusive source data window end represented by the batch.",
    )
    row_count: int = Field(
        default=0, description="Number of rows or records stored in the raw batch."
    )
    content_hash: str | None = Field(
        default=None, description="Hash of the raw content used for idempotency checks."
    )
    fetched_at: datetime = Field(
        description="Timestamp when the raw payload was fetched."
    )
    schema_version: str | None = Field(
        default=None, description="Source schema version observed for the raw batch."
    )
    is_fallback_source: bool = Field(
        default=False,
        description="Whether the batch came from a fallback source adapter.",
    )


class ValidationResultRecord(BaseModel):
    """Structured output for one validation check."""

    validation_id: int = Field(
        description="Unique metadata identifier for the validation result."
    )
    run_id: int = Field(
        description="Ingestion run associated with the validation check."
    )
    raw_batch_id: int | None = Field(
        default=None,
        description="Raw batch associated with the validation check, if any.",
    )
    dataset_name: str = Field(
        description="Dataset registry key validated by the check."
    )
    check_name: str = Field(description="Stable validation rule or check name.")
    check_level: str = Field(
        description="Severity or scope level for the validation check."
    )
    status: ValidationStatus = Field(
        description="Structured outcome produced by the validation check."
    )
    subject_key: str | None = Field(
        default=None,
        description="Specific entity, partition, or metric subject validated.",
    )
    metric_value: float | None = Field(
        default=None, description="Observed metric value produced by the check."
    )
    threshold_value: float | None = Field(
        default=None,
        description="Threshold value used to evaluate the observed metric.",
    )
    details_json: str | None = Field(
        default=None,
        description="JSON-encoded diagnostic details for the validation result.",
    )
    created_at: datetime = Field(
        description="Timestamp when the validation result was created."
    )


class SourceWatermarkRecord(BaseModel):
    """Latest successful incremental boundary for one dataset-source pair."""

    dataset_name: str = Field(
        description="Dataset registry key tracked by this watermark."
    )
    source_name: str = Field(
        description="Source adapter name tracked by this watermark."
    )
    latest_available_date: date | None = Field(
        default=None, description="Latest date known to be available from the source."
    )
    latest_fetched_date: date | None = Field(
        default=None, description="Latest date successfully fetched from the source."
    )
    latest_successful_run_id: int | None = Field(
        default=None,
        description="Most recent successful run that advanced the watermark.",
    )
    updated_at: datetime = Field(
        description="Timestamp when the watermark record was last updated."
    )
