"""Dataset definition models for the generic ETL registry."""

from __future__ import annotations

from pathlib import PurePath

from pydantic import BaseModel, Field, field_validator

from tradepilot.etl.models import DatasetCategory, StorageZone


class DatasetDefinition(BaseModel):
    """Metadata contract for one dataset in the ETL registry."""

    dataset_name: str = Field(
        description="Stable registry key and safe path component for the dataset."
    )
    category: DatasetCategory = Field(
        description="Business family used to group related datasets."
    )
    grain: str = Field(
        description="Smallest logical observation level, such as daily stock bars."
    )
    primary_source: str = Field(
        description="Default source adapter name used to fetch the dataset."
    )
    storage_zone: StorageZone = Field(
        description="Lakehouse zone where the dataset is primarily stored."
    )
    fallback_sources: list[str] = Field(
        default_factory=list,
        description="Ordered fallback source adapter names for degraded fetching.",
    )
    validation_sources: list[str] = Field(
        default_factory=list,
        description="Independent source names used to cross-check dataset quality.",
    )
    partition_strategy: str | None = Field(
        default=None,
        description="Storage partitioning rule applied when writing dataset files.",
    )
    canonical_schema_name: str | None = Field(
        default=None,
        description="Canonical schema identifier expected after normalization.",
    )
    validation_rule_names: list[str] = Field(
        default_factory=list,
        description="Validation rule names that should run for this dataset.",
    )
    supports_incremental: bool = Field(
        default=False,
        description="Whether the dataset supports watermark-based incremental sync.",
    )
    watermark_key: str | None = Field(
        default=None,
        description="Field name used as the incremental sync watermark.",
    )
    timing_semantics: str | None = Field(
        default=None,
        description="Timing convention for interpreting record dates and availability.",
    )
    dependencies: list[str] = Field(
        default_factory=list,
        description="Dataset names that must be available before this dataset runs.",
    )

    @field_validator("dataset_name", "grain", "primary_source")
    @classmethod
    def _validate_required_text(cls, value: str) -> str:
        """Reject blank required text fields."""

        stripped = value.strip()
        if not stripped:
            raise ValueError("value must not be blank")
        return stripped

    @field_validator("dataset_name")
    @classmethod
    def _validate_dataset_name(cls, value: str) -> str:
        """Reject dataset names that are not safe path components."""

        path = PurePath(value)
        if (
            path.is_absolute()
            or any(part in {"", ".", ".."} for part in path.parts)
            or len(path.parts) != 1
        ):
            raise ValueError("dataset_name must be a single safe path component")
        return value
