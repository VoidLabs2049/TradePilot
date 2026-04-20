"""Dataset definition models for the generic ETL registry."""

from __future__ import annotations

from pathlib import PurePath

from pydantic import BaseModel, Field, field_validator

from tradepilot.etl.models import DatasetCategory, StorageZone


class DatasetDefinition(BaseModel):
    """Metadata contract for one dataset in the ETL registry."""

    dataset_name: str
    category: DatasetCategory
    grain: str
    primary_source: str
    storage_zone: StorageZone
    fallback_sources: list[str] = Field(default_factory=list)
    validation_sources: list[str] = Field(default_factory=list)
    partition_strategy: str | None = None
    canonical_schema_name: str | None = None
    validation_rule_names: list[str] = Field(default_factory=list)
    supports_incremental: bool = False
    watermark_key: str | None = None
    timing_semantics: str | None = None
    dependencies: list[str] = Field(default_factory=list)

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
        if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts) or len(path.parts) != 1:
            raise ValueError("dataset_name must be a single safe path component")
        return value
