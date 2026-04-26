"""Stage A ETL foundation skeleton."""

from tradepilot.etl.datasets import DatasetDefinition
from tradepilot.etl.models import (
    DatasetCategory,
    IngestionRequest,
    IngestionRunRecord,
    RawBatchRecord,
    RunStatus,
    SourceFetchResult,
    SourceWatermarkRecord,
    StorageZone,
    TriggerMode,
    ValidationResultRecord,
    ValidationStatus,
)
from tradepilot.etl.registry import (
    DatasetRegistry,
    get_dataset,
    has_dataset,
    list_datasets,
    register_dataset,
    register_stage_b_datasets,
)

__all__ = [
    "DatasetCategory",
    "DatasetDefinition",
    "DatasetRegistry",
    "IngestionRequest",
    "IngestionRunRecord",
    "RawBatchRecord",
    "RunStatus",
    "SourceFetchResult",
    "SourceWatermarkRecord",
    "StorageZone",
    "TriggerMode",
    "ValidationResultRecord",
    "ValidationStatus",
    "get_dataset",
    "has_dataset",
    "list_datasets",
    "register_dataset",
    "register_stage_b_datasets",
]
