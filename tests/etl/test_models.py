"""Contract tests for ETL models."""

from __future__ import annotations

from datetime import datetime
import unittest

from pydantic import ValidationError

from tradepilot.etl.datasets import DatasetDefinition
from tradepilot.etl.models import (
    DatasetCategory,
    IngestionRequest,
    IngestionRunRecord,
    RunStatus,
    StorageZone,
    TriggerMode,
    ValidationStatus,
)


class ETLModelTests(unittest.TestCase):
    """Verify Stage A model contracts."""

    def test_enum_values_are_stable(self) -> None:
        """Expose stable string values for downstream contracts."""

        self.assertEqual(DatasetCategory.MARKET.value, "market")
        self.assertEqual(StorageZone.NORMALIZED.value, "normalized")
        self.assertEqual(TriggerMode.RETRY_FAILED.value, "retry_failed")
        self.assertEqual(RunStatus.PARTIAL_SUCCESS.value, "partial_success")
        self.assertEqual(ValidationStatus.PASS_WITH_CAVEAT.value, "pass_with_caveat")

    def test_dataset_definition_requires_core_fields(self) -> None:
        """Reject missing required dataset metadata."""

        with self.assertRaises(ValidationError):
            DatasetDefinition(
                dataset_name="market.etf_daily",
                category=DatasetCategory.MARKET,
                grain="daily",
                primary_source="tushare",
            )

    def test_dataset_definition_defaults_are_stable(self) -> None:
        """Provide predictable optional defaults."""

        definition = DatasetDefinition(
            dataset_name="market.etf_daily",
            category=DatasetCategory.MARKET,
            grain="daily",
            primary_source="tushare",
            storage_zone=StorageZone.RAW,
        )

        self.assertEqual(definition.fallback_sources, [])
        self.assertEqual(definition.validation_rule_names, [])
        self.assertFalse(definition.supports_incremental)

    def test_dataset_definition_rejects_unsafe_dataset_name(self) -> None:
        """Reject dataset names that are not safe path components."""

        with self.assertRaises(ValidationError):
            DatasetDefinition(
                dataset_name="../legacy",
                category=DatasetCategory.MARKET,
                grain="daily",
                primary_source="tushare",
                storage_zone=StorageZone.RAW,
            )

    def test_request_and_run_record_serialize_enums_as_values(self) -> None:
        """Keep model serialization readable for future metadata writes."""

        request = IngestionRequest(trigger_mode=TriggerMode.BACKFILL)
        run = IngestionRunRecord(
            run_id=1,
            job_name="market_sync",
            dataset_name="market.etf_daily",
            source_name="tushare",
            trigger_mode=TriggerMode.BACKFILL,
            status=RunStatus.RUNNING,
            started_at=datetime(2026, 4, 19, 9, 0, 0),
        )

        self.assertEqual(request.model_dump(mode="json")["trigger_mode"], "backfill")
        self.assertEqual(run.model_dump(mode="json")["status"], "running")


if __name__ == "__main__":
    unittest.main()
