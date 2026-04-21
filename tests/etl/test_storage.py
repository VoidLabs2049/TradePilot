"""Contract tests for ETL storage path planning."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from tradepilot.etl.models import StorageZone
from tradepilot.etl.storage import (
    build_partition_path,
    build_zone_path,
    ensure_zone_roots,
)


class StoragePathTests(unittest.TestCase):
    """Verify Stage A lakehouse path conventions."""

    def test_ensure_zone_roots_creates_zone_directories(self) -> None:
        """Create all top-level lakehouse zones."""

        with TemporaryDirectory() as temp_dir:
            roots = ensure_zone_roots(Path(temp_dir))

            self.assertTrue(roots[StorageZone.RAW].is_dir())
            self.assertTrue(roots[StorageZone.NORMALIZED].is_dir())
            self.assertTrue(roots[StorageZone.DERIVED].is_dir())

    def test_build_zone_path_preserves_dataset_name(self) -> None:
        """Keep dotted dataset names as directory names."""

        path = build_zone_path(
            dataset_name="market.etf_daily",
            zone=StorageZone.NORMALIZED,
            lakehouse_root=Path("/tmp/lakehouse"),
        )

        self.assertEqual(path, Path("/tmp/lakehouse/normalized/market.etf_daily"))

    def test_build_partition_path_sorts_mapping_parts(self) -> None:
        """Build stable partition paths from mapping key-value parts."""

        path = build_partition_path(
            dataset_name="market.etf_daily",
            zone=StorageZone.RAW,
            partition_parts={"year": 2026, "month": 4},
            lakehouse_root=Path("/tmp/lakehouse"),
        )

        self.assertEqual(
            path,
            Path("/tmp/lakehouse/raw/market.etf_daily/month=4/year=2026"),
        )

    def test_build_zone_path_rejects_unsafe_dataset_name(self) -> None:
        """Reject dataset names that would escape the lakehouse root."""

        with self.assertRaisesRegex(ValueError, "single safe path component"):
            build_zone_path(
                dataset_name="../legacy",
                zone=StorageZone.NORMALIZED,
                lakehouse_root=Path("/tmp/lakehouse"),
            )

    def test_build_partition_path_rejects_unsafe_partition_key(self) -> None:
        """Reject partition keys that would create path-like segments."""

        with self.assertRaisesRegex(ValueError, "partition key"):
            build_partition_path(
                dataset_name="market.etf_daily",
                zone=StorageZone.RAW,
                partition_parts={"../year": 2026},
                lakehouse_root=Path("/tmp/lakehouse"),
            )

    def test_build_partition_path_rejects_unsafe_partition_value(self) -> None:
        """Reject partition values that would create path-like segments."""

        with self.assertRaisesRegex(ValueError, "partition value"):
            build_partition_path(
                dataset_name="market.etf_daily",
                zone=StorageZone.RAW,
                partition_parts={"year": "../2026"},
                lakehouse_root=Path("/tmp/lakehouse"),
            )


if __name__ == "__main__":
    unittest.main()
