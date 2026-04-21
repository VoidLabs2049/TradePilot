"""Contract tests for ETL-related DuckDB initialization."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import unittest

from tradepilot import db


class DuckDBInitTests(unittest.TestCase):
    """Verify Stage A schema initialization behavior."""

    def setUp(self) -> None:
        """Swap the database path to a temporary DuckDB file."""

        self._original_db_path = db.DB_PATH
        self._original_thread_local = db._thread_local
        self._original_initialized = db._initialized
        self._temp_dir = TemporaryDirectory()
        db.DB_PATH = Path(self._temp_dir.name) / "test.duckdb"
        db.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        db._thread_local = threading.local()
        db._initialized = False

    def tearDown(self) -> None:
        """Restore the original database globals."""

        conn = getattr(db._thread_local, "conn", None)
        if conn is not None:
            conn.close()
        db._thread_local = self._original_thread_local
        db.DB_PATH = self._original_db_path
        db._initialized = self._original_initialized
        self._temp_dir.cleanup()

    def test_get_conn_initializes_etl_and_legacy_tables(self) -> None:
        """Create the new ETL tables without dropping legacy tables."""

        conn = db.get_conn()

        table_names = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
        self.assertIn("etl_ingestion_runs", table_names)
        self.assertIn("etl_raw_batches", table_names)
        self.assertIn("etl_validation_results", table_names)
        self.assertIn("etl_source_watermarks", table_names)
        self.assertIn("canonical_trading_calendar", table_names)
        self.assertIn("source_registry", table_names)
        self.assertIn("ingestion_runs", table_names)
        self.assertIn("trading_calendar", table_names)

    def test_get_conn_is_idempotent(self) -> None:
        """Allow repeated initialization without schema churn."""

        first_conn = db.get_conn()
        db._initialized = False
        second_conn = db.get_conn()

        self.assertIs(first_conn, second_conn)
        self.assertTrue(db._initialized)


if __name__ == "__main__":
    unittest.main()
