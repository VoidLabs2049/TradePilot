"""Stage D ETF all-weather rebalance snapshot tests."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import unittest

import pandas as pd

from tradepilot import db
from tradepilot.etl.models import RunStatus
from tradepilot.etl.read_models import (
    get_latest_etf_aw_snapshot,
    list_etf_aw_snapshots,
)
from tradepilot.etl.service import ETLService


class StageDRebalanceSnapshotTests(unittest.TestCase):
    """Verify ETF all-weather snapshot read model behavior."""

    def setUp(self) -> None:
        self._original_db_path = db.DB_PATH
        self._original_thread_local = db._thread_local
        self._original_initialized = db._initialized
        self._temp_dir = TemporaryDirectory()
        db.DB_PATH = Path(self._temp_dir.name) / "test.duckdb"
        db._thread_local = threading.local()
        db._initialized = False
        self.conn = db.get_conn()
        self.lakehouse_root = Path(self._temp_dir.name) / "lakehouse"
        self.service = ETLService(
            conn=self.conn,
            source_adapters=[],
            lakehouse_root=self.lakehouse_root,
        )
        self.service.run_bootstrap("reference.etf_aw_sleeves.frozen_v1")

    def tearDown(self) -> None:
        conn = getattr(db._thread_local, "conn", None)
        if conn is not None:
            conn.close()
        db._thread_local = self._original_thread_local
        db.DB_PATH = self._original_db_path
        db._initialized = self._original_initialized
        self._temp_dir.cleanup()

    def test_snapshot_builder_outputs_complete_sleeve_rows(self) -> None:
        self._insert_rebalance(date(2024, 7, 22))
        self._write_sleeve_daily(
            date(2024, 1, 1),
            date(2024, 7, 22),
            daily_return=0.001,
        )
        self._insert_watermarks(date(2024, 7, 22))

        result = self.service.run_bootstrap(
            "derived.etf_aw_rebalance_snapshot.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        self.assertEqual(result["status"], RunStatus.SUCCESS.value)
        self.assertEqual(result["records_written"], 6)
        self.assertTrue(all(result["validation"].values()))
        frame = self._read_snapshot_file(2024, 7)
        self.assertEqual(len(frame), 6)
        self.assertEqual(set(frame["data_status"]), {"complete"})
        equity = frame[frame["sleeve_code"] == "510300.SH"].iloc[0]
        self.assertAlmostEqual(equity["return_1m"], (1.001**21) - 1, places=8)
        self.assertAlmostEqual(equity["return_3m"], (1.001**63) - 1, places=8)
        self.assertAlmostEqual(equity["return_6m"], (1.001**126) - 1, places=8)
        self.assertEqual(equity["volatility_3m"], 0)
        self.assertEqual(equity["max_drawdown_6m"], 0)

    def test_missing_rebalance_row_still_outputs_frozen_universe(self) -> None:
        self._insert_rebalance(date(2024, 7, 22))
        self._write_sleeve_daily(
            date(2024, 1, 1),
            date(2024, 7, 22),
            missing={("518850.SH", date(2024, 7, 22))},
        )
        self._insert_watermarks(date(2024, 7, 22))

        result = self.service.run_bootstrap(
            "derived.etf_aw_rebalance_snapshot.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        self.assertEqual(result["status"], RunStatus.SUCCESS.value)
        frame = self._read_snapshot_file(2024, 7)
        self.assertEqual(len(frame), 6)
        missing = frame[frame["sleeve_code"] == "518850.SH"].iloc[0]
        self.assertEqual(missing["data_status"], "missing")
        self.assertTrue(pd.isna(missing["close"]))

    def test_short_history_marks_partial(self) -> None:
        self._insert_rebalance(date(2024, 7, 22))
        self._write_sleeve_daily(date(2024, 7, 1), date(2024, 7, 22))
        self._insert_watermarks(date(2024, 7, 22))

        result = self.service.run_bootstrap(
            "derived.etf_aw_rebalance_snapshot.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        self.assertEqual(result["status"], RunStatus.SUCCESS.value)
        frame = self._read_snapshot_file(2024, 7)
        self.assertEqual(set(frame["data_status"]), {"partial"})

    def test_lagged_watermark_marks_stale(self) -> None:
        self._insert_rebalance(date(2024, 7, 22))
        self._write_sleeve_daily(date(2024, 1, 1), date(2024, 7, 22))
        self._insert_watermarks(date(2024, 7, 19))

        result = self.service.run_bootstrap(
            "derived.etf_aw_rebalance_snapshot.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        self.assertEqual(result["status"], RunStatus.SUCCESS.value)
        frame = self._read_snapshot_file(2024, 7)
        self.assertEqual(set(frame["data_status"]), {"stale"})

    def test_lagged_source_panel_marks_stale_before_missing(self) -> None:
        self._insert_rebalance(date(2024, 7, 22))
        self._write_sleeve_daily(date(2024, 1, 1), date(2024, 7, 19))
        self._insert_watermarks(date(2024, 7, 22))

        result = self.service.run_bootstrap(
            "derived.etf_aw_rebalance_snapshot.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        self.assertEqual(result["status"], RunStatus.SUCCESS.value)
        frame = self._read_snapshot_file(2024, 7)
        self.assertEqual(set(frame["data_status"]), {"stale"})
        equity = frame[frame["sleeve_code"] == "510300.SH"].iloc[0]
        self.assertEqual(equity["source_max_trade_date"], date(2024, 7, 19))
        self.assertIn("source_lag", equity["quality_notes"])

    def test_repeat_run_upserts_without_duplicate_business_keys(self) -> None:
        self._insert_rebalance(date(2024, 7, 22))
        self._write_sleeve_daily(date(2024, 1, 1), date(2024, 7, 22))
        self._insert_watermarks(date(2024, 7, 22))

        self.service.run_bootstrap(
            "derived.etf_aw_rebalance_snapshot.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )
        result = self.service.run_bootstrap(
            "derived.etf_aw_rebalance_snapshot.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        frame = self._read_snapshot_file(2024, 7)
        self.assertEqual(result["records_updated"], 6)
        self.assertEqual(len(frame), 6)
        self.assertFalse(
            frame.duplicated(["calendar_name", "rebalance_date", "sleeve_code"]).any()
        )

    def test_read_service_returns_latest_snapshot_contract(self) -> None:
        self._insert_rebalance(date(2024, 7, 22))
        self._write_sleeve_daily(date(2024, 1, 1), date(2024, 7, 22))
        self._insert_watermarks(date(2024, 7, 22))
        self.service.run_bootstrap(
            "derived.etf_aw_rebalance_snapshot.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        snapshot = get_latest_etf_aw_snapshot(
            as_of_date=date(2024, 7, 31),
            lakehouse_root=self.lakehouse_root,
        )

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot["schema_version"], "etf_aw_snapshot_v1")
        self.assertEqual(snapshot["contract_version"], "etf_aw_snapshot_contract_v1")
        self.assertEqual(snapshot["rebalance_date"], "2024-07-22")
        self.assertEqual(len(snapshot["sleeves"]), 6)

    def test_read_service_skips_future_partitions_for_as_of_date(self) -> None:
        self._insert_rebalance(date(2024, 7, 22))
        self._insert_rebalance(date(2024, 8, 22))
        self._write_sleeve_daily(date(2024, 1, 1), date(2024, 8, 22))
        self._insert_watermarks(date(2024, 8, 22))
        self.service.run_bootstrap(
            "derived.etf_aw_rebalance_snapshot.build",
            start=date(2024, 7, 1),
            end=date(2024, 8, 31),
        )

        snapshot = get_latest_etf_aw_snapshot(
            as_of_date=date(2024, 8, 1),
            lakehouse_root=self.lakehouse_root,
        )

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot["rebalance_date"], "2024-07-22")

    def test_read_service_defends_against_invalid_snapshot_partition(self) -> None:
        path = self._snapshot_file_path(2024, 7)
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"calendar_name": ["broken"]}).to_parquet(path, index=False)

        snapshot = get_latest_etf_aw_snapshot(
            as_of_date=date(2024, 7, 31),
            lakehouse_root=self.lakehouse_root,
        )

        self.assertIsNone(snapshot)

    def test_read_service_groups_history_by_calendar_name(self) -> None:
        self._insert_rebalance(date(2024, 7, 22))
        self._write_sleeve_daily(date(2024, 1, 1), date(2024, 7, 22))
        self._insert_watermarks(date(2024, 7, 22))
        self.service.run_bootstrap(
            "derived.etf_aw_rebalance_snapshot.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )
        frame = self._read_snapshot_file(2024, 7)
        alternate = frame.copy()
        alternate["calendar_name"] = "alternate_calendar"
        self._write_snapshot_file(2024, 7, pd.concat([frame, alternate]))

        snapshots = list_etf_aw_snapshots(
            date(2024, 7, 1),
            date(2024, 7, 31),
            lakehouse_root=self.lakehouse_root,
        )

        self.assertEqual(len(snapshots), 2)
        self.assertEqual(
            {snapshot["calendar_name"] for snapshot in snapshots},
            {"alternate_calendar", "etf_aw_v2_monthly_post_20"},
        )

    def _insert_rebalance(self, rebalance_date: date) -> None:
        self.conn.execute(
            """
            INSERT INTO canonical_rebalance_calendar (
                calendar_name, calendar_month, rebalance_date, effective_date, notes, updated_at
            ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                "etf_aw_v2_monthly_post_20",
                f"{rebalance_date.year:04d}-{rebalance_date.month:02d}",
                rebalance_date,
                rebalance_date,
                "{}",
            ],
        )

    def _insert_watermarks(self, fetched_date: date) -> None:
        for dataset_name in (
            "market.etf_daily",
            "market.etf_adj_factor",
            "reference.trading_calendar",
        ):
            self.conn.execute(
                """
                INSERT INTO etl_source_watermarks (
                    dataset_name, source_name, latest_available_date,
                    latest_fetched_date, latest_successful_run_id, updated_at
                ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [dataset_name, "fixture", fetched_date, fetched_date, 1],
            )

    def _write_sleeve_daily(
        self,
        start: date,
        end: date,
        *,
        daily_return: float = 0.001,
        missing: set[tuple[str, date]] | None = None,
    ) -> None:
        missing = missing or set()
        rows: list[dict] = []
        codes = [
            ("510300.SH", "equity_large"),
            ("159845.SZ", "equity_small"),
            ("513100.SH", "equity_overseas"),
            ("511010.SH", "bond"),
            ("518850.SH", "gold"),
            ("159001.SZ", "cash"),
        ]
        current = start
        dates: list[date] = []
        while current <= end:
            if current.weekday() < 5:
                dates.append(current)
            current += timedelta(days=1)
        for code, role in codes:
            price = 10.0
            previous = None
            for trade_date in dates:
                if (code, trade_date) in missing:
                    continue
                price = price * (1 + daily_return)
                pct = None if previous is None else daily_return * 100
                previous = price
                rows.append(
                    {
                        "sleeve_code": code,
                        "sleeve_role": role,
                        "instrument_id": code,
                        "trade_date": trade_date,
                        "open": price,
                        "high": price,
                        "low": price,
                        "close": price,
                        "adj_factor": 1.0,
                        "adj_close": price,
                        "pct_chg": pct,
                        "adj_pct_chg": pct,
                        "volume": 1.0,
                        "amount": 1.0,
                        "source_name": "fixture",
                        "ingested_at": pd.Timestamp("2024-07-22"),
                        "quality_status": "pass",
                    }
                )
        self.service._write_etf_aw_sleeve_daily(pd.DataFrame(rows))

    def _read_snapshot_file(self, year: int, month: int) -> pd.DataFrame:
        path = self._snapshot_file_path(year, month)
        return pd.read_parquet(path)

    def _write_snapshot_file(self, year: int, month: int, frame: pd.DataFrame) -> None:
        path = self._snapshot_file_path(year, month)
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(path, index=False)

    def _snapshot_file_path(self, year: int, month: int) -> Path:
        return (
            self.lakehouse_root
            / "derived"
            / "derived.etf_aw_rebalance_snapshot"
            / str(year)
            / f"{month:02d}"
            / "part-00000.parquet"
        )


if __name__ == "__main__":
    unittest.main()
