"""Stage C reference calendar materialization tests."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
import json
import threading
import unittest

import pandas as pd

from tradepilot import db
from tradepilot.etl.models import IngestionRequest, RunStatus
from tradepilot.etl.service import ETLService
from tradepilot.etl.sources.tushare import TushareSourceAdapter


class AdjFactorMockTushareClient:
    """Deterministic no-network client for ETF adjustment factor tests."""

    def __init__(self) -> None:
        self.etf_adj_factor_calls: list[str] = []
        self.etf_adj_factor_windows: list[tuple[str, str]] = []
        self.etf_daily_calls: list[str] = []
        self.etf_daily_windows: list[tuple[str, str]] = []

    def get_etf_daily(
        self, etf_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        self.etf_daily_calls.append(etf_code)
        self.etf_daily_windows.append((start_date, end_date))
        dates = pd.date_range(start_date, end_date, freq="D")
        return pd.DataFrame(
            {
                "date": dates,
                "etf_code": [etf_code] * len(dates),
                "open": [10.0, 10.5][: len(dates)],
                "high": [10.5, 11.0][: len(dates)],
                "low": [9.9, 10.4][: len(dates)],
                "close": [10.0, 11.0][: len(dates)],
                "pre_close": [9.9, 10.0][: len(dates)],
                "change": [0.1, 1.0][: len(dates)],
                "pct_chg": [1.010101, 10.0][: len(dates)],
                "volume": [1000.0, 1200.0][: len(dates)],
                "amount": [10000.0, 13200.0][: len(dates)],
            }
        )

    def get_etf_adj_factor(
        self, etf_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        self.etf_adj_factor_calls.append(etf_code)
        self.etf_adj_factor_windows.append((start_date, end_date))
        dates = pd.date_range(start_date, end_date, freq="D")
        return pd.DataFrame(
            {
                "date": dates,
                "etf_code": [etf_code] * len(dates),
                "adj_factor": [1.0 + idx * 0.01 for idx in range(len(dates))],
            }
        )


class StageCRebalanceCalendarTests(unittest.TestCase):
    """Verify deterministic rebalance calendar generation from canonical dates."""

    def setUp(self) -> None:
        self._original_db_path = db.DB_PATH
        self._original_thread_local = db._thread_local
        self._original_initialized = db._initialized
        self._temp_dir = TemporaryDirectory()
        db.DB_PATH = Path(self._temp_dir.name) / "test.duckdb"
        db._thread_local = threading.local()
        db._initialized = False
        self.conn = db.get_conn()
        self.service = ETLService(
            conn=self.conn,
            source_adapters=[],
            lakehouse_root=Path(self._temp_dir.name) / "lakehouse",
        )

    def tearDown(self) -> None:
        conn = getattr(db._thread_local, "conn", None)
        if conn is not None:
            conn.close()
        db._thread_local = self._original_thread_local
        db.DB_PATH = self._original_db_path
        db._initialized = self._original_initialized
        self._temp_dir.cleanup()

    def test_monthly_post_20_rebalance_calendar_is_materialized(self) -> None:
        self._insert_calendar_window(date(2024, 1, 20), date(2024, 2, 29))

        result = self.service.run_bootstrap(
            "reference.rebalance_calendar.monthly_post_20",
            start=date(2024, 1, 1),
            end=date(2024, 2, 29),
        )

        self.assertEqual(result["status"], RunStatus.SUCCESS.value)
        self.assertEqual(result["months_total"], 2)
        self.assertEqual(result["records_written"], 2)
        rows = self.conn.execute("""
            SELECT calendar_name, rebalance_date, effective_date, notes
            FROM canonical_rebalance_calendar
            ORDER BY rebalance_date
            """).fetchall()
        self.assertEqual(
            [(row[0], row[1], row[2]) for row in rows],
            [
                (
                    "etf_aw_v1_monthly_post_20",
                    date(2024, 1, 22),
                    date(2024, 1, 22),
                ),
                (
                    "etf_aw_v1_monthly_post_20",
                    date(2024, 2, 20),
                    date(2024, 2, 20),
                ),
            ],
        )
        self.assertEqual(json.loads(rows[0][3])["calendar_month"], "2024-01")
        self.assertEqual(
            json.loads(rows[0][3])["rule_name"],
            "first_common_open_day_on_or_after_20th",
        )

    def test_rebalance_calendar_requires_complete_sh_sz_calendar(self) -> None:
        self._insert_calendar_window(
            date(2024, 1, 20),
            date(2024, 1, 31),
            exchanges=("SH",),
        )

        result = self.service.run_bootstrap(
            "reference.rebalance_calendar.monthly_post_20",
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
        )

        self.assertEqual(result["status"], RunStatus.FAILED.value)
        self.assertEqual(result["months_total"], 1)
        self.assertEqual(result["records_written"], 0)
        self.assertEqual(
            result["missing_calendar_windows"],
            [
                {
                    "calendar_month": "2024-01",
                    "start": "2024-01-20",
                    "end": "2024-01-31",
                }
            ],
        )
        count = self.conn.execute(
            "SELECT COUNT(*) FROM canonical_rebalance_calendar"
        ).fetchone()[0]
        self.assertEqual(count, 0)

    def test_rebalance_calendar_repeat_run_is_idempotent(self) -> None:
        self._insert_calendar_window(date(2024, 1, 20), date(2024, 1, 31))

        first = self.service.run_bootstrap(
            "reference.rebalance_calendar.monthly_post_20",
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
        )
        second = self.service.run_bootstrap(
            "reference.rebalance_calendar.monthly_post_20",
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
        )

        self.assertEqual(first["status"], RunStatus.SUCCESS.value)
        self.assertEqual(second["status"], RunStatus.SUCCESS.value)
        self.assertEqual(second["duplicate_calendar_months"], 0)
        rows = self.conn.execute("""
            SELECT COUNT(*), COUNT(DISTINCT rebalance_date)
            FROM canonical_rebalance_calendar
            """).fetchone()
        self.assertEqual(rows, (1, 1))

    def test_frozen_etf_aw_sleeves_are_materialized(self) -> None:
        result = self.service.run_bootstrap("reference.etf_aw_sleeves.frozen_v1")

        self.assertEqual(result["status"], RunStatus.SUCCESS.value)
        self.assertEqual(result["records_written"], 5)
        self.assertEqual(
            result["sleeve_codes"],
            ["510300.SH", "159845.SZ", "511010.SH", "518850.SH", "159001.SZ"],
        )
        self.assertTrue(all(result["validation"].values()))
        rows = self.conn.execute("""
            SELECT sleeve_code, sleeve_role, sleeve_type, listing_exchange,
                   benchmark_name, exposure_note, is_active
            FROM canonical_sleeves
            ORDER BY sleeve_code
            """).fetchall()
        self.assertEqual(
            [(row[0], row[1], row[2], row[3], row[6]) for row in rows],
            [
                ("159001.SZ", "cash", "cash", "SZ", True),
                ("159845.SZ", "equity_small", "equity_small", "SZ", True),
                ("510300.SH", "equity_large", "equity_large", "SH", True),
                ("511010.SH", "bond", "bond", "SH", True),
                ("518850.SH", "gold", "gold", "SH", True),
            ],
        )
        self.assertTrue(all(row[4] for row in rows))
        self.assertTrue(all(row[5] for row in rows))

        instrument_count = self.conn.execute("""
            SELECT COUNT(*)
            FROM canonical_instruments
            WHERE instrument_id IN (
                '510300.SH', '159845.SZ', '511010.SH', '518850.SH', '159001.SZ'
            )
              AND instrument_type = 'etf'
            """).fetchone()[0]
        self.assertEqual(instrument_count, 5)

    def test_frozen_etf_aw_sleeves_do_not_include_old_bond_candidate(self) -> None:
        self.service.run_bootstrap("reference.etf_aw_sleeves.frozen_v1")

        count = self.conn.execute("""
            SELECT COUNT(*)
            FROM canonical_sleeves
            WHERE sleeve_code = '511020.SH'
            """).fetchone()[0]
        self.assertEqual(count, 0)

    def test_frozen_etf_aw_sleeves_do_not_overwrite_existing_instrument(self) -> None:
        self.conn.execute("""
            INSERT INTO canonical_instruments (
                instrument_id, source_instrument_id, instrument_name,
                instrument_type, exchange, is_active, source_name
            ) VALUES (
                '510300.SH', '510300.SH', 'Existing Tushare Name',
                'etf', 'SH', TRUE, 'tushare'
            )
            """)

        result = self.service.run_bootstrap("reference.etf_aw_sleeves.frozen_v1")

        self.assertEqual(result["status"], RunStatus.SUCCESS.value)
        instrument = self.conn.execute("""
            SELECT instrument_name, source_name
            FROM canonical_instruments
            WHERE instrument_id = '510300.SH'
            """).fetchone()
        self.assertEqual(instrument, ("Existing Tushare Name", "tushare"))

    def test_etf_adj_factor_sync_writes_normalized_partition(self) -> None:
        client = AdjFactorMockTushareClient()
        service = ETLService(
            conn=self.conn,
            source_adapters=[TushareSourceAdapter(client)],
            lakehouse_root=Path(self._temp_dir.name) / "adj-lakehouse",
        )
        self._insert_etf_instrument("510300.SH")
        self._insert_calendar_window(date(2024, 1, 22), date(2024, 1, 23))

        result = service.run_dataset_sync(
            "market.etf_adj_factor",
            IngestionRequest(
                request_start=date(2024, 1, 22),
                request_end=date(2024, 1, 23),
                context={"instrument_ids": ["510300.SH"]},
            ),
        )

        self.assertEqual(result.status, RunStatus.SUCCESS)
        self.assertTrue(result.watermark_updated)
        self.assertEqual(result.records_written, 2)
        self.assertEqual(client.etf_adj_factor_calls, ["510300.SH"])
        self.assertEqual(client.etf_adj_factor_windows, [("2024-01-22", "2024-01-23")])

        normalized_file = (
            Path(self._temp_dir.name)
            / "adj-lakehouse"
            / "normalized"
            / "market.etf_adj_factor"
            / "2024"
            / "01"
            / "part-00000.parquet"
        )
        self.assertTrue(normalized_file.exists())
        frame = pd.read_parquet(normalized_file)
        self.assertEqual(
            list(frame.columns),
            [
                "instrument_id",
                "trade_date",
                "adj_factor",
                "source_name",
                "raw_batch_id",
                "ingested_at",
                "quality_status",
            ],
        )
        self.assertEqual(frame["instrument_id"].tolist(), ["510300.SH", "510300.SH"])
        self.assertEqual(frame["adj_factor"].tolist(), [1.0, 1.01])

        watermark = self.conn.execute("""
            SELECT latest_fetched_date
            FROM etl_source_watermarks
            WHERE dataset_name = 'market.etf_adj_factor'
              AND source_name = 'tushare'
            """).fetchone()[0]
        self.assertEqual(watermark, date(2024, 1, 23))

    def test_etf_adj_factor_repeat_sync_updates_existing_keys(self) -> None:
        client = AdjFactorMockTushareClient()
        service = ETLService(
            conn=self.conn,
            source_adapters=[TushareSourceAdapter(client)],
            lakehouse_root=Path(self._temp_dir.name) / "adj-lakehouse",
        )
        self._insert_etf_instrument("510300.SH")
        self._insert_calendar_window(date(2024, 1, 22), date(2024, 1, 22))

        first = service.run_dataset_sync(
            "market.etf_adj_factor",
            IngestionRequest(
                request_start=date(2024, 1, 22),
                request_end=date(2024, 1, 22),
                context={"instrument_ids": ["510300.SH"]},
            ),
        )
        second = service.run_dataset_sync(
            "market.etf_adj_factor",
            IngestionRequest(
                request_start=date(2024, 1, 22),
                request_end=date(2024, 1, 22),
                context={"instrument_ids": ["510300.SH"]},
            ),
        )

        self.assertEqual(first.status, RunStatus.SUCCESS)
        self.assertEqual(second.status, RunStatus.SUCCESS)
        inserted, updated = self.conn.execute(
            """
            SELECT records_inserted, records_updated
            FROM etl_ingestion_runs
            WHERE run_id = ?
            """,
            [second.run_id],
        ).fetchone()
        self.assertEqual(inserted, 0)
        self.assertEqual(updated, 1)
        normalized_file = (
            Path(self._temp_dir.name)
            / "adj-lakehouse"
            / "normalized"
            / "market.etf_adj_factor"
            / "2024"
            / "01"
            / "part-00000.parquet"
        )
        frame = pd.read_parquet(normalized_file)
        self.assertEqual(len(frame), 1)

    def test_derived_etf_aw_sleeve_daily_builds_adjustment_aware_panel(self) -> None:
        client = AdjFactorMockTushareClient()
        service = ETLService(
            conn=self.conn,
            source_adapters=[TushareSourceAdapter(client)],
            lakehouse_root=Path(self._temp_dir.name) / "panel-lakehouse",
        )
        service.run_bootstrap("reference.etf_aw_sleeves.frozen_v1")
        self._insert_calendar_window(date(2024, 1, 22), date(2024, 1, 23))
        daily = service.run_dataset_sync(
            "market.etf_daily",
            IngestionRequest(
                request_start=date(2024, 1, 22),
                request_end=date(2024, 1, 23),
                context={"instrument_ids": ["510300.SH"]},
            ),
        )
        adj = service.run_dataset_sync(
            "market.etf_adj_factor",
            IngestionRequest(
                request_start=date(2024, 1, 22),
                request_end=date(2024, 1, 23),
                context={"instrument_ids": ["510300.SH"]},
            ),
        )

        result = service.run_bootstrap(
            "derived.etf_aw_sleeve_daily.build",
            start=date(2024, 1, 22),
            end=date(2024, 1, 23),
        )

        self.assertEqual(daily.status, RunStatus.SUCCESS)
        self.assertEqual(adj.status, RunStatus.SUCCESS)
        self.assertEqual(result["status"], RunStatus.SUCCESS.value)
        self.assertEqual(result["records_written"], 2)
        self.assertEqual(result["records_inserted"], 2)
        self.assertTrue(all(result["validation"].values()))

        derived_file = (
            Path(self._temp_dir.name)
            / "panel-lakehouse"
            / "derived"
            / "derived.etf_aw_sleeve_daily"
            / "2024"
            / "01"
            / "part-00000.parquet"
        )
        self.assertTrue(derived_file.exists())
        frame = pd.read_parquet(derived_file).sort_values("trade_date")
        self.assertEqual(frame["sleeve_code"].tolist(), ["510300.SH", "510300.SH"])
        self.assertEqual(
            frame["sleeve_role"].tolist(), ["equity_large", "equity_large"]
        )
        self.assertEqual(frame["adj_factor"].tolist(), [1.0, 1.01])
        self.assertEqual(frame["adj_close"].round(2).tolist(), [10.0, 11.11])
        self.assertTrue(pd.isna(frame["adj_pct_chg"].iloc[0]))
        self.assertAlmostEqual(frame["adj_pct_chg"].iloc[1], 11.1, places=6)

    def test_derived_etf_aw_sleeve_daily_requires_market_inputs(self) -> None:
        service = ETLService(
            conn=self.conn,
            source_adapters=[],
            lakehouse_root=Path(self._temp_dir.name) / "panel-lakehouse",
        )
        result = service.run_bootstrap(
            "derived.etf_aw_sleeve_daily.build",
            start=date(2024, 1, 22),
            end=date(2024, 1, 23),
        )

        self.assertEqual(result["status"], RunStatus.FAILED.value)
        self.assertEqual(
            result["missing_inputs"], ["market.etf_daily", "market.etf_adj_factor"]
        )

    def _insert_calendar_window(
        self,
        start: date,
        end: date,
        exchanges: tuple[str, ...] = ("SH", "SZ"),
    ) -> None:
        current = start
        rows: list[tuple[str, date, bool, date | None]] = []
        while current <= end:
            for exchange in exchanges:
                rows.append(
                    (
                        exchange,
                        current,
                        current.weekday() < 5,
                        current - timedelta(days=1),
                    )
                )
            current += timedelta(days=1)
        self.conn.executemany(
            """
            INSERT INTO canonical_trading_calendar (
                exchange, trade_date, is_open, pretrade_date
            ) VALUES (?, ?, ?, ?)
            """,
            rows,
        )

    def _insert_etf_instrument(self, instrument_id: str) -> None:
        exchange = instrument_id.rsplit(".", 1)[-1]
        self.conn.execute(
            """
            INSERT INTO canonical_instruments (
                instrument_id, source_instrument_id, instrument_name,
                instrument_type, exchange, is_active, source_name
            ) VALUES (?, ?, ?, 'etf', ?, TRUE, 'test')
            """,
            [instrument_id, instrument_id, instrument_id, exchange],
        )


if __name__ == "__main__":
    unittest.main()
