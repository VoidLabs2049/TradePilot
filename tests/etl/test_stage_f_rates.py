"""Stage F rates dataset tests."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import unittest

import pandas as pd

from tradepilot import db
from tradepilot.etl.models import IngestionRequest, RunStatus, StorageZone
from tradepilot.etl.normalizers import LprNormalizer
from tradepilot.etl.service import ETLService
from tradepilot.etl.sources.tushare import TushareSourceAdapter
from tradepilot.etl.storage import build_dataset_file_path
from tradepilot.etl.validators import DailyRatesValidator, has_blocking_failures


class StageFRatesMockTushareClient:
    """Deterministic no-network rates client for Stage F tests."""

    def __init__(self) -> None:
        self.shibor_windows: list[tuple[str, str]] = []
        self.lpr_windows: list[tuple[str, str]] = []

    def get_shibor(self, start_date: str, end_date: str) -> pd.DataFrame:
        self.shibor_windows.append((start_date, end_date))
        return pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-04-20")],
                "1w": [1.85],
                "on": [1.72],
            }
        )

    def get_lpr(self, start_date: str, end_date: str) -> pd.DataFrame:
        self.lpr_windows.append((start_date, end_date))
        return pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-04-20")],
                "1y": [3.10],
                "5y": [3.60],
            }
        )


class StageFRatesTests(unittest.TestCase):
    """Verify Stage F daily rates and LPR first slice."""

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
        self.client = StageFRatesMockTushareClient()
        self.service = ETLService(
            conn=self.conn,
            source_adapters=[TushareSourceAdapter(self.client)],
            lakehouse_root=self.lakehouse_root,
        )

    def tearDown(self) -> None:
        conn = getattr(db._thread_local, "conn", None)
        if conn is not None:
            conn.close()
        db._thread_local = self._original_thread_local
        db.DB_PATH = self._original_db_path
        db._initialized = self._original_initialized
        self._temp_dir.cleanup()

    def test_rates_datasets_are_registered(self) -> None:
        self.assertTrue(self.service.registry.has_dataset("rates.daily_rates"))
        self.assertTrue(self.service.registry.has_dataset("rates.lpr"))

    def test_daily_rates_run_through_source_to_canonical_write(self) -> None:
        self._insert_calendar_window(date(2026, 4, 20), date(2026, 4, 20))

        result = self.service.run_dataset_sync(
            "rates.daily_rates",
            IngestionRequest(
                request_start=date(2026, 4, 20),
                request_end=date(2026, 4, 20),
            ),
        )

        self.assertEqual(result.status, RunStatus.SUCCESS)
        self.assertEqual(result.records_written, 2)
        self.assertEqual(result.validation_counts["pass_with_caveat"], 1)
        self.assertEqual(self.client.shibor_windows, [("2026-04-20", "2026-04-20")])
        path = build_dataset_file_path(
            "rates.daily_rates",
            StorageZone.NORMALIZED,
            [("year", 2026), ("month", "04")],
            lakehouse_root=self.lakehouse_root,
        )
        frame = pd.read_parquet(path).sort_values("field_name").reset_index(drop=True)
        self.assertEqual(
            frame["field_name"].tolist(), ["shibor_1w", "shibor_overnight"]
        )
        self.assertEqual(
            frame.loc[frame["field_name"].eq("shibor_1w"), "unit"].iloc[0],
            "percent",
        )
        self.assertTrue(all(frame["quality_status"].eq("pass_with_caveat")))
        watermark = self.conn.execute("""
            SELECT latest_fetched_date
            FROM etl_source_watermarks
            WHERE dataset_name = 'rates.daily_rates'
              AND source_name = 'tushare'
            """).fetchone()
        self.assertEqual(watermark[0], date(2026, 4, 20))

    def test_lpr_run_uses_effective_date_and_quote_watermark(self) -> None:
        self._insert_calendar_window(date(2026, 4, 20), date(2026, 4, 20))

        result = self.service.run_dataset_sync(
            "rates.lpr",
            IngestionRequest(
                request_start=date(2026, 4, 20),
                request_end=date(2026, 4, 20),
            ),
        )

        self.assertEqual(result.status, RunStatus.SUCCESS)
        self.assertEqual(result.records_written, 2)
        self.assertEqual(result.validation_counts["pass_with_caveat"], 1)
        self.assertEqual(self.client.lpr_windows, [("2026-04-20", "2026-04-20")])
        path = build_dataset_file_path(
            "rates.lpr",
            StorageZone.NORMALIZED,
            [("year", 2026), ("month", "04")],
            lakehouse_root=self.lakehouse_root,
        )
        frame = pd.read_parquet(path).sort_values("field_name").reset_index(drop=True)
        self.assertEqual(frame["field_name"].tolist(), ["lpr_1y", "lpr_5y"])
        self.assertEqual(
            pd.to_datetime(frame["quote_date"]).dt.date.tolist(),
            [date(2026, 4, 20)] * 2,
        )
        self.assertEqual(
            pd.to_datetime(frame["effective_date"]).dt.date.tolist(),
            [date(2026, 4, 20)] * 2,
        )
        watermark = self.conn.execute("""
            SELECT latest_fetched_date
            FROM etl_source_watermarks
            WHERE dataset_name = 'rates.lpr'
              AND source_name = 'tushare'
            """).fetchone()
        self.assertEqual(watermark[0], date(2026, 4, 20))

    def test_lpr_normalizer_falls_back_to_day_20_and_next_open_day(self) -> None:
        calendar = pd.DataFrame(
            {
                "exchange": ["SH", "SZ", "SH", "SZ"],
                "trade_date": [
                    date(2026, 5, 20),
                    date(2026, 5, 20),
                    date(2026, 5, 21),
                    date(2026, 5, 21),
                ],
                "is_open": [False, False, True, True],
            }
        )

        canonical = (
            LprNormalizer()
            .normalize(
                pd.DataFrame({"period_label": ["2026-05"], "1y": [3.0]}),
                {
                    "source_name": "tushare",
                    "raw_batch_id": 9,
                    "canonical_trading_calendar": calendar,
                },
            )
            .canonical_payload
        )

        self.assertEqual(canonical.iloc[0]["quote_date"], date(2026, 5, 20))
        self.assertEqual(canonical.iloc[0]["release_date"], date(2026, 5, 20))
        self.assertEqual(canonical.iloc[0]["effective_date"], date(2026, 5, 21))
        self.assertEqual(
            canonical.iloc[0]["source_caveat"],
            "tushare_wrapper_source_date_inferred_month_20",
        )

    def test_daily_rates_validator_blocks_duplicate_keys(self) -> None:
        payload = pd.DataFrame(
            {
                "field_name": ["shibor_1w", "shibor_1w"],
                "trade_date": [date(2026, 4, 20), date(2026, 4, 20)],
                "value": [1.8, 1.9],
                "unit": ["percent", "percent"],
                "field_role": ["primary", "primary"],
                "release_date": [date(2026, 4, 20), date(2026, 4, 20)],
                "effective_date": [date(2026, 4, 20), date(2026, 4, 20)],
                "revision_note": ["low_revision_risk", "low_revision_risk"],
                "source_caveat": ["wrapper", "wrapper"],
            }
        )

        results = DailyRatesValidator().validate(
            payload,
            {"dataset_name": "rates.daily_rates", "run_id": 1},
        )

        self.assertTrue(has_blocking_failures(results))

    def _insert_calendar_window(self, start: date, end: date) -> None:
        dates = pd.date_range(start, end, freq="D")
        rows = []
        for value in dates:
            for exchange in ("SH", "SZ"):
                rows.append(
                    {
                        "exchange": exchange,
                        "trade_date": value.date(),
                        "is_open": True,
                        "pretrade_date": None,
                        "updated_at": pd.Timestamp("2026-01-01"),
                    }
                )
        self.conn.register("stage_f_calendar_rows", pd.DataFrame(rows))
        try:
            self.conn.execute("""
                INSERT INTO canonical_trading_calendar (
                    exchange, trade_date, is_open, pretrade_date, updated_at
                )
                SELECT exchange, trade_date, is_open, pretrade_date, updated_at
                FROM stage_f_calendar_rows
                """)
        finally:
            self.conn.unregister("stage_f_calendar_rows")


if __name__ == "__main__":
    unittest.main()
