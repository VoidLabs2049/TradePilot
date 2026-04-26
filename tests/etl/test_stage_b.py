"""Stage B ETL executable path tests."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import unittest

import pandas as pd

from tradepilot import db
from tradepilot.etl.models import IngestionRequest, RunStatus, SourceFetchResult
from tradepilot.etl.normalizers import (
    InstrumentNormalizer,
    MarketDailyNormalizer,
    TradingCalendarNormalizer,
)
from tradepilot.etl.service import ETLService
from tradepilot.etl.sources.tushare import TushareSourceAdapter
from tradepilot.etl.validators import (
    InstrumentValidator,
    MarketDailyValidator,
    TradingCalendarValidator,
    has_blocking_failures,
)


class MockTushareClient:
    """Deterministic no-network Tushare client for Stage B tests."""

    def get_trade_calendar(
        self, start_date: str, end_date: str, exchange: str = "SSE"
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "exchange": ["SH" if exchange == "SSE" else "SZ"],
                "trade_date": [pd.Timestamp("2026-04-24")],
                "is_open": [True],
                "pretrade_date": [pd.Timestamp("2026-04-23")],
            }
        )

    def get_etf_catalog(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "code": ["510300.SH"],
                "name": ["沪深300ETF"],
                "list_date": ["20120528"],
                "delist_date": [None],
            }
        )

    def get_index_catalog(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "code": ["000300"],
                "name": ["沪深300"],
                "list_date": ["20050408"],
                "delist_date": [None],
            }
        )

    def get_etf_daily(
        self, etf_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-04-24")],
                "etf_code": [etf_code],
                "open": [4.0],
                "high": [4.2],
                "low": [3.9],
                "close": [4.1],
                "pre_close": [4.0],
                "change": [0.1],
                "pct_chg": [2.5],
                "volume": [1000.0],
                "amount": [4100.0],
            }
        )

    def get_index_daily(
        self, index_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-04-24")],
                "index_code": [index_code],
                "open": [4000.0],
                "high": [4100.0],
                "low": [3990.0],
                "close": [4050.0],
                "volume": [1000.0],
                "amount": [4100.0],
            }
        )


class StageBSourceNormalizerValidatorTests(unittest.TestCase):
    """Verify Stage B source, normalizer, and validator contracts."""

    def test_tushare_source_returns_typed_result(self) -> None:
        adapter = TushareSourceAdapter(MockTushareClient())

        result = adapter.fetch(
            "reference.trading_calendar",
            IngestionRequest(
                request_start=date(2026, 4, 24),
                request_end=date(2026, 4, 24),
                context={"exchange": "SH"},
            ),
        )

        self.assertIsInstance(result, SourceFetchResult)
        self.assertEqual(result.dataset_name, "reference.trading_calendar")
        self.assertEqual(result.row_count, len(result.payload))
        self.assertIsInstance(result.payload, pd.DataFrame)

    def test_normalizers_emit_canonical_fields(self) -> None:
        calendar = (
            TradingCalendarNormalizer()
            .normalize(
                pd.DataFrame(
                    {
                        "exchange": ["SSE"],
                        "cal_date": ["20260424"],
                        "is_open": [1],
                        "pretrade_date": ["20260423"],
                    }
                )
            )
            .canonical_payload
        )
        self.assertEqual(
            list(calendar.columns),
            ["exchange", "trade_date", "is_open", "pretrade_date"],
        )
        self.assertEqual(calendar.iloc[0]["exchange"], "SH")

        instruments = (
            InstrumentNormalizer()
            .normalize(
                pd.DataFrame(
                    {
                        "code": ["510300"],
                        "name": ["沪深300ETF"],
                        "instrument_type": ["etf"],
                    }
                ),
                {"source_name": "tushare"},
            )
            .canonical_payload
        )
        self.assertEqual(instruments.iloc[0]["instrument_id"], "510300.SH")

        daily = (
            MarketDailyNormalizer()
            .normalize(
                pd.DataFrame(
                    {"date": ["2026-04-24"], "etf_code": ["510300.SH"], "close": [4.1]}
                ),
                {"source_name": "tushare", "raw_batch_id": 7, "instrument_type": "etf"},
            )
            .canonical_payload
        )
        self.assertIn("quality_status", daily.columns)
        self.assertEqual(daily.iloc[0]["raw_batch_id"], 7)

    def test_validators_block_bad_data(self) -> None:
        calendar = pd.DataFrame(
            {
                "exchange": ["SH", "SH"],
                "trade_date": [date(2026, 4, 24), date(2026, 4, 24)],
                "is_open": [True, True],
                "pretrade_date": [date(2026, 4, 23), date(2026, 4, 23)],
            }
        )
        self.assertTrue(
            has_blocking_failures(
                TradingCalendarValidator().validate(
                    calendar,
                    {"dataset_name": "reference.trading_calendar", "run_id": 1},
                )
            )
        )

        instruments = pd.DataFrame(
            {
                "instrument_id": ["510300"],
                "source_instrument_id": ["510300"],
                "instrument_name": ["沪深300ETF"],
                "instrument_type": ["etf"],
                "exchange": ["SH"],
                "list_date": [None],
                "delist_date": [None],
                "is_active": [True],
                "source_name": ["tushare"],
            }
        )
        self.assertTrue(
            has_blocking_failures(
                InstrumentValidator().validate(
                    instruments, {"dataset_name": "reference.instruments", "run_id": 1}
                )
            )
        )

        daily = pd.DataFrame(
            {
                "instrument_id": ["510300.SH"],
                "trade_date": [date(2026, 4, 24)],
                "open": [4.0],
                "high": [3.9],
                "low": [4.1],
                "close": [4.0],
                "pre_close": [4.0],
                "change": [0.0],
                "pct_chg": [0.0],
                "volume": [100.0],
                "amount": [400.0],
            }
        )
        self.assertTrue(
            has_blocking_failures(
                MarketDailyValidator().validate(
                    daily, {"dataset_name": "market.etf_daily", "run_id": 1}
                )
            )
        )


class StageBServiceIntegrationTests(unittest.TestCase):
    """Verify the first executable Stage B ETL vertical slice."""

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
            source_adapters=[TushareSourceAdapter(MockTushareClient())],
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

    def test_run_market_dataset_autofills_dependencies_and_writes_outputs(self) -> None:
        result = self.service.run_dataset_sync(
            "market.etf_daily",
            IngestionRequest(
                request_start=date(2026, 4, 24),
                request_end=date(2026, 4, 24),
                context={"instrument_ids": ["510300.SH"]},
            ),
        )

        self.assertEqual(result.status, RunStatus.SUCCESS)
        self.assertTrue(result.watermark_updated)
        self.assertGreaterEqual(len(result.raw_batch_ids), 1)

        instrument_count = self.conn.execute(
            "SELECT COUNT(*) FROM canonical_instruments WHERE instrument_id = '510300.SH'"
        ).fetchone()[0]
        calendar_count = self.conn.execute(
            "SELECT COUNT(*) FROM canonical_trading_calendar WHERE trade_date = DATE '2026-04-24'"
        ).fetchone()[0]
        validation_count = self.conn.execute(
            "SELECT COUNT(*) FROM etl_validation_results WHERE run_id = ?",
            [result.run_id],
        ).fetchone()[0]

        self.assertEqual(instrument_count, 1)
        self.assertGreaterEqual(calendar_count, 1)
        self.assertGreater(validation_count, 0)
        normalized_file = (
            Path(self._temp_dir.name)
            / "lakehouse"
            / "normalized"
            / "market.etf_daily"
            / "year=2026"
            / "month=04"
            / "part-00000.parquet"
        )
        self.assertTrue(normalized_file.exists())


if __name__ == "__main__":
    unittest.main()
