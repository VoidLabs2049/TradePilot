"""Futures market data ingestion tests."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import unittest

import pandas as pd

from tradepilot import db
from tradepilot.data.tushare_client import TushareClient
from tradepilot.etl.models import IngestionRequest, RunStatus
from tradepilot.etl.service import ETLService
from tradepilot.etl.sources.tushare import TushareSourceAdapter
from tradepilot.etl.validators import (
    FuturesContractDailyValidator,
    has_blocking_failures,
)


class FakeTusharePro:
    """Record futures endpoint calls and return deterministic source rows."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, str]]] = []

    def fut_basic(self, **kwargs: str) -> pd.DataFrame:
        self.calls.append(("fut_basic", kwargs))
        return pd.DataFrame(
            {
                "ts_code": ["M2609.DCE"],
                "symbol": ["M2609"],
                "exchange": ["DCE"],
                "name": ["豆粕2609"],
                "fut_code": ["M"],
                "multiplier": ["10"],
                "trade_unit": ["10吨/手"],
                "per_unit": ["1"],
                "quote_unit": ["元（人民币）/吨"],
                "list_date": ["20250915"],
                "delist_date": ["20260914"],
            }
        )

    def fut_mapping(self, **kwargs: str) -> pd.DataFrame:
        self.calls.append(("fut_mapping", kwargs))
        return pd.DataFrame(
            {
                "ts_code": ["M.DCE", "M.DCE"],
                "trade_date": ["20260408", "20260409"],
                "mapping_ts_code": ["M2605.DCE", "M2609.DCE"],
            }
        )

    def fut_daily(self, **kwargs: str) -> pd.DataFrame:
        self.calls.append(("fut_daily", kwargs))
        code = kwargs["ts_code"]
        return pd.DataFrame(
            {
                "ts_code": [code],
                "trade_date": ["20260409"],
                "pre_close": [2960.0],
                "pre_settle": [2958.0],
                "open": [2962.0],
                "high": [2980.0],
                "low": [2955.0],
                "close": [2971.0],
                "settle": [2968.0],
                "change1": [13.0],
                "change2": [10.0],
                "vol": [1000.0],
                "amount": [296800.0],
                "oi": [1990000.0],
                "oi_chg": [310000.0],
            }
        )


class FuturesIngestionTests(unittest.TestCase):
    """Verify futures endpoints and lakehouse ingestion behavior."""

    def setUp(self) -> None:
        self._original_db_path = db.DB_PATH
        self._original_thread_local = db._thread_local
        self._original_initialized = db._initialized
        self._temp_dir = TemporaryDirectory()
        db.DB_PATH = Path(self._temp_dir.name) / "test.duckdb"
        db._thread_local = threading.local()
        db._initialized = False
        self.conn = db.get_conn()
        self.pro = FakeTusharePro()
        self.client = TushareClient()
        self.client._pro = self.pro
        self.lakehouse_root = Path(self._temp_dir.name) / "lakehouse"
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

    def test_client_normalizes_futures_endpoints(self) -> None:
        basic = self.client.get_futures_basic("dce")
        mapping = self.client.get_futures_mapping("m.dce", "2026-04-08", "2026-04-09")
        daily = self.client.get_futures_daily("m2609.dce", "2026-04-09", "2026-04-09")

        self.assertEqual(basic.iloc[0]["contract_code"], "M2609.DCE")
        self.assertEqual(basic.iloc[0]["multiplier"], 10)
        self.assertEqual(
            mapping["active_contract"].tolist(), ["M2605.DCE", "M2609.DCE"]
        )
        self.assertEqual(daily.iloc[0]["contract_code"], "M2609.DCE")
        self.assertEqual(daily.iloc[0]["volume"], 1000.0)
        self.assertEqual(self.pro.calls[1][1]["ts_code"], "M.DCE")
        self.assertEqual(self.pro.calls[2][1]["ts_code"], "M2609.DCE")

    def test_m_dce_mapping_and_contract_daily_are_written(self) -> None:
        window = IngestionRequest(
            request_start=date(2026, 4, 8),
            request_end=date(2026, 4, 9),
            context={"root_codes": ["M.DCE"]},
        )
        mapping_result = self.service.run_dataset_sync("market.futures_mapping", window)
        daily_result = self.service.run_dataset_sync(
            "market.futures_contract_daily",
            IngestionRequest(
                request_start=date(2026, 4, 8),
                request_end=date(2026, 4, 9),
                context={"contract_codes": ["M2605.DCE", "M2609.DCE"]},
            ),
        )

        self.assertEqual(mapping_result.status, RunStatus.SUCCESS)
        self.assertEqual(mapping_result.records_written, 2)
        self.assertEqual(daily_result.status, RunStatus.SUCCESS)
        self.assertEqual(daily_result.records_written, 2)

        mapping_path = (
            self.lakehouse_root
            / "normalized"
            / "market.futures_mapping"
            / "2026"
            / "04"
            / "part-00000.parquet"
        )
        daily_path = (
            self.lakehouse_root
            / "normalized"
            / "market.futures_contract_daily"
            / "2026"
            / "04"
            / "part-00000.parquet"
        )
        self.assertTrue(mapping_path.exists())
        self.assertTrue(daily_path.exists())
        mapping = pd.read_parquet(mapping_path)
        daily = pd.read_parquet(daily_path)
        self.assertEqual(mapping.iloc[-1]["active_contract"], "M2609.DCE")
        self.assertEqual(set(daily["contract_code"]), {"M2605.DCE", "M2609.DCE"})
        self.assertIn("settle", daily.columns)
        self.assertIn("oi", daily.columns)

        runs = self.conn.execute(
            """
            SELECT dataset_name, status
            FROM etl_ingestion_runs
            WHERE dataset_name LIKE 'market.futures%'
            ORDER BY run_id
            """
        ).fetchall()
        self.assertEqual(
            runs,
            [
                ("market.futures_mapping", "success"),
                ("market.futures_contract_daily", "success"),
            ],
        )

    def test_missing_historical_settle_is_a_non_blocking_warning(self) -> None:
        payload = self.client.get_futures_daily("M2609.DCE", "2026-04-09", "2026-04-09")
        payload["settle"] = float("nan")

        results = FuturesContractDailyValidator().validate(payload)

        settle_result = next(
            result
            for result in results
            if result.check_name == "futures_daily.settle_availability"
        )
        self.assertEqual(settle_result.status.value, "warning")
        self.assertFalse(has_blocking_failures(results))


if __name__ == "__main__":
    unittest.main()
