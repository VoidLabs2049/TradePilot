"""Stage B ETL executable path tests."""

from __future__ import annotations

from datetime import date
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import unittest

import pandas as pd

from tradepilot import db
from tradepilot.etl.datasets import DatasetDefinition
from tradepilot.etl.models import (
    DatasetCategory,
    DependencyType,
    IngestionRequest,
    RunStatus,
    SourceFetchResult,
    StorageZone,
    TriggerMode,
    ValidationStatus,
)
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

    def __init__(self) -> None:
        self.trade_calendar_calls: list[str] = []
        self.trade_calendar_windows: list[tuple[str, str]] = []
        self.etf_daily_calls: list[str] = []
        self.etf_daily_windows: list[tuple[str, str]] = []
        self.index_daily_calls: list[str] = []
        self.index_daily_windows: list[tuple[str, str]] = []

    def get_trade_calendar(
        self, start_date: str, end_date: str, exchange: str = "SSE"
    ) -> pd.DataFrame:
        self.trade_calendar_calls.append(exchange)
        self.trade_calendar_windows.append((start_date, end_date))
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
        self.etf_daily_calls.append(etf_code)
        self.etf_daily_windows.append((start_date, end_date))
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
        self.index_daily_calls.append(index_code)
        self.index_daily_windows.append((start_date, end_date))
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


class EmptyReferenceMockTushareClient(MockTushareClient):
    """Mock client that returns no reference rows."""

    def get_etf_catalog(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "code": pd.Series(dtype="object"),
                "name": pd.Series(dtype="object"),
                "list_date": pd.Series(dtype="object"),
                "delist_date": pd.Series(dtype="object"),
            }
        )

    def get_index_catalog(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "code": pd.Series(dtype="object"),
                "name": pd.Series(dtype="object"),
                "list_date": pd.Series(dtype="object"),
                "delist_date": pd.Series(dtype="object"),
            }
        )


class FullCalendarMockTushareClient(MockTushareClient):
    """Mock client that returns complete natural-day calendar windows."""

    def get_trade_calendar(
        self, start_date: str, end_date: str, exchange: str = "SSE"
    ) -> pd.DataFrame:
        self.trade_calendar_calls.append(exchange)
        self.trade_calendar_windows.append((start_date, end_date))
        dates = pd.date_range(start_date, end_date, freq="D")
        return pd.DataFrame(
            {
                "exchange": ["SH" if exchange == "SSE" else "SZ"] * len(dates),
                "trade_date": dates,
                "is_open": [True] * len(dates),
                "pretrade_date": dates - pd.Timedelta(days=1),
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

    def test_tushare_source_deduplicates_requests_and_records_endpoint(self) -> None:
        client = MockTushareClient()
        adapter = TushareSourceAdapter(client)

        calendar = adapter.fetch(
            "reference.trading_calendar",
            IngestionRequest(
                request_start=date(2026, 4, 24),
                request_end=date(2026, 4, 24),
                context={"exchanges": ["SH", "SSE", "SZ", "SZSE"]},
            ),
        )
        self.assertEqual(calendar.source_endpoint, "trade_cal")
        self.assertEqual(client.trade_calendar_calls, ["SSE", "SZSE"])

        instruments = adapter.fetch(
            "reference.instruments",
            IngestionRequest(context={"instrument_type": "etf"}),
        )
        self.assertEqual(instruments.source_endpoint, "fund_basic")

        adapter.fetch(
            "market.etf_daily",
            IngestionRequest(
                request_start=date(2026, 4, 24),
                request_end=date(2026, 4, 24),
                context={"instrument_ids": ["510300.SH", "510300.SH"]},
            ),
        )
        self.assertEqual(client.etf_daily_calls, ["510300.SH"])

    def test_tushare_source_normalizes_reversed_request_windows(self) -> None:
        client = MockTushareClient()
        adapter = TushareSourceAdapter(client)

        result = adapter.fetch(
            "market.etf_daily",
            IngestionRequest(
                request_start=date(2026, 4, 25),
                request_end=date(2026, 4, 24),
                context={"instrument_ids": ["510300.SH"]},
            ),
        )

        self.assertEqual(client.etf_daily_windows, [("2026-04-24", "2026-04-25")])
        self.assertEqual(result.window_start, date(2026, 4, 24))
        self.assertEqual(result.window_end, date(2026, 4, 25))

    def test_dependency_type_supports_freshness_checks(self) -> None:
        self.assertEqual(DependencyType.FRESHNESS.value, "freshness")

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
                        "code": ["510300", "5012011.SH"],
                        "name": ["沪深300ETF", "科创红土LOF(退市)"],
                        "instrument_type": ["etf", "etf"],
                    }
                ),
                {"source_name": "tushare"},
            )
            .canonical_payload
        )
        self.assertEqual(len(instruments), 1)
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

    def test_market_daily_validator_uses_instrument_exchange_for_calendar(self) -> None:
        payload = pd.DataFrame(
            {
                "instrument_id": ["159915.SZ"],
                "trade_date": [date(2026, 4, 24)],
                "open": [1.0],
                "high": [1.1],
                "low": [0.9],
                "close": [1.0],
                "pre_close": [1.0],
                "change": [0.0],
                "pct_chg": [0.0],
                "volume": [100.0],
                "amount": [100.0],
            }
        )
        results = MarketDailyValidator().validate(
            payload,
            {
                "dataset_name": "market.etf_daily",
                "run_id": 1,
                "instrument_type": "etf",
                "canonical_instruments": pd.DataFrame(
                    {
                        "instrument_id": ["159915.SZ"],
                        "instrument_type": ["etf"],
                        "exchange": ["SZ"],
                    }
                ),
                "canonical_trading_calendar": pd.DataFrame(
                    {
                        "exchange": ["SH"],
                        "trade_date": [date(2026, 4, 24)],
                    }
                ),
            },
        )

        self.assertTrue(
            any(
                result.check_name == "market_daily.trade_date_open"
                and result.status == ValidationStatus.FAIL
                for result in results
            )
        )

    def test_market_daily_validator_runs_price_consistency_rules(self) -> None:
        daily = pd.DataFrame(
            {
                "instrument_id": ["510300.SH"],
                "trade_date": [date(2026, 4, 24)],
                "open": [4.0],
                "high": [4.2],
                "low": [3.9],
                "close": [4.1],
                "pre_close": [4.0],
                "change": [0.2],
                "pct_chg": [1.0],
                "volume": [100.0],
                "amount": [400.0],
            }
        )

        results = MarketDailyValidator().validate(
            daily, {"dataset_name": "market.etf_daily", "run_id": 1}
        )

        self.assertTrue(
            any(
                result.check_name == "market_daily.change_consistency"
                and result.status == ValidationStatus.WARNING
                for result in results
            )
        )
        self.assertTrue(
            any(
                result.check_name == "market_daily.pct_chg_consistency"
                and result.status == ValidationStatus.WARNING
                for result in results
            )
        )

    def test_calendar_validator_runs_continuity_rules(self) -> None:
        calendar = pd.DataFrame(
            {
                "exchange": ["SH", "SH"],
                "trade_date": [date(2026, 4, 24), date(2026, 4, 26)],
                "is_open": [True, True],
                "pretrade_date": [date(2026, 4, 23), date(2026, 4, 24)],
            }
        )

        results = TradingCalendarValidator().validate(
            calendar,
            {"dataset_name": "reference.trading_calendar", "run_id": 1},
        )

        self.assertTrue(
            any(
                result.check_name == "calendar.date_continuity"
                and result.status == ValidationStatus.FAIL
                for result in results
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

        dependency_rows = self.conn.execute(
            """
            SELECT check_name, status, details_json
            FROM etl_validation_results
            WHERE run_id = ? AND check_level = 'dependency'
            ORDER BY check_name
            """,
            [result.run_id],
        ).fetchall()
        self.assertEqual(
            {row[0] for row in dependency_rows},
            {
                "dependency_preflight.snapshot_missing",
                "dependency_preflight.window_missing",
            },
        )
        self.assertTrue(
            all(json.loads(row[2])["auto_run_attempted"] for row in dependency_rows)
        )

        raw_batch_id = result.raw_batch_ids[0]
        raw_storage_path = self.conn.execute(
            "SELECT storage_path FROM etl_raw_batches WHERE raw_batch_id = ?",
            [raw_batch_id],
        ).fetchone()[0]
        self.assertTrue(raw_storage_path.endswith(f"batch-{raw_batch_id}.parquet"))

        normalized_file = (
            Path(self._temp_dir.name)
            / "lakehouse"
            / "normalized"
            / "market.etf_daily"
            / "2026"
            / "04"
            / "part-00000.parquet"
        )
        self.assertTrue(normalized_file.exists())
        self.assertEqual(
            [path.name for path in normalized_file.parent.glob("*.parquet")],
            ["part-00000.parquet"],
        )

    def test_calendar_window_dependency_requires_full_request_coverage(self) -> None:
        definition = self.service.registry.get_dataset("market.etf_daily")
        self.conn.execute("""
            INSERT INTO canonical_trading_calendar (
                exchange, trade_date, is_open, pretrade_date
            ) VALUES ('SH', DATE '2026-04-24', TRUE, DATE '2026-04-23')
            """)

        available = self.service._dependency_available(
            definition,
            "reference.trading_calendar",
            IngestionRequest(
                request_start=date(2026, 4, 24),
                request_end=date(2026, 4, 25),
            ),
        )

        self.assertFalse(available)

    def test_calendar_window_dependency_requires_requested_exchange(self) -> None:
        definition = self.service.registry.get_dataset("market.etf_daily")
        self.conn.execute("""
            INSERT INTO canonical_instruments (
                instrument_id, source_instrument_id, instrument_name,
                instrument_type, exchange, is_active, source_name
            ) VALUES (
                '159915.SZ', '159915.SZ', '创业板ETF',
                'etf', 'SZ', TRUE, 'tushare'
            )
            """)
        self.conn.execute("""
            INSERT INTO canonical_trading_calendar (
                exchange, trade_date, is_open, pretrade_date
            ) VALUES ('SH', DATE '2026-04-24', TRUE, DATE '2026-04-23')
            """)

        available = self.service._dependency_available(
            definition,
            "reference.trading_calendar",
            IngestionRequest(
                request_start=date(2026, 4, 24),
                request_end=date(2026, 4, 24),
                context={"instrument_ids": ["159915.SZ"]},
            ),
        )

        self.assertFalse(available)

    def test_empty_reference_payload_fails_without_watermark(self) -> None:
        service = ETLService(
            conn=self.conn,
            source_adapters=[TushareSourceAdapter(EmptyReferenceMockTushareClient())],
            lakehouse_root=Path(self._temp_dir.name) / "empty-lakehouse",
        )

        result = service.run_dataset_sync("reference.instruments", IngestionRequest())

        self.assertEqual(result.status, RunStatus.FAILED)
        empty_check_count = self.conn.execute(
            """
            SELECT COUNT(*) FROM etl_validation_results
            WHERE run_id = ?
              AND check_name = 'source_contract.empty_payload'
              AND status = 'fail'
            """,
            [result.run_id],
        ).fetchone()[0]
        watermark_count = self.conn.execute("""
            SELECT COUNT(*) FROM etl_source_watermarks
            WHERE dataset_name = 'reference.instruments'
            """).fetchone()[0]
        self.assertEqual(empty_check_count, 1)
        self.assertEqual(watermark_count, 0)

    def test_sequence_id_allocation_skips_existing_legacy_ids(self) -> None:
        self.conn.execute("""
            INSERT INTO etl_ingestion_runs (
                run_id, job_name, dataset_name, source_name,
                trigger_mode, status, started_at
            ) VALUES (
                50, 'legacy', 'reference.instruments', 'tushare',
                'manual', 'success', CURRENT_TIMESTAMP
            )
            """)

        result = self.service.run_dataset_sync(
            "reference.instruments",
            IngestionRequest(context={"instrument_type": "etf"}),
        )

        self.assertEqual(result.status, RunStatus.SUCCESS)
        self.assertGreater(result.run_id, 50)

    def test_freshness_dependency_checks_watermark_recency(self) -> None:
        definition = DatasetDefinition(
            dataset_name="market.test_daily",
            category=DatasetCategory.MARKET,
            grain="instrument_trade_date",
            primary_source="tushare",
            storage_zone=StorageZone.NORMALIZED,
        )
        self.conn.execute("""
            INSERT INTO etl_source_watermarks (
                dataset_name, source_name, latest_available_date,
                latest_fetched_date, latest_successful_run_id, updated_at
            ) VALUES (
                'reference.instruments', 'tushare',
                DATE '2026-04-23', DATE '2026-04-23', 1, CURRENT_TIMESTAMP
            )
            """)

        stale = self.service._dependency_available(
            definition,
            "reference.instruments",
            IngestionRequest(request_end=date(2026, 4, 24)),
            DependencyType.FRESHNESS,
        )
        fresh_with_grace = self.service._dependency_available(
            definition,
            "reference.instruments",
            IngestionRequest(
                request_end=date(2026, 4, 24),
                context={"freshness_max_age_days": 1},
            ),
            DependencyType.FRESHNESS,
        )

        self.assertFalse(stale)
        self.assertTrue(fresh_with_grace)

    def test_market_rewrite_reports_updated_records(self) -> None:
        first = self.service.run_dataset_sync(
            "market.etf_daily",
            IngestionRequest(
                request_start=date(2026, 4, 24),
                request_end=date(2026, 4, 24),
                context={"instrument_ids": ["510300.SH"]},
            ),
        )
        second = self.service.run_dataset_sync(
            "market.etf_daily",
            IngestionRequest(
                request_start=date(2026, 4, 24),
                request_end=date(2026, 4, 24),
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

    def test_calendar_full_history_bootstrap_runs_monthly_backfill(self) -> None:
        client = FullCalendarMockTushareClient()
        service = ETLService(
            conn=self.conn,
            source_adapters=[TushareSourceAdapter(client)],
            lakehouse_root=Path(self._temp_dir.name) / "bootstrap-lakehouse",
        )

        result = service.run_bootstrap(
            "reference.trading_calendar.full_history",
            start=date(2026, 1, 30),
            end=date(2026, 2, 2),
        )

        self.assertEqual(result["status"], RunStatus.SUCCESS.value)
        self.assertEqual(result["windows_total"], 2)
        self.assertEqual(result["windows_processed"], 2)
        self.assertEqual(result["windows_skipped"], 0)
        self.assertTrue(result["final_coverage_ok"])
        self.assertEqual(result["duplicate_business_keys"], 0)
        self.assertTrue(result["final_validation_passed"])
        self.assertEqual(
            client.trade_calendar_windows,
            [
                ("2026-01-30", "2026-01-31"),
                ("2026-01-30", "2026-01-31"),
                ("2026-02-01", "2026-02-02"),
                ("2026-02-01", "2026-02-02"),
            ],
        )
        self.assertEqual(client.trade_calendar_calls, ["SSE", "SZSE", "SSE", "SZSE"])

        trigger_modes = self.conn.execute("""
            SELECT DISTINCT trigger_mode
            FROM etl_ingestion_runs
            WHERE dataset_name = 'reference.trading_calendar'
            """).fetchall()
        self.assertEqual(trigger_modes, [(TriggerMode.BACKFILL.value,)])

        covered_rows = self.conn.execute("""
            SELECT COUNT(*)
            FROM canonical_trading_calendar
            WHERE trade_date BETWEEN DATE '2026-01-30' AND DATE '2026-02-02'
              AND exchange IN ('SH', 'SZ')
            """).fetchone()[0]
        self.assertEqual(covered_rows, 8)

    def test_calendar_bootstrap_skips_complete_windows_and_keeps_watermark(
        self,
    ) -> None:
        client = FullCalendarMockTushareClient()
        service = ETLService(
            conn=self.conn,
            source_adapters=[TushareSourceAdapter(client)],
            lakehouse_root=Path(self._temp_dir.name) / "bootstrap-lakehouse",
        )
        self.conn.execute("""
            INSERT INTO etl_source_watermarks (
                dataset_name, source_name, latest_available_date,
                latest_fetched_date, latest_successful_run_id, updated_at
            ) VALUES (
                'reference.trading_calendar', 'tushare',
                DATE '2026-03-31', DATE '2026-03-31', 99, CURRENT_TIMESTAMP
            )
            """)

        first = service.run_bootstrap(
            "reference.trading_calendar.full_history",
            start=date(2026, 1, 1),
            end=date(2026, 1, 2),
        )
        count_after_first = self.conn.execute(
            "SELECT COUNT(*) FROM canonical_trading_calendar"
        ).fetchone()[0]
        watermark_after_first = self.conn.execute("""
            SELECT latest_fetched_date, latest_successful_run_id
            FROM etl_source_watermarks
            WHERE dataset_name = 'reference.trading_calendar'
              AND source_name = 'tushare'
            """).fetchone()

        second = service.run_bootstrap(
            "reference.trading_calendar.full_history",
            start=date(2026, 1, 1),
            end=date(2026, 1, 2),
        )
        count_after_second = self.conn.execute(
            "SELECT COUNT(*) FROM canonical_trading_calendar"
        ).fetchone()[0]

        self.assertEqual(first["status"], RunStatus.SUCCESS.value)
        self.assertEqual(second["status"], RunStatus.SUCCESS.value)
        self.assertEqual(second["windows_processed"], 0)
        self.assertEqual(second["windows_skipped"], 1)
        self.assertEqual(count_after_first, 4)
        self.assertEqual(count_after_second, count_after_first)
        self.assertEqual(watermark_after_first, (date(2026, 3, 31), 99))


if __name__ == "__main__":
    unittest.main()
