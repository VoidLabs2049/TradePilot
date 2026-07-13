"""Stage F rates dataset tests."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import unittest

import pandas as pd

from tradepilot import db
from tradepilot.data.tushare_client import TushareClient
from tradepilot.etl.models import (
    IngestionRequest,
    RunStatus,
    StorageZone,
    ValidationStatus,
)
from tradepilot.etl.normalizers import LprNormalizer
from tradepilot.etl.service import ETLService
from tradepilot.etl.sources.tushare import TushareSourceAdapter
from tradepilot.etl.storage import build_dataset_file_path
from tradepilot.etl.validators import (
    DailyRatesValidator,
    GovCurvePointsValidator,
    LprValidator,
    MacroSlowFieldsValidator,
    has_blocking_failures,
)


class StageFRatesMockTushareClient:
    """Deterministic no-network rates client for Stage F tests."""

    def __init__(self) -> None:
        self.shibor_windows: list[tuple[str, str]] = []
        self.lpr_windows: list[tuple[str, str]] = []
        self.macro_windows: list[tuple[str, str]] = []
        self.curve_windows: list[tuple[str, str]] = []

    def get_shibor(self, start_date: str, end_date: str) -> pd.DataFrame:
        self.shibor_windows.append((start_date, end_date))
        return pd.DataFrame({
            "date": [pd.Timestamp(start_date)],
            "1w": [1.85],
            "on": [1.72],
        })

    def get_lpr(self, start_date: str, end_date: str) -> pd.DataFrame:
        self.lpr_windows.append((start_date, end_date))
        return pd.DataFrame({
            "date": [pd.Timestamp(start_date)],
            "1y": [3.10],
            "5y": [3.60],
        })

    def get_macro_slow_fields(self, start_date: str, end_date: str) -> pd.DataFrame:
        self.macro_windows.append((start_date, end_date))
        return pd.DataFrame({
            "period_label": ["2026-03"],
            "official_pmi": [50.8],
        })

    def get_gov_curve_points(self, start_date: str, end_date: str) -> pd.DataFrame:
        self.curve_windows.append((start_date, end_date))
        return pd.DataFrame({
            "curve_date": [pd.Timestamp(start_date)],
            "curve_code": ["cn_gov_bond"],
            "1y": [1.55],
            "10y": [2.35],
        })


class StageFRatesPermissionDeniedCurveClient(StageFRatesMockTushareClient):
    """Rates client that simulates missing Tushare yc_cb permission."""

    def get_gov_curve_points(self, start_date: str, end_date: str) -> pd.DataFrame:
        self.curve_windows.append((start_date, end_date))
        raise RuntimeError("Tushare yc_cb 接口无访问权限")


class StageFRatesMockAkShare:
    """Deterministic AKShare curve fallback fixture."""

    def __init__(self) -> None:
        self.curve_calls: list[dict[str, str]] = []

    def bond_china_yield(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Return ChinaBond curve rows matching AKShare bond_china_yield."""

        self.curve_calls.append({"start_date": start_date, "end_date": end_date})
        return pd.DataFrame({
            "曲线名称": [
                "中债国债收益率曲线",
                "中债商业银行普通债收益率曲线(AAA)",
                "中债中短期票据收益率曲线(AAA)",
            ],
            "日期": ["2026-04-20", "2026-04-20", "2026-04-20"],
            "1年": [1.55, 9.99, 8.88],
            "10年": [2.35, 9.98, 8.87],
        })


class StageFRatesRawTusharePro:
    """Raw Tushare pro fixture for provider boundary parsing tests."""

    def __init__(self) -> None:
        self.curve_calls: list[dict[str, str]] = []

    def yc_cb(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Return long-format government curve rows matching Tushare yc_cb."""

        self.curve_calls.append({"start_date": start_date, "end_date": end_date})
        return pd.DataFrame({
            "trade_date": ["20260420", "20260420", "20260420", "20260420"],
            "ts_code": ["1001.CB", "1001.CB", "1002.CB", "1001.CB"],
            "curve_type": ["0", "0.0", "0", "1"],
            "curve_term": [1.0, 10.0, 1.0, 10.0],
            "yield": [1.55, 2.35, 9.99, 8.88],
        })


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
        self.assertTrue(self.service.registry.has_dataset("macro.slow_fields"))
        self.assertTrue(self.service.registry.has_dataset("rates.daily_rates"))
        self.assertTrue(self.service.registry.has_dataset("rates.lpr"))
        self.assertTrue(self.service.registry.has_dataset("rates.gov_curve_points"))

    def test_macro_slow_fields_run_through_source_to_canonical_write(self) -> None:
        self._insert_calendar_window(date(2026, 3, 1), date(2026, 4, 1))

        result = self.service.run_dataset_sync(
            "macro.slow_fields",
            IngestionRequest(
                request_start=date(2026, 3, 1),
                request_end=date(2026, 3, 31),
            ),
        )

        self.assertEqual(result.status, RunStatus.SUCCESS)
        self.assertEqual(result.records_written, 1)
        self.assertEqual(result.validation_counts["pass_with_caveat"], 1)
        self.assertEqual(self.client.macro_windows, [("2026-03-01", "2026-03-31")])
        path = build_dataset_file_path(
            "macro.slow_fields",
            StorageZone.NORMALIZED,
            [("year", 2026), ("month", "04")],
            lakehouse_root=self.lakehouse_root,
        )
        frame = pd.read_parquet(path)
        self.assertEqual(frame.iloc[0]["field_name"], "official_pmi")
        self.assertEqual(frame.iloc[0]["period_label"], "2026-03")
        self.assertEqual(frame.iloc[0]["release_date"], date(2026, 4, 1))
        self.assertEqual(frame.iloc[0]["effective_date"], date(2026, 4, 1))

    def test_gov_curve_points_run_through_source_to_canonical_write(self) -> None:
        self._insert_calendar_window(date(2026, 4, 20), date(2026, 4, 20))

        result = self.service.run_dataset_sync(
            "rates.gov_curve_points",
            IngestionRequest(
                request_start=date(2026, 4, 20),
                request_end=date(2026, 4, 20),
            ),
        )

        self.assertEqual(result.status, RunStatus.SUCCESS)
        self.assertEqual(result.records_written, 2)
        self.assertEqual(result.validation_counts["pass_with_caveat"], 1)
        self.assertEqual(self.client.curve_windows, [("2026-04-20", "2026-04-20")])
        path = build_dataset_file_path(
            "rates.gov_curve_points",
            StorageZone.NORMALIZED,
            [("year", 2026), ("month", "04")],
            lakehouse_root=self.lakehouse_root,
        )
        frame = pd.read_parquet(path).sort_values("field_name").reset_index(drop=True)
        self.assertEqual(
            frame["field_name"].tolist(),
            ["cn_gov_10y_yield", "cn_gov_1y_yield"],
        )
        self.assertEqual(
            frame.loc[frame["field_name"].eq("cn_gov_10y_yield"), "field_role"].iloc[0],
            "primary",
        )

    def test_gov_curve_points_falls_back_to_akshare_when_yc_cb_denied(self) -> None:
        self._insert_calendar_window(date(2026, 4, 20), date(2026, 4, 20))
        client = StageFRatesPermissionDeniedCurveClient()
        akshare = StageFRatesMockAkShare()
        service = ETLService(
            conn=self.conn,
            source_adapters=[TushareSourceAdapter(client, akshare_module=akshare)],
            lakehouse_root=self.lakehouse_root,
        )

        result = service.run_dataset_sync(
            "rates.gov_curve_points",
            IngestionRequest(
                request_start=date(2026, 4, 20),
                request_end=date(2026, 4, 20),
            ),
        )

        self.assertEqual(result.status, RunStatus.SUCCESS)
        self.assertEqual(result.records_written, 2)
        self.assertEqual(client.curve_windows, [("2026-04-20", "2026-04-20")])
        self.assertEqual(
            akshare.curve_calls,
            [{"start_date": "20260420", "end_date": "20260420"}],
        )
        path = build_dataset_file_path(
            "rates.gov_curve_points",
            StorageZone.NORMALIZED,
            [("year", 2026), ("month", "04")],
            lakehouse_root=self.lakehouse_root,
        )
        frame = pd.read_parquet(path).sort_values("field_name").reset_index(drop=True)
        self.assertEqual(frame["curve_code"].unique().tolist(), ["cn_gov_bond"])
        self.assertEqual(frame["value"].tolist(), [2.35, 1.55])

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

    def test_tushare_client_pivots_yc_cb_long_format_curve_points(self) -> None:
        pro = StageFRatesRawTusharePro()
        client = TushareClient.__new__(TushareClient)
        client._pro = pro

        frame = client.get_gov_curve_points("2026-04-20", "2026-04-20")

        self.assertEqual(
            pro.curve_calls,
            [{"start_date": "20260420", "end_date": "20260420"}],
        )
        self.assertEqual(
            frame.columns.tolist(), ["curve_date", "curve_code", "1y", "10y"]
        )
        self.assertEqual(len(frame), 1)
        row = frame.iloc[0]
        self.assertEqual(row["curve_date"], pd.Timestamp("2026-04-20"))
        self.assertEqual(row["curve_code"], "1001.CB")
        self.assertEqual(row["1y"], 1.55)
        self.assertEqual(row["10y"], 2.35)

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
        calendar = pd.DataFrame({
            "exchange": ["SH", "SZ", "SH", "SZ"],
            "trade_date": [
                date(2026, 5, 20),
                date(2026, 5, 20),
                date(2026, 5, 21),
                date(2026, 5, 21),
            ],
            "is_open": [False, False, True, True],
        })

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

    def test_lpr_normalizer_requires_next_open_day_from_calendar(self) -> None:
        calendar = pd.DataFrame({
            "exchange": ["SH", "SZ"],
            "trade_date": [date(2026, 5, 20), date(2026, 5, 20)],
            "is_open": [False, False],
        })

        canonical = (
            LprNormalizer()
            .normalize(
                pd.DataFrame({"date": [pd.Timestamp("2026-05-20")], "1y": [3.0]}),
                {
                    "source_name": "tushare",
                    "raw_batch_id": 9,
                    "canonical_trading_calendar": calendar,
                },
            )
            .canonical_payload
        )

        self.assertEqual(canonical.iloc[0]["quote_date"], date(2026, 5, 20))
        self.assertIsNone(canonical.iloc[0]["effective_date"])

    def test_lpr_run_fails_when_calendar_lacks_next_open_day(self) -> None:
        self._insert_calendar_window(
            date(2026, 5, 20), date(2026, 5, 20), is_open=False
        )

        result = self.service.run_dataset_sync(
            "rates.lpr",
            IngestionRequest(
                request_start=date(2026, 5, 20),
                request_end=date(2026, 5, 20),
            ),
        )

        self.assertEqual(result.status, RunStatus.FAILED)
        self.assertEqual(result.records_written, 0)
        self.assertFalse(result.watermark_updated)
        failed_checks = {
            check_name
            for check_name, status in self.conn.execute(
                """
                SELECT check_name, status
                FROM etl_validation_results
                WHERE run_id = ?
                """,
                [result.run_id],
            ).fetchall()
            if status == ValidationStatus.FAIL.value
        }
        self.assertIn("lpr.effective_date_required", failed_checks)

    def test_daily_rates_validator_blocks_duplicate_keys(self) -> None:
        payload = pd.DataFrame({
            "field_name": ["shibor_1w", "shibor_1w"],
            "trade_date": [date(2026, 4, 20), date(2026, 4, 20)],
            "value": [1.8, 1.9],
            "unit": ["percent", "percent"],
            "field_role": ["primary", "primary"],
            "release_date": [date(2026, 4, 20), date(2026, 4, 20)],
            "effective_date": [date(2026, 4, 20), date(2026, 4, 20)],
            "revision_note": ["low_revision_risk", "low_revision_risk"],
            "source_caveat": ["wrapper", "wrapper"],
        })

        results = DailyRatesValidator().validate(
            payload,
            {"dataset_name": "rates.daily_rates", "run_id": 1},
        )

        self.assertTrue(has_blocking_failures(results))

    def test_daily_rates_validator_requires_release_and_revision_metadata(self) -> None:
        payload = pd.DataFrame({
            "field_name": ["shibor_1w"],
            "trade_date": [date(2026, 4, 20)],
            "value": [1.8],
            "unit": ["percent"],
            "field_role": ["primary"],
            "release_date": ["not-a-date"],
            "effective_date": [date(2026, 4, 20)],
            "revision_note": [""],
            "source_caveat": ["wrapper"],
        })

        results = DailyRatesValidator().validate(
            payload,
            {"dataset_name": "rates.daily_rates", "run_id": 1},
        )
        failed_checks = {
            result.check_name
            for result in results
            if result.status == ValidationStatus.FAIL
        }

        self.assertIn("daily_rates.release_date_required", failed_checks)
        self.assertIn("daily_rates.revision_note_required", failed_checks)
        self.assertTrue(has_blocking_failures(results))

    def test_lpr_validator_compares_string_dates_as_dates(self) -> None:
        payload = pd.DataFrame({
            "field_name": ["lpr_1y"],
            "quote_date": ["2026-10-02"],
            "value": [3.1],
            "unit": ["percent"],
            "field_role": ["primary"],
            "release_date": ["2026-10-02"],
            "effective_date": ["2026-02-01"],
            "revision_note": ["low_revision_risk_relative_to_other_slow_fields"],
            "source_caveat": ["wrapper"],
        })

        results = LprValidator().validate(
            payload,
            {"dataset_name": "rates.lpr", "run_id": 1},
        )
        failures = [
            result
            for result in results
            if result.check_name == "lpr.effective_date_after_release"
            and result.status == ValidationStatus.FAIL
        ]

        self.assertEqual(len(failures), 1)
        self.assertTrue(has_blocking_failures(results))

    def _insert_calendar_window(
        self, start: date, end: date, is_open: bool = True
    ) -> None:
        dates = pd.date_range(start, end, freq="D")
        rows = []
        for value in dates:
            for exchange in ("SH", "SZ"):
                rows.append({
                    "exchange": exchange,
                    "trade_date": value.date(),
                    "is_open": is_open,
                    "pretrade_date": None,
                    "updated_at": pd.Timestamp("2026-01-01"),
                })
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
