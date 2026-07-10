"""Stage H ETF all-weather backtest kernel tests."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
import json
import threading
import unittest
from unittest.mock import patch

import pandas as pd

from tradepilot import db
from tradepilot.etl import service as etl_service
from tradepilot.etl.models import RunStatus
from tradepilot.etl.service import ETLService


class StageHBacktestKernelTests(unittest.TestCase):
    """Verify the minimal ETF all-weather backtest acceptance fixture."""

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

    def tearDown(self) -> None:
        conn = getattr(db._thread_local, "conn", None)
        if conn is not None:
            conn.close()
        db._thread_local = self._original_thread_local
        db.DB_PATH = self._original_db_path
        db._initialized = self._original_initialized
        self._temp_dir.cleanup()

    def test_equal_weight_kernel_outputs_nav_metrics_and_turnover(self) -> None:
        self.assertTrue(
            self.service.registry.has_dataset("derived.etf_aw_backtest_kernel")
        )
        self._insert_rebalance(date(2024, 1, 22))
        self._insert_rebalance(date(2024, 2, 20))
        self._write_sleeve_daily(date(2024, 1, 22), date(2024, 2, 23))
        self._write_target_weights([date(2024, 1, 22), date(2024, 2, 20)])

        result = self.service.run_bootstrap(
            "derived.etf_aw_backtest_kernel.build",
            start=date(2024, 1, 1),
            end=date(2024, 2, 29),
        )

        self.assertEqual(result["status"], RunStatus.SUCCESS.value)
        self.assertTrue(all(result["validation"].values()))
        frame = self._read_backtest_file(2024, 2)
        self.assertIn("daily_nav", set(frame["observation_type"]))
        self.assertIn("metric", set(frame["observation_type"]))
        self.assertIn("turnover", set(frame["observation_type"]))
        metrics = frame[frame["observation_type"] == "metric"]
        self.assertIn("max_drawdown", set(metrics["metric_name"]))
        self.assertIn("sharpe_ratio", set(metrics["metric_name"]))
        nav = frame[frame["observation_type"] == "daily_nav"].sort_values(
            "observation_date"
        )
        self.assertGreater(float(nav.iloc[-1]["net_value"]), 1.0)

    def test_repeat_run_upserts_without_duplicate_business_keys(self) -> None:
        self._insert_rebalance(date(2024, 1, 22))
        self._write_sleeve_daily(date(2024, 1, 22), date(2024, 1, 31))
        self._write_target_weights([date(2024, 1, 22)])

        self.service.run_bootstrap(
            "derived.etf_aw_backtest_kernel.build",
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
        )
        result = self.service.run_bootstrap(
            "derived.etf_aw_backtest_kernel.build",
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
        )

        frame = self._read_backtest_file(2024, 1)
        self.assertEqual(result["records_written"], len(frame))
        self.assertEqual(result["records_inserted"], 0)
        self.assertFalse(
            frame.duplicated(
                [
                    "calendar_name",
                    "strategy_name",
                    "strategy_version",
                    "observation_type",
                    "observation_date",
                    "metric_name",
                ]
            ).any()
        )

    def test_bootstrap_requires_target_weight_artifact(self) -> None:
        self._insert_rebalance(date(2024, 1, 22))
        self._write_sleeve_daily(date(2024, 1, 22), date(2024, 1, 31))

        result = self.service.run_bootstrap(
            "derived.etf_aw_backtest_kernel.build",
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
        )

        self.assertEqual(result["status"], RunStatus.FAILED.value)
        self.assertEqual(
            result["error_message"],
            "ETF all-weather target weight is missing",
        )

    def test_duplicate_weight_rows_block_pure_kernel(self) -> None:
        rebalance = pd.DataFrame(
            [
                {
                    "calendar_name": "etf_aw_v1_monthly_post_20",
                    "rebalance_date": date(2024, 1, 22),
                }
            ]
        )
        weights = self._equal_weights(date(2024, 1, 22))
        weights = pd.concat([weights, weights.iloc[[0]]], ignore_index=True)

        frame = self.service._make_etf_aw_backtest_kernel_frame(
            panel=self._sleeve_daily_frame(date(2024, 1, 22), date(2024, 1, 31)),
            rebalance=rebalance,
            weights=weights,
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
        )

        self.assertEqual(set(frame["observation_type"]), {"diagnostic"})
        notes = json.loads(frame.iloc[0]["quality_notes_json"])
        self.assertIn("duplicate_weight_rows", notes["reasons"])

    def test_missing_sleeve_weight_blocks_pure_kernel(self) -> None:
        rebalance = pd.DataFrame(
            [
                {
                    "calendar_name": "etf_aw_v1_monthly_post_20",
                    "rebalance_date": date(2024, 1, 22),
                }
            ]
        )
        weights = self._equal_weights(date(2024, 1, 22)).iloc[:-1].copy()

        frame = self.service._make_etf_aw_backtest_kernel_frame(
            panel=self._sleeve_daily_frame(date(2024, 1, 22), date(2024, 1, 31)),
            rebalance=rebalance,
            weights=weights,
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
        )

        self.assertEqual(set(frame["observation_type"]), {"diagnostic"})
        notes = json.loads(frame.iloc[0]["quality_notes_json"])
        self.assertIn("missing_sleeve_weight", notes["reasons"])

    def test_missing_sleeve_return_blocks_pure_kernel(self) -> None:
        rebalance = pd.DataFrame(
            [
                {
                    "calendar_name": "etf_aw_v1_monthly_post_20",
                    "rebalance_date": date(2024, 1, 22),
                }
            ]
        )
        panel = self._sleeve_daily_frame(date(2024, 1, 22), date(2024, 1, 31))
        panel = panel[
            ~(
                (panel["trade_date"] == date(2024, 1, 24))
                & (panel["sleeve_code"] == "518850.SH")
            )
        ].copy()

        frame = self.service._make_etf_aw_backtest_kernel_frame(
            panel=panel,
            rebalance=rebalance,
            weights=self._equal_weights(date(2024, 1, 22)),
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
        )

        self.assertEqual(set(frame["observation_type"]), {"diagnostic"})
        self.assertEqual(len(frame), 1)
        notes = json.loads(frame.iloc[0]["quality_notes_json"])
        self.assertIn("missing_sleeve_return", notes["reasons"])

    def test_blocked_kernel_diagnostic_uses_target_weight_strategy_cycle(
        self,
    ) -> None:
        rebalance_date = date(2024, 1, 22)
        rebalance = pd.DataFrame(
            [
                {
                    "calendar_name": "etf_aw_v1_monthly_post_20",
                    "rebalance_date": rebalance_date,
                }
            ]
        )
        weights = self._equal_weights(rebalance_date)
        weights["strategy_name"] = "etf_aw_v1"
        weights["strategy_version"] = "target_weight_inverse_vol_v1"
        panel = self._sleeve_daily_frame(rebalance_date, date(2024, 1, 31))
        panel = panel[panel["sleeve_code"] != "518850.SH"].copy()

        frame = self.service._make_etf_aw_backtest_kernel_frame(
            panel=panel,
            rebalance=rebalance,
            weights=weights,
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
        )

        self.assertEqual(set(frame["observation_type"]), {"diagnostic"})
        self.assertEqual(frame.iloc[0]["strategy_name"], "etf_aw_v1")
        self.assertEqual(
            frame.iloc[0]["strategy_version"],
            "target_weight_inverse_vol_v1",
        )
        self.assertEqual(frame.iloc[0]["observation_date"], rebalance_date)
        notes = json.loads(frame.iloc[0]["quality_notes_json"])
        self.assertEqual(notes["rebalance_date"], rebalance_date.isoformat())

    def test_rebalance_date_without_matching_trade_date_blocks_pure_kernel(
        self,
    ) -> None:
        rebalance = pd.DataFrame(
            [
                {
                    "calendar_name": "etf_aw_v1_monthly_post_20",
                    "rebalance_date": date(2024, 1, 20),
                }
            ]
        )

        frame = self.service._make_etf_aw_backtest_kernel_frame(
            panel=self._sleeve_daily_frame(date(2024, 1, 22), date(2024, 1, 31)),
            rebalance=rebalance,
            weights=self._equal_weights(date(2024, 1, 20)),
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
        )

        self.assertEqual(set(frame["observation_type"]), {"diagnostic"})
        notes = json.loads(frame.iloc[0]["quality_notes_json"])
        self.assertIn("rebalance_date_without_trading_day", notes["reasons"])

    def test_monthly_returns_do_not_absorb_next_month_first_day(self) -> None:
        rebalance = pd.DataFrame(
            [
                {
                    "calendar_name": "etf_aw_v1_monthly_post_20",
                    "rebalance_date": date(2024, 1, 30),
                }
            ]
        )
        panel = self._sleeve_daily_frame_with_returns(
            [
                (date(2024, 1, 30), 0.01),
                (date(2024, 1, 31), 0.02),
                (date(2024, 2, 2), 0.03),
            ]
        )
        captured: dict[str, list[float]] = {}
        original_metric_values = etl_service._backtest_metric_values

        def capture_metrics(
            daily_returns: list[float],
            monthly_returns: list[float],
            final_nav: float,
        ) -> dict[str, float | None]:
            captured["monthly_returns"] = monthly_returns
            return original_metric_values(daily_returns, monthly_returns, final_nav)

        with patch(
            "tradepilot.etl.service._backtest_metric_values",
            side_effect=capture_metrics,
        ):
            self.service._make_etf_aw_backtest_kernel_frame(
                panel=panel,
                rebalance=rebalance,
                weights=self._equal_weights(date(2024, 1, 30)),
                start=date(2024, 1, 1),
                end=date(2024, 2, 29),
            )

        self.assertEqual(len(captured["monthly_returns"]), 2)
        self.assertAlmostEqual(captured["monthly_returns"][0], 1.01 * 1.02 - 1.0)
        self.assertAlmostEqual(captured["monthly_returns"][1], 0.03)

    def test_empty_backtest_metrics_keep_monthly_periods_key(self) -> None:
        metrics = etl_service._backtest_metric_values([], [], 1.0)

        self.assertIn("monthly_periods", metrics)
        self.assertIsNone(metrics["monthly_periods"])

    def _insert_rebalance(self, rebalance_date: date) -> None:
        self.conn.execute(
            """
            INSERT INTO canonical_rebalance_calendar (
                calendar_name, calendar_month, rebalance_date, effective_date, notes, updated_at
            ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                "etf_aw_v1_monthly_post_20",
                f"{rebalance_date.year:04d}-{rebalance_date.month:02d}",
                rebalance_date,
                rebalance_date,
                "{}",
            ],
        )

    def _write_sleeve_daily(self, start: date, end: date) -> None:
        self.service._write_etf_aw_sleeve_daily(self._sleeve_daily_frame(start, end))

    def _write_target_weights(self, rebalance_dates: list[date]) -> None:
        rows: list[dict] = []
        sleeves = [
            ("510300.SH", "equity_large"),
            ("159845.SZ", "equity_small"),
            ("511010.SH", "bond"),
            ("518850.SH", "gold"),
            ("159001.SZ", "cash"),
        ]
        for rebalance_date in rebalance_dates:
            for code, role in sleeves:
                rows.append(
                    {
                        "schema_version": "etf_aw_target_weight_v1",
                        "contract_version": "etf_aw_target_weight_contract_v1",
                        "calendar_name": "etf_aw_v1_monthly_post_20",
                        "rebalance_date": rebalance_date,
                        "effective_date": rebalance_date,
                        "strategy_name": "etf_aw_v1",
                        "strategy_version": "target_weight_inverse_vol_v1",
                        "sleeve_code": code,
                        "sleeve_role": role,
                        "risk_budget": 0.2,
                        "volatility_estimate": 0.01,
                        "volatility_floor": 0.005,
                        "raw_target_weight": 0.2,
                        "constrained_target_weight": 0.2,
                        "target_weight": 0.2,
                        "target_weight_status": "complete",
                        "optimizer_name": "budgeted_inverse_vol",
                        "optimizer_basis": "fixture",
                        "turnover_estimate": None,
                        "quality_notes_json": "{}",
                        "source_risk_budget_rebalance_date": rebalance_date,
                        "source_sleeve_daily_max_trade_date": rebalance_date,
                        "ingested_at": pd.Timestamp("2024-01-22"),
                    }
                )
        self.service._write_etf_aw_target_weight(pd.DataFrame(rows))

    def _sleeve_daily_frame(self, start: date, end: date) -> pd.DataFrame:
        rows: list[dict] = []
        codes = [
            ("510300.SH", "equity_large", 0.001),
            ("159845.SZ", "equity_small", 0.0015),
            ("511010.SH", "bond", 0.0002),
            ("518850.SH", "gold", 0.0006),
            ("159001.SZ", "cash", 0.0001),
        ]
        current = start
        dates: list[date] = []
        while current <= end:
            if current.weekday() < 5:
                dates.append(current)
            current += timedelta(days=1)
        for code, role, daily_return in codes:
            price = 10.0
            previous = None
            for trade_date in dates:
                price *= 1.0 + daily_return
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
                        "ingested_at": pd.Timestamp("2024-01-22"),
                        "quality_status": "pass",
                    }
                )
        return pd.DataFrame(rows)

    def _sleeve_daily_frame_with_returns(
        self, daily_returns: list[tuple[date, float]]
    ) -> pd.DataFrame:
        rows: list[dict] = []
        codes = [
            ("510300.SH", "equity_large"),
            ("159845.SZ", "equity_small"),
            ("511010.SH", "bond"),
            ("518850.SH", "gold"),
            ("159001.SZ", "cash"),
        ]
        for code, role in codes:
            price = 10.0
            for trade_date, daily_return in daily_returns:
                price *= 1.0 + daily_return
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
                        "pct_chg": daily_return * 100,
                        "adj_pct_chg": daily_return * 100,
                        "volume": 1.0,
                        "amount": 1.0,
                        "source_name": "fixture",
                        "ingested_at": pd.Timestamp("2024-01-30"),
                        "quality_status": "pass",
                    }
                )
        return pd.DataFrame(rows)

    def _equal_weights(self, rebalance_date: date) -> pd.DataFrame:
        codes = ["510300.SH", "159845.SZ", "511010.SH", "518850.SH", "159001.SZ"]
        return pd.DataFrame(
            [
                {
                    "calendar_name": "etf_aw_v1_monthly_post_20",
                    "rebalance_date": rebalance_date,
                    "sleeve_code": code,
                    "target_weight": 0.2,
                }
                for code in codes
            ]
        )

    def _read_backtest_file(self, year: int, month: int) -> pd.DataFrame:
        path = (
            self.lakehouse_root
            / "derived"
            / "derived.etf_aw_backtest_kernel"
            / str(year)
            / f"{month:02d}"
            / "part-00000.parquet"
        )
        return pd.read_parquet(path)


if __name__ == "__main__":
    unittest.main()
