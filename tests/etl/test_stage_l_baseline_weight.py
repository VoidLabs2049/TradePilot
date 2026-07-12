"""Stage L ETF all-weather baseline weight tests."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
import json
import threading
import unittest

import pandas as pd

from tradepilot import db
from tradepilot.etl import service as etl_service
from tradepilot.etl.etf_aw_universe import (
    ETF_AW_SLEEVE_CODE_BY_ROLE,
    ETF_AW_SLEEVE_ROLE_ORDER,
)
from tradepilot.etl.models import RunStatus
from tradepilot.etl.service import ETLService


class StageLBaselineWeightTests(unittest.TestCase):
    """Verify static inverse-vol baseline weight generation."""

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

    def test_static_inverse_vol_baseline_writes_complete_artifact(self) -> None:
        rebalance_date = date(2024, 7, 22)
        self._insert_rebalance(rebalance_date)
        self.service._write_etf_aw_sleeve_daily(
            self._panel(rebalance_date, observations=70)
        )

        result = self.service.run_bootstrap(
            "derived.etf_aw_baseline_weight.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        self.assertEqual(result["status"], RunStatus.SUCCESS.value)
        self.assertTrue(all(result["validation"].values()))
        frame = self._read_baseline_file(2024, 7)
        self.assertEqual(len(frame), 5)
        self.assertEqual(set(frame["baseline_name"]), {"static_inverse_vol"})
        self.assertEqual(set(frame["baseline_version"]), {"static_inverse_vol_v1"})
        self.assertEqual(set(frame["estimation_window_days"]), {63})
        self.assertEqual(set(frame["min_observation_days"]), {42})
        self.assertAlmostEqual(float(frame["target_weight"].sum()), 1.0, places=6)
        self.assertTrue((frame["target_weight"] >= 0.0).all())
        notes = json.loads(frame.iloc[0]["quality_notes_json"])
        self.assertEqual(notes["minimum_observations"], 42)

    def test_baseline_blocks_incomplete_volatility_vector(self) -> None:
        rebalance_date = date(2024, 7, 22)
        rebalance = pd.DataFrame(
            [
                {
                    "calendar_name": "etf_aw_v1_monthly_post_20",
                    "rebalance_date": rebalance_date,
                }
            ]
        )

        frame = self.service._make_etf_aw_baseline_weight_frame(
            rebalance=rebalance,
            panel=self._panel(rebalance_date, observations=20),
        )

        self.assertTrue(frame.empty)

    def test_baseline_validation_rejects_partial_vector(self) -> None:
        rebalance_date = date(2024, 7, 22)
        rebalance = pd.DataFrame(
            [
                {
                    "calendar_name": "etf_aw_v1_monthly_post_20",
                    "rebalance_date": rebalance_date,
                }
            ]
        )
        frame = self.service._make_etf_aw_baseline_weight_frame(
            rebalance=rebalance,
            panel=self._panel(rebalance_date, observations=70),
        ).iloc[:-1]

        validation = etl_service._validate_baseline_weight_frame(frame)

        self.assertFalse(validation["five_roles_per_rebalance_date"])

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

    def _panel(self, rebalance_date: date, *, observations: int) -> pd.DataFrame:
        rows = []
        start = rebalance_date - timedelta(days=observations + 10)
        dates = [start + timedelta(days=offset) for offset in range(observations + 10)]
        returns_by_role = {
            "equity_large": 0.010,
            "equity_small": 0.015,
            "bond": 0.004,
            "gold": 0.008,
            "cash": 0.002,
        }
        for role in ETF_AW_SLEEVE_ROLE_ORDER:
            price = 1.0
            for index, trade_date in enumerate(dates):
                daily_return = returns_by_role[role] + (index % 3) * 0.0001
                price *= 1.0 + daily_return
                rows.append(
                    {
                        "sleeve_code": ETF_AW_SLEEVE_CODE_BY_ROLE[role],
                        "sleeve_role": role,
                        "instrument_id": ETF_AW_SLEEVE_CODE_BY_ROLE[role],
                        "trade_date": trade_date,
                        "open": price,
                        "high": price,
                        "low": price,
                        "close": price,
                        "adj_factor": 1.0,
                        "adj_close": price,
                        "adj_pct_chg": daily_return * 100,
                        "vol": 1.0,
                        "amount": 1.0,
                        "source_name": "fixture",
                        "ingested_at": pd.Timestamp("2024-07-22 15:00:00"),
                        "quality_status": "pass",
                    }
                )
        return pd.DataFrame(rows)

    def _read_baseline_file(self, year: int, month: int) -> pd.DataFrame:
        return pd.read_parquet(
            self.lakehouse_root
            / "derived"
            / "derived.etf_aw_baseline_weight"
            / f"{year:04d}"
            / f"{month:02d}"
            / "part-00000.parquet"
        )


if __name__ == "__main__":
    unittest.main()
