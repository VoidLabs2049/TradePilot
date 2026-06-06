"""Stage G ETF all-weather market feature tests."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import json
import threading
import unittest

import pandas as pd

from tradepilot import db
from tradepilot.etl.models import RunStatus
from tradepilot.etl.service import ETLService


class StageGMarketFeaturesTests(unittest.TestCase):
    """Verify Stage G market feature materialization."""

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

    def test_market_features_build_long_form_contract(self) -> None:
        self._write_snapshot(
            [
                self._row("510300.SH", "equity_large", 0.02, 0.04, 0.06),
                self._row("159845.SZ", "equity_small", 0.02, 0.04, 0.06),
                self._row("511010.SH", "bond", -0.02, -0.04, -0.06),
                self._row("518850.SH", "gold", -0.02, -0.04, -0.06),
                self._row("159001.SZ", "cash", 0.0, 0.0, 0.0),
            ]
        )
        self.service.run_bootstrap(
            "derived.etf_aw_regime_score.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        result = self.service.run_bootstrap(
            "derived.etf_aw_market_features.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        self.assertEqual(result["status"], RunStatus.SUCCESS.value)
        self.assertEqual(result["records_written"], 37)
        self.assertTrue(all(result["validation"].values()))
        frame = self._read_features_file(2024, 7)
        self.assertEqual(len(frame), 37)
        self.assertFalse(
            frame.duplicated(
                [
                    "calendar_name",
                    "rebalance_date",
                    "feature_name",
                    "feature_scope",
                    "feature_subject",
                ]
            ).any()
        )
        self.assertFalse(
            any(
                any(token in name for token in ("macro", "rate", "lpr", "curve"))
                for name in frame["feature_name"].astype(str)
            )
        )
        direction = self._feature(frame, "direction_score", "sleeve", "equity_large")
        self.assertEqual(direction["feature_value"], 100.0)
        self.assertEqual(direction["feature_status"], "complete")
        self.assertTrue(
            json.loads(direction["quality_notes"])[
                "computed_with_stage_e_scorer_helper"
            ]
        )
        equity = self._feature(frame, "equity_score", "group", "equity")
        self.assertEqual(equity["feature_value"], 100.0)
        market = self._feature(frame, "market_score", "regime", "market_only")
        self.assertEqual(market["source_dataset"], "derived.etf_aw_regime_score")
        self.assertEqual(market["feature_status"], "complete")

    def test_missing_regime_score_outputs_missing_regime_features(self) -> None:
        self._write_snapshot(
            [
                self._row("510300.SH", "equity_large", 0.02, 0.04, 0.06),
                self._row("159845.SZ", "equity_small", 0.02, 0.04, 0.06),
                self._row("511010.SH", "bond", -0.02, -0.04, -0.06),
                self._row("518850.SH", "gold", -0.02, -0.04, -0.06),
                self._row("159001.SZ", "cash", 0.0, 0.0, 0.0),
            ]
        )

        result = self.service.run_bootstrap(
            "derived.etf_aw_market_features.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        self.assertEqual(result["status"], RunStatus.SUCCESS.value)
        frame = self._read_features_file(2024, 7)
        market = self._feature(frame, "market_score", "regime", "market_only")
        self.assertTrue(pd.isna(market["feature_value"]))
        self.assertEqual(market["feature_status"], "missing")
        direction = self._feature(frame, "direction_score", "sleeve", "equity_large")
        self.assertEqual(direction["feature_value"], 100.0)

    def test_repeat_rebuild_upserts_without_duplicate_business_keys(self) -> None:
        self._write_snapshot(
            [
                self._row("510300.SH", "equity_large", 0.02, 0.04, 0.06),
                self._row("159845.SZ", "equity_small", 0.02, 0.04, 0.06),
                self._row("511010.SH", "bond", -0.02, -0.04, -0.06),
                self._row("518850.SH", "gold", -0.02, -0.04, -0.06),
                self._row("159001.SZ", "cash", 0.0, 0.0, 0.0),
            ]
        )
        self.service.run_bootstrap(
            "derived.etf_aw_regime_score.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )
        self.service.run_bootstrap(
            "derived.etf_aw_market_features.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )
        result = self.service.run_bootstrap(
            "derived.etf_aw_market_features.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        frame = self._read_features_file(2024, 7)
        self.assertEqual(result["records_updated"], 37)
        self.assertEqual(len(frame), 37)
        self.assertFalse(
            frame.duplicated(
                [
                    "calendar_name",
                    "rebalance_date",
                    "feature_name",
                    "feature_scope",
                    "feature_subject",
                ]
            ).any()
        )

    def _row(
        self,
        sleeve_code: str,
        sleeve_role: str,
        return_1m: float | None,
        return_3m: float | None,
        return_6m: float | None,
        data_status: str = "complete",
    ) -> dict:
        return {
            "calendar_name": "etf_aw_v1_monthly_post_20",
            "calendar_month": "2024-07",
            "rebalance_date": date(2024, 7, 22),
            "effective_date": date(2024, 7, 22),
            "sleeve_code": sleeve_code,
            "sleeve_role": sleeve_role,
            "close": 10.0 if data_status != "missing" else None,
            "adj_factor": 1.0 if data_status != "missing" else None,
            "adj_close": 10.0 if data_status != "missing" else None,
            "return_1m": return_1m,
            "return_3m": return_3m,
            "return_6m": return_6m,
            "volatility_3m": 0.10 if data_status != "missing" else None,
            "max_drawdown_6m": 0.0 if data_status != "missing" else None,
            "data_status": data_status,
            "quality_notes": json.dumps({}),
            "source_max_trade_date": date(2024, 7, 22),
            "ingested_at": pd.Timestamp("2024-07-22"),
        }

    def _write_snapshot(self, rows: list[dict]) -> None:
        self.service._write_etf_aw_rebalance_snapshot(pd.DataFrame(rows))

    def _read_features_file(self, year: int, month: int) -> pd.DataFrame:
        return pd.read_parquet(self._features_file_path(year, month))

    def _features_file_path(self, year: int, month: int) -> Path:
        return (
            self.lakehouse_root
            / "derived"
            / "derived.etf_aw_market_features"
            / str(year)
            / f"{month:02d}"
            / "part-00000.parquet"
        )

    def _feature(
        self, frame: pd.DataFrame, name: str, scope: str, subject: str
    ) -> pd.Series:
        return frame[
            frame["feature_name"].eq(name)
            & frame["feature_scope"].eq(scope)
            & frame["feature_subject"].eq(subject)
        ].iloc[0]


if __name__ == "__main__":
    unittest.main()
