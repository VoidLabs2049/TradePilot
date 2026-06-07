"""Tests for the ETF all-weather viewer CLI."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import pandas as pd
from click.testing import CliRunner

from tools.etl_review.view_etf_aw import main


class ViewEtfAwCliTests(unittest.TestCase):
    """Verify ETF all-weather views can be filtered by code and date range."""

    def test_daily_view_filters_bare_etf_code_and_dates(self) -> None:
        with TemporaryDirectory() as temp_dir:
            lakehouse = Path(temp_dir) / "lakehouse"
            daily_path = (
                lakehouse / "derived" / "derived.etf_aw_sleeve_daily" / "2026" / "05"
            )
            daily_path.mkdir(parents=True)
            pd.DataFrame(
                {
                    "trade_date": ["2026-05-06", "2026-05-07", "2026-05-07"],
                    "sleeve_code": ["510300.SH", "510300.SH", "511010.SH"],
                    "sleeve_role": ["equity_large", "equity_large", "bond"],
                    "close": [4.8, 4.9, 141.0],
                    "adj_factor": [1.2, 1.2, 1.0],
                    "adj_close": [5.76, 5.88, 141.0],
                    "pct_chg": [1.0, 2.0, 0.1],
                    "adj_pct_chg": [1.0, 2.083333, 0.1],
                    "volume": [100.0, 110.0, 120.0],
                    "amount": [1000.0, 1100.0, 1200.0],
                    "quality_status": ["pass", "pass", "pass"],
                }
            ).to_parquet(daily_path / "part-00000.parquet", index=False)

            result = CliRunner().invoke(
                main,
                [
                    "510300",
                    "2026-05-07",
                    "2026-05-31",
                    "--lakehouse-root",
                    str(lakehouse),
                ],
            )

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("etf=510300.SH", result.output)
        self.assertIn("rows=1", result.output)
        self.assertIn("2026-05-07", result.output)
        self.assertNotIn("511010.SH", result.output)

    def test_snapshot_view_can_write_csv(self) -> None:
        with TemporaryDirectory() as temp_dir:
            lakehouse = Path(temp_dir) / "lakehouse"
            snapshot_path = (
                lakehouse
                / "derived"
                / "derived.etf_aw_rebalance_snapshot"
                / "2026"
                / "05"
            )
            snapshot_path.mkdir(parents=True)
            pd.DataFrame(
                {
                    "rebalance_date": ["2026-05-20"],
                    "sleeve_code": ["159845.SZ"],
                    "sleeve_role": ["equity_small"],
                    "close": [3.642],
                    "adj_factor": [0.263],
                    "adj_close": [0.957846],
                    "return_1m": [0.072754],
                    "return_3m": [0.097980],
                    "return_6m": [0.172569],
                    "volatility_3m": [0.241486],
                    "max_drawdown_6m": [-0.133635],
                    "data_status": ["complete"],
                }
            ).to_parquet(snapshot_path / "part-00000.parquet", index=False)
            csv_path = Path(temp_dir) / "snapshot.csv"

            result = CliRunner().invoke(
                main,
                [
                    "159845.SZ",
                    "2026-05-01",
                    "2026-05-31",
                    "--dataset",
                    "snapshot",
                    "--csv",
                    str(csv_path),
                    "--lakehouse-root",
                    str(lakehouse),
                ],
            )

            self.assertEqual(result.exit_code, 0, result.output)
            self.assertTrue(csv_path.exists())
            exported = pd.read_csv(csv_path)
            self.assertEqual(exported["sleeve_code"].tolist(), ["159845.SZ"])


if __name__ == "__main__":
    unittest.main()
