"""Tests for the commodity futures stage 1 roll audit."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import pandas as pd
from click.testing import CliRunner

from tradepilot.etl.futures_stage1 import build_stage1_report, main


class FuturesStage1Tests(unittest.TestCase):
    """Verify the stage 1 M single-contract and roll audit."""

    def test_builds_m_roll_audit_from_lakehouse_snapshot(self) -> None:
        with TemporaryDirectory() as temp_dir:
            lakehouse_root = Path(temp_dir) / "lakehouse"
            _write_stage1_fixture(lakehouse_root)

            report = build_stage1_report(lakehouse_root=lakehouse_root)

        self.assertEqual(report.root_code, "M.DCE")
        self.assertEqual(report.roll_date, date(2025, 4, 7))
        self.assertEqual(report.roll_from, "M2505.DCE")
        self.assertEqual(report.roll_to, "M2509.DCE")
        self.assertEqual(len(report.window_rows), 22)
        self.assertEqual(report.contract_calculation.multiplier, 10.0)
        self.assertEqual(report.contract_calculation.one_lot_notional, 30560.0)
        self.assertEqual(report.contract_calculation.pnl_for_one_percent_move, 305.6)
        self.assertEqual(report.roll_gap.close_gap, 171.0)
        self.assertAlmostEqual(report.roll_gap.close_gap_pct, 171.0 / 2885.0)
        self.assertTrue(
            any(
                row.trade_date == date(2025, 4, 7)
                and row.contract_code == "M2509.DCE"
                and row.is_mapped_active
                for row in report.window_rows
            )
        )

    def test_cli_writes_report(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            lakehouse_root = root / "lakehouse"
            output = root / "stage-1.md"
            _write_stage1_fixture(lakehouse_root)

            result = CliRunner().invoke(
                main,
                [
                    "--lakehouse-root",
                    str(lakehouse_root),
                    "--output",
                    str(output),
                ],
            )

            self.assertEqual(result.exit_code, 0, result.output)
            text = output.read_text(encoding="utf-8")

        self.assertIn("M2505.DCE", text)
        self.assertIn("M2509.DCE", text)
        self.assertIn("5.9272%", text)
        self.assertIn("不构建连续合约", text)

    def test_rejects_missing_roll_window_core_field(self) -> None:
        with TemporaryDirectory() as temp_dir:
            lakehouse_root = Path(temp_dir) / "lakehouse"
            _write_stage1_fixture(lakehouse_root, missing_volume=True)

            with self.assertRaisesRegex(ValueError, "missing close/settle/volume/oi"):
                build_stage1_report(lakehouse_root=lakehouse_root)


def _write_stage1_fixture(
    lakehouse_root: Path, *, missing_volume: bool = False
) -> None:
    """Write the minimal normalized parquet fixture for the stage 1 audit."""

    dates = [date(2025, 3, 28) + timedelta(days=offset) for offset in range(11)]
    mapping_dates = [
        date(2025, 3, 28),
        date(2025, 3, 31),
        date(2025, 4, 1),
        date(2025, 4, 2),
        date(2025, 4, 3),
        date(2025, 4, 7),
        date(2025, 4, 8),
        date(2025, 4, 9),
        date(2025, 4, 10),
        date(2025, 4, 11),
        date(2025, 4, 14),
    ]
    assert len(dates) == len(mapping_dates)
    mapping = pd.DataFrame(
        {
            "root_code": ["M.DCE"] * len(mapping_dates),
            "trade_date": mapping_dates,
            "active_contract": ["M2505.DCE"] * 5 + ["M2509.DCE"] * 6,
        }
    )
    instruments = pd.DataFrame(
        {
            "contract_code": ["M2509.DCE"],
            "symbol": ["M2509"],
            "exchange": ["DCE"],
            "name": ["豆粕2509"],
            "futures_code": ["M"],
            "multiplier": [10.0],
            "trade_unit": ["吨"],
            "per_unit": [10.0],
            "quote_unit": ["人民币元/吨"],
            "list_date": [date(2024, 9, 15)],
            "delist_date": [date(2025, 9, 14)],
        }
    )
    daily_rows: list[dict[str, object]] = []
    for index, trade_date in enumerate(mapping_dates):
        old_close = 2800.0 + index
        new_close = 3000.0 + index
        if trade_date == date(2025, 4, 7):
            old_close = 2885.0
            new_close = 3056.0
        for contract_code, close in [
            ("M2505.DCE", old_close),
            ("M2509.DCE", new_close),
        ]:
            daily_rows.append(
                {
                    "contract_code": contract_code,
                    "trade_date": trade_date,
                    "pre_close": close - 3,
                    "pre_settle": close - 2,
                    "open": close - 1,
                    "high": close + 8,
                    "low": close - 8,
                    "close": close,
                    "settle": close + 1,
                    "change1": 3.0,
                    "change2": 2.0,
                    "volume": (
                        pd.NA
                        if missing_volume and trade_date == date(2025, 4, 7)
                        else 1000.0 + index
                    ),
                    "amount": 1.0,
                    "oi": 2000.0 + index,
                    "oi_chg": 10.0,
                }
            )
    _write_parquet(
        lakehouse_root,
        "normalized/reference.futures_instruments/2025/04/part-00000.parquet",
        instruments,
    )
    _write_parquet(
        lakehouse_root,
        "normalized/market.futures_mapping/2025/04/part-00000.parquet",
        mapping,
    )
    _write_parquet(
        lakehouse_root,
        "normalized/market.futures_contract_daily/2025/04/part-00000.parquet",
        pd.DataFrame(daily_rows),
    )


def _write_parquet(
    lakehouse_root: Path, relative_path: str, frame: pd.DataFrame
) -> None:
    """Write one parquet file below the temporary lakehouse root."""

    path = lakehouse_root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


if __name__ == "__main__":
    unittest.main()
