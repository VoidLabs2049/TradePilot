"""Tests for the commodity futures stage 2 continuous-contract builder."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import pandas as pd
from click.testing import CliRunner

from tradepilot.etl.futures_stage2 import (
    build_continuous_contract,
    main,
    write_continuous_contract,
)


class FuturesStage2Tests(unittest.TestCase):
    """Verify the M continuous-contract construction rules."""

    def test_ratio_back_adjusts_only_history_before_rolls(self) -> None:
        with TemporaryDirectory() as temp_dir:
            lakehouse_root = Path(temp_dir) / "lakehouse"
            _write_stage2_fixture(lakehouse_root)

            frame = build_continuous_contract(lakehouse_root=lakehouse_root)

        self.assertEqual(len(frame), 7)
        self.assertEqual(frame["active_contract"].tolist(), _EXPECTED_CONTRACTS)
        self.assertEqual(frame["is_roll_day"].tolist(), _EXPECTED_ROLL_FLAGS)
        self.assertEqual(
            frame["roll_gap"].tolist(), [0.0, 0.0, 100.0, 0.0, 0.0, 50.0, 0.0]
        )
        first_roll_ratio = 1100.0 / 1000.0
        second_roll_ratio = 1180.0 / 1130.0
        expected_adjustments = [
            first_roll_ratio * second_roll_ratio,
            first_roll_ratio * second_roll_ratio,
            second_roll_ratio,
            second_roll_ratio,
            second_roll_ratio,
            1.0,
            1.0,
        ]
        expected_adjusted_close = [
            1000.0 * expected_adjustments[0],
            1010.0 * expected_adjustments[1],
            1100.0 * expected_adjustments[2],
            1120.0 * expected_adjustments[3],
            1130.0 * expected_adjustments[4],
            1180.0,
            1190.0,
        ]
        self.assertEqual(
            frame["cumulative_roll_adjustment"].tolist(), expected_adjustments
        )
        self.assertEqual(
            frame["roll_ratio"].tolist(),
            [1.0, 1.0, 1.1, 1.0, 1.0, second_roll_ratio, 1.0],
        )
        self.assertTrue(
            all(
                abs(actual - expected) < 0.000001
                for actual, expected in zip(
                    frame["adjusted_close"].tolist(), expected_adjusted_close
                )
            )
        )
        roll_row = frame[frame["trade_date"].eq(date(2025, 1, 3))].iloc[0]
        self.assertAlmostEqual(roll_row["naive_return"], 1100.0 / 1010.0 - 1)
        self.assertAlmostEqual(roll_row["continuous_return"], 1000.0 / 1010.0 - 1)
        self.assertEqual(roll_row["performance_price_field"], "adjusted_close")
        self.assertEqual(roll_row["audit_settle_field"], "adjusted_settle")

    def test_settle_audit_series_is_retained(self) -> None:
        with TemporaryDirectory() as temp_dir:
            lakehouse_root = Path(temp_dir) / "lakehouse"
            _write_stage2_fixture(lakehouse_root)

            frame = build_continuous_contract(lakehouse_root=lakehouse_root)

        self.assertIn("settle_return_audit", frame.columns)
        first_roll_ratio = 1101.0 / 1001.0
        second_roll_ratio = 1181.0 / 1131.0
        expected_adjustments = [
            first_roll_ratio * second_roll_ratio,
            first_roll_ratio * second_roll_ratio,
            second_roll_ratio,
            second_roll_ratio,
            second_roll_ratio,
            1.0,
            1.0,
        ]
        expected_adjusted_settle = [
            1001.0 * expected_adjustments[0],
            1011.0 * expected_adjustments[1],
            1101.0 * expected_adjustments[2],
            1121.0 * expected_adjustments[3],
            1131.0 * expected_adjustments[4],
            1181.0,
            1191.0,
        ]
        self.assertTrue(
            all(
                abs(actual - expected) < 0.000001
                for actual, expected in zip(
                    frame["adjusted_settle"].tolist(), expected_adjusted_settle
                )
            )
        )
        self.assertAlmostEqual(frame.loc[2, "settle_return_audit"], 1001.0 / 1011.0 - 1)

    def test_writes_derived_parquet_and_report(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            lakehouse_root = root / "lakehouse"
            output = root / "stage-2.md"
            _write_stage2_fixture(lakehouse_root)

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
            parquet_path = (
                lakehouse_root
                / "derived"
                / "derived.futures_continuous_contract"
                / "M.DCE"
                / "part-00000.parquet"
            )
            written = pd.read_parquet(parquet_path)

        self.assertIn("比值法后向复权", text)
        self.assertIn("continuous_return", text)
        self.assertEqual(len(written), 7)
        self.assertIn("settle_return_audit", written.columns)

    def test_write_helper_returns_manifest(self) -> None:
        with TemporaryDirectory() as temp_dir:
            lakehouse_root = Path(temp_dir) / "lakehouse"
            _write_stage2_fixture(lakehouse_root)
            frame = build_continuous_contract(lakehouse_root=lakehouse_root)

            result = write_continuous_contract(
                frame=frame,
                lakehouse_root=lakehouse_root,
                root_code="M.DCE",
            )

        self.assertEqual(result.row_count, 7)
        self.assertEqual(
            result.relative_path,
            "derived/derived.futures_continuous_contract/M.DCE/part-00000.parquet",
        )


_DATES = [
    date(2025, 1, 1),
    date(2025, 1, 2),
    date(2025, 1, 3),
    date(2025, 1, 6),
    date(2025, 1, 7),
    date(2025, 1, 8),
    date(2025, 1, 9),
]
_EXPECTED_CONTRACTS = [
    "M2501.DCE",
    "M2501.DCE",
    "M2505.DCE",
    "M2505.DCE",
    "M2505.DCE",
    "M2509.DCE",
    "M2509.DCE",
]
_EXPECTED_ROLL_FLAGS = [False, False, True, False, False, True, False]


def _write_stage2_fixture(lakehouse_root: Path) -> None:
    """Write a two-roll normalized parquet fixture."""

    mapping = pd.DataFrame(
        {
            "root_code": ["M.DCE"] * len(_DATES),
            "trade_date": _DATES,
            "active_contract": _EXPECTED_CONTRACTS,
            "raw_batch_id": [11] * len(_DATES),
        }
    )
    daily_rows = [
        _daily_row("M2501.DCE", date(2025, 1, 1), 1000.0, 101),
        _daily_row("M2501.DCE", date(2025, 1, 2), 1010.0, 101),
        _daily_row("M2501.DCE", date(2025, 1, 3), 1000.0, 101),
        _daily_row("M2505.DCE", date(2025, 1, 3), 1100.0, 102),
        _daily_row("M2505.DCE", date(2025, 1, 6), 1120.0, 102),
        _daily_row("M2505.DCE", date(2025, 1, 7), 1130.0, 102),
        _daily_row("M2505.DCE", date(2025, 1, 8), 1130.0, 102),
        _daily_row("M2509.DCE", date(2025, 1, 8), 1180.0, 103),
        _daily_row("M2509.DCE", date(2025, 1, 9), 1190.0, 103),
    ]
    _write_parquet(
        lakehouse_root,
        "normalized/market.futures_mapping/2025/01/part-00000.parquet",
        mapping,
    )
    _write_parquet(
        lakehouse_root,
        "normalized/market.futures_contract_daily/2025/01/part-00000.parquet",
        pd.DataFrame(daily_rows),
    )


def _daily_row(
    contract_code: str, trade_date: date, close: float, raw_batch_id: int
) -> dict[str, object]:
    """Return one normalized futures daily row."""

    return {
        "contract_code": contract_code,
        "trade_date": trade_date,
        "pre_close": close - 1,
        "pre_settle": close,
        "open": close - 2,
        "high": close + 8,
        "low": close - 8,
        "close": close,
        "settle": close + 1,
        "change1": 1.0,
        "change2": 1.0,
        "volume": 1000.0,
        "amount": 1.0,
        "oi": 2000.0,
        "oi_chg": 10.0,
        "raw_batch_id": raw_batch_id,
    }


def _write_parquet(
    lakehouse_root: Path, relative_path: str, frame: pd.DataFrame
) -> None:
    """Write one parquet file below the temporary lakehouse root."""

    path = lakehouse_root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


if __name__ == "__main__":
    unittest.main()
