"""Tests for commodity futures Stage 3 quality cards."""

from __future__ import annotations

from datetime import date
import math
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import pandas as pd
from click.testing import CliRunner

from tradepilot.etl.futures_stage3 import (
    QualityDecision,
    _annualized_return,
    build_quality_card,
    main,
    render_quality_card,
    render_quality_card_json,
)


class FuturesStage3Tests(unittest.TestCase):
    """Verify single-root Stage 3 quality-card metrics."""

    def test_builds_accept_quality_card_from_continuous_contract(self) -> None:
        with TemporaryDirectory() as temp_dir:
            lakehouse_root = Path(temp_dir) / "lakehouse"
            _write_stage3_fixture(lakehouse_root)

            card = build_quality_card(
                lakehouse_root=lakehouse_root,
                min_return_rows=4,
                portfolio_notional=100_000.0,
                target_weight=0.10,
            )

        self.assertEqual(card.root_code, "M.DCE")
        self.assertEqual(card.decision, QualityDecision.ACCEPT)
        self.assertEqual(card.row_count, 6)
        self.assertEqual(card.return_count, 5)
        self.assertEqual(card.return_missing_count, 1)
        self.assertEqual(card.roll_count, 1)
        self.assertEqual(card.abnormal_roll_count, 0)
        self.assertEqual(card.zero_volume_days, 0)
        self.assertEqual(card.zero_oi_days, 0)
        self.assertEqual(card.latest_contract, "M2505.DCE")
        self.assertEqual(card.multiplier, 10.0)
        self.assertEqual(card.one_lot_notional, 10_500.0)
        self.assertEqual(card.nearest_lots, 1)
        self.assertAlmostEqual(card.integer_lot_error_pct, 0.05)

    def test_render_includes_decision_and_no_basket_claim(self) -> None:
        with TemporaryDirectory() as temp_dir:
            lakehouse_root = Path(temp_dir) / "lakehouse"
            _write_stage3_fixture(lakehouse_root)

            card = build_quality_card(
                lakehouse_root=lakehouse_root,
                min_return_rows=4,
            )
            text = render_quality_card(card)

        self.assertIn("阶段 3", text)
        self.assertIn("结论：`accept`", text)
        self.assertIn("不构建商品篮子", text)

    def test_render_json_includes_structured_decision(self) -> None:
        with TemporaryDirectory() as temp_dir:
            lakehouse_root = Path(temp_dir) / "lakehouse"
            _write_stage3_fixture(lakehouse_root)

            card = build_quality_card(
                lakehouse_root=lakehouse_root,
                min_return_rows=4,
            )
            text = render_quality_card_json(card)

        self.assertIn('"schema_version": 1', text)
        self.assertIn('"decision": "accept"', text)
        self.assertIn('"root_code": "M.DCE"', text)

    def test_cli_writes_quality_card_report(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            lakehouse_root = root / "lakehouse"
            output = root / "stage-3.md"
            json_output = root / "stage-3.json"
            _write_stage3_fixture(lakehouse_root)

            result = CliRunner().invoke(
                main,
                [
                    "--lakehouse-root",
                    str(lakehouse_root),
                    "--output",
                    str(output),
                    "--json-output",
                    str(json_output),
                ],
            )

            self.assertEqual(result.exit_code, 0, result.output)
            text = output.read_text(encoding="utf-8")
            json_text = json_output.read_text(encoding="utf-8")

        self.assertIn("单品种质量卡", text)
        self.assertIn('"decision": "reject"', json_text)
        self.assertIn("decision=reject", result.output)

    def test_rejects_missing_stage2_continuous_contract(self) -> None:
        with TemporaryDirectory() as temp_dir:
            lakehouse_root = Path(temp_dir) / "lakehouse"

            with self.assertRaisesRegex(ValueError, "missing Stage 2"):
                build_quality_card(lakehouse_root=lakehouse_root)

    def test_rejects_missing_core_fields_after_first_return(self) -> None:
        with TemporaryDirectory() as temp_dir:
            lakehouse_root = Path(temp_dir) / "lakehouse"
            _write_stage3_fixture(lakehouse_root, missing_return=True)

            with self.assertRaisesRegex(ValueError, "missing core fields"):
                build_quality_card(lakehouse_root=lakehouse_root)

    def test_rejects_first_row_missing_price_or_liquidity(self) -> None:
        with TemporaryDirectory() as temp_dir:
            lakehouse_root = Path(temp_dir) / "lakehouse"
            _write_stage3_fixture(lakehouse_root, first_raw_close_missing=True)

            with self.assertRaisesRegex(ValueError, "missing core fields"):
                build_quality_card(lakehouse_root=lakehouse_root)

    def test_rejects_negative_volume_or_oi(self) -> None:
        with TemporaryDirectory() as temp_dir:
            lakehouse_root = Path(temp_dir) / "lakehouse"
            _write_stage3_fixture(lakehouse_root, negative_volume=True)

            with self.assertRaisesRegex(ValueError, "negative volume/oi"):
                build_quality_card(lakehouse_root=lakehouse_root)

    def test_full_zero_volume_or_oi_rejects_card(self) -> None:
        with TemporaryDirectory() as temp_dir:
            lakehouse_root = Path(temp_dir) / "lakehouse"
            _write_stage3_fixture(lakehouse_root, zero_volume=True)

            card = build_quality_card(
                lakehouse_root=lakehouse_root,
                min_return_rows=4,
            )

        self.assertEqual(card.decision, QualityDecision.REJECT)
        self.assertIn("volume is zero for the full sample", card.decision_reasons)

    def test_no_roll_average_holding_days_is_zero(self) -> None:
        with TemporaryDirectory() as temp_dir:
            lakehouse_root = Path(temp_dir) / "lakehouse"
            _write_stage3_fixture(lakehouse_root, no_roll=True)

            card = build_quality_card(
                lakehouse_root=lakehouse_root,
                min_return_rows=4,
            )

        self.assertEqual(card.roll_count, 0)
        self.assertEqual(card.average_holding_days, 0.0)

    def test_annualized_return_returns_nan_for_wiped_out_series(self) -> None:
        result = _annualized_return(pd.Series([0.01, -1.0]))

        self.assertTrue(math.isnan(result))

    def test_rejects_conflicting_instrument_metadata(self) -> None:
        with TemporaryDirectory() as temp_dir:
            lakehouse_root = Path(temp_dir) / "lakehouse"
            _write_stage3_fixture(lakehouse_root, duplicate_instrument=True)

            with self.assertRaisesRegex(
                ValueError, "instrument metadata is not unique"
            ):
                build_quality_card(lakehouse_root=lakehouse_root, min_return_rows=4)


def _write_stage3_fixture(
    lakehouse_root: Path,
    *,
    missing_return: bool = False,
    first_raw_close_missing: bool = False,
    negative_volume: bool = False,
    zero_volume: bool = False,
    no_roll: bool = False,
    duplicate_instrument: bool = False,
) -> None:
    """Write a minimal Stage 2 derived frame and instrument metadata."""

    dates = [
        date(2025, 1, 1),
        date(2025, 1, 2),
        date(2025, 1, 3),
        date(2025, 1, 6),
        date(2025, 1, 7),
        date(2025, 1, 8),
    ]
    returns = [pd.NA, 0.01, 0.009900990099, 0.009803921569, -0.004854368932, 0.01]
    if missing_return:
        returns[3] = pd.NA
    active_contracts = [
        "M2501.DCE",
        "M2501.DCE",
        "M2501.DCE",
        "M2505.DCE",
        "M2505.DCE",
        "M2505.DCE",
    ]
    roll_days = [False, False, False, True, False, False]
    if no_roll:
        active_contracts = ["M2505.DCE"] * len(dates)
        roll_days = [False] * len(dates)
    raw_close = [1000.0, 1010.0, 1020.0, 1030.0, 1040.0, 1050.0]
    if first_raw_close_missing:
        raw_close[0] = pd.NA
    volume = [1000.0, 1100.0, 1200.0, 1300.0, 1400.0, 1500.0]
    if negative_volume:
        volume[0] = -1.0
    if zero_volume:
        volume = [0.0] * len(dates)
    frame = pd.DataFrame(
        {
            "trade_date": dates,
            "root_symbol": ["M.DCE"] * len(dates),
            "active_contract": active_contracts,
            "raw_close": raw_close,
            "adjusted_close": [1000.0, 1010.0, 1020.0, 1030.0, 1025.0, 1035.25],
            "continuous_return": returns,
            "volume": volume,
            "oi": [2000.0, 2100.0, 2200.0, 2300.0, 2400.0, 2500.0],
            "is_roll_day": roll_days,
        }
    )
    _write_parquet(
        lakehouse_root,
        "derived/derived.futures_continuous_contract/M.DCE/part-00000.parquet",
        frame,
    )
    instruments = pd.DataFrame(
        {
            "contract_code": ["M2505.DCE"],
            "multiplier": [10.0],
            "trade_unit": ["吨"],
            "quote_unit": ["人民币元/吨"],
        }
    )
    if duplicate_instrument:
        instruments = pd.concat(
            [
                instruments,
                pd.DataFrame(
                    {
                        "contract_code": ["M2505.DCE"],
                        "multiplier": [20.0],
                        "trade_unit": ["吨"],
                        "quote_unit": ["人民币元/吨"],
                    }
                ),
            ],
            ignore_index=True,
        )
    _write_parquet(
        lakehouse_root,
        "normalized/reference.futures_instruments/2025/01/part-00000.parquet",
        instruments,
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
